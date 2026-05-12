#!/usr/bin/env python3
"""Generic PDP probe for Shopify collection pages.

Collects product handles from one or more collection feeds, visits each PDP,
and extracts configured selector child content into Excel columns.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, FeatureNotFound, Tag
from openpyxl import Workbook

# =========================
# USER INPUTS (EDIT THESE)
# =========================
BRAND = "Paige"
COLLECTION_URL = [
    "https://shop.paige.com/collections/women-denim",
]
SELECTOR = "#headlessui-disclosure-panel-_r_27_ > div > ul > li"
# Optional click-open controls for accordions/tabs.
# Example for Paige:
# PARENT_SELECTOR = "div[data-headlessui-state]"
# SET_PARENT_SELECTOR_TO_OPEN = True
PARENT_SELECTOR = ""
SET_PARENT_SELECTOR_TO_OPEN = False
CLICK_TARGET_TEXTS: List[str] = ["DETAILS"]
BROWSER_RENDER_ENABLED = True
BROWSER_RENDER_TIMEOUT_MS = 45000

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
TIMEOUT = 30
MAX_RETRIES = 5


@dataclass
class PDPRow:
    handle: str
    child_values: Dict[str, str]
    url: str


def setup_logger() -> logging.Logger:
    logger = logging.getLogger("retail_pdp_probe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    timestamp = datetime.now().strftime("%Y-%m-%d")
    preferred_log = OUTPUT_DIR / f"{BRAND.lower()}_pdp_probe_{timestamp}.log"
    fallback_log = OUTPUT_DIR / f"{BRAND.lower()}_run.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    try:
        file_handler = logging.FileHandler(preferred_log, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as exc:  # pragma: no cover
        logger.warning(
            "Could not open preferred log file (%s). Falling back to %s. Error: %s",
            preferred_log,
            fallback_log,
            exc,
        )
        try:
            fallback_handler = logging.FileHandler(fallback_log, encoding="utf-8")
            fallback_handler.setFormatter(formatter)
            logger.addHandler(fallback_handler)
        except Exception as fallback_exc:  # pragma: no cover
            logger.warning("Failed to configure fallback log file: %s", fallback_exc)

    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"})
    return session


def get_with_retries(session: requests.Session, url: str, logger: logging.Logger) -> requests.Response:
    delay = 1.0
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=TIMEOUT)
            if response.status_code in {429, 500, 502, 503, 504}:
                logger.warning("Transient HTTP %s for %s (attempt %s/%s)", response.status_code, url, attempt, MAX_RETRIES)
                time.sleep(delay)
                delay *= 2
                continue
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Request error for %s (attempt %s/%s): %s", url, attempt, MAX_RETRIES, exc)
            time.sleep(delay)
            delay *= 2

    raise RuntimeError(f"Failed request after {MAX_RETRIES} attempts: {url} | Last error: {last_error}")


def iter_collection_products(session: requests.Session, collection_url: str, logger: logging.Logger) -> Iterable[dict]:
    page = 1
    while True:
        feed_url = f"{collection_url.rstrip('/')}/products.json?limit=250&page={page}"
        response = get_with_retries(session, feed_url, logger)
        payload = response.json()
        products = payload.get("products") or []
        logger.info("Collection %s page %s returned %s products", collection_url, page, len(products))
        if not products:
            break
        for product in products:
            yield product
        page += 1


def normalize_text(tag: Tag) -> str:
    text = " ".join(part.strip() for part in tag.stripped_strings if part.strip())
    return re.sub(r"\s+", " ", text).strip()


def child_letter(index: int) -> str:
    letters = ""
    value = index
    while value > 0:
        value -= 1
        letters = chr(ord("a") + (value % 26)) + letters
        value //= 26
    return letters


def selector_with_dynamic_id_fallbacks(selector: str) -> List[str]:
    candidates = [selector]

    normalized_shopify = re.sub(
        r"#shopify-section-template--\d+__default",
        "[id^='shopify-section-template--'][id$='__default']",
        selector,
    )
    if normalized_shopify not in candidates:
        candidates.append(normalized_shopify)

    normalized_details = re.sub(
        r"#Details-Content-content_block_[A-Za-z0-9_-]+",
        "[id^='Details-Content-content_block_']",
        selector,
    )
    if normalized_details not in candidates:
        candidates.append(normalized_details)

    normalized_headless = re.sub(
        r"#headlessui-disclosure-panel-[A-Za-z0-9_-]+",
        "[id^='headlessui-disclosure-panel-']",
        selector,
    )
    if normalized_headless not in candidates:
        candidates.append(normalized_headless)

    return candidates


def find_parent_with_fallbacks(soup: BeautifulSoup, selector: str, logger: logging.Logger) -> Tag | None:
    for candidate in selector_with_dynamic_id_fallbacks(selector):
        parent = soup.select_one(candidate)
        if parent is not None:
            if candidate != selector:
                logger.info("Selector matched with dynamic-id fallback: %s", candidate)
            return parent

    # For strongly scoped dynamic-id selectors (Headless UI / details blocks),
    # do not fall back to overly broad reduced paths like "div > ul > li".
    if "headlessui-disclosure-panel" in selector or "Details-Content-content_block_" in selector:
        return None

    parts = [part.strip() for part in selector.split(">") if part.strip()]
    for start in range(1, len(parts)):
        reduced = " > ".join(parts[start:])
        for candidate in selector_with_dynamic_id_fallbacks(reduced):
            parent = soup.select_one(candidate)
            if parent is not None:
                logger.info("Selector matched with reduced path fallback: %s", candidate)
                return parent

    return soup.select_one("div.product-features-content.product-web-des-2 > ul")


def build_soup_candidates(html: str) -> List[BeautifulSoup]:
    soups: List[BeautifulSoup] = []
    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html, parser)
        except FeatureNotFound:
            continue
        soups.append(soup)
    if not soups:  # pragma: no cover
        soups.append(BeautifulSoup(html, "html.parser"))
    return soups


def extract_child_columns_from_soup(soup: BeautifulSoup, selector: str, logger: logging.Logger) -> Dict[str, str]:
    for candidate in selector_with_dynamic_id_fallbacks(selector):
        matches = [node for node in soup.select(candidate) if isinstance(node, Tag)]
        if len(matches) > 1:
            if candidate != selector:
                logger.info("Selector matched with dynamic-id list fallback: %s", candidate)
            values = [normalize_text(node) for node in matches]
            values = [v for v in values if v]
            return {f"Child {idx}": value for idx, value in enumerate(values, start=1)}

    parent = find_parent_with_fallbacks(soup, selector, logger)
    if parent is None:
        return {}

    result: Dict[str, str] = {}
    children = [child for child in parent.find_all(recursive=False) if isinstance(child, Tag)]
    for child_index, child in enumerate(children, start=1):
        nested = [node for node in child.find_all(recursive=False) if isinstance(node, Tag)]
        if nested:
            for nested_index, node in enumerate(nested, start=1):
                value = normalize_text(node)
                if value:
                    result[f"Child {child_index}{child_letter(nested_index)}"] = value
        else:
            value = normalize_text(child)
            if value:
                result[f"Child {child_index}"] = value

    return result


def extract_child_columns(html: str, selector: str, logger: logging.Logger) -> Dict[str, str]:
    best_result: Dict[str, str] = {}
    best_len = 0

    for soup in build_soup_candidates(html):
        result = extract_child_columns_from_soup(soup, selector, logger)
        if len(result) > best_len:
            best_result = result
            best_len = len(result)

    return best_result


def render_page_html_with_clicks(
    url: str,
    parent_selector: str,
    set_parent_selector_to_open: bool,
    click_target_texts: Sequence[str],
    logger: logging.Logger,
    timeout_ms: int = BROWSER_RENDER_TIMEOUT_MS,
) -> Optional[str]:
    """Render a PDP with Playwright, click disclosure toggles, and return DOM HTML."""
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        logger.info("Playwright unavailable, skipping browser-render pass: %s", exc)
        return None

    def dismiss_known_overlays(page) -> None:
        overlay_selectors = [
            "#attentive_overlay",
            "iframe#attentive_creative",
            "[id*='attentive_overlay']",
            "[data-testid*='attentive']",
        ]
        for selector in overlay_selectors:
            try:
                page.evaluate(
                    """(sel) => {
                        document.querySelectorAll(sel).forEach((el) => {
                            try { el.remove(); } catch (e) {}
                            if (el.style) {
                                el.style.display = 'none';
                                el.style.visibility = 'hidden';
                                el.style.pointerEvents = 'none';
                            }
                        });
                    }""",
                    selector,
                )
            except Exception:
                continue
        try:
            page.add_style_tag(
                content="""
                #attentive_overlay, iframe#attentive_creative, [id*='attentive_overlay'] {
                    display: none !important;
                    visibility: hidden !important;
                    pointer-events: none !important;
                }
                """
            )
        except Exception:
            pass

    def click_with_fallback(locator, label: str) -> bool:
        try:
            locator.click(timeout=5000)
            return True
        except Exception as exc:
            logger.info("Normal click failed for %s; retrying with force: %s", label, exc)
        try:
            locator.click(timeout=5000, force=True)
            return True
        except Exception as exc:
            logger.info("Force click failed for %s; retrying with JS click: %s", label, exc)
        try:
            handle = locator.element_handle(timeout=5000)
            if handle is None:
                return False
            locator.page.evaluate("(el) => el.click()", handle)
            return True
        except Exception as exc:
            logger.info("JS click failed for %s: %s", label, exc)
            return False

    html: Optional[str] = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 15000))
            except PlaywrightTimeoutError:
                logger.info("Network idle wait timed out for %s; continuing", url)

            dismiss_known_overlays(page)
            clicked = 0

            if set_parent_selector_to_open and parent_selector.strip():
                parent_locator = page.locator(parent_selector)
                parent_count = parent_locator.count()
                logger.info("Found %s parent-selector nodes for click-open check", parent_count)
                for idx in range(parent_count):
                    current = parent_locator.nth(idx)
                    state = (current.get_attribute("data-headlessui-state") or "").strip().lower()
                    aria_expanded = (current.get_attribute("aria-expanded") or "").strip().lower()
                    if state == "open" or aria_expanded == "true":
                        continue
                    button_locator = current.locator("button,[role='button']")
                    if button_locator.count() > 0:
                        if click_with_fallback(button_locator.first, f"{parent_selector}[{idx}] button"):
                            clicked += 1
                    elif click_with_fallback(current, f"{parent_selector}[{idx}]"):
                        clicked += 1

            for label in click_target_texts:
                target = label.strip()
                if not target:
                    continue
                locator = page.locator(f"button:has-text('{target}')")
                if locator.count() > 0:
                    first = locator.first
                    state = (first.get_attribute("data-headlessui-state") or "").strip().lower()
                    aria_expanded = (first.get_attribute("aria-expanded") or "").strip().lower()
                    if state != "open" and aria_expanded != "true":
                        if click_with_fallback(first, f"button:{target}"):
                            clicked += 1
                    continue

                fallback = page.locator(f"text={target}")
                if fallback.count() > 0 and click_with_fallback(fallback.first, f"text:{target}"):
                    clicked += 1

            if clicked:
                page.wait_for_timeout(500)
            else:
                logger.info("No click target matched for %s", url)

            html = page.content()
            browser.close()
    except Exception as exc:
        logger.warning("Browser-render pass failed for %s: %s", url, exc)
        return None

    return html


def select_best_extraction(
    html_candidates: Sequence[Tuple[str, str]],
    selector: str,
    logger: logging.Logger,
) -> Dict[str, str]:
    best: Dict[str, str] = {}
    best_count = 0
    best_source = ""
    for source_label, html in html_candidates:
        cols = extract_child_columns(html, selector, logger)
        if len(cols) > best_count:
            best = cols
            best_count = len(cols)
            best_source = source_label
    if best_source:
        logger.info("Selector extraction source chosen: %s (%s columns)", best_source, best_count)
    return best


def probe_pdp_rows(session: requests.Session, logger: logging.Logger) -> List[PDPRow]:
    seen_handles: set[str] = set()
    rows: List[PDPRow] = []

    for collection_url in COLLECTION_URL:
        for product in iter_collection_products(session, collection_url, logger):
            handle = (product.get("handle") or "").strip()
            if not handle or handle in seen_handles:
                continue
            seen_handles.add(handle)

            product_url = urljoin(collection_url, f"/products/{handle}")
            try:
                html_candidates: List[Tuple[str, str]] = []
                try:
                    response = get_with_retries(session, product_url, logger)
                    html_candidates.append(("http", response.text))
                except Exception as exc:
                    logger.warning("HTTP fetch failed for %s; will try browser-render path: %s", product_url, exc)

                if BROWSER_RENDER_ENABLED and (CLICK_TARGET_TEXTS or (SET_PARENT_SELECTOR_TO_OPEN and PARENT_SELECTOR.strip())):
                    rendered_html = render_page_html_with_clicks(
                        product_url,
                        PARENT_SELECTOR,
                        SET_PARENT_SELECTOR_TO_OPEN,
                        CLICK_TARGET_TEXTS,
                        logger,
                    )
                    if rendered_html:
                        html_candidates.insert(0, ("browser_click", rendered_html))

                if not html_candidates:
                    raise RuntimeError("No HTML candidates available after HTTP and browser-render attempts")

                child_values = select_best_extraction(html_candidates, SELECTOR, logger)
                rows.append(PDPRow(handle=handle, child_values=child_values, url=product_url))
                logger.info("PDP parsed | handle=%s | columns=%s", handle, len(child_values))
            except Exception as exc:
                logger.error("Failed PDP parse for %s (%s): %s", handle, product_url, exc)
                rows.append(PDPRow(handle=handle, child_values={}, url=product_url))

    return rows


def column_sort_key(name: str) -> tuple[int, int, str]:
    match = re.fullmatch(r"Child\s+(\d+)([a-z]+)?", name)
    if not match:
        return (10_000, 0, name)
    number = int(match.group(1))
    suffix = match.group(2) or ""
    if not suffix:
        return (number, 0, "")
    return (number, 1, suffix)


def export_to_excel(rows: List[PDPRow], logger: logging.Logger) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"{BRAND}_PDP_{timestamp}.xlsx"

    all_child_headers = sorted({header for row in rows for header in row.child_values}, key=column_sort_key)
    headers = ["product.handle", *all_child_headers, "product.url"]

    wb = Workbook()
    ws = wb.active
    ws.title = "PDP Probe"
    ws.append(headers)

    for row in rows:
        ws.append([row.handle, *[row.child_values.get(header, "") for header in all_child_headers], row.url])

    wb.save(output_path)
    logger.info("Excel written: %s", output_path.resolve())
    return output_path


def main() -> None:
    logger = setup_logger()
    logger.info("Starting PDP probe for brand: %s", BRAND)
    logger.info("Configured %s collection URLs", len(COLLECTION_URL))

    session = build_session()
    rows = probe_pdp_rows(session, logger)
    logger.info("Collected %s PDP rows", len(rows))
    export_to_excel(rows, logger)


if __name__ == "__main__":
    main()
