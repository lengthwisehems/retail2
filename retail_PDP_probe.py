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
from typing import Dict, Iterable, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, FeatureNotFound, Tag
from openpyxl import Workbook

# =========================
# USER INPUTS (EDIT THESE)
# =========================
BRAND = "Ksubi"
COLLECTION_URL = [
    "https://ksubi.com/collections/womens-denim",
    "https://ksubi.com/collections/womens-denim-sale",
]
SELECTOR = (
    "#shopify-section-template--19885400981690__default > section > div > section > "
    "div > div.product_section.js-product_section.container.is-justify-space-between."
    "has-padding-bottom > div.product__information.has-product-sticker.one-half.column."
    "medium-down--one-whole > div > div.product-features-section.product-features-features.active "
    "> div.product-features-content.product-web-des-2 > ul"
)

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


def find_parent_with_fallbacks(soup: BeautifulSoup, selector: str, logger: logging.Logger) -> Tag | None:
    parent = soup.select_one(selector)
    if parent is not None:
        return parent

    normalized = re.sub(r"#shopify-section-template--\d+__default", "[id^='shopify-section-template--'][id$='__default']", selector)
    if normalized != selector:
        parent = soup.select_one(normalized)
        if parent is not None:
            logger.info("Selector matched with dynamic section-id fallback")
            return parent

    # Staud (and similar themes) use block ids like:
    # #Details-Content-content_block_NMNAMR > div
    # where the trailing block token can vary by product.
    details_normalized = re.sub(
        r"#Details-Content-content_block_[A-Za-z0-9_-]+",
        "[id^='Details-Content-content_block_']",
        selector,
    )
    if details_normalized != selector:
        parent = soup.select_one(details_normalized)
        if parent is not None:
            logger.info("Selector matched with dynamic details-block-id fallback")
            return parent

    parts = [part.strip() for part in selector.split(">") if part.strip()]
    for start in range(1, len(parts)):
        reduced = " > ".join(parts[start:])
        parent = soup.select_one(reduced)
        if parent is not None:
            logger.info("Selector matched with reduced path fallback: %s", reduced)
            return parent

    return soup.select_one("div.product-features-content.product-web-des-2 > ul")




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

    return candidates


def build_soup_candidates(html: str) -> List[BeautifulSoup]:
    soups: List[BeautifulSoup] = []
    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html, parser)
        except FeatureNotFound:
            continue
        soups.append(soup)
    if not soups:  # pragma: no cover - BeautifulSoup always has html.parser
        soups.append(BeautifulSoup(html, "html.parser"))
    return soups


def extract_child_columns_from_soup(soup: BeautifulSoup, selector: str, logger: logging.Logger) -> Dict[str, str]:

    # If selector targets a repeated child (e.g., ... > li), capture every match
    # instead of only traversing from a single parent node.
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
                response = get_with_retries(session, product_url, logger)
                child_values = extract_child_columns(response.text, SELECTOR, logger)
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
