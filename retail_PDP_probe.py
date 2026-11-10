#!/usr/bin/env python3
"""Generic PDP probe for Shopify collection pages.

Collects product handles from one or more collection feeds, visits each PDP,
and extracts configured selector child content into Excel columns.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Comment, FeatureNotFound, NavigableString, Tag
from openpyxl import Workbook

# =========================
# USER INPUTS (EDIT THESE)
# =========================
BRAND = "AYR"
COLLECTION_URL = [
    "https://ayr.com/collections/final-sale-womenswear",
    "https://ayr.com/collections/jeans-womenswear",
]
SELECTOR = "#shopify-section-template--20264212136015__product-details > section"
# Optional click-open controls for accordions/tabs.
# Example for Paige:
# PARENT_SELECTOR = "div[data-headlessui-state]"
# SET_PARENT_SELECTOR_TO_OPEN = True
PARENT_SELECTOR = ""
SET_PARENT_SELECTOR_TO_OPEN = False
# Button texts to click open before reading the page (e.g. ["DETAILS"] for Paige).
# Leave empty when the selector content is already in the page (AYR). The browser
# is still used automatically for any page that gets a bot check.
CLICK_TARGET_TEXTS: List[str] = []
BROWSER_RENDER_ENABLED = True
BROWSER_RENDER_TIMEOUT_MS = 45000
# When the site blocks plain requests (HTTP 403/429), the run switches to a real
# browser. It starts hidden (headless); if that is blocked too and this is True,
# it reopens in a visible Chrome window. If a "verify you are human" box shows
# in that window, click it - the script waits BROWSER_VISIBLE_WAIT_MS for you.
BROWSER_HEADLESS = True
BROWSER_RETRY_VISIBLE = True
BROWSER_CHALLENGE_WAIT_MS = 20000
BROWSER_VISIBLE_WAIT_MS = 60000

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
# Headers for plain (non-browser) requests. Kept deliberately simple: on a
# blocked IP, a full fake set of Chrome headers (Sec-Fetch-*, sec-ch-ua) drew a
# hard 403 where these drew a softer 429 - claiming to be Chrome while not
# behaving like it looks more suspicious, not less.
HTTP_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"}
TIMEOUT = 30
MAX_RETRIES = 6
# 403 and 429 mean the site is blocking this client (a bot check, an access-denied
# page, or a rate limit). Waiting rarely clears these, so they get a short capped
# wait and few attempts, then the run falls back to a real browser.
BLOCK_STATUSES = {403, 429}
CHALLENGE_MAX_ATTEMPTS = 2
CHALLENGE_MAX_WAIT_SECONDS = 15.0
# Pace between requests. It adapts both ways: each 429 slows it down, each clean
# response brings it back toward the floor.
REQUEST_DELAY_SECONDS = 0.5
REQUEST_DELAY_MAX_SECONDS = 6.0
REQUEST_DELAY_STEP_SECONDS = 0.75
REQUEST_DELAY_DECAY = 0.9
# Backoff used for real rate limits / 5xx when the server sends no Retry-After.
RETRY_BASE_DELAY_SECONDS = 5.0
RETRY_MAX_DELAY_SECONDS = 60.0

_PACE: Dict[str, float] = {"interval": REQUEST_DELAY_SECONDS, "last": 0.0}
# Run-wide state: once plain requests are blocked, PDPs go straight to the browser.
_STATE: Dict[str, object] = {"http_blocked": False, "saved_block_pages": set()}


class BlockedError(RuntimeError):
    """The site refused this client (bot check, access denied or rate limit)."""


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
    session.headers.update(HTTP_HEADERS)
    return session


CHALLENGE_MARKERS = ("verifying your connection", "just a moment", "checking your browser", "cf-chl-")
BLOCK_TITLE_MARKERS = (
    "access denied", "attention required", "you have been blocked", "forbidden",
    "too many requests", "error 1020", "request blocked", "verify you are human",
)


def text_is_bot_challenge(text: str) -> bool:
    head = (text or "")[:5000].lower()
    return any(marker in head for marker in CHALLENGE_MARKERS)


def page_title(html: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s+", " ", match.group(1)).strip()[:120] if match else ""


def html_looks_blocked(html: str) -> bool:
    title = page_title(html).lower()
    return text_is_bot_challenge(html) or any(marker in title for marker in BLOCK_TITLE_MARKERS)


def is_bot_challenge(response: requests.Response) -> bool:
    """True for a bot-check interstitial (as opposed to a plain refusal)."""
    if response.headers.get("cf-mitigated", "").lower() == "challenge":
        return True
    return text_is_bot_challenge(response.text)


def is_blocked_response(response: requests.Response) -> bool:
    if response.status_code in BLOCK_STATUSES:
        return True
    return response.status_code == 503 and is_bot_challenge(response)


def describe_block(response: requests.Response) -> str:
    kind = "bot check" if is_bot_challenge(response) else (
        "rate limit" if response.status_code == 429 else "access denied"
    )
    title = page_title(response.text)
    return f"HTTP {response.status_code} {kind}" + (f" (page title: '{title}')" if title else "")


def save_block_page(response: requests.Response, logger: logging.Logger) -> None:
    """Keep one copy of each kind of block page so it can be inspected later."""
    saved = _STATE["saved_block_pages"]
    if response.status_code in saved:  # type: ignore[operator]
        return
    saved.add(response.status_code)  # type: ignore[union-attr]
    path = OUTPUT_DIR / f"{BRAND}_blocked_HTTP{response.status_code}.html"
    try:
        headers = "\n".join(f"{k}: {v}" for k, v in response.headers.items())
        path.write_text(f"<!-- URL: {response.url}\n{headers}\n-->\n{response.text}", encoding="utf-8")
        logger.info("Saved the blocked response for inspection: %s", path)
    except Exception as exc:  # pragma: no cover
        logger.info("Could not save the blocked response: %s", exc)


def pace_request() -> None:
    """Hold every request to the current global interval."""
    wait = _PACE["last"] + _PACE["interval"] - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _PACE["last"] = time.monotonic()


def slow_pace() -> None:
    _PACE["interval"] = min(REQUEST_DELAY_MAX_SECONDS, _PACE["interval"] + REQUEST_DELAY_STEP_SECONDS)


def relax_pace() -> None:
    _PACE["interval"] = max(REQUEST_DELAY_SECONDS, _PACE["interval"] * REQUEST_DELAY_DECAY)


def parse_retry_after(value: Optional[str]) -> Optional[float]:
    """Return seconds to wait from a Retry-After header (seconds form only)."""
    if not value:
        return None
    try:
        return max(0.0, float(value.strip()))
    except ValueError:
        return None


def get_with_retries(session: requests.Session, url: str, logger: logging.Logger) -> requests.Response:
    delay = RETRY_BASE_DELAY_SECONDS
    last_error: str = "none"
    blocked = False
    attempt = 0
    max_attempts = MAX_RETRIES
    while attempt < max_attempts:
        attempt += 1
        pace_request()
        try:
            response = session.get(url, timeout=TIMEOUT)
        except requests.RequestException as exc:
            blocked = False
            last_error = str(exc)
            if attempt < max_attempts:
                logger.warning("Request error for %s (attempt %s/%s): %s. Waiting %.0fs", url, attempt, max_attempts, exc, delay)
                time.sleep(delay)
                delay = min(delay * 2, RETRY_MAX_DELAY_SECONDS)
            continue

        if is_blocked_response(response):
            blocked = True
            slow_pace()
            last_error = describe_block(response)
            save_block_page(response, logger)
            max_attempts = min(max_attempts, CHALLENGE_MAX_ATTEMPTS)
            if attempt < max_attempts:
                retry_after = parse_retry_after(response.headers.get("Retry-After"))
                wait = min(retry_after if retry_after is not None else delay, CHALLENGE_MAX_WAIT_SECONDS)
                logger.warning(
                    "Blocked: %s for %s (attempt %s/%s). Waiting %.0fs",
                    last_error, url, attempt, max_attempts, wait,
                )
                time.sleep(wait)
            continue

        if response.status_code in {500, 502, 503, 504}:
            blocked = False
            last_error = f"HTTP {response.status_code}"
            wait = parse_retry_after(response.headers.get("Retry-After"))
            wait = min(wait, RETRY_MAX_DELAY_SECONDS) if wait is not None else delay
            if attempt < max_attempts:
                logger.warning(
                    "HTTP %s for %s (attempt %s/%s). Waiting %.0fs before retrying",
                    response.status_code, url, attempt, max_attempts, wait,
                )
                time.sleep(wait)
                delay = min(delay * 2, RETRY_MAX_DELAY_SECONDS)
            continue

        relax_pace()
        response.raise_for_status()
        return response

    message = f"Failed request after {attempt} attempts: {url} | Last error: {last_error}."
    if blocked:
        raise BlockedError(message + " The site is refusing plain requests from this IP; waiting will not clear it.")
    raise RuntimeError(message)


def warn_if_collection_missing(session: requests.Session, collection_url: str, logger: logging.Logger) -> None:
    """Shopify returns an empty product list for collections that don't exist, so check the page itself."""
    try:
        response = session.get(collection_url, timeout=TIMEOUT)
    except requests.RequestException:
        return
    if response.status_code == 404:
        logger.warning(
            "Collection page returned 404 - this collection URL does not exist. Check the URL: %s",
            collection_url,
        )


def iter_collection_products(session: requests.Session, collection_url: str, logger: logging.Logger) -> Iterable[dict]:
    page = 1
    while True:
        feed_url = f"{collection_url.rstrip('/')}/products.json?limit=250&page={page}"
        response = get_with_retries(session, feed_url, logger)
        payload = response.json()
        products = payload.get("products") or []
        logger.info("Collection %s page %s returned %s products", collection_url, page, len(products))
        if not products:
            if page == 1:
                warn_if_collection_missing(session, collection_url, logger)
            break
        for product in products:
            yield product
        page += 1


def load_sync_playwright(logger: logging.Logger):
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        logger.warning(
            "Playwright is not installed, so the browser fallback is unavailable (%s). "
            "Install it with: pip install playwright  then: playwright install chromium",
            exc,
        )
        return None
    return sync_playwright


class BrowserSession:
    """One real browser shared by the whole run.

    Sharing it means a bot check passed once stays passed (its cookies carry
    over to every later page), and the browser starts once instead of per page.
    It prefers the installed Google Chrome over Playwright's bundled Chromium,
    starts hidden, and - if the hidden browser is blocked - reopens visibly.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self._playwright = None
        self._browser = None
        self.context = None
        self.visible = False
        self.force_visible = not BROWSER_HEADLESS
        self.unavailable = False

    def _launch(self, visible: bool) -> bool:
        if self.context is not None and self.visible == visible:
            return True
        self._shutdown()
        sync_playwright = load_sync_playwright(self.logger)
        if sync_playwright is None:
            self.unavailable = True
            return False
        self._playwright = sync_playwright().start()
        mode = "visible window" if visible else "headless"
        for channel in ("chrome", None):
            name = "Google Chrome" if channel else "Playwright Chromium"
            kwargs = {"headless": not visible, "args": ["--disable-blink-features=AutomationControlled"]}
            if channel:
                kwargs["channel"] = channel
            try:
                self._browser = self._playwright.chromium.launch(**kwargs)
            except Exception as exc:
                self.logger.info("Could not start %s (%s): %s", name, mode, str(exc).strip().splitlines()[0])
                continue
            self.logger.info("Browser started: %s (%s)", name, mode)
            break
        if self._browser is None:
            self._shutdown()
            return False

        # A headless browser announces itself as "HeadlessChrome"; present it as
        # the same Chrome version a person would be running.
        major = (self._browser.version or "124").split(".")[0]
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"
        )
        extra_headers = {
            "sec-ch-ua": f'"Google Chrome";v="{major}", "Chromium";v="{major}", "Not_A Brand";v="24"',
            "sec-ch-ua-platform": '"Windows"',
        }
        self.context = self._browser.new_context(
            user_agent=user_agent,
            locale="en-US",
            viewport={"width": 1366, "height": 900},
            extra_http_headers=extra_headers,
        )
        self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.visible = visible
        return True

    def _shutdown(self) -> None:
        for closer in (self._browser, self._playwright):
            if closer is None:
                continue
            try:
                closer.close() if closer is self._browser else closer.stop()
            except Exception:
                pass
        self._browser = None
        self._playwright = None
        self.context = None

    def close(self) -> None:
        self._shutdown()

    def _wait_until_clear(self, page, nav_status: Dict[str, Optional[int]], label: str, wait_ms: int) -> bool:
        deadline = time.monotonic() + wait_ms / 1000
        announced = False
        while True:
            try:
                html = page.content()
            except Exception:
                html = ""
            status = nav_status.get("status")
            blocked = not html or html_looks_blocked(html) or (status is not None and status in BLOCK_STATUSES)
            if not blocked:
                if announced:
                    self.logger.info("Browser got past the block for %s", label)
                return True
            if time.monotonic() > deadline:
                self.logger.warning(
                    "Browser still blocked after %.0fs (HTTP %s, page title: '%s'): %s",
                    wait_ms / 1000, status, page_title(html), label,
                )
                return False
            if not announced:
                self.logger.info(
                    "Browser is blocked for %s (HTTP %s, page title: '%s'); waiting up to %.0fs for it to clear",
                    label, status, page_title(html), wait_ms / 1000,
                )
                announced = True
            page.wait_for_timeout(1000)

    def open_page(self, url: str):
        """Open url and wait out any block. Returns the page, or None if still blocked."""
        if self.unavailable:
            return None
        modes = [True] if self.force_visible else ([False, True] if BROWSER_RETRY_VISIBLE else [False])
        for visible in modes:
            if not self._launch(visible):
                if self.unavailable:
                    return None
                continue
            page = self.context.new_page()
            nav_status: Dict[str, Optional[int]] = {"status": None}

            def on_response(response, page=page, nav_status=nav_status) -> None:
                try:
                    if response.request.is_navigation_request() and response.frame == page.main_frame:
                        nav_status["status"] = response.status
                except Exception:
                    pass

            page.on("response", on_response)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_RENDER_TIMEOUT_MS)
                wait_ms = BROWSER_VISIBLE_WAIT_MS if visible else BROWSER_CHALLENGE_WAIT_MS
                if self._wait_until_clear(page, nav_status, url, wait_ms):
                    return page
            except Exception as exc:
                self.logger.warning("Browser could not load %s: %s", url, str(exc).strip().splitlines()[0])
            try:
                page.close()
            except Exception:
                pass
            if not visible and True in modes:
                self.logger.warning(
                    "The hidden browser was blocked; reopening in a visible Chrome window. "
                    "If a 'verify you are human' box appears there, click it."
                )
                self.force_visible = True
        return None


def handles_from_links(hrefs: Sequence[Optional[str]]) -> List[str]:
    handles: List[str] = []
    for href in hrefs:
        match = re.search(r"/products/([^/?#]+)", href or "")
        if match and match.group(1) not in handles:
            handles.append(match.group(1))
    return handles


def browser_collection_products(browser: BrowserSession, collection_url: str, logger: logging.Logger) -> List[dict]:
    """Read a collection's products through the real browser when plain requests are blocked."""
    page = browser.open_page(collection_url)
    if page is None:
        return []

    feed_path = f"{urlparse(collection_url).path.rstrip('/')}/products.json"
    products: List[dict] = []
    try:
        # Ask for the JSON feed from inside the page, so the request carries the
        # cookies the browser earned by getting past the block.
        page_no = 1
        while True:
            result = page.evaluate(
                """async (url) => {
                    const r = await fetch(url, {credentials: 'same-origin', headers: {Accept: 'application/json'}});
                    return {status: r.status, text: await r.text()};
                }""",
                f"{feed_path}?limit=250&page={page_no}",
            )
            status = result.get("status")
            try:
                batch = json.loads(result.get("text") or "").get("products") or []
            except (ValueError, AttributeError):
                logger.warning("Browser feed request for %s page %s was not JSON (HTTP %s)", collection_url, page_no, status)
                break
            logger.info("Browser: collection %s page %s returned %s products", collection_url, page_no, len(batch))
            if not batch:
                break
            products.extend(batch)
            page_no += 1

        if not products:
            hrefs = page.eval_on_selector_all("a[href*='/products/']", "els => els.map(e => e.getAttribute('href'))")
            products = [{"handle": handle} for handle in handles_from_links(hrefs)]
            logger.warning(
                "Browser feed unavailable; using the %s product links visible on the collection page "
                "(products further down an infinite-scroll page may be missing)",
                len(products),
            )
    except Exception as exc:
        logger.warning("Browser collection fallback failed for %s: %s", collection_url, exc)
    finally:
        try:
            page.close()
        except Exception:
            pass
    return products


def collect_collection_products(
    session: requests.Session,
    collection_url: str,
    logger: logging.Logger,
    browser: Optional[BrowserSession] = None,
) -> List[dict]:
    """Collection products from the JSON feed, falling back to a real browser when blocked."""
    products: List[dict] = []
    if not _STATE["http_blocked"]:
        try:
            for product in iter_collection_products(session, collection_url, logger):
                products.append(product)
            return products
        except BlockedError as exc:
            if not BROWSER_RENDER_ENABLED or browser is None:
                raise
            _STATE["http_blocked"] = True
            logger.warning("%s Switching to a real browser for the rest of the run.", exc)
    elif browser is None or not BROWSER_RENDER_ENABLED:
        raise BlockedError(f"Plain requests are blocked and the browser is disabled: {collection_url}")
    browser_products = browser_collection_products(browser, collection_url, logger)
    if not products and not browser_products:
        raise BlockedError(f"Blocked for both plain requests and the browser: {collection_url}")
    return products + browser_products


def normalize_text(tag: Tag) -> str:
    text = " ".join(part.strip() for part in tag.stripped_strings if part.strip())
    return re.sub(r"\s+", " ", text).strip()


BLOCK_TAGS = {
    "p", "div", "li", "ul", "ol", "dl", "dt", "dd", "table", "section", "article",
    "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre", "hr",
}


def has_loose_lines(tag: Tag) -> bool:
    """True when a block holds text directly (not wrapped in a child tag) or uses <br> line breaks."""
    for node in tag.children:
        if isinstance(node, NavigableString) and not isinstance(node, Comment) and node.strip():
            return True
        if isinstance(node, Tag) and node.name == "br":
            return True
    return False


def split_loose_lines(tag: Tag) -> List[str]:
    """Split a block into lines at <br> tags; block-level children (e.g. <p>) become their own line."""
    lines: List[str] = []
    buffer: List[str] = []

    def flush() -> None:
        text = re.sub(r"\s+", " ", " ".join(buffer)).strip()
        if text:
            lines.append(text)
        buffer.clear()

    for node in tag.children:
        if isinstance(node, Comment):
            continue
        if isinstance(node, NavigableString):
            buffer.append(str(node))
        elif isinstance(node, Tag):
            if node.name == "br":
                flush()
            elif node.name in BLOCK_TAGS:
                flush()
                text = normalize_text(node)
                if text:
                    lines.append(text)
            else:
                buffer.append(" ".join(node.stripped_strings))
    flush()
    return lines


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
        if nested and has_loose_lines(child):
            for nested_index, value in enumerate(split_loose_lines(child), start=1):
                result[f"Child {child_index}{child_letter(nested_index)}"] = value
        elif nested:
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
    browser: "BrowserSession",
    timeout_ms: int = BROWSER_RENDER_TIMEOUT_MS,
) -> Optional[str]:
    """Render a PDP in the shared browser, click disclosure toggles, and return DOM HTML."""
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
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
    page = browser.open_page(url)
    if page is None:
        return None
    try:
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
    except Exception as exc:
        logger.warning("Browser-render pass failed for %s: %s", url, exc)
        return None
    finally:
        try:
            page.close()
        except Exception:
            pass

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


def browser_page_html(browser: "BrowserSession", url: str, logger: logging.Logger) -> Optional[str]:
    """The page's HTML from the shared browser, with no clicking."""
    page = browser.open_page(url)
    if page is None:
        return None
    try:
        return page.content()
    except Exception as exc:
        logger.warning("Browser could not read %s: %s", url, exc)
        return None
    finally:
        try:
            page.close()
        except Exception:
            pass


def probe_pdp_rows(
    session: requests.Session,
    logger: logging.Logger,
    browser: Optional["BrowserSession"] = None,
) -> List[PDPRow]:
    seen_handles: set[str] = set()
    rows: List[PDPRow] = []
    if browser is None:
        browser = BrowserSession(logger)

    # After this many PDPs in a row are blocked over plain HTTP, stop trying
    # HTTP and go straight to the browser for the rest of the run.
    challenge_streak_limit = 3
    challenge_streak = 0

    for collection_url in COLLECTION_URL:
        try:
            products = collect_collection_products(session, collection_url, logger, browser)
        except Exception as exc:
            logger.error("Skipping collection %s: %s", collection_url, exc)
            continue
        for product in products:
            handle = (product.get("handle") or "").strip()
            if not handle or handle in seen_handles:
                continue
            seen_handles.add(handle)

            product_url = urljoin(collection_url, f"/products/{handle}")
            try:
                html_candidates: List[Tuple[str, str]] = []
                http_failed = False
                if BROWSER_RENDER_ENABLED and (_STATE["http_blocked"] or challenge_streak >= challenge_streak_limit):
                    http_failed = True
                else:
                    try:
                        response = get_with_retries(session, product_url, logger)
                        html_candidates.append(("http", response.text))
                        challenge_streak = 0
                    except BlockedError as exc:
                        http_failed = True
                        challenge_streak += 1
                        logger.warning("HTTP fetch blocked for %s; will use the browser: %s", product_url, exc)
                        if challenge_streak == challenge_streak_limit and BROWSER_RENDER_ENABLED:
                            logger.warning(
                                "%s product pages in a row were blocked; using the browser only for the rest of the run",
                                challenge_streak_limit,
                            )
                    except Exception as exc:
                        http_failed = True
                        logger.warning("HTTP fetch failed for %s; will use the browser: %s", product_url, exc)

                wants_clicks = bool(CLICK_TARGET_TEXTS) or (SET_PARENT_SELECTOR_TO_OPEN and PARENT_SELECTOR.strip())
                if BROWSER_RENDER_ENABLED and wants_clicks:
                    rendered_html = render_page_html_with_clicks(
                        product_url,
                        PARENT_SELECTOR,
                        SET_PARENT_SELECTOR_TO_OPEN,
                        CLICK_TARGET_TEXTS,
                        logger,
                        browser,
                    )
                    if rendered_html:
                        html_candidates.insert(0, ("browser_click", rendered_html))
                elif BROWSER_RENDER_ENABLED and http_failed:
                    rendered_html = browser_page_html(browser, product_url, logger)
                    if rendered_html:
                        html_candidates.insert(0, ("browser", rendered_html))

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
    browser = BrowserSession(logger)
    try:
        rows = probe_pdp_rows(session, logger, browser)
    finally:
        browser.close()
    logger.info("Collected %s PDP rows", len(rows))
    empty = [row.handle for row in rows if not row.child_values]
    if empty:
        logger.warning(
            "No selector data for %s of %s product(s): %s",
            len(empty), len(rows), ", ".join(empty[:20]) + (" ..." if len(empty) > 20 else ""),
        )
    export_to_excel(rows, logger)


if __name__ == "__main__":
    main()
