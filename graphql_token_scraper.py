"""Token harvester for Shopify collection/PDP pages.

Given a collection URL, this script searches the collection page, linked app
scripts, and each PDP discovered on that collection for candidate GraphQL
tokens (32-character hex strings). Results are written to an Excel workbook
with one sheet containing the deduped tokens and a concatenated string of all
tokens for quick copy/paste.

The script follows the repo logging/output conventions: a single requests
session with retries/backoff, desktop User-Agent, Output folder for exports,
and a run log with a fallback destination when the preferred path is
unavailable.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from openpyxl import Workbook
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from urllib3.exceptions import InsecureRequestWarning


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Set this to the target collection URL before running.
COLLECTION_URL = "https://www.ebdenim.com/collections/pants"

LOG_PATH = BASE_DIR / "token_probe_run.log"
EXCEL_BASENAME = "graphql_tokens.xlsx"

# Hosts to treat as app endpoints when scanning HTML/script bodies. This lets us
# probe third-party app URLs seen in page markup without hardcoding per-brand
# secrets.
APP_HOST_KEYWORDS = [
    "rebuyengine.com",
    "cached.rebuyengine.com",
    "cdn.rebuyengine.com",
    "avada.io",
    "avada.app",
    "hengam.io",
    "nice-team.net",
    "postscript.io",
    "shopifycloud.com",
    "shopifycdn.net",
]

# Hosts to skip fetching entirely (noise / unreachable in automation).
SKIP_HOSTS = {
    "cct.google",
    "fonts.shopify.com",
    "monorail-edge-ca.shopifycloud.com",
    "monorail-edge-staging.shopifycloud.com",
    "monorail-edge.shopifysvc.com",
    "analytics.google.com",
    "www.google-analytics.com",
    "www.merchant-center-analytics.goog",
}

# Resource extensions that commonly carry text payloads with tokens. Images are
# skipped to reduce noise.
TEXT_RESOURCE_EXTENSIONS = (
    ".js",
    ".json",
    ".txt",
    ".html",
    ".htm",
    ".css",
)

# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------


def build_session() -> requests.Session:
    """Create a session with retries, UA, and relaxed SSL for noisy hosts."""

    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        }
    )
    # Disable SSL verification noise for environments with custom MITM proxies.
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # type: ignore[attr-defined]
    return session


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------


def configure_logger() -> logging.Logger:
    logger = logging.getLogger("token_probe")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_path = LOG_PATH
    file_handler: logging.Handler | None = None
    try:
        file_handler = logging.FileHandler(file_path, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError:
        fallback = OUTPUT_DIR / "token_probe_run.log"
        logger.warning("Primary log path unavailable, using fallback: %s", fallback)
        file_handler = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------


def should_skip_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host in SKIP_HOSTS


def normalize_product_url(url: str) -> str:
    """Drop variant query parameters to avoid redundant PDP hits.

    If an unexpected URL format (e.g., malformed IPv6 host) is encountered,
    return the original string instead of raising to keep the crawl moving.
    """

    try:
        parsed = urlparse(url)
    except ValueError:
        return url
    if "/products/" in parsed.path and "variant=" in parsed.query:
        return parsed._replace(query="", fragment="").geturl()
    return url


def fetch_text(session: requests.Session, url: str, logger: logging.Logger) -> str:
    if should_skip_url(url):
        logger.info("Skipping noisy host: %s", url)
        return ""
    try:
        resp = session.get(url, timeout=30, verify=False)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "").lower()
        if not is_textual(url, content_type):
            return ""
        return resp.text
    except requests.RequestException as exc:  # pragma: no cover - network-dependent
        logger.warning("Fetch failed %s -> %s", url, exc)
        return ""


def extract_tokens(text: str) -> Set[str]:
    return set(re.findall(r"\b[0-9a-fA-F]{32}\b", text))


def is_textual(url: str, content_type: str | None = None) -> bool:
    lowered = url.lower()
    if any(lowered.endswith(ext) for ext in TEXT_RESOURCE_EXTENSIONS):
        return True
    if content_type:
        ctype = content_type.lower()
        return any(token in ctype for token in ["text", "json", "javascript", "xml"])
    return False


def extract_handles(html: str, base_url: str) -> List[str]:
    host = urlparse(base_url).netloc
    pattern = re.compile(r"/products/([a-zA-Z0-9-]+)")
    handles = set(pattern.findall(html))
    # Also look for canonical links that might include the full URL
    full_pattern = re.compile(r"https?://[^\\s\"']+/products/([a-zA-Z0-9-]+)")
    handles.update(full_pattern.findall(html))
    deduped = []
    seen = set()
    for handle in handles:
        if handle not in seen:
            seen.add(handle)
            deduped.append(handle)
    return deduped


def iter_script_urls(html: str, base_url: str) -> Iterable[str]:
    src_pattern = re.compile(r"<script[^>]+src=\"([^\"]+)\"", re.I)
    for match in src_pattern.finditer(html):
        src = match.group(1)
        yield urljoin(base_url, src)


def iter_link_urls(html: str, base_url: str) -> Iterable[str]:
    """Pull stylesheet and preload links that may carry app payloads."""

    href_pattern = re.compile(r"<link[^>]+href=\"([^\"]+)\"", re.I)
    for match in href_pattern.finditer(html):
        href = match.group(1)
        yield urljoin(base_url, href)


def iter_app_urls(text: str, base_url: str) -> Iterable[str]:
    """Extract app-related URLs from arbitrary text using keyword matches."""

    normalized = text.replace("\\/", "/")
    url_pattern = re.compile(r"https?://[^\s\"']+", re.I)
    for match in url_pattern.finditer(normalized):
        raw = match.group(0)
        try:
            candidate = normalize_product_url(raw)
        except ValueError:
            continue
        if any(keyword in candidate for keyword in APP_HOST_KEYWORDS):
            yield candidate


def iter_network_urls(text: str, base_url: str) -> Iterable[str]:
    """Broader URL gatherer to mimic DevTools network harvesting."""

    normalized = text.replace("\\/", "/")
    url_pattern = re.compile(r"https?://[^\s\"']+", re.I)
    for match in url_pattern.finditer(normalized):
        raw = match.group(0)
        try:
            candidate = normalize_product_url(raw)
        except ValueError:
            continue
        # Skip obvious binary assets to keep requests reasonable.
        if any(candidate.lower().endswith(ext) for ext in TEXT_RESOURCE_EXTENSIONS) or any(
            keyword in candidate for keyword in APP_HOST_KEYWORDS
        ):
            yield candidate


# ---------------------------------------------------------------------------
# Token gathering
# ---------------------------------------------------------------------------


def process_text_blob(
    source_url: str,
    text: str,
    token_sources: Dict[str, Set[str]],
    logger: logging.Logger,
) -> str:
    normalized = text.replace("\\/", "/")
    tokens = extract_tokens(normalized)
    if tokens:
        logger.info("%s tokens found in %s", len(tokens), source_url)
    for tok in tokens:
        token_sources.setdefault(tok, set()).add(source_url)
    return normalized


def crawl_with_playwright(
    url: str,
    logger: logging.Logger,
    errors: List[str],
) -> Tuple[str, List[Tuple[str, str]], List[str], List[str]]:
    """Load a page in Playwright, capture HTML and text-based responses."""

    html = ""
    bodies: List[Tuple[str, str]] = []
    pending_urls: List[str] = []
    request_urls: List[str] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(ignore_https_errors=True)

            def handle_response(response):
                resp_url = normalize_product_url(response.url)
                if should_skip_url(resp_url):
                    logger.info("Skipping noisy host: %s", resp_url)
                    return
                try:
                    content_type = response.headers.get("content-type")
                except Exception:
                    content_type = None

                textual = is_textual(resp_url, content_type)
                keyword_match = any(k in resp_url for k in APP_HOST_KEYWORDS)
                if not (textual or keyword_match):
                    return
                try:
                    body_text = response.text()
                except Exception as exc:
                    errors.append(f"{resp_url} -> {exc}")
                    logger.warning("Failed to read response %s -> %s", resp_url, exc)
                    return
                if body_text:
                    bodies.append((resp_url, body_text))

            def handle_request(request):
                req_url = normalize_product_url(request.url)
                if should_skip_url(req_url):
                    return
                try:
                    accept_header = request.headers.get("accept")
                except Exception:
                    accept_header = None
                textual = is_textual(req_url, accept_header)
                keyword_match = any(k in req_url for k in APP_HOST_KEYWORDS)
                if textual or keyword_match:
                    request_urls.append(req_url)

            page.on("response", handle_response)
            page.on("request", handle_request)
            logger.info("Navigating with Playwright: %s", url)
            page.goto(url, wait_until="load", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except PlaywrightTimeoutError:
                logger.info("Networkidle wait timed out; continuing with captured responses")
            page.wait_for_timeout(5000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            page.wait_for_timeout(3000)
            html = page.content()
            page.close()
            browser.close()
    except PlaywrightTimeoutError as exc:  # pragma: no cover - network/browser dependent
        errors.append(f"Playwright timeout {url} -> {exc}")
        logger.warning("Playwright timeout %s -> %s", url, exc)
    except Exception as exc:  # pragma: no cover - runtime env dependent
        errors.append(f"Playwright failure {url} -> {exc}")
        logger.warning("Playwright failure %s -> %s", url, exc)
    return html, bodies, pending_urls, request_urls


def gather_tokens_from_page(
    session: requests.Session,
    url: str,
    logger: logging.Logger,
    seen_urls: Set[str],
    token_sources: Dict[str, Set[str]],
    errors: List[str],
) -> Set[str]:
    tokens: Set[str] = set()

    html, responses, pending_urls, request_urls = crawl_with_playwright(
        url, logger, errors
    )
    if not html:
        html = fetch_text(session, url, logger)
    if html:
        normalized_html = process_text_blob(url, html, token_sources, logger)
        tokens.update(extract_tokens(normalized_html))
    else:
        return tokens

    candidate_urls = (
        list(iter_script_urls(normalized_html, url))
        + list(iter_link_urls(normalized_html, url))
        + list(iter_app_urls(normalized_html, url))
        + list(iter_network_urls(normalized_html, url))
    )

    # Process responses captured during Playwright navigation first.
    for resp_url, body in responses:
        if resp_url in seen_urls:
            continue
        seen_urls.add(resp_url)
        normalized_body = process_text_blob(resp_url, body, token_sources, logger)
        tokens.update(extract_tokens(normalized_body))
        candidate_urls.extend(
            list(iter_app_urls(normalized_body, resp_url))
            + list(iter_network_urls(normalized_body, resp_url))
        )

    # Fetch any response URLs that failed to provide a body via Playwright.
    for pending in pending_urls:
        if pending in seen_urls:
            continue
        seen_urls.add(pending)
        body = fetch_text(session, pending, logger)
        if not body:
            continue
        normalized_body = process_text_blob(pending, body, token_sources, logger)
        tokens.update(extract_tokens(normalized_body))
        candidate_urls.extend(
            list(iter_app_urls(normalized_body, pending))
            + list(iter_network_urls(normalized_body, pending))
        )

    # Fetch textual/keyword URLs captured from requests (without responses).
    for req_url in request_urls:
        if req_url in seen_urls:
            continue
        seen_urls.add(req_url)
        body = fetch_text(session, req_url, logger)
        if not body:
            continue
        normalized_body = process_text_blob(req_url, body, token_sources, logger)
        tokens.update(extract_tokens(normalized_body))
        candidate_urls.extend(
            list(iter_app_urls(normalized_body, req_url))
            + list(iter_network_urls(normalized_body, req_url))
        )

    # Fetch any additional discovered URLs not yet seen.
    for candidate in candidate_urls:
        if candidate in seen_urls:
            continue
        seen_urls.add(candidate)
        body = fetch_text(session, candidate, logger)
        if not body:
            continue
        normalized_body = process_text_blob(candidate, body, token_sources, logger)
        tokens.update(extract_tokens(normalized_body))
    return tokens


def gather_tokens(
    session: requests.Session, collection_url: str, logger: logging.Logger
) -> Dict[str, Set[str]]:
    all_tokens: Dict[str, Set[str]] = {}
    seen_urls: Set[str] = set()
    errors: List[str] = []

    logger.info("Fetching collection page: %s", collection_url)
    collection_tokens = gather_tokens_from_page(
        session, collection_url, logger, seen_urls, all_tokens, errors
    )
    logger.info(
        "%s tokens collected from collection page and scripts", len(collection_tokens)
    )

    collection_html = fetch_text(session, collection_url, logger)
    handles = extract_handles(collection_html, collection_url)
    logger.info("Discovered %s product handles", len(handles))

    parsed = urlparse(collection_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    for handle in handles:
        pdp_url = f"{base}/products/{handle}"
        logger.info("Processing PDP: %s", pdp_url)
        gather_tokens_from_page(
            session, pdp_url, logger, seen_urls, all_tokens, errors
        )

    if errors:
        logger.info("Token probe completed with %s errors", len(errors))
        for err in errors:
            logger.info("Error: %s", err)

    return all_tokens


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def write_excel(tokens: Dict[str, Set[str]], output_path: Path, logger: logging.Logger) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Tokens"
    ws.append(["token", "sources", "all_tokens_concatenated"])

    sorted_tokens = sorted(tokens.keys())
    joined = ", ".join(f'"{tok}"' for tok in sorted_tokens)
    first = True
    for tok in sorted_tokens:
        sources = "; ".join(sorted(tokens.get(tok, [])))
        if first:
            ws.append([tok, sources, joined])
            first = False
        else:
            ws.append([tok, sources, ""])
    try:
        wb.save(output_path)
        logger.info("Workbook written: %s", output_path.resolve())
    except OSError as exc:  # pragma: no cover - filesystem dependent
        fallback = OUTPUT_DIR / f"graphql_tokens_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"
        logger.warning(
            "Primary workbook path unavailable (%s). Saving to fallback: %s",
            exc,
            fallback,
        )
        wb.save(fallback)
        logger.info("Workbook written: %s", fallback.resolve())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    logger = configure_logger()
    session = build_session()

    token_sources = gather_tokens(session, COLLECTION_URL, logger)
    logger.info("Total unique tokens: %s", len(token_sources))

    excel_path = OUTPUT_DIR / EXCEL_BASENAME
    write_excel(token_sources, excel_path, logger)


if __name__ == "__main__":
    main()
