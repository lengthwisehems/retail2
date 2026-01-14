"""Token harvester for Shopify collection/PDP pages.

Given a collection URL, this script searches the collection page, linked app
scripts, and each PDP discovered on that collection for candidate GraphQL
tokens and secret-like config values. Results are written to an Excel
workbook with a token sheet (deduped) and a secrets sheet (per key discovery
with context), plus a concatenated string of all tokens for quick copy/paste.

The script follows the repo logging/output conventions: a single requests
session with retries/backoff, desktop User-Agent, Output folder for exports,
and a run log with a fallback destination when the preferred path is
unavailable.
"""

# IMPORTANT — Shopify Storefront API token formats are NOT reliable indicators
# of validity. Shopify does not publish an authoritative list of all possible
# token formats. Historically:
# - Pre-2020 tokens: 32-char hex strings.
# - Post-2020 tokens: prefixed (shpat_, shpca_, shppa_) + 32 chars.
# - Community has observed tokens like shpua_.
# Shopify DOES NOT guarantee:
#   - That prefixes remain stable.
#   - That token length is fixed.
#   - That format predicts scope permissions.
# DO NOT validate tokens with regex or length checks. Treat all secrets as opaque.

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
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
COLLECTION_URL = "https://www.aninebing.com/collections/denim-1"

LOG_PATH = BASE_DIR / "aninebing_token_probe_run.log"
EXCEL_BASENAME = "aninebing_graphql_tokens.xlsx"

# Hosts to treat as app endpoints when scanning HTML/script bodies. This lets us
# probe third-party app URLs seen in page markup without hardcoding per-brand
# secrets.
APP_HOST_KEYWORDS = [
    "aa.agkn.com",
    "ad.360yield.com",
    "ad.tpmn.co.kr",
    "ad.tpmn.io",
    "ade.clmbtech.com",
    "analytics.tiktok.com",
    "anine-bing-merch.immersiveecommerce.com",
    "aninebing-us.attn.tv",
    "api.st2.antavo.com",
    "api.yotpo.com",
    "app.consentmo.com",
    "bat.bing.com",
    "c.bing.com",
    "cdn.attn.tv",
    "cdn.heapanalytics.com",
    "cdn.jsdelivr.net",
    "cdn.resonate.com",
    "cdn.shopify.com",
    "cdn-loyalty.yotpo.com",
    "cdn-swell-assets.yotpo.com",
    "cdn-widgetsrepository.yotpo.com",
    "cm.g.doubleclick.net",
    "criteo-partners.tremorhub.com",
    "criteo-sync.teads.tv",
    "crossborder-integration.global-e.com",
    "cs.media.net",
    "dis.criteo.com",
    "dpm.demdex.net",
    "ds.reson8.com",
    "easygdpr.b-cdn.net",
    "eb2.3lift.com",
    "exchange.mediavine.com",
    "g10300385420.co",
    "gcc.metizapps.com",
    "gdprcdn.b-cdn.net",
    "gum.criteo.com",
    "he.lijit.com",
    "heapanalytics.com",
    "herochat-plugin.chatbotize.com",
    "i.liadm.com",
    "ib.adnxs.com",
    "jadserve.postrelease.com",
    "js.findmine.com",
    "lit.findmine.com",
    "live-chat.chatbotize.com",
    "login.dotomi.com",
    "maxcdn.bootstrapcdn.com",
    "otlp-http-production.shopifysvc.com",
    "p.yotpo.com",
    "pagead2.googlesyndication.com",
    "partner.medialiance.com",
    "partner.mediawallahscript.com",
    "pdimg-prod-fmv3.findmine.com",
    "ping.fastsimon.com",
    "pippio.com",
    "pixel.rubiconproject.com",
    "premcdn.swymrelay.com",
    "public-prod-dspcookiematching.dmxleo.com",
    "r.casalemedia.com",
    "redirectify.app",
    "rtb-csync.smartadserver.com",
    "s3.global-e.com",
    "script.hotjar.com",
    "sec.webeyez.com",
    "settings.fastsimon.com",
    "shop.app",
    "shopify.rakutenadvertising.io",
    "shopify-gtm-suite.getelevar.com",
    "shopify-init.blackcrow.ai",
    "simage2.pubmatic.com",
    "simage4.pubmatic.com",
    "sp.booxi.com",
    "sslwidget.criteo.com",
    "static.criteo.net",
    "static.hotjar.com",
    "static.klaviyo.com",
    "static.shopmy.us",
    "static-autocomplete.fastsimon.com",
    "static-tracking.klaviyo.com",
    "staticw2.yotpo.com",
    "swymstore-v3premium-01.swymrelay.com",
    "swymv3premium-01.azureedge.net",
    "sync.1rx.io",
    "sync.outbrain.com",
    "sync.targeting.unrulymedia.com",
    "sync-t1.taboola.com",
    "szero.narvar.com",
    "tag.rmp.rakuten.com",
    "tapestry.tapad.com",
    "tikjv.aninebing.com",
    "tr.snapchat.com",
    "track.securedvisit.com",
    "trends.revcontent.com",
    "us3-api.eng.bloomreach.com",
    "waw.chat.getzowie.com",
    "webservices.global-e.com",
    "widget.eu.criteo.com",
    "www.aninebing.com",
    "www.booxi.com",
    "www.youtube.com",
    "x.bidswitch.net",
]

# Hosts to skip fetching entirely (noise / unreachable in automation).
SKIP_HOSTS = {
    "cct.google",
    "www.google.com",
    "google.com",
    "monorail-edge-ca.shopifycloud.com",
    "monorail-edge-staging.shopifycloud.com",
    "monorail-edge.shopifysvc.com",
    "analytics.google.com",
    "www.google-analytics.com",
    "googleads.g.doubleclick.net",
    "www.googleadservices.com",
    "www.googletagmanager.com",
    "fonts.shopify.com",
    "fonts.googleapis.com",
    "fonts.gstatic.com",
    "fonts.shopifycdn.com",
    "fonts.cdnfonts.com",
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

# Extensions/content that should be treated as binary/noisy and skipped.
BINARY_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".svg",
    ".ico",
    ".bmp",
    ".tif",
    ".tiff",
    ".mp4",
    ".mov",
    ".avi",
    ".mp3",
    ".wav",
    ".woff",
    ".woff2",
    ".ttf",
    ".eot",
)

MIN_HEURISTIC_LENGTH = 32

HEX_32_PATTERN = re.compile(r"\b[0-9a-f]{32}\b")
BROAD_32_PATTERN = re.compile(r"(?<![0-9a-z])[0-9a-z]{32}(?![0-9a-z])", re.I)
PREFIXED_32_PATTERN = re.compile(r"shp(?:at|ca|pa|ua)_[0-9a-zA-Z]{32}")

# Keys that should be treated as suspicious when found inside structured
# objects (inline script JSON/config blobs). Substring, case-insensitive
# matching is applied.
KEY_CANDIDATES = [
    "accessToken",
    "storefront_access_token",
    "apiKey",
    "client_secret",
    "token",
    "authToken",
    "AUTH-TOKEN",
    "storeFrontApi",
    "X-Shopify-Storefront-Access-Token",
    "x-shopify-storefront-access-token",
    "storefrontaccesstoken",
    "storefrontAccessToken",
]

NOISY_SECRET_KEYS = {
    "shopifycheckoutapitoken",
    "checkoutapitoken",
    "shopify-checkout-api-token",
}

KNOWN_PUBLIC_TOKENS = set()

# Heuristic literal patterns that often correspond to opaque credentials.
HEURISTIC_LITERAL_PATTERNS = (HEX_32_PATTERN, PREFIXED_32_PATTERN)

visited_urls: Set[str] = set()
page_html_cache: Dict[str, str] = {}

# Type aliases for readability
SecretFinding = Dict[str, str]
TokenRecord = Dict[str, Set[str]]
TokenStore = Dict[str, TokenRecord]

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


def normalize_for_visit(url: str) -> str:
    try:
        return normalize_product_url(url)
    except Exception:
        return url


def is_binary_url(url: str) -> bool:
    lowered = url.lower()
    if lowered.startswith("blob:"):
        return True
    if any(ext in lowered for ext in BINARY_EXTENSIONS):
        return True
    try:
        path = urlparse(lowered).path
    except ValueError:
        return True
    return any(path.endswith(ext) for ext in BINARY_EXTENSIONS)


def should_skip_url(url: str) -> bool:
    if "${" in url or "}" in url:
        return True
    try:
        parsed = urlparse(url)
    except ValueError:
        return True
    host = parsed.netloc.lower()
    scheme = parsed.scheme.lower()
    if "global-e.com" in host:
        return True
    if host in SKIP_HOSTS:
        return True
    if scheme in {"blob", "data"}:
        return True
    return is_binary_url(url)


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


def fetch_text(
    session: requests.Session,
    url: str,
    logger: logging.Logger,
    *,
    respect_visited: bool = True,
) -> str:
    normalized = normalize_for_visit(url)
    if respect_visited and normalized in visited_urls:
        return ""
    if should_skip_url(normalized):
        logger.info("Skipping noisy/binary host: %s", normalized)
        if respect_visited:
            visited_urls.add(normalized)
        return ""
    if respect_visited:
        visited_urls.add(normalized)
    try:
        resp = session.get(normalized, timeout=30, verify=False)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "").lower()
        if not is_textual(normalized, content_type):
            return ""
        text = resp.text
        return text
    except requests.RequestException as exc:  # pragma: no cover - network-dependent
        logger.warning("Fetch failed %s -> %s", normalized, exc)
        return ""


def record_token(tokens: TokenStore, token: str, source_url: str, reason: str) -> None:
    if token in KNOWN_PUBLIC_TOKENS:
        return
    entry = tokens.setdefault(token, {"sources": set(), "reasons": set()})
    entry["sources"].add(source_url)
    entry["reasons"].add(reason)


def extract_literal_tokens(text: str) -> Set[str]:
    """Capture opaque credential-like strings using heuristic literal patterns."""

    hits: Set[str] = set()
    for pattern in HEURISTIC_LITERAL_PATTERNS:
        hits.update(pattern.findall(text))
    return hits


VARIABLE_ASSIGNMENT_RE = re.compile(
    r"(?:var|let|const)?\s*([A-Za-z_$][\w$]*)\s*=\s*(['\"`])([^'\"`]+)\2"
)

variable_assignments: Dict[str, Set[str]] = {}


def update_variable_assignments(text: str) -> None:
    for match in VARIABLE_ASSIGNMENT_RE.finditer(text):
        name = match.group(1)
        value = match.group(3)
        if not name or not value:
            continue
        variable_assignments.setdefault(name, set()).add(value)


def resolve_js_variable_value(text: str, variable_name: str) -> str | None:
    pattern = re.compile(
        rf"(?:var|let|const)?\s*{re.escape(variable_name)}\s*=\s*(['\"`])([^'\"`]+)\1"
    )
    match = pattern.search(text)
    if match:
        return match.group(2)
    values = variable_assignments.get(variable_name)
    if values:
        return sorted(values)[0]
    return None


def extract_key_candidate_values_from_text(
    text: str,
    blocked_values: Set[str],
) -> List[Tuple[str, str, str]]:
    hits: List[Tuple[str, str, str]] = []
    for key in KEY_CANDIDATES:
        if normalize_key(key) in NOISY_SECRET_KEYS:
            continue
        key_pattern = re.compile(
            rf"(?:['\"])?{re.escape(key)}(?:['\"])?\s*:\s*([^,}}]+)",
            re.I,
        )
        for match in key_pattern.finditer(text):
            if normalize_key(key) in NOISY_SECRET_KEYS:
                continue
            raw_value = match.group(1).strip()
            if raw_value.startswith(("'", '"')):
                value_match = re.match(r"""['"]([^'"]+)['"]""", raw_value)
                if not value_match:
                    continue
                value = value_match.group(1)
                extraction = "key_match"
            else:
                variable_name = re.sub(r"[^A-Za-z0-9_$]", "", raw_value)
                if not variable_name:
                    continue
                value = resolve_js_variable_value(text, variable_name) or ""
                extraction = "variable_extraction"
            if value and value not in blocked_values:
                hits.append((key, value, extraction))
    return hits


def extract_literal_tokens_from_scripts(html: str) -> Set[str]:
    hits: Set[str] = set()
    for blob in extract_script_blobs(html):
        if blob["text"]:
            hits.update(extract_literal_tokens(blob["text"]))
    return hits


def is_textual(url: str, content_type: str | None = None) -> bool:
    lowered = url.lower()
    if is_binary_url(lowered):
        return False
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
        candidate = urljoin(base_url, src)
        if should_skip_url(candidate):
            continue
        yield candidate


def iter_link_urls(html: str, base_url: str) -> Iterable[str]:
    """Pull stylesheet and preload links that may carry app payloads."""

    soup = BeautifulSoup(html, "html.parser")
    try:
        base_host = urlparse(base_url).netloc.lower()
    except ValueError:
        base_host = ""
    for tag in soup.find_all("link", href=True):
        rel = " ".join(tag.get("rel", [])) if tag.get("rel") else ""
        if "alternate" in rel.lower() or tag.get("hreflang"):
            continue
        candidate = urljoin(base_url, tag["href"])
        if should_skip_url(candidate):
            continue
        try:
            host = urlparse(candidate).netloc.lower()
        except ValueError:
            continue
        if host and base_host and not (
            host == base_host
            or host.endswith(f".{base_host}")
            or any(keyword in candidate for keyword in APP_HOST_KEYWORDS)
        ):
            continue
        yield candidate


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
        if should_skip_url(candidate):
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
        if should_skip_url(candidate):
            continue
        # Skip obvious binary assets to keep requests reasonable.
        if any(candidate.lower().endswith(ext) for ext in TEXT_RESOURCE_EXTENSIONS) or any(
            keyword in candidate for keyword in APP_HOST_KEYWORDS
        ):
            yield candidate


def extract_script_blobs(html: str) -> List[Dict[str, Any]]:
    """Return script contents with minimal context (index/id/type)."""

    soup = BeautifulSoup(html, "html.parser")
    blobs: List[Dict[str, Any]] = []
    for idx, tag in enumerate(soup.find_all("script")):
        attrs = {k: (" ".join(v) if isinstance(v, list) else str(v)) for k, v in tag.attrs.items()}
        text = tag.string if tag.string is not None else tag.decode_contents()
        blobs.append(
            {
                "index": idx,
                "attrs": attrs,
                "text": text or "",
                "type": attrs.get("type", ""),
            }
        )
    return blobs


def extract_candidate_json_strings(script_text: str, is_json_type: bool) -> List[str]:
    """Find JSON-like substrings inside a script body.

    Priority order:
    - Entire body when the script tag advertises JSON (e.g., application/json, ld+json).
    - Inline assignments like `window.__xyz = {...};` captured via a regex.
    - Fallback to the full body when it starts with a JSON object/array.
    """

    candidates: List[str] = []
    stripped = script_text.strip()
    if not stripped:
        return candidates

    if is_json_type:
        candidates.append(stripped)
        return candidates

    # Assignments to window/global objects
    assign_pattern_obj = re.compile(r"=\s*({.*?})\s*;", re.S)
    assign_pattern_array = re.compile(r"=\s*(\[.*?\])\s*;", re.S)
    for pattern in (assign_pattern_obj, assign_pattern_array):
        for match in pattern.finditer(script_text):
            candidates.append(match.group(1).strip())

    # Walk the script text to locate any JSON-like payloads using raw_decode.
    decoder = json.JSONDecoder()
    idx = 0
    while idx < len(script_text):
        ch = script_text[idx]
        if ch in "[{":
            try:
                _, end = decoder.raw_decode(script_text, idx)
                candidates.append(script_text[idx:end].strip())
                idx = end
                continue
            except Exception:
                pass
        idx += 1

    # Fallback: if the whole body looks like JSON, try it too.
    if stripped.startswith("{") or stripped.startswith("["):
        candidates.append(stripped)

    return candidates


def safe_json_loads(blob: str) -> Any | None:
    try:
        return json.loads(blob)
    except Exception:
        return None


def normalize_key(key: str) -> str:
    return re.sub(r"[^a-z0-9]", "", key.lower())


def extract_noisy_meta_tokens(html: str) -> Set[str]:
    soup = BeautifulSoup(html, "html.parser")
    noisy_values: Set[str] = set()
    for tag in soup.find_all("meta"):
        name = tag.get("name") or tag.get("id") or tag.get("property") or ""
        if name and normalize_key(name) in NOISY_SECRET_KEYS:
            content = tag.get("content")
            if content:
                noisy_values.add(content)
    return noisy_values


def collect_noisy_values(obj: Any) -> Set[str]:
    noisy_values: Set[str] = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if normalize_key(k) in NOISY_SECRET_KEYS and isinstance(v, str):
                noisy_values.add(v)
            noisy_values.update(collect_noisy_values(v))
    elif isinstance(obj, list):
        for item in obj:
            noisy_values.update(collect_noisy_values(item))
    return noisy_values


def find_candidate_keys(
    obj: Any,
    key_candidates: Sequence[str],
    path: str = "",
    blocked_values: Set[str] | None = None,
) -> List[Tuple[str, str, Any]]:
    """Recursively search for keys that include any candidate token."""

    hits: List[Tuple[str, str, Any]] = []
    lower_candidates = [kc.lower() for kc in key_candidates]

    if isinstance(obj, dict):
        for k, v in obj.items():
            new_path = f"{path}.{k}" if path else k
            normalized_key = normalize_key(k)
            if normalized_key in NOISY_SECRET_KEYS:
                continue
            if blocked_values and isinstance(v, str) and v in blocked_values:
                continue
            if any(candidate in k.lower() for candidate in lower_candidates):
                hits.append((new_path, k, v))
            hits.extend(find_candidate_keys(v, key_candidates, new_path, blocked_values))
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            new_path = f"{path}[{idx}]" if path else f"[{idx}]"
            hits.extend(find_candidate_keys(item, key_candidates, new_path, blocked_values))

    return hits

# ---------------------------------------------------------------------------
# Token gathering
# ---------------------------------------------------------------------------


def process_text_blob(
    source_url: str,
    text: str,
    token_sources: TokenStore,
    logger: logging.Logger,
    blocked_values: Set[str] | None = None,
) -> Tuple[str, Set[str], int]:
    normalized = text.replace("\\/", "/")
    lower_url = source_url.lower()
    blocked_values = blocked_values or set()
    update_variable_assignments(normalized)
    hex_hits = HEX_32_PATTERN.findall(normalized)
    broad_hits = BROAD_32_PATTERN.findall(normalized)
    prefixed_hits = PREFIXED_32_PATTERN.findall(normalized)
    hex_hits += HEX_32_PATTERN.findall(source_url)
    broad_hits += BROAD_32_PATTERN.findall(source_url)
    prefixed_hits += PREFIXED_32_PATTERN.findall(source_url)
    regex_hits = set(hex_hits + broad_hits + prefixed_hits)
    for tok in regex_hits:
        record_token(token_sources, tok, source_url, "regex_match")
    tokens: Set[str] = set()
    key_value_hits = extract_key_candidate_values_from_text(normalized, blocked_values)
    for _, token_value, extraction in key_value_hits:
        record_token(token_sources, token_value, source_url, extraction)
        if extraction == "variable_extraction":
            logger.info("Resolved variable token from %s", source_url)
    if lower_url.endswith((".js", ".mjs", ".json")):
        tokens = extract_literal_tokens(normalized)
    elif "<script" in normalized.lower():
        tokens = extract_literal_tokens_from_scripts(normalized)
    regex_count = len(hex_hits) + len(broad_hits) + len(prefixed_hits)
    for tok in tokens:
        record_token(token_sources, tok, source_url, "heuristic_literal")
    return normalized, tokens, regex_count


def inspect_scripts_for_secrets(
    html: str,
    page_url: str,
    findings: List[SecretFinding],
    logger: logging.Logger,
    token_sources: TokenStore,
    blocked_values: Set[str] | None = None,
) -> None:
    """Parse script tags and extract suspicious keys from JSON/config blobs."""

    blocked_values = blocked_values or set()
    for blob in extract_script_blobs(html):
        is_json_type = "json" in blob["type"].lower()
        if blob["text"]:
            update_variable_assignments(blob["text"])
            key_value_hits = extract_key_candidate_values_from_text(
                blob["text"], blocked_values
            )
            for key_name, token_value, extraction in key_value_hits:
                record_token(
                    token_sources, token_value, page_url, extraction
                )
                findings.append(
                    {
                        "page_url": page_url,
                        "key_name": key_name,
                        "key_path": "",
                        "value": token_value,
                        "source_type": "script_text",
                        "script_index": str(blob["index"]),
                        "script_id": blob["attrs"].get("id", ""),
                        "script_type": blob["type"],
                        "extracted_by": extraction,
                    }
                )
        candidates = extract_candidate_json_strings(blob["text"], is_json_type)
        for candidate in candidates:
            obj = safe_json_loads(candidate)
            if obj is None:
                continue
            noisy_values = collect_noisy_values(obj)
            combined_blocked = blocked_values | noisy_values
            hits = find_candidate_keys(
                obj,
                KEY_CANDIDATES,
                blocked_values=combined_blocked,
            )
            for key_path, key_name, value in hits:
                value_text = json.dumps(value, ensure_ascii=False)
                token_value = value if isinstance(value, str) else value_text
                if token_value and token_value not in combined_blocked:
                    record_token(
                        token_sources, str(token_value), page_url, "key_match"
                    )
                findings.append(
                    {
                        "page_url": page_url,
                        "key_name": key_name,
                        "key_path": key_path,
                        "value": value_text,
                        "source_type": "inline_script_json"
                        if is_json_type
                        else "config_blob",
                        "script_index": str(blob["index"]),
                        "script_id": blob["attrs"].get("id", ""),
                        "script_type": blob["type"],
                        "extracted_by": "key_match",
                    }
                )


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
    response_seen: Set[str] = set()
    request_seen: Set[str] = set()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(ignore_https_errors=True)

            def handle_response(response):
                resp_url = normalize_for_visit(response.url)
                if resp_url in response_seen or should_skip_url(resp_url):
                    return
                response_seen.add(resp_url)
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
                req_url = normalize_for_visit(request.url)
                if req_url in request_seen or should_skip_url(req_url):
                    return
                request_seen.add(req_url)
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
    token_sources: TokenStore,
    errors: List[str],
    findings: List[SecretFinding],
) -> Set[str]:
    tokens: Set[str] = set()
    regex_total = 0

    normalized_url = normalize_for_visit(url)
    if normalized_url in visited_urls:
        logger.info("Skipping already visited: %s", normalized_url)
        return tokens
    if should_skip_url(normalized_url):
        logger.info("Skipping disallowed URL: %s", normalized_url)
        visited_urls.add(normalized_url)
        return tokens
    visited_urls.add(normalized_url)

    html, responses, pending_urls, request_urls = crawl_with_playwright(
        normalized_url, logger, errors
    )
    if not html:
        html = fetch_text(session, normalized_url, logger, respect_visited=False)
    if html:
        page_html_cache[normalized_url] = html
        blocked_values = extract_noisy_meta_tokens(html)
        normalized_html, token_hits, regex_count = process_text_blob(
            normalized_url, html, token_sources, logger, blocked_values
        )
        tokens.update(token_hits)
        regex_total += regex_count
        inspect_scripts_for_secrets(
            normalized_html,
            normalized_url,
            findings,
            logger,
            token_sources,
            blocked_values,
        )
    else:
        return tokens

    candidate_urls = (
        list(iter_script_urls(normalized_html, url))
        + list(iter_link_urls(normalized_html, url))
        + list(iter_app_urls(normalized_html, url))
        + list(iter_network_urls(normalized_html, url))
    )

    # Process responses captured during Playwright navigation first.
    processed_responses: Set[str] = set()
    for resp_url, body in responses:
        if resp_url in processed_responses or should_skip_url(resp_url):
            continue
        processed_responses.add(resp_url)
        visited_urls.add(resp_url)
        normalized_body, token_hits, regex_count = process_text_blob(
            resp_url, body, token_sources, logger
        )
        tokens.update(token_hits)
        regex_total += regex_count
        candidate_urls.extend(
            list(iter_app_urls(normalized_body, resp_url))
            + list(iter_network_urls(normalized_body, resp_url))
        )

    # Fetch any response URLs that failed to provide a body via Playwright.
    for pending in pending_urls:
        if pending in visited_urls or should_skip_url(pending):
            continue
        body = fetch_text(session, pending, logger)
        if not body:
            continue
        normalized_body, token_hits, regex_count = process_text_blob(
            pending, body, token_sources, logger
        )
        tokens.update(token_hits)
        regex_total += regex_count
        candidate_urls.extend(
            list(iter_app_urls(normalized_body, pending))
            + list(iter_network_urls(normalized_body, pending))
        )

    # Fetch textual/keyword URLs captured from requests (without responses).
    for req_url in request_urls:
        if req_url in visited_urls or should_skip_url(req_url):
            continue
        body = fetch_text(session, req_url, logger)
        if not body:
            continue
        normalized_body, token_hits, regex_count = process_text_blob(
            req_url, body, token_sources, logger
        )
        tokens.update(token_hits)
        regex_total += regex_count
        candidate_urls.extend(
            list(iter_app_urls(normalized_body, req_url))
            + list(iter_network_urls(normalized_body, req_url))
        )

    # Fetch any additional discovered URLs not yet seen.
    for candidate in candidate_urls:
        if candidate in visited_urls or should_skip_url(candidate):
            continue
        body = fetch_text(session, candidate, logger)
        if not body:
            continue
        normalized_body, token_hits, regex_count = process_text_blob(
            candidate, body, token_sources, logger
        )
        tokens.update(token_hits)
        regex_total += regex_count
    if regex_total:
        logger.info("%s tokens found in %s", regex_total, normalized_url)
    return tokens


def gather_tokens(
    session: requests.Session,
    collection_url: str,
    logger: logging.Logger,
    token_store: TokenStore | None = None,
    secrets_store: List[SecretFinding] | None = None,
) -> Tuple[TokenStore, List[SecretFinding]]:
    all_tokens: TokenStore = token_store if token_store is not None else {}
    secrets: List[SecretFinding] = secrets_store if secrets_store is not None else []
    errors: List[str] = []

    logger.info("Fetching collection page: %s", collection_url)
    collection_tokens = gather_tokens_from_page(
        session, collection_url, logger, all_tokens, errors, secrets
    )
    logger.info(
        "%s tokens collected from collection page and scripts", len(collection_tokens)
    )

    collection_html = page_html_cache.get(normalize_for_visit(collection_url))
    if not collection_html:
        collection_html = fetch_text(session, collection_url, logger)
    handles = extract_handles(collection_html, collection_url)
    logger.info("Discovered %s product handles", len(handles))

    parsed = urlparse(collection_url)
    base = f"{parsed.scheme}://{parsed.netloc}"

    for handle in handles:
        pdp_url = f"{base}/products/{handle}"
        logger.info("Processing PDP: %s", pdp_url)
        gather_tokens_from_page(
            session, pdp_url, logger, all_tokens, errors, secrets
        )

    if errors:
        logger.info("Token probe completed with %s errors", len(errors))
        for err in errors:
            logger.info("Error: %s", err)

    return all_tokens, secrets


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def write_excel(
    tokens: TokenStore,
    secrets: List[SecretFinding],
    output_path: Path,
    logger: logging.Logger,
) -> None:
    wb = Workbook()
    wb.remove(wb.active)

    def write_token_sheet(title: str, tokens_to_write: List[str]) -> None:
        ws = wb.create_sheet(title)
        ws.append(["token", "sources", "reasons", "all_tokens_concatenated"])
        joined = ", ".join(f'"{tok}"' for tok in tokens_to_write)
        first = True
        for tok in tokens_to_write:
            record = tokens.get(tok, {})
            sources = "; ".join(sorted(record.get("sources", [])))
            reasons = "; ".join(sorted(record.get("reasons", [])))
            if first:
                ws.append([tok, sources, reasons, joined])
                first = False
            else:
                ws.append([tok, sources, reasons, ""])

    sorted_tokens = sorted(tokens.keys())
    hex_tokens = [tok for tok in sorted_tokens if HEX_32_PATTERN.search(tok)]
    broad_tokens = [tok for tok in sorted_tokens if BROAD_32_PATTERN.search(tok)]
    prefixed_tokens = [
        tok for tok in sorted_tokens if PREFIXED_32_PATTERN.search(tok)
    ]
    key_candidate_tokens = [
        tok
        for tok in sorted_tokens
        if tokens.get(tok, {}).get("reasons", set())
        & {"key_match", "variable_extraction"}
    ]
    heuristic_tokens = [
        tok
        for tok in sorted_tokens
        if "heuristic_literal" in tokens.get(tok, {}).get("reasons", set())
    ]

    write_token_sheet("32 character", hex_tokens)
    write_token_sheet("32 character - broad", broad_tokens)
    write_token_sheet("Pre-Fix", prefixed_tokens)
    write_token_sheet("Key Candidates", key_candidate_tokens)
    write_token_sheet("heuristic_literal", heuristic_tokens)

    secrets_ws = wb.create_sheet("Secrets")
    secrets_ws.append(
        [
            "page_url",
            "key_name",
            "key_path",
            "value",
            "source_type",
            "script_index",
            "script_id",
            "script_type",
            "extracted_by",
        ]
    )
    for finding in secrets:
        secrets_ws.append(
            [
                finding.get("page_url", ""),
                finding.get("key_name", ""),
                finding.get("key_path", ""),
                finding.get("value", ""),
                finding.get("source_type", ""),
                finding.get("script_index", ""),
                finding.get("script_id", ""),
                finding.get("script_type", ""),
                finding.get("extracted_by", ""),
            ]
        )
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

    token_sources: TokenStore = {}
    secrets: List[SecretFinding] = []
    try:
        token_sources, secrets = gather_tokens(
            session, COLLECTION_URL, logger, token_sources, secrets
        )
    except KeyboardInterrupt:
        logger.warning("Interrupted; writing partial results before exit.")
        excel_path = OUTPUT_DIR / EXCEL_BASENAME
        write_excel(token_sources, secrets, excel_path, logger)
        return
    logger.info("Total unique tokens: %s", len(token_sources))
    logger.info("Total secret key hits: %s", len(secrets))

    excel_path = OUTPUT_DIR / EXCEL_BASENAME
    write_excel(token_sources, secrets, excel_path, logger)


if __name__ == "__main__":
    main()
