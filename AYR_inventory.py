"""AYR denim inventory scraper.

Run from Windows Command Prompt with one command (paste the file path):

    python "C:\\path\\to\\AYR_inventory.py"              -> normal run, writes the CSV
    python "C:\\path\\to\\AYR_inventory.py" --dry-run    -> does everything, prints a
                                                           preview, writes NO CSV

Every log line starts with a US Central timestamp. The product-page step shows
progress and an ETA.
"""
import argparse
import csv
import json
import logging
import re
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup, Comment, FeatureNotFound, NavigableString, Tag
from urllib3.exceptions import InsecureRequestWarning
import warnings

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND = "AYR"
LOG_FILE = BASE_DIR / f"{BRAND.lower()}_run.log"
CENTRAL_TZ = ZoneInfo("America/Chicago")

STORE_HOST = "https://ayr-production.myshopify.com"
# Product pages and product .json. ayr.com and ayr-production.myshopify.com both
# redirect here, so they share one rate limiter: trying another address after a
# block does not help (same finding as Good American).
SITE_HOST = "https://www.ayr.com"
GRAPHQL_ENDPOINTS = [f"{STORE_HOST}/api/unstable/graphql.json"]
GRAPHQL_TOKEN = "32431e71f8b832fcb465240380b72ecc"
COLLECTION_HANDLES = ["jeans-womenswear", "final-sale-womenswear"]

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
# Which headers to send. False = simple headers (default). retail_PDP_probe.py
# found on AYR that a full fake set of Chrome headers drew a hard 403 where
# simple ones drew a softer 429. True = the full Chrome set Good American sends.
USE_FULL_BROWSER_HEADERS = False
SIMPLE_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"}
BROWSER_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Chromium";v="120", "Not(A:Brand";v="24", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}
HEADERS = BROWSER_HEADERS if USE_FULL_BROWSER_HEADERS else SIMPLE_HEADERS

# Product-page pacing - the Good American method (Goodamerican_inventory.py).
# A 429 from AYR is Cloudflare, and waiting does not clear it: honouring the
# server's 60s Retry-After five times is what stalled the 11/16 run for 5 minutes
# on the-bud. So every request waits its turn at a shared pace; a block slows the
# pace, clean responses speed it back up; a wait is capped; a page gets at most
# 2 tries in the main pass and one more at the end of the run.
PDP_MIN_INTERVAL = 0.5      # floor: seconds between any two /products/ requests
PDP_MAX_INTERVAL = 6.0      # ceiling once blocks have forced the pace down
PDP_INTERVAL_STEP = 0.75    # added to the interval after each block
PDP_INTERVAL_DECAY = 0.9    # multiplier applied after each clean response
PDP_BLOCK_MAX_WAIT = 15.0   # cap: a challenge outlasts any wait worth making
PDP_BLOCK_SLEEP = 5.0       # wait when the server gives no Retry-After
PDP_GETS_PER_TRY = 2        # HTTP GETs per try
PDP_TRIES_PER_PAGE = 2      # tries per page across the whole run
BLOCK_STATUSES = {403, 429}
TRANSIENT_STATUSES = {500, 502, 503, 504}

# A product is dropped when its description (GraphQL text OR the product-page
# description) contains any of these.
BANNED_DESC_TERMS = ["skirt", "shorts", "jacket"]
BANNED_TAG_TERMS = ["skirt", "shorts"]
# A product is dropped when it carries any of these tags (whole tag, any case).
EXCLUDED_TAGS = [
    "menswear", "tops", "Tops - Men", "tee promo", "tee-bundle-new", "sweats",
    "sweats-men", "Sweaters", "Woven Shirts", "ss tees", "Jackets", "Jackets - Men",
    "Woven Shirts - Men", "cashmere", "sweatsuit", "Outerwear", "Knits", "Knits - Men",
    "Shorts", "coat", "polo", "sweat pants", "sweatpant", "sweatpants", "sweatshirt",
    "tees", "rugby", "tank", "accessories", "belt", "Skirts", "suit",
    "gender-neutral", "raglan", "Dresses", "bomber", "hoodie", "Blazers", "scoopneck",
    "striped T's", "cardigan", "vest", "dress", "turtleneck", "bandana", "scarf",
    "beanie", "hats", "tie",
]
EXCLUDED_TAGS_LOWER = {t.strip().lower() for t in EXCLUDED_TAGS}
TARGET_PRODUCT_TYPE = "Denim"

# Product-page "details" section (Product Description / Sizing / Material / Care /
# Made In). Same selector as retail_PDP_probe.py; the template number can change,
# so an id-suffix fallback is tried too.
PDP_DETAILS_SELECTORS = [
    "#shopify-section-template--20264212136015__product-details > section",
    "[id$='__product-details'] > section",
]

FLOAT_REGEX = re.compile(r"([-+]?[0-9]*\.?[0-9]+(?:\s+\d+/\d+)?)")
FRACTION_REGEX = re.compile(r"(?P<int>\d+)?\s*(?P<num>\d+)/(\d+)")
COLOR_SNIPPET_REGEX = re.compile(r"\.\s+")

SIZE_LIST = {
    s.upper() for s in [
        "00", "0", "2", "4", "6", "8", "10", "12", "14", "15 Plus", "16 Plus", "18 Plus",
        "20 Plus", "22 Plus", "24 Plus", "26 Plus", "28 Plus", "30 Plus", "32 Plus",
        "XS", "S", "M", "L", "XL", "XXL", "1XL", "2XL", "3XL", "4XL", "5XL", "00-4",
        "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus",
    ] + [str(n) for n in range(22, 41)]
}
INSEAM_LETTERS = {"S": "Petite", "R": "Regular", "L": "Long"}
DESCRIPTION_HEADINGS_TO_SKIP = {"product description", "sizing"}
LENGTH_WORDS = ("petite", "long", "regular")


@dataclass
class VariantRow:
    product_id: str
    handle: str
    product_title: str
    color: str
    size: str
    inseam_code: str
    variant_id: str
    sku: str
    barcode: str
    available: bool
    price: str
    compare_at: str
    image_url: Optional[str]
    online_url: Optional[str]
    description: str
    tags: List[str]
    vendor: str
    product_type: str
    seo_title: Optional[str]
    variant_title: str = ""
    updated_at: str = ""
    option_values: List[str] = field(default_factory=list)


# --------------------------------------------------------------------------
# Logging (Central-time timestamps)
# --------------------------------------------------------------------------
class CentralFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):  # noqa: N802 (logging API name)
        dt = datetime.fromtimestamp(record.created, tz=CENTRAL_TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def setup_logger() -> logging.Logger:
    # Windows Command Prompt is not UTF-8 by default; never crash on a curly quote.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except Exception:
            pass

    logger = logging.getLogger(BRAND)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = CentralFormatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    try:
        fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        fallback = OUTPUT_DIR / f"{BRAND.lower()}_run.log"
        logger.warning("Primary log path unavailable, falling back to %s", fallback)
        fh = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def build_session() -> requests.Session:
    # No automatic urllib3 retries: they honoured a 60s Retry-After five times
    # per address. Retries are handled by request_with_retry / _get_with_backoff.
    warnings.simplefilter("ignore", InsecureRequestWarning)
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    return session


def request_with_retry(session: requests.Session, method: str, url: str, logger: logging.Logger,
                       max_retries: int = 3, **kwargs: Any) -> requests.Response:
    """GraphQL calls (Good American's request_with_retry): 429/5xx retried after 1s, 2s, 4s."""
    response = None
    for attempt in range(max_retries):
        response = session.request(method, url, timeout=40, **kwargs)
        if response.status_code in {429} | TRANSIENT_STATUSES and attempt + 1 < max_retries:
            logger.warning("Retrying %s %s (HTTP %s)", method, url, response.status_code)
            time.sleep(2 ** attempt)
            continue
        break
    return response


# ---- product-page fetching (Good American method) ------------------------
_PACE: Dict[str, float] = {"interval": PDP_MIN_INTERVAL, "last": 0.0}
_PAGE_CACHE: Dict[Tuple[str, str], Optional[str]] = {}
_PAGE_TRIES: Dict[Tuple[str, str], int] = {}
_PAGES_BLOCKED: Set[Tuple[str, str]] = set()   # (kind, handle) never read because of a block / error
_REQUEST_COUNTS: Dict[str, int] = {"http_gets": 0, "blocked": 0}


def _pace_request() -> None:
    """Hold every /products/ request to the current shared interval."""
    wait = _PACE["last"] + _PACE["interval"] - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _PACE["last"] = time.monotonic()


def _is_bot_challenge(response: requests.Response) -> bool:
    return any(m in (response.text or "")[:5000].lower() for m in CHALLENGE_MARKERS)


def _get_with_backoff(session: requests.Session, url: str, logger: logging.Logger) -> Optional[requests.Response]:
    """Paced GET. A block (403/429) or server error slows the pace for every
    later request and gets ONE capped wait and retry - never a long sleep."""
    response = None
    for attempt in range(PDP_GETS_PER_TRY):
        _pace_request()
        _REQUEST_COUNTS["http_gets"] += 1
        try:
            response = session.get(url, timeout=30)
        except requests.RequestException as exc:
            logger.warning("Request error for %s: %s", url, exc)
            response = None
        status = response.status_code if response is not None else None
        if status is not None and status not in BLOCK_STATUSES and status not in TRANSIENT_STATUSES:
            _PACE["interval"] = max(PDP_MIN_INTERVAL, _PACE["interval"] * PDP_INTERVAL_DECAY)
            return response
        if status in BLOCK_STATUSES:
            _REQUEST_COUNTS["blocked"] += 1
        _PACE["interval"] = min(PDP_MAX_INTERVAL, _PACE["interval"] + PDP_INTERVAL_STEP)
        if attempt + 1 >= PDP_GETS_PER_TRY:
            break
        retry_after = (response.headers.get("Retry-After", "") if response is not None else "").strip()
        wait = float(retry_after) if retry_after.replace(".", "", 1).isdigit() else PDP_BLOCK_SLEEP * (attempt + 1)
        wait = min(wait, PDP_BLOCK_MAX_WAIT)
        kind = ("Bot challenge" if response is not None and _is_bot_challenge(response) else
                "Blocked" if status in BLOCK_STATUSES else "Server error" if status else "Connection error")
        logger.warning("%s (HTTP %s) on %s; waiting %.0fs (pace now %.1fs)", kind, status, url, wait, _PACE["interval"])
        time.sleep(wait)
    return response


def fetch_page(session: requests.Session, handle: str, kind: str, logger: logging.Logger) -> Optional[str]:
    """Product page ("html") or product .json ("json"), fetched once and cached.
    A blocked or failed page is NOT cached, so the end-of-run pass can try again;
    a page that was really answered (200, 404...) is cached for the whole run."""
    key = (kind, handle)
    if key in _PAGE_CACHE:
        return _PAGE_CACHE[key]
    if _PAGE_TRIES.get(key, 0) >= PDP_TRIES_PER_PAGE:
        return None
    _PAGE_TRIES[key] = _PAGE_TRIES.get(key, 0) + 1
    url = f"{SITE_HOST}/products/{handle}" + (".json" if kind == "json" else "")
    response = _get_with_backoff(session, url, logger)
    status = response.status_code if response is not None else None
    blocked = status is None or status in BLOCK_STATUSES or status in TRANSIENT_STATUSES
    if status == 200 and kind == "html" and html_looks_blocked(response.text):
        blocked = True
    if blocked:
        _PAGES_BLOCKED.add(key)
        logger.warning("Could not read %s for %s (HTTP %s); will try again at the end", kind, handle, status)
        return None
    _PAGES_BLOCKED.discard(key)
    text = response.text if status == 200 else None
    if status != 200:
        logger.warning("%s for %s returned HTTP %s", kind, handle, status)
    _PAGE_CACHE[key] = text
    return text


def fmt_duration(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"


# --------------------------------------------------------------------------
# Measurements (unchanged validated path)
# --------------------------------------------------------------------------
def convert_fraction(text: str) -> Optional[Decimal]:
    text = text.strip().replace("\u00bd", " 1/2")
    frac_match = FRACTION_REGEX.search(text)
    if frac_match:
        num = Decimal(frac_match.group("num"))
        den = Decimal(frac_match.group(3))
        whole = Decimal(frac_match.group("int")) if frac_match.group("int") else Decimal(0)
        return whole + num / den
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def extract_number_after(label: str, text: str) -> str:
    pattern = re.compile(rf"{label}[^\d]*(\d[\d\s./]*)", re.IGNORECASE)
    m = pattern.search(text)
    if not m:
        return ""
    val = m.group(1)
    dec = convert_fraction(val)
    return str(dec) if dec is not None else ""


def measurement_text_from_html(html: str) -> str:
    """Return the sizing/measurement block text if present, else full text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    sizing = soup.find(id="sizing-details-panel")
    if sizing:
        return sizing.get_text(" ", strip=True)
    return soup.get_text(" ", strip=True)


def parse_inseams(text: str) -> Dict[str, str]:
    """Parse inseam measurements from the sizing text.

    Handles standard "Short/Regular/Long Inseam" patterns, missing spaces
    (e.g., "33 1/2Long Inseam"), and height-based guidance such as
    "5'4 and under" -> Short, "5'4-5'7" -> Regular, "5'8 and over" -> Long.
    """
    mapping: Dict[str, str] = {}
    if not text:
        return mapping

    norm = " ".join(text.split())

    def assign(code: str, match: Optional[re.Match]) -> None:
        if not match:
            return
        dec = convert_fraction(match.group(1))
        if dec is not None:
            mapping.setdefault(code, str(dec))
            mapping.setdefault(str(dec), str(dec))

    for code, label in [("S", "Short"), ("R", "Regular"), ("L", "Long")]:
        assign(code, re.search(rf"(\d[\d\s./]*)\s*\"?\s*{label}\s*Inseam", norm, re.IGNORECASE))
        if code not in mapping:
            assign(code, re.search(rf"(\d[\d\s./]*)\s*\"?\s*{label}\b", norm, re.IGNORECASE))

    height_patterns = [
        ("S", r"(\d[\d\s./]*)\s*\"?\s*(?:in)?seam[^\d]{0,40}5'\s*4\s*(?:and\s*under|under)"),
        ("R", r"(\d[\d\s./]*)\s*\"?\s*(?:in)?seam[^\d]{0,40}5'\s*4\s*-\s*5'\s*7"),
        ("L", r"(\d[\d\s./]*)\s*\"?\s*(?:in)?seam[^\d]{0,40}5'\s*8\s*(?:and\s*over|and\s*above|and\s*over)") ,
    ]
    for code, pattern in height_patterns:
        if code in mapping:
            continue
        assign(code, re.search(pattern, norm, re.IGNORECASE))

    if not mapping:
        assign("default", re.search(r"Inseam:\s*(\d[\d\s./]*)", norm, re.IGNORECASE))

    return mapping


def to_decimal(value: str) -> Optional[Decimal]:
    try:
        return Decimal(str(value)) if str(value).strip() != "" else None
    except InvalidOperation:
        return None


# --------------------------------------------------------------------------
# Text helpers
# --------------------------------------------------------------------------
def norm_for_match(text: str) -> str:
    """Lower-case, curly->straight quotes, dashes->spaces, single spaces.

    Used ONLY for keyword matching so "Mid-Rise", "mid rise" and "Mid  Rise"
    all compare equal. Output columns are not built from this.
    """
    text = strip_accents(straighten_quotes(text or "")).lower()
    text = re.sub(r"[-\u2010\u2011\u2012\u2013\u2014]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def has_phrase(norm_text: str, phrases: Iterable[str]) -> bool:
    """Whole-word / whole-phrase match on text already passed through norm_for_match."""
    for phrase in phrases:
        p = norm_for_match(phrase)
        if not p:
            continue
        if re.search(rf"(?<![a-z0-9]){re.escape(p)}(?![a-z0-9])", norm_text):
            return True
    return False


def strip_accents(text: str) -> str:
    """Accented letter -> plain letter ("Cr\u00e8me" -> "Creme", "\u00c9" -> "E").
    Only accents are removed; symbols such as \u2122 or \u00bd are left alone."""
    decomposed = unicodedata.normalize("NFD", text or "")
    return unicodedata.normalize("NFC", "".join(c for c in decomposed if not unicodedata.combining(c)))


# Columns left exactly as the site gives them (changing a character could break a link).
NO_PLAIN_TEXT_COLUMNS = {"Image URL", "SKU URL"}

_PLAIN_REPLACEMENTS = {
    "\u2010": "-", "\u2011": "-", "\u2012": "-", "\u2013": "-", "\u2014": "-", "\u2015": "-", "\u2212": "-",
    "\u2026": "...", "\u2044": "/", "\u00d7": "x",
    "\u00f8": "o", "\u00d8": "O", "\u00e6": "ae", "\u00c6": "AE", "\u0153": "oe", "\u0152": "OE",
    "\u00df": "ss", "\u0142": "l", "\u0141": "L", "\u0111": "d", "\u0110": "D",
}


def to_plain_text(text: str) -> str:
    """Per Carrie 11/17: no special characters in the output.
    Curly quotes -> straight, accented letters -> plain letters, fractions -> text
    ("\u00bd" -> "1/2", "31\u00bd" -> "31 1/2"), long dashes -> "-", "\u2026" -> "...",
    odd spaces -> " ". Anything else that is not plain text (\u2122, \u00ae, \u00a9, emoji...) is removed."""
    text = strip_accents(straighten_quotes(text or ""))
    out: List[str] = []
    for ch in text:
        if ch.isascii():
            out.append(ch)
        elif ch in _PLAIN_REPLACEMENTS:
            out.append(_PLAIN_REPLACEMENTS[ch])
        elif "VULGAR FRACTION" in unicodedata.name(ch, ""):
            frac = unicodedata.normalize("NFKD", ch).replace("\u2044", "/")
            if out and out[-1][-1:].isdigit():
                frac = " " + frac
            out.append(frac)
        elif ch.isspace():
            out.append(" ")
        else:
            out.append("\x00")  # dropped (trademark, registered, copyright, symbols, emoji)
    result = "".join(out)
    # Tidy only the spaces a dropped symbol leaves behind ("R \u00a9 2026" -> "R 2026").
    result = re.sub(r" \x00+(?= |$)", "", result)
    result = re.sub(r"^\x00+ ", "", result)
    return result.replace("\x00", "")


def plain_text_rows(rows: List[Dict[str, Any]]) -> None:
    """Final pass: every text column (except the URLs) is plain text."""
    for r in rows:
        for key, value in r.items():
            if isinstance(value, str) and not key.startswith("_") and key not in NO_PLAIN_TEXT_COLUMNS:
                r[key] = to_plain_text(value)


def straighten_quotes(text: str) -> str:
    return (
        (text or "")
        .replace("\u2018", "'").replace("\u2019", "'").replace("\u201a", "'").replace("\u201b", "'")
        .replace("\u201c", '"').replace("\u201d", '"').replace("\u201e", '"').replace("\u201f", '"')
    )


def clean_description(text: str) -> str:
    """Description column: plain text (see to_plain_text), then '-' -> space."""
    return to_plain_text(text).replace("-", " ")


_FINAL_SALE_PATTERNS = [
    re.compile(r"[\(\[\{]\s*final[\s\-_]*sale\s*[\)\]\}]", re.IGNORECASE),  # (Final Sale)
    re.compile(r"\s*[-\u2013\u2014|:]\s*final[\s\-_]*sale\b", re.IGNORECASE),  # - Final Sale
    re.compile(r"\bfinal[\s\-_]*sale\b", re.IGNORECASE),  # Final Sale
    re.compile(r"[\(\[\{]\s*(?:final|sale)\s*[\)\]\}]", re.IGNORECASE),  # (Final) / (Sale)
    re.compile(r"\s*[-\u2013\u2014|:]\s*(?:final|sale)\b", re.IGNORECASE),  # - Sale
    re.compile(r"\b(?:final|sale)\b", re.IGNORECASE),  # Final / Sale
]


def strip_final_sale(title: str) -> str:
    """Remove 'Final Sale' / 'Final' / 'Sale' (in brackets, after a dash, or bare)
    from title text, then tidy leftover spaces/brackets/dashes.
    Use ONLY on text built from product.title - never on handle, URL or ids."""
    text = title or ""
    for pat in _FINAL_SALE_PATTERNS:
        text = pat.sub(" ", text)
    text = re.sub(r"[\(\[\{]\s*[\)\]\}]", " ", text)  # empty brackets left behind
    text = re.sub(r"\s+", " ", text).strip()
    return text.strip(" -\u2013\u2014|:/").strip()


def title_has_final_sale(title: str) -> bool:
    return strip_final_sale(title) != re.sub(r"\s+", " ", (title or "")).strip()


def is_promo(title: str, handle: str) -> bool:
    for text in (title, handle):
        norm = re.sub(r"[\-_\(\)\[\]\{\}]", " ", (text or "").lower())
        if re.search(r"\b(final|sale)\b", norm):
            return True
    return False


# --------------------------------------------------------------------------
# Product-page description (logic from retail_PDP_probe.py)
# --------------------------------------------------------------------------
BLOCK_TAGS = {
    "p", "div", "li", "ul", "ol", "dl", "dt", "dd", "table", "section", "article",
    "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "pre", "hr",
}
CHALLENGE_MARKERS = ("verifying your connection", "just a moment", "checking your browser", "cf-chl-")
BLOCK_TITLE_MARKERS = (
    "access denied", "attention required", "you have been blocked", "forbidden",
    "too many requests", "error 1020", "request blocked", "verify you are human",
)


def page_title(html: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s+", " ", match.group(1)).strip()[:120] if match else ""


def html_looks_blocked(html: str) -> bool:
    head = (html or "")[:5000].lower()
    title = page_title(html).lower()
    return any(m in head for m in CHALLENGE_MARKERS) or any(m in title for m in BLOCK_TITLE_MARKERS)


def normalize_text(tag: Tag) -> str:
    text = " ".join(part.strip() for part in tag.stripped_strings if part.strip())
    return re.sub(r"\s+", " ", text).strip()


def has_loose_lines(tag: Tag) -> bool:
    for node in tag.children:
        if isinstance(node, NavigableString) and not isinstance(node, Comment) and node.strip():
            return True
        if isinstance(node, Tag) and node.name == "br":
            return True
    return False


def split_loose_lines(tag: Tag) -> List[str]:
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


def _child_values_from_soup(soup: BeautifulSoup) -> List[str]:
    parent = None
    for sel in PDP_DETAILS_SELECTORS:
        parent = soup.select_one(sel)
        if parent is not None:
            break
    if parent is None:
        return []
    values: List[str] = []
    for child in [c for c in parent.find_all(recursive=False) if isinstance(c, Tag)]:
        nested = [n for n in child.find_all(recursive=False) if isinstance(n, Tag)]
        if nested and has_loose_lines(child):
            values.extend(v for v in split_loose_lines(child) if v)
        elif nested:
            values.extend(v for v in (normalize_text(n) for n in nested) if v)
        else:
            value = normalize_text(child)
            if value:
                values.append(value)
    return values


def pdp_child_values(html: str) -> List[str]:
    """All child elements of the product-details section, in page order
    (same extraction as retail_PDP_probe.py; best of lxml / html.parser)."""
    if not html:
        return []
    best: List[str] = []
    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html, parser)
        except FeatureNotFound:
            continue
        vals = _child_values_from_soup(soup)
        if len(vals) > len(best):
            best = vals
    return best


def fetch_inseam_siblings(html: str) -> Optional[List[str]]:
    """Inseam radio-button values on the product page.

    Returns None when the page could not be read (empty / blocked), so the
    caller does NOT treat it as checked. Returns [] when the page was read and
    has no Inseam fieldset (that is the site saying "single length").
    """
    if not html or html_looks_blocked(html):
        return None
    soup = BeautifulSoup(html, "html.parser")
    for fs in soup.find_all("fieldset"):
        legend = fs.find("legend")
        label = (legend.get_text(" ", strip=True) if legend else "").lower()
        if not label.startswith("inseam"):
            continue
        values = [inp.get("data-display-name") or inp.get("value") or "" for inp in fs.find_all("input", attrs={"type": "radio"})]
        m = re.search(r"--num-values\s*:\s*(\d+)", fs.get("style") or "")
        if m and int(m.group(1)) > len(values):
            values += [""] * (int(m.group(1)) - len(values))
        return values
    return []


# --------------------------------------------------------------------------
# Option columns: which is color / size / inseam
# --------------------------------------------------------------------------
COLOR_OPTION_NAMES = {"color", "colour", "colorway", "wash"}


def _is_number(value: str) -> bool:
    return re.fullmatch(r"\d+(?:\.\d+)?(?:\s*\d+/\d+)?\s*[\"\u201d]?", (value or "").strip()) is not None


def _num(value: str) -> Optional[float]:
    m = re.match(r"(\d+(?:\.\d+)?)", (value or "").strip())
    return float(m.group(1)) if m else None


def classify_options(option_names: Sequence[str], variant_values: Sequence[Sequence[str]], logger: Optional[logging.Logger] = None,
                     label: str = "") -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Return (color_idx, size_idx, inseam_idx) for a product, judged across ALL its variants."""
    n = len(option_names)
    columns: List[List[str]] = [[(vals[i] if i < len(vals) else "") for vals in variant_values] for i in range(n)]
    distinct = [list(dict.fromkeys(v for v in col if v)) for col in columns]

    color_idx = next((i for i, nm in enumerate(option_names) if (nm or "").strip().lower() in COLOR_OPTION_NAMES), None)
    others = [i for i in range(n) if i != color_idx and distinct[i]]
    if color_idx is None and others and not all(_is_number(v) for v in distinct[others[0]]) \
            and not set(v.upper() for v in distinct[others[0]]) <= SIZE_LIST:
        color_idx = others.pop(0)

    numeric_cols = [i for i in others if all(_is_number(v) for v in distinct[i])]
    letter_cols = [i for i in others if {v.strip().upper() for v in distinct[i]} <= set(INSEAM_LETTERS)]

    size_idx: Optional[int] = None
    inseam_idx: Optional[int] = None

    for i in letter_cols:
        vals = {v.strip().upper() for v in distinct[i]}
        other_numeric = any(any(_is_number(v) for v in distinct[j]) for j in others if j != i)
        # "S"/"L" are sizes only when no other option column contains a number.
        if "R" not in vals and not other_numeric:
            continue
        inseam_idx = i
        break

    if inseam_idx is None and len(numeric_cols) >= 2:
        def inseam_like(i: int) -> bool:
            nums = [_num(v) for v in distinct[i]]
            return 2 <= len(nums) <= 3 and all(x is not None and 24 <= x <= 37 for x in nums)

        a, b = numeric_cols[0], numeric_cols[1]
        cand = [i for i in (a, b) if inseam_like(i)]
        if len(distinct[a]) != len(distinct[b]) and cand:
            fewer = a if len(distinct[a]) < len(distinct[b]) else b
            if fewer in cand:
                inseam_idx = fewer
        if inseam_idx is None and len(distinct[a]) == len(distinct[b]):
            inseam_idx = b  # counts can't separate them: later option is inseam
        if inseam_idx is not None and logger:
            size_col = a if inseam_idx == b else b
            pairs = {(vals[size_col], vals[inseam_idx]) for vals in variant_values if len(vals) > max(size_col, inseam_idx)}
            missing = [(s, ins) for s in distinct[size_col] for ins in distinct[inseam_idx] if (s, ins) not in pairs]
            if missing:
                logger.info("%s: numeric inseam column is not paired with every size (%s missing combos)", label, len(missing))

    remaining = [i for i in others if i != inseam_idx]
    if remaining:
        size_idx = max(remaining, key=lambda i: (len(distinct[i]), -i))
    return color_idx, size_idx, inseam_idx


def numeric_inseam_labels(values: Sequence[str], description: str) -> Dict[str, str]:
    """Map numeric inseam values -> Petite/Regular/Long using the description's
    '"S" for' / '"R" for' / '"L" for' guidance."""
    desc = straighten_quotes(description or "")
    has_s, has_r, has_l = ('"S" for' in desc), ('"R" for' in desc), ('"L" for' in desc)
    ordered = sorted(dict.fromkeys(values), key=lambda v: _num(v) or 0)
    if has_s and has_r and has_l and len(ordered) == 3:
        names = ["Petite", "Regular", "Long"]
    elif has_s and has_r and not has_l and len(ordered) == 2:
        names = ["Petite", "Regular"]
    elif has_r and has_l and not has_s and len(ordered) == 2:
        names = ["Regular", "Long"]
    else:
        return {}
    return dict(zip(ordered, names))


# --------------------------------------------------------------------------
# Derived columns
# --------------------------------------------------------------------------
def normalize_color_snippet(description: str, color: str) -> str:
    """Text the description writes for THIS color only, e.g. "Caviar: Velvety soft
    denim in a beautiful washed black." -> "caviar: velvety soft ... washed black".
    The color must be the label before a colon, alone or in a list of color
    names ("Beep Beep, Tell Me: ..."). A sentence that only mentions the color
    ("Skyscraper, Russell, and Fade to Black all have ... creasing") is ignored.
    """
    if not description or not color:
        return ""
    name = r"[A-Z][\w'\u2019]*(?:\s+[\w'\u2019]+){0,3}"
    pattern = re.compile(
        rf"(?<![\w])({re.escape(color)}(?:\s*(?:,|&|\band\b)\s*{name})*)\s*:\s*([^.]*)",
        re.IGNORECASE,
    )
    m = pattern.search(description)
    if not m:
        return ""
    return f"{color}: {m.group(2)}".lower().strip()


def derive_color_simplified(snippet: str) -> str:
    if not snippet:
        return ""
    if "medium" in snippet and "wash" in snippet:
        return "Medium"
    if "dark" in snippet and "wash" in snippet:
        return "Dark"
    if "light" in snippet and "wash" in snippet:
        return "Light"
    if "black" in snippet or "rich brown" in snippet:
        return "Dark"
    if "white" in snippet and "cream" in snippet:
        return "Light"
    return ""


def derive_color_standardized(snippet: str) -> str:
    if not snippet:
        return ""
    if ("medium" in snippet and "wash" in snippet) or ("dark" in snippet and "wash" in snippet) or ("light" in snippet and "wash" in snippet):
        return "Blue"
    if "black" in snippet:
        return "Black"
    if "brown" in snippet:
        return "Brown"
    if "white" in snippet or "cream" in snippet:
        return "White"
    return ""


COLOR_STANDARD_RULES: List[Tuple[str, List[str]]] = [
    ("Animal Print", ["Animal Print", "leopard", "snake", "Camo"]),
    ("Blue", ["Blue", "Bleu", "Blues", "Navy", "indigo"]),
    ("Brown", ["Brown", "Cinnamon", "Coffee", "Espresso"]),
    ("Green", ["Green", "Olive", "Cypress", "Sage"]),
    ("Gray", ["Grey", "Gray", "Asphalt"]),
    ("Orange", ["Orange"]),
    ("Pink", ["Pink", "Blush", "Coral"]),
    ("Print", ["Print"]),
    ("Purple", ["Purple", "Violet"]),
    ("Red", ["Red", "wine", "burgundy"]),
    ("Tan", ["Tan", "Sand", "Sable", "Beige", "khaki"]),
    ("White", ["White", "ecru", "egret", "cream", "Creme", "Blizzard", "Ivory", "Parchment", "Blanc"]),
    ("Black", ["Black", "Noir", "Onyx", "Raven"]),
]


def color_standardized_from_color(color: str) -> str:
    norm = norm_for_match(color)
    for label, words in COLOR_STANDARD_RULES:
        if has_phrase(norm, words):
            return label
    return ""


JEAN_STYLE_KEYWORDS = [
    "barrel", "barrell", "bowed", "bow leg", "horseshoe", "skinny", "flare", "bootcut",
    "boot cut", "taper", "tapering", "tapered", "wide leg", "palazzo", "straight leg",
    "stovepipe", "cigarette", "baggy", "slouchy, relaxed", "straight",
]


def derive_jean_style(description: str, leg_opening: str) -> str:
    """Step 1: description keywords (first match wins)."""
    d = norm_for_match(description)
    lo = to_decimal(leg_opening)
    if has_phrase(d, ["Barrel", "Barrell", "Bowed", "Bow leg", "horseshoe"]):
        return "Barrel"
    if has_phrase(d, ["Skinny"]):
        return "Skinny"
    if has_phrase(d, ["Flare"]):
        return "Flare"
    if has_phrase(d, ["Bootcut", "Boot-cut", "Boot cut"]):
        return "Bootcut"
    if has_phrase(d, ["Taper", "Tapering", "Tapered"]):
        return "Tapered"
    if has_phrase(d, ["Wide leg", "Wide-leg", "palazzo"]):
        return "Wide Leg"
    if has_phrase(d, ["Straight leg", "straight-leg"]) and lo is not None:
        if lo <= Decimal("15.5"):
            return "Straight from Knee"
        if lo <= Decimal("17"):
            return "Straight from Knee/Thigh"
        return "Straight from Thigh"
    if has_phrase(d, ["stovepipe", "cigarette"]):
        return "Straight from Knee"
    if has_phrase(d, ["Baggy", "Slouchy, relaxed"]):
        return "Baggy"
    if has_phrase(d, ["Straight"]) and lo is None:
        return "Straight from Knee/Thigh"
    return ""


RISE_RULES: List[Tuple[str, List[str]]] = [
    ("Ultra Low", ["Rise: Super Low", "Rise: Ultra Low", "Rise - Super Low", "Rise - Ultra Low", "super low rise",
                   "super low-rise", "ultra low rise", "ultra low-rise", "super low waist", "super low-waist",
                   "ultra low waist", "ultra low-waist"]),
    ("Ultra High", ["Rise: Super High", "Rise: Ultra High", "Rise - Super High", "Rise - Ultra High", "highest rise",
                    "super high rise", "super high-rise", "ultra high rise", "ultra high-rise", "super high waist",
                    "super high-waist", "super high-waisted", "super high waisted", "ultra high waist",
                    "ultra high-waist"]),
    ("Mid", ["Rise: Mid", "Rise - Mid", "Mid-Rise", "mid waisted", "mid-waisted", "sits mid", "Mid Rise"]),
    ("Low", ["Rise: Low", "Rise - Low", "Low-Rise", "Low Rise", "hip-hugging fit", "low on the hips", "low waisted",
             "low-waisted"]),
    ("High", ["Rise: High", "Rise - High", "High-Rise", "High Rise", "High Waist", "High-Waist", "High Waisted",
              "High-Waisted", "sits high", "high on the hip", "high on the waist"]),
]


def derive_rise_label(pdp_description: str, graphql_description: str, tags: List[str]) -> str:
    # New keyword list (whole words/phrases) on the product-page description.
    d = norm_for_match(pdp_description)
    for label, phrases in RISE_RULES:
        if has_phrase(d, phrases):
            return label
    # Existing rules, unchanged, as the fallback.
    desc = graphql_description.lower()
    tags_lower = [t.lower() for t in tags]
    if "high-" in desc or "high rise" in desc:
        return "High"
    if "mid-" in desc or "mid rise" in desc:
        return "Mid"
    if "low-" in desc or "low rise" in desc or "loose on" in desc:
        return "Low"
    if any("high rise" in t for t in tags_lower):
        return "High"
    if any("mid rise" in t for t in tags_lower):
        return "Mid"
    if any(sub in tag for tag in tags_lower for sub in ["low rise", "slouchy", "low waist"]):
        return "Low"
    return ""


def derive_inseam_style(description: str, seo_title: str) -> str:
    desc = description.lower()
    seo = (seo_title or "").lower()
    if "ankle" in desc:
        return "Ankle"
    if "crop" in desc:
        return "Cropped"
    if "full length" in desc:
        return "Full Length"
    if "ankle" in seo:
        return "Ankle"
    if "crop" in seo:
        return "Cropped"
    return "Full Length"


def parse_country(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    heading = soup.find(id="made-in-details-heading")
    if not heading:
        return ""
    panel = soup.find(id="made-in-details-panel")
    text = panel.get_text(" ", strip=True).lower() if panel else heading.get_text(" ", strip=True).lower()
    if "california" in text:
        return "USA"
    if "china" in text:
        return "China"
    return ""


def to_mmddyy(dt_str: str) -> str:
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except Exception:
        return dt_str


def to_central_datetime(dt_str: str) -> str:
    """ISO timestamp -> 'M/D/YYYY H:MM:SS AM/PM' in US Central (same as Goodamerican 'Updated At')."""
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local = dt.astimezone(CENTRAL_TZ)
        return f"{local.month}/{local.day}/{local.year} {local.strftime('%I').lstrip('0') or '12'}:{local.strftime('%M:%S %p')}"
    except Exception:
        return dt_str


# --------------------------------------------------------------------------
# Fetching
# --------------------------------------------------------------------------
def excluded_tag_hits(tags: Iterable[str]) -> List[str]:
    return [t for t in tags if (t or "").strip().lower() in EXCLUDED_TAGS_LOWER]


def banned_desc_hits(text: str) -> List[str]:
    low = (text or "").lower()
    return [t for t in BANNED_DESC_TERMS if t in low]


def fetch_graphql_products(session: requests.Session, logger: logging.Logger) -> Tuple[List[VariantRow], Dict[str, Dict[str, Any]]]:
    rows: List[VariantRow] = []
    product_options: Dict[str, Dict[str, Any]] = {}
    query = """
    query ($handle: String!, $cursor: String) {
      collection(handle: $handle) {
        products(first: 50, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id
              handle
              title
              description
              productType
              tags
              vendor
              onlineStoreUrl
              updatedAt
              options { name values }
              seo { title description }
              featuredImage { url altText }
              variants(first: 250) {
                edges {
                  node {
                    id
                    title
                    sku
                    availableForSale
                    selectedOptions { name value }
                    price { amount }
                    compareAtPrice { amount }
                    image { url altText }
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    headers = {**HEADERS, "X-Shopify-Storefront-Access-Token": GRAPHQL_TOKEN}
    seen_products: Set[str] = set()
    for endpoint in GRAPHQL_ENDPOINTS:
        for handle in COLLECTION_HANDLES:
            cursor = None
            page = 0
            while True:
                page += 1
                payload = {"query": query, "variables": {"handle": handle, "cursor": cursor}}
                try:
                    resp = request_with_retry(session, "POST", endpoint, logger, json=payload, headers=headers)
                except requests.RequestException as exc:
                    logger.warning("GraphQL request failed %s: %s", endpoint, exc)
                    break
                if resp.status_code != 200:
                    logger.warning("GraphQL request failed %s: %s", endpoint, resp.text[:200])
                    break
                data = resp.json().get("data", {})
                prod_block = data.get("collection", {}) or {}
                products = prod_block.get("products")
                if not products:
                    break
                logger.info("Collection %s page %s: %s products", handle, page, len(products.get("edges", [])))
                for edge in products.get("edges", []):
                    node = edge.get("node", {})
                    product_id = node.get("id", "").split("/")[-1]
                    if product_id in seen_products:
                        continue  # same product listed in both collections
                    seen_products.add(product_id)
                    product_type = node.get("productType") or ""
                    desc = node.get("description") or ""
                    handle_val = node.get("handle", "")
                    if product_type != TARGET_PRODUCT_TYPE:
                        continue
                    hits = banned_desc_hits(desc)
                    if hits:
                        logger.info("Removed %s: description contains %s", handle_val, hits)
                        continue
                    tags = node.get("tags", []) or []
                    tags_lower = [t.lower() for t in tags]
                    if any(term in tag for tag in tags_lower for term in BANNED_TAG_TERMS):
                        logger.info("Removed %s: tag contains skirt/shorts", handle_val)
                        continue
                    tag_hits = excluded_tag_hits(tags)
                    if tag_hits:
                        logger.info("Removed %s: excluded tag(s) %s", handle_val, tag_hits)
                        continue
                    seo = node.get("seo") or {}
                    option_names = [o.get("name", "") for o in (node.get("options") or [])]
                    variant_nodes = [v_edge.get("node", {}) for v_edge in (node.get("variants", {}) or {}).get("edges", [])]
                    value_lists: List[List[str]] = []
                    for v in variant_nodes:
                        by_name = {opt.get("name", ""): opt.get("value", "") for opt in v.get("selectedOptions", [])}
                        value_lists.append([by_name.get(nm, "") for nm in option_names])
                    color_idx, size_idx, inseam_idx = classify_options(option_names, value_lists, logger, handle_val)
                    product_options[product_id] = {
                        "option_names": option_names,
                        "color_idx": color_idx,
                        "size_idx": size_idx,
                        "inseam_idx": inseam_idx,
                        "inseam_values": list(dict.fromkeys(vals[inseam_idx] for vals in value_lists if inseam_idx is not None and vals[inseam_idx])),
                    }
                    for v, vals in zip(variant_nodes, value_lists):
                        variant_id = v.get("id", "").split("/")[-1]
                        color = vals[color_idx] if color_idx is not None else ""
                        size = vals[size_idx] if size_idx is not None else ""
                        inseam_code = vals[inseam_idx] if inseam_idx is not None else ""
                        rows.append(
                            VariantRow(
                                product_id=product_id,
                                handle=handle_val,
                                product_title=node.get("title", ""),
                                color=color,
                                size=size,
                                inseam_code=inseam_code,
                                variant_id=variant_id,
                                sku=v.get("sku", ""),
                                barcode="",
                                available=bool(v.get("availableForSale")),
                                price=(v.get("price") or {}).get("amount") or "",
                                compare_at=(v.get("compareAtPrice") or {}).get("amount") or "",
                                image_url=(v.get("image") or {}).get("url"),
                                online_url=node.get("onlineStoreUrl"),
                                description=desc,
                                tags=tags,
                                vendor=node.get("vendor", ""),
                                product_type=product_type,
                                seo_title=seo.get("title"),
                                variant_title=v.get("title", ""),
                                updated_at=node.get("updatedAt") or "",
                                option_values=vals,
                            )
                        )
                page_info = products.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break
    logger.info("Collected %s variants from GraphQL (%s products kept)", len(rows), len(product_options))
    return rows, product_options


def fetch_product_json(session: requests.Session, handle: str, logger: logging.Logger) -> Dict[str, Any]:
    text = fetch_page(session, handle, "json", logger)
    if not text:
        return {}
    try:
        return json.loads(text).get("product", {}) or {}
    except ValueError:
        logger.warning("Product .json for %s was not JSON", handle)
        return {}


def fetch_pdp_html(session: requests.Session, handle: str, logger: logging.Logger) -> str:
    return fetch_page(session, handle, "html", logger) or ""


def fetch_all_pages(session: requests.Session, handles: List[str], logger: logging.Logger
                    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    """Product .json + product page for every handle, with progress/ETA.
    Pages the site blocked get one more try at the end, once the pace has backed off."""
    json_cache: Dict[str, Dict[str, Any]] = {}
    pdp_cache: Dict[str, str] = {}

    def run_pass(todo: List[str], label: str) -> None:
        start = time.monotonic()
        for i, handle in enumerate(todo, start=1):
            if not json_cache.get(handle):
                json_cache[handle] = fetch_product_json(session, handle, logger)
            if not pdp_cache.get(handle):
                pdp_cache[handle] = fetch_pdp_html(session, handle, logger)
            elapsed = time.monotonic() - start
            eta = elapsed / i * (len(todo) - i)
            logger.info("%s %s/%s (%s) - elapsed %s, ETA %s", label, i, len(todo), handle, fmt_duration(elapsed), fmt_duration(eta))

    run_pass(handles, "Product pages")
    retry = sorted({h for _, h in _PAGES_BLOCKED})
    if retry:
        logger.info("Second try for %s handle(s) the site blocked: %s", len(retry), ", ".join(retry))
        run_pass(retry, "Second try")

    logger.info("Product pages: %s HTTP requests, %s blocked; final pace %.1fs between requests",
                _REQUEST_COUNTS["http_gets"], _REQUEST_COUNTS["blocked"], _PACE["interval"])
    missing_json = sorted(h for k, h in _PAGES_BLOCKED if k == "json")
    missing_html = sorted(h for k, h in _PAGES_BLOCKED if k == "html")
    if missing_json:
        logger.warning("Product .json NEVER read for %s handle(s) - their Quantity Available, Barcode, "
                       "Published/Created At are missing: %s", len(missing_json), ", ".join(missing_json))
    if missing_html:
        logger.warning("Product page NEVER read for %s handle(s) - their Description falls back to GraphQL and "
                       "Rise / Inseam / Leg Opening / Country are blank: %s", len(missing_html), ", ".join(missing_html))
    return json_cache, pdp_cache


# --------------------------------------------------------------------------
# Inseam Label Multi
# --------------------------------------------------------------------------
def _alt_base_and_length(alt_title: str) -> Tuple[str, str]:
    parts = [p.strip() for p in (alt_title or "").split(" / ")]
    if not parts:
        return "", ""
    last = parts[-1]
    if any(last.lower() == w or last.lower().startswith(w) for w in LENGTH_WORDS):
        return " / ".join(parts[:-1]), last
    return " / ".join(parts), ""


def apply_inseam_label_multi(rows: List[Dict[str, Any]], pdp_html: Optional[Dict[str, str]], logger: logging.Logger) -> None:
    # Step 0 - everything is Single.
    for row in rows:
        row["Inseam Label Multi"] = "Single"

    # Step 1 - two different lengths sharing one base.
    by_base: Dict[str, Set[str]] = defaultdict(set)
    for row in rows:
        base, length = _alt_base_and_length(row["Product Title Alt"])
        by_base[base.upper()].add(length)
    for row in rows:
        base, _ = _alt_base_and_length(row["Product Title Alt"])
        if len(by_base[base.upper()]) > 1:
            row["Inseam Label Multi"] = "Multi"
    logger.info("Inseam Label Multi step 1: %s of %s rows Multi", sum(r["Inseam Label Multi"] == "Multi" for r in rows), len(rows))

    if pdp_html is None:
        return  # no network: step 1 is final

    # Step 2 - ask the product page for the ones step 1 could not settle.
    pending = sorted({
        row["Handle"] for row in rows
        if row["Inseam Label Multi"] == "Single" and _alt_base_and_length(row["Product Title Alt"])[1]
    })
    logger.info("Inseam Label Multi step 2: %s handle(s) to check on the product page", len(pending))
    for handle in pending:
        handle_rows = [r for r in rows if r["Handle"] == handle]
        if handle_rows and handle_rows[0]["Inseam Label Multi"] == "Multi":
            continue
        siblings = fetch_inseam_siblings(pdp_html.get(handle, ""))
        if siblings is None:
            logger.warning("Inseam Label Multi: product page for %s could not be read; left unchecked", handle)
            continue
        if len(siblings) <= 1:
            continue
        families = {(r["_title_key"], r["_color_key"]) for r in handle_rows}
        changed = 0
        for r in rows:
            if (r["_title_key"], r["_color_key"]) in families and r["Inseam Label Multi"] != "Multi":
                r["Inseam Label Multi"] = "Multi"
                changed += 1
        logger.info("Inseam Label Multi: %s shows %s inseams -> %s row(s) set to Multi", handle, len(siblings), changed)

    # Step 3 - clean up: Multi rows with no length name their length.
    for row in rows:
        if row["Inseam Label Multi"] == "Multi" and not _alt_base_and_length(row["Product Title Alt"])[1]:
            row["Product Title Alt"] = f"{row['Product Title Alt']} / Regular"


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
OUTPUT_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Created At",
    "Updated At",
    "Product",
    "Product Title Alt",
    "Style Name",
    "Product Type",
    "Tags",
    "Vendor",
    "Description",
    "Variant Title",
    "Color",
    "Size",
    "Rise",
    "Inseam",
    "Leg Opening",
    "Price",
    "Compare at Price",
    "Promo",
    "Available for Sale",
    "Quantity Available",
    "Notify Me Signups",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Label",
    "Inseam Label Multi",
    "Rise Label",
    "Inseam Style",
    "Color - Simplified",
    "Color - Standardized",
    "Fabric Source",
    "Country Produced",
]


def build_rows(variants: List[VariantRow], product_options: Dict[str, Dict[str, Any]],
               json_cache: Dict[str, Dict[str, Any]], pdp_cache: Dict[str, str],
               logger: logging.Logger) -> List[Dict[str, Any]]:
    measurement_cache: Dict[str, str] = {}
    pdp_text_cache: Dict[str, str] = {}
    country_cache: Dict[str, str] = {}
    desc_cache: Dict[str, str] = {}
    out: List[Dict[str, Any]] = []

    # Product-page description (+ removal when it names a banned term).
    removed_handles: Set[str] = set()
    for handle in dict.fromkeys(v.handle for v in variants):
        children = pdp_child_values(pdp_cache.get(handle, ""))
        if children:
            # Leave out the "Product Description" and "Sizing" headings (Material /
            # Care / Made In headings stay), per Carrie 11/17.
            children = [c for c in children if c.strip().lower() not in DESCRIPTION_HEADINGS_TO_SKIP]
            raw = "; ".join(children)
        else:
            raw = next(v.description for v in variants if v.handle == handle)
            logger.warning("No product-page description for %s; using the GraphQL description", handle)
        hits = banned_desc_hits(raw)
        if hits:
            logger.info("Removed %s: product-page description contains %s", handle, hits)
            removed_handles.add(handle)
        desc_cache[handle] = clean_description(raw)
    variants = [v for v in variants if v.handle not in removed_handles]

    for var in variants:
        if var.handle not in measurement_cache:
            html = pdp_cache.get(var.handle, "")
            measurement_cache[var.handle] = measurement_text_from_html(html)
            pdp_text_cache[var.handle] = BeautifulSoup(html, "html.parser").get_text(" ", strip=True) if html else ""
            country_cache[var.handle] = parse_country(html)

        product_json = json_cache.get(var.handle, {})
        pj_variants = {str(v.get("id")): v for v in product_json.get("variants", [])}
        pj_variant = pj_variants.get(var.variant_id, {})
        published_at = pj_variant.get("published_at") or product_json.get("published_at") or ""
        created_at = pj_variant.get("created_at") or product_json.get("created_at") or ""
        # GraphQL product.updatedAt. (The product .json "updated_at" is just the time of
        # the request - it changes on every fetch - so it is not used.)
        updated_at = var.updated_at
        qty = pj_variant.get("inventory_quantity")
        qty_available = qty if isinstance(qty, int) and qty >= 0 else 0
        notify_me = abs(qty) if isinstance(qty, int) and qty < 0 else 0

        description = desc_cache[var.handle]
        measurement_text = measurement_cache.get(var.handle) or pdp_text_cache.get(var.handle, "")
        rise = extract_number_after("Front Rise", measurement_text) or extract_number_after("Rise", measurement_text)
        leg_opening = extract_number_after("Leg Opening", measurement_text)
        inseam_map = parse_inseams(measurement_text)
        inseam_val = ""
        if var.inseam_code:
            inseam_val = inseam_map.get(var.inseam_code, "")
            if not inseam_val and _is_number(var.inseam_code):
                inseam_val = str(convert_fraction(re.sub(r"[\"\u201d]", "", var.inseam_code)) or "")
        if not inseam_val and inseam_map:
            inseam_val = next(iter(inseam_map.values()))

        # Inseam Label
        opts = product_options.get(var.product_id, {})
        code = var.inseam_code.strip()
        if code.upper() in INSEAM_LETTERS:
            inseam_label = INSEAM_LETTERS[code.upper()]
            has_length = True
        elif code:
            mapping = numeric_inseam_labels(opts.get("inseam_values", []), description)
            inseam_label = mapping.get(code, "")
            has_length = bool(inseam_label)
            if not inseam_label:
                logger.warning("%s: numeric inseam %s has no S/R/L guidance in the description; Inseam Label left blank", var.handle, code)
        else:
            inseam_label = "Regular"
            has_length = False

        clean_title = to_plain_text(strip_final_sale(var.product_title)).strip()
        color = to_plain_text(var.color).strip()

        # Product: title / [Petite|Regular|Long when the variant's inseam is S/R/L /] Color
        product_parts = [clean_title]
        if code.upper() in INSEAM_LETTERS:
            product_parts.append(INSEAM_LETTERS[code.upper()])
        if color:
            product_parts.append(color)
        product_field = " / ".join(product_parts)

        # Product Title Alt: title / Color / Inseam Label (only when the product really has a length option)
        alt_parts = [clean_title] + ([color] if color else []) + ([inseam_label] if has_length else [])
        product_title_alt = " / ".join(alt_parts)

        color_snippet = normalize_color_snippet(var.description, var.color)
        color_standardized = color_standardized_from_color(color) or derive_color_standardized(color_snippet)
        color_simplified = derive_color_simplified(color_snippet)
        if color_standardized == "Black":
            color_simplified = "Dark"
        fabric_source = "Caitac" if any(t.lower() == "caitac" for t in var.tags) else ""

        out.append({
            "Style Id": var.product_id,
            "Handle": var.handle,
            "Published At": to_mmddyy(published_at),
            "Created At": to_mmddyy(created_at),
            "Updated At": to_central_datetime(updated_at),
            "Product": product_field,
            "Product Title Alt": product_title_alt,
            "Style Name": "",
            "Product Type": var.product_type,
            "Tags": ", ".join(var.tags),
            "Vendor": var.vendor,
            "Description": description,
            "Variant Title": "",
            "Color": color,
            "Size": var.size,
            "Rise": rise,
            "Inseam": inseam_val,
            "Leg Opening": leg_opening,
            "Price": var.price,
            "Compare at Price": var.compare_at,
            "Promo": "Final Sale" if is_promo(var.product_title, var.handle) else "",
            "Available for Sale": "TRUE" if var.available else "FALSE",
            "Quantity Available": qty_available,
            "Notify Me Signups": notify_me,
            "Quantity of style": 0,
            "SKU - Shopify": var.variant_id,
            "SKU - Brand": var.sku,
            "Barcode": pj_variant.get("barcode", ""),
            "Image URL": var.image_url or (product_json.get("image", {}) or {}).get("src", ""),
            "SKU URL": var.online_url or f"https://www.ayr.com/products/{var.handle}",
            "Jean Style": derive_jean_style(description, leg_opening),
            "Inseam Label": inseam_label,
            "Inseam Label Multi": "",
            "Rise Label": derive_rise_label(description, var.description, var.tags),
            "Inseam Style": derive_inseam_style(var.description, var.seo_title or ""),
            "Color - Simplified": color_simplified,
            "Color - Standardized": color_standardized,
            "Fabric Source": fabric_source,
            "Country Produced": country_cache.get(var.handle, ""),
            # helper keys (not written)
            "_raw_title": var.product_title,
            "_title_key": clean_title.upper(),
            "_color_key": color.upper(),
            "_title_had_final_sale": title_has_final_sale(var.product_title),
            "_final_sale": is_promo(var.product_title, var.handle),
            "_created_raw": created_at,
        })

    # Jean Style step 2: still blank and no keywords in title or description ->
    # copy from other items with the same product.title if they all agree.
    styles_by_title: Dict[str, Set[str]] = defaultdict(set)
    for r in out:
        if r["Jean Style"]:
            styles_by_title[r["_title_key"]].add(r["Jean Style"])
    for r in out:
        if r["Jean Style"]:
            continue
        if has_phrase(norm_for_match(r["_raw_title"]), JEAN_STYLE_KEYWORDS) or has_phrase(norm_for_match(r["Description"]), JEAN_STYLE_KEYWORDS):
            continue
        styles = styles_by_title.get(r["_title_key"], set())
        if len(styles) == 1:
            r["Jean Style"] = next(iter(styles))

    # Style Name: title + first word of Jean Style
    for r in out:
        first = r["Jean Style"].split()[0] if r["Jean Style"] else ""
        r["Style Name"] = f"{strip_final_sale(r['_raw_title'])} {first}".strip()

    # Product " / OLD" (Carrie 11/17): when a style whose title or handle has
    # final/sale ends up with the same Product as a style without it, the
    # final-sale style gets " / OLD".
    by_product: Dict[str, Dict[str, bool]] = defaultdict(dict)
    for r in out:
        by_product[r["Product"].upper()][r["Style Id"]] = r["_final_sale"]
    for r in out:
        ids = by_product[r["Product"].upper()]
        if len(ids) > 1 and any(ids.values()) and not all(ids.values()) and r["_final_sale"]:
            r["Product"] = f"{r['Product']} / OLD"

    return out


def finalize_rows(out: List[Dict[str, Any]]) -> None:
    # Variant Title = Product Title Alt (after the Multi clean-up) / Size
    for r in out:
        r["Variant Title"] = f"{r['Product Title Alt']} / {r['Size']}" if r["Size"] else r["Product Title Alt"]
    # Quantity of style: sum by style id + color + inseam label
    totals: Dict[Tuple[str, str, str], int] = defaultdict(int)
    for r in out:
        totals[(r["Style Id"], r["_color_key"], r["Inseam Label"])] += int(r["Quantity Available"] or 0)
    for r in out:
        r["Quantity of style"] = totals[(r["Style Id"], r["_color_key"], r["Inseam Label"])]


def _parse_dt(value: str) -> Optional[datetime]:
    try:
        dt = datetime.fromisoformat((value or "").replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def insert_old_marker_variant_title(title: str, size: str, inseam_label: str, has_length: bool, marker: str) -> Optional[str]:
    """OLD goes after the color, right before the size (add-on 2.3).
    "The LaLa / Waylon / Long / 24" -> "The LaLa / Waylon / Long OLD / 24"
    "The Space / Asphalt / XS"      -> "The Space / Asphalt / OLD / XS"
    Returns None when the size can't be found (caller skips and logs)."""
    parts = [p.strip() for p in (title or "").split(" / ")]
    idx = next((i for i in range(len(parts) - 1, -1, -1) if size and parts[i].lower() == size.strip().lower()), None)
    if idx is None or idx == 0:
        return None
    if has_length and parts[idx - 1].lower() == (inseam_label or "").lower():
        parts[idx - 1] = f"{parts[idx - 1]} {marker}"
    else:
        parts.insert(idx, marker)
    return " / ".join(parts)


def insert_old_marker_alt_title(alt: str, inseam_label: str, has_length: bool, marker: str) -> str:
    """Alt title: marker at the end, same inseam rule.
    "The LaLa / Waylon / Long" -> "... / Long OLD"; "The Space / Asphalt" -> "... / Asphalt / OLD"."""
    if not alt:
        return alt
    parts = [p.strip() for p in alt.split(" / ")]
    if has_length and parts[-1].lower() == (inseam_label or "").lower():
        parts[-1] = f"{parts[-1]} {marker}"
    else:
        parts.append(marker)
    return " / ".join(parts)


def apply_old_listing_markers(rows: List[Dict[str, Any]], logger: logging.Logger) -> None:
    """Two or more SKUs (different listings) with the same Variant Title (Carrie 11/17):
    1. If some of the listings have final/sale in the title or handle and some don't,
       the final-sale listing(s) get OLD.
    2. Otherwise the older listing(s) by Created At get OLD; the newest gets none.
    Only Variant Title and Product Title Alt change, and the marker is always "OLD".
    A tie or missing date in case 2 changes nothing and is logged.
    A listing is Style Id + Color: AYR keeps several colors under one style id /
    handle, so style id or handle alone does not identify a style."""
    groups: Dict[str, Dict[Tuple[str, str], List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        groups[r["Variant Title"].strip().upper()][(r["Style Id"], r["_color_key"])].append(r)

    marked = skipped = 0
    for vt, by_style in groups.items():
        skus = {r["SKU - Shopify"] for style_rows in by_style.values() for r in style_rows}
        if len(by_style) < 2 or len(skus) < 2:
            continue
        created = {sid: _parse_dt(style_rows[0]["_created_raw"]) for sid, style_rows in by_style.items()}
        final_sale = {sid: style_rows[0]["_final_sale"] for sid, style_rows in by_style.items()}
        if any(final_sale.values()) and not all(final_sale.values()):
            targets = [sid for sid in by_style if final_sale[sid]]
            reason = "final sale"
        else:
            dates = list(created.values())
            if any(d is None for d in dates) or len(set(dates)) < len(dates):
                logger.warning("OLD skipped: '%s' is shared by %s listings but Created At is missing or tied (%s)",
                               vt, len(by_style), ", ".join(f"{sid} {color}" for sid, color in by_style))
                skipped += 1
                continue
            newest = max(by_style, key=lambda sid: created[sid])
            targets = [sid for sid in by_style if sid != newest]
            reason = "older Created At"
        far_future = datetime.max.replace(tzinfo=timezone.utc)
        targets.sort(key=lambda sid: (created[sid] or far_future, sid))
        marker = "OLD"
        for sid in targets:
            for r in by_style[sid]:
                has_length = bool(_alt_base_and_length(r["Product Title Alt"])[1])
                new_vt = insert_old_marker_variant_title(r["Variant Title"], r["Size"], r["Inseam Label"], has_length, marker)
                if new_vt is None:
                    logger.warning("OLD skipped: size '%s' not found in '%s' (SKU %s)", r["Size"], r["Variant Title"], r["SKU - Shopify"])
                    skipped += 1
                    continue
                r["Product Title Alt"] = insert_old_marker_alt_title(r["Product Title Alt"], r["Inseam Label"], has_length, marker)
                r["Variant Title"] = new_vt
                marked += 1
                logger.info("OLD marked (%s): SKU %s %s -> %s", reason, r["SKU - Shopify"], r["Handle"], new_vt)
    logger.info("OLD listing rule: %s row(s) marked, %s skipped", marked, skipped)


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="AYR inventory scraper")
    parser.add_argument("--dry-run", action="store_true", help="do everything but write no CSV; print a preview instead")
    args = parser.parse_args(argv)

    logger = setup_logger()
    started = time.monotonic()
    logger.info("AYR scrape started (%s)", "DRY RUN - no CSV will be written" if args.dry_run else "LIVE RUN")
    session = build_session()
    variants, product_options = fetch_graphql_products(session, logger)

    handles = list(dict.fromkeys(v.handle for v in variants))
    json_cache, pdp_cache = fetch_all_pages(session, handles, logger)

    rows = build_rows(variants, product_options, json_cache, pdp_cache, logger)
    apply_inseam_label_multi(rows, pdp_cache, logger)
    finalize_rows(rows)
    apply_old_listing_markers(rows, logger)
    plain_text_rows(rows)

    if args.dry_run:
        logger.info("DRY RUN preview: %s rows, %s handles", len(rows), len({r["Handle"] for r in rows}))
        seen: Set[Tuple[str, str, str]] = set()
        for r in rows:
            key = (r["Handle"], r["Color"], r["Inseam Label"])
            if key in seen:
                continue
            seen.add(key)
            logger.info("  %s | %s | %s | %s | %s | %s", r["Variant Title"], r["Style Name"], r["Jean Style"],
                        r["Rise Label"], r["Inseam Label Multi"], r["Promo"])
        logger.info("DRY RUN complete in %s - nothing written. Run again without --dry-run to write the CSV.",
                    fmt_duration(time.monotonic() - started))
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"AYR_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(OUTPUT_HEADERS)
        for r in rows:
            writer.writerow([r[h] for h in OUTPUT_HEADERS])
    logger.info("CSV written: %s", output_path.resolve())
    logger.info("Finished %s rows in %s", len(rows), fmt_duration(time.monotonic() - started))


if __name__ == "__main__":
    main()
