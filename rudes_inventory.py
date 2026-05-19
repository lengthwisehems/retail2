# -*- coding: utf-8 -*-
"""Rudes Denim inventory scraper — Storefront GraphQL + PDP + OCR size charts."""
from __future__ import annotations

import csv
import html
import logging
import re
import time
import warnings
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GRAPHQL_URL   = "https://rudes-jeans.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN = "58ea06b1762dd2cd2daa40fa0ec73fc7"
HOST_ROTATION = [
    "https://rudes-jeans.myshopify.com",
    "https://www.rudesdenim.com",
    "https://rudesdenim.com",
]
PDP_HOST          = "https://rudesdenim.com"
COLLECTION_HANDLE = "shop-all"
SLEEP             = 0.3
# Set True only if EasyOCR is installed and you want size-chart OCR measurements.
# Leave False (default) to use description-text measurements only (much faster).
OCR_ENABLED       = True
OCR_TARGET_SIZE   = "26"   # size column to read from the size chart image

BASE_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE   = BASE_DIR / "rudes_inventory.log"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# CSV Headers
# ---------------------------------------------------------------------------
CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At",
    "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size",
    "Rise", "Inseam", "Leg Opening", "Size Chart",
    "Price", "Compare at Price", "Available for Sale",
    "Quantity Available", "Quantity of Style",
    "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL",
    "Jean Style", "Inseam Label", "Inseam Style", "Rise Label",
    "Color - Simplified", "Color - Standardized",
]

# ---------------------------------------------------------------------------
# Excluded product title words/phrases (whole-word match, case-insensitive)
# ---------------------------------------------------------------------------
EXCLUDE_TITLE_WORDS: List[str] = [
    "Accessories", "Bag", "Bermuda", "Bermudas", "Blazers", "Blouses",
    "Bodysuits", "Button Up", "Capri", "Cardigans", "Clothing Tops", "Coat",
    "Coats & Jackets", "Core Handbags", "Crop Tops", "Denim Shorts", "Dress",
    "Dresses", "Fashion Core Handbags", "Fashion Handbags", "Handbag",
    "Hoodies", "Jacket", "Jackets", "Jogger Shorts", "Jort", "Jumpsuit", "Jumpsuits",
    "Long Sleeve", "Neck", "One-Pieces", "Outerwear", "Pant Suits", "Purse",
    "Romper", "Rompers", "Shacket", "Shipping Protection", "Shirt", "Shirts",
    "Shirts & Tops", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Suits",
    "Sweater", "Sweaters", "Sweatpant", "Sweatpants", "Sweats", "Sweatshirts",
    "Swim", "Tank", "Tank Tops", "Tee", "Top", "Tops", "Trench", "T-Shirts",
    "Vest", "Vests",
]

# ---------------------------------------------------------------------------
# Style Name removal phrases (Rudes — includes "The", no "Straight Leg")
# ---------------------------------------------------------------------------
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "Accent Hardware", "Ankle", "Belted", "Cargo", "Chap", "Coated",
    "Constructed", "Corduroy", "Crop", "Cropped", "Crushed", "Crystal",
    "Cuff", "Cuffed", "Cutoff", "Cut-out", "Darted", "Destroyed",
    "Drawstring", "Fit", "Flag", "Flap", "Flip", "Frayed Seam", "Mini",
    "Front Yoke", "Frontier", "High Rise", "High-Rise", "Inch", "Inset",
    "Jean", "Krystal", "Leather", "Lightweight", "Lo", "Long", "Low Rise",
    "Low-Rise", "Mid Rise", "Mid-Rise", "Ms.", "Pant", "Pants", "Patch",
    "Petite", "Pleated", "Pocket", "Poplin", "Retro", "Rolled Hem",
    "Seamed", "slit", "Stacked Waist", "Stacked", "Stoned", "Studded",
    "Super", "The", "Track Pant", "Trashed", "Trouser", "Ultra",
    "Vegan Leather", "vent", "V-High Rise", "Vintage",
    "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "w/ Slit Hem",
    "W/ Stud Detailing", "W/ Wide Cuff", "W/Flap", "Wax", "Welt Pocket",
    "With Cuff", "With Frayed Seam", "Zipper",
]

# ---------------------------------------------------------------------------
# Valid sizes (for option disambiguation fallback)
# ---------------------------------------------------------------------------
VALID_SIZES: Set[str] = {
    "00", "0", "2", "4", "6", "8", "10", "12", "14",
    "15 Plus", "16 Plus", "18 Plus", "20 Plus", "22 Plus",
    "23",                                          # Rudes uses 23
    "24 Plus", "24", "25", "26 Plus", "26", "27", "28 Plus",
    "29", "30 Plus", "30", "31", "32 Plus", "32",
    "XXS", "XS", "S", "M", "L", "XL", "XXL",
    "1XL", "2XL", "3XL", "4XL", "5XL",
    "00-4", "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("rudes")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for path in (LOG_FILE, OUTPUT_DIR / "rudes_inventory.log"):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(path, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
            break
        except OSError:
            pass
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


LOGGER = _setup_logger()
log = LOGGER.info


# ===========================================================================
# Utility helpers
# ===========================================================================

FRACTION_UNICODE = {
    "¼": " 1/4", "½": " 1/2", "¾": " 3/4",
    "⅛": " 1/8", "⅜": " 3/8", "⅝": " 5/8", "⅞": " 7/8",
}


def normalize_text(text: str) -> str:
    for sym, repl in FRACTION_UNICODE.items():
        text = text.replace(sym, repl)
    text = text.replace("“", '"').replace("”", '"')
    text = text.replace("‘", "'").replace("’", "'")
    text = text.replace("″", '"').replace("′", "'")
    text = text.replace(" ", " ")
    return text


def strip_html(raw: str) -> str:
    if not raw:
        return ""
    import html as _html
    text = _html.unescape(raw)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return value
    return f"{dt.month}/{dt.day}/{dt.year}"


def format_price(value) -> str:
    if value is None:
        return ""
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def to_float(v) -> Optional[float]:
    try:
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def join_tags(tags: Iterable[str]) -> str:
    import html as _html
    return ", ".join(
        _html.unescape(str(t)).strip() for t in tags if t and str(t).strip()
    )


def contains_phrase(text: str, phrase: str) -> bool:
    """Whole-word/phrase, case-insensitive."""
    if not text or not phrase:
        return False
    pattern = re.escape(phrase.strip().lower()).replace(r"\ ", r"\s+")
    return bool(re.search(
        rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", text.lower()))


def text_has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(contains_phrase(text, p) for p in phrases)


def parse_mixed_fraction(raw: str) -> Optional[float]:
    s = normalize_text(raw).strip().rstrip('"').rstrip("'").strip()
    if not s:
        return None
    m = re.fullmatch(r"(\d+)\s+(\d+)/(\d+)", s)
    if m:
        w, n, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return w + n / d if d else None
    m = re.fullmatch(r"(\d+)/(\d+)", s)
    if m:
        n, d = int(m.group(1)), int(m.group(2))
        if not d:
            return None
        # "11/12" in descriptions means a measurement range; take the higher value
        if n >= 5:
            return float(d)
        return n / d
    try:
        return float(s)
    except ValueError:
        return None


def format_decimal(val: Optional[float]) -> str:
    if val is None:
        return ""
    return f"{val:.6f}".rstrip("0").rstrip(".")


def should_exclude_product(title: str) -> bool:
    t = title.lower()
    return any(contains_phrase(t, w) for w in EXCLUDE_TITLE_WORDS)


# ===========================================================================
# HTTP session
# ===========================================================================

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    })
    session.verify = False
    return session


# ===========================================================================
# GraphQL fetcher
# ===========================================================================

_GRAPHQL_QUERY = """
query RudesShopAll($cursor: String) {
  collection(handle: "shop-all") {
    products(first: 250, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        createdAt
        publishedAt
        productType
        tags
        vendor
        onlineStoreUrl
        description
        featuredImage { url }
        variants(first: 250) {
          nodes {
            id
            title
            availableForSale
            price { amount }
            compareAtPrice { amount }
            barcode
            sku
            selectedOptions { name value }
          }
        }
      }
    }
  }
}
"""


def fetch_graphql_products(session: requests.Session) -> List[Dict]:
    products: Dict[str, Dict] = {}
    cursor = None
    page = 0
    while True:
        page += 1
        variables: Dict = {"cursor": cursor} if cursor else {}
        try:
            resp = session.post(
                GRAPHQL_URL,
                headers={
                    "X-Shopify-Storefront-Access-Token": GRAPHQL_TOKEN,
                    "Content-Type": "application/json",
                },
                json={"query": _GRAPHQL_QUERY, "variables": variables},
                timeout=45,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            LOGGER.error("GraphQL failed (page %s): %s", page, exc)
            break
        payload = resp.json()
        if payload.get("errors"):
            fatal = [e for e in payload["errors"]
                     if "ACCESS_DENIED" not in str(
                         e.get("extensions", {}).get("code", ""))]
            if fatal:
                LOGGER.error("GraphQL fatal errors: %s", fatal)
                break
        data = (payload.get("data") or {}).get("collection") or {}
        if not data:
            LOGGER.warning("No collection data on page %s", page)
            break
        block = data.get("products") or {}
        for node in block.get("nodes") or []:
            handle = node.get("handle", "")
            if handle and handle not in products:
                products[handle] = node
        page_info = block.get("pageInfo") or {}
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        log("GraphQL page %s: %s products", page, len(products))
        time.sleep(SLEEP)
    log("GraphQL complete: %s unique products", len(products))
    return list(products.values())


# ===========================================================================
# products.json handle list (dedup reference)
# ===========================================================================

def fetch_products_json_handles(session: requests.Session) -> Set[str]:
    handles: Set[str] = set()
    for host in HOST_ROTATION:
        for page in range(1, 20):
            url = f"{host}/collections/{COLLECTION_HANDLE}/products.json"
            try:
                resp = session.get(
                    url, params={"limit": 250, "page": page}, timeout=30)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
                prods = resp.json().get("products") or []
                if not prods:
                    break
                for p in prods:
                    h = (p.get("handle") or "").strip()
                    if h:
                        handles.add(h)
                time.sleep(SLEEP)
            except Exception as exc:
                LOGGER.warning("products.json page %s from %s: %s",
                               page, host, exc)
                break
        if handles:
            log("products.json: %s handles from %s", len(handles), host)
            break
    return handles


# ===========================================================================
# PDP helpers
# ===========================================================================

_RE_TRIPLE_UNDERSCORE = re.compile(
    r"https?://[^\"'<> )]*___[^\"'<> )]+", re.IGNORECASE)
_RE_CDN_FILES_WEBP = re.compile(
    r"https://cdn\.shopify\.com/s/files/1/0792/0563/0243/files/"
    r"[^\"'<> )]+\.webp(?:\?[^\"'<> )]*)?",
    re.IGNORECASE,
)
_RE_VARIANT_BLOCK = re.compile(
    r"variants\s*:\s*\[(?P<body>.*?)\]\s*,?\s*\}", re.S)
_RE_VARIANT_OBJ = re.compile(
    r"\{[^{}]*?\bid\s*:\s*(?P<id>\d+)[^{}]*?"
    r"\bquantity\s*:\s*(?P<qty>-?\d+)[^{}]*?\}",
    re.S,
)


def extract_restock_quantities(html_text: str) -> Dict[str, int]:
    if "_ReStockConfig.product" not in html_text:
        return {}
    m = _RE_VARIANT_BLOCK.search(html_text)
    if not m:
        return {}
    return {
        mo.group("id"): int(mo.group("qty"))
        for mo in _RE_VARIANT_OBJ.finditer(m.group("body"))
    }


def has_size_chart_link(pdp_html: str) -> bool:
    try:
        soup = BeautifulSoup(pdp_html, "html.parser")
        link = soup.find("a", class_="product__info__link")
        return link is not None and "size chart" in link.get_text(
            strip=True).lower()
    except Exception:
        return (bool(re.search(r"product__info__link", pdp_html, re.I))
                and bool(re.search(r"size\s+chart", pdp_html, re.I)))


def find_size_chart_url(html_text: str) -> Optional[str]:
    m = _RE_TRIPLE_UNDERSCORE.findall(html_text)
    if m:
        return m[0]
    m = _RE_CDN_FILES_WEBP.findall(html_text)
    return m[0] if m else None


def check_leg_opening_laying_flat(pdp_html: str) -> bool:
    return bool(re.search(
        r"Leg\s+Opening\s*:\s*[\d./\s½¼¾]+"
        r"[\"″″]?\s*\(Laying\s+Flat\)",
        pdp_html, re.I,
    ))


def extract_pdp_description(pdp_html: str) -> str:
    if not pdp_html:
        return ""
    try:
        soup = BeautifulSoup(pdp_html, "html.parser")
        desc = soup.find(
            "div",
            class_=lambda c: c and "product__block__description" in c and "rte" in c,
        )
        if not desc:
            desc = soup.find("div", class_="product__block__description")
        if not desc:
            desc = soup.find(
                "div", class_=re.compile(r"product.*description", re.I))
        if desc:
            text = desc.get_text(" ", strip=True)
            return re.sub(r"\s+", " ", normalize_text(text)).strip()
        meta = soup.find("meta", {"name": "description"})
        if meta:
            import html as _html
            return _html.unescape(meta.get("content", ""))
    except Exception as exc:
        LOGGER.warning("PDP description: %s", exc)
    return ""


def extract_measures_from_text(text: str) -> Tuple[str, str, str]:
    norm = normalize_text(text)

    def grab(patterns: List[str]) -> str:
        for pat in patterns:
            m = re.search(pat, norm, re.IGNORECASE)
            if m:
                raw = m.group(1).split("|")[0].strip()
                val = parse_mixed_fraction(raw)
                if val is not None:
                    return format_decimal(val)
        return ""

    rise = grab([
        r'(?:Front\s+)?Rise\s*:\s*([0-9][^,;|<\n"]*)',
        r'\|\s*Rise\s+([0-9][^,;|<\n"]*)',
    ])
    inseam = grab([
        r'Inseam\s*:\s*([0-9][^,;|<\n"]*)',
        r':\s*Inseam\s+([0-9][^,;|<\n"]*)',
        r'Inseam\s+([0-9][^,;|<\n"]*)',
        r'\|\s*Inseam\s+([0-9][^,;|<\n"]*)',
    ])
    leg = grab([
        r'Leg\s+Opening\s*:\s*([0-9][^,;|<\n"(]*)',
        r'Leg\s+Openning\s*:\s*([0-9][^,;|<\n"(]*)',
    ])
    return rise, inseam, leg


# ---------------------------------------------------------------------------
# OCR integration (from rudes_sizechart_ocr)
# ---------------------------------------------------------------------------
try:
    from rudes_sizechart_ocr import ocr_size_chart as _ocr_size_chart  # type: ignore
    _OCR_AVAILABLE = True
    log("rudes_sizechart_ocr: OCR available")
except ImportError:
    _OCR_AVAILABLE = False
    LOGGER.warning("rudes_sizechart_ocr not importable; OCR disabled")


def get_measurements(
    session: requests.Session,
    pdp_html: str,
    description: str,
    unique_sizes: List[str],
    ocr_cache: Dict[str, Tuple[str, str, str]],   # keyed by url only
) -> Tuple[Dict[str, Tuple[str, str, str]], str]:
    """Return ({size: (rise, inseam, leg)}, size_chart_url).
    OCR runs at most once per unique image URL (for OCR_TARGET_SIZE='26').
    """
    desc_meas = extract_measures_from_text(description)
    size_chart_url = ""

    if pdp_html and has_size_chart_link(pdp_html):
        size_chart_url = find_size_chart_url(pdp_html) or ""

    if not size_chart_url or not _OCR_AVAILABLE or not OCR_ENABLED:
        return {s: desc_meas for s in unique_sizes}, size_chart_url

    # OCR once per image URL
    if size_chart_url not in ocr_cache:
        log("OCR: reading size chart for target size %s — %s",
            OCR_TARGET_SIZE, size_chart_url[-70:])
        r, i, lo = _ocr_size_chart(
            session, size_chart_url, OCR_TARGET_SIZE, LOGGER)
        ocr_cache[size_chart_url] = (
            r or desc_meas[0],
            i or desc_meas[1],
            lo or desc_meas[2],
        )
    meas = ocr_cache[size_chart_url]
    return {s: meas for s in unique_sizes}, size_chart_url


# ===========================================================================
# Color and Size extraction
# ===========================================================================

def extract_color_and_size(v: Dict) -> Tuple[str, str]:
    """Return (color, size) from selectedOptions.
    Prefers explicit option names; falls back to VALID_SIZES check."""
    opts = v.get("selectedOptions") or []
    by_name: Dict[str, str] = {
        o["name"].lower(): o["value"].strip() for o in opts if o
    }
    # Use named options if both present
    color_named = by_name.get("color", "")
    size_named  = by_name.get("size", "")
    if color_named and size_named:
        return color_named, size_named
    # Fallback: positional + VALID_SIZES
    opt1 = opts[0]["value"].strip() if opts else ""
    opt2 = opts[1]["value"].strip() if len(opts) > 1 else ""
    o1_size = opt1 in VALID_SIZES
    o2_size = opt2 in VALID_SIZES
    if o1_size and not o2_size:
        return opt2, opt1
    if o2_size and not o1_size:
        return opt1, opt2
    return opt1, opt2


# ===========================================================================
# Product Type
# ===========================================================================

_WASH_PHRASES = (
    "medium light wash", "light to medium wash", "medium to light wash",
    "light-to-medium wash", "medium-to-light wash", "medium-light wash",
    "light-medium wash", "light/medium wash", "medium/light wash",
    "light medium wash", "medium to dark wash", "dark to medium wash",
    "dark-to-medium wash", "medium-to-dark wash", "dark medium wash",
    "medium/dark wash", "dark/medium wash", "medium-dark wash",
    "dark-medium wash", "dark wash", "black wash", "blue wash",
    "indigo wash", "light vintage wash", "ecru wash", "white wash",
    "ivory wash", "grey wash", "mid blue wash", "mid-blue wash",
    "medium stone wash", "stone washed", "vintage washed", "medium wash",
)


def derive_product_type(title: str, description: str) -> str:
    t = title.lower()
    d = description.lower()
    if text_has_any(t, ("jean", "jeans")):
        return "Jeans"
    if text_has_any(d, ("jean", "jeans")):
        return "Jeans"
    if contains_phrase(d, "denim") and contains_phrase(d, "inseam"):
        return "Jeans"
    if text_has_any(d, _WASH_PHRASES):
        return "Jeans"
    if contains_phrase(d, "blue") and contains_phrase(d, "inseam"):
        return "Jeans"
    return "Pants"


# ===========================================================================
# Style Name
# ===========================================================================

def derive_style_name_base(product_title: str) -> str:
    """Steps 1+2: replace '-' with space, then strip styling words."""
    # Step 1
    text = product_title.replace("-", " ")
    text = text.replace('"', " ").replace("“", " ").replace("”", " ")
    text = re.sub(r"\b\d+\b", " ", text)
    # Step 2: remove phrases longest-first
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        norm = phrase.replace("-", " ")
        if norm.endswith("."):
            text = re.sub(
                rf"\b{re.escape(norm)}(?=\s|$)", " ", text,
                flags=re.IGNORECASE)
        else:
            text = re.sub(
                rf"\b{re.escape(norm)}\b", " ", text,
                flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    # Clean dangling single-letter suffix (e.g. trailing "V")
    text = re.sub(r"(?<!\w)[A-Z]\s*$", "", text).strip()
    return re.sub(r"\s+", " ", text).strip()


# ===========================================================================
# Jean Style
# ===========================================================================

def _straight_bucket(leg_opening: str) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15:
        return "Straight from Knee"
    if lo <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def derive_jean_style(
    title: str, description: str, leg_opening: str, tags_str: str
) -> str:
    t = title.lower()
    d = description.lower()
    tg = tags_str.lower()

    # Step 1: title
    if text_has_any(t, ("barrel", "barrell", "bowed", "bow leg",
                         "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if text_has_any(t, ("tapered", "relaxed skinny", "mom")):
        return "Tapered"
    if contains_phrase(t, "baggy"):
        return "Baggy"
    if contains_phrase(t, "flare"):
        return "Flare"
    if text_has_any(t, ("bootcut", "boot-cut", "boot")):
        return "Bootcut"
    if contains_phrase(t, "skinny"):
        return "Skinny"
    if text_has_any(t, ("wide leg", "wide-leg", "trouser")):
        return "Wide Leg"
    if contains_phrase(t, "boyfriend"):
        return "Boyfriend"
    if contains_phrase(t, "cigarette"):
        return "Straight from Knee"
    if contains_phrase(t, "straight"):
        if text_has_any(d, ("classic straight-leg", "slim straight",
                             "slim-straight", "classic straight fit",
                             "cigarette")):
            return "Straight from Knee"
        if text_has_any(d, ("relaxed straight-leg", "loose",
                             "relaxed yet polished straight leg", "relaxed")):
            return "Straight from Thigh"
        return _straight_bucket(leg_opening) or "Straight from Knee/Thigh"

    # Step 2: description
    if text_has_any(d, ("barrel", "barrell", "bowed", "bow leg",
                         "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if contains_phrase(d, "skinny"):
        return "Skinny"
    if contains_phrase(d, "baggy"):
        return "Baggy"
    if text_has_any(d, ("flare", "balanced silhouette")):
        return "Flare"
    if text_has_any(d, ("bootcut", "boot-cut", "boot cut")):
        return "Bootcut"
    if text_has_any(d, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(d, ("wide leg", "wide-leg", "palazzo")):
        return "Wide Leg"
    if contains_phrase(d, "straight"):
        if text_has_any(d, ("relaxed straight-leg", "loose",
                             "relaxed yet polished straight leg", "relaxed")):
            return "Straight from Thigh"
        return _straight_bucket(leg_opening) or "Straight from Knee/Thigh"

    # Step 3: title-no-styling inference → post-processing

    # Step 4: tags
    if text_has_any(tg, ("filter_style_barrel", "barrel", "barrell",
                          "bowed", "bow leg", "stovepipe", "stove-pipe",
                          "horseshoe")):
        return "Barrel"
    if text_has_any(tg, ("filter_style_skinny", "filter_style_superskinny",
                          "skinny")):
        return "Skinny"
    if text_has_any(tg, ("filter_style_flare", "flare")):
        return "Flare"
    if text_has_any(tg, ("filter_style_boot", "bootcut", "boot cut",
                          "boot-cut")):
        return "Bootcut"
    if text_has_any(tg, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(tg, ("filter_style_wide", "wide leg", "wide-leg",
                          "wideleg", "palazzo")):
        return "Wide Leg"
    if contains_phrase(tg, "filter_style_cigarette"):
        return "Straight from Knee"
    if text_has_any(tg, ("straight", "filter_style_straight")):
        return _straight_bucket(leg_opening) or ""
    if contains_phrase(tg, "baggy"):
        return "Baggy"
    if contains_phrase(tg, "boyfriend"):
        return "Boyfriend"

    return ""   # Step 5: leave blank


# ===========================================================================
# Inseam Label
# ===========================================================================

def derive_inseam_label(
    title: str, size: str, jean_style: str, inseam: str
) -> str:
    t = title.lower()
    s = (size or "").lower()
    if "petite" in t or s.endswith("p"):
        return "Petite"
    non_taper_long = {
        "Barrel", "Bootcut", "Flare", "Wide Leg", "Straight from Thigh",
        "Baggy", "Boyfriend", "Straight from Knee/Thigh",
    }
    taper_styles = {"Skinny", "Tapered", "Straight from Knee"}
    inseam_val = to_float(inseam)
    if jean_style in non_taper_long and inseam_val is not None and inseam_val >= 33:
        return "Long"
    if jean_style in taper_styles and inseam_val is not None and inseam_val >= 30:
        return "Long"
    return "Regular"


# ===========================================================================
# Inseam Style
# ===========================================================================

_NON_TAPER = {
    "Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
    "Boyfriend", "Baggy", "Flare", "Straight from Thigh",
}
_TAPER = {"Tapered", "Skinny", "Straight from Knee"}


def derive_inseam_style(jean_style: str, inseam_label: str, inseam: str) -> str:
    val = to_float(inseam)
    if val is None:
        return ""
    is_petite = inseam_label == "Petite"
    if jean_style in _NON_TAPER or not jean_style:
        if is_petite:
            if val <= 25:  return "Cropped"
            if val < 28:   return "Ankle"
            return "Full Length"
        if val <= 27:  return "Cropped"
        if val < 30:   return "Ankle"
        return "Full Length"
    if jean_style in _TAPER:
        if is_petite:
            return "Cropped" if val <= 26 else "Full Length"
        return "Cropped" if val <= 27 else "Full Length"
    return ""


# ===========================================================================
# Rise Label
# ===========================================================================

def derive_rise_label(
    title: str, description: str, tags_str: str, rise: str
) -> str:
    t = title.lower()
    # Step 1: title
    if text_has_any(t, ("super low rise", "super low-rise", "ultra low rise",
                         "ultra low-rise", "super low waist", "super low-waist",
                         "ultra low waist", "ultra low-waist")):
        return "Ultra Low"
    if text_has_any(t, ("super high rise", "super high-rise", "ultra high rise",
                         "ultra high-rise", "super high waist", "super high-waist",
                         "ultra high waist", "ultra high-waist")):
        return "Ultra High"
    if text_has_any(t, ("mid-rise", "mid rise")):
        return "Mid"
    if text_has_any(t, ("low-rise", "low rise")):
        return "Low"
    if text_has_any(t, ("high-rise", "high rise")):
        return "High"

    # Step 2: description
    d = description.lower()
    if text_has_any(d, ("rise: super low", "rise: ultra low",
                         "rise - super low", "rise - ultra low",
                         "super low rise", "super low-rise",
                         "ultra low rise", "ultra low-rise",
                         "super low waist", "super low-waist",
                         "ultra low waist", "ultra low-waist")):
        return "Ultra Low"
    if text_has_any(d, ("rise: super high", "rise: ultra high",
                         "rise - super high", "rise - ultra high",
                         "super high rise", "super high-rise",
                         "ultra high rise", "ultra high-rise",
                         "super high waist", "super high-waist",
                         "ultra high waist", "ultra high-waist")):
        return "Ultra High"
    if text_has_any(d, ("rise: mid", "rise - mid", "mid-rise", "mid rise")):
        # Conflict: desc has both High and Mid → defer to rise value
        if text_has_any(d, ("high rise", "high-rise")):
            rv = to_float(rise)
            if rv is not None:
                return "High" if rv >= 12 else "Mid"
        return "Mid"
    if text_has_any(d, ("rise: low", "rise - low", "low-rise", "low rise",
                         "hip-hugging fit", "sit comfortably on your hips",
                         "low on the hip", "low on the waist")):
        return "Low"
    if text_has_any(d, ("rise: high", "rise - high", "high-rise", "high rise",
                         "high waist", "high-waist", "high waisted",
                         "high-waisted", "high on the hip", "high on the waist",
                         "elevated waistline", "elevated, cinched waistline")):
        return "High"

    # Step 3: tags
    tg = tags_str.lower()
    if text_has_any(tg, ("rise: super low", "super low", "rise: ultra low")):
        return "Ultra Low"
    if text_has_any(tg, ("rise: super high", "super high", "ultra rise",
                          "rise: ultra high")):
        return "Ultra High"
    if text_has_any(tg, ("rise: high", "high rise")):
        return "High"
    if text_has_any(tg, ("rise: mid", "mid rise")):
        return "Mid"
    if text_has_any(tg, ("rise: low", "low rise")):
        return "Low"

    # Step 4: style-name inference → post-processing
    return ""


# ===========================================================================
# Color - Standardized
# ===========================================================================

_COLOR_STD_COLOR_MAP = [
    (("animal print", "leopard", "snake", "camo"), "Animal Print"),
    (("blue", "bleu", "blues", "navy", "indigo"),  "Blue"),
    (("brown", "cinnamon", "coffee", "espresso"),  "Brown"),
    (("green", "olive", "cypress", "sage"),        "Green"),
    (("grey", "gray"),                             "Gray"),
    (("orange",),                                  "Orange"),
    (("pink", "blush", "coral"),                   "Pink"),
    (("purple", "violet"),                         "Purple"),
    (("red", "wine", "burgundy"),                  "Red"),
    (("tan", "sand", "sable", "beige", "khaki"),   "Tan"),
    (("white", "ecru", "egret", "cream", "crème",
      "blizzard", "ivory", "parchment", "blanc"),  "White"),
    (("yellow",),                                  "Yellow"),
    (("black", "noir", "onyx", "raven"),           "Black"),
    (("print",),                                   "Print"),
]

_COLOR_STD_DESC_MAP = [
    (("animal print", "leopard", "snake"),         "Animal Print"),
    (("brown",),                                   "Brown"),
    (("green", "olive", "cypress", "sage"),        "Green"),
    (("grey", "gray", "smoke"),                    "Gray"),
    (("orange",),                                  "Orange"),
    (("pink",),                                    "Pink"),
    (("purple", "maroon", "violet"),               "Purple"),
    (("red", "wine", "burgundy"),                  "Red"),
    (("tan", "beige", "khaki"),                    "Tan"),
    (("white", "ecru", "pearly", "cream"),         "White"),
    (("yellow",),                                  "Yellow"),
    (("blue", "navy", "indigo"),                   "Blue"),
    (("black", "washed-black"),                    "Black"),
    (("print", "stripes"),                         "Print"),
]

_BLUE_WASH_DESC_PHRASES = (
    "dark base", "acid wash", "acid-wash", "dark rinse",
    "dark stretch denim", "dark wash", "dark washed",
    "dark vintage wash", "dark vintage inspired wash",
    "rich dark base", "rich, dark base", "medium base", "medium wash",
    "medium vintage wash", "medium rinse", "medium washed",
    "medium vintage inspired wash", "a lighter, spring-ready wash",
    "a lighter, summer-ready wash", "light base", "light wash",
    "light vintage wash", "light rinse", "light washed",
    "light vintage inspired wash", "season-ready wash",
    "medium-dark", "deep yet tranquil hue", "medium-light",
)


def derive_color_standardized(color: str, description: str) -> str:
    c = color.lower()
    for keys, out in _COLOR_STD_COLOR_MAP:
        if text_has_any(c, keys):
            return out
    d = description.lower()
    for keys, out in _COLOR_STD_DESC_MAP:
        if text_has_any(d, keys):
            return out
    if text_has_any(d, _BLUE_WASH_DESC_PHRASES):
        return "Blue"
    return ""


# ===========================================================================
# Color - Simplified
# ===========================================================================

def derive_color_simplified(color: str, description: str, standardized: str) -> str:
    s = standardized.lower()
    c = color.lower()
    d = description.lower()
    # Step 1: from Color - Standardized
    if text_has_any(s, ("black", "brown")):
        return "Dark"
    if text_has_any(s, ("white", "tan")):
        return "Light"
    # Step 2: from Color
    if text_has_any(c, ("wine", "burgundy", "navy", "dark",
                         "hunter green", "deep", "midnight")):
        return "Dark"
    if text_has_any(c, ("pastel", "cream", "moonwashed", "light")):
        return "Light"
    if text_has_any(c, ("medium", "mid")):
        return "Medium"
    # Step 3: description
    if text_has_any(d, ("medium light", "light to medium", "medium to light",
                         "light-to-medium", "medium-to-light", "medium-light",
                         "light-medium", "light/medium", "medium/light",
                         "light medium")):
        return "Light to Medium"
    if text_has_any(d, ("medium to dark", "dark to medium", "dark-to-medium",
                         "medium-to-dark", "dark medium", "medium/dark",
                         "dark/medium", "medium-dark", "dark-medium")):
        return "Medium to Dark"
    if text_has_any(d, ("mid blue", "mid-blue", "medium stone wash",
                         "classic stone washed blue", "vintage washed blue",
                         "classic vintage blue", "medium blue", "medium wash",
                         "classic blue", "medium-blue wash",
                         "mid-tone blue wash", "perfectly blended wash")):
        return "Medium"
    if text_has_any(d, ("light blue", "pale blue", "light vintage", "soft blue",
                         "soft pink", "ecru", "white", "acid wash", "acid-wash",
                         "light", "khaki", "tan", "ivory", "light gray wash",
                         "light silver-blue", "light wash", "lighter accents")):
        return "Light"
    if text_has_any(d, ("dark", "deep", "black", "wine", "burgundy",
                         "midnight blue", "forest green", "navy",
                         "complex wash", "darker", "deep yet tranquil hue",
                         "deep, luxurious wash", "deep, rich hue",
                         "rich yet subtle", "rich, deep blue",
                         "urbane grey wash")):
        return "Dark"
    return ""


# ===========================================================================
# Post-processing
# ===========================================================================

IDX = {h: i for i, h in enumerate(CSV_HEADERS)}


def _col(row: List[str], name: str) -> str:
    return row[IDX[name]]


def _set(row: List[str], name: str, val: str) -> None:
    row[IDX[name]] = val


def apply_style_name_rules(rows: List[List[str]]) -> None:
    """Steps 3.1 and 3.2 of the Style Name rules."""
    idx_pro = IDX["Product"]
    idx_sn  = IDX["Style Name"]
    idx_leg = IDX["Leg Opening"]
    idx_js  = IDX["Jean Style"]

    # Rule 1: products with the same first word of their style-name base but
    # different rest → if same leg opening, fill with the most-frequent name.
    by_first: Dict[str, List[List[str]]] = {}
    for row in rows:
        fw = (row[idx_sn].split(" ", 1)[0] if row[idx_sn] else "").strip().lower()
        if fw:
            by_first.setdefault(fw, []).append(row)

    for fw, group in by_first.items():
        if len(group) < 2:
            continue
        by_leg: Dict[str, List[List[str]]] = {}
        for r in group:
            by_leg.setdefault(r[idx_leg], []).append(r)
        for _leg_val, leg_rows in by_leg.items():
            non_mat = [r for r in leg_rows
                       if "maternity" not in r[idx_pro].lower()]
            if not non_mat:
                continue
            snames = [r[idx_sn] for r in non_mat if r[idx_sn]]
            if len(set(snames)) <= 1:
                continue
            most_common = max(set(snames), key=snames.count)
            for r in non_mat:
                r[idx_sn] = most_common

    # Rule 2: one-word style names
    for row in rows:
        sn = row[idx_sn].strip()
        if not sn or len(sn.split()) != 1:
            continue
        leg = row[idx_leg]
        # 2a: siblings whose style name starts with the same first word and
        # already has multiple words → take the most frequent
        candidates = [
            r[idx_sn] for r in rows
            if (r[idx_sn].split(" ", 1)[0] if r[idx_sn] else "").strip().lower()
               == sn.lower()
            and r[idx_leg] == leg
            and len(r[idx_sn].split()) > 1
        ]
        if candidates:
            row[idx_sn] = max(set(candidates), key=candidates.count)
            continue
        # 2b: append Jean Style label (strip "from ..." bucket suffix)
        js = row[idx_js]
        if js:
            js_label = js.split(" from ")[0].strip()
            row[idx_sn] = f"{sn} {js_label}".strip()
        # 2c: keep single word as-is


def apply_jean_style_inference(rows: List[List[str]]) -> None:
    """Step 3: infer Jean Style from siblings sharing the same PRODUCT_TITLE_NO_STYLING."""
    idx_js  = IDX["Jean Style"]
    idx_pro = IDX["Product"]

    _cache: Dict[int, str] = {}

    def stripped(i: int) -> str:
        if i not in _cache:
            _cache[i] = derive_style_name_base(rows[i][idx_pro]).lower()
        return _cache[i]

    for i, row in enumerate(rows):
        if row[idx_js]:
            continue
        my = stripped(i)
        matches = [
            rows[j][idx_js] for j in range(len(rows))
            if j != i
            and rows[j][idx_js]
            and stripped(j) == my
        ]
        if matches:
            row[idx_js] = max(set(matches), key=matches.count)


def apply_inseam_label_refresh(rows: List[List[str]]) -> None:
    for row in rows:
        _set(row, "Inseam Label", derive_inseam_label(
            _col(row, "Product"), _col(row, "Size"),
            _col(row, "Jean Style"), _col(row, "Inseam")))


def apply_inseam_style_refresh(rows: List[List[str]]) -> None:
    for row in rows:
        _set(row, "Inseam Style", derive_inseam_style(
            _col(row, "Jean Style"), _col(row, "Inseam Label"),
            _col(row, "Inseam")))


def apply_rise_label_inference(rows: List[List[str]]) -> None:
    """Step 4: infer Rise Label from siblings sharing same Style Name."""
    for row in rows:
        if _col(row, "Rise Label"):
            continue
        sn = _col(row, "Style Name")
        if not sn:
            continue
        siblings = [r for r in rows
                    if _col(r, "Style Name") == sn and _col(r, "Rise Label")]
        if not siblings:
            continue
        rv = to_float(_col(row, "Rise"))
        if rv is not None:
            best = min(
                siblings,
                key=lambda r: abs((to_float(_col(r, "Rise")) or 999) - rv),
            )
            _set(row, "Rise Label", _col(best, "Rise Label"))
        else:
            labels = [_col(r, "Rise Label") for r in siblings]
            _set(row, "Rise Label", max(set(labels), key=labels.count))


def apply_color_inference(rows: List[List[str]]) -> None:
    """Step 4 of Color rules: group inference by Color value."""
    by_color: Dict[str, List[List[str]]] = {}
    for row in rows:
        key = _col(row, "Color").strip().lower()
        if key:
            by_color.setdefault(key, []).append(row)
    for _, group in by_color.items():
        stds  = [_col(r, "Color - Standardized") for r in group
                 if _col(r, "Color - Standardized")]
        simps = [_col(r, "Color - Simplified") for r in group
                 if _col(r, "Color - Simplified")]
        best_std  = max(set(stds),  key=stds.count)  if stds  else ""
        best_simp = max(set(simps), key=simps.count) if simps else ""
        for r in group:
            if not _col(r, "Color - Standardized") and best_std:
                _set(r, "Color - Standardized", best_std)
            if not _col(r, "Color - Simplified") and best_simp:
                _set(r, "Color - Simplified", best_simp)


# ===========================================================================
# Main scraper
# ===========================================================================

class RudesScraper:
    def __init__(self) -> None:
        self.session   = make_session()
        self._pdp_cache: Dict[str, str] = {}
        self._ocr_cache: Dict[str, Tuple[str, str, str]] = {}

    def _get_pdp_html(self, handle: str) -> str:
        if handle not in self._pdp_cache:
            html_text = ""
            for host in [PDP_HOST, "https://www.rudesdenim.com",
                          "https://rudes-jeans.myshopify.com"]:
                url = f"{host}/products/{handle}"
                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.ok:
                        html_text = resp.text
                        break
                except Exception as exc:
                    LOGGER.warning("PDP %s from %s: %s", handle, host, exc)
            self._pdp_cache[handle] = html_text
            time.sleep(SLEEP)
        return self._pdp_cache[handle]

    def build_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []

        log("Fetching GraphQL products from 'shop-all'...")
        products = fetch_graphql_products(self.session)

        log("Fetching products.json for dedup check...")
        json_handles = fetch_products_json_handles(self.session)
        if json_handles:
            log("products.json: %s handles; GraphQL: %s",
                len(json_handles), len(products))

        log("Processing %s products...", len(products))
        for idx, product in enumerate(products, start=1):
            handle = product.get("handle", "")
            if not handle:
                continue

            title = product.get("title") or ""
            if should_exclude_product(title):
                log("Skip (excluded): %s", title)
                continue

            product_id   = (product.get("id") or "").replace(
                "gid://shopify/Product/", "")
            published_at = format_date(product.get("publishedAt"))
            created_at   = format_date(product.get("createdAt"))
            tags         = product.get("tags") or []
            tags_str     = join_tags(tags)
            vendor       = product.get("vendor") or ""
            online_url   = (product.get("onlineStoreUrl")
                             or f"{PDP_HOST}/products/{handle}")
            image_url    = ((product.get("featuredImage") or {}).get("url") or "")

            variants = ((product.get("variants") or {}).get("nodes") or [])
            if not variants:
                log("No variants for %s, skipping", handle)
                continue

            pdp_html = self._get_pdp_html(handle)

            description = extract_pdp_description(pdp_html)
            if not description:
                description = strip_html(product.get("description") or "")

            if contains_phrase(description.lower(), "jumpsuit"):
                log("Skip (jumpsuit in description): %s", handle)
                continue

            product_type = derive_product_type(title, description)

            unique_sizes = list({
                extract_color_and_size(v)[1] for v in variants
                if extract_color_and_size(v)[1]
            })

            meas_by_size, size_chart_url = get_measurements(
                self.session, pdp_html, description,
                unique_sizes, self._ocr_cache,
            )

            laying_flat = check_leg_opening_laying_flat(pdp_html)
            qty_map     = extract_restock_quantities(pdp_html)

            style_qty = sum(
                qty_map.get(
                    (v.get("id") or "").replace("gid://shopify/ProductVariant/", ""),
                    0,
                )
                for v in variants
            )

            style_name = derive_style_name_base(title)

            for v in variants:
                v_id_full   = v.get("id") or ""
                sku_shopify = v_id_full.replace("gid://shopify/ProductVariant/", "")
                sku_brand   = v.get("sku") or ""
                barcode     = v.get("barcode") or ""
                available   = "TRUE" if v.get("availableForSale") else "FALSE"

                color, size = extract_color_and_size(v)

                fallback = meas_by_size.get("26", ("", "", ""))
                raw_rise, raw_inseam, raw_leg = (
                    meas_by_size.get(size) or fallback
                )

                # Laying-flat doubling (product-level flag)
                leg_opening = raw_leg
                if leg_opening and laying_flat:
                    lv = to_float(leg_opening)
                    if lv is not None:
                        leg_opening = format_decimal(lv * 2)

                jean_style = derive_jean_style(
                    title, description, leg_opening, tags_str)

                # Wide Leg / Baggy < 15 doubling
                if leg_opening and jean_style in ("Wide Leg", "Baggy"):
                    lv = to_float(leg_opening)
                    if lv is not None and lv < 15:
                        leg_opening = format_decimal(lv * 2)
                        jean_style  = derive_jean_style(
                            title, description, leg_opening, tags_str)

                inseam_label = derive_inseam_label(
                    title, size, jean_style, raw_inseam)
                inseam_style = derive_inseam_style(
                    jean_style, inseam_label, raw_inseam)
                rise_label   = derive_rise_label(
                    title, description, tags_str, raw_rise)
                color_std    = derive_color_standardized(color, description)
                color_simp   = derive_color_simplified(
                    color, description, color_std)

                variant_title = " - ".join(p for p in [title, color, size] if p)

                price_obj  = v.get("price") or {}
                price = format_price(
                    price_obj.get("amount")
                    if isinstance(price_obj, dict) else price_obj)
                cmp_obj = v.get("compareAtPrice")
                compare_at = format_price(
                    (cmp_obj or {}).get("amount")
                    if isinstance(cmp_obj, dict) else cmp_obj)

                qty_avail_str = (
                    str(qty_map[sku_shopify])
                    if sku_shopify in qty_map else ""
                )

                row: List[str] = [""] * len(CSV_HEADERS)
                _set(row, "Style Id",             product_id)
                _set(row, "Handle",               handle)
                _set(row, "Published At",         published_at)
                _set(row, "Created At",           created_at)
                _set(row, "Product",              title)
                _set(row, "Style Name",           style_name)
                _set(row, "Product Type",         product_type)
                _set(row, "Tags",                 tags_str)
                _set(row, "Vendor",               vendor)
                _set(row, "Description",          description)
                _set(row, "Variant Title",        variant_title)
                _set(row, "Color",                color)
                _set(row, "Size",                 size)
                _set(row, "Rise",           raw_rise)
                _set(row, "Inseam",               raw_inseam)
                _set(row, "Leg Opening",          leg_opening)
                _set(row, "Size Chart",           size_chart_url)
                _set(row, "Price",                price)
                _set(row, "Compare at Price",     compare_at)
                _set(row, "Available for Sale",   available)
                _set(row, "Quantity Available",   qty_avail_str)
                _set(row, "Quantity of Style",    str(style_qty) if style_qty else "")
                _set(row, "SKU - Shopify",        sku_shopify)
                _set(row, "SKU - Brand",          sku_brand)
                _set(row, "Barcode",              barcode)
                _set(row, "Image URL",            image_url)
                _set(row, "SKU URL",              online_url)
                _set(row, "Jean Style",           jean_style)
                _set(row, "Inseam Label",         inseam_label)
                _set(row, "Inseam Style",         inseam_style)
                _set(row, "Rise Label",           rise_label)
                _set(row, "Color - Simplified",   color_simp)
                _set(row, "Color - Standardized", color_std)
                rows.append(row)

            if idx % 5 == 0 or idx == len(products):
                log("Progress: %s/%s products (%s rows)",
                    idx, len(products), len(rows))

        log("Post-processing: Jean Style inference...")
        apply_jean_style_inference(rows)
        log("Post-processing: Style Name rules...")
        apply_style_name_rules(rows)
        log("Post-processing: Inseam Label refresh...")
        apply_inseam_label_refresh(rows)
        log("Post-processing: Inseam Style refresh...")
        apply_inseam_style_refresh(rows)
        log("Post-processing: Rise Label inference...")
        apply_rise_label_inference(rows)
        log("Post-processing: Color inference...")
        apply_color_inference(rows)

        return rows

    def write_csv(self, rows: List[List[str]]) -> Path:
        ts   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = OUTPUT_DIR / f"RUDES_{ts}.csv"
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADERS)
            writer.writerows(rows)
        log("Wrote %s rows to %s", len(rows), path)
        return path

    def run(self) -> Path:
        log("=== Rudes inventory run started ===")
        rows = self.build_rows()
        path = self.write_csv(rows)
        log("=== Run complete: %s ===", path)
        print(f"Done — {path}")
        return path


def main() -> None:
    RudesScraper().run()


if __name__ == "__main__":
    main()
