#!/usr/bin/env python3
"""Pistola Denim inventory scraper  -  GraphQL + PDP + Yotpo edition.

Data sources:
  - Shopify Storefront GraphQL API: product/variant core data
  - Shopify REST /products/{handle}.json: inventory quantities per variant
  - PDP HTML scraping: measurements (Rise, Inseam, Leg Opening) and Stretch
  - Yotpo API: Review AVG and Review Count
"""
from __future__ import annotations

import csv
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry

disable_warnings(InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BRAND = "PISTOLA"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / f"{BRAND.lower()}_inventory_rebuy.log"

GRAPHQL_URL = "https://pistola-denim.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKENS = [
    "234bc5fb0739b70c70baf489a06352ba",
    "a5f419fa0d495c4bb4be279b6dcadf23",
]

HOST_ROTATION = [
    "https://pistola-denim.myshopify.com",
    "https://www.pistoladenim.com",
    "https://pistoladenim.com",
]
PDP_HOST = "https://www.pistoladenim.com"
COLLECTION_HANDLES = ["all-denim", "sale"]

YOTPO_APP_KEY = "1maBA6ctnRD1GNlOziHctkBiWGhzUrBXY5Q2ErZn"

REBUY_WIDGET_ID = "28256"
REBUY_API_KEY = "d894da4c5b0317da53860dda0d297d9d8057c12e"
REBUY_API_BASE = "https://rebuyengine.com/api/v1/custom/id"

EXCLUDED_PRODUCT_TYPES = {
    "TEES & TANKS", "KNIT TOPS", "ONE PIECE", "DENIM SHORTS",
    "SWEATER", "DRESS", "DENIM JACKETS", "WOVEN TOPS", "SWEATERS",
    "RTW SHORTS", "RTW TOPS", "RTW DRESSES", "RTW ROMPERS", "RTW SKIRTS",
    "RTW JUMPSUITS", "06", "05", "09", "04",
}

# Only products whose type resolves to one of these are kept
_ALLOWED_TYPES_UPPER = {"DENIM PANTS", "01", "DENIM", "JEANS", "RTW PANTS", "11"}

CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Product",
    "Style Name", "Product Type", "Tags", "Vendor", "Description",
    "Variant Title", "Color", "Size", "Rise", "Inseam", "Leg Opening",
    "Price", "Compare at Price", "Available for Sale", "Quantity Available",
    "Quantity of style", "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL", "Jean Style", "Product Line", "Inseam Label",
    "Inseam Style", "Rise Label", "Color - Standardized", "Stretch",
    "Review AVG", "Review Count", "Wishlist count",
]

STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "Accent Hardware", "Ankle", "Belted", "Coated", "Corduroy", "Crop",
    "Crushed", "Crystal", "Cuff", "Cuffed", "Cutoff", "Cropped", "Darted",
    "Destroyed", "Drawstring", "Fit", "Flag", "Flap Pocket", "Flap", "Flip",
    "Frayed Seam", "Front Yoke", "Frontier", "High Rise", "Mid Rise",
    "Inch", "Inset", "Jean", "Krystal", "Leather", "Lightweight", "Lo",
    "Long", "Petite", "Patch", "Pocket", "Pant", "Pants", "Pleated",
    "Poplin", "Relaxed", "Rolled Hem", "Seamed", "Stacked Waist", "Stacked",
    "Studded", "Super", "Track Pant", "Trashed", "Trouser", "Ultra",
    "Vegan Leather", "Vintage", "vent", "slit",
    "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean",
    "w/ Slit Hem", "W/ Stud Detailing", "W/ Wide Cuff", "W/Flap", "Wax",
    "Welt Pocket", "With Cuff", "With Frayed Seam", "Zipper",
]

FRACTION_MAP = {"½": "1/2", "¼": "1/4", "¾": "3/4",
                "⅛": "1/8", "⅜": "3/8", "⅝": "5/8", "⅞": "7/8"}

GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 250, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        description
        createdAt
        publishedAt
        productType
        tags
        vendor
        onlineStoreUrl
        featuredImage { url }
        variants(first: 250) {
          nodes {
            id
            title
            sku
            barcode
            availableForSale
            price { amount }
            compareAtPrice { amount }
            selectedOptions { name value }
          }
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def configure_logging() -> logging.Logger:
    OUTPUT_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger(BRAND)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    try:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except OSError:
        pass
    return logger


log = configure_logging()


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        "Accept-Language": "en-US,en;q=0.9",
    })
    session.verify = False
    return session


SESSION = build_session()
_CURRENT_TOKEN = 0


def graphql_request(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    global _CURRENT_TOKEN
    for attempt in range(len(GRAPHQL_TOKENS) * 2):
        token = GRAPHQL_TOKENS[_CURRENT_TOKEN % len(GRAPHQL_TOKENS)]
        try:
            resp = SESSION.post(
                GRAPHQL_URL,
                headers={"X-Shopify-Storefront-Access-Token": token, "Content-Type": "application/json"},
                json={"query": query, "variables": variables},
                timeout=45,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("errors"):
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data.get("data", {})
        except Exception as exc:
            log.warning("GraphQL attempt %d failed (token %d): %s", attempt + 1, _CURRENT_TOKEN, exc)
            _CURRENT_TOKEN += 1
            time.sleep(1.5 ** attempt)
    raise RuntimeError("All GraphQL attempts exhausted")


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------
def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return ""


def format_price(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return ""


def join_tags(tags: Iterable[str]) -> str:
    return ", ".join(t.strip() for t in tags if t and t.strip())


def strip_gid(value: str, prefix: str) -> str:
    if value.startswith(prefix):
        return value[len(prefix):]
    return value.split("/")[-1] if "/" in value else value


def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_fraction(raw: str) -> Optional[float]:
    """Convert mixed-fraction string like '10 3/4' to 10.75."""
    text = (raw or "").strip()
    for sym, rep in FRACTION_MAP.items():
        text = text.replace(sym, rep)
    text = re.sub('[\u201c\u201d\u2033\u2032\u2018\u2019"\']', "", text)
    text = re.sub(r"\b(in|inch|inches)\b", "", text, flags=re.IGNORECASE).strip()
    if not text:
        return None
    parts = text.split()
    total = 0.0
    try:
        for p in parts:
            if "/" in p:
                num, den = p.split("/", 1)
                total += float(num) / float(den)
            else:
                total += float(p)
    except (ValueError, ZeroDivisionError):
        return None
    return total


def format_measurement(value: Optional[float]) -> str:
    if value is None:
        return ""
    s = f"{value:.6f}".rstrip("0").rstrip(".")
    return s


def extract_measurement(text: str, labels: Sequence[str], require_colon: bool = False) -> str:
    """Pull a measurement number from text after any of the given labels.

    require_colon=True prevents false matches like 'High Rise 23' when only
    searching for label 'Rise'  -  a colon must separate the label from the value.
    """
    norm = text or ""
    for sym, rep in FRACTION_MAP.items():
        norm = norm.replace(sym, " " + rep)
    norm = norm.replace("\u2018", "'").replace("\u2019", "'").replace("\u201c", '"').replace("\u201d", '"')
    colon_pat = r"\s*:\s*" if require_colon else r"\s*:?\s*"
    for label in labels:
        m = re.search(
            rf"{re.escape(label)}{colon_pat}([0-9]+(?:\s+[0-9]+/[0-9]+|/[0-9]+|\.[0-9]+)?)",
            norm, re.IGNORECASE,
        )
        if m:
            val = parse_fraction(m.group(1))
            if val is not None:
                return format_measurement(val)
    return ""


def extract_tag_value(tags: Iterable[str], prefix: str) -> str:
    pl = prefix.lower() + ":"
    for tag in tags:
        if tag.lower().startswith(pl):
            return tag[len(prefix) + 1:].strip()
    return ""


def contains_word(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    pat = re.escape(phrase.strip()).replace(r"\ ", r"\s+")
    return bool(re.search(rf"(?<![a-z0-9]){pat}(?![a-z0-9])", text.lower()))


def text_has_any(text: str, phrases: Sequence[str]) -> bool:
    return any(contains_word(text, p) for p in phrases)


def fix_smart_quotes(text: str) -> str:
    return (text or "").replace("\u201c", '"').replace("\u201d", '"').replace("\u2018", "'").replace("\u2019", "'")


# ---------------------------------------------------------------------------
# Product type resolution
# ---------------------------------------------------------------------------
def resolve_product_type(raw: str) -> Optional[str]:
    """Return canonical product type string or None to exclude."""
    upper = (raw or "").strip().upper()
    if upper not in _ALLOWED_TYPES_UPPER:
        return None
    if upper in ("DENIM PANTS", "01", "DENIM", "JEANS"):
        return "Jeans"
    if upper in ("RTW PANTS", "11"):
        return "Pants"
    return None


# ---------------------------------------------------------------------------
# Product Line from Delivery tag
# ---------------------------------------------------------------------------
def derive_product_line(tags: List[str]) -> str:
    """Extract Product Line from the first Delivery: tag. H25D3 → H25-3."""
    for tag in tags:
        if tag.startswith("Delivery: "):
            code = tag[len("Delivery: "):].strip()
            m = re.match(r"([A-Z][0-9]{2})D([0-9]+)", code, re.IGNORECASE)
            if m:
                return f"{m.group(1).upper()}-{m.group(2)}"
            return code
    return ""


# ---------------------------------------------------------------------------
# Style Name derivation
# ---------------------------------------------------------------------------
def derive_style_name_base(product_title: str) -> str:
    """Step 1+2: clean title then remove styling words.

    Step 1 removes the color suffix after any dash variant:
      'Name - Color', 'Name- Color', 'Name - Color'
    """
    parts = re.split(r"\s*[-–—]\s*", product_title or "")
    text = parts[0].strip()
    text = re.sub(r'"', " ", text)
    text = re.sub(r"\b\d+\b", " ", text)
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Jean Style
# ---------------------------------------------------------------------------
def _straight_bucket(leg_opening_str: str) -> str:
    lo = safe_float(leg_opening_str)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17.5:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def _map_jean_from_text(text: str, lo_str: str, allow_mom: bool = False) -> str:
    t = (text or "").lower()
    if not t:
        return ""
    # Step order matters  -  first match wins
    taper_words = ["balloon", "tapered", "mom"] if allow_mom else ["balloon", "tapered"]
    if text_has_any(t, taper_words):
        return "Tapered"
    if contains_word(t, "baggy"):
        return "Baggy"
    if contains_word(t, "flare"):
        return "Flare"
    if text_has_any(t, ["bootcut", "boot"]):
        return "Bootcut"
    if contains_word(t, "boyfriend"):
        return "Boyfriend"
    if contains_word(t, "skinny"):
        return "Skinny"
    if text_has_any(t, ["bowed wide leg", "wide leg", "palazzo"]):
        return "Wide Leg"
    if text_has_any(t, ["cigarette", "slim straight"]):
        return "Straight from Knee"
    if text_has_any(t, ["bowed straight", "straight"]):
        return _straight_bucket(lo_str)
    if text_has_any(t, ["barrel", "bowed", "bow leg", "stovepipe", "stove-pipe",
                         "crescent", "curved utility", "horseshoe"]):
        return "Barrel"
    return ""


def derive_jean_style(style_name: str, description: str, leg_opening: str) -> str:
    mapped = _map_jean_from_text(style_name, leg_opening, allow_mom=True)
    if mapped:
        return mapped
    mapped = _map_jean_from_text(description, leg_opening, allow_mom=False)
    return mapped


# ---------------------------------------------------------------------------
# Inseam Label
# ---------------------------------------------------------------------------
def derive_inseam_label(title: str, size: str, jean_style: str, inseam: str) -> str:
    if "petite" in (title or "").lower() or (size or "").upper().endswith("P"):
        return "Petite"
    inseam_val = safe_float(inseam)
    if inseam_val is None:
        return "Regular"
    wide_group = {"Barrel", "Bootcut", "Flare", "Wide Leg", "Straight from Thigh",
                  "Baggy", "Boyfriend", "Straight from Knee/Thigh"}
    taper_group = {"Skinny", "Tapered", "Straight from Knee"}
    if jean_style in wide_group and inseam_val >= 33:
        return "Long"
    if jean_style in taper_group and inseam_val >= 30:
        return "Long"
    return "Regular"


# ---------------------------------------------------------------------------
# Inseam Style (3-input table lookup)
# ---------------------------------------------------------------------------
NON_TAPER_STYLES = {"Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
                    "Boyfriend", "Baggy", "Flare", "Straight from Thigh"}
TAPER_STYLES = {"Tapered", "Skinny", "Straight from Knee"}


def _measurement_inseam_style(jean_style: str, inseam_label: str, inseam: str) -> str:
    val = safe_float(inseam)
    if val is None:
        return ""
    if jean_style in NON_TAPER_STYLES:
        if inseam_label == "Petite":
            return "Cropped" if val <= 25 else ("Ankle" if val < 28 else "Full Length")
        return "Cropped" if val <= 27 else ("Ankle" if val < 30 else "Full Length")
    if jean_style in TAPER_STYLES:
        if inseam_label == "Petite":
            return "Cropped" if val <= 26 else "Full Length"
        return "Cropped" if val <= 27 else "Full Length"
    return ""


def _title_inseam_style(title: str) -> str:
    t = (title or "").lower()
    if "ankle" in t:
        return "Ankle"
    if "cropped" in t or "crop" in t:
        return "Cropped"
    return ""


def _description_inseam_style(description: str, jean_style: str) -> str:
    d = (description or "").lower()
    is_taper = jean_style in TAPER_STYLES
    crop_kw = ["crop", "cropped"]
    ankle_kw = ["ankle length", "ankle opening", "at the ankle"]
    full_kw = ["full length", "full-length"]
    has_crop = text_has_any(d, crop_kw)
    has_ankle = text_has_any(d, ankle_kw)
    has_full = text_has_any(d, full_kw)
    if (has_crop and has_ankle) or contains_word(d, "cropped ankle"):
        return "Cropped" if is_taper else "Cropped Ankle"
    if has_crop:
        return "Cropped"
    if has_ankle:
        return "Full Length" if is_taper else "Ankle"
    if has_full:
        return "Full Length"
    return ""


# Lookup table: (MEASUREMENT, TITLE, DESCRIPTION) → Inseam Style
_INSEAM_STYLE_TABLE: Dict[Tuple[str, str, str], str] = {
    ("Full Length", "Cropped", "Cropped"): "Cropped",
    ("Full Length", "", "Full Length"): "Full Length",
    ("Full Length", "", "Cropped"): "Full Length",
    ("Full Length", "", ""): "Full Length",
    ("Cropped", "Cropped", "Cropped Ankle"): "Cropped",
    ("Cropped", "Cropped", "Cropped"): "Cropped",
    ("Cropped", "Cropped", ""): "Cropped",
    ("Cropped", "", "Full Length"): "Cropped",
    ("Cropped", "", "Cropped Ankle"): "Cropped",
    ("Cropped", "", "Cropped"): "Cropped",
    ("Cropped", "", ""): "Cropped",
    ("Ankle", "Cropped", "Cropped"): "Cropped",
    ("Ankle", "Ankle", "Cropped Ankle"): "Ankle",
    ("Ankle", "Ankle", "Cropped"): "Ankle",
    ("Ankle", "", "Full Length"): "Ankle",
    ("Ankle", "", "Cropped Ankle"): "Ankle",
    ("Ankle", "", "Cropped"): "Ankle",
    ("Ankle", "", ""): "Ankle",
    ("", "Cropped", "Cropped Ankle"): "Cropped",
    ("", "Ankle", "Cropped Ankle"): "Ankle",
    ("", "Ankle", ""): "Ankle",
    ("", "", "Full Length"): "Full Length",
    ("", "", "Cropped"): "Cropped",
    ("", "", "Ankle"): "Ankle",
}


def derive_inseam_style(jean_style: str, inseam_label: str, inseam: str,
                         title: str, description: str) -> str:
    m_style = _measurement_inseam_style(jean_style, inseam_label, inseam)
    t_style = _title_inseam_style(title)
    d_style = _description_inseam_style(description, jean_style)
    result = _INSEAM_STYLE_TABLE.get((m_style, t_style, d_style), "")
    if not result and m_style:
        result = m_style
    return result


# ---------------------------------------------------------------------------
# Rise Label
# ---------------------------------------------------------------------------
def derive_rise_label(title: str, description: str, tags: List[str],
                       style_name: str) -> str:
    def _check(text: str) -> str:
        t = (text or "").lower()
        if text_has_any(t, ["super low rise", "super low-rise", "ultra low rise", "ultra low-rise",
                              "super low waist", "super low-waist", "ultra low waist", "ultra low-waist"]):
            return "Ultra Low"
        if text_has_any(t, ["super high rise", "super high-rise", "ultra high rise", "ultra high-rise",
                              "super high waist", "super high-waist", "ultra high waist", "ultra high-waist"]):
            return "Ultra High"
        if text_has_any(t, ["mid-rise", "mid rise"]):
            return "Mid"
        if text_has_any(t, ["low-rise", "low rise"]):
            return "Low"
        if text_has_any(t, ["high-rise", "high rise"]):
            return "High"
        return ""

    result = _check(title)
    if result:
        return result
    result = _check(description)
    if result:
        return result
    # Step 4: from tags
    ultra_low_tags = {"Rise: Super Low", "Rise: Ultra Low"}
    ultra_high_tags = {"Rise: Super High", "Rise: Ultra High"}
    rise_tags = [t for t in tags if t.startswith("Rise:")]
    if len(rise_tags) == 1:
        tag_val = rise_tags[0]
        if tag_val in ultra_low_tags:
            return "Ultra Low"
        if tag_val in ultra_high_tags:
            return "Ultra High"
        if "Rise: High" in tag_val:
            return "High"
        if "Rise: Mid" in tag_val:
            return "Mid"
        if "Rise: Low" in tag_val:
            return "Low"
    elif len(rise_tags) > 1:
        # Multiple rise tags  -  defer to inference step (handled in post-processing)
        pass
    return ""


# ---------------------------------------------------------------------------
# Color  -  Standardized
# ---------------------------------------------------------------------------
def _color_std_from_text(text: str, whole_word_only: bool = True) -> str:
    """Map text against color rules. Returns first match."""
    t = (text or "").lower()

    def _has(phrase: str) -> bool:
        if whole_word_only:
            return contains_word(t, phrase)
        return phrase.lower() in t

    rules = [
        (["animal print", "leopard", "snake", "camo"], "Animal Print"),
        (["blue", "blues", "navy", "indigo"], "Blue"),
        (["brown", "cinnamon", "coffee", "espresso"], "Brown"),
        (["green", "olive", "cypress", "sage"], "Green"),
        (["grey", "gray"], "Gray"),
        (["orange"], "Orange"),
        (["pink", "blush", "coral"], "Pink"),
        (["print"], "Print"),
        (["purple", "violet"], "Purple"),
        (["red", "wine", "burgundy"], "Red"),
        (["tan", "beige", "khaki"], "Tan"),
        (["white", "ecru", "egret", "cream", "blizzard", "parchment", "blanc"], "White"),
        (["yellow"], "Yellow"),
        (["black", "noir", "onyx", "raven"], "Black"),
    ]
    for keywords, label in rules:
        if any(_has(k) for k in keywords):
            return label
    return ""


def _color_std_from_desc(description: str) -> str:
    d = (description or "").lower()
    rules_desc = [
        (["animal print", "leopard", "snake"], "Animal Print"),
        (["brown"], "Brown"),
        (["green", "olive", "cypress", "sage"], "Green"),
        (["grey", "gray", "smoke"], "Gray"),
        (["orange"], "Orange"),
        (["pink"], "Pink"),
        (["print"], "Print"),
        (["purple", "maroon", "violet"], "Purple"),
        (["red", "wine", "burgundy"], "Red"),
        (["tan", "beige", "khaki"], "Tan"),
        (["white", "ecru", "pearly", "cream"], "White"),
        (["yellow"], "Yellow"),
        (["blue", "navy", "indigo"], "Blue"),
        (["black", "washed-black"], "Black"),
    ]
    for keywords, label in rules_desc:
        if any(contains_word(d, k) for k in keywords):
            return label
    # Denim wash cues → Blue
    wash_cues = [
        "dark base", "acid wash", "dark rinse", "dark stretch denim", "dark wash",
        "dark washed", "dark vintage wash", "dark vintage inspired wash", "rich dark base",
        "rich, dark base", "medium base", "medium wash", "medium vintage wash",
        "medium rinse", "medium washed", "medium vintage inspired wash",
        "a lighter, spring-ready wash", "a lighter, summer-ready wash",
        "light base", "light wash", "light vintage wash", "light rinse", "light washed",
        "light vintage inspired wash", "season-ready wash", "medium-dark", "medium-light",
    ]
    if any(contains_word(d, cue) for cue in wash_cues):
        return "Blue"
    return ""


def _color_std_from_tags(tags: List[str]) -> str:
    tag_text = " ".join(tags).lower()
    rules_tags = [
        (["animal print", "leopard", "snake"], "Animal Print"),
        (["brown"], "Brown"),
        (["green"], "Green"),
        (["grey", "gray", "smoke"], "Grey"),
        (["orange"], "Orange"),
        (["pink"], "Pink"),
        (["print"], "Print"),
        (["purple"], "Purple"),
        (["red"], "Red"),
        (["tan", "beige", "khaki"], "Tan"),
        (["yellow"], "Yellow"),
        (["white", "whitedenim", "ecru"], "White"),
        (["blue", "indigo"], "Blue"),
        (["black"], "Black"),
    ]
    for keywords, label in rules_tags:
        if any(contains_word(tag_text, k) for k in keywords):
            return label
    return ""


def derive_color_standardized(color: str, description: str, tags: List[str]) -> str:
    result = _color_std_from_text(color)
    if result:
        return result
    # Check for "light/medium/dark [color-name] wash" in description → Blue
    if color:
        d_lower = (description or "").lower()
        c_escaped = re.escape(color.lower())
        if re.search(rf"\b(light|medium|dark)\s+{c_escaped}\s+wash\b", d_lower):
            return "Blue"
    result = _color_std_from_desc(description)
    if result:
        return result
    return ""  # steps 3 (peer inference) and 4 (tags) handled in post-processing


# ---------------------------------------------------------------------------
# Stretch
# ---------------------------------------------------------------------------
def derive_stretch_from_description(description: str) -> str:
    d = (description or "").lower()
    if contains_word(d, "rigid") or contains_word(d, "non-stretch"):
        return "Rigid"
    return ""


def derive_stretch_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    marker = soup.find("div", class_="stretch-scale-marker")
    if not marker:
        return ""
    style = (marker.get("style") or "").lower()
    m = re.search(r"left\s*:\s*([0-9]+(?:\.[0-9]+)?)\s*%", style)
    if not m:
        return ""
    pct = float(m.group(1))
    if pct <= 5:
        return "Rigid"
    if pct <= 55:
        return "Medium Stretch"
    return "High Stretch"


# ---------------------------------------------------------------------------
# PDP scraping
# ---------------------------------------------------------------------------
_PDP_CACHE: Dict[str, Dict[str, str]] = {}


def fetch_pdp(handle: str) -> Dict[str, str]:
    if handle in _PDP_CACHE:
        return _PDP_CACHE[handle]
    result = {"rise": "", "inseam": "", "leg_opening": "", "stretch": "", "html": ""}
    for host in [PDP_HOST] + [h for h in HOST_ROTATION if h != PDP_HOST]:
        url = f"{host.rstrip('/')}/products/{handle}"
        try:
            resp = SESSION.get(url, timeout=40)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            html = resp.text
            result["html"] = html
            soup = BeautifulSoup(html, "html.parser")

            # Build a targeted measurement string from bullet list items first;
            # this avoids false matches from product titles in the page header.
            li_texts = [li.get_text(" ") for li in soup.find_all("li")]
            li_text = " ".join(li_texts)
            all_text = soup.get_text(" ")

            # Rise: require colon so "High Rise" in product titles doesn't
            # accidentally match a nearby number.
            result["rise"] = (
                extract_measurement(li_text, ["Front Rise", "Rise"], require_colon=True)
                or extract_measurement(all_text, ["Front Rise", "Rise"], require_colon=True)
            )
            # Inseam/LO: try bullets first, fall back to full page text.
            result["inseam"] = (
                extract_measurement(li_text, ["Inseam", "Inleg"])
                or extract_measurement(all_text, ["Inseam", "Inleg"])
            )
            result["leg_opening"] = (
                extract_measurement(li_text, ["Leg Opening"])
                or extract_measurement(all_text, ["Leg Opening"])
            )
            result["stretch"] = derive_stretch_from_html(html)
            break
        except Exception as exc:
            log.debug("PDP fetch failed %s: %s", url, exc)
    _PDP_CACHE[handle] = result
    return result


# ---------------------------------------------------------------------------
# Inventory from Rebuy API
# The REST /products/{handle}.json endpoint caps inventory_quantity at 20;
# Rebuy returns actual quantities without this limit.
# ---------------------------------------------------------------------------
_REBUY_INV_CACHE: Dict[int, Dict[int, int]] = {}


def fetch_rebuy_inventory(product_id: int) -> Dict[int, int]:
    """Return {variant_id: inventory_quantity} from the Rebuy custom widget API."""
    if product_id in _REBUY_INV_CACHE:
        return _REBUY_INV_CACHE[product_id]
    mapping: Dict[int, int] = {}
    try:
        resp = SESSION.get(
            f"{REBUY_API_BASE}/{REBUY_WIDGET_ID}",
            params={
                "key": REBUY_API_KEY,
                "shopify_product_ids": str(product_id),
                "limit": 1,
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        for product in payload.get("data", []):
            if int(product.get("id", 0)) == product_id:
                for v in product.get("variants", []):
                    vid = v.get("id")
                    qty = v.get("inventory_quantity")
                    if vid is not None and qty is not None:
                        mapping[int(vid)] = int(qty)
                break
    except Exception as exc:
        log.debug("Rebuy inventory fetch failed for product %d: %s", product_id, exc)
    _REBUY_INV_CACHE[product_id] = mapping
    return mapping


# ---------------------------------------------------------------------------
# Yotpo reviews
# ---------------------------------------------------------------------------
_YOTPO_CACHE: Dict[int, Tuple[str, str]] = {}


def fetch_yotpo(product_id: int) -> Tuple[str, str]:
    """Return (average_score, total_reviews) as strings."""
    if product_id in _YOTPO_CACHE:
        return _YOTPO_CACHE[product_id]
    url = f"https://api.yotpo.com/products/{YOTPO_APP_KEY}/{product_id}/bottomline"
    try:
        resp = SESSION.get(url, timeout=20)
        resp.raise_for_status()
        bl = resp.json().get("response", {}).get("bottomline", {})
        avg = str(bl.get("average_score", "")) if bl.get("average_score") else ""
        total = str(int(bl.get("total_reviews", 0))) if bl.get("total_reviews") else ""
        result = (avg, total)
    except Exception:
        result = ("", "")
    _YOTPO_CACHE[product_id] = result
    return result


# ---------------------------------------------------------------------------
# Collection fetch
# ---------------------------------------------------------------------------
def fetch_collection_products() -> List[Dict[str, Any]]:
    """Fetch products from all-denim and sale collections, deduplicating by handle."""
    merged: Dict[str, Dict[str, Any]] = {}
    for handle in COLLECTION_HANDLES:
        cursor = None
        page = 0
        while True:
            page += 1
            data = graphql_request(GRAPHQL_QUERY, {"handle": handle, "cursor": cursor})
            collection = data.get("collection")
            if not collection:
                log.warning("Collection '%s' not found", handle)
                break
            block = collection["products"]
            nodes = block.get("nodes", [])
            for product in nodes:
                ph = product.get("handle", "")
                if ph not in merged:
                    product["_source_collection"] = handle
                    merged[ph] = product
            log.info("Collection '%s' page %d: %d products (total %d)", handle, page, len(nodes), len(merged))
            if not block.get("pageInfo", {}).get("hasNextPage"):
                break
            cursor = block["pageInfo"].get("endCursor")
            if not cursor:
                break
        time.sleep(0.3)
    return list(merged.values())


# ---------------------------------------------------------------------------
# Size / color extraction from variant
# ---------------------------------------------------------------------------
def get_option(variant: Dict[str, Any], name: str) -> str:
    for opt in variant.get("selectedOptions") or []:
        if (opt.get("name") or "").strip().lower() == name.lower():
            return str(opt.get("value") or "").strip()
    return ""


def process_size(variant: Dict[str, Any]) -> Tuple[str, bool]:
    """Return (size_clean, is_petite). Strips trailing P from size."""
    raw = get_option(variant, "size")
    if not raw:
        raw = (variant.get("title") or "").rsplit("/", 1)[-1].strip()
    is_petite = raw.upper().endswith("P")
    size_clean = raw[:-1] if is_petite else raw
    return size_clean, is_petite


def extract_color_from_title(title: str) -> str:
    """Everything after the last dash separator in the product title.

    Handles all three formats:
      'Name - Color'   (space-hyphen-space)
      'Name- Color'    (hyphen-space, no leading space)
      'Name - Color'   (en-dash with spaces)
    """
    parts = re.split(r"\s*[-–—]\s*", title)
    if len(parts) > 1:
        return parts[-1].strip()
    return ""


# ---------------------------------------------------------------------------
# Inseam from description fallback
# ---------------------------------------------------------------------------
def inseam_from_description(description: str) -> str:
    """Find a double-digit number in description (excluding height callouts like 5'4\")."""
    d = (description or "")
    # Remove height patterns: 5'4", 5'10", etc.
    clean = re.sub(r"\d+[']\d+\"?", "", d)
    m = re.search(r"\b([0-9]{2})\b", clean)
    if m:
        val = int(m.group(1))
        if 20 <= val <= 40:
            return str(val)
    return ""


# ---------------------------------------------------------------------------
# Build rows
# ---------------------------------------------------------------------------
def build_rows(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total = len(products)
    for idx, product in enumerate(products, 1):
        product_id_raw = product.get("id", "")
        style_id = strip_gid(product_id_raw, "gid://shopify/Product/")
        product_id_int = int(style_id) if style_id.isdigit() else 0

        handle = product.get("handle", "")
        title = product.get("title", "")
        raw_type = product.get("productType", "")
        product_type = resolve_product_type(raw_type)
        if product_type is None:
            log.info("[%d/%d] Skipping %s (type=%s)", idx, total, handle, raw_type)
            continue

        tags: List[str] = product.get("tags") or []
        vendor = product.get("vendor", "")
        description_raw = fix_smart_quotes(product.get("description", "") or "")
        published_at = format_date(product.get("publishedAt", ""))
        created_at = format_date(product.get("createdAt", ""))
        online_store_url = product.get("onlineStoreUrl") or f"{PDP_HOST}/products/{handle}"
        image_url = (product.get("featuredImage") or {}).get("url", "")

        color = extract_color_from_title(title)
        product_line = derive_product_line(tags)
        tags_str = join_tags(tags)

        # PDP: measurements + stretch
        pdp = fetch_pdp(handle)
        rise = pdp.get("rise", "")
        inseam = pdp.get("inseam", "")
        leg_opening = pdp.get("leg_opening", "")
        stretch_from_desc = derive_stretch_from_description(description_raw)
        stretch = stretch_from_desc or pdp.get("stretch", "")

        # Inventory: from Rebuy API (REST JSON caps at 20; Rebuy returns real quantities)
        inv_map = fetch_rebuy_inventory(product_id_int)

        # Style name base (Step 1+2 only; Steps 3-4 in post-processing)
        style_name_base = derive_style_name_base(title)

        # Reviews
        avg_score, total_reviews = fetch_yotpo(product_id_int)

        # Quantity of style = sum of all variant inventory
        quantity_of_style = sum(inv_map.values()) if inv_map else ""

        variants = (product.get("variants") or {}).get("nodes") or []
        for variant in variants:
            variant_id_raw = variant.get("id", "")
            sku_shopify = strip_gid(variant_id_raw, "gid://shopify/ProductVariant/")
            sku_brand = variant.get("sku", "") or ""
            barcode = variant.get("barcode", "") or ""

            size, is_petite = process_size(variant)
            available = variant.get("availableForSale")
            available_str = "TRUE" if available else "FALSE"

            # Inventory quantity from REST
            variant_id_int = int(sku_shopify) if sku_shopify.isdigit() else 0
            qty_available = inv_map.get(variant_id_int, "")

            price_raw = (variant.get("price") or {}).get("amount", "")
            compare_raw = (variant.get("compareAtPrice") or {}).get("amount", "")
            price = format_price(price_raw)
            compare_at_price = format_price(compare_raw) if compare_raw else ""

            variant_title = f"{title} - {size}".strip()

            row: Dict[str, Any] = {
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": title,
                "Style Name": style_name_base,
                "Product Type": product_type,
                "Tags": tags_str,
                "Vendor": vendor,
                "Description": description_raw,
                "Variant Title": variant_title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": price,
                "Compare at Price": compare_at_price,
                "Available for Sale": available_str,
                "Quantity Available": str(qty_available) if qty_available != "" else "",
                "Quantity of style": str(quantity_of_style) if quantity_of_style != "" else "",
                "SKU - Shopify": sku_shopify,
                "SKU - Brand": sku_brand,
                "Barcode": barcode,
                "Image URL": image_url,
                "SKU URL": online_store_url,
                "Jean Style": "",  # filled by derive + post-processing
                "Product Line": product_line,
                "Inseam Label": "",  # filled after Jean Style
                "Inseam Style": "",
                "Rise Label": derive_rise_label(title, description_raw, tags, style_name_base),
                "Color - Standardized": derive_color_standardized(color, description_raw, tags),
                "Stretch": stretch,
                "Review AVG": avg_score,
                "Review Count": total_reviews,
                "Wishlist count": "",
                # Internal flags (removed before CSV output)
                "_is_petite": is_petite or "petite" in title.lower(),
                "_tags_list": tags,
            }
            rows.append(row)

        if idx % 20 == 0 or idx == total:
            log.info("[%d/%d] processed", idx, total)
        time.sleep(0.15)

    return rows


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------
def _idx(header: str) -> int:
    return CSV_HEADERS.index(header)


def apply_petite_inseam_rule(rows: List[Dict[str, Any]]) -> None:
    """Blank Inseam for petite if same Style Name + Color + Inseam exists for non-petite."""
    groups: Dict[Tuple[str, str, str], List[Dict]] = defaultdict(list)
    for row in rows:
        key = (row["Style Name"], row["Color"], row["Inseam"])
        groups[key].append(row)
    for group in groups.values():
        has_petite = any("petite" in row["Product"].lower() for row in group)
        has_non_petite = any("petite" not in row["Product"].lower() for row in group)
        if has_petite and has_non_petite:
            for row in group:
                if "petite" in row["Product"].lower():
                    row["Inseam"] = ""


def apply_inseam_from_description_fallback(rows: List[Dict[str, Any]]) -> None:
    """If Inseam still blank, try to find a double-digit measurement in description."""
    for row in rows:
        if not row["Inseam"]:
            val = inseam_from_description(row["Description"])
            if val:
                row["Inseam"] = val


def apply_measurement_inference(rows: List[Dict[str, Any]]) -> None:
    """Propagate Inseam and Leg Opening within product-title stem families.

    Rise is NOT propagated  -  if a product has no Rise measurement it stays blank.
    Petite products are grouped with their non-petite counterparts (PETITE prefix stripped)
    so that inseam/LO can propagate across both.
    """
    def _stem(title: str) -> str:
        # Split at any dash variant, strip PETITE prefix, lowercase
        t = re.split(r"\s*[-–—]\s*", title or "")[0].strip()
        t = re.sub(r"^PETITE\s+", "", t, flags=re.IGNORECASE)
        return t.lower()

    groups: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        groups[_stem(row["Product"])].append(row)

    for group in groups.values():
        legs = [r["Leg Opening"] for r in group if r["Leg Opening"]]
        inseams = [r["Inseam"] for r in group if r["Inseam"]]
        most_leg = max(set(legs), key=legs.count) if legs else ""
        most_inseam = max(set(inseams), key=inseams.count) if inseams else ""
        for row in group:
            if not row["Leg Opening"] and most_leg:
                row["Leg Opening"] = most_leg
            if not row["Inseam"] and most_inseam:
                row["Inseam"] = most_inseam


def apply_style_name_rules(rows: List[Dict[str, Any]]) -> None:
    """Steps 3-4 of Style Name derivation (Paige pattern)."""
    # Step 3a: unify by first word of *style name base* when leg opening matches.
    # Using the style name first word (not product title first word) prevents PETITE
    # products from being grouped together regardless of their actual style.
    first_word_groups: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        fw = (row["Style Name"].split(" ", 1)[0] or "").strip().lower()
        if fw:
            first_word_groups[fw].append(row)

    for fw, group in first_word_groups.items():
        if len(group) < 2:
            continue
        by_leg: Dict[str, List[Dict]] = defaultdict(list)
        for r in group:
            by_leg[r["Leg Opening"]].append(r)
        for leg_val, leg_rows in by_leg.items():
            non_mat = [r for r in leg_rows if "maternity" not in r["Product"].lower()]
            if not non_mat:
                continue
            styles = [r["Style Name"] for r in non_mat if r["Style Name"]]
            if len(set(styles)) <= 1:
                continue
            most_common = max(set(styles), key=styles.count)
            for r in non_mat:
                r["Style Name"] = most_common

    # Step 3b: one-word style names  -  look for multi-word peers with same first word
    # and same leg opening; if found use the most common. Otherwise append full Jean Style.
    for row in rows:
        sn = row["Style Name"].strip()
        if not sn or len(sn.split()) != 1:
            continue
        fw = sn.lower()
        same_family = [
            r for r in rows
            if (r["Style Name"].split(" ", 1)[0] or "").strip().lower() == fw
            and r["Leg Opening"] == row["Leg Opening"]
            and len(r["Style Name"].split()) > 1
        ]
        if same_family:
            candidates = [r["Style Name"] for r in same_family]
            row["Style Name"] = max(set(candidates), key=candidates.count)
            continue
        jean_full = row["Jean Style"].strip()
        if jean_full:
            row["Style Name"] = f"{sn} {jean_full}".strip()

    # Step 4: if style name ends in "Wide" (without "Leg"), append "Leg"
    for row in rows:
        sn = row["Style Name"]
        if re.search(r"\bWide$", sn, re.IGNORECASE):
            row["Style Name"] = sn + " Leg"


def apply_jean_style(rows: List[Dict[str, Any]]) -> None:
    """Derive Jean Style from updated Style Name and Description."""
    for row in rows:
        js = _map_jean_from_text(row["Style Name"], row["Leg Opening"], allow_mom=True)
        if not js:
            js = _map_jean_from_text(row["Description"], row["Leg Opening"], allow_mom=False)
        row["Jean Style"] = js

    # Step 3: infer from peers with same style name
    by_style: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        by_style[row["Style Name"]].append(row)
    for row in rows:
        if not row["Jean Style"] and row["Style Name"]:
            peers = [r["Jean Style"] for r in by_style[row["Style Name"]] if r["Jean Style"]]
            if peers and len(set(peers)) == 1:
                row["Jean Style"] = peers[0]


def apply_inseam_label_and_style(rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        il = derive_inseam_label(row["Product"], row["Size"], row["Jean Style"], row["Inseam"])
        row["Inseam Label"] = il
        row["Inseam Style"] = derive_inseam_style(
            row["Jean Style"], il, row["Inseam"], row["Product"], row["Description"]
        )


def apply_rise_label_inference(rows: List[Dict[str, Any]]) -> None:
    """Step 5: infer Rise Label from style-name peers."""
    by_style: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        by_style[row["Style Name"]].append(row)

    for row in rows:
        if row["Rise Label"]:
            continue
        style = row["Style Name"]
        rise = row["Rise"]
        if not style:
            continue
        peers = [r for r in by_style[style] if r["Rise Label"]]
        if not peers:
            continue
        if len({p["Rise Label"] for p in peers}) == 1:
            row["Rise Label"] = peers[0]["Rise Label"]
        elif rise:
            # Pick peer with closest Rise value
            def _dist(p: Dict) -> float:
                pv = safe_float(p["Rise"])
                rv = safe_float(rise)
                if pv is None or rv is None:
                    return float("inf")
                return abs(pv - rv)
            closest = min(peers, key=_dist)
            row["Rise Label"] = closest["Rise Label"]

    # Multiple rise tags  -  resolve using Rise measurement
    for row in rows:
        if row["Rise Label"]:
            continue
        tags = row.get("_tags_list", [])
        rise_tags = [t for t in tags if t.startswith("Rise:")]
        if len(rise_tags) > 1:
            rise_val = safe_float(row["Rise"])
            # Map rise tags to rise label then find closest
            # Defer to existing rule: use most common from style peers
            pass


def apply_color_standardized_inference(rows: List[Dict[str, Any]]) -> None:
    """Steps 3+4: fill Color - Standardized from same-color peers, then tags."""
    # Step 3: same-color peer inference
    by_color: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        key = (row["Color"] or "").strip().lower()
        if key:
            by_color[key].append(row)
    for group in by_color.values():
        filled = [r["Color - Standardized"] for r in group if r["Color - Standardized"]]
        if not filled:
            continue
        most = max(set(filled), key=filled.count)
        for r in group:
            if not r["Color - Standardized"]:
                r["Color - Standardized"] = most

    # Step 4: tags
    for row in rows:
        if not row["Color - Standardized"]:
            row["Color - Standardized"] = _color_std_from_tags(row.get("_tags_list", []))

    # Step 6: second peer pass after tags
    by_color2: Dict[str, List[Dict]] = defaultdict(list)
    for row in rows:
        key = (row["Color"] or "").strip().lower()
        if key:
            by_color2[key].append(row)
    for group in by_color2.values():
        filled = [r["Color - Standardized"] for r in group if r["Color - Standardized"]]
        if not filled:
            continue
        most = max(set(filled), key=filled.count)
        for r in group:
            if not r["Color - Standardized"]:
                r["Color - Standardized"] = most


def post_process(rows: List[Dict[str, Any]]) -> None:
    log.info("Post-processing %d rows …", len(rows))
    # Description fallback runs FIRST so that a product whose PDP returned blank
    # inseam but whose description has the value (e.g. "33 inch inseam") gets it
    # set before peer inference can overwrite with a neighbour's incorrect value.
    apply_inseam_from_description_fallback(rows)
    apply_measurement_inference(rows)
    apply_petite_inseam_rule(rows)
    # Jean style needs style name first pass (before style name rules,
    # so we can use jean_style in one-word style name rule step 3b)
    apply_jean_style(rows)
    apply_style_name_rules(rows)
    # Re-derive jean style after style name is finalized
    apply_jean_style(rows)
    apply_inseam_label_and_style(rows)
    apply_rise_label_inference(rows)
    apply_color_standardized_inference(rows)
    log.info("Post-processing complete.")


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------
def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        key = (r["Style Id"], r["SKU - Shopify"], r["SKU - Brand"])
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------
def write_csv(rows: List[Dict[str, Any]]) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = OUTPUT_DIR / f"{BRAND}_{ts}.csv"
    # Strip internal-only keys before writing
    internal_keys = {"_is_petite", "_tags_list"}
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            clean = {k: v for k, v in row.items() if k not in internal_keys}
            writer.writerow(clean)
    log.info("CSV written: %s (%d rows)", path, len(rows))
    return path


# ---------------------------------------------------------------------------
# Validation against task examples
# ---------------------------------------------------------------------------
VALIDATION_EXAMPLES = [
    ("aleks-crop-straight-leg-crop-river",      "Aleks Mid Rise Straight Cropped Jean - River",                           "Alek Straight",    "15.5"),
    ("aleks-high-rise-straight-long-muse",      "Aleks Mid Rise Straight Long Jean- Muse",                                "Alek Straight",    "15.5"),
    ("aline-high-rise-skinny-linden",           "Aline High Rise Skinny Jean - Linden",                                   "Aline Skinny",     "10"),
    ("beau-seamed-crescent-raw-jean",           "Beau Seamed Crescent Jean - Nightfall",                                  "Beau Crescent",    "18"),
    ("caleb-high-rise-relaxed-stacked-waist-2", "Caleb High Rise Stacked Waist Jean - Era",                               "Caleb Wide Leg",   "23.5"),
    ("cassie-super-high-rise-straight-bramble", "Cassie Super High Rise Straight Jean - Bramble",                         "Cassie Straight",  "16.5"),
    ("donny-rolled-hem-boyfriend-1",            "Donny Rolled Hem Boyfriend Jean - Heartfelt",                            "Donny Boyfriend",  "18"),
    ("issa-mid-rise-relaxed-straight",          "Issa Mid Rise Relaxed Straight - Road Trip",                             "Issa Straight",    "16"),
    ("kacey-mid-rise-cuffed-straight",          "Kacey Mid Rise Cuffed Boyfriend - Berkeley",                             "Kacey Boyfriend",  "17.5"),
    ("lana-crop-high-rise-ultra-wide-leg-4",    "Lana Crop High Rise Ultra Wide Jean - Bistro",                           "Lana Wide Leg",    "24"),
    ("cassie-super-high-rise-straight-petite-savvy-vintage",
     "PETITE Cassie Super High Rise Straight - Savvy Vintage",          "Cassie Straight",  "16.5"),
]


def validate_style_names(rows: List[Dict[str, Any]]) -> None:
    row_by_handle: Dict[str, List[Dict]] = defaultdict(list)
    for r in rows:
        row_by_handle[r["Handle"]].append(r)

    all_ok = True
    for handle, title, expected_sn, expected_lo in VALIDATION_EXAMPLES:
        matching = [r for r in row_by_handle.get(handle, []) if r["Product"] == title]
        if not matching:
            # Check just by handle
            matching = row_by_handle.get(handle, [])
        if not matching:
            log.warning("VALIDATION: handle '%s' not found in output", handle)
            all_ok = False
            continue
        r = matching[0]
        got_sn = r["Style Name"].strip()
        got_lo = r["Leg Opening"].strip()
        ok_sn = got_sn == expected_sn.strip()
        ok_lo = got_lo == expected_lo.strip()
        status = "OK" if (ok_sn and ok_lo) else "FAIL"
        if not ok_sn or not ok_lo:
            all_ok = False
        log.info("VALIDATION [%s] %s  SN: %r (expected %r)  LO: %r (expected %r)",
                 status, handle, got_sn, expected_sn, got_lo, expected_lo)
    if all_ok:
        log.info("VALIDATION: all %d examples passed", len(VALIDATION_EXAMPLES))
    else:
        log.warning("VALIDATION: some examples failed  -  check Style Name derivation")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Starting %s inventory scrape", BRAND)
    products = fetch_collection_products()
    log.info("Total unique products fetched: %d", len(products))

    rows = build_rows(products)
    log.info("Rows before post-processing: %d", len(rows))

    post_process(rows)
    rows = dedupe_rows(rows)
    log.info("Rows after dedup: %d", len(rows))

    validate_style_names(rows)
    write_csv(rows)
    log.info("Done.")


if __name__ == "__main__":
    main()
