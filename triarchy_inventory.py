# -*- coding: utf-8 -*-
"""Triarchy inventory scraper — Storefront GraphQL + PDP + Deco inventory."""
from __future__ import annotations

import csv
import html
import json
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
GRAPHQL_URL = "https://triarchy-atelier-denim.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN = "a1a822d8fe8512d5d00e92c7918111ce"
HOST_ROTATION = [
    "https://triarchy-atelier-denim.myshopify.com",
    "https://www.triarchy.com",
    "https://triarchy.com",
]
PDP_HOST = "https://triarchy.com"
DECO_SEARCH_URL = "https://triarchy.com/apps/search-deco-label/search"
DECO_HANDLE_BATCH_SIZE = 20
COLLECTION_HANDLE = "jeans"
SLEEP = 0.3

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE = BASE_DIR / "triarchy_inventory.log"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# CSV headers
# ---------------------------------------------------------------------------
CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At",
    "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size",
    "Rise", "Inseam", "Leg Opening",
    "Price", "Compare at Price", "Available for Sale",
    "Quantity Available", "Quantity of style",
    "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL",
    "Jean Style", "Inseam Style", "Rise Label",
    "Color - Simplified", "Color - Standardized",
]

# ---------------------------------------------------------------------------
# Style Name removal phrases (Triarchy-specific)
# ---------------------------------------------------------------------------
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "Accent Hardware", "Ankle", "Belted", "Chap", "Coated", "Constructed",
    "Corduroy", "Crop", "Cropped", "Crushed", "Crystal", "Cuff", "Cuffed",
    "Cutoff", "Cut-out", "Darted", "Destroyed", "Drawstring", "Fit", "Flag",
    "Flap", "Flip", "Frayed Seam", "Front Yoke", "Frontier",
    "High Rise", "High-Rise", "Inch", "Inset", "Jean", "Krystal", "Leather",
    "Lightweight", "Lo", "Long", "Low Rise", "Low-Rise", "Mid Rise", "Mid-Rise",
    "Ms.", "Pant", "Pants", "Patch", "Petite", "Pleated", "Pocket", "Poplin",
    "Retro", "Rolled Hem", "Seamed", "slit", "Stacked Waist", "Stacked",
    "Stoned", "Straight Leg", "Studded", "Super", "Track Pant", "Trashed",
    "Trouser", "Ultra", "Vegan Leather", "vent", "V-High Rise", "Vintage",
    "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "w/ Slit Hem",
    "W/ Stud Detailing", "W/ Wide Cuff", "W/Flap", "Wax", "Welt Pocket",
    "With Cuff", "With Frayed Seam", "Zipper",
]

VALID_SIZES: Set[str] = {
    "00", "0", "2", "4", "6", "8", "10", "12", "14",
    "15 Plus", "16 Plus", "18 Plus", "20 Plus", "22 Plus",
    "24 Plus", "24", "25", "26 Plus", "26", "27", "28 Plus",
    "29", "30 Plus", "30", "31", "32 Plus", "32",
    "XS", "S", "M", "L", "XL", "XXL", "1XL", "2XL", "3XL", "4XL", "5XL",
    "00-4", "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("triarchy")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for path in (LOG_FILE, OUTPUT_DIR / "triarchy_inventory.log"):
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

FRACTION_UNICODE = {"¼": " 1/4", "½": " 1/2", "¾": " 3/4",
                    "⅛": " 1/8", "⅜": " 3/8", "⅝": " 5/8", "⅞": " 7/8"}


def normalize_text(text: str) -> str:
    """Normalise unicode quotes and fraction chars to ASCII."""
    for sym, repl in FRACTION_UNICODE.items():
        text = text.replace(sym, repl)
    text = text.replace("“", '"').replace("”", '"')  # curly quotes → "
    text = text.replace("’", "'").replace("‘", "'")  # curly apos
    text = text.replace("″", '"').replace("′", "'")  # prime symbols
    text = text.replace(" ", " ")  # non-breaking space
    return text


def strip_html_tags(raw: str) -> str:
    if not raw:
        return ""
    text = html.unescape(raw)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return value
    return dt.strftime("%-m/%-d/%Y")


def format_price(value) -> str:
    if value is None:
        return ""
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return str(value)


def safe_str(v) -> str:
    return "" if v is None else str(v)


def to_float(v: str) -> Optional[float]:
    try:
        return float(v) if v != "" else None
    except (TypeError, ValueError):
        return None


def join_tags(tags: Iterable[str]) -> str:
    cleaned = [html.unescape(str(t)).strip() for t in tags if t and str(t).strip()]
    return ", ".join(cleaned)


def parse_mixed_fraction(raw: str) -> Optional[float]:
    """Parse '10 1/2', '17 1/4', '30.75', '13' → float."""
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
        return n / d if d else None
    try:
        return float(s)
    except ValueError:
        return None


def format_decimal(val: Optional[float]) -> str:
    if val is None:
        return ""
    s = f"{val:.6f}".rstrip("0").rstrip(".")
    return s


def contains_phrase(text: str, phrase: str) -> bool:
    """Whole-word/phrase match, case-insensitive."""
    if not text or not phrase:
        return False
    pattern = re.escape(phrase.strip()).replace(r"\ ", r"\s+")
    return bool(re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", text.lower()))


def text_has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(contains_phrase(text, p) for p in phrases)


# ===========================================================================
# Measurement extraction from PDP description
# ===========================================================================

def extract_labeled_measurement(text: str, labels: List[str]) -> str:
    """Extract the first numeric value following any of the given labels."""
    norm = normalize_text(text)
    # Replace "Front Inseam" / "Front Rise" with a placeholder so standalone
    # "Inseam" searches don't accidentally match the front-rise label.
    norm_for_inseam = re.sub(r"\bFront\s+(?:Rise|Inseam)\b", "FrontMeasurement", norm, flags=re.I)
    use_norm = norm_for_inseam if any(l.lower() in ("inseam", "inleg") for l in labels) else norm
    number_pat = r"([\d]+(?:\s+\d+/\d+)?(?:\.\d+)?)"
    for label in labels:
        m = re.search(rf"{re.escape(label)}\s*:\s*{number_pat}", use_norm, re.I)
        if m:
            val = parse_mixed_fraction(m.group(1))
            if val is not None:
                return format_decimal(val)
    return ""


def extract_pdp_measurements(description_text: str) -> Tuple[str, str, str]:
    """Return (rise, inseam, leg_opening) as decimal strings from accordion text."""
    rise = extract_labeled_measurement(description_text, ["Front Rise", "Front Inseam"])
    inseam = extract_labeled_measurement(description_text, ["Inseam", "Inleg"])
    leg = extract_labeled_measurement(description_text, ["Leg Opening"])
    return rise, inseam, leg


def inseam_fallback_from_description(description: str, inseam_current: str) -> str:
    """If inseam is blank, try to find a double-digit number in the description
    (excluding height callouts like 5'4\" and percentage values)."""
    if inseam_current:
        return inseam_current
    text = normalize_text(description)
    # Remove height callouts: 5'4", 5'7", etc.
    text = re.sub(r"\d+'\s*\d+\"?", "", text)
    # Remove percentage values (e.g. 98%, 100%, 70%)
    text = re.sub(r"\d+\s*%", "", text)
    # Find double-digit standalone numbers in a realistic inseam range (20–40)
    for m in re.finditer(r"\b(\d{2})\b", text):
        val = int(m.group(1))
        if 20 <= val <= 40:
            return m.group(1)
    return ""


# ===========================================================================
# Style Name derivation
# ===========================================================================

def derive_style_name_base(product_title: str) -> str:
    """Steps 1 and 2 of Style Name rules."""
    # Step 1: remove everything after " - " (color separator with spaces)
    if " - " in product_title:
        text = product_title.split(" - ")[0]
    else:
        text = product_title
    # Remove curly quotes and digit words
    text = text.replace('"', " ").replace('"', " ").replace('"', " ")
    text = re.sub(r"\b\d+\b", " ", text)
    # Step 2: remove styling phrases (longest first for correct phrase priority)
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        # Phrases ending in "." (like "Ms.") don't have a word boundary after the dot;
        # use a lookahead for whitespace or end-of-string instead.
        if phrase.endswith("."):
            text = re.sub(rf"\b{re.escape(phrase)}(?=\s|$)", " ", text, flags=re.IGNORECASE)
        else:
            text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    # Clean up dangling single-letter prefixes left by phrase removal (e.g., "V-")
    text = re.sub(r"(?<!\w)[A-Z]-\s*", "", text).strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ===========================================================================
# Color extraction
# ===========================================================================

def extract_color(product_title: str, variant_title: str, option1: str) -> str:
    """Pull Color from product title ' - COLOR'.
    If no ' - ' separator is present the product has no embedded color name;
    fall back to variant title before ' / ' only when that value looks like a
    real color name (more than 4 characters), otherwise return blank.
    """
    if " - " in product_title:
        return product_title.split(" - ", 1)[1].strip()
    # Variant title fallback: skip short codes like "LVB", "OW", "TRT", etc.
    v_color = ""
    if " / " in variant_title:
        v_color = variant_title.split(" / ", 1)[0].strip()
    elif "/" in variant_title:
        v_color = variant_title.rsplit("/", 1)[0].strip()
    else:
        v_color = (option1 or "").strip()
    # Only accept if it looks like a real color name (not a short code)
    if len(v_color) > 4:
        return v_color
    return ""


def extract_size(variant_title: str, option2: str) -> str:
    """Pull Size from variant title after ' / ' or validate option2."""
    if " / " in variant_title:
        size = variant_title.split(" / ", 1)[1].strip()
        return size
    # No " / " — validate option2 against allowed sizes
    candidate = (option2 or "").strip()
    if candidate in VALID_SIZES:
        return candidate
    # Try the raw variant_title itself
    if variant_title in VALID_SIZES:
        return variant_title
    return candidate


# ===========================================================================
# Jean Style derivation
# ===========================================================================

def _straight_bucket(leg_opening: str) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def _map_jean_from_title(title: str, leg_opening: str) -> str:
    t = title.lower()
    if text_has_any(t, ("barrel", "barrell", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if text_has_any(t, ("tapered", "mom")):
        return "Tapered"
    if contains_phrase(t, "baggy"):
        return "Baggy"
    if contains_phrase(t, "flare"):
        return "Flare"
    if text_has_any(t, ("bootcut", "boot")):
        return "Bootcut"
    if contains_phrase(t, "skinny"):
        return "Skinny"
    if text_has_any(t, ("wide leg", "wide-leg")):
        return "Wide Leg"
    if text_has_any(t, ("cigarette", "slim")):
        return "Straight from Knee"
    if contains_phrase(t, "straight"):
        return _straight_bucket(leg_opening)
    if contains_phrase(t, "relaxed"):
        return _straight_bucket(leg_opening)
    return ""


def _map_jean_from_description(desc: str, leg_opening: str) -> str:
    d = desc.lower()
    if text_has_any(d, ("barrel", "barrell", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if contains_phrase(d, "skinny"):
        return "Skinny"
    if contains_phrase(d, "flare"):
        return "Flare"
    if contains_phrase(d, "bootcut"):
        return "Bootcut"
    if text_has_any(d, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(d, ("wide leg", "wide-leg", "palazzo")):
        return "Wide Leg"
    # Straight / relaxed style checks with leg opening
    has_straight = contains_phrase(d, "straight")
    has_relaxed = contains_phrase(d, "relaxed style")
    if has_straight or has_relaxed:
        return _straight_bucket(leg_opening)
    if text_has_any(d, ("baggy", "loose fit")):
        return "Baggy"
    return ""


def derive_jean_style(title: str, description: str, leg_opening: str) -> str:
    """Steps 1-2 of Jean Style rules; Step 3 is applied in post-processing."""
    result = _map_jean_from_title(title, leg_opening)
    if result:
        return result
    result = _map_jean_from_description(description, leg_opening)
    return result


# ===========================================================================
# Inseam Style derivation
# ===========================================================================

NON_TAPER = {"Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
             "Boyfriend", "Baggy", "Flare", "Straight from Thigh"}
TAPER = {"Tapered", "Skinny", "Straight from Knee"}

KEYWORDS_FOR_CROP = ("crop", "cropped")
KEYWORDS_FOR_ANKLE = (
    "ankle length", "ankle opening", "at the ankle", "extends to your ankle",
)
KEYWORDS_FOR_FULL = (
    "full length", "full-length",
    "leg that seems to stretch for miles",
    "long inseam",
    "legs will be what stand out here, looking miles long",
    "endless length",
    "seems like miles",
)


def _measurement_inseam_style(jean_style: str, inseam: str, is_petite: bool) -> str:
    val = to_float(inseam)
    if val is None:
        return ""
    if jean_style in NON_TAPER:
        if is_petite:
            if val <= 25:
                return "Cropped"
            if val < 28:
                return "Ankle"
            return "Full Length"
        else:
            if val <= 27:
                return "Cropped"
            if val < 30:
                return "Ankle"
            return "Full Length"
    if jean_style in TAPER:
        if val <= 27:
            return "Cropped"
        return "Full Length"
    return ""


def _title_inseam_style(title: str, jean_style: str) -> str:
    t = title.lower()
    is_taper = jean_style in TAPER
    if contains_phrase(t, "ankle"):
        return "Full Length" if is_taper else "Ankle"
    if text_has_any(t, ("cropped", "crop")):
        return "Cropped"
    return ""


def _description_inseam_style(desc: str, jean_style: str) -> str:
    d = desc.lower()
    is_taper = jean_style in TAPER
    has_crop = text_has_any(d, KEYWORDS_FOR_CROP)
    has_ankle = text_has_any(d, KEYWORDS_FOR_ANKLE)
    has_cropped_ankle = contains_phrase(d, "cropped ankle")
    has_full = text_has_any(d, KEYWORDS_FOR_FULL)

    if (has_crop and has_ankle) or has_cropped_ankle:
        return "Cropped" if is_taper else "Ankle"
    if has_crop:
        return "Cropped"
    if has_ankle:
        return "Full Length" if is_taper else "Ankle"
    if has_full:
        return "Full Length"
    return ""


def resolve_inseam_style_table(m_is: str, t_is: str, d_is: str) -> str:
    """Apply the lookup table from the spec."""
    # Normalise "Cropped Ankle" → "Ankle" in description column
    if d_is == "Cropped Ankle":
        d_is = "Ankle"
    # Rule 1: description says Full Length → always Full Length
    if d_is == "Full Length":
        return "Full Length"
    # Rule 2: measurement says Full Length → Full Length (measurement is authoritative
    # when it clearly conflicts with a description ankle keyword caused by design language
    # like "raw hems at the ankle" rather than an ankle-length inseam)
    if m_is == "Full Length":
        return "Full Length"
    # Rule 3: any column says Ankle → Ankle
    if m_is == "Ankle" or t_is == "Ankle" or d_is == "Ankle":
        return "Ankle"
    # Rule 4: any column says Cropped → Cropped
    if m_is == "Cropped" or t_is == "Cropped" or d_is == "Cropped":
        return "Cropped"
    # Rule 5: all blank or remaining → Full Length
    return "Full Length"


def derive_inseam_style(jean_style: str, title: str, inseam: str, description: str) -> str:
    is_petite = "petite" in title.lower()
    m_is = _measurement_inseam_style(jean_style, inseam, is_petite)
    t_is = _title_inseam_style(title, jean_style)
    d_is = _description_inseam_style(description, jean_style)
    return resolve_inseam_style_table(m_is, t_is, d_is)


# ===========================================================================
# Rise Label derivation
# ===========================================================================

def derive_rise_label(title: str, description: str) -> str:
    """Steps 1–2 of Rise Label rules; Steps 3–4 applied in post-processing."""
    for src in (title, description):
        s = src.lower()
        # Ultra Low (check before Low)
        if text_has_any(s, ("super low rise", "super low-rise", "ultra low rise",
                             "ultra low-rise", "super low waist", "super low-waist",
                             "ultra low waist", "ultra low-waist",
                             "v-low rise", "v-low-rise")):
            return "Ultra Low"
        # Ultra High (check before High)
        if text_has_any(s, ("super high rise", "super high-rise", "ultra high rise",
                             "ultra high-rise", "super high waist", "super high-waist",
                             "ultra high waist", "ultra high-waist",
                             "v-high rise", "v-high-rise")):
            return "Ultra High"
        # Mid
        if text_has_any(s, ("mid-rise", "mid rise")):
            return "Mid"
        # Low (check before High to avoid "low" matching "high")
        if text_has_any(s, ("low-rise", "low rise",
                             "rise: low", "rise - low", "low on the hip", "low on the waist")):
            return "Low"
        # High
        if text_has_any(s, ("high-rise", "high rise", "high waist", "high-waist",
                             "rise: high", "rise - high",
                             "high on the hip", "high on the waist",
                             "sit perfectly at your waist", "elevated waistline",
                             "elevated, cinched waistline")):
            return "High"
    return ""


# ===========================================================================
# Color - Standardized derivation
# ===========================================================================

_COLOR_STD_MAPPING = [
    (("animal print", "leopard", "snake", "camo"), "Animal Print"),
    (("blue", "navy", "indigo"), "Blue"),
    (("brown", "cinnamon", "coffee", "espresso"), "Brown"),
    (("green", "olive", "cypress", "sage"), "Green"),
    (("grey", "gray"), "Gray"),
    (("orange",), "Orange"),
    (("pink", "blush", "coral"), "Pink"),
    (("purple", "violet"), "Purple"),
    (("red", "wine", "burgundy"), "Red"),
    (("tan", "beige", "khaki"), "Tan"),
    (("white", "ecru", "egret", "cream", "blizzard", "parchment", "blanc"), "White"),
    (("yellow",), "Yellow"),
    (("black", "noir", "onyx", "raven"), "Black"),
]

_BLUE_WASH_PHRASES = (
    "dark base", "acid wash", "acid-wash", "dark rinse", "dark stretch denim",
    "dark wash", "dark washed", "dark vintage wash", "dark vintage inspired wash",
    "rich dark base", "rich, dark base", "medium base", "medium wash",
    "medium vintage wash", "medium rinse", "medium washed",
    "medium vintage inspired wash", "a lighter, spring-ready wash",
    "a lighter, summer-ready wash", "light base", "light wash",
    "light vintage wash", "light rinse", "light washed",
    "light vintage inspired wash", "season-ready wash",
    "medium-dark", "medium-light",
)


def derive_color_standardized(color: str, description: str, tags_str: str) -> str:
    # Step 1: from Color
    c = color.lower()
    for keys, out in _COLOR_STD_MAPPING:
        if text_has_any(c, keys):
            return out
    # Step 2: from Description
    d = description.lower()
    for keys, out in _COLOR_STD_MAPPING:
        if text_has_any(d, keys):
            return out
    if text_has_any(d, _BLUE_WASH_PHRASES):
        return "Blue"
    # Step 3: group inference handled in post-processing
    # Step 4: from Tags
    t = tags_str.lower()
    for keys, out in _COLOR_STD_MAPPING:
        if text_has_any(t, keys):
            return out
    return ""


# ===========================================================================
# Color - Simplified derivation
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
    # Step 2: from Color keywords
    if text_has_any(c, ("wine", "burgundy", "navy", "dark", "hunter green", "deep")):
        return "Dark"
    if text_has_any(c, ("pastel", "cream", "moonwashed", "light")):
        return "Light"
    if text_has_any(c, ("medium", "mid")):
        return "Medium"
    # Step 3: from Description
    if text_has_any(d, ("medium light", "light to medium", "medium to light",
                         "light-to-medium", "medium-to-light", "medium-light",
                         "light-medium", "light/medium", "medium/light", "light medium")):
        return "Light to Medium"
    if text_has_any(d, ("medium to dark", "dark to medium", "dark-to-medium",
                         "medium-to-dark", "dark medium", "medium/dark", "dark/medium",
                         "medium-dark", "dark-medium")):
        return "Medium to Dark"
    if text_has_any(d, ("dark", "deep", "black", "wine", "burgundy",
                         "midnight blue", "forest green", "navy")):
        return "Dark"
    if text_has_any(d, ("light blue", "pale blue", "light vintage", "soft blue",
                         "soft pink", "ecru", "white", "acid wash", "acid-wash",
                         "light", "khaki", "tan", "ivory")):
        return "Light"
    if text_has_any(d, ("mid blue", "mid-blue", "medium stone wash",
                         "classic stone washed blue", "vintage washed blue",
                         "classic vintage blue", "medium blue", "medium wash",
                         "classic blue", "lighter indigo")):
        return "Medium"
    # Step 4: group inference handled in post-processing
    return ""


# ===========================================================================
# Product Type cleanup
# ===========================================================================

def resolve_product_type(product_type: str, category_name: str) -> str:
    bad = {"", "uncategorized", "nior", "indigo"}
    if (product_type or "").strip().lower() in bad:
        return (category_name or "").strip()
    return (product_type or "").strip()


# ===========================================================================
# HTTP session
# ===========================================================================

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "POST"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/124.0.0.0 Safari/537.36"),
    })
    return session


# ===========================================================================
# GraphQL fetcher
# ===========================================================================

_GRAPHQL_QUERY = """
query TriarchyJeans($cursor: String) {
  collection(handle: "jeans") {
    products(first: 250, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        createdAt
        publishedAt
        productType
        category { name }
        tags
        vendor
        onlineStoreUrl
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
            LOGGER.error("GraphQL request failed (page %s): %s", page, exc)
            break
        payload = resp.json()
        if payload.get("errors"):
            # Filter non-fatal errors (e.g., access-denied on quantityAvailable)
            fatal = [e for e in payload["errors"] if "ACCESS_DENIED" not in str(e.get("extensions", {}).get("code", ""))]
            if fatal:
                LOGGER.error("GraphQL fatal errors: %s", fatal)
                break
        data = (payload.get("data") or {}).get("collection", {})
        if not data:
            LOGGER.warning("No collection data returned on page %s", page)
            break
        block = data.get("products", {})
        for node in block.get("nodes", []):
            handle = node.get("handle", "")
            if handle and handle not in products:
                products[handle] = node
        page_info = block.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        log("GraphQL page %s: %s products so far", page, len(products))
        time.sleep(SLEEP)
    log("GraphQL fetch complete: %s unique products", len(products))
    return list(products.values())


# ===========================================================================
# products.json fetcher (for dedup handle list)
# ===========================================================================

def fetch_products_json_handles(session: requests.Session) -> Set[str]:
    handles: Set[str] = set()
    for host in HOST_ROTATION:
        for page in range(1, 20):
            url = f"{host}/collections/{COLLECTION_HANDLE}/products.json"
            try:
                resp = session.get(url, params={"limit": 250, "page": page}, timeout=30)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
                products = resp.json().get("products", [])
                if not products:
                    break
                for p in products:
                    h = (p.get("handle") or "").strip()
                    if h:
                        handles.add(h)
                time.sleep(SLEEP)
            except Exception as exc:
                LOGGER.warning("products.json page %s from %s: %s", page, host, exc)
                break
        if handles:
            log("products.json found %s unique handles from %s", len(handles), host)
            break
    return handles


# ===========================================================================
# PDP scraper
# ===========================================================================

def fetch_pdp(session: requests.Session, handle: str) -> Dict[str, str]:
    """Fetch description + measurements from the product page accordion."""
    for host in [PDP_HOST, "https://www.triarchy.com"]:
        url = f"{host}/products/{handle}"
        try:
            resp = session.get(url, timeout=30, verify=False)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            # Try the tri-accordion-wrapper first
            acc = soup.find("div", class_="tri-accordion-wrapper")
            if not acc:
                # Broader fallback
                acc = soup.find("div", class_=re.compile(r"accordion", re.I))
            if not acc:
                acc = soup.find("div", id=re.compile(r"ProductInfo", re.I))
            text = acc.get_text(" ", strip=True) if acc else ""
            text = normalize_text(text)
            # Also try meta description as fallback
            if not text:
                meta = soup.find("meta", {"name": "description"})
                text = html.unescape(meta.get("content", "")) if meta else ""
            rise, inseam, leg = extract_pdp_measurements(text)
            return {"description": text, "rise": rise, "inseam": inseam,
                    "leg_opening": leg}
        except Exception as exc:
            LOGGER.warning("PDP fetch failed for %s from %s: %s", handle, host, exc)
    return {"description": "", "rise": "", "inseam": "", "leg_opening": ""}


# ===========================================================================
# Deco inventory fetcher
# ===========================================================================

def fetch_deco_inventory(session: requests.Session, handles: List[str]) -> Dict[str, int]:
    """Return {variant_id_str: inventory_quantity} from the Deco app endpoint."""
    inv_map: Dict[str, int] = {}
    for start in range(0, len(handles), DECO_HANDLE_BATCH_SIZE):
        batch = handles[start: start + DECO_HANDLE_BATCH_SIZE]
        try:
            resp = session.get(
                DECO_SEARCH_URL,
                params={"handleSearch": ",".join(batch)},
                timeout=30,
                verify=False,
            )
            if not resp.ok:
                LOGGER.warning("Deco HTTP %s for batch %s", resp.status_code, batch[:3])
                continue
            payload = resp.json()
        except Exception as exc:
            LOGGER.warning("Deco request failed for batch %s: %s", batch[:3], exc)
            continue
        if not isinstance(payload, dict):
            continue
        for handle in batch:
            product = payload.get(handle)
            if not isinstance(product, dict):
                continue
            for variant in product.get("variants") or []:
                if not isinstance(variant, dict):
                    continue
                vid = str(variant.get("id") or "").strip()
                qty = variant.get("inventory_quantity")
                if vid and qty is not None:
                    try:
                        inv_map[vid] = int(qty)
                    except (TypeError, ValueError):
                        pass
        time.sleep(SLEEP)
    log("Deco inventory: %s variant records fetched", len(inv_map))
    return inv_map


# ===========================================================================
# Post-processing helpers
# ===========================================================================

IDX = {h: i for i, h in enumerate(CSV_HEADERS)}


def _col(row: List[str], name: str) -> str:
    return row[IDX[name]]


def _set(row: List[str], name: str, val: str) -> None:
    row[IDX[name]] = val


def apply_style_name_rules(rows: List[List[str]]) -> None:
    """Steps 3.1 and 3.2 of the Style Name rules."""
    idx_product = IDX["Product"]
    idx_sn = IDX["Style Name"]
    idx_leg = IDX["Leg Opening"]
    idx_js = IDX["Jean Style"]

    # Rule 1: same first word, different rest, same leg opening → most-frequent style name
    by_first: Dict[str, List[List[str]]] = {}
    for row in rows:
        title = row[idx_product]
        fw = (title.split(" ", 1)[0] if title else "").strip().lower()
        if fw:
            by_first.setdefault(fw, []).append(row)

    for fw, group in by_first.items():
        if len(group) < 2:
            continue
        by_leg: Dict[str, List[List[str]]] = {}
        for r in group:
            by_leg.setdefault(r[idx_leg], []).append(r)
        for leg_val, leg_rows in by_leg.items():
            non_mat = [r for r in leg_rows if "maternity" not in r[idx_product].lower()]
            if not non_mat:
                continue
            style_names = [r[idx_sn] for r in non_mat if r[idx_sn]]
            if len(set(style_names)) <= 1:
                continue
            most_common = max(set(style_names), key=style_names.count)
            for r in non_mat:
                _set(r, "Style Name", most_common)

    # Rule 2: one-word style names
    for row in rows:
        sn = _col(row, "Style Name").strip()
        if not sn or len(sn.split()) != 1:
            continue
        title = row[idx_product]
        fw = (title.split(" ", 1)[0] if title else "").strip().lower()
        leg = row[idx_leg]
        # 2a: find siblings with same first word + leg opening that have multi-word style name
        candidates = [
            r[idx_sn]
            for r in rows
            if (r[idx_product].split(" ", 1)[0] if r[idx_product] else "").strip().lower() == fw
            and r[idx_leg] == leg
            and len(r[idx_sn].split()) > 1
        ]
        if candidates:
            _set(row, "Style Name", max(set(candidates), key=candidates.count))
            continue
        # 2b: add first word of Jean Style
        js = _col(row, "Jean Style")
        if js:
            first_js_word = js.split()[0]
            _set(row, "Style Name", f"{sn} {first_js_word}".strip())
        # 2c: keep as is if Jean Style blank


def apply_jean_style_inference(rows: List[List[str]]) -> None:
    """Step 3: infer Jean Style from siblings sharing same Style Name + Leg Opening."""
    for row in rows:
        if _col(row, "Jean Style"):
            continue
        sn = _col(row, "Style Name")
        leg = _col(row, "Leg Opening")
        if not sn:
            continue
        matches = [
            _col(r, "Jean Style")
            for r in rows
            if _col(r, "Style Name") == sn
            and _col(r, "Leg Opening") == leg
            and _col(r, "Jean Style")
        ]
        if matches:
            _set(row, "Jean Style", max(set(matches), key=matches.count))


def apply_inseam_style_refresh(rows: List[List[str]]) -> None:
    """Recompute Inseam Style after Jean Style may have been updated."""
    for row in rows:
        js = _col(row, "Jean Style")
        title = _col(row, "Product")
        inseam = _col(row, "Inseam")
        desc = _col(row, "Description")
        _set(row, "Inseam Style", derive_inseam_style(js, title, inseam, desc))


def apply_rise_label_inference(rows: List[List[str]]) -> None:
    """Step 4: infer Rise Label from siblings sharing same Style Name + Rise measurement."""
    for row in rows:
        if _col(row, "Rise Label"):
            continue
        sn = _col(row, "Style Name")
        rise = _col(row, "Rise")
        if not sn or not rise:
            continue
        matches = [
            _col(r, "Rise Label")
            for r in rows
            if _col(r, "Style Name") == sn and _col(r, "Rise") == rise and _col(r, "Rise Label")
        ]
        if not matches:
            continue
        _set(row, "Rise Label", max(set(matches), key=matches.count))


def apply_color_inference(rows: List[List[str]]) -> None:
    """Group-based color inference (step 3 of Color rules)."""
    by_color: Dict[str, List[List[str]]] = {}
    for row in rows:
        key = _col(row, "Color").strip().lower()
        if key:
            by_color.setdefault(key, []).append(row)
    for _, group in by_color.items():
        stds = [_col(r, "Color - Standardized") for r in group if _col(r, "Color - Standardized")]
        simps = [_col(r, "Color - Simplified") for r in group if _col(r, "Color - Simplified")]
        best_std = max(set(stds), key=stds.count) if stds else ""
        best_simp = max(set(simps), key=simps.count) if simps else ""
        for r in group:
            if not _col(r, "Color - Standardized") and best_std:
                _set(r, "Color - Standardized", best_std)
            if not _col(r, "Color - Simplified") and best_simp:
                _set(r, "Color - Simplified", best_simp)


def apply_petite_inseam_rule(rows: List[List[str]]) -> None:
    """If same Style Name + Color + Inseam has both petite and non-petite, blank petite inseam."""
    grouped: Dict[Tuple[str, str, str], List[List[str]]] = {}
    for row in rows:
        key = (_col(row, "Style Name"), _col(row, "Color"), _col(row, "Inseam"))
        grouped.setdefault(key, []).append(row)
    for group in grouped.values():
        has_petite = any("petite" in _col(r, "Product").lower() for r in group)
        has_non = any("petite" not in _col(r, "Product").lower() for r in group)
        if not (has_petite and has_non):
            continue
        for r in group:
            if "petite" in _col(r, "Product").lower():
                _set(r, "Inseam", "")


# ===========================================================================
# Main scraper
# ===========================================================================

class TriarchyScraper:
    def __init__(self) -> None:
        self.session = make_session()
        self._pdp_cache: Dict[str, Dict[str, str]] = {}

    def _pdp(self, handle: str) -> Dict[str, str]:
        if handle not in self._pdp_cache:
            self._pdp_cache[handle] = fetch_pdp(self.session, handle)
            time.sleep(SLEEP)
        return self._pdp_cache[handle]

    def build_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []

        log("Fetching GraphQL products from 'jeans' collection...")
        products = fetch_graphql_products(self.session)

        # Also fetch products.json handles for dedup confirmation
        log("Fetching products.json for handle deduplication...")
        json_handles = fetch_products_json_handles(self.session)
        # If GraphQL returned products that aren't in json_handles, still include them
        # (json_handles is secondary; GraphQL is primary source)
        if json_handles:
            # Filter to only handles known in products.json, but keep GraphQL as master
            before = len(products)
            # Deduplicate: GraphQL already deduplicates by handle; no further filtering needed
            log("products.json handles: %s (GraphQL handles: %s)", len(json_handles), before)

        all_handles = [p.get("handle", "") for p in products if p.get("handle")]

        log("Fetching Deco inventory for %s handles...", len(all_handles))
        inv_map = fetch_deco_inventory(self.session, all_handles)

        log("Fetching PDP pages and building rows...")
        for idx, product in enumerate(products, start=1):
            handle = product.get("handle", "")
            if not handle:
                continue

            product_id = (product.get("id") or "").replace("gid://shopify/Product/", "")
            title = product.get("title") or ""
            published_at = format_date(product.get("publishedAt"))
            created_at = format_date(product.get("createdAt"))
            product_type_raw = (product.get("productType") or "").strip()
            category_name = ((product.get("category") or {}).get("name") or "").strip()
            product_type = resolve_product_type(product_type_raw, category_name)
            tags = product.get("tags") or []
            tags_str = join_tags(tags)
            vendor = product.get("vendor") or ""
            online_store_url = product.get("onlineStoreUrl") or f"{PDP_HOST}/products/{handle}"
            image_url = ((product.get("featuredImage") or {}).get("url") or "")

            # Fetch PDP
            pdp = self._pdp(handle)
            description = pdp.get("description", "")
            rise = pdp.get("rise", "")
            inseam = pdp.get("inseam", "")
            leg_opening = pdp.get("leg_opening", "")

            # Inseam fallback from double-digit in description
            inseam = inseam_fallback_from_description(description, inseam)

            # Variants
            variants = (product.get("variants") or {}).get("nodes") or []
            if not variants:
                log("No variants for %s, skipping", handle)
                continue

            # Style-level inventory total (from Deco)
            style_qty = 0
            for v in variants:
                vid = (v.get("id") or "").replace("gid://shopify/ProductVariant/", "")
                qty = inv_map.get(vid, 0)
                style_qty += qty

            # Derive product-level fields (shared across variants)
            style_name = derive_style_name_base(title)

            # Jean Style — will be refined in post-processing
            jean_style = derive_jean_style(title, description, leg_opening)
            rise_label = derive_rise_label(title, description)
            inseam_style = derive_inseam_style(jean_style, title, inseam, description)

            for v in variants:
                v_id_full = v.get("id") or ""
                sku_shopify = v_id_full.replace("gid://shopify/ProductVariant/", "")
                sku_brand = v.get("sku") or ""
                barcode = v.get("barcode") or ""
                v_title = v.get("title") or ""
                available = "TRUE" if v.get("availableForSale") else "FALSE"

                # Selected options
                opts = v.get("selectedOptions") or []
                opt_map: Dict[str, str] = {o["name"].lower(): o["value"] for o in opts if o}
                option1 = opt_map.get("color") or (opts[0]["value"] if opts else "")
                option2 = opt_map.get("size") or (opts[1]["value"] if len(opts) > 1 else "")

                color = extract_color(title, v_title, option1)
                size = extract_size(v_title, option2)
                variant_title = f"{title} - {size}" if size else title

                price_obj = v.get("price") or {}
                price = format_price(price_obj.get("amount") if isinstance(price_obj, dict) else price_obj)
                cmp_obj = v.get("compareAtPrice")
                compare_at = format_price((cmp_obj or {}).get("amount") if isinstance(cmp_obj, dict) else cmp_obj)

                qty_avail = inv_map.get(sku_shopify, "")
                qty_avail_str = str(qty_avail) if qty_avail != "" else ""

                color_std = derive_color_standardized(color, description, tags_str)
                color_simp = derive_color_simplified(color, description, color_std)

                row: List[str] = [""] * len(CSV_HEADERS)
                _set(row, "Style Id", product_id)
                _set(row, "Handle", handle)
                _set(row, "Published At", published_at)
                _set(row, "Created At", created_at)
                _set(row, "Product", title)
                _set(row, "Style Name", style_name)
                _set(row, "Product Type", product_type)
                _set(row, "Tags", tags_str)
                _set(row, "Vendor", vendor)
                _set(row, "Description", description)
                _set(row, "Variant Title", variant_title)
                _set(row, "Color", color)
                _set(row, "Size", size)
                _set(row, "Rise", rise)
                _set(row, "Inseam", inseam)
                _set(row, "Leg Opening", leg_opening)
                _set(row, "Price", price)
                _set(row, "Compare at Price", compare_at)
                _set(row, "Available for Sale", available)
                _set(row, "Quantity Available", qty_avail_str)
                _set(row, "Quantity of style", str(style_qty) if style_qty else "")
                _set(row, "SKU - Shopify", sku_shopify)
                _set(row, "SKU - Brand", sku_brand)
                _set(row, "Barcode", barcode)
                _set(row, "Image URL", image_url)
                _set(row, "SKU URL", online_store_url)
                _set(row, "Jean Style", jean_style)
                _set(row, "Inseam Style", inseam_style)
                _set(row, "Rise Label", rise_label)
                _set(row, "Color - Simplified", color_simp)
                _set(row, "Color - Standardized", color_std)
                rows.append(row)

            if idx % 5 == 0 or idx == len(products):
                log("Progress: %s/%s products processed (%s rows)", idx, len(products), len(rows))

        # Post-processing passes
        log("Post-processing: style name rules...")
        apply_style_name_rules(rows)
        log("Post-processing: Jean Style inference...")
        apply_jean_style_inference(rows)
        log("Post-processing: Inseam Style refresh...")
        apply_inseam_style_refresh(rows)
        log("Post-processing: Rise Label inference...")
        apply_rise_label_inference(rows)
        log("Post-processing: Color inference...")
        apply_color_inference(rows)
        log("Post-processing: Petite inseam rule...")
        apply_petite_inseam_rule(rows)

        return rows

    def write_csv(self, rows: List[List[str]]) -> Path:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = OUTPUT_DIR / f"TRIARCHY_{ts}.csv"
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADERS)
            writer.writerows(rows)
        log("Wrote %s rows to %s", len(rows), path)
        return path

    def run(self) -> Path:
        log("=== Triarchy inventory run started ===")
        rows = self.build_rows()
        path = self.write_csv(rows)
        log("=== Run complete: %s ===", path)
        print(f"Done — {path}")
        return path


def main() -> None:
    TriarchyScraper().run()


if __name__ == "__main__":
    main()
