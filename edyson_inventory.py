# -*- coding: utf-8 -*-
"""Edyson inventory scraper — Storefront GraphQL + PDP measurements."""
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
GRAPHQL_URL = "https://edysonsdenim.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN = "17595f4862ea375e0e788738bbe98f65"
HOST_ROTATION = [
    "https://edysonsdenim.myshopify.com",
    "https://www.edyson.com",
    "https://edyson.com",
]
PDP_HOST = "https://edyson.com"
COLLECTION_HANDLE = "all-products"
SLEEP = 0.3

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE = BASE_DIR / "edyson_inventory.log"
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
    "Jean Style", "Inseam Label", "Inseam Style", "Rise Label",
    "Hem Style", "Color - Simplified", "Color - Standardized",
]

# ---------------------------------------------------------------------------
# Filter words — products whose title contains these are excluded
# ---------------------------------------------------------------------------
FILTER_WORDS = [
    "Accessories", "Bag", "Bermuda", "Bermudas", "Blazers", "Blouses",
    "Bodysuits", "Button Up", "Capri", "Cardigans", "Clothing Tops", "Coat",
    "Coats & Jackets", "Core Handbags", "Crop Tops", "Denim Shorts", "Dress",
    "Dresses", "Fashion Core Handbags", "Fashion Handbags", "Handbag",
    "Hoodies", "Jacket", "Jackets", "Jogger Shorts", "Jort", "Jumpsuits",
    "Long Sleeve", "Neck", "One-Pieces", "Outerwear", "Pant Suits", "Purse",
    "Romper", "Rompers", "Shacket", "Shipping Protection", "Shirt", "Shirts",
    "Shirts & Tops", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Suits",
    "Sweater", "Sweaters", "Sweatpant", "Sweatpants", "Sweats", "Sweatshirts",
    "Swim", "Tank", "Tank Tops", "Tee", "Top", "Tops", "Trench", "T-Shirts",
    "Vest", "Vests",
]

# ---------------------------------------------------------------------------
# Style Name removal phrases
# ---------------------------------------------------------------------------
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "Accent Hardware", "Ankle", "Belted", "Cargo", "Chap", "Coated",
    "Constructed", "Corduroy", "Crop", "Cropped", "Crushed", "Crystal",
    "Cuff", "Cuffed", "Cutoff", "Cut-out", "Darted", "Destroyed",
    "Drawstring", "Extra", "Fit", "Flag", "Flap", "Flip", "Frayed Seam",
    "Front Yoke", "Frontier", "High Rise", "High-Rise", "Inch", "Inset",
    "Jean", "Kick", "Krystal", "Leather", "Lightweight", "Lo", "Long",
    "Low Rise", "Low-Rise", "Mid Rise", "Mid-Rise", "Ms.", "Pant", "Pants",
    "Patch", "Petite", "Pleated", "Pocket", "Poplin", "Retro", "Roll Up", "Rolled Hem",
    "Rolled Up", "Seamed", "slit", "Slouchy", "Stacked Waist", "Stacked",
    "Stoned", "Studded", "Super", "The", "Track Pant", "Trashed", "Trouser",
    "Ultra", "Utlity", "Utillity", "Utility", "Vegan Leather", "vent", "V-High Rise", "Vintage",
    "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "w/ Slit Hem",
    "W/ Stud Detailing", "W/ Wide Cuff", "W/Flap", "Wax", "Welt Pocket",
    "With Cuff", "With Frayed Seam", "Zipper",
]

# Valid size values used to identify the Size option
VALID_SIZES: Set[str] = {
    "00", "0", "2", "4", "6", "8", "10", "12", "14",
    "15 Plus", "16 Plus", "18 Plus", "20 Plus", "22 Plus",
    "24 Plus", "26 Plus", "28 Plus", "30 Plus", "32 Plus",
    "XS", "S", "M", "L", "XL", "XXL", "1XL", "2XL", "3XL", "4XL", "5XL",
    "00-4", "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus",
    "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32",
    "33", "34", "35", "36", "37", "38", "39", "40",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("edyson")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for path in (LOG_FILE, OUTPUT_DIR / "edyson_inventory.log"):
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
    text = text.replace("’", "'").replace(" ", " ")
    # normalize curly/smart inch marks to straight "
    text = text.replace("”", '"').replace("’", "'")
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
    return f"{dt.month}/{dt.day}/{dt.year}"


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
    pattern = re.escape(phrase.strip().lower()).replace(r"\ ", r"\s+")
    return bool(re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", text.lower()))


def text_has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(contains_phrase(text, p) for p in phrases)


def should_filter_product(title: str) -> bool:
    """Return True if the product title contains any filter word."""
    for w in FILTER_WORDS:
        pat = r"(?<![a-zA-Z0-9])" + re.escape(w) + r"(?![a-zA-Z0-9])"
        if re.search(pat, title, re.IGNORECASE):
            return True
    return False


# ===========================================================================
# Measurement extraction from PDP
# ===========================================================================

def _extract_measurement(text: str, label: str) -> str:
    """Extract numeric measurement after a label, e.g. 'Rise 11 1/2"'."""
    norm = normalize_text(text)
    # Pattern: label followed by optional whitespace/colon, then a number (possibly fraction),
    # then optional whitespace, then inch mark (or next newline)
    pattern = rf'\b{re.escape(label)}\b\s*:?\s*([\d]+(?:\s+\d+/\d+)?(?:\.\d+)?)\s*[""″]?'
    m = re.search(pattern, norm, re.IGNORECASE)
    if m:
        val = parse_mixed_fraction(m.group(1))
        if val is not None:
            return format_decimal(val)
    return ""


def _extract_description_from_pdp(soup: BeautifulSoup) -> str:
    """Extract description text from PDP using Edyson's ProductInfo selectors.

    Part 1: #ProductInfo-template--*__main-product > div:nth-child(2) > max-height
    Part 2: #ProductInfo-template--*__main-product > collapsible-row.product--accordion.accordion
    Concatenated with '; '.
    """
    # Find the ProductInfo container — template ID varies, match by prefix/suffix
    product_info = soup.find(id=re.compile(r'^ProductInfo-template--.*__main-product$'))
    if not product_info:
        product_info = soup.find(id=re.compile(r'^ProductInfo-template--'))
    if not product_info:
        return ""

    # Part 1: direct element child at position 2 (nth-child(2)) → max-height sub-element
    part1 = ""
    element_children = [c for c in product_info.children
                        if hasattr(c, 'name') and c.name]
    if len(element_children) >= 2:
        second_child = element_children[1]
        if second_child.name == 'div':
            max_height_el = second_child.find('max-height')
            if max_height_el:
                part1 = re.sub(r'\s+', ' ', max_height_el.get_text(' ', strip=True)).strip()
            else:
                part1 = re.sub(r'\s+', ' ', second_child.get_text(' ', strip=True)).strip()

    # Part 2: collapsible-row.product--accordion.accordion
    part2 = ""
    try:
        collapsible = product_info.select_one('collapsible-row.product--accordion.accordion')
        if collapsible:
            part2 = re.sub(r'\s+', ' ', collapsible.get_text(' ', strip=True)).strip()
    except Exception:
        pass

    parts = [p for p in [part1, part2] if p]
    return "; ".join(parts)


def fetch_pdp(session: requests.Session, handle: str) -> Dict[str, str]:
    """Fetch Rise, Inseam, Leg Opening measurements and description from the PDP."""
    for host in [PDP_HOST, "https://www.edyson.com", "https://edysonsdenim.myshopify.com"]:
        url = f"{host}/products/{handle}"
        try:
            resp = session.get(url, timeout=30, verify=False)
            if resp.status_code != 200:
                continue
            text = normalize_text(resp.text)
            soup = BeautifulSoup(text, "html.parser")
            page_text = soup.get_text(" ", strip=True)
            page_text = normalize_text(page_text)

            rise = _extract_measurement(page_text, "Rise")
            leg = _extract_measurement(page_text, "Leg Opening")

            # Inseam: "roll up" number takes priority; fall back to Inseam/Inleg label
            inseam = ""
            m = re.search(r'roll\s*up\s*(?:to\s*)?([\d]+(?:\s+\d+/\d+)?)\s*[""″]',
                          page_text, re.I)
            if m:
                val = parse_mixed_fraction(m.group(1))
                if val is not None:
                    inseam = format_decimal(val)
            if not inseam:
                inseam = _extract_measurement(page_text, "Inseam")
            if not inseam:
                inseam = _extract_measurement(page_text, "Inleg")

            description = _extract_description_from_pdp(soup)

            return {"rise": rise, "inseam": inseam, "leg_opening": leg,
                    "description": description}
        except Exception as exc:
            LOGGER.warning("PDP fetch failed for %s from %s: %s", handle, host, exc)
    return {"rise": "", "inseam": "", "leg_opening": "", "description": ""}


# ===========================================================================
# Style Name derivation
# ===========================================================================

def derive_style_name_base(product_title: str) -> str:
    """Steps 1 and 2 of Style Name rules (Edyson-specific)."""
    text = product_title
    # Step 1: replace "-" with space
    text = text.replace("-", " ")
    # Remove quotes
    text = text.replace('"', " ").replace("“", " ").replace("”", " ")
    # Remove digits
    text = re.sub(r"\b\d+\b", " ", text)
    # Step 2: remove styling phrases (longest first)
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        # Handle "Jean" to also remove "Jeans"
        if phrase == "Jean":
            text = re.sub(r"\bJeans?\b", " ", text, flags=re.IGNORECASE)
        elif phrase.endswith("."):
            text = re.sub(rf"\b{re.escape(phrase)}(?=\s|$)", " ", text, flags=re.IGNORECASE)
        else:
            text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    # Clean up dangling single-letter artifacts
    text = re.sub(r"(?<!\w)[A-Z]-\s*", "", text).strip()
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _jean_style_prefix(jean_style: str) -> str:
    """For rule 3.2b: return Jean Style up to ' from ' (e.g. 'Wide Leg' → 'Wide Leg', 'Straight from Thigh' → 'Straight')."""
    if " from " in jean_style:
        return jean_style.split(" from ")[0].strip()
    return jean_style.strip()


# ===========================================================================
# Color / Size extraction
# ===========================================================================

def extract_color_size(selected_options: List[Dict]) -> Tuple[str, str]:
    """Return (color, size) from selectedOptions list.
    Primary: use option name. Fallback: check value against VALID_SIZES.
    """
    opt_map_by_name: Dict[str, str] = {}
    for o in (selected_options or []):
        name = (o.get("name") or "").strip().lower()
        val = (o.get("value") or "").strip()
        opt_map_by_name[name] = val

    size_by_name = opt_map_by_name.get("size", "")
    color_by_name = opt_map_by_name.get("color", "")
    if size_by_name and color_by_name:
        return color_by_name, size_by_name

    # Fallback: check which option value is in VALID_SIZES
    vals = [o.get("value", "").strip() for o in (selected_options or [])]
    if len(vals) >= 2:
        if vals[0] in VALID_SIZES:
            return vals[1], vals[0]
        if vals[1] in VALID_SIZES:
            return vals[0], vals[1]

    # Last resort: return by name even if one is blank
    return color_by_name or (vals[1] if len(vals) > 1 else ""), size_by_name or (vals[0] if vals else "")


# ===========================================================================
# Product Type derivation
# ===========================================================================

def derive_product_type(title: str, description: str, tags_str: str) -> str:
    """4-step product type derivation."""
    # Step 1: from title — direct word-boundary regex for reliability
    if re.search(r'\bjeans?\b', title, re.IGNORECASE):
        return "Jeans"
    # Step 2: from description
    if contains_phrase(description, "Jean") or contains_phrase(description, "Jeans"):
        return "Jeans"
    # "Denim" → Jeans, but NOT if "Non Denim" is also present
    if contains_phrase(description, "Denim") and not contains_phrase(description, "Non Denim"):
        return "Jeans"
    # Step 3: from tags
    if contains_phrase(tags_str, "Jean") or contains_phrase(tags_str, "Jeans"):
        return "Jeans"
    if contains_phrase(tags_str, "Non Denim"):
        return "Pants"
    # Step 4: default
    return "Pants"


# ===========================================================================
# Jean Style derivation
# ===========================================================================

def _straight_bucket(leg_opening: str) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17.5:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def _map_jean_from_title(title: str, description: str, leg_opening: str) -> str:
    t = title.lower()
    d = description.lower()
    # Barrel (check before Straight since "curved straight" → Barrel)
    if text_has_any(t, ("barrel", "barrell", "bowed", "bow leg", "stovepipe",
                        "stove-pipe", "curved straight", "horseshoe")):
        return "Barrel"
    # Tapered
    if text_has_any(t, ("tapered", "relaxed skinny", "mom")):
        return "Tapered"
    # Straight checks (before Baggy — Straight takes priority in title)
    if contains_phrase(t, "straight"):
        if text_has_any(d, ("relaxed straight-leg", "relaxed straight", "wide straight")):
            return "Straight from Thigh"
        bucket = _straight_bucket(leg_opening)
        if bucket:
            return bucket
    # Baggy
    if contains_phrase(t, "baggy"):
        return "Baggy"
    # Bootcut
    if text_has_any(t, ("bootcut", "boot-cut", "boot", "slim flare", "slim kick flare")):
        return "Bootcut"
    # Flare
    if contains_phrase(t, "flare"):
        return "Flare"
    # Skinny
    if contains_phrase(t, "skinny"):
        return "Skinny"
    # Wide Leg
    if text_has_any(t, ("wide leg", "wide-leg", "trouser")):
        return "Wide Leg"
    # Boyfriend
    if contains_phrase(t, "boyfriend"):
        return "Boyfriend"
    # Cigarette
    if contains_phrase(t, "cigarette"):
        return "Straight from Knee"
    # Straight + classic description cues
    if contains_phrase(t, "straight"):
        if text_has_any(d, ("classic straight-leg", "slim straight", "slim-straight",
                             "classic straight fit", "cigarette")):
            return "Straight from Knee"
    return ""


def _map_jean_from_tags(tags_str: str, leg_opening: str) -> str:
    t = tags_str.lower()
    if text_has_any(t, ("filter_style_barrel", "barrel", "barrell", "bowed",
                        "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if text_has_any(t, ("filter_style_skinny", "filter_style_superskinny", "skinny")):
        return "Skinny"
    if text_has_any(t, ("filter_style_flare", "flare")):
        return "Flare"
    if text_has_any(t, ("filter_style_boot", "bootcut", "boot cut", "boot-cut")):
        return "Bootcut"
    if text_has_any(t, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(t, ("filter_style_wide", "wide leg", "wide-leg", "wideleg", "palazzo")):
        return "Wide Leg"
    if contains_phrase(t, "filter_style_cigarette"):
        return "Straight from Knee"
    lo = to_float(leg_opening)
    if text_has_any(t, ("filter_style_straight", "straight")):
        return _straight_bucket(leg_opening)
    if contains_phrase(t, "baggy"):
        return "Baggy"
    if contains_phrase(t, "boyfriend"):
        return "Boyfriend"
    return ""


def _map_jean_from_description(desc: str, leg_opening: str) -> str:
    d = desc.lower()
    if text_has_any(d, ("barrel", "barrell", "bowed", "bow leg", "stovepipe",
                        "stove-pipe", "horseshoe")):
        return "Barrel"
    if contains_phrase(d, "skinny"):
        return "Skinny"
    if text_has_any(d, ("wide leg", "wide-leg", "palazzo")):
        return "Wide Leg"
    if contains_phrase(d, "flare"):
        return "Flare"
    if text_has_any(d, ("bootcut", "boot-cut", "boot cut")):
        return "Bootcut"
    if text_has_any(d, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(d, ("baggy", "cargo")):
        return "Baggy"
    has_straight = contains_phrase(d, "straight")
    if has_straight:
        if text_has_any(d, ("relaxed straight-leg", "relaxed straight", "wide straight")):
            return "Straight from Thigh"
        return _straight_bucket(leg_opening)
    return ""


def _map_jean_from_style_name(style_name: str, leg_opening: str) -> str:
    """Infer Jean Style from derived style name (step 3 fallback)."""
    sn = style_name.lower()
    if text_has_any(sn, ("barrel", "curved straight", "horseshoe", "stovepipe")):
        return "Barrel"
    if text_has_any(sn, ("wide leg",)):
        return "Wide Leg"
    if contains_phrase(sn, "bootcut"):
        return "Bootcut"
    if contains_phrase(sn, "flare"):
        return "Flare"
    if contains_phrase(sn, "skinny"):
        return "Skinny"
    if text_has_any(sn, ("tapered", "mom")):
        return "Tapered"
    if contains_phrase(sn, "straight"):
        return _straight_bucket(leg_opening)
    if contains_phrase(sn, "baggy"):
        return "Baggy"
    if contains_phrase(sn, "boyfriend"):
        return "Boyfriend"
    return ""


def derive_jean_style(title: str, description: str, tags_str: str, leg_opening: str,
                      style_name: str = "") -> str:
    """Per-product steps: title → description → style name. Tags/siblings in post-processing."""
    # Step 1: title
    result = _map_jean_from_title(title, description, leg_opening)
    if result:
        return result
    # Step 2: description
    result = _map_jean_from_description(description, leg_opening)
    if result:
        return result
    # Step 3: style name (fallback before post-processing sibling/tag passes)
    if style_name:
        result = _map_jean_from_style_name(style_name, leg_opening)
        if result:
            return result
    return ""


# ===========================================================================
# Inseam Label derivation
# ===========================================================================

NON_TAPER = {"Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
             "Boyfriend", "Baggy", "Flare", "Straight from Thigh"}
TAPER = {"Tapered", "Skinny", "Straight from Knee"}


def derive_inseam_label(jean_style: str, inseam: str, title: str, size: str) -> str:
    if "petite" in title.lower() or (size or "").upper().endswith("P"):
        return "Petite"
    val = to_float(inseam)
    if val is None:
        return "Regular"
    if jean_style in NON_TAPER and val >= 33:
        return "Long"
    if jean_style in TAPER and val >= 30:
        return "Long"
    return "Regular"


# ===========================================================================
# Inseam Style derivation
# ===========================================================================

def derive_inseam_style(jean_style: str, inseam: str, inseam_label: str) -> str:
    if not inseam:
        return ""
    val = to_float(inseam)
    if val is None:
        return ""
    is_petite = inseam_label == "Petite"

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
        if is_petite:
            if val <= 26:
                return "Cropped"
            return "Full Length"
        else:
            if val <= 27:
                return "Cropped"
            return "Full Length"

    return ""


# ===========================================================================
# Rise Label derivation
# ===========================================================================

def derive_rise_label(title: str, description: str) -> str:
    for src in (title, description):
        s = src.lower()
        if text_has_any(s, ("super low rise", "super low-rise", "ultra low rise",
                             "ultra low-rise", "super low waist", "super low-waist",
                             "ultra low waist", "ultra low-waist")):
            return "Ultra Low"
        if text_has_any(s, ("super high rise", "super high-rise", "ultra high rise",
                             "ultra high-rise", "super high waist", "super high-waist",
                             "ultra high waist", "ultra high-waist")):
            return "Ultra High"
        if text_has_any(s, ("mid-rise", "mid rise", "rise: mid", "rise - mid")):
            return "Mid"
        if text_has_any(s, ("low-rise", "low rise", "rise: low", "rise - low",
                             "hip-hugging fit", "sit comfortably on your hips",
                             "low on the hip", "low on the waist")):
            return "Low"
        if text_has_any(s, ("high-rise", "high rise", "high waist", "high-waist",
                             "rise: high", "rise - high", "high on the hip",
                             "high on the waist", "elevated waistline",
                             "elevated, cinched waistline")):
            return "High"
    # If both High Rise and mid-rise found, defer to rise measurement (handled in post-processing)
    return ""


# ===========================================================================
# Hem Style derivation
# ===========================================================================

def derive_hem_style(description: str) -> str:
    d = description.lower()
    if text_has_any(d, ("split hem", "side slits", "side-slits", "slit inseams at the hem",
                         "slit at the hem", "split hem", "slit hem")):
        return "Split Hem"
    if text_has_any(d, ("released hem", "undone finished hem", "undone hem", "released-hem")):
        return "Released Hem"
    if text_has_any(d, ("raw hem", "raw-edge hem", "raw edge hem", "raw-hem",
                         "raw-cut", "raw cut")):
        return "Raw Hem"
    if text_has_any(d, ("clean hem", "finished hem", "clean-edge hem",
                         "tacking detail at bottom hem", "3/8\" hem",
                         "clean finished hem", "clean-finished hem",
                         "hem: clean", "hem : clean")):
        return "Clean Hem"
    if text_has_any(d, ("wide hem", "wide-hem", "trouser hem")):
        return "Wide Hem"
    if text_has_any(d, ("distressed hem", "distressed-hem", "destroyed hem",
                         "destructed hem", "hem: destroyed", "hem : destroyed",
                         "hem : distressed", "hem: distressed")):
        return "Distressed Hem"
    if text_has_any(d, ("hem: frayed", "hem : frayed")):
        return "Frayed Hem"
    if contains_phrase(d, "zippers at the hem"):
        return "Zipper Hem"
    if text_has_any(d, ("rolled up", "roll up")):
        return "Rolled Hem"
    return ""


# ===========================================================================
# Color - Standardized derivation
# ===========================================================================

_COLOR_STD_MAPPING = [
    (("animal print", "leopard", "snake", "camo"), "Animal Print"),
    (("blue", "bleu", "blues", "navy", "indigo"), "Blue"),
    (("tan", "sand", "buff", "cement", "ginger", "sable", "beige", "khaki"), "Tan"),
    (("brown", "cinnamon", "camel", "chocolate", "pecan", "oak", "coffee", "espresso"), "Brown"),
    (("green", "olive", "cypress", "moss", "sage"), "Green"),
    (("grey", "gray", "stone"), "Gray"),
    (("orange",), "Orange"),
    (("pink", "blush", "coral"), "Pink"),
    (("purple", "violet"), "Purple"),
    (("red", "wine", "cherry", "burgundy"), "Red"),
    (("white", "ecru", "egret", "cream", "crème", "blizzard", "ivory",
      "parchment", "blanc"), "White"),
    (("yellow", "sunny"), "Yellow"),
    (("black", "noir", "onyx", "raven"), "Black"),
    (("dark", "medium", "light"), "Blue"),  # wash labels → Blue
]

_BLUE_WASH_PHRASES = (
    "dark base", "acid wash", "acid-wash", "dark rinse", "dark stretch denim",
    "dark wash", "dark washed", "dark vintage wash", "dark vintage inspired wash",
    "rich dark base", "medium base", "medium wash", "medium vintage wash",
    "medium rinse", "medium washed", "medium vintage inspired wash",
    "light wash", "light vintage wash", "light rinse", "light washed",
    "light vintage inspired wash", "season-ready wash", "medium-dark", "medium-light",
)


def derive_color_standardized(color: str, description: str) -> str:
    c = color.lower()
    for keys, out in _COLOR_STD_MAPPING:
        if text_has_any(c, keys):
            return out

    d = description.lower()
    desc_mapping = [
        (("animal print", "leopard", "snake"), "Animal Print"),
        (("brown",), "Brown"),
        (("green", "olive"), "Green"),
        (("grey", "gray", "smoke"), "Gray"),
        (("orange",), "Orange"),
        (("pink",), "Pink"),
        (("print", "stripes"), "Print"),
        (("purple", "maroon", "violet"), "Purple"),
        (("red", "wine", "burgundy"), "Red"),
        (("tan", "beige", "khaki"), "Tan"),
        (("white", "ecru", "pearly", "cream"), "White"),
        (("yellow",), "Yellow"),
        (("blue", "navy", "indigo"), "Blue"),
        (("black", "washed-black"), "Black"),
    ]
    for keys, out in desc_mapping:
        if text_has_any(d, keys):
            return out
    if text_has_any(d, _BLUE_WASH_PHRASES):
        return "Blue"
    return ""


# ===========================================================================
# Color - Simplified derivation
# ===========================================================================

def derive_color_simplified(color: str, description: str, standardized: str) -> str:
    s = standardized.lower()
    c = color.lower()
    d = description.lower()

    if text_has_any(s, ("black", "brown")):
        return "Dark"
    if text_has_any(s, ("white", "tan")):
        return "Light"

    if text_has_any(c, ("wine", "burgundy", "navy", "dark", "hunter green",
                         "deep", "midnight")):
        return "Dark"
    if text_has_any(c, ("pastel", "cream", "blush", "icy", "moonwashed", "light")):
        return "Light"
    if text_has_any(c, ("medium", "mid")):
        return "Medium"

    if text_has_any(d, ("medium light", "light to medium", "medium to light",
                         "light-to-medium", "medium-to-light", "medium-light",
                         "light-medium", "light/medium", "medium/light", "light medium")):
        return "Light to Medium"
    if text_has_any(d, ("medium to dark", "dark to medium", "dark-to-medium",
                         "medium-to-dark", "dark medium", "medium/dark", "dark/medium",
                         "medium-dark", "dark-medium")):
        return "Medium to Dark"
    if text_has_any(d, ("dark", "deep", "black", "wine", "burgundy", "midnight blue",
                         "forest green", "navy", "complex wash", "darker",
                         "deep yet tranquil hue", "deep, luxurious wash",
                         "deep, rich hue", "rich yet subtle", "rich, deep blue",
                         "urbane grey wash")):
        return "Dark"
    if text_has_any(d, ("light blue", "pale blue", "light vintage", "soft blue",
                         "soft pink", "ecru", "white", "acid wash", "acid-wash",
                         "light", "khaki", "tan", "ivory", "light gray wash",
                         "light silver-blue", "light wash", "lighter accents")):
        return "Light"
    if text_has_any(d, ("mid blue", "mid-blue", "medium stone wash",
                         "classic stone washed blue", "vintage washed blue",
                         "classic vintage blue", "medium blue", "medium wash",
                         "classic blue", "medium-blue wash", "mid-tone blue wash",
                         "perfectly blended wash")):
        return "Medium"
    return ""


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
query EdysonProducts($cursor: String) {
  collection(handle: "all-products") {
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
        description
        onlineStoreUrl
        totalInventory
        featuredImage { url }
        variants(first: 250) {
          nodes {
            id
            title
            availableForSale
            quantityAvailable
            price { amount }
            compareAtPrice { amount }
            barcode
            sku
            selectedOptions { name value }
            image { url }
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
            fatal = [e for e in payload["errors"]
                     if "ACCESS_DENIED" not in str(e.get("extensions", {}).get("code", ""))]
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
        for page in range(1, 25):
            url = f"{host}/collections/{COLLECTION_HANDLE}/products.json"
            try:
                resp = session.get(url, params={"limit": 250, "page": page}, timeout=30)
                if resp.status_code == 404:
                    break
                resp.raise_for_status()
                prods = resp.json().get("products", [])
                if not prods:
                    break
                for p in prods:
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
# Post-processing helpers
# ===========================================================================

IDX = {h: i for i, h in enumerate(CSV_HEADERS)}


def _col(row: List[str], name: str) -> str:
    return row[IDX[name]]


def _set(row: List[str], name: str, val: str) -> None:
    row[IDX[name]] = val


def apply_style_name_rules(rows: List[List[str]]) -> None:
    """Steps 3.1 and 3.2 of Style Name rules."""
    idx_product = IDX["Product"]
    idx_sn = IDX["Style Name"]
    idx_leg = IDX["Leg Opening"]
    idx_js = IDX["Jean Style"]

    # Rule 1: same first word + leg opening within 1.5" → most-frequent style name.
    # Maternity products are excluded. Blank leg openings are skipped.
    by_first: Dict[str, List[List[str]]] = {}
    for row in rows:
        title = row[idx_product]
        fw = (title.split(" ", 1)[0] if title else "").strip().lower()
        if fw:
            by_first.setdefault(fw, []).append(row)

    for fw, group in by_first.items():
        if len(group) < 2:
            continue
        non_mat = [r for r in group if "maternity" not in r[idx_product].lower()]
        if len(non_mat) < 2:
            continue
        leg_floats = [to_float(r[idx_leg]) for r in non_mat]

        # Compute target style name for each non-maternity row based on
        # compatible siblings (leg opening within 1.5"). Assignments are
        # calculated from original values to avoid cascade effects.
        assignments: List[Optional[str]] = []
        for i, row in enumerate(non_mat):
            lo_i = leg_floats[i]
            if lo_i is None:
                assignments.append(None)
                continue
            compatible_snames = [
                non_mat[j][idx_sn]
                for j, lo_j in enumerate(leg_floats)
                if lo_j is not None and abs(lo_j - lo_i) <= 1.5 and non_mat[j][idx_sn]
            ]
            if not compatible_snames or len(set(compatible_snames)) <= 1:
                assignments.append(None)
                continue
            multi_word = [sn for sn in compatible_snames if len(sn.split()) > 1]
            most_common = (max(set(multi_word), key=multi_word.count) if multi_word
                           else max(set(compatible_snames), key=compatible_snames.count))
            assignments.append(most_common)

        for row, target in zip(non_mat, assignments):
            if target is not None:
                _set(row, "Style Name", target)

    # Rule 2: one-word style names
    for row in rows:
        sn = _col(row, "Style Name").strip()
        if not sn or len(sn.split()) != 1:
            continue
        title = row[idx_product]
        fw = (title.split(" ", 1)[0] if title else "").strip().lower()
        leg = row[idx_leg]
        # 2a: siblings with same first word + leg opening that have multi-word style name
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
        # 2b: add Jean Style prefix to Style Name
        js = _col(row, "Jean Style")
        if js:
            prefix = _jean_style_prefix(js)
            _set(row, "Style Name", f"{sn} {prefix}".strip())
        # 2c: keep as singular word if Jean Style blank


def apply_jean_style_inference(rows: List[List[str]]) -> None:
    """Step 3: infer Jean Style from siblings sharing same PRODUCT_TITLE_NO_STYLING + Leg Opening."""
    # Build stripped title for each row
    stripped: List[str] = [derive_style_name_base(_col(r, "Product")) for r in rows]

    for i, row in enumerate(rows):
        if _col(row, "Jean Style"):
            continue
        my_stripped = stripped[i]
        leg = _col(row, "Leg Opening")
        if not my_stripped:
            continue
        matches = [
            _col(rows[j], "Jean Style")
            for j, s in enumerate(stripped)
            if s == my_stripped
            and _col(rows[j], "Leg Opening") == leg
            and _col(rows[j], "Jean Style")
        ]
        if matches:
            _set(row, "Jean Style", max(set(matches), key=matches.count))


def apply_jean_style_from_tags(rows: List[List[str]]) -> None:
    """Fill blank Jean Style from tags (post-processing step between the two sibling passes)."""
    for row in rows:
        if _col(row, "Jean Style"):
            continue
        tags_str = _col(row, "Tags")
        leg_opening = _col(row, "Leg Opening")
        result = _map_jean_from_tags(tags_str, leg_opening)
        if result:
            _set(row, "Jean Style", result)


def apply_inseam_label_style_refresh(rows: List[List[str]]) -> None:
    """Recompute Inseam Label and Inseam Style after Jean Style may have been updated."""
    for row in rows:
        js = _col(row, "Jean Style")
        title = _col(row, "Product")
        inseam = _col(row, "Inseam")
        size = _col(row, "Size")
        label = derive_inseam_label(js, inseam, title, size)
        _set(row, "Inseam Label", label)
        _set(row, "Inseam Style", derive_inseam_style(js, inseam, label))


def apply_rise_label_inference(rows: List[List[str]]) -> None:
    """Step 4: infer Rise Label from siblings sharing same Style Name."""
    # First: where both High and Mid appear in description, defer to Rise measurement
    for row in rows:
        rl = _col(row, "Rise Label")
        desc = _col(row, "Description").lower()
        if rl and contains_phrase(desc, "high rise") and contains_phrase(desc, "mid-rise"):
            rise_val = to_float(_col(row, "Rise"))
            if rise_val is not None:
                _set(row, "Rise Label", "High" if rise_val >= 12 else "Mid")

    # Then fill blanks from Style Name siblings
    for row in rows:
        if _col(row, "Rise Label"):
            continue
        sn = _col(row, "Style Name")
        rise = _col(row, "Rise")
        if not sn:
            continue
        matches = [
            (_col(r, "Rise Label"), _col(r, "Rise"))
            for r in rows
            if _col(r, "Style Name") == sn and _col(r, "Rise Label")
        ]
        if not matches:
            continue
        if len(set(m[0] for m in matches)) == 1:
            _set(row, "Rise Label", matches[0][0])
        else:
            # Pick closest rise measurement
            my_rise = to_float(rise)
            if my_rise is None:
                continue
            best_label = min(matches, key=lambda x: abs((to_float(x[1]) or 999) - my_rise))[0]
            _set(row, "Rise Label", best_label)


def apply_color_inference(rows: List[List[str]]) -> None:
    """Group-based color inference (step 3/4 of Color rules)."""
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
    """If same Style Name + Color + Inseam has petite and non-petite, blank petite inseam."""
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

class EdysonScraper:
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

        log("Fetching GraphQL products from 'all-products' collection...")
        products = fetch_graphql_products(self.session)

        log("Fetching products.json for handle deduplication...")
        json_handles = fetch_products_json_handles(self.session)
        if json_handles:
            log("products.json handles: %s (GraphQL: %s)", len(json_handles), len(products))

        log("Filtering products by title...")
        before = len(products)
        products = [p for p in products if not should_filter_product(p.get("title", ""))]
        log("Filtered %s → %s products", before, len(products))

        all_handles = [p.get("handle", "") for p in products if p.get("handle")]

        log("Fetching PDP pages and building rows...")
        for idx, product in enumerate(products, start=1):
            handle = product.get("handle", "")
            if not handle:
                continue

            product_id = (product.get("id") or "").replace("gid://shopify/Product/", "")
            title = product.get("title") or ""
            published_at = format_date(product.get("publishedAt"))
            created_at = format_date(product.get("createdAt"))
            tags = product.get("tags") or []
            tags_str = join_tags(tags)
            vendor = product.get("vendor") or ""
            online_store_url = (product.get("onlineStoreUrl") or
                                f"{PDP_HOST}/products/{handle}")
            image_url = ((product.get("featuredImage") or {}).get("url") or "")
            total_inventory = product.get("totalInventory")

            # Fetch PDP for measurements and description
            pdp = self._pdp(handle)
            rise = pdp.get("rise", "")
            inseam = pdp.get("inseam", "")
            leg_opening = pdp.get("leg_opening", "")
            # Use PDP description (CSS selector concat); fall back to GraphQL plain text
            description = pdp.get("description", "") or (product.get("description") or "").strip()

            # Variants
            variants = (product.get("variants") or {}).get("nodes") or []
            if not variants:
                log("No variants for %s, skipping", handle)
                continue

            # Derive product-level fields
            product_type = derive_product_type(title, description, tags_str)
            style_name = derive_style_name_base(title)
            jean_style = derive_jean_style(title, description, tags_str, leg_opening, style_name)
            rise_label = derive_rise_label(title, description)
            hem_style = derive_hem_style(description)

            for v in variants:
                v_id_full = v.get("id") or ""
                sku_shopify = v_id_full.replace("gid://shopify/ProductVariant/", "")
                sku_brand = v.get("sku") or ""
                barcode = v.get("barcode") or ""
                available = "TRUE" if v.get("availableForSale") else "FALSE"
                qty_avail = v.get("quantityAvailable")
                qty_avail_str = str(qty_avail) if qty_avail is not None else ""

                opts = v.get("selectedOptions") or []
                color, size = extract_color_size(opts)

                variant_title = f"{title} / {color} / {size}" if (color and size) else title

                price_obj = v.get("price") or {}
                price = format_price(
                    price_obj.get("amount") if isinstance(price_obj, dict) else price_obj)
                cmp_obj = v.get("compareAtPrice")
                compare_at = format_price(
                    (cmp_obj or {}).get("amount") if isinstance(cmp_obj, dict) else cmp_obj)

                v_image = ((v.get("image") or {}).get("url") or "")
                effective_image = v_image or image_url

                color_std = derive_color_standardized(color, description)
                color_simp = derive_color_simplified(color, description, color_std)

                inseam_label = derive_inseam_label(jean_style, inseam, title, size)
                inseam_style = derive_inseam_style(jean_style, inseam, inseam_label)

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
                _set(row, "Quantity of style",
                     str(total_inventory) if total_inventory is not None else "")
                _set(row, "SKU - Shopify", sku_shopify)
                _set(row, "SKU - Brand", sku_brand)
                _set(row, "Barcode", barcode)
                _set(row, "Image URL", effective_image)
                _set(row, "SKU URL", online_store_url)
                _set(row, "Jean Style", jean_style)
                _set(row, "Inseam Label", inseam_label)
                _set(row, "Inseam Style", inseam_style)
                _set(row, "Rise Label", rise_label)
                _set(row, "Hem Style", hem_style)
                _set(row, "Color - Simplified", color_simp)
                _set(row, "Color - Standardized", color_std)
                rows.append(row)

            if idx % 5 == 0 or idx == len(products):
                log("Progress: %s/%s products processed (%s rows)", idx, len(products), len(rows))

        # Post-processing passes (order: style names → siblings → tags → siblings)
        log("Post-processing: petite inseam rule...")
        apply_petite_inseam_rule(rows)
        log("Post-processing: style name rules...")
        apply_style_name_rules(rows)
        log("Post-processing: Jean Style inference (pass 1: siblings)...")
        apply_jean_style_inference(rows)
        log("Post-processing: Jean Style from tags...")
        apply_jean_style_from_tags(rows)
        log("Post-processing: Jean Style inference (pass 2: siblings)...")
        apply_jean_style_inference(rows)
        log("Post-processing: Inseam Label/Style refresh...")
        apply_inseam_label_style_refresh(rows)
        log("Post-processing: Rise Label inference...")
        apply_rise_label_inference(rows)
        log("Post-processing: Color inference...")
        apply_color_inference(rows)

        return rows

    def write_csv(self, rows: List[List[str]]) -> Path:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = OUTPUT_DIR / f"EDYSON_{ts}.csv"
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADERS)
            writer.writerows(rows)
        log("Wrote %s rows to %s", len(rows), path)
        return path

    def run(self) -> Path:
        log("=== Edyson inventory run started ===")
        rows = self.build_rows()
        path = self.write_csv(rows)
        log("=== Run complete: %s ===", path)
        print(f"Done — {path}")
        return path


def main() -> None:
    EdysonScraper().run()


if __name__ == "__main__":
    main()
