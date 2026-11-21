# -*- coding: utf-8 -*-
"""Ramy Brook inventory scraper — Storefront GraphQL + Algolia + Swym + PDP."""
from __future__ import annotations

import argparse
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
from zoneinfo import ZoneInfo

import requests
import urllib3
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GRAPHQL_URL   = "https://ramybrook-ocm-dev.myshopify.com/api/unstable/graphql.json"
# Tried in order. The first has inventory access; the backup only returns
# product data, in which case inventory falls back to Algolia.
GRAPHQL_TOKENS = [
    "bccb7998f634f1518fd6959f60d8e60c",
    "78e4e572d236efcd96d9d3a6853f27c1",
]
# Product pages are only ever fetched from www: the other hosts share its rate
# limiter, so rotating between them would not get around a block.
PDP_HOST      = "https://www.ramybrook.com"
COLLECTION_HANDLE = "jeans"

ALGOLIA_APP_ID    = "A3H9DJFT2H"
ALGOLIA_API_KEY   = "0ab3695a0f8a0d0dc29a11561629b41b"
ALGOLIA_INDEX     = "shopify_products"
ALGOLIA_COLLECTION_FILTER = "collections:jeans"
_alg = ALGOLIA_APP_ID.lower()
ALGOLIA_HOSTS = [
    f"https://{_alg}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query",
    f"https://{_alg}-1.algolia.net/1/indexes/{ALGOLIA_INDEX}/query",
    f"https://{_alg}-2.algolia.net/1/indexes/{ALGOLIA_INDEX}/query",
    f"https://{_alg}-3.algolia.net/1/indexes/{ALGOLIA_INDEX}/query",
    f"https://{_alg}.algolia.net/1/indexes/{ALGOLIA_INDEX}/query",
]

SWYM_PID      = "JYxW0bO//HIl29BRB2i1vARfPY5YSr+7Xdr/iqq8FgE="
SWYM_API_BASE = "https://swymstore-v3premium-01.swymrelay.com"
ET_WISHLIST   = 4

INSTORE_LOCATIONS = ["31061344320", "31223316544", "60898017344", "64122552384"]
ONLINE_LOCATION   = "30839832640"
INCOMING_LOCATION = "60935602240"

# Product-page (PDP) fetching. Next Shipment is only published on the PDP, so
# every product page is read once per run. Most Shopify sites sit behind
# Cloudflare, where a block comes back as a 403 or 429 bot challenge that
# waiting won't clear. So the pace is global and adapts both ways, waits are
# capped, each handle gets at most two tries in the whole run, and the phase
# stops once the site is clearly refusing us. Same approach as
# Goodamerican_inventory.py, plus 403 handling and the consecutive-block stop.
PDP_MIN_INTERVAL    = 0.5   # floor: seconds between any two /products/ requests
PDP_MAX_INTERVAL    = 6.0   # ceiling once blocks have forced the pace down
PDP_INTERVAL_STEP   = 0.75  # added to the interval after each block
PDP_INTERVAL_DECAY  = 0.9   # multiplier applied after each clean response
PDP_BLOCK_MAX_WAIT  = 15.0  # cap: a challenge outlasts any wait worth making
PDP_HANDLE_ATTEMPTS = 2     # fetch attempts per handle across the whole run
PDP_GET_RETRIES     = 2     # GETs per attempt (one retry after a block)
PDP_BLOCK_SLEEP     = 5.0   # wait after a block with no usable Retry-After
# Ramy Brook's Cloudflare answers a blocked request with 403 as well as 429.
PDP_BLOCK_STATUSES  = (403, 429)
# After this many pages in a row are blocked, stop requesting product pages for
# the rest of the run: retrying a site that is refusing us only makes the
# block worse and turns a one-minute run into ten. Remaining pages are reported.
PDP_MAX_CONSECUTIVE_BLOCKS = 5

# A real Chrome never sends a User-Agent without the rest of these; a lone
# User-Agent is a well-known trigger for Cloudflare's managed challenge.
BROWSER_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": ('"Chromium";v="122", "Not(A:Brand";v="24", '
                  '"Google Chrome";v="122"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# What the scraper sent before the ground-rules rewrite: a User-Agent only.
MINIMAL_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/124.0.0.0 Safari/537.36"),
}
# Chosen with --pdp-headers. Which one a site's bot protection accepts depends
# on the machine making the request; ramybrook_pdp_probe.py tests both.
PDP_HEADER_PROFILES = {"browser": BROWSER_HEADERS, "minimal": MINIMAL_HEADERS}

CENTRAL_TZ = ZoneInfo("America/Chicago")

SLEEP = 0.3

BASE_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE   = BASE_DIR / "ramybrook_inventory.log"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# CSV headers
# ---------------------------------------------------------------------------
CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Updated At",
    "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size",
    "Rise", "Inseam", "Leg Opening",
    "Price", "Compare at Price", "Available for Sale",
    "Quantity Available",
    "Quantity Available (Instore Inventory)",
    "Quantity Available (Online Inventory)",
    "Quantity Available (Incoming Inventory)",
    "Quantity of style",
    "Daily Purchases",
    "Purchase Reset Date",
    "Wishlist count",
    "Next Shipment",
    "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL",
    "Jean Style", "Inseam Label", "Inseam Style", "Rise Label",
    "Color - Simplified", "Color - Standardized",
]

# ---------------------------------------------------------------------------
# Filter words — product removed if title OR productType contains any of these
# ---------------------------------------------------------------------------
FILTER_WORDS: List[str] = [
    "Accessories", "Accessory", "Bermuda", "Bermudas", "Blazer", "Blazers",
    "Blouse", "Blouses", "Bodysuit", "Bodysuits", "Capri", "Cardigan",
    "Cardigans", "Clothing Top", "Clothing Tops", "Coat", "Coats",
    "Coats & Jackets", "Core Handbags", "Corset", "Corsets", "Crop Top",
    "Crop Tops", "Denim Short", "Denim Shorts", "Dress", "Dresses",
    "Fashion Core Handbag", "Fashion Core Handbags", "Fashion Handbag",
    "Fashion Handbags", "Goodies Accessories", "Goodies Accessory", "Handbag",
    "Heel", "Heels", "Hoodie", "Hoodies", "Jacket", "Jackets",
    "Jogger Short", "Jogger Shorts", "Jumpsuit", "Jumpsuits",
    "Long Sleeve", "Long Sleeves", "Neck", "One Piece", "One Pieces",
    "One-Piece", "One-Pieces", "Outerwear", "Pant", "Pant Suit",
    "Pant Suits", "Pants", "Purse", "Romper", "Rompers", "Sandel", "Sandle",
    "Shacket", "Shipping Protection", "Shirt", "Shirts", "Shirts & Tops",
    "Shoe", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Suit", "Suits",
    "Sweat", "Sweater", "Sweaters", "Sweatpant", "Sweatpants", "Sweats",
    "Sweatshirt", "Sweatshirts", "Swim", "T Shirt", "T Shirts", "Tank",
    "Tank Tops", "Tee", "Tees", "Top", "Tops", "Tote", "Trench",
    "T-shirt", "T-Shirts", "Vest", "Vests", "Zip Up",
]

# ---------------------------------------------------------------------------
# Style Name removal phrases
# ---------------------------------------------------------------------------
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "1999", "5-Pocket", "Accent Hardware", "Ankle", "Beaded", "Belted",
    "Braided", "Cargo", "Carpenter", "Chap", "Checkered", "Coated",
    "Constructed", "Contrast", "Corduroy", "Crochet", "Crop", "Cropped",
    "Crushed", "Crystal", "Cuff", "Cuffed", "Cutoff", "Cut-Out", "Darted",
    "Destroyed", "Distressed", "Drawstring", "Embroidery", "Embroidered", "Faux", "Fit",
    "Flag", "Flap Pocket", "Flap", "Flip", "Floral", "Frayed Seam",
    "Front Yoke", "Frontier", "Graffitimetalik", "High Rise", "High Waisted",
    "High-Rise", "Inch", "Inset", "Jean W/ Slit Hem", "Jean", "Krushed",
    "Krystal", "Leather", "Lightweight", "Lo", "Long", "Low And Loose",
    "Low Rise", "Low Waised", "Low-Rise", "Mid Rise", "Mid Waisted",
    "Mid-Rise", "Ms.", "Panel", "Pant", "Pants", "Patch", "Petite",
    "Pintucked", "Plaid", "Pleated", "Plus", "Pocket Pant", "Pocket",
    "Poplin", "Printed", "Raw Hem", "Renaissance", "Repair", "Retro",
    "Rinse", "Ripped", "Rolled Hem", "Saddle", "Seam", "Seamed Front Yoke",
    "Seamed", "Selvedge", "Sequin", "Side Seam Snaps", "Slice", "Slit",
    "Snake Print", "Sneaker Length", "Sott", "Spark", "Sparkle", "Spliced",
    "Split", "Stacked Waist", "Stacked", "Stitched", "Stoned",
    "Studded", "Suede", "Super", "The", "Track Pant", "Trashed", "Trim",
    "Trouser Jean", "Trouser", "Ultra", "Vegan Leather", "Velvet", "Vent",
    "V-High Rise", "Vintage", "W/ Contrast Front Panel", "W/ Cuff",
    "W/ Flap Jean", "W/ Slit Hem", "W/ Stud Detailing", "W/ Wide Cuff",
    "W/Flap", "Wax", "Welt Pocket", "With Cuff", "With Frayed Seam",
]

# ---------------------------------------------------------------------------
# Valid size values
# ---------------------------------------------------------------------------
VALID_SIZES: Set[str] = {
    "00", "0", "2", "4", "6", "8", "10", "12", "14",
    "15 Plus", "16 Plus", "18 Plus", "20 Plus", "22 Plus",
    "24 Plus", "26 Plus", "28 Plus", "30 Plus", "32 Plus",
    "XS", "S", "M", "L", "XL", "XXL", "1XL", "2XL", "3XL", "4XL", "5XL",
    "00-4", "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus",
    "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32",
    "33", "34", "35", "36", "37", "38", "39", "40",
}

NON_TAPER = {"Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
             "Boyfriend", "Baggy", "Flare", "Straight from Thigh", "Straight Leg"}
TAPER     = {"Tapered", "Skinny", "Straight from Knee"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("ramybrook")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for path in (LOG_FILE, OUTPUT_DIR / "ramybrook_inventory.log"):
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

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
FRACTION_UNICODE = {
    "¼": " 1/4", "½": " 1/2", "¾": " 3/4",
    "‘": " 1/8", "⅜": " 3/8", "⅝": " 5/8", "⅞": " 7/8",
}


def normalize_text(text: str) -> str:
    """Normalize Unicode fractions and smart quotes to ASCII equivalents."""
    if not text:
        return ""
    for sym, repl in FRACTION_UNICODE.items():
        text = text.replace(sym, repl)
    text = (text
            .replace("“", '"').replace("”", '"')
            .replace("‘", "'").replace("’", "'")
            .replace("″", '"').replace("′", "'")
            .replace("´", "'").replace(" ", " "))
    return text


def clean_description(raw: str) -> str:
    """Strip HTML, normalize quotes, collapse whitespace."""
    if not raw:
        return ""
    text = html.unescape(raw)
    text = re.sub(r"<[^>]+>", " ", text)
    text = normalize_text(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def format_date(value: Optional[str]) -> str:
    """ISO timestamp -> 'MM/DD/YYYY' (zero-padded, date as Shopify states it)."""
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return value
    return dt.strftime("%m/%d/%Y")


def format_datetime_central(value: Optional[str]) -> str:
    """ISO timestamp -> 'MM/DD/YYYY HH:MM:SS' (24-hour) in US Central time."""
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return ""
    return dt.astimezone(CENTRAL_TZ).strftime("%m/%d/%Y %H:%M:%S")


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
    return ", ".join(html.unescape(str(t)).strip() for t in tags if t and str(t).strip())


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
        return n / d if d else None
    try:
        return float(s)
    except ValueError:
        return None


def format_decimal(val: Optional[float]) -> str:
    if val is None:
        return ""
    return f"{val:.6f}".rstrip("0").rstrip(".")


def contains_phrase(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    pattern = re.escape(phrase.strip().lower()).replace(r"\ ", r"\s+")
    return bool(re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", text.lower()))


def text_has_any(text: str, phrases: Iterable[str]) -> bool:
    return any(contains_phrase(text, p) for p in phrases)


def should_filter_product(title: str, product_type: str) -> bool:
    for w in FILTER_WORDS:
        pat = r"(?<![a-zA-Z0-9])" + re.escape(w) + r"(?![a-zA-Z0-9])"
        if re.search(pat, title, re.IGNORECASE):
            return True
        if re.search(pat, product_type, re.IGNORECASE):
            return True
    return False

# ---------------------------------------------------------------------------
# Measurement extraction from GraphQL description
# ---------------------------------------------------------------------------

def _fit_section(description: str) -> str:
    """Return text starting from 'Fit Notes' or 'Size & Fit'."""
    norm = normalize_text(description)
    for marker in ("fit notes", "size & fit"):
        m = re.search(rf"(?<![a-z]){re.escape(marker)}(?![a-z])", norm, re.I)
        if m:
            return norm[m.start():]
    return norm


def _extract_measurement(fit_text: str, labels: List[str]) -> str:
    norm = normalize_text(fit_text)
    for label in labels:
        pattern = rf'\b{re.escape(label)}\b\s*:?\s*([\d]+(?:\s+\d+/\d+)?(?:\.\d+)?)\s*["]?'
        m = re.search(pattern, norm, re.IGNORECASE)
        if m:
            val = parse_mixed_fraction(m.group(1))
            if val is not None:
                return format_decimal(val)
    return ""


def extract_rise(description: str) -> str:
    fit = _fit_section(description)
    result = _extract_measurement(fit, ["Rise", "Front Rise"])
    if not result:
        result = _extract_measurement(fit, ["Rise Length"])
    return result


def extract_inseam(description: str) -> str:
    fit = _fit_section(description)
    result = _extract_measurement(fit, ["Inseam", "Length", "Inleg"])
    # If the result is ≤16" it likely captured a leg-opening; search for a
    # longer-form label followed by a number >16 instead.
    if result:
        try:
            if float(result) <= 16:
                norm = normalize_text(fit)
                for label in ("Inseam Length", "Length"):
                    m = re.search(
                        rf'\b{re.escape(label)}\s*:?\s*([\d]+(?:\s+\d+/\d+)?(?:\.\d+)?)',
                        norm, re.IGNORECASE)
                    if m:
                        v = parse_mixed_fraction(m.group(1))
                        if v is not None and v > 16:
                            result = format_decimal(v)
                            break
        except ValueError:
            pass
    return result


def extract_leg_opening(description: str) -> str:
    return _extract_measurement(_fit_section(description), ["Leg Opening"])

# ---------------------------------------------------------------------------
# Color / Size extraction
# ---------------------------------------------------------------------------

def extract_color_size(selected_options: List[Dict]) -> Tuple[str, str]:
    if not selected_options:
        return "", ""
    # Try by option name first
    by_name: Dict[str, str] = {}
    for o in selected_options:
        name = (o.get("name") or "").strip().lower()
        val  = (o.get("value") or "").strip()
        by_name[name] = val
    if "color" in by_name and "size" in by_name:
        return by_name["color"], by_name["size"]

    # Fallback: find the option whose value is in VALID_SIZES
    size_val  = ""
    color_val = ""
    size_idx  = -1
    for i, o in enumerate(selected_options):
        if (o.get("value") or "").strip() in VALID_SIZES:
            size_val = (o.get("value") or "").strip()
            size_idx = i
            break

    if size_idx != -1:
        # Color = first non-size option that doesn't look like a style code
        for i, o in enumerate(selected_options):
            if i == size_idx:
                continue
            val = (o.get("value") or "").strip()
            # Skip style codes like "D02265003"
            if not re.match(r'^[A-Za-z]\d{6,}', val):
                color_val = val
                break
        if not color_val:
            # Pick any non-size option
            for i, o in enumerate(selected_options):
                if i != size_idx:
                    color_val = (o.get("value") or "").strip()
                    break

    # Last resort: positional with old option1/option2 logic
    if not color_val and not size_val:
        vals = [(o.get("value") or "").strip() for o in selected_options]
        if len(vals) >= 2:
            if vals[0] in VALID_SIZES:
                return vals[1], vals[0]
            if vals[1] in VALID_SIZES:
                return vals[0], vals[1]
        return (vals[1] if len(vals) > 1 else ""), (vals[0] if vals else "")

    return color_val, size_val

# ---------------------------------------------------------------------------
# Style Name derivation
# ---------------------------------------------------------------------------

def derive_style_name_base(product_title: str) -> str:
    """Step 1: strip pipe/color suffixes. Step 2: remove styling words."""
    text = (product_title or "")
    text = text.split(" | ", 1)[0]   # strip Ramy Brook pipe-notation color suffix
    text = text.split(" - ", 1)[0]   # strip " - Color" suffix
    text = text.replace("Boot Cut", "Bootcut").strip()
    text = text.replace('"', " ").replace("“", " ").replace("”", " ")
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        if phrase.lower() == "jean":
            text = re.sub(r"\bJeans?\b", " ", text, flags=re.IGNORECASE)
        elif phrase.endswith("."):
            text = re.sub(rf"\b{re.escape(phrase)}(?=\s|$)", " ", text, flags=re.IGNORECASE)
        else:
            text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\b\d+\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Clean dangling single-letter artifacts
    text = re.sub(r"(?<!\w)[A-Z]-\s*", "", text).strip()
    return re.sub(r"\s+", " ", text).strip()

# ---------------------------------------------------------------------------
# Jean Style derivation
# ---------------------------------------------------------------------------

def _straight_bucket(leg_opening: str) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17.0:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def _map_jean_from_title(title: str, leg_opening: str) -> str:
    t = title.lower()
    if text_has_any(t, ("barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
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
    return ""


def _map_jean_from_description(desc: str, leg_opening: str) -> str:
    d = desc.lower()
    if text_has_any(d, ("barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if contains_phrase(d, "skinny"):
        return "Skinny"
    if contains_phrase(d, "flare"):
        return "Flare"
    if text_has_any(d, ("bootcut", "boot cut")):
        return "Bootcut"
    if text_has_any(d, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(d, ("wide leg", "wide-leg", "palazzo")):
        return "Wide Leg"
    if contains_phrase(d, "straight"):
        bucket = _straight_bucket(leg_opening)
        if bucket:
            return bucket
    if text_has_any(d, ("baggy", "loose fit", "loose jean", "relaxed leg")):
        return "Baggy"
    if contains_phrase(d, "straight"):
        return "Straight Leg"
    return ""


def _map_jean_from_algolia_cats(cats: List[str], leg_opening: str) -> str:
    combined = " ".join(cats).lower()
    if text_has_any(combined, ("barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if contains_phrase(combined, "skinny"):
        return "Skinny"
    if contains_phrase(combined, "flare"):
        return "Flare"
    if text_has_any(combined, ("bootcut", "boot")):
        return "Bootcut"
    if text_has_any(combined, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(combined, ("wide leg", "wide-leg", "palazzo")):
        return "Wide Leg"
    if text_has_any(combined, ("cigarette", "slim")):
        return "Straight from Knee"
    if contains_phrase(combined, "straight"):
        bucket = _straight_bucket(leg_opening)
        return bucket if bucket else "Straight Leg"
    if contains_phrase(combined, "baggy"):
        return "Baggy"
    return ""


def derive_jean_style(title: str, description: str, leg_opening: str,
                      algolia_cats: Optional[List[str]] = None) -> str:
    result = _map_jean_from_title(title, leg_opening)
    if result:
        return result
    result = _map_jean_from_description(description, leg_opening)
    if result:
        return result
    if algolia_cats:
        result = _map_jean_from_algolia_cats(algolia_cats, leg_opening)
        if result:
            return result
    return ""

# ---------------------------------------------------------------------------
# Inseam Label / Style derivation
# ---------------------------------------------------------------------------

def derive_inseam_label(jean_style: str, inseam: str, title: str,
                        size: str, description: str) -> str:
    t = title.lower()
    if "petite" in t:
        return "Petite"
    if (size or "").upper().endswith("P"):
        return "Petite"
    if "perfect for women under 5'4" in description.lower():
        return "Petite"
    val = to_float(inseam)
    if val is None:
        return "Regular"
    if jean_style in NON_TAPER and val >= 33:
        return "Long"
    if jean_style in TAPER and val >= 30:
        return "Long"
    return "Regular"


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
            return "Cropped" if val <= 26 else "Full Length"
        else:
            return "Cropped" if val <= 27.5 else "Full Length"

    return ""

# ---------------------------------------------------------------------------
# Rise Label derivation
# ---------------------------------------------------------------------------

def derive_rise_label(title: str, description: str, tags_str: str) -> str:
    # Step 1: title
    t = title.lower()
    if text_has_any(t, ("super low rise", "super low-rise", "ultra low rise", "ultra low-rise",
                         "super low waist", "super low-waist", "ultra low waist", "ultra low-waist")):
        return "Ultra Low"
    if text_has_any(t, ("super high rise", "super high-rise", "ultra high rise", "ultra high-rise",
                         "super high waist", "super high-waist", "ultra high waist", "ultra high-waist")):
        return "Ultra High"
    if text_has_any(t, ("mid-rise", "mid rise")):
        return "Mid"
    if text_has_any(t, ("low-rise", "low rise")):
        return "Low"
    if text_has_any(t, ("high-rise", "high rise")):
        return "High"

    # Step 2: description
    d = description.lower()
    if text_has_any(d, ("rise: super low", "rise: ultra low", "rise - super low", "rise - ultra low",
                         "super low rise", "super low-rise", "ultra low rise", "ultra low-rise",
                         "super low waist", "super low-waist", "ultra low waist", "ultra low-waist")):
        return "Ultra Low"
    if text_has_any(d, ("rise: super high", "rise: ultra high", "rise - super high", "rise - ultra high",
                         "super high rise", "super high-rise", "ultra high rise", "ultra high-rise",
                         "super high waist", "super high-waist", "ultra high waist", "ultra high-waist")):
        return "Ultra High"
    if text_has_any(d, ("rise: mid", "rise - mid", "mid-rise", "mid waisted", "mid-waisted",
                         "sits mid", "mid rise")):
        return "Mid"
    if text_has_any(d, ("rise: low", "rise - low", "low-rise", "low rise", "hip-hugging fit",
                         "sit comfortably on your hips", "low waisted", "low-waisted",
                         "sits low", "low on the hip", "low on the waist")):
        return "Low"
    if text_has_any(d, ("rise: high", "rise - high", "high-rise", "high rise", "high waist",
                         "high-waist", "high waisted", "high-waisted", "sits high",
                         "high on the hip", "high on the waist", "elevated waistline",
                         "elevated, cinched waistline")):
        return "High"

    # Step 3: tags
    tg = tags_str.lower()
    if text_has_any(tg, ("rise: super low", "super low", "rise: ultra low")):
        return "Ultra Low"
    if text_has_any(tg, ("rise: super high", "super high", "ultra rise", "rise: ultra high")):
        return "Ultra High"
    if text_has_any(tg, ("rise: high", "high rise", "rise:high-rise")):
        return "High"
    if text_has_any(tg, ("rise: mid", "mid rise", "rise:mid-rise")):
        return "Mid"
    if text_has_any(tg, ("rise: low", "low rise", "rise:low-rise")):
        return "Low"

    return ""

# ---------------------------------------------------------------------------
# Color - Standardized
# ---------------------------------------------------------------------------

def derive_color_standardized(color: str, product_title: str) -> str:
    c = color.lower()
    rules = [
        (("animal", "leopard", "snake", "camo"), "Animal Print"),
        (("blue", "bleu", "blues", "navy", "indigo"), "Blue"),
        (("tan", "sand", "buff", "cement", "ginger", "sable", "beige", "khaki"), "Tan"),
        (("brown", "cinnamon", "camel", "chocolate", "pecan", "oak", "coffee", "espresso"), "Brown"),
        (("green", "olive", "cypress", "moss", "clover", "sage"), "Green"),
        (("grey", "gray"), "Gray"),
        (("orange",), "Orange"),
        (("pink", "blush", "coral"), "Pink"),
        (("print", "checkered"), "Print"),
        (("purple", "blackberry", "violet"), "Purple"),
        (("red", "wine", "cherry", "chrimson", "crimson", "burgundy"), "Red"),
        (("white", "ecru", "egret", "cream", "creme", "blizzard", "ivory", "parchment", "blanc"), "White"),
        (("black", "noir", "onyx", "raven"), "Black"),
        (("wash", "rinse"), "Blue"),
    ]
    for keys, out in rules:
        if text_has_any(c, keys):
            return out
    # Yellow: check product title
    if text_has_any(product_title.lower(), ("yellow", "sunny")):
        return "Yellow"
    return ""

# ---------------------------------------------------------------------------
# Color - Simplified
# ---------------------------------------------------------------------------

def derive_color_simplified(color: str, standardized: str) -> str:
    s = standardized.lower()
    c = color.lower()
    # Step 1: by standardized
    if text_has_any(s, ("black", "brown")):
        return "Dark"
    if text_has_any(s, ("white", "tan")):
        return "Light"
    # Step 2: by color
    if text_has_any(c, ("wine", "burgundy", "navy", "dark", "hunter green",
                         "blackberry", "deep", "midnight")):
        return "Dark"
    if text_has_any(c, ("pastel", "cream", "moonwashed", "bleach", "bleached", "light")):
        return "Light"
    if text_has_any(c, ("medium", "mid")):
        return "Medium"
    return ""

# ---------------------------------------------------------------------------
# Next Shipment extraction from PDP
# ---------------------------------------------------------------------------

_MONTH_ABBRS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def _parse_shipment_date_part(part: str) -> str:
    part = part.strip()
    m = re.match(r"([A-Za-z]+)\s+(\d+)(?:st|nd|rd|th)?", part)
    if m:
        month_num = _MONTH_ABBRS.get(m.group(1).lower()[:3])
        day_num   = int(m.group(2))
        if month_num:
            return f"{month_num}/{day_num:02d}"
    return part


def _reformat_shipment_date(raw: str) -> str:
    parts = raw.split(" - ", 1)
    if len(parts) == 2:
        return f"{_parse_shipment_date_part(parts[0])} - {_parse_shipment_date_part(parts[1])}"
    return raw


# ---------------------------------------------------------------------------
# Product page (PDP) fetching
# ---------------------------------------------------------------------------

_PDP_PAGE_CACHE: Dict[str, Optional[str]] = {}
# Status per handle, so a missing page (404) can be told apart from a failed
# request. Only an exact 404 means the product is gone.
_PDP_STATUS_CACHE: Dict[str, Optional[int]] = {}
# Handles whose PDP was blocked (403/429). Deliberately kept out of the caches
# so a later pass can retry, and reported at the end so a run that lost data
# says so.
_PDP_THROTTLED: Set[str] = set()
# Kind of block seen per handle (e.g. "HTTP 403, Cloudflare challenge"), so the
# log says *how* the site refused, not just that it did.
_PDP_BLOCK_KIND: Dict[str, str] = {}
# Fetch attempts per handle, so a page the challenge keeps blocking costs two
# attempts rather than being re-requested indefinitely.
_PDP_ATTEMPTS: Dict[str, int] = {}
_PDP_COUNTS: Dict[str, int] = {"pdp_pages": 0, "pages_read": 0, "http_gets": 0, "blocked": 0,
                               "browser_pages": 0, "browser_read": 0}
# "floor" is what clean responses decay back to; --pdp-interval sets it.
_PACE: Dict[str, float] = {"interval": PDP_MIN_INTERVAL, "floor": PDP_MIN_INTERVAL, "last": 0.0}


def make_pdp_session(profile: str = "browser") -> requests.Session:
    """Session for www product pages: browser headers, no automatic retries.

    The shared session's urllib3 Retry would silently retry 429s and honour any
    Retry-After in full, which is exactly what a bot challenge punishes. Here
    _get_with_backoff handles blocks itself.
    """
    session = requests.Session()
    session.headers.update(PDP_HEADER_PROFILES[profile])
    return session


def _pace_pdp_request() -> None:
    """Hold every /products/ request to the current global interval."""
    wait = _PACE["last"] + _PACE["interval"] - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _PACE["last"] = time.monotonic()


def describe_block(response) -> str:
    """Short description of how the site refused a request, for the log."""
    headers = {k.lower(): v for k, v in (response.headers or {}).items()}
    body = (response.text or "")[:5000]
    if headers.get("cf-mitigated"):
        kind = f"Cloudflare {headers['cf-mitigated']}"
    elif "Verifying your connection" in body:
        kind = "Shopify bot check (Verifying your connection)"
    elif "Just a moment" in body or "challenge-platform" in body:
        kind = "Cloudflare challenge page"
    else:
        title = re.search(r"<title[^>]*>(.*?)</title>", body, re.S | re.I)
        if title:
            title_text = re.sub(r"\s+", " ", title.group(1)).strip()[:60]
            kind = f"page titled '{title_text}'"
        else:
            kind = f"{len(response.text or '')}-byte response"
    return f"HTTP {response.status_code}, {kind}"


def _get_with_backoff(session: requests.Session, url: str, **kwargs):
    """Paced GET. On a block (403/429): slow the global pace, wait (capped),
    retry once.

    The pace adapts in both directions -- clean responses decay it back toward
    the floor -- so one bad patch does not slow the rest of the run.
    """
    response = None
    for attempt in range(PDP_GET_RETRIES):
        _pace_pdp_request()
        _PDP_COUNTS["http_gets"] += 1
        response = session.get(url, timeout=30, **kwargs)
        if response.status_code not in PDP_BLOCK_STATUSES:
            _PACE["interval"] = max(_PACE["floor"],
                                    _PACE["interval"] * PDP_INTERVAL_DECAY)
            return response
        _PDP_COUNTS["blocked"] += 1
        _PACE["interval"] = min(max(PDP_MAX_INTERVAL, _PACE["floor"]),
                                _PACE["interval"] + PDP_INTERVAL_STEP)
        if attempt + 1 >= PDP_GET_RETRIES:
            break
        retry_after = response.headers.get("Retry-After", "").strip()
        wait = (float(retry_after) if retry_after.replace(".", "", 1).isdigit()
                else PDP_BLOCK_SLEEP * (attempt + 1))
        # Retry-After assumes a rate limit the server will forgive; a challenge
        # is not forgiven on a timer, so a long wait only slows the run.
        wait = min(wait, PDP_BLOCK_MAX_WAIT)
        LOGGER.warning("Blocked (%s) on %s; waiting %.1fs then retrying once "
                       "(pace now %.1fs)", describe_block(response), url, wait,
                       _PACE["interval"])
        time.sleep(wait)
    return response


def fetch_product_page(session: requests.Session, handle: str) -> Optional[str]:
    """The PDP HTML, fetched once per handle and shared by every lookup.

    Returns None when the page could not be read; check _PDP_STATUS_CACHE /
    _PDP_THROTTLED for why. A block (403/429) is never cached as "missing".
    """
    if handle in _PDP_PAGE_CACHE:
        return _PDP_PAGE_CACHE[handle]
    if _PDP_ATTEMPTS.get(handle, 0) >= PDP_HANDLE_ATTEMPTS:
        return None
    _PDP_ATTEMPTS[handle] = _PDP_ATTEMPTS.get(handle, 0) + 1
    page: Optional[str] = None
    status: Optional[int] = None
    try:
        _PDP_COUNTS["pdp_pages"] += 1
        response = _get_with_backoff(session, f"{PDP_HOST}/products/{handle}")
        status = response.status_code
        if status == 200:
            page = response.text
            _PDP_COUNTS["pages_read"] += 1
        elif status in PDP_BLOCK_STATUSES:
            _PDP_BLOCK_KIND[handle] = describe_block(response)
            LOGGER.warning("PDP blocked for %s (%s)", handle, _PDP_BLOCK_KIND[handle])
        else:
            LOGGER.warning("PDP HTTP %s for %s", status, handle)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("PDP fetch failed for %s: %s", handle, exc)
    if status in PDP_BLOCK_STATUSES:
        _PDP_THROTTLED.add(handle)
        return None
    _PDP_THROTTLED.discard(handle)
    _PDP_BLOCK_KIND.pop(handle, None)
    _PDP_PAGE_CACHE[handle] = page
    _PDP_STATUS_CACHE[handle] = status
    return page


def pdp_is_missing(session: requests.Session, handle: str) -> bool:
    """True only when the PDP genuinely 404s -- never for a failed request."""
    fetch_product_page(session, handle)
    return _PDP_STATUS_CACHE.get(handle) == 404


_MAIN_PRODUCT_JSON_RE = re.compile(
    r'<script[^>]*data-product-main-product-json[^>]*>(.*?)</script>', re.S)


def pdp_main_product(page: str, handle: str) -> Optional[Dict]:
    """The page's own product record, or None if it can't be found.

    The page also embeds other products (the 'cross_sell' list inside this
    same record, plus recommendation blocks), each with its own
    nextIncomingDate. So read the top-level record and check its handle,
    never the first match anywhere on the page.
    """
    candidates: List[Dict] = []
    m = _MAIN_PRODUCT_JSON_RE.search(page)
    if m:
        try:
            candidates.append(json.loads(m.group(1)))
        except ValueError:
            pass
    # Fallback: the theme also assigns the same record to window.BARREL.product
    pos = page.find("window.BARREL.product")
    if pos != -1:
        brace = page.find("{", pos)
        if brace != -1:
            try:
                candidates.append(json.JSONDecoder().raw_decode(page, brace)[0])
            except ValueError:
                pass
    for data in candidates:
        if isinstance(data, dict) and data.get("handle") == handle:
            return data
    return None


def parse_next_shipment(page: str, handle: str) -> Optional[str]:
    """Formatted Next Shipment ('' when the product has none), or None when
    the page's product record could not be read."""
    product = pdp_main_product(page, handle)
    if product is None:
        return None
    raw = product.get("nextIncomingDate")
    return _reformat_shipment_date(str(raw)) if raw else ""


# ---------------------------------------------------------------------------
# Browser (Playwright) fallback for product pages
# ---------------------------------------------------------------------------
# Ramy Brook's Cloudflare answers plain HTTP requests from some connections
# with a 403 "Verify your browser" challenge on nearly every product page. That
# challenge needs JavaScript to run in a real browser, so no header set gets
# past it. When plain requests keep getting blocked, the remaining pages are
# opened in a real browser instead: once the challenge clears, its clearance
# cookie covers the following pages in the same browser session.
PDP_BROWSER_CHALLENGE_WAIT = 30.0   # seconds to let a challenge clear (or be clicked)
PDP_BROWSER_NAV_TIMEOUT_MS = 45000
_CHALLENGE_TITLES = ("just a moment", "verify your browser", "verifying your connection",
                     "attention required")


class BrowserFetcher:
    """One real browser, one tab, pages loaded one at a time at the global pace."""

    def __init__(self, headless: bool) -> None:
        self.headless = headless
        self.unavailable_reason: Optional[str] = None
        self._pw = None
        self._browser = None
        self._page = None
        self._told_user = False

    def _start(self) -> bool:
        if self._page is not None:
            return True
        if self.unavailable_reason:
            return False
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # noqa: BLE001
            self.unavailable_reason = (f"Playwright is not installed ({exc}). Install it with: "
                                       "pip install playwright  then  playwright install chromium")
            return False
        try:
            self._pw = sync_playwright().start()
            args = ["--disable-blink-features=AutomationControlled"]
            launch_error = None
            # A real installed Chrome passes Cloudflare's check more readily than
            # Playwright's bundled Chromium, so try it first.
            for kwargs in ({"channel": "chrome"}, {}):
                try:
                    self._browser = self._pw.chromium.launch(headless=self.headless,
                                                             args=args, **kwargs)
                    break
                except Exception as exc:  # noqa: BLE001
                    launch_error = exc
            if self._browser is None:
                raise RuntimeError(launch_error)
            context_kwargs: Dict = {"locale": "en-US", "viewport": {"width": 1366, "height": 900}}
            if self.headless:
                # Headless Chrome announces itself as "HeadlessChrome"; drop that.
                major = (self._browser.version or "124").split(".")[0]
                context_kwargs["user_agent"] = (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36")
            context = self._browser.new_context(**context_kwargs)
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
            # Images, video and fonts aren't needed to read the product record.
            context.route("**/*", lambda route: route.abort()
                          if route.request.resource_type in ("image", "media", "font")
                          else route.continue_())
            self._page = context.new_page()
            log("Browser fallback started (%s, %s)", self._browser.version,
                "headless" if self.headless else "visible window")
            return True
        except Exception as exc:  # noqa: BLE001
            self.unavailable_reason = f"browser could not be started: {exc}"
            self.close()
            return False

    def fetch(self, handle: str) -> Tuple[str, Optional[str]]:
        """Returns (outcome, html): outcome is 'read', 'gone', 'blocked' or 'failed'."""
        if not self._start():
            return "failed", None
        url = f"{PDP_HOST}/products/{handle}"
        _pace_pdp_request()
        _PDP_COUNTS["browser_pages"] += 1
        try:
            response = self._page.goto(url, wait_until="domcontentloaded",
                                       timeout=PDP_BROWSER_NAV_TIMEOUT_MS)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Browser could not load %s: %s", handle, exc)
            return "failed", None
        status = response.status if response else None
        deadline = time.monotonic() + PDP_BROWSER_CHALLENGE_WAIT
        while True:
            try:
                page_html = self._page.content()
                title = (self._page.title() or "").strip().lower()
            except Exception:  # noqa: BLE001 -- page navigating (challenge reload)
                page_html, title = "", ""
            if page_html and pdp_main_product(page_html, handle) is not None:
                _PACE["interval"] = max(_PACE["floor"], _PACE["interval"] * PDP_INTERVAL_DECAY)
                return "read", page_html
            challenged = any(t in title for t in _CHALLENGE_TITLES)
            if status == 404 and not challenged:
                return "gone", None
            if challenged and not self.headless and not self._told_user:
                LOGGER.warning("Cloudflare is checking the browser. If a 'Verify you are "
                               "human' box appears in the browser window, click it once.")
                self._told_user = True
            if time.monotonic() >= deadline:
                _PACE["interval"] = min(max(PDP_MAX_INTERVAL, _PACE["floor"]),
                                        _PACE["interval"] + PDP_INTERVAL_STEP)
                return ("blocked" if challenged or status in PDP_BLOCK_STATUSES
                        else "failed"), None
            self._page.wait_for_timeout(1000)

    def close(self) -> None:
        for closer in (lambda: self._browser and self._browser.close(),
                       lambda: self._pw and self._pw.stop()):
            try:
                closer()
            except Exception:  # noqa: BLE001
                pass
        self._page = self._browser = self._pw = None


def _fmt_eta(seconds: float) -> str:
    seconds = max(0, int(round(seconds)))
    return f"{seconds // 60}m{seconds % 60:02d}s"


def fill_next_shipment(session: requests.Session, rows: List[List[str]],
                       browser_mode: str = "auto", headless: bool = False) -> Dict[str, List[str]]:
    """Read Next Shipment off each product's PDP and write it onto its rows.

    Next Shipment is not in GraphQL, Algolia, the collection page or the
    product .json, so every product with a blank Next Shipment needs its page.
    browser_mode: 'auto' = plain requests first, real browser for pages they
    can't get; 'always' = browser only; 'never' = plain requests only.
    Returns the handles that could not be read, by reason. Unread handles
    stay blank.
    """
    pending = sorted({_col(r, "Handle") for r in rows
                      if _col(r, "Handle") and not _col(r, "Next Shipment")})
    values: Dict[str, str] = {}
    # Final outcome per handle: read / gone / blocked / browser_blocked /
    # failed / no_record / not_attempted. Later attempts overwrite earlier ones.
    outcome: Dict[str, str] = {h: "not_attempted" for h in pending}
    total = len(pending)

    def _record(handle: str, page: Optional[str], how: str) -> None:
        if page is None:
            outcome[handle] = how
            return
        value = parse_next_shipment(page, handle)
        if value is None:
            outcome[handle] = "no_record"
        else:
            values[handle] = value
            outcome[handle] = "read"

    def _progress(label: str, i: int, n: int, started: float) -> None:
        if i % 10 == 0 or i == n:
            elapsed = time.monotonic() - started
            log("%s %s/%s (elapsed %s, ETA %s, pace %.1fs)", label, i, n,
                _fmt_eta(elapsed), _fmt_eta(elapsed / i * (n - i)), _PACE["interval"])

    # Phase 1: plain HTTP requests, stopped once the site is clearly refusing them.
    if browser_mode != "always":
        streak = 0
        started = time.monotonic()
        for i, handle in enumerate(pending, start=1):
            page = fetch_product_page(session, handle)
            if handle in _PDP_THROTTLED:
                _record(handle, None, "blocked")
                streak += 1
            else:
                status = _PDP_STATUS_CACHE.get(handle)
                _record(handle, page, "gone" if status == 404 else "failed")
                streak = 0
            _progress("PDP", i, total, started)
            if streak >= PDP_MAX_CONSECUTIVE_BLOCKS:
                LOGGER.warning("%s product pages in a row were blocked (%s); no more "
                               "plain requests this run", streak,
                               _PDP_BLOCK_KIND.get(handle, "blocked"))
                break
        # Without a browser to fall back on, give blocked pages one more plain try
        # if the site wasn't refusing everything.
        if browser_mode == "never" and _PDP_THROTTLED and streak < PDP_MAX_CONSECUTIVE_BLOCKS:
            retry = sorted(_PDP_THROTTLED)
            log("Retrying %s blocked PDP(s) once...", len(retry))
            for handle in retry:
                page = fetch_product_page(session, handle)
                if handle not in _PDP_THROTTLED:
                    _record(handle, page, "gone" if _PDP_STATUS_CACHE.get(handle) == 404
                            else "failed")

    # Phase 2: real browser for everything plain requests couldn't get.
    if browser_mode != "never":
        todo = [h for h in pending
                if outcome[h] in ("blocked", "not_attempted", "failed")]
        if todo:
            log("Opening %s product page(s) in a browser...", len(todo))
            fetcher = BrowserFetcher(headless=headless)
            try:
                streak = 0
                started = time.monotonic()
                for i, handle in enumerate(todo, start=1):
                    result, page = fetcher.fetch(handle)
                    if fetcher.unavailable_reason:
                        LOGGER.warning("Browser fallback unavailable: %s", fetcher.unavailable_reason)
                        break
                    if result == "read":
                        _PDP_THROTTLED.discard(handle)
                        _PDP_COUNTS["browser_read"] += 1
                        _record(handle, page, "read")
                        streak = 0
                    else:
                        _record(handle, None, {"blocked": "browser_blocked"}.get(result, result))
                        streak = streak + 1 if result == "blocked" else 0
                    _progress("Browser PDP", i, len(todo), started)
                    if streak >= PDP_MAX_CONSECUTIVE_BLOCKS:
                        LOGGER.warning("Cloudflare kept blocking the browser too (%s pages in "
                                       "a row); giving up on product pages this run", streak)
                        break
            finally:
                fetcher.close()

    for row in rows:
        handle = _col(row, "Handle")
        if handle in values:
            _set(row, "Next Shipment", values[handle])

    problems: Dict[str, List[str]] = {}
    for handle, result in outcome.items():
        if result != "read":
            problems.setdefault(result, []).append(handle)
    return {k: sorted(v) for k, v in problems.items()}


def report_pdp_run(problems: Dict[str, List[str]]) -> None:
    """End-of-run PDP numbers, with losses reported separately so a degraded
    run doesn't look like a clean one."""
    log("Product pages read: %s by plain request (%s attempted, %s HTTP GETs, %s "
        "blocked) + %s in the browser (%s attempted); final pace %.1fs",
        _PDP_COUNTS["pages_read"], _PDP_COUNTS["pdp_pages"], _PDP_COUNTS["http_gets"],
        _PDP_COUNTS["blocked"], _PDP_COUNTS["browser_read"],
        _PDP_COUNTS["browser_pages"], _PACE["interval"])
    kinds = sorted(set(_PDP_BLOCK_KIND.values()))
    if kinds:
        LOGGER.warning("How the site blocked plain requests: %s", "; ".join(kinds))
    messages = {
        "blocked": "PDP blocked (plain request; browser not used)",
        "browser_blocked": "PDP blocked even in the browser",
        "not_attempted": "PDP not requested (stopped after repeated blocks)",
        "gone": "PDP returned 404 (product page gone)",
        "failed": "PDP could not be read (other HTTP error, timeout or no browser)",
        "no_record": "PDP read but its product record was not found",
    }
    for key, message in messages.items():
        if problems.get(key):
            LOGGER.warning("%s; Next Shipment left blank for %s handle(s): %s",
                           message, len(problems[key]), ", ".join(problems[key]))

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# GraphQL fetcher
# ---------------------------------------------------------------------------

_GRAPHQL_QUERY = """
query RamyBrookProducts($cursor: String) {
  collection(handle: "jeans") {
    products(first: 250, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        createdAt
        updatedAt
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


class ScrapeError(RuntimeError):
    """Raised when the scrape can't produce a trustworthy CSV."""


def _fetch_graphql_with_token(session: requests.Session, token: str) -> List[Dict]:
    """Fetch every page of the jeans collection with one token.

    Raises ScrapeError on any failure, so a partial product list is never
    returned. ACCESS_DENIED errors (e.g. a token without inventory scope)
    are not fatal: those fields come back null and Algolia fills them.
    """
    products: List[Dict] = []
    cursor = None
    page = 0
    denied_fields: Set[str] = set()
    while True:
        page += 1
        variables: Dict = {"cursor": cursor} if cursor else {}
        try:
            resp = session.post(
                GRAPHQL_URL,
                headers={
                    "X-Shopify-Storefront-Access-Token": token,
                    "Content-Type": "application/json",
                },
                json={"query": _GRAPHQL_QUERY, "variables": variables},
                timeout=45,
            )
            resp.raise_for_status()
            payload = resp.json()
        except (requests.RequestException, ValueError) as exc:
            raise ScrapeError(f"GraphQL request failed (page {page}): {exc}") from exc
        if payload.get("errors"):
            fatal = []
            for e in payload["errors"]:
                if "ACCESS_DENIED" in str((e.get("extensions") or {}).get("code", "")):
                    path = e.get("path") or []
                    if path:
                        denied_fields.add(str(path[-1]))
                else:
                    fatal.append(e)
            if fatal:
                raise ScrapeError(f"GraphQL errors (page {page}): {fatal}")
        data = (payload.get("data") or {}).get("collection") or {}
        if not data:
            raise ScrapeError(f"No 'jeans' collection data returned (page {page})")
        block = data.get("products", {})
        for node in block.get("nodes", []):
            products.append(node)
        page_info = block.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        log("GraphQL page %s: %s products so far", page, len(products))
        time.sleep(SLEEP)
    if denied_fields:
        LOGGER.warning("GraphQL token has no access to %s; using Algolia inventory instead",
                       ", ".join(sorted(denied_fields)))
    return products


def fetch_graphql_products(session: requests.Session) -> List[Dict]:
    """Try each token in GRAPHQL_TOKENS in turn; raise ScrapeError if all fail."""
    failures: List[str] = []
    for i, token in enumerate(GRAPHQL_TOKENS, start=1):
        label = f"token {i}/{len(GRAPHQL_TOKENS)} ({token[:6]}…)"
        try:
            products = _fetch_graphql_with_token(session, token)
        except ScrapeError as exc:
            LOGGER.warning("GraphQL %s failed: %s", label, exc)
            failures.append(f"{label}: {exc}")
            continue
        if not products:
            LOGGER.warning("GraphQL %s returned 0 products", label)
            failures.append(f"{label}: 0 products")
            continue
        if i > 1:
            LOGGER.warning("Primary GraphQL token failed; using backup %s", label)
        log("GraphQL fetch complete with %s: %s product nodes", label, len(products))
        return products
    raise ScrapeError("All GraphQL tokens failed — " + " | ".join(failures))

# ---------------------------------------------------------------------------
# Algolia — fetch all variants (distinct=false) for the jeans collection
# ---------------------------------------------------------------------------

def fetch_algolia_variant_map(session: requests.Session) -> Dict[str, Dict]:
    """Return map: variant Shopify ID (str) → Algolia hit.

    Queries all pages of the jeans collection (distinct=false so every
    variant record is returned).  Rotates through ALGOLIA_HOSTS for each
    page request.
    """
    by_id: Dict[str, Dict] = {}
    page = 0
    headers = {
        "X-Algolia-Application-Id": ALGOLIA_APP_ID,
        "X-Algolia-API-Key": ALGOLIA_API_KEY,
        "Content-Type": "application/json",
    }
    import urllib.parse
    while True:
        params = "&".join([
            "distinct=false",
            "hitsPerPage=200",
            f"page={page}",
            f"filters={urllib.parse.quote(ALGOLIA_COLLECTION_FILTER)}",
        ])
        url = ALGOLIA_HOSTS[page % len(ALGOLIA_HOSTS)]
        try:
            resp = session.post(url, headers=headers,
                                json={"params": params}, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            LOGGER.warning("Algolia page %s (%s) failed: %s", page, url, exc)
            # Try remaining hosts before giving up on this page
            succeeded = False
            for fallback in ALGOLIA_HOSTS:
                if fallback == url:
                    continue
                try:
                    resp = session.post(fallback, headers=headers,
                                        json={"params": params}, timeout=30)
                    resp.raise_for_status()
                    succeeded = True
                    break
                except Exception:
                    pass
            if not succeeded:
                break
        data = resp.json()
        hits = data.get("hits", [])
        if not hits:
            break
        for hit in hits:
            for key in (
                str(hit.get("objectID") or ""),
                str(hit.get("id") or ""),
                str(hit.get("sku") or ""),
            ):
                if key:
                    by_id[key] = hit
        nb_pages = data.get("nbPages", 1)
        page += 1
        if page >= nb_pages:
            break
        time.sleep(SLEEP)
    log("Algolia variant map: %s entries across %s page(s)", len(by_id), page)
    return by_id


def algolia_inventory(hit: Dict) -> Tuple[str, str, str]:
    """Return (instore, online, incoming) inventory strings from Algolia hit."""
    loc = hit.get("locations_inventory") or {}
    instore   = sum(int(loc.get(lid) or 0) for lid in INSTORE_LOCATIONS)
    online    = int(loc.get(ONLINE_LOCATION) or 0)
    incoming  = int(loc.get(INCOMING_LOCATION) or 0)
    return str(instore), str(online), str(incoming)


def algolia_total_quantity(hit: Dict) -> Optional[int]:
    """Instore + Online + Incoming from an Algolia hit, or None if there is no hit.

    Used for 'Quantity Available' when GraphQL returns no quantityAvailable.
    """
    if not hit or not isinstance(hit.get("locations_inventory"), dict):
        return None
    instore, online, incoming = algolia_inventory(hit)
    return int(instore) + int(online) + int(incoming)

# ---------------------------------------------------------------------------
# Swym wishlist counts
# ---------------------------------------------------------------------------

def fetch_swym_wishlist_map(session: requests.Session,
                            products: List[Dict]) -> Dict[str, str]:
    """Return map: product numeric ID → wishlist count string."""
    counts: Dict[str, str] = {}
    total = len(products)
    for i, p in enumerate(products):
        raw_id = p.get("id") or ""
        numeric_id = raw_id.replace("gid://shopify/Product/", "")
        handle = p.get("handle") or ""
        du = f"{PDP_HOST}/products/{handle}"
        try:
            r = session.get(
                f"{SWYM_API_BASE}/api/v3/product/social-count",
                params={"pid": SWYM_PID, "du": du, "empi": numeric_id, "topic": "addToWishlist"},
                verify=False,
                timeout=15,
            )
            if r.ok:
                count = int((r.json().get("data") or {}).get("count") or 0)
            else:
                # fallback: eventcount API
                r2 = session.get(
                    f"{SWYM_API_BASE}/api/v2/provider/eventcount",
                    params={"pid": SWYM_PID, "du": du, "et": ET_WISHLIST, "empi": numeric_id},
                    verify=False,
                    timeout=15,
                )
                count = int(r2.json().get("count") or 0) if r2.ok else 0
        except Exception as exc:
            LOGGER.warning("Swym wishlist failed for %s: %s", handle, exc)
            count = 0
        counts[numeric_id] = str(count)
        if (i + 1) % 10 == 0 or i + 1 == total:
            log("Swym: %s/%s products done", i + 1, total)
        time.sleep(0.15)
    return counts

# ---------------------------------------------------------------------------
# Post-processing helpers
# ---------------------------------------------------------------------------

IDX = {h: i for i, h in enumerate(CSV_HEADERS)}


def _col(row: List[str], name: str) -> str:
    return row[IDX[name]]


def _set(row: List[str], name: str, val: str) -> None:
    row[IDX[name]] = val


def apply_petite_inseam_rule(rows: List[List[str]]) -> None:
    """Blank inseam for petite/cropped/ankle/sneaker-length when a normal sibling shares same Style Name + Color + Inseam."""
    PETITE_KEYWORDS = ("petite", "cropped", "crop", "ankle", "sneaker length")
    grouped: Dict[Tuple[str, str, str], List[List[str]]] = {}
    for row in rows:
        key = (_col(row, "Style Name"), _col(row, "Color"), _col(row, "Inseam"))
        grouped.setdefault(key, []).append(row)
    for group in grouped.values():
        def _is_petite(r: List[str]) -> bool:
            return any(kw in _col(r, "Product").lower() for kw in PETITE_KEYWORDS)
        has_petite  = any(_is_petite(r) for r in group)
        has_regular = any(not _is_petite(r) for r in group)
        if not (has_petite and has_regular):
            continue
        for r in group:
            if _is_petite(r):
                _set(r, "Inseam", "")


def apply_style_name_rules(rows: List[List[str]]) -> None:
    """Rule 1: unify single-word style names by first product word + leg opening.
    Rule 2: one-word → look for sibling or append Jean Style prefix."""
    idx_product = IDX["Product"]
    idx_sn      = IDX["Style Name"]
    idx_leg     = IDX["Leg Opening"]
    idx_js      = IDX["Jean Style"]

    # Rule 1
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
        assignments: List[Optional[str]] = []
        for i, row in enumerate(non_mat):
            lo_i = leg_floats[i]
            if lo_i is None or len(non_mat[i][idx_sn].split()) > 1:
                assignments.append(None)
                continue
            compatible = [
                non_mat[j][idx_sn]
                for j, lo_j in enumerate(leg_floats)
                if lo_j is not None and abs(lo_j - lo_i) <= 1.5 and non_mat[j][idx_sn]
            ]
            if not compatible or len(set(compatible)) <= 1:
                assignments.append(None)
                continue
            multi = [sn for sn in compatible if len(sn.split()) > 1]
            best  = (max(set(multi), key=multi.count) if multi
                     else max(set(compatible), key=compatible.count))
            assignments.append(best)
        for row, target in zip(non_mat, assignments):
            if target is not None:
                _set(row, "Style Name", target)

    # Rule 2
    for row in rows:
        sn = _col(row, "Style Name").strip()
        if not sn or len(sn.split()) != 1:
            continue
        title = row[idx_product]
        fw = (title.split(" ", 1)[0] if title else "").strip().lower()
        leg = row[idx_leg]
        lo_val = to_float(leg)
        candidates = [
            r[idx_sn]
            for r in rows
            if (r[idx_product].split(" ", 1)[0] if r[idx_product] else "").strip().lower() == fw
            and len(r[idx_sn].split()) > 1
            and (lo_val is None
                 or (to_float(r[idx_leg]) is not None
                     and abs(to_float(r[idx_leg]) - lo_val) <= 1.5))
        ]
        if candidates:
            _set(row, "Style Name", max(set(candidates), key=candidates.count))
            continue
        js = _col(row, "Jean Style")
        if js:
            prefix = js.split()[0]
            _set(row, "Style Name", f"{sn} {prefix}".strip())


def apply_jean_style_inference(rows: List[List[str]]) -> None:
    """Step 3: infer Jean Style from siblings sharing same Style Name + Leg Opening."""
    for row in rows:
        if _col(row, "Jean Style"):
            continue
        sn  = _col(row, "Style Name")
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


def apply_jean_style_from_algolia_tags(rows: List[List[str]],
                                       algolia_map: Dict[str, Dict]) -> None:
    """Step 4: Algolia named_tags.category[0-4]."""
    for row in rows:
        if _col(row, "Jean Style"):
            continue
        sku = _col(row, "SKU - Shopify")
        hit = algolia_map.get(sku) or {}
        named_tags = hit.get("named_tags") or {}
        cats: List[str] = []
        for i in range(5):
            cat = named_tags.get(f"category[{i}]") or named_tags.get(f"category_{i}") or ""
            if cat:
                cats.append(cat)
        if not cats:
            # also try the "category" list field
            raw_cats = hit.get("named_tags", {}).get("category") or []
            if isinstance(raw_cats, list):
                cats = [str(c) for c in raw_cats[:5]]
        if cats:
            result = _map_jean_from_algolia_cats(cats, _col(row, "Leg Opening"))
            if result:
                _set(row, "Jean Style", result)


def apply_inseam_label_style_refresh(rows: List[List[str]]) -> None:
    for row in rows:
        js    = _col(row, "Jean Style")
        title = _col(row, "Product")
        inseam = _col(row, "Inseam")
        size   = _col(row, "Size")
        desc   = _col(row, "Description")
        label  = derive_inseam_label(js, inseam, title, size, desc)
        _set(row, "Inseam Label", label)
        _set(row, "Inseam Style", derive_inseam_style(js, inseam, label))


def apply_rise_label_multiple_tags(rows: List[List[str]]) -> None:
    """When a product has multiple conflicting rise tags, defer to closest sibling rise measurement."""
    RISE_TAGS = (
        ("ultra low", "ultra low"), ("super low", "ultra low"),
        ("ultra high", "ultra high"), ("super high", "ultra high"),
        ("rise:high-rise", "high"), ("high rise", "high"),
        ("rise:mid-rise", "mid"), ("mid rise", "mid"),
        ("rise:low-rise", "low"), ("low rise", "low"),
    )
    for row in rows:
        tags = _col(row, "Tags").lower()
        rise_matches = [label for kw, label in RISE_TAGS if kw in tags]
        if len(set(rise_matches)) <= 1:
            continue
        # Multiple different rise labels in tags → look at Style Name siblings
        sn = _col(row, "Style Name")
        my_rise = to_float(_col(row, "Rise"))
        if not sn or my_rise is None:
            continue
        siblings = [
            (_col(r, "Rise Label"), to_float(_col(r, "Rise")))
            for r in rows
            if _col(r, "Style Name") == sn and _col(r, "Rise Label")
            and to_float(_col(r, "Rise")) is not None
        ]
        if siblings:
            best = min(siblings, key=lambda x: abs((x[1] or 999) - my_rise))
            if abs((best[1] or 999) - my_rise) <= 1.0:
                _set(row, "Rise Label", best[0])


def apply_rise_label_inference(rows: List[List[str]]) -> None:
    """Step 4: infer Rise Label from Style Name siblings."""
    apply_rise_label_multiple_tags(rows)
    for row in rows:
        if _col(row, "Rise Label"):
            continue
        sn   = _col(row, "Style Name")
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
            my_rise = to_float(rise)
            if my_rise is None:
                continue
            best = min(matches, key=lambda x: abs((to_float(x[1]) or 999) - my_rise))
            _set(row, "Rise Label", best[0])


def apply_color_inference(rows: List[List[str]]) -> None:
    by_color: Dict[str, List[List[str]]] = {}
    for row in rows:
        key = _col(row, "Color").strip().lower()
        if key:
            by_color.setdefault(key, []).append(row)
    for _, group in by_color.items():
        stds  = [_col(r, "Color - Standardized") for r in group if _col(r, "Color - Standardized")]
        simps = [_col(r, "Color - Simplified")   for r in group if _col(r, "Color - Simplified")]
        best_std  = max(set(stds),  key=stds.count)  if stds  else ""
        best_simp = max(set(simps), key=simps.count) if simps else ""
        for r in group:
            if not _col(r, "Color - Standardized") and best_std:
                _set(r, "Color - Standardized", best_std)
            if not _col(r, "Color - Simplified") and best_simp:
                _set(r, "Color - Simplified", best_simp)

# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

class RamyBrookScraper:
    def __init__(self, pdp_headers: str = "browser", pdp_browser: str = "auto",
                 pdp_headless: bool = False) -> None:
        self.session = make_session()
        self.pdp_session = make_pdp_session(pdp_headers)
        self.pdp_browser = pdp_browser
        self.pdp_headless = pdp_headless
        self.pdp_problems: Dict[str, List[str]] = {}

    def build_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []

        log("Fetching GraphQL products from 'jeans' collection...")
        products = fetch_graphql_products(self.session)

        log("Filtering products...")
        before = len(products)
        products = [
            p for p in products
            if not should_filter_product(
                p.get("title") or "",
                p.get("productType") or "",
            )
        ]
        log("Filtered %s → %s products", before, len(products))

        log("Fetching Algolia variant data...")
        algolia_map = fetch_algolia_variant_map(self.session)

        log("Fetching Swym wishlist counts...")
        wishlist_map = fetch_swym_wishlist_map(self.session, products)

        log("Building rows...")
        seen_skus: Set[str] = set()
        algolia_qty_variants = 0   # Quantity Available filled from Algolia
        missing_qty_variants = 0   # no quantity from GraphQL or Algolia
        algolia_style_totals = 0   # Quantity of style summed from variant rows
        for idx, product in enumerate(products, start=1):
            handle = product.get("handle") or ""
            if not handle:
                continue

            product_id   = (product.get("id") or "").replace("gid://shopify/Product/", "")
            title        = product.get("title") or ""
            published_at = format_date(product.get("publishedAt"))
            created_at   = format_date(product.get("createdAt"))
            updated_at   = format_datetime_central(product.get("updatedAt"))
            tags        = product.get("tags") or []
            tags_str     = join_tags(tags)
            vendor       = product.get("vendor") or ""
            product_type = product.get("productType") or ""
            total_inv    = product.get("totalInventory")
            image_url    = ((product.get("featuredImage") or {}).get("url") or "")
            online_url   = product.get("onlineStoreUrl") or f"{PDP_HOST}/products/{handle}"
            wishlist     = wishlist_map.get(product_id, "")

            raw_desc  = product.get("description") or ""
            desc      = clean_description(raw_desc)

            rise        = extract_rise(desc)
            inseam      = extract_inseam(desc)
            leg_opening = extract_leg_opening(desc)

            # Algolia named_tags for Jean Style step 4
            # (collected per-variant below; we use the first hit found for the product)
            algolia_cats: List[str] = []

            next_shipment = ""   # filled from the PDP by fill_next_shipment()

            variants = (product.get("variants") or {}).get("nodes") or []
            if not variants:
                log("No variants for %s, skipping", handle)
                continue

            jean_style = derive_jean_style(title, desc, leg_opening)
            rise_label = derive_rise_label(title, desc, tags_str)

            product_row_start = len(rows)
            for v in variants:
                v_id_full   = v.get("id") or ""
                sku_shopify = v_id_full.replace("gid://shopify/ProductVariant/", "")
                if sku_shopify in seen_skus:
                    continue
                seen_skus.add(sku_shopify)
                sku_brand   = v.get("title") or ""
                barcode     = v.get("barcode") or ""
                available   = "TRUE" if v.get("availableForSale") else "FALSE"
                qty_avail   = v.get("quantityAvailable")
                qty_str     = str(qty_avail) if qty_avail is not None else ""

                opts = v.get("selectedOptions") or []
                color, size = extract_color_size(opts)

                variant_title = (f"{title} / {color} / {size}"
                                 if (color and size) else title)

                price_obj = v.get("price") or {}
                price = format_price(
                    price_obj.get("amount") if isinstance(price_obj, dict) else price_obj)
                cmp_obj = v.get("compareAtPrice")
                compare_at = format_price(
                    (cmp_obj or {}).get("amount") if isinstance(cmp_obj, dict) else cmp_obj)

                v_image = ((v.get("image") or {}).get("url") or "")
                eff_image = v_image or image_url

                # Algolia lookup
                alg_hit = (algolia_map.get(sku_shopify)
                           or algolia_map.get(v.get("sku") or "")
                           or {})
                instore_qty, online_qty, incoming_qty = algolia_inventory(alg_hit)
                if qty_avail is None:
                    alg_total = algolia_total_quantity(alg_hit)
                    if alg_total is not None:
                        qty_str = str(alg_total)
                        algolia_qty_variants += 1
                    else:
                        missing_qty_variants += 1
                roc = alg_hit.get("recently_ordered_count")
                ga_purchases = str(roc) if roc is not None else ""

                # Collect Algolia categories for step-4 Jean Style (use first hit found)
                if not algolia_cats and alg_hit:
                    named = alg_hit.get("named_tags") or {}
                    for ci in range(5):
                        cat = named.get(f"category[{ci}]") or named.get(f"category_{ci}") or ""
                        if cat:
                            algolia_cats.append(cat)
                    if not algolia_cats:
                        raw_cat = named.get("category")
                        if isinstance(raw_cat, list):
                            algolia_cats = [str(c) for c in raw_cat[:5]]

                color_std  = derive_color_standardized(color, title)
                color_simp = derive_color_simplified(color, color_std)

                inseam_label = derive_inseam_label(jean_style, inseam, title, size, desc)
                inseam_style = derive_inseam_style(jean_style, inseam, inseam_label)

                row: List[str] = [""] * len(CSV_HEADERS)
                _set(row, "Style Id",            product_id)
                _set(row, "Handle",              handle)
                _set(row, "Published At",        published_at)
                _set(row, "Created At",          created_at)
                _set(row, "Updated At",          updated_at)
                _title = title.replace("Boot Cut", "Bootcut")
                if " | " in _title:
                    _set(row, "Product", _title.replace(" | ", " - "))
                else:
                    _set(row, "Product", f"{_title} - {color}" if color else _title)
                _set(row, "Style Name",          derive_style_name_base(title))
                _set(row, "Product Type",        product_type)
                _set(row, "Tags",                tags_str)
                _set(row, "Vendor",              vendor)
                _set(row, "Description",         desc)
                _set(row, "Variant Title",       variant_title)
                _set(row, "Color",               color)
                _set(row, "Size",                size)
                _set(row, "Rise",                rise)
                _set(row, "Inseam",              inseam)
                _set(row, "Leg Opening",         leg_opening)
                _set(row, "Price",               price)
                _set(row, "Compare at Price",    compare_at)
                _set(row, "Available for Sale",  available)
                _set(row, "Quantity Available",  qty_str)
                _set(row, "Quantity Available (Instore Inventory)",  instore_qty)
                _set(row, "Quantity Available (Online Inventory)",   online_qty)
                _set(row, "Quantity Available (Incoming Inventory)", incoming_qty)
                _set(row, "Quantity of style",
                     str(total_inv) if total_inv is not None else "")
                _set(row, "Daily Purchases", ga_purchases)
                alg_updated = alg_hit.get("updated_at") or ""
                if alg_updated:
                    try:
                        from datetime import datetime, timezone
                        dt = datetime.fromisoformat(alg_updated.replace("Z", "+00:00"))
                        alg_updated = dt.strftime("%m/%d/%Y %H:%M:%S")
                    except ValueError:
                        pass
                _set(row, "Purchase Reset Date", alg_updated)
                _set(row, "Wishlist count",      wishlist)
                _set(row, "Next Shipment",       next_shipment)
                _set(row, "SKU - Shopify",       sku_shopify)
                _set(row, "SKU - Brand",         sku_brand)
                _set(row, "Barcode",             barcode)
                _set(row, "Image URL",           eff_image)
                _set(row, "SKU URL",             online_url)
                _set(row, "Jean Style",          jean_style)
                _set(row, "Inseam Label",        inseam_label)
                _set(row, "Inseam Style",        inseam_style)
                _set(row, "Rise Label",          rise_label)
                _set(row, "Color - Simplified",  color_simp)
                _set(row, "Color - Standardized", color_std)
                rows.append(row)

            # No GraphQL totalInventory → sum this style's Quantity Available
            if total_inv is None:
                product_rows = rows[product_row_start:]
                qtys = [_col(r, "Quantity Available") for r in product_rows]
                if product_rows and all(q != "" for q in qtys):
                    style_total = str(sum(int(q) for q in qtys))
                    for r in product_rows:
                        _set(r, "Quantity of style", style_total)
                    algolia_style_totals += 1

            # Backfill Algolia step-4 Jean Style if still blank after variants loop
            if algolia_cats:
                for row in rows[-(len(variants)):]:
                    if not _col(row, "Jean Style"):
                        result = _map_jean_from_algolia_cats(
                            algolia_cats, _col(row, "Leg Opening"))
                        if result:
                            _set(row, "Jean Style", result)

            if idx % 5 == 0 or idx == len(products):
                log("Progress: %s/%s products (%s rows)", idx, len(products), len(rows))

        if algolia_qty_variants:
            LOGGER.warning("Quantity Available from Algolia (Instore+Online+Incoming) for "
                           "%s/%s variants; Quantity of style summed for %s styles",
                           algolia_qty_variants, len(rows), algolia_style_totals)
        if missing_qty_variants:
            LOGGER.warning("No Quantity Available from GraphQL or Algolia for %s variants",
                           missing_qty_variants)
        if not rows:
            raise ScrapeError("No rows generated")

        log("Fetching Next Shipment from product pages...")
        self.pdp_problems = fill_next_shipment(self.pdp_session, rows,
                                               self.pdp_browser, self.pdp_headless)

        # Post-processing
        log("Post-processing: petite inseam rule...")
        apply_petite_inseam_rule(rows)
        log("Post-processing: style name rules...")
        apply_style_name_rules(rows)
        log("Post-processing: Jean Style inference (siblings pass 1)...")
        apply_jean_style_inference(rows)
        log("Post-processing: Jean Style from Algolia tags...")
        apply_jean_style_from_algolia_tags(rows, algolia_map)
        log("Post-processing: Jean Style inference (siblings pass 2)...")
        apply_jean_style_inference(rows)
        log("Post-processing: Inseam Label/Style refresh...")
        apply_inseam_label_style_refresh(rows)
        log("Post-processing: Rise Label inference...")
        apply_rise_label_inference(rows)
        log("Post-processing: Color inference...")
        apply_color_inference(rows)
        return rows

    def write_csv(self, rows: List[List[str]]) -> Path:
        ts   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        path = OUTPUT_DIR / f"RAMYBROOK_{ts}.csv"
        with path.open("w", newline="", encoding="utf-8-sig") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADERS)
            writer.writerows(rows)
        log("Wrote %s rows to %s", len(rows), path)
        return path

    def run(self) -> Path:
        log("=== Ramy Brook inventory run started ===")
        rows = self.build_rows()
        report_pdp_run(self.pdp_problems)
        path = self.write_csv(rows)
        log("=== Run complete: %s ===", path)
        print(f"Done — {path}")
        return path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ramy Brook jeans inventory scraper.")
    # How readily the site challenges depends on the IP, so the pace is
    # tunable: raise it if the log shows 429s, lower it if it never does.
    parser.add_argument("--pdp-interval", type=float, default=PDP_MIN_INTERVAL,
                        help="seconds between product-page requests "
                             f"(default {PDP_MIN_INTERVAL})")
    parser.add_argument("--pdp-headers", choices=sorted(PDP_HEADER_PROFILES),
                        default="browser",
                        help="headers sent to product pages (default browser); "
                             "run ramybrook_pdp_probe.py to see which the site accepts")
    parser.add_argument("--pdp-browser", choices=["auto", "always", "never"], default="auto",
                        help="auto (default): plain requests first, a real browser "
                             "(Playwright) for pages the site blocks; always: browser "
                             "only; never: plain requests only")
    parser.add_argument("--pdp-headless", action="store_true",
                        help="run the fallback browser without a visible window "
                             "(Cloudflare blocks hidden browsers more often)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    _PACE["interval"] = _PACE["floor"] = max(0.0, args.pdp_interval)
    try:
        log("PDP headers: %s; browser fallback: %s%s; PDP interval floor: %.1fs",
            args.pdp_headers, args.pdp_browser, " (headless)" if args.pdp_headless else "",
            _PACE["floor"])
        RamyBrookScraper(args.pdp_headers, args.pdp_browser, args.pdp_headless).run()
    except ScrapeError as exc:
        LOGGER.error("Ramy Brook scrape FAILED — no CSV written: %s", exc)
        raise SystemExit(f"ERROR: Ramy Brook scrape failed, no CSV written: {exc}")


if __name__ == "__main__":
    main()
