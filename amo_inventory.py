# -*- coding: utf-8 -*-
"""AMO Denim inventory scraper — Storefront GraphQL + products.json + PDP HTML."""
from __future__ import annotations

import csv
import html as _html
import json
import logging
import re
import time
import unicodedata
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GRAPHQL_TOKEN = "ac815fead4ba77e7fb5878d399557653"
COLLECTION_HANDLE = "denim"

# Host rotation — every host is tried in order for each request type.
HOST_ROTATION = [
    "https://amo-denim.myshopify.com",
    "https://www.amodenim.com",
    "https://amodenim.com",
]
PDP_HOST = "https://amodenim.com"

RETRIES     = 3
RETRY_DELAY = 2.0
SLEEP       = 0.15

BASE_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE   = BASE_DIR / "amo_inventory.log"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Central Standard Time (UTC-6, no DST tracking — matches spec's "CST")
CST = timezone(timedelta(hours=-6))

# ---------------------------------------------------------------------------
# CSV headers
# ---------------------------------------------------------------------------
CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At",
    "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size",
    "Rise", "Back Rise", "Inseam", "Leg Opening",
    "Price", "Compare at Price", "Available for Sale",
    "Quantity Available", "Quantity of style",
    "Purchase Reset Date",
    "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL",
    "Jean Style", "Inseam Label", "Inseam Style", "Rise Label",
]
# Internal working column: PRODUCT_TITLE_NO_STYLING, used by the Style Name
# and Jean Style sibling rules. Dropped before the CSV is written.
WORK_HEADERS = CSV_HEADERS + ["Style Name Source"]
IDX = {h: i for i, h in enumerate(WORK_HEADERS)}

# ---------------------------------------------------------------------------
# Filter words — product removed if title OR productType/filtercategory match
# ---------------------------------------------------------------------------
FILTER_WORDS: List[str] = [
    "Capris", "Capri", "Cami", "Dresses", "Dress", "Jackets", "Jacket",
    "Henley", "Shorts", "Short", "Shirts", "Shirt", "Sleeve", "Sweaters",
    "Sweater", "Skirts", "Skirt", "Hair", "Belts", "Belt", "Socks", "Tees",
    "Tee", "Thermal", "Tops", "Top", "Tanks", "Tank", "Sweatpants",
    "Sweatshirt",
]

# ---------------------------------------------------------------------------
# Style Name removal phrases
# ---------------------------------------------------------------------------
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "1999", "5-Pocket", "Accent Hardware", "Ankle", "Beaded", "Bee's Knees",
    "Belted", "Braided", "Button", "Cargo", "Carpenter", "Chap", "Checkered",
    "Chew", "Coated", "Constructed", "Contrast", "Corduroy", "Crochet",
    "Crop", "Cropped", "Crushed", "Crystal", "Cuff", "Cuffed", "Cutoff",
    "Cut-Out", "Darted", "Destroyed", "Diamond Cut", "Distressed",
    "Double Flood", "Double Heel", "Double Prep", "Double Sneak", "Drawn",
    "Drawstring", "Embroidery", "Exposed", "Faux", "Fit", "Flag",
    "Flap Pocket", "Flap", "Flip", "Flood", "Floral", "Fray", "Frayed Seam",
    "Fringe", "Front Yoke", "Frontier", "Graffitimetalik", "Heel", "Heyday",
    "High Rise", "High Waisted", "High-Rise", "Hover cuff", "Hover", "Inch",
    "Inset", "Jean W/ Slit Hem", "Jean", "Krushed", "Krystal", "Leather",
    "Lightweight", "Lil", "Lo", "Long", "low and loose", "Low Rise",
    "Low Waised", "Low-Rise", "Mid Rise", "Mid Waisted", "Mid-Rise", "Ms.",
    "Nacho", "Nerdy", "Panel", "Pant", "Pants", "Patch Pocket", "Patch",
    "petite", "PETITES The Lil", "Pintucked", "Plaid", "Pleated", "Pleaty",
    "Plus", "Pocket Pant", "Pocket", "Poplin", "Prep", "Printed", "Regular",
    "Renaissance", "Repair", "Retro", "Rinse", "Ripped", "Rolled Hem",
    "Saddle", "Sailor", "Seam", "Seamed Front Yoke", "Seamed", "Selvedge",
    "Sequin", "Side Seam Snaps", "Skimp", "Slice", "Slit", "SNACKS!",
    "Snake Print", "Sneak", "Sneaker Length", "Sott", "Spark", "SPARKLE",
    "Spliced", "Split", "Stacked Waist", "Stacked", "Step Fray", "Stitched",
    "Stoned", "Straight", "Studded", "Stunner Zip", "Suede", "Super",
    "Swisher", "Tall", "The Laced Up", "The Side Zip Slung", "The",
    "Track Pant", "Trashed", "Trim", "Trouser Jean", "Trouser", "Tune Up",
    "Tux", "Ultra", "Utility", "Vegan Leather", "Velvet", "Vent",
    "V-High Rise", "Vintage", "W/ Contrast Front Panel", "w/ Cuff",
    "w/ Flap Jean", "w/ Raw Hem", "w/ Slit Hem", "W/ Stud Detailing",
    "W/ Wide Cuff", "W/Flap", "Wax", "Welt Pocket", "Wide Hem", "With Cuff",
    "With Frayed Seam", "Zip", "Zipper",
]

# Words stripped from the handle when deriving Color
COLOR_STRIP_TOKENS = {"crop", "petite", "petites", "regular", "tall"}

# Length descriptors split out into their own Variant Title segment
LENGTH_TOKENS = ("Petite", "Regular", "Long", "Tall")

# ---------------------------------------------------------------------------
# Valid size values
# ---------------------------------------------------------------------------
VALID_SIZES: Set[str] = {
    "00", "0", "2", "4", "6", "8", "10", "12", "14",
    "15 Plus", "16 Plus", "18 Plus", "20 Plus", "22 Plus",
    "24 Plus", "26 Plus", "28 Plus", "30 Plus", "32 Plus",
    "XS", "S", "M", "L", "XL", "XXL", "1XL", "2XL", "3XL", "4XL", "5XL",
    "00-4", "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus",
    "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32",
    "33", "34", "35", "36", "37", "38", "39", "40",
}

NON_TAPER = {"Straight from Knee/Thigh", "Bootcut", "Wide Leg", "Boyfriend",
             "Barrel", "Baggy", "Flare", "Straight from Thigh"}
TAPER     = {"Tapered", "Skinny", "Barrel", "Straight from Knee"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"),
              logging.StreamHandler()],
)


def log(msg, *args):
    logging.info(msg, *args)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _set(row: List[str], header: str, value) -> None:
    row[IDX[header]] = "" if value is None else str(value)


def _col(row: List[str], header: str) -> str:
    return row[IDX[header]]


def to_float(val) -> Optional[float]:
    if val in (None, ""):
        return None
    try:
        return float(str(val).strip())
    except (TypeError, ValueError):
        return None


def text_has_any(haystack: str, needles) -> bool:
    h = (haystack or "").lower()
    return any(n.lower() in h for n in needles)


def normalize_quotes(text: str) -> str:
    """Curly quotes/apostrophes -> straight; strip stray control chars."""
    if not text:
        return ""
    return (text.replace("‘", "'").replace("’", "'")
                .replace("“", '"').replace("”", '"')
                .replace("–", "-").replace("—", "-"))


def clean_description(raw: str) -> str:
    if not raw:
        return ""
    s = _html.unescape(raw)
    s = re.sub(r"<[^>]+>", " ", s)
    s = normalize_quotes(s)
    return re.sub(r"\s+", " ", s).strip()


def format_date(iso: str) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%Y")
    except ValueError:
        return ""


def format_purchase_reset(iso: str) -> str:
    """product.updatedAt -> MM/DD/YYYY HH:MM:SS AM/PM in Central Standard Time."""
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return dt.astimezone(CST).strftime("%m/%d/%Y %I:%M:%S %p")


def format_price(val) -> str:
    if val in (None, ""):
        return ""
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return ""


def join_tags(tags) -> str:
    if isinstance(tags, list):
        return ", ".join(str(t) for t in tags)
    return str(tags or "")


def frac_to_decimal(txt: str) -> str:
    """'10 1/8' -> '10.125'; '14.5' -> '14.5'. Rounded to 3 decimals."""
    if not txt:
        return ""
    txt = txt.strip()
    m = re.match(r"(\d+)\s*(?:(\d+)\s*/\s*(\d+))?", txt)
    if not m:
        return ""
    whole = float(m.group(1))
    if m.group(2) and m.group(3):
        try:
            whole += int(m.group(2)) / int(m.group(3))
        except ZeroDivisionError:
            pass
    dec = re.match(r"\d+\.\d+", txt)
    if dec:
        whole = float(dec.group(0))
    val = round(whole, 3)
    return f"{val:g}"


# ---------------------------------------------------------------------------
# HTTP session with host rotation
# ---------------------------------------------------------------------------
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36")


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "*/*"})
    return s


def _rotate_get(session: requests.Session, path: str, **kwargs) -> Optional[requests.Response]:
    """GET `path` trying every host in HOST_ROTATION, with retries per host."""
    for host in HOST_ROTATION:
        url = f"{host}{path}"
        for attempt in range(RETRIES):
            try:
                r = session.get(url, timeout=40, **kwargs)
                if r.status_code == 200:
                    return r
                if r.status_code == 404:
                    return None
                raise requests.HTTPError(f"status {r.status_code}")
            except Exception as exc:
                if attempt < RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))
                else:
                    log("  GET failed %s: %s", url, exc)
    return None


PRODUCTS_QUERY = """
query($cursor: String) {
  collection(handle: "%s") {
    products(first: 250, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id handle title vendor productType tags description
        publishedAt createdAt updatedAt onlineStoreUrl
        featuredImage { url }
        variants(first: 250) {
          nodes {
            id title sku barcode availableForSale
            price { amount }
            compareAtPrice { amount }
            selectedOptions { name value }
            image { url }
          }
        }
      }
    }
  }
}
""" % COLLECTION_HANDLE


def fetch_graphql_products(session: requests.Session) -> List[dict]:
    """Fetch every product in the denim collection, rotating hosts on failure."""
    out: List[dict] = []
    cursor = None
    while True:
        payload = {"query": PRODUCTS_QUERY, "variables": {"cursor": cursor}}
        data = None
        for host in HOST_ROTATION:
            url = f"{host}/api/unstable/graphql.json"
            for attempt in range(RETRIES):
                try:
                    r = session.post(
                        url, json=payload, timeout=60,
                        headers={"X-Shopify-Storefront-Access-Token": GRAPHQL_TOKEN,
                                 "Content-Type": "application/json"})
                    r.raise_for_status()
                    body = r.json()
                    coll = (body.get("data") or {}).get("collection")
                    if coll:
                        data = coll["products"]
                        break
                    raise ValueError("no collection in response")
                except Exception as exc:
                    if attempt < RETRIES - 1:
                        time.sleep(RETRY_DELAY * (attempt + 1))
                    else:
                        log("  GraphQL failed %s: %s", url, exc)
            if data:
                break
        if not data:
            break
        out.extend(data.get("nodes") or [])
        info = data.get("pageInfo") or {}
        if not info.get("hasNextPage"):
            break
        cursor = info.get("endCursor")
    return out


def fetch_product_json(session: requests.Session, handle: str) -> dict:
    """/products/<handle>.json -> inventory_quantity, barcode per variant."""
    r = _rotate_get(session, f"/products/{handle}.json")
    if not r:
        return {}
    try:
        return r.json().get("product") or {}
    except ValueError:
        return {}


def fetch_pdp_html(session: requests.Session, handle: str) -> str:
    r = _rotate_get(session, f"/products/{handle}")
    return r.text if r else ""


# ---------------------------------------------------------------------------
# PDP details block
# ---------------------------------------------------------------------------
def extract_details_block(html_text: str) -> str:
    """Pull the 'Fit & Measurements' collapsible text from the PDP.

    The Shopify template id (e.g. Details-collapsible_tab_6jMfaJ-template--
    16696342642754__main) changes between deploys, so match on the section
    heading instead and fall back to a raw-text scan for the measurements.
    """
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    for el in soup.select('[id^="Details-collapsible"]'):
        text = re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()
        if re.search(r"(Front\s*[Rr]ise|Inseam)\s*=", text):
            # Drop the leading "Fit & Measurements" heading
            text = re.sub(r"^\s*Fit\s*&\s*Measurements\s*", "", text)
            return normalize_quotes(text).strip()
    # Fallback: scan the whole page for the measurement sentence
    flat = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", _html.unescape(html_text)))
    m = re.search(r"[^.]*Inseam\s*=.{0,220}", flat)
    return normalize_quotes(m.group(0)).strip() if m else ""


def extract_measurement(details: str, label: str) -> str:
    """'Front rise = 10 3/4"' -> '10.75'. Label matched case-insensitively."""
    if not details:
        return ""
    m = re.search(rf"{label}\s*=\s*(\d+(?:\.\d+)?(?:\s+\d+\s*/\s*\d+)?)",
                  details, re.IGNORECASE)
    return frac_to_decimal(m.group(1)) if m else ""


# ---------------------------------------------------------------------------
# Product / Style Name / Color / Size
# ---------------------------------------------------------------------------
def clean_title(title: str) -> str:
    """Strip special characters, straighten quotes, '-' -> space."""
    t = normalize_quotes(title or "")
    t = unicodedata.normalize("NFKD", t)
    t = t.replace("-", " ")
    t = re.sub(r"[^\w\s'/&.]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def product_title_for_product_field(title: str) -> str:
    """Product field title: cleaned, with "Boot" -> "Bootcut"."""
    t = clean_title(title)
    t = re.sub(r"\bBoot\b", "Bootcut", t, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", t).strip()


def derive_length_token(title: str, handle: str, color: str) -> str:
    """Length descriptor in the title/handle that is not part of the color."""
    color_words = {w.lower() for w in re.split(r"[\s\-]+", color or "") if w}
    hay = f"{clean_title(title)} {handle.replace('-', ' ')}".lower()
    for tok in LENGTH_TOKENS:
        low = tok.lower()
        if low in color_words:
            continue
        if re.search(rf"\b{low}\b", hay):
            return tok
    return ""


def strip_length_token(text: str, token: str) -> str:
    if not token:
        return text
    out = re.sub(rf"\b{re.escape(token)}\b", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", out).strip()


def derive_color(handle: str, title: str) -> str:
    """Handle minus the title words minus crop/petite/regular/tall."""
    if not handle:
        return ""
    title_words = {w.lower() for w in re.split(r"[\s\-]+", clean_title(title)) if w}
    tokens = [t for t in handle.split("-") if t]
    kept = [t for t in tokens
            if t.lower() not in title_words and t.lower() not in COLOR_STRIP_TOKENS]
    if not kept:
        return ""
    return "-".join(p.capitalize() if not p.isdigit() else p for p in kept)


def extract_size(selected_options: List[dict]) -> Tuple[str, bool]:
    """Return (size, is_petite). Size may live in option1 or option2."""
    vals = [(o.get("value") or "").strip() for o in (selected_options or [])]
    for v in vals:
        if v in VALID_SIZES:
            return v, False
    # Numbers suffixed with P/S (petite) or L/T (long)
    for v in vals:
        m = re.fullmatch(r"(\d{1,2})\s*([PS])", v, re.IGNORECASE)
        if m and m.group(1) in VALID_SIZES:
            return m.group(1), True
        m = re.fullmatch(r"(\d{1,2})\s*([LT])", v, re.IGNORECASE)
        if m and m.group(1) in VALID_SIZES:
            return m.group(1), False
    return (vals[-1] if vals else ""), False


def derive_style_name_base(product_title: str) -> str:
    """STEP 1 clean title (Boot -> Bootcut), STEP 2 remove styling words."""
    text = clean_title(product_title)
    text = re.sub(r"\bBoot\b", "Bootcut", text, flags=re.IGNORECASE)
    text = text.replace('"', " ")
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        if phrase.lower() == "jean":
            text = re.sub(r"\bJeans?\b", " ", text, flags=re.IGNORECASE)
        elif phrase.endswith("."):
            text = re.sub(rf"\b{re.escape(phrase)}(?=\s|$)", " ", text,
                          flags=re.IGNORECASE)
        else:
            text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text,
                          flags=re.IGNORECASE)
    text = re.sub(r"\b\d+\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # Numeric-only names (e.g. the "143") are emptied by the number strip.
    # Fall back to the cleaned title so the one-word rule can still run.
    if not text:
        text = clean_title(product_title)
    return text


# ---------------------------------------------------------------------------
# Jean Style
# ---------------------------------------------------------------------------
def _straight_bucket(leg_opening) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo < 17.5:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def jean_style_from_title(title: str, desc: str, leg_opening) -> str:
    t = (title or "").lower()
    if text_has_any(t, ("barrel", "barrell", "bowed", "bow leg", "stovepipe",
                        "stove-pipe", "curved straight", "horseshoe",
                        "straight wide leg, with a subtle taper")):
        return "Barrel"
    if text_has_any(t, ("tapered", "relaxed skinny", "mom")):
        return "Tapered"
    if "straight" in t:
        bucket = _straight_bucket(leg_opening)
        if bucket:
            return bucket
        if text_has_any(desc, ("relaxed straight-leg", "relaxed straight",
                               "wide, straight", "wide straight")):
            return "Straight from Thigh"
    if "baggy" in t:
        return "Baggy"
    if text_has_any(t, ("bootcut", "boot-cut", "boot", "slim flare",
                        "slim kick flare")):
        return "Bootcut"
    if "flare" in t:
        return "Flare"
    if "skinny" in t:
        return "Skinny"
    # "Trouser" deliberately excluded here: an Easy Army Trouser style is a
    # straight pant, so the leg-opening rules must decide rather than the name.
    if text_has_any(t, ("wide leg", "wide-leg")):
        return "Wide Leg"
    if "boyfriend" in t:
        return "Boyfriend"
    if "cigarette" in t:
        return "Straight from Knee"
    if "straight" in t and text_has_any(
            desc, ("classic straight-leg", "slim straight", "slim-straight",
                   "classic straight fit", "cigarette")):
        return "Straight from Knee"
    return ""


def jean_style_from_desc(desc: str, leg_opening) -> str:
    d = (desc or "").lower()
    if text_has_any(d, ("barrel", "barrell", "bowed", "bow leg", "stovepipe",
                        "stove-pipe", "curved outseam", "horseshoe",
                        "straight wide leg, with a subtle taper")):
        return "Barrel"
    if "skinny" in d:
        return "Skinny"
    if text_has_any(d, ("bootcut", "boot-cut", "boot cut")):
        return "Bootcut"
    if text_has_any(d, ("taper", "tapering", "tapered")):
        return "Tapered"
    if text_has_any(d, ("wide leg", "wide-leg", "flared wide leg",
                        "easy flare", "palazzo")):
        return "Wide Leg"
    if text_has_any(d, ("relaxed straight-leg", "relaxed straight",
                        "wide straight")):
        return "Straight from Thigh"
    if (text_has_any(d, ("90s-inspired, slim fit", "cigarette leg",
                         "slim and straight"))
            or ("straight" in d
                and "fitted throughout the waist, hips, and thighs" in d)):
        return "Straight from Knee"
    if "straight" in d:
        lo = to_float(leg_opening)
        if lo is not None:
            if lo < 15.5:
                return "Straight from Knee"
            if lo < 17.5:
                return "Straight from Knee/Thigh"
            return "Straight from Thigh"
    if "flare" in d:
        return "Flare"
    if "straight" in d:
        return "Straight from Knee/Thigh"
    if text_has_any(d, ("baggy", "loose fit throught the leg",
                        "loose fit through the leg")):
        return "Baggy"
    return ""


def jean_style_from_tags(tags: str, leg_opening) -> str:
    g = (tags or "").lower()
    if text_has_any(g, ("filter_style_barrel", "barrel", "barrell", "bowed",
                        "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if text_has_any(g, ("filter_style_skinny", "filter_style_superskinny",
                        "skinny")):
        return "Skinny"
    if text_has_any(g, ("filter_style_flare", "flare")):
        return "Flare"
    if text_has_any(g, ("fit_boot", "filter_style_boot", "bootcut",
                        "boot cut", "boot-cut")):
        return "Bootcut"
    if text_has_any(g, ("statement leg", "taper", "tapering", "tapered")):
        return "Tapered"
    if "filter_style_cigarette" in g:
        return "Straight from Knee"
    if text_has_any(g, ("fit_straight", "filter_style_straight", "straight")):
        bucket = _straight_bucket(leg_opening)
        if bucket:
            return bucket
    if "baggy" in g:
        return "Baggy"
    if "boyfriend" in g:
        return "Boyfriend"
    if text_has_any(g, ("fit_wide", "filter_style_wide", "wide leg",
                        "wide-leg", "wideleg", "palazzo")):
        return "Wide Leg"
    return ""


def derive_jean_style(title: str, desc: str, tags: str, leg_opening) -> str:
    """Steps 1, 2 and 4. Step 3 (sibling inference) runs in post-processing."""
    return (jean_style_from_title(title, desc, leg_opening)
            or jean_style_from_desc(desc, leg_opening)
            or jean_style_from_tags(tags, leg_opening))


# ---------------------------------------------------------------------------
# Rise Label
# ---------------------------------------------------------------------------
ULTRA_LOW = ("super low rise", "super low-rise", "ultra low rise",
             "ultra low-rise", "super low waist", "super low-waist",
             "ultra low waist", "ultra low-waist")
ULTRA_HIGH = ("super high rise", "super high-rise", "ultra high rise",
              "ultra high-rise", "super high waist", "super high-waist",
              "ultra high waist", "ultra high-waist")


def rise_label_from_title(title: str) -> str:
    t = (title or "").lower()
    if text_has_any(t, ULTRA_LOW):
        return "Ultra Low"
    if text_has_any(t, ULTRA_HIGH):
        return "Ultra High"
    if text_has_any(t, ("mid-rise", "mid rise")):
        return "Mid"
    if text_has_any(t, ("low-rise", "low rise")):
        return "Low"
    if text_has_any(t, ("high-rise", "high rise")):
        return "High"
    return ""


def rise_label_from_desc(desc: str, rise) -> str:
    d = (desc or "").lower()
    if text_has_any(d, ("rise: super low", "rise: ultra low",
                        "rise - super low", "rise - ultra low") + ULTRA_LOW):
        return "Ultra Low"
    if text_has_any(d, ("rise: super high", "rise: ultra high",
                        "rise - super high", "rise - ultra high") + ULTRA_HIGH):
        return "Ultra High"
    has_high = text_has_any(d, ("rise: high", "rise : high", "rise - high",
                                "high-rise", "high rise", "high waist",
                                "high-waist", "high waisted", "high-waisted",
                                "high on the hip", "high on the waist",
                                "elevated waistline",
                                "elevated, cinched waistline"))
    has_mid = text_has_any(d, ("sit at the high hip", "rise: mid", "rise : mid",
                               "rise - mid", "mid-rise", "mid rise"))
    if has_high and has_mid:
        r = to_float(rise)
        if r is not None:
            return "High" if r >= 12 else "Mid"
    if has_mid:
        return "Mid"
    if text_has_any(d, ("rise: low", "rise : low", "rise - low", "low-rise",
                        "low rise", "hip-hugging fit",
                        "sit comfortably on your hips", "low on the hips",
                        "low on the hip", "low on the waist")):
        return "Low"
    if has_high:
        return "High"
    return ""


def rise_label_from_tags(tags: str) -> Tuple[str, bool]:
    """Returns (label, ambiguous) — ambiguous when multiple rise tags match."""
    parts = [p.strip().lower() for p in (tags or "").split(",")]
    hits: List[str] = []
    for p in parts:
        if p in ("rise: super low", "super low"):
            hits.append("Ultra Low")
        elif p in ("rise: super high", "super high", "ultra rise",
                   "rise: ultra high"):
            hits.append("Ultra High")
        elif p in ("rise: high", "high rise", "high"):
            hits.append("High")
        elif p in ("rise: mid", "mid rise", "mid"):
            hits.append("Mid")
        elif p in ("rise: low", "low rise", "low"):
            hits.append("Low")
    uniq = list(dict.fromkeys(hits))
    if not uniq:
        return "", False
    if len(uniq) > 1:
        return uniq[0], True
    return uniq[0], False


def derive_rise_label(title: str, desc: str, rise) -> str:
    """Steps 1-2. Tag and sibling steps run in post-processing."""
    return rise_label_from_title(title) or rise_label_from_desc(desc, rise)


# ---------------------------------------------------------------------------
# Inseam Label / Inseam Style
# ---------------------------------------------------------------------------
def derive_inseam_label(jean_style: str, inseam, title: str, handle: str,
                        size: str, desc: str, size_is_petite: bool) -> str:
    hay = f"{title} {handle} {size}".lower()
    if (size_is_petite or text_has_any(hay, ("petite", "petites", "lil"))
            or re.fullmatch(r"\d+\s*[ps]", (size or "").strip().lower() or "x")
            or "for women under 5'4" in (desc or "").lower()):
        return "Petite"
    if (text_has_any(hay, ("long", "tall", "extra length"))
            or re.fullmatch(r"\d+\s*[lt]", (size or "").strip().lower() or "x")
            or "with a little more length" in (desc or "").lower()):
        return "Long"
    ins = to_float(inseam)
    if ins is not None:
        if jean_style in NON_TAPER and ins >= 33:
            return "Long"
        if jean_style in {"Barrel", "Skinny", "Tapered",
                          "Straight from Knee"} and ins >= 31.5:
            return "Long"
    return "Regular"


def _measurement_inseam_style(jean_style: str, inseam, rise, label: str,
                              desc: str) -> str:
    ins = to_float(inseam)
    if ins is None:
        return ""
    r = to_float(rise)
    ir = (ins + r) if r is not None else None
    d = (desc or "").lower()
    dropped = "dropped crotch" in d

    if jean_style in NON_TAPER:
        if label == "Petite":
            if ins < 25:
                return "Cropped"
            if ins < 26:
                if ir is None:
                    return ""
                return "Cropped" if ir < 36 else "Ankle"
            if ins < 28:
                return "Ankle"
            if ins < 29:
                if dropped:
                    return "Full Length"
                if ir is None:
                    return ""
                return "Full Length" if ir >= 38 else "Ankle"
            return "Full Length"
        if label == "Regular":
            if ins < 26:
                return "Cropped"
            if ins < 27:
                if ir is None:
                    return ""
                return "Cropped" if ir < 38 else "Ankle"
            if ins <= 29:
                return "Ankle"
            if ins < 30:
                if dropped:
                    return "Full Length"
                if ir is None:
                    return ""
                return "Full Length" if ir >= 42 else "Ankle"
            return "Full Length"
        if label == "Long":
            if ins < 28:
                return "Cropped"
            if ins <= 31:
                return "Ankle"
            return "Full Length"

    if jean_style in TAPER:
        if label == "Petite":
            if ins < 25:
                return "Cropped"
            if ins <= 27:
                return "Ankle"
            return "Full Length"
        if label == "Regular":
            if ir is None:
                return ""
            if ir < 37:
                return "Cropped"
            if ir <= 38:
                return "Ankle"
            return "Full Length"
        if label == "Long":
            if ins < 27:
                return "Cropped"
            if ins <= 30:
                return "Ankle"
            return "Full Length"
    return ""


def _keyword_inseam_style(jean_style: str, desc: str) -> str:
    d = (desc or "").lower()
    if text_has_any(d, ("full length", "long inseam", "full inseam",
                        "slight amount of stacking", "full dramatic length",
                        "elongated inseam")):
        return "Full Length"
    if jean_style in NON_TAPER:
        if "cropped at the ankle" in d:
            return "Ankle"
        if "cropped" in d:
            return "Ankle"
    if jean_style in TAPER:
        if "hits just above the ankle" in d:
            return "Cropped"
        if "at the ankle" in d:
            return "Full Length"
    return ""


def derive_inseam_style(jean_style: str, inseam, rise, label: str, desc: str,
                        tags: str) -> str:
    return (_measurement_inseam_style(jean_style, inseam, rise, label, desc)
            or _keyword_inseam_style(jean_style, desc)
            or ("Full Length" if "full length" in (tags or "").lower() else ""))


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------
def apply_style_name_rules(rows: List[List[str]]) -> None:
    """STEP 3 rule 1 (sibling unification) and rule 2 (one-word names)."""
    idx_t, idx_sn = IDX["Style Name Source"], IDX["Style Name"]
    idx_leg = IDX["Leg Opening"]

    by_first: Dict[str, List[List[str]]] = {}
    for row in rows:
        src = row[idx_t]
        fw = (src.split(" ", 1)[0] if src else "").strip().lower()
        if fw:
            by_first.setdefault(fw, []).append(row)

    # Rule 1 — same first word + matching leg opening -> most frequent name
    for _, group in by_first.items():
        non_mat = [r for r in group if "maternity" not in r[idx_t].lower()]
        if len(non_mat) < 2:
            continue
        legs = [to_float(r[idx_leg]) for r in non_mat]
        targets: List[Optional[str]] = []
        for i, _row in enumerate(non_mat):
            lo_i = legs[i]
            if lo_i is None or len(non_mat[i][idx_sn].split()) > 1:
                targets.append(None)
                continue
            compat = [non_mat[j][idx_sn] for j, lo_j in enumerate(legs)
                      if lo_j is not None and abs(lo_j - lo_i) <= 1.5
                      and non_mat[j][idx_sn]]
            if not compat or len(set(compat)) <= 1:
                targets.append(None)
                continue
            multi = [sn for sn in compat if len(sn.split()) > 1]
            targets.append(max(set(multi), key=multi.count) if multi
                           else max(set(compat), key=compat.count))
        for row, tgt in zip(non_mat, targets):
            if tgt:
                row[idx_sn] = tgt

    # Rule 2 — one-word Style Name
    for row in rows:
        sn = row[idx_sn].strip()
        if not sn or len(sn.split()) != 1:
            continue
        fw = sn.lower()
        lo_val = to_float(row[idx_leg])
        cands = [r[idx_sn] for r in rows
                 if r[idx_sn].split(" ", 1)[0].strip().lower() == fw
                 and len(r[idx_sn].split()) > 1
                 and (lo_val is None
                      or (to_float(r[idx_leg]) is not None
                          and abs(to_float(r[idx_leg]) - lo_val) <= 1.5))]
        if cands:
            row[idx_sn] = max(set(cands), key=cands.count)
            continue
        js = row[IDX["Jean Style"]]
        if js:
            row[idx_sn] = f"{sn} {js.split()[0]}".strip()


def apply_jean_style_sibling_inference(rows: List[List[str]]) -> None:
    """Jean Style step 3 — inherit from rows sharing PRODUCT_TITLE_NO_STYLING."""
    idx_js, idx_src = IDX["Jean Style"], IDX["Style Name Source"]
    groups: Dict[str, Set[str]] = {}
    for row in rows:
        key = row[idx_src].strip().lower()
        if key and row[idx_js]:
            groups.setdefault(key, set()).add(row[idx_js])
    for row in rows:
        if row[idx_js]:
            continue
        vals = groups.get(row[idx_src].strip().lower()) or set()
        if len(vals) == 1:
            row[idx_js] = next(iter(vals))


def apply_rise_label_steps(rows: List[List[str]]) -> None:
    """Rise Label steps 3-5: sibling match, tags, then sibling match again."""
    idx_rl, idx_sn, idx_rise = IDX["Rise Label"], IDX["Style Name"], IDX["Rise"]
    idx_tags = IDX["Tags"]

    def sibling_fill() -> None:
        by_style: Dict[str, List[List[str]]] = {}
        for r in rows:
            if r[idx_sn] and r[idx_rl]:
                by_style.setdefault(r[idx_sn], []).append(r)
        for row in rows:
            if row[idx_rl]:
                continue
            sibs = by_style.get(row[idx_sn]) or []
            if not sibs:
                continue
            labels = {s[idx_rl] for s in sibs}
            if len(labels) == 1:
                row[idx_rl] = next(iter(labels))
                continue
            mine = to_float(row[idx_rise])
            if mine is None:
                continue
            scored = [(abs(to_float(s[idx_rise]) - mine), s[idx_rl])
                      for s in sibs if to_float(s[idx_rise]) is not None]
            if scored:
                row[idx_rl] = min(scored)[1]

    sibling_fill()                                          # Step 3
    for row in rows:                                        # Step 4 — tags
        if row[idx_rl]:
            continue
        label, ambiguous = rise_label_from_tags(row[idx_tags])
        if not label:
            continue
        if not ambiguous:
            row[idx_rl] = label
            continue
        mine = to_float(row[idx_rise])
        best = None
        for s in rows:
            if s is row or s[idx_sn] != row[idx_sn] or not s[idx_rl]:
                continue
            sr = to_float(s[idx_rise])
            if mine is None or sr is None or abs(sr - mine) > 1.0:
                continue
            d = abs(sr - mine)
            if best is None or d < best[0]:
                best = (d, s[idx_rl])
        row[idx_rl] = best[1] if best else label
    sibling_fill()                                          # Step 5


def apply_inseam_refresh(rows: List[List[str]]) -> None:
    """Recompute Inseam Label/Style after Jean Style backfill."""
    for row in rows:
        js = row[IDX["Jean Style"]]
        if not js:
            continue
        label = row[IDX["Inseam Label"]]
        if not row[IDX["Inseam Style"]]:
            row[IDX["Inseam Style"]] = derive_inseam_style(
                js, row[IDX["Inseam"]], row[IDX["Rise"]], label,
                row[IDX["Description"]], row[IDX["Tags"]])


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------
def should_filter(title: str, product_type: str, tags) -> bool:
    filtercats = " ".join(t for t in (tags or [])
                          if str(t).lower().startswith("filtercategory"))
    hay = f" {title} {product_type} {filtercats} ".lower()
    return any(re.search(rf"\b{re.escape(w.lower())}\b", hay)
               for w in FILTER_WORDS)


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------
def build_rows(session: requests.Session) -> List[List[str]]:
    log("Fetching products from Storefront GraphQL (collection=%s)...",
        COLLECTION_HANDLE)
    products = fetch_graphql_products(session)
    log("GraphQL returned %s products", len(products))

    # De-duplicate by product id
    seen_pids: Set[str] = set()
    unique = []
    for p in products:
        pid = p.get("id") or ""
        if pid in seen_pids:
            continue
        seen_pids.add(pid)
        unique.append(p)
    products = unique

    before = len(products)
    products = [p for p in products
                if not should_filter(p.get("title") or "",
                                     p.get("productType") or "",
                                     p.get("tags") or [])]
    log("Filtered %s -> %s products", before, len(products))

    rows: List[List[str]] = []
    seen_variants: Set[str] = set()

    for i, product in enumerate(products, start=1):
        handle = product.get("handle") or ""
        if not handle:
            continue

        product_id = (product.get("id") or "").replace(
            "gid://shopify/Product/", "")
        title        = product.get("title") or ""
        vendor       = product.get("vendor") or ""
        product_type = product.get("productType") or ""
        tags_str     = join_tags(product.get("tags"))
        published_at = format_date(product.get("publishedAt"))
        created_at   = format_date(product.get("createdAt"))
        reset_date   = format_purchase_reset(product.get("updatedAt"))
        image_url    = ((product.get("featuredImage") or {}).get("url") or "")
        sku_url      = f"{PDP_HOST}/products/{handle}"

        graphql_desc = clean_description(product.get("description") or "")

        # PDP: measurement block appended to the description
        details = extract_details_block(fetch_pdp_html(session, handle))
        desc = f"{graphql_desc} {details}".strip() if details else graphql_desc

        rise      = extract_measurement(details, r"Front\s*Rise")
        back_rise = extract_measurement(details, r"Back\s*Rise")
        inseam    = extract_measurement(details, r"Inseam")
        leg_open  = extract_measurement(details, r"Leg\s*Opening")

        # products.json: inventory + barcode
        pjson = fetch_product_json(session, handle)
        inv_map: Dict[str, dict] = {}
        for vj in (pjson.get("variants") or []):
            inv_map[str(vj.get("id") or "")] = vj

        product_field = product_title_for_product_field(title)
        color = derive_color(handle, title)
        style_src = derive_style_name_base(title)

        # Variant Title splits any length descriptor out of the title and
        # appends it as its own trailing segment.
        length_token = derive_length_token(title, handle, color)
        vt_title = strip_length_token(product_field, length_token)

        jean_style = derive_jean_style(title, desc, tags_str, leg_open)
        rise_label = derive_rise_label(title, desc, rise)

        variants = (product.get("variants") or {}).get("nodes") or []
        style_total = 0
        has_qty = False
        product_rows: List[List[str]] = []

        for v in variants:
            vid = (v.get("id") or "").replace(
                "gid://shopify/ProductVariant/", "")
            if not vid or vid in seen_variants:
                continue
            seen_variants.add(vid)

            size, is_petite = extract_size(v.get("selectedOptions") or [])
            vj = inv_map.get(vid, {})

            qty = vj.get("inventory_quantity")
            if qty is not None:
                has_qty = True
                try:
                    style_total += int(qty)
                except (TypeError, ValueError):
                    pass

            price_obj = v.get("price") or {}
            cmp_obj = v.get("compareAtPrice")
            price = format_price(price_obj.get("amount")
                                 if isinstance(price_obj, dict) else price_obj)
            compare_at = format_price((cmp_obj or {}).get("amount")
                                      if isinstance(cmp_obj, dict) else cmp_obj)

            # Product spec step 1 replaces "-" with a space; the Color column
            # keeps the hyphenated form (e.g. Color "Rally-Stripe" ->
            # Product "Billie / Rally Stripe").
            color_disp = color.replace("-", " ")
            prod_full = (f"{product_field} / {color_disp}"
                         if color_disp else product_field)
            inseam_label = derive_inseam_label(
                jean_style, inseam, title, handle, size, desc, is_petite)
            inseam_style = derive_inseam_style(
                jean_style, inseam, rise, inseam_label, desc, tags_str)

            row = [""] * len(WORK_HEADERS)
            _set(row, "Style Id",           product_id)
            _set(row, "Handle",             handle)
            _set(row, "Published At",       published_at)
            _set(row, "Created At",         created_at)
            _set(row, "Product",            prod_full)
            _set(row, "Style Name",         style_src)
            _set(row, "Product Type",       product_type)
            _set(row, "Tags",               tags_str)
            _set(row, "Vendor",             vendor)
            _set(row, "Description",        desc)
            _set(row, "Variant Title", " / ".join(
                p for p in (vt_title, color_disp, size, length_token) if p))
            _set(row, "Color",              color)
            _set(row, "Size",               size)
            _set(row, "Rise",               rise)
            _set(row, "Back Rise",          back_rise)
            _set(row, "Inseam",             inseam)
            _set(row, "Leg Opening",        leg_open)
            _set(row, "Price",              price)
            _set(row, "Compare at Price",   compare_at)
            _set(row, "Available for Sale",
                 "TRUE" if v.get("availableForSale") else "FALSE")
            _set(row, "Quantity Available", "" if qty is None else qty)
            _set(row, "Purchase Reset Date", reset_date)
            _set(row, "SKU - Shopify",      vid)
            _set(row, "SKU - Brand",        v.get("sku") or "")
            _set(row, "Barcode",            vj.get("barcode") or
                                            v.get("barcode") or "")
            _set(row, "Image URL",          ((v.get("image") or {}).get("url")
                                             or image_url))
            _set(row, "SKU URL",            sku_url)
            _set(row, "Jean Style",         jean_style)
            _set(row, "Inseam Label",       inseam_label)
            _set(row, "Inseam Style",       inseam_style)
            _set(row, "Rise Label",         rise_label)
            _set(row, "Style Name Source",  style_src)
            product_rows.append(row)

        for r in product_rows:
            _set(r, "Quantity of style", style_total if has_qty else "")
        rows.extend(product_rows)

        if i % 5 == 0 or i == len(products):
            log("Progress: %s/%s products (%s rows)", i, len(products),
                len(rows))
        time.sleep(SLEEP)

    log("Post-processing: Jean Style sibling inference...")
    apply_jean_style_sibling_inference(rows)
    log("Post-processing: Style Name rules...")
    apply_style_name_rules(rows)
    log("Post-processing: Rise Label steps 3-5...")
    apply_rise_label_steps(rows)
    log("Post-processing: Inseam refresh...")
    apply_inseam_refresh(rows)
    return rows


def write_csv(rows: List[List[str]], path: Path) -> None:
    keep = [IDX[h] for h in CSV_HEADERS]
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(CSV_HEADERS)
        for r in rows:
            w.writerow([r[i] for i in keep])


def main() -> None:
    session = make_session()
    rows = build_rows(session)
    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = OUTPUT_DIR / f"AMO_{stamp}.csv"
    write_csv(rows, out)
    log("Wrote %s rows -> %s", len(rows), out)


if __name__ == "__main__":
    main()
