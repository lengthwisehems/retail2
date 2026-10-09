import argparse
import csv
import logging
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from fractions import Fraction
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND = "GOODAMERICAN"
LOG_PATH = BASE_DIR / f"{BRAND.lower()}_run.log"

HOST_ROTATION = [
    "checkout.goodamerican.com",
    "good-american.myshopify.com",
    "www.goodamerican.com",
]

STOREFRONT_TOKEN = "45b215a31a1aa259d1e128badf92328a"

SEARCHSPRING_URL = "https://5ojqb3.a.searchspring.io/api/search/autocomplete.json"
SEARCHSPRING_SITE_ID = "5ojqb3"

COLLECTION_HANDLES = ["womens-jeans", "sale"]

# ---------------------------------------------------------------------------
# Products are dropped when the product title contains any of these words.
# Kept near the top so it is easy to edit.
# ---------------------------------------------------------------------------
FILTER_WORDS: List[str] = [
    "Accessories", "Accessory", "Bermuda", "Bermudas", "Bikini", "Blazer",
    "Blazers", "Blouse", "Blouses", "Bodysuit", "Bodysuits", "Button Up",
    "Button-Up", "Capri", "Cardigan", "Cardigans", "Clothing Top",
    "Clothing Tops", "Coat", "Coats", "Coats & Jackets", "Core Handbags",
    "Corset", "Corsets", "Crop Top", "Crop Tops", "Denim Short",
    "Denim Shorts", "Donation", "Dress", "Dresses", "Fashion Core Handbag",
    "Fashion Core Handbags", "Fashion Handbag", "Fashion Handbags",
    "Gift Wrap", "Goodies Accessories", "Goodies Accessory", "Handbag", "Hat",
    "Heel", "Heels", "Henley", "Hoodie", "Hoodies", "Jacket", "Jackets",
    "Jogger Short", "Jogger Shorts", "Jort", "Jumpsuit", "Jumpsuits", "Neck",
    "One Piece", "One Pieces", "One-Piece", "One-Pieces", "Outerwear",
    "Pant Suit", "Pant Suits", "Purse", "Romper", "Rompers", "Sandel",
    "Sandle", "Scarf", "Scrunchie", "Shacket", "Shipping",
    "Shipping Protection", "Shirt", "Shirts", "Shirts & Tops", "Shoe",
    "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Sleeve", "Sleeves",
    "Suit", "Suits", "Sweat", "Sweater", "Sweaters", "Sweatpant",
    "Sweatpants", "Sweats", "Sweatshirt", "Sweatshirts", "Swim", "T Shirt",
    "T Shirts", "Tank", "Tank Tops", "Tee", "Tees", "Top", "Vest", "Vests",
]
EXCLUDED_TITLE_TERMS = FILTER_WORDS

EXCLUDED_PRODUCT_TYPES = {
    "blazers",
    "bodysuits",
    "dresses",
    "goodies accessories",
    "jackets",
    "jumpsuits",
    "shipping protection",
    "shorts",
    "skirts",
    "sweater",
    "sweats",
    "swim",
    "tank",
    "tops",
    "bermudas",
    "blouses",
    "cardigans",
    "clothing tops",
    "crop tops",
    "denim shorts",
    "hoodies",
    "jogger shorts",
    "one-pieces",
    "pant suits",
    "shirts",
    "suits",
    "sweaters",
    "sweatshirts",
    "tank tops",
    "t-shirts",
    "vests",
    "hat",
    "scarves",
    "scrunchies",
}

SIZE_VALUES = {
    "00",
    "0",
    "2",
    "4",
    "6",
    "8",
    "10",
    "12",
    "14",
    "15 PLUS",
    "16 PLUS",
    "18 PLUS",
    "20 PLUS",
    "22 PLUS",
    "24 PLUS",
    "26 PLUS",
    "28 PLUS",
    "30 PLUS",
    "32 PLUS",
    "XS",
    "S",
    "M",
    "L",
    "XL",
    "XXL",
    "1XL",
    "2XL",
    "3XL",
    "4XL",
    "5XL",
    "00-4",
    "6-12",
    "14-18 PLUS",
    "20-26 PLUS",
    "28-32 PLUS",
}

LENGTH_LABELS = {
    "REGULAR": "Regular",
    "STANDARD": "Regular",
    "LONG": "Long",
    "TALL": "Long",
    "SHORT": "Petite",
    "PETITE": "Petite",
}

# ---------------------------------------------------------------------------
# Product naming engine — keyword categories.
# Each entry is (keyword, label); label defaults to the keyword.
# Matching is case-insensitive PLAIN SUBSTRING (not whole-word) by design:
# short keywords such as "LO" are meant to match inside longer words.
# ---------------------------------------------------------------------------
JEAN_STYLE_KEYWORDS: List[Tuple[str, str]] = [
    ("WIDE", "WIDE LEG"), ("PALAZZO", "PALAZZO"), ("BOOTCUT", "BOOTCUT"),
    ("CIGARETTE", "CIGARETTE"), ("STRAIGHT", "STRAIGHT"), ("FLARES", "FLARE"),
    ("BARREL", "BARREL"), ("BAGGY", "BAGGY"), ("BOOT", "BOOTCUT"),
    ("SKINNY", "SKINNY"), ("WIDE LEG", "WIDE LEG"), ("BOYFRIEND", "BOYFRIEND"),
    ("LOOSE", "LOOSE"), ("PARACHUTE", "PARACHUTE"), ("FLARE ", "FLARE"),
]
PRODUCT_LINE_KEYWORDS: List[Tuple[str, str]] = [
    ("DOLLY JOLEANS", "DOLLY JOLEANS"), ("DOLLY", "DOLLY"),
    ("SUPER COMPRESSION", "SUPER COMPRESSION"),
    ("LIGHT COMPRESSION", "LIGHT COMPRESSION"), ("COMPRESSION", "COMPRESSION"),
    ("ALWAYS FITS", "ALWAYS FITS"), ("SOFT TECH", "SOFT TECH"),
    ("SOFTTECH", "SOFT TECH"), ("SOFT-TECH", "SOFT TECH"),
    ("NEVER FADES", "NEVER FADE"), ("NEVER FADE ", "NEVER FADE"),
    ("POWER STRETCH", "POWER STRETCH"), ("SOFT SCULPT", "SOFT SCULPT"),
    ("SOFT STRETCH", "SOFT STRETCH"),
    ("BETTER THAN LEATHER", "BETTER THAN LEATHER"),
    ("BETTER THAN SUEDE", "BETTER THAN SUEDE"), ("ALWAYS FIT", "ALWAYS FITS"),
    ("JEANIUS", "JEANIUS"), ("WEIGHTLESS", "WEIGHTLESS"),
]
PULLON_KEYWORDS: List[Tuple[str, str]] = [
    ("PULL ON", "PULL ON"), ("PULLON", "PULL ON"), ("PULL-ON", "PULL ON"),
]
TYPE2_KEYWORDS: List[Tuple[str, str]] = [
    ("LEGGINGS", "LEGGINGS"), ("TROUSERS", "TROUSERS"),
    ("SWEATPANTS", "SWEATPANTS"), ("TROUSER ", "TROUSERS"),
    ("TROUSER", "TROUSERS"),
]
FABRIC_KEYWORDS: List[Tuple[str, str]] = [
    ("LIGHTWEIGHT", "LIGHT WEIGHT"), ("LIGHT WEIGHT", "LIGHT WEIGHT"),
    ("HEAVYWEIGHT", "HEAVY WEIGHT"), ("HEAVY WEIGHT", "HEAVY WEIGHT"),
    ("MIDDLEWEIGHT", "MIDDLE WEIGHT"), ("MIDDLE WEIGHT", "MIDDLE WEIGHT"),
    ("VAPOR", "VAPOR"), ("VEGAN", "VEGAN"), ("FAUX", "FAUX"),
    ("COATED", "COATED"), ("WAX", "WAX"), ("SELVEDGE", "SELVAGE"),
    ("SELVAGE", "SELVAGE"), ("STRIPED", "STRIPED"), ("CHECKERED", "CHECKERED"),
    ("PLAID", "PLAID"), ("FLAG", "FLAG"), ("CRUSHED", "CRUSHED"),
    ("KRUSHED", "KRUSHED"), ("FLORAL PRINT", "FLORAL PRINT"),
    ("LEOPARD PRINT", "LEOPARD PRINT"), ("LEOPARD", "LEOPARD"),
    ("SNAKE PRINT", "SNAKE PRINT"), ("SNAKE", "SNAKE"), ("PRINTED", "PRINTED"),
    ("PRINT", "PRINT"), ("FLOCKED DENIM", "FLOCKED DENIM"),
    ("LEATHER", "LEATHER"), ("VELVET DENIM", "VELVET DENIM"),
    ("LEATHERETTE", "LEATHERETTE"), ("CANVAS", "CANVAS"),
    ("CHIFFON", "CHIFFON"), ("CORDUROY", "CORDUROY"), ("CROCHET", "CROCHET"),
    ("FLANNEL", "FLANNEL"), ("FLOCKED", "FLOCKED"), ("LITE LINEN", "LITE LINEN"),
    ("LINEN", "LINEN"), ("LINNEN", "LINEN"), ("MESH", "MESH"), ("WOOL", "WOOL"),
    ("PONTE", "PONTE"), ("POPLIN", "POPLIN"), ("VELVET", "VELVET"),
    ("SCUBA", "SCUBA"), ("SILK", "SILK"), ("STONE", "STONE"), ("SUEDE", "SUEDE"),
    ("TERRY", "TERRY"), ("TWILL", "TWILL"), ("DENIM", "DENIM"),
    ("RINSE", "RINSE"), ("WASH", "WASH"),
]
INSEAM_LABEL_KEYWORDS: List[Tuple[str, str]] = [
    ("PETITE", "PETITE"), ("LONG", "LONG"), ("X27 S", "PETITE"),
    ("PETITE X27 S", "PETITE"), ("LONG INSEAM", "LONG"), ("REGULAR", "REGULAR"),
    ("EXTENDED", "LONG"),
]
RISE_KEYWORDS: List[Tuple[str, str]] = [
    ("SUPER HIGH WAIST", "ULTRA HIGH RISE"), ("SUPER HIGH-WAIST", "ULTRA HIGH RISE"),
    ("ULTRA HIGH WAIST", "ULTRA HIGH RISE"), ("ULTRA HIGH-WAIST", "ULTRA HIGH RISE"),
    ("SUPER HIGH RISE", "ULTRA HIGH RISE"), ("SUPER HIGH-RISE", "ULTRA HIGH RISE"),
    ("SUPER LOW WAIST", "ULTRA LOW RISE"), ("SUPER LOW-WAIST", "ULTRA LOW RISE"),
    ("ULTRA HIGH RISE", "ULTRA HIGH RISE"), ("ULTRA HIGH-RISE", "ULTRA HIGH RISE"),
    ("ULTRA LOW WAIST", "ULTRA LOW RISE"), ("ULTRA LOW-WAIST", "ULTRA LOW RISE"),
    ("SUPER LOW RISE", "ULTRA LOW RISE"), ("SUPER LOW-RISE", "ULTRA LOW RISE"),
    ("ULTRA LOW RISE", "ULTRA LOW RISE"), ("ULTRA LOW-RISE", "ULTRA LOW RISE"),
    ("STACKED WAIST", "HIGH RISE"), ("HIGH WAISTED", "HIGH RISE"),
    ("HIGH-WAISTED", "HIGH RISE"), ("LOW WAISTED", "LOW RISE"),
    ("LOW-WAISTED", "LOW RISE"), ("MID WAISTED", "MID RISE"),
    ("V-HIGH RISE", "HIGH RISE"), ("HIGH WAIST", "HIGH RISE"),
    ("HIGH-WAIST", "HIGH RISE"), ("LOW WAISED", "LOW RISE"),
    ("SUPER HIGH", "ULTRA HIGH RISE"), ("SUPER LOW", "ULTRA LOW RISE"),
    ("HIGH RISE", "HIGH RISE"), ("HIGHRISE", "HIGH RISE"),
    ("HIGH-RISE", "HIGH RISE"), ("LOW WAIST", "LOW RISE"),
    ("LOW-WAIST", "LOW RISE"), ("LOW RISE", "LOW RISE"),
    ("LOW-RISE", "LOW RISE"), ("MID RISE", "MID RISE"), ("MID-RISE", "MID RISE"),
    ("HIGH", "HIGH RISE"), ("LOW", "LOW RISE"), ("MID", "MID RISE"),
    ("LO", "LOW RISE"),
]
INSEAM_STYLE_KEYWORDS: List[Tuple[str, str]] = [
    ("CROPPED", "CROPPED"), ("ANKLE", "ANKLE"), ("CROP", "CROPPED"),
    ("FULL LENGTH", "FULL LENGTH"),
]
TYPE_KEYWORDS: List[Tuple[str, str]] = [
    ("JEANS", "JEANS"), ("PANTS", "PANTS"), ("PANT", "PANTS"), ("JEAN", "JEANS"),
]
STYLING_KEYWORDS: List[Tuple[str, str]] = [
    ("WITH", "WITH"), ("W/", "W"), ("W /", "W"), (" W ", "W"),
    ("STIRRUP", "STIRRUP"), ("DIAMOND CUT", "DIAMOND CUT"),
    ("W DEEP V YOKE", "W DEEP V YOKE"), ("V WAIST", "V WAIST"),
    ("DEEP V", "DEEP V"), ("CARGO", "CARGO"), ("EMBELLISHED", "EMBELLISHED"),
    ("EMBROIDERED", "EMBROIDERED"), ("SHINE", "SHINE"), ("SHINY", "SHINY"),
    ("PANELED", "PANELED"), ("PANELLED", "PANELED"), ("PANNELED", "PANELED"),
    ("COLOR BLOCK", "COLOR BLOCK"), ("COLOR BLOCKED", "COLOR BLOCKED"),
    ("COLORBLOCK ", "COLOR BLOCK"), ("COLORBLOCKED", "COLOR BLOCKED"),
    ("CONTRAST", "CONTRAST"), ("PANEL", "PANEL"),
    ("SMOOTH MATTE", "SMOOTH MATTE"), ("BUTTON FRONT", "BUTTON FRONT"),
    ("SLIT FRONT", "SLIT FRONT"), ("SIDE SEAM SNAPS", "SIDE SEAM SNAPS"),
    ("TWISTED OUTSEAM", "TWISTED OUTSEAM"), ("BELTED", "BELTED"),
    ("DRAWSTRING", "DRAWSTRING"), ("ELASTIC WAIST", "ELASTIC WAIST"),
    ("PINTUCKED", "PINTUCKED"), ("VENT", "VENT"), ("PLEATED", "PLEATED"),
    ("DARTED", "DARTED"), ("STITCHED", "STITCHED"), ("PLEATY", "PLEATY"),
    ("SEAMED", "SEAMED"), ("PRESSED", "PRESSED"), ("SEAM ", "SEAM"),
    ("SEAMS", "SEAMS"), ("INSET", "INSET"), ("SIDE ZIP", "SIDE ZIP"),
    ("ZIP ", "ZIP"), ("RIPPED", "RIPPED"), ("TRASHED", "TRASHED"),
    ("PATCHWORK", "PATCHWORK"), ("SPLATTER", "SPLATTER"),
    ("REWORKED", "REWORKED"), ("DISTRESSED", "DISTRESSED"),
    ("DESTROYED", "DESTROYED"), ("W KNEE SLITS", "W KNEE SLITS"),
    ("W KNEE RIPS", "W KNEE RIPS"), ("EXPOSED", "EXPOSED"),
    ("REPAIR", "REPAIR"), ("CUT-OUT", "CUT OUT"), ("PATCH", "PATCH"),
    ("KNEE", "KNEE"), ("SLITS", "SLITS"), ("SLIT FRONT", "SLIT FRONT"),
    ("RIP ", "RIP"), ("RIPS", "RIPS"), ("BRAIDED", "BRAIDED"),
    ("EMBROIDERED FLORAL", "EMBROIDERED FLORAL"),
    ("FLORAL EMBROIDERY", "FLORAL EMBROIDERY"), ("LACE ", "LACE"),
    ("BEADED", "BEADED"), ("ACCENT HARDWARE", "ACCENT HARDWARE"),
    ("HARDWARE", "HARDWARE"), ("W/ STUD DETAILING", "W/ STUD DETAILING"),
    ("STUDDED", "STUDDED"), ("SEQUIN", "SEQUIN"), ("STONED", "STONED"),
    ("PEARL ", "PEARL"), ("CRYSTAL STARS", "CRYSTAL STARS"),
    ("CRYSTAL", "CRYSTAL"), ("KRYSTAL", "KRYSTAL"), ("SPARKLE", "SPARKLE"),
    ("DIAMOND", "DIAMOND"), ("RHINESTONE", "RHINESTONE"), ("STAR ", "STAR"),
    ("STARS", "STARS"), ("FRINGE", "FRINGE"), ("STRIPES", "STRIPES"),
    ("STRIPE ", "STRIPE"), ("EMBROIDERY", "EMBROIDERY"),
    ("EMBELLISHMENT", "EMBELLISHMENT"), ("DETAIL", "DETAIL"),
    ("TRIMMED", "TRIMMED"), ("TRIM ", "TRIM"),
    ("W DARTED BACK POCKET", "W DARTED BACK POCKET"),
    ("W DARTED BACK PKT", "W DARTED BACK PKT"),
    ("SPLIT POCKETS", "SPLIT POCKETS"), ("PATCH POCKET", "PATCH POCKET"),
    ("WELT POCKET", "WELT POCKET"), ("BACK POCKET", "BACK POCKET"),
    ("FLAP ", "FLAP"), ("POCKET", "POCKET"),
    ("W DOUBLE NEEDLE TROUSER HEM", "W DOUBLE NEEDLE TROUSER HEM"),
    ("FRAYED SEAMS", "FRAYED SEAMS"), ("FRAYED SEAM", "FRAYED SEAM"),
    ("W CUFFED HEM", "W CUFFED HEM"), ("TROUSER HEM", "TROUSER HEM"),
    ("CHEWED", "CHEWED"), ("CUTOFF", "CUTOFF"), ("FRAY", "FRAY"),
    ("ROLLED HEM", "ROLLED HEM"), ("HOVER CUFF", "HOVER CUFF"),
    ("RAW HEM", "RAW HEM"), ("RAW", "RAW"), ("ROLLED", "ROLLED"),
    ("SLICE", "SLICE"), ("SLIT", "SLIT"), ("SPLICED", "SPLICED"),
    ("SPLIT", "SPLIT"), ("STEP FRAY", "STEP FRAY"), ("SLIT HEM", "SLIT HEM"),
    ("WIDE CUFF", "WIDE CUFF"), ("WIDE HEM", "WIDE HEM"), ("CUFF", "CUFF"),
    ("CUFFED", "CUFFED"), ("HEM", "HEM"),
]
JEAN_STYLE_ADJ_KEYWORDS: List[Tuple[str, str]] = [
    ("SLIM", "SLIM"), ("STANDARD", "STANDARD"), ("EXTREME", "EXTREME"),
    ("RELAXED", "RELAXED"), ("CURVE", "CURVE"), ("KICK", "KICK"),
    ("MINI", "MINI"), ("TRUE", "TRUE"), ("MICRO", "MICRO"),
    ("OVERSIZED", "OVERSIZED"), ("LOW SLUNG", "LOW SLUNG"), ("WIDE", "WIDE"),
]

# Product Line resolution, in order. Add new lines between GOOD EASE and
# GOOD ICON (or anywhere in this list) as needed.
PRODUCT_LINE_CONTAINS_RULES: List[Tuple[str, str]] = [
    ("THE KHLOE", "THE KHLOE"),
    ("ALWAYS FITS", "ALWAYS FITS"),
    ("DOLLY", "DOLLY"),
    ("GOOD LEGS", "GOOD LEGS"),
    ("GOOD WAIST", "GOOD WAIST"),
    ("GOOD CURVE", "GOOD CURVE"),
    ("GOOD CLASSIC", "GOOD CLASSIC"),
    ("GOOD 90", "GOOD 90s"),
    ("JEANIUS", "JEANIUS"),
    ("GOOD SKATE", "GOOD SKATE"),
    ("GOOD EASE", "GOOD EASE"),
    # <-- add additional product lines here
    ("GOOD ICON", "GOOD ICON"),
]

CSV_HEADERS = [
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
    "Inseam",
    "Price",
    "Compare at Price",
    "Promo",
    "Available for Sale",
    "Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Product Line",
    "Inseam Label",
    "Rise Label",
    "Hem Style",
    "Inseam Style",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def configure_logging() -> None:
    handlers: List[logging.Handler] = []
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    try:
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    except OSError as exc:
        fallback_path = OUTPUT_DIR / f"{BRAND.lower()}_run.log"
        logging.basicConfig(level=logging.INFO)
        logging.warning(
            "Primary log file unavailable (%s). Falling back to %s",
            exc,
            fallback_path,
        )
        try:
            file_handler = logging.FileHandler(fallback_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except OSError:
            handlers.append(logging.StreamHandler())

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    logging.basicConfig(level=logging.INFO, handlers=handlers)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Checkout storefront inventory scraper.")
    parser.add_argument("--max-products", type=int, default=None)
    parser.add_argument("--max-variants", type=int, default=None)
    return parser.parse_args()


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def normalize_output_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value)

    decode_attempts = [("latin1", "utf-8"), ("cp1252", "utf-8")]
    for src, dest in decode_attempts:
        if any(token in text for token in ["Ã", "â", "ï»¿", "Â"]):
            try:
                text = text.encode(src, errors="ignore").decode(dest, errors="ignore")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue

    replacements = {
        "â€™": "'",
        "â€˜": "'",
        "â€œ": '"',
        "â€": '"',
        "ï»¿": "",
        "﻿": "",
        "​": "",
        "é": "e",
        "É": "E",
        "–": " ",
        "—": " ",
        "-": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return clean_text(text)


def clean_description_text(value: Optional[str]) -> str:
    """Straighten curly quotes/apostrophes and drop trademark marks."""
    text = value or ""
    for curly, straight in (("\u2018", "'"), ("\u2019", "'"), ("\u201a", "'"),
                            ("\u201c", '"'), ("\u201d", '"'), ("\u201e", '"'),
                            ("\u00b4", "'"), ("\u02bc", "'")):
        text = text.replace(curly, straight)
    for mark in ("\u2122", "\u00ae", "\u2120", "\u00a9"):
        text = text.replace(mark, "")
    return re.sub(r"\s+", " ", text).strip()


def normalize_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("-", " ").lower()).strip()


def normalize_output_text_keep_hyphen(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value)
    decode_attempts = [("latin1", "utf-8"), ("cp1252", "utf-8")]
    for src, dest in decode_attempts:
        if any(token in text for token in ["Ã", "â", "ï»¿", "Â"]):
            try:
                text = text.encode(src, errors="ignore").decode(dest, errors="ignore")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
    replacements = {
        "â€™": "'",
        "â€˜": "'",
        "â€œ": '"',
        "â€": '"',
        "ï»¿": "",
        "﻿": "",
        "​": "",
        "é": "e",
        "É": "E",
        "–": "-",
        "—": "-",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return clean_text(text)


def title_case_preserve_acronyms(value: str) -> str:
    tokens = normalize_key(value).split()
    formatted = []
    for token in tokens:
        if token.isdigit():
            formatted.append(token)
            continue
        if re.fullmatch(r"\d+s", token):
            formatted.append(token.upper())
            continue
        if token.isupper() and len(token) <= 3:
            formatted.append(token)
            continue
        formatted.append(token.capitalize())
    return " ".join(formatted)


def normalize_size(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip()).upper()


CENTRAL_TZ = ZoneInfo("America/Chicago")


def parse_datetime_central(value: Optional[str]) -> str:
    """ISO timestamp -> 'M/D/YYYY H:MM:SS AM/PM' in US Central time."""
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return ""
    local = dt.astimezone(CENTRAL_TZ)
    return (f"{local.month}/{local.day}/{local.year} "
            f"{local.strftime('%I:%M:%S %p').lstrip('0')}")


def parse_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return format_month_day_year(parsed)


def format_month_day_year(value: datetime) -> str:
    try:
        return value.strftime("%-m/%-d/%Y")
    except ValueError:
        return value.strftime("%#m/%#d/%Y")


def extract_gid_suffix(gid: Optional[str]) -> str:
    if not gid:
        return ""
    return gid.split("/")[-1]


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, object]] = None,
    max_retries: int = 3,
) -> requests.Response:
    for attempt in range(max_retries):
        response = session.request(
            method,
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=30,
        )
        if response.status_code in {429, 500, 502, 503, 504}:
            sleep_for = 2 ** attempt
            logging.warning("Retrying %s %s (status %s)", method, url, response.status_code)
            time.sleep(sleep_for)
            continue
        response.raise_for_status()
        return response
    response.raise_for_status()
    return response


def storefront_post(
    session: requests.Session,
    query: str,
    variables: Dict[str, object],
) -> Dict[str, object]:
    headers = {
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    errors: List[str] = []
    for host in HOST_ROTATION:
        url = f"https://{host}/api/unstable/graphql.json"
        try:
            response = request_with_retry(
                session,
                "POST",
                url,
                headers=headers,
                payload={"query": query, "variables": variables},
            )
        except requests.RequestException as exc:
            errors.append(f"{host}: {exc}")
            continue
        payload = response.json()
        if payload.get("errors"):
            errors.append(f"{host}: {payload['errors']}")
            continue
        return payload["data"]
    raise RuntimeError(f"Storefront request failed: {errors}")


def fetch_collection_products(
    session: requests.Session,
    handle: str,
) -> List[Dict[str, object]]:
    query = """
    query ($handle: String!, $cursor: String) {
      collectionByHandle(handle: $handle) {
        products(first: 250, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            handle
            title
            publishedAt
            createdAt
            updatedAt
            productType
            tags
            vendor
            description
            totalInventory
            onlineStoreUrl
            images(first: 1) {
              nodes {
                url
              }
            }
            options {
              name
            }
            variants(first: 250) {
              nodes {
                id
                title
                sku
                barcode
                availableForSale
                quantityAvailable
                price {
                  amount
                }
                compareAtPrice {
                  amount
                }
                selectedOptions {
                  name
                  value
                }
              }
            }
          }
        }
      }
    }
    """
    products: List[Dict[str, object]] = []
    cursor: Optional[str] = None
    while True:
        data = storefront_post(session, query, {"handle": handle, "cursor": cursor})
        collection = data.get("collectionByHandle")
        if not collection:
            logging.warning("Collection not found: %s", handle)
            break
        product_connection = collection["products"]
        products.extend(product_connection["nodes"])
        page_info = product_connection["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return products


def fetch_searchspring_data(session: requests.Session) -> Dict[str, Dict[str, str]]:
    data_map: Dict[str, Dict[str, str]] = {}
    page = 1
    while True:
        params = {
            "siteId": SEARCHSPRING_SITE_ID,
            "resultsFormat": "json",
            "resultsPerPage": "250",
            "page": str(page),
        }
        response = request_with_retry(session, "GET", SEARCHSPRING_URL, params=params)
        payload = response.json()

        results: Iterable[Dict[str, object]] = []
        if isinstance(payload.get("results"), dict) and payload["results"].get("products"):
            results = payload["results"]["products"]
        elif isinstance(payload.get("results"), list):
            results = payload["results"]
        elif isinstance(payload.get("items"), list):
            results = payload["items"]

        results_list = list(results)
        if not results_list:
            break

        for item in results_list:
            handle = item.get("handle") or item.get("ss_handle") or item.get("product_handle")
            if not handle:
                url = item.get("url") or item.get("ss_url")
                if isinstance(url, str):
                    match = re.search(r"/products/([^/?#]+)", url)
                    if match:
                        handle = match.group(1)
            if not handle:
                continue
            ss_pct = item.get("ss_instock_pct")
            if isinstance(ss_pct, (int, float, str)) and str(ss_pct).isdigit():
                instock_pct = f"{int(ss_pct)}%"
            else:
                instock_pct = ""
            image_url = (
                item.get("imageUrl")
                or item.get("image_url")
                or item.get("image")
                or item.get("ss_image")
            )
            data_map[handle] = {
                "instock_pct": instock_pct,
                "image_url": image_url or "",
            }
        page += 1
    logging.info("Searchspring entries loaded: %s", len(data_map))
    return data_map


_FILTER_PATTERNS = [
    re.compile(r"\b" + re.escape(normalize_key(term)) + r"\b")
    for term in FILTER_WORDS if normalize_key(term)
]


def has_excluded_title(title: str) -> bool:
    """Drop the product when its title contains a filter word.

    normalize_key lowercases, so the patterns are built from lowercased terms;
    comparing the Title-Case list directly never matched. Whole-word matching
    keeps "Top" from firing on a colour like "Topaz".
    """
    normalized = normalize_key(title)
    return any(pattern.search(normalized) for pattern in _FILTER_PATTERNS)


def is_excluded_product_type(product_type: str) -> bool:
    normalized = normalize_key(product_type)
    return normalized in EXCLUDED_PRODUCT_TYPES


def determine_variant_options(
    product_options: List[Dict[str, object]],
    selected_options: List[Dict[str, object]],
) -> Tuple[str, str, str]:
    option_names = [opt["name"] for opt in product_options if opt.get("name")]
    selected_map = {opt["name"]: opt["value"] for opt in selected_options if opt.get("name")}
    values = []
    for name in option_names:
        values.append(selected_map.get(name, ""))
    while len(values) < 3:
        values.append("")
    option1, option2, option3 = values[:3]
    return option1, option2, option3


def parse_size_and_length(option2: str, option3: str) -> Tuple[str, str]:
    size = ""
    length = ""
    opt2_norm = normalize_size(option2)
    opt3_norm = normalize_size(option3)

    if opt2_norm in SIZE_VALUES:
        size = opt2_norm
    if opt3_norm in SIZE_VALUES and not size:
        size = opt3_norm

    for candidate in (opt2_norm, opt3_norm):
        if candidate in LENGTH_LABELS:
            if candidate in {"SHORT", "PETITE"}:
                length = "PETITE"
            elif candidate in {"LONG", "TALL"}:
                length = "LONG"
            else:
                length = "REGULAR"
            break

    return size, length


def infer_length_from_title(product_title: str) -> str:
    title_norm = normalize_key(product_title)
    if re.search(r"\bpetite\b", title_norm):
        return "PETITE"
    if re.search(r"\blong\b", title_norm):
        return "LONG"
    return ""


def adjust_base_title_for_variant(base_title: str, base_counter: Counter[str]) -> str:
    base = clean_text(base_title.replace("-", " "))
    norm = normalize_key(base)

    for token in ["petite", "long"]:
        prefix = f"good {token} "
        if norm.startswith(prefix):
            counterpart = norm.replace(f"good {token} ", "good ", 1)
            if base_counter.get(counterpart, 0) > 0:
                base = re.sub(rf"^\s*good\s+{token}\s+", "GOOD ", base, flags=re.IGNORECASE)
            else:
                return clean_text(base)
            break

    base = re.sub(r"\b(PETITE|LONG)\b", "", base, flags=re.IGNORECASE)
    return clean_text(base)


def build_variant_title(
    product_title: str,
    size: str,
    length: str,
    option1: str,
    base_counter: Counter[str],
) -> str:
    full_title = normalize_output_text(product_title)
    title_parts = [clean_text(part) for part in full_title.split("|")]
    base_title = title_parts[0] if title_parts else full_title
    base_title = adjust_base_title_for_variant(base_title, base_counter)

    color_code = title_parts[1] if len(title_parts) > 1 and title_parts[1] else normalize_output_text(option1)

    parts = [base_title, color_code]
    if size:
        parts.append(size)
    if length:
        parts.append(length)
    else:
        inferred = infer_length_from_title(full_title)
        if inferred:
            parts.append(inferred)
    return " | ".join([part for part in parts if part])


def extract_inseam(description: str, option2: str, option3: str) -> str:
    if not description:
        return ""
    text = normalize_output_text(description)
    lower = text.lower()

    label_to_values: Dict[str, List[str]] = {"Regular": [], "Long": [], "Petite": []}
    label_aliases = {"regular": "Regular", "standard": "Regular", "long": "Long", "tall": "Long", "short": "Petite", "petite": "Petite"}

    labeled_patterns = [
        r"inseam[^\d]{0,30}(regular|standard|long|tall|short|petite)[^\d]{0,10}([0-9]+(?:\.[0-9]+)?)",
        r"(regular|standard|long|tall|short|petite)\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?)",
    ]
    for pat in labeled_patterns:
        for match in re.finditer(pat, lower):
            key = label_aliases.get(match.group(1), "")
            if key:
                label_to_values[key].append(match.group(2))

    desired_label = ""
    for opt in (option2, option3):
        norm = normalize_size(opt)
        if norm in LENGTH_LABELS:
            desired_label = LENGTH_LABELS[norm]
            break

    if desired_label and label_to_values.get(desired_label):
        return label_to_values[desired_label][0]

    inseam_generic = re.search(r"inseam\s*[:\-]?\s*(?:regular|standard|long|tall|short|petite)?\s*([0-9]+(?:\.[0-9]+)?)", lower)
    if inseam_generic:
        return inseam_generic.group(1)

    if "inseam" not in lower and desired_label and label_to_values.get(desired_label):
        return label_to_values[desired_label][0]

    for label in ["Regular", "Long", "Petite"]:
        if label_to_values[label]:
            return label_to_values[label][0]
    return ""


def extract_promo(tags: List[str]) -> str:
    promos = []
    for tag in tags:
        lowered = tag.lower()
        if lowered.startswith("promo:") or lowered.startswith("porem:"):
            date_part = tag.split(":", 1)[-1].strip()
            for match in re.finditer(r"(\d{4})/(\d{1,2})/(\d{1,2})", date_part):
                year, month, day = (int(value) for value in match.groups())
                if not (1 <= month <= 12 and 1 <= day <= 31):
                    continue
                promos.append(f"{month:02d}/{day:02d}/{year}")
    return ", ".join(sorted(set(promos)))


def determine_jean_style(title: str, description: str) -> str:
    title_norm = normalize_key(title)
    desc_norm = normalize_key(description)
    style = ""

    if "flare" in title_norm:
        style = "Flare"
    elif "bootcut" in title_norm or "boot" in title_norm:
        style = "Bootcut"
    elif "skinny" in title_norm:
        style = "Skinny"
    elif any(word in title_norm for word in ["barrel", "bowed", "bow leg", "horseshoe"]):
        style = "Barrel"
    elif any(word in title_norm for word in ["wide", "skate", "palazzo", "ease"]):
        style = "Wide Leg"
    elif any(word in title_norm for word in ["baggy", "work pant"]):
        style = "Baggy"
    elif "boyfriend" in title_norm:
        style = "Boyfriend"
    elif "flare" in desc_norm:
        style = "Flare"
    elif "bootcut" in desc_norm:
        style = "Bootcut"
    elif "skinny" in desc_norm:
        style = "Skinny"
    elif any(word in desc_norm for word in ["barrel", "bowed", "bow leg", "horseshoe"]):
        style = "Barrel"
    elif "slim straight" in title_norm or "slim straight" in desc_norm:
        style = "Straight From Knee"
    elif "straight" in title_norm or "straight" in desc_norm:
        style = "Straight"

    knee_phrases = [
        "an iconic straight fit with vintage inspired",
        "an iconic straight fit with vintage-inspired",
        "body hugging and booty shaping",
        "body hugging, booty shaping",
        "body-hugging, booty shaping",
        "booty-shaping, body-sculpting",
        "booty shaping, body sculpting",
        "straight leg from thigh to ankle",
        "close fit through the hip and thigh",
        "curve defining",
        "fitted through your hips and thighs",
        "fitted throughout the hips",
        "sculpting jeans",
        "form-hugging fit from your waist to your thighs",
        "form-hugging fit through the hips and thighs",
        "form hugging fit from your waist to your thighs",
        "form hugging fit through the hips and thighs",
        "hugs your hips and thighs",
        "straight jeans flatter every curve",
    ]
    if style in {"", "Straight"} and any(phrase in desc_norm for phrase in knee_phrases):
        style = "Straight From Knee"

    thigh_phrases = [
        "loose straight legs",
        "loose through hips and thigh",
        "loose through the hip and thigh",
        "loose through the hips and thigh",
        "dropped crotch and baggy, relaxed legs",
        "straight fit through the hips and thighs",
        "we took everything khloe loves about our denim",
    ]
    if style in {"", "Straight"} and any(phrase in desc_norm for phrase in thigh_phrases):
        style = "Straight From Thigh"
    if style in {"", "Straight"} and "relaxed fit" in desc_norm and "straight" in desc_norm:
        style = "Straight From Thigh"

    if style in {"", "Straight"} and "straight" in title_norm and "relax" in title_norm:
        style = "Straight From Thigh"
    if style in {"", "Straight"} and "wide" in desc_norm:
        style = "Wide Leg"

    if style == "Straight":
        return ""
    return style




def normalize_product_line_key(value: str) -> str:
    phrase = normalize_key(value).strip(" -|,.;:")
    if not phrase:
        return ""
    words = phrase.split()
    if words and words[-1].endswith("s") and len(words[-1]) > 1 and words[-1].isalpha():
        words[-1] = words[-1][:-1]
    return " ".join(words)


def extract_before_good_phrase(title: str) -> Tuple[str, str]:
    normalized = normalize_output_text(title)
    base_title = clean_text(normalized.split("|")[0])
    match = re.search(r"\bgood\b", base_title, flags=re.IGNORECASE)
    if not match:
        return "LIMITED EDITION", ""
    prefix = base_title[: match.start()].strip(" -|,.;:")
    if not prefix:
        return "CORE", ""
    return "RAW", clean_text(prefix)


def build_product_line_context(products: List[Dict[str, object]]) -> Dict[str, str]:
    cluster_surface_counts: Dict[str, Counter[str]] = defaultdict(Counter)
    cluster_total_counts: Dict[str, int] = defaultdict(int)
    cluster_pre_good_counts: Dict[str, int] = defaultdict(int)
    cluster_title_counts: Dict[str, int] = defaultdict(int)
    raw_key_by_pid: Dict[str, str] = {}
    status_by_pid: Dict[str, str] = {}

    normalized_titles = []
    for product in products:
        title = normalize_output_text(product.get("title", ""))
        normalized_titles.append(normalize_key(title))

    for product, title_norm in zip(products, normalized_titles):
        pid = str(product.get("id", ""))
        title = normalize_output_text(product.get("title", ""))
        status, raw = extract_before_good_phrase(title)
        status_by_pid[pid] = status
        if status == "RAW" and raw:
            key = normalize_product_line_key(raw)
            if key:
                raw_key_by_pid[pid] = key
                cluster_surface_counts[key][title_case_preserve_acronyms(raw)] += 1
                cluster_total_counts[key] += 1

    for key in cluster_surface_counts:
        if not key:
            continue
        pattern = r"\b" + re.escape(key) + r"s?\b"
        pre_good_pattern = r"\b" + re.escape(key) + r"s?\s+good\b"
        for title_norm in normalized_titles:
            if re.search(pattern, title_norm, flags=re.IGNORECASE):
                cluster_title_counts[key] += 1
            if re.search(pre_good_pattern, title_norm, flags=re.IGNORECASE):
                cluster_pre_good_counts[key] += 1

    canonical_by_key: Dict[str, str] = {}
    for key, counter in cluster_surface_counts.items():
        best_count = max(counter.values())
        winners = [surface for surface, count in counter.items() if count == best_count]
        winners.sort(key=lambda s: (0 if s == s.title() else 1, -len(s), s))
        canonical_by_key[key] = winners[0]

    result: Dict[str, str] = {}
    for product in products:
        pid = str(product.get("id", ""))
        title = normalize_output_text(product.get("title", ""))
        status = status_by_pid.get(pid, "LIMITED EDITION")
        if status == "RAW":
            key = raw_key_by_pid.get(pid, "")
            result[pid] = canonical_by_key.get(key, "Core")
            continue
        if status == "CORE":
            result[pid] = "Core"
        else:
            result[pid] = "Limited Edition"

        title_norm = normalize_key(title)
        if "good" not in title_norm:
            continue

        best_key = ""
        best_len = -1
        best_freq = -1
        for key, canonical in canonical_by_key.items():
            if not key:
                continue
            total = cluster_total_counts.get(key, 0)
            pre_good = cluster_pre_good_counts.get(key, 0)
            if total == 0 or pre_good / total < 0.8:
                continue
            pattern = r"\b" + re.escape(key) + r"s?\b"
            if re.search(pattern, title_norm, flags=re.IGNORECASE):
                phrase_len = len(key)
                freq = cluster_total_counts.get(key, 0)
                if phrase_len > best_len or (phrase_len == best_len and freq > best_freq):
                    best_key = key
                    best_len = phrase_len
                    best_freq = freq
        if best_key:
            result[pid] = canonical_by_key[best_key]
    return result


def build_base_title_counter(products: List[Dict[str, object]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for product in products:
        title = normalize_output_text(product.get("title", ""))
        base = clean_text(title.split("|")[0])
        counter[normalize_key(base)] += 1
    return counter


def build_grouping_key_set(
    products: List[Dict[str, object]],
    product_line_map: Dict[str, str],
) -> set:
    keys = set()
    for product in products:
        title = normalize_output_text_keep_hyphen(product.get("title", ""))
        style_name = clean_text(title.split("|")[0])
        product_line = product_line_map.get(str(product.get("id", "")), "")
        grouping = normalize_key(style_name)
        pl_norm = normalize_product_line_key(product_line)
        if pl_norm:
            pattern = r"\b" + re.escape(pl_norm) + r"s?\b"
            grouping = re.sub(pattern, "", grouping, flags=re.IGNORECASE)

        rise_patterns = [
            r"ultra high rise",
            r"super high rise",
            r"high rise",
            r"mid rise",
            r"med rise",
            r"medium rise",
            r"low rise",
        ]
        for pat in rise_patterns:
            grouping = re.sub(r"\b" + pat + r"\b", "", grouping)

        grouping = re.sub(r"\bwide leg\b", "wide", grouping)
        if re.search(r"\bskate\b", grouping) and re.search(r"\bwide\b", grouping):
            has_good = bool(re.search(r"\bgood\b", grouping))
            grouping = re.sub(r"\bgood\b", "", grouping)
            grouping = re.sub(r"\bskate\b", "", grouping)
            grouping = re.sub(r"\bwide\b", "", grouping)
            grouping = clean_text(grouping)
            prefix = "good skate wide" if has_good else "skate wide"
            grouping = f"{prefix} {grouping}".strip()

        grouping = re.sub(r"\bcropped\b", "crop", grouping)
        grouping = re.sub(r"\bmini[- ]boot\b", "mini boot", grouping)
        grouping = re.sub(r"\b(petite|long)\b", "", grouping)
        grouping = re.sub(r"\b(jean|jeans|pants)\b", "", grouping)
        grouping = clean_text(grouping)
        keys.add(normalize_key(grouping))
    return keys


def build_style_grouping(
    style_name: str,
    product_line: str,
    base_title_set: set,
    grouping_key_set: set,
) -> str:
    grouping = normalize_key(style_name)
    pl_norm = normalize_product_line_key(product_line)
    if pl_norm:
        pattern = r"\b" + re.escape(pl_norm) + r"s?\b"
        grouping = re.sub(pattern, "", grouping, flags=re.IGNORECASE)

    rise_patterns = [
        r"ultra high rise",
        r"super high rise",
        r"high rise",
        r"mid rise",
        r"med rise",
        r"medium rise",
        r"low rise",
    ]
    for pat in rise_patterns:
        grouping = re.sub(r"\b" + pat + r"\b", "", grouping)

    grouping = re.sub(r"\bcropped\b", "crop", grouping)
    grouping = re.sub(r"\bankle\b", "ankle", grouping)
    grouping = re.sub(r"\bmini[- ]boot\b", "mini boot", grouping)
    grouping = re.sub(r"\bwide leg\b", "wide", grouping)

    tokens = grouping.split()
    if "skate" in tokens and "wide" in tokens:
        has_good = "good" in tokens
        tokens = [t for t in tokens if t not in {"good", "skate", "wide"}]
        prefix = ["good", "skate", "wide"] if has_good else ["skate", "wide"]
        tokens = prefix + tokens
        grouping = " ".join(tokens)

    style_norm = normalize_key(style_name)
    for token in ["petite", "long"]:
        if f"good {token}" in style_norm:
            counterpart = re.sub(rf"\b{token}\b", "", grouping)
            counterpart = re.sub(r"\b(jean|jeans|pants)\b", "", counterpart)
            counterpart = re.sub(r"\bcropped\b", "crop", counterpart)
            counterpart = clean_text(counterpart)
            if normalize_key(counterpart) in grouping_key_set:
                grouping = re.sub(rf"\b{token}\b", "", grouping)
        else:
            grouping = re.sub(rf"\b{token}\b", "", grouping)

    grouping = clean_text(grouping)

    garment = ""
    for suffix in ["jean", "jeans", "pants", "leggings", "trousers", "sweatpants"]:
        if grouping.endswith(f" {suffix}") or grouping == suffix:
            garment = suffix
            grouping = grouping[: -len(suffix)].strip()
            break

    tokens = grouping.split()
    crop_tokens = [t for t in tokens if t in {"crop", "ankle"}]
    tokens = [t for t in tokens if t not in {"crop", "ankle"}]

    pull_on_present = False
    if "pull on" in grouping:
        pull_on_present = True
        tokens = [t for t in tokens if t not in {"pull", "on"}]

    if pull_on_present:
        tokens.extend(crop_tokens)
        tokens.append("pull on")
    else:
        tokens.extend(crop_tokens)

    if garment:
        if garment in {"jean", "jeans", "pants"}:
            garment = ""
        else:
            tokens.append(garment)

    grouping = clean_text(grouping)
    if tokens:
        grouping = clean_text(" ".join(tokens))
    return title_case_preserve_acronyms(grouping)


def determine_product_line(title: str) -> str:
    base_title = clean_text(normalize_output_text(title).split("|")[0])
    upper_title = base_title.upper()
    if not re.search(r"\bgood\b", upper_title, flags=re.IGNORECASE):
        return "Limited Edition"
    prefix = re.split(r"\bgood\b", upper_title, maxsplit=1, flags=re.IGNORECASE)[0].strip(" -|,.;:")
    if not prefix:
        return "Core"
    return clean_text(prefix.title())


def determine_inseam_label(option2: str, option3: str, title: str, inseam: str) -> str:
    for opt in (option2, option3):
        normalized = normalize_size(opt)
        if normalized in LENGTH_LABELS:
            return LENGTH_LABELS[normalized]

    title_norm = normalize_key(title)
    has_petite = "petite" in title_norm
    has_long = "long" in title_norm
    has_regular = "regular" in title_norm or "standard" in title_norm

    conflict = (has_petite and has_long) or (has_long and has_regular)
    if not conflict:
        if has_petite:
            return "Petite"
        if has_long:
            return "Long"

    if conflict and inseam:
        try:
            if float(inseam) > 32:
                return "Long"
        except ValueError:
            pass
    return ""


def determine_rise_label(description: str) -> str:
    desc_norm = normalize_key(description)
    if "ultra high rise" in desc_norm or "ultra high-rise" in desc_norm:
        return "Ultra High"
    if "super high rise" in desc_norm or "super high-rise" in desc_norm:
        return "Ultra High"
    if "high rise" in desc_norm or "high-rise" in desc_norm:
        return "High"
    if "mid rise" in desc_norm or "mid-rise" in desc_norm:
        return "Mid"
    if "low rise" in desc_norm or "low-rise" in desc_norm:
        return "Low"
    return ""


def determine_hem_style(description: str) -> str:
    desc_norm = normalize_key(description)
    if any(phrase in desc_norm for phrase in ["slits at hem", "twisted outseam slit detail"]):
        return "Split Hem"
    if any(
        phrase in desc_norm
        for phrase in [
            "clean hem",
            "clean details and hem",
            "clean hem with grinding",
            "grinded hem",
            "clean, cuffed",
        ]
    ):
        return "Clean Hem"
    if any(
        phrase in desc_norm
        for phrase in [
            "raw hem",
            "raw, distressed step hem",
            "frayed hem",
            "released, raw hem",
            "released hem",
        ]
    ):
        return "Raw Hem"
    if any(phrase in desc_norm for phrase in ["wide hem", "tuxedo hem", "trouser hem"]):
        return "Wide Hem"
    if any(
        phrase in desc_norm
        for phrase in [
            "distressed hem",
            "chewed hem",
            "light distressed hem",
            "distressed knees and hem",
            "distressed detailing and hem",
        ]
    ):
        return "Distressed Hem"
    return ""


def determine_inseam_style(title: str, handle: str, description: str, inseam: str) -> str:
    title_norm = normalize_key(title)
    handle_norm = normalize_key(handle)
    desc_norm = normalize_key(description)

    if "crop" in title_norm or "kick" in title_norm or "crop" in handle_norm or "kick" in handle_norm:
        return "Crop"
    if "ankle" in title_norm or "ankle" in handle_norm:
        return "Ankle"

    if any(
        phrase in desc_norm
        for phrase in [
            "full length",
            "stack at the ankle",
            "floor-length",
            "full-length",
            "floor-sweeping",
            "floor-skimming",
            "floor sweeping",
            "floor-grazing",
            "hit just below the ankle",
            "long line silhouette",
            "slouchy leg",
        ]
    ):
        return "Full Length"
    if any(
        phrase in desc_norm
        for phrase in [
            "ankle-length",
            "hits just right at the ankle",
            "hit at the ankle",
            "taper at the ankle",
            "tapers at the ankle",
            "tapers slightly at the ankle",
        ]
    ):
        return "Ankle"

    try:
        if inseam and float(inseam) >= 32:
            return "Full Length"
    except ValueError:
        pass
    return ""


def determine_color_standardized(tags: List[str], description: str) -> str:
    tags_norm = [normalize_key(tag) for tag in tags]
    if any("animal" in tag for tag in tags_norm):
        return "Animal Print"
    if any("print" in tag for tag in tags_norm):
        return "Print"
    if any("pink" in tag for tag in tags_norm):
        return "Pink"
    if any("blue" in tag for tag in tags_norm):
        return "Blue"
    if any("black" in tag for tag in tags_norm):
        return "Black"
    if any("brown" in tag for tag in tags_norm):
        return "Brown"
    if any("grey" in tag for tag in tags_norm):
        return "Grey"
    if any("white" in tag for tag in tags_norm):
        return "White"

    desc_norm = normalize_key(description)
    if "blue wash" in desc_norm:
        return "Blue"
    if "black wash" in desc_norm:
        return "Black"
    if "white wash" in desc_norm:
        return "White"
    return ""


def determine_color_simplified(
    tags: List[str],
    description: str,
    color_standardized: str,
) -> str:
    tags_norm = [normalize_key(tag) for tag in tags]
    desc_norm = normalize_key(description)
    color_norm = normalize_key(color_standardized)

    if color_norm in {"white", "tan"} or any(
        term in tags_norm for term in ["washlightblue", "lightwash"]
    ):
        return "Light"
    if color_norm == "black" or any("washdarkblue" in tag for tag in tags_norm):
        return "Dark"
    if any("washmediumlightblue" in tag for tag in tags_norm):
        return "Light to Medium"
    if any("washmediumdarkblue" in tag for tag in tags_norm):
        return "Medium to Dark"
    if any("washmediumblue" in tag for tag in tags_norm):
        return "Medium"
    if "light blue wash" in desc_norm or "white wash" in desc_norm:
        return "Light"
    if "black wash" in desc_norm or "dark blue wash" in desc_norm:
        return "Dark"
    if "medium light blue wash" in desc_norm:
        return "Light to Medium"
    if "medium dark blue wash" in desc_norm:
        return "Medium to Dark"
    if "medium blue wash" in desc_norm:
        return "Medium"
    return ""


def determine_stretch(description: str) -> str:
    desc_norm = normalize_key(description)
    desc_norm = desc_norm.replace("-", " ")
    if any(term in desc_norm for term in ["ridgid", "rigid", "non stretch", "no stretch"]):
        return "Rigid"
    if any(term in desc_norm for term in ["super stretch", "compression", "softtech"]):
        return "Medium Stretch"
    if "comfort stretch" in desc_norm:
        return "Low to Medium Stretch"
    if "comfort denim" in desc_norm:
        return "Low Stretch"
    if "always fit" in desc_norm:
        return "High Stretch"
    if any(term in desc_norm for term in ["power stretch", "jeanius", "pull on", "pull-on"]):
        return "Medium to High Stretch"
    if "stretch" in desc_norm:
        return "Stretch"
    return ""


# ---------------------------------------------------------------------------
# Product naming engine (Steps 1-7)
# ---------------------------------------------------------------------------
def _clean_naming_title(product_title: str) -> str:
    """Everything before the first '|', trimmed."""
    text = normalize_output_text(product_title or "")
    return clean_text(text.split("|")[0])


def _kw_present(keyword: str, hay: str, whole_word: bool) -> bool:
    token = keyword.upper()
    if whole_word:
        return bool(re.search(r"\b" + re.escape(token.strip()) + r"\b", hay))
    return token in hay


def mode1_first_match(title: str, pairs: List[Tuple[str, str]],
                      whole_word: bool = False) -> str:
    """Return the first RAW keyword found anywhere in the title, else ''."""
    hay = (title or "").upper()
    for keyword, _label in pairs:
        if _kw_present(keyword, hay, whole_word):
            return keyword
    return ""


def mode2_maximal_join(title: str, pairs: List[Tuple[str, str]],
                       whole_word: bool = False) -> str:
    """Keep only the longest/most specific matches, join their labels.

    A matched keyword is dropped when it is a substring of another, longer
    matched keyword ("DOLLY" loses to "DOLLY PARTON"). Surviving labels are
    emitted in keyword-list order.
    """
    hay = (title or "").upper()
    matched = [(i, kw, lbl) for i, (kw, lbl) in enumerate(pairs)
               if _kw_present(kw, hay, whole_word)]
    if not matched:
        return ""
    survivors = []
    for i, kw, lbl in matched:
        ku = kw.upper().strip()
        shadowed = any(
            ku != other.upper().strip()
            and len(other.upper().strip()) > len(ku)
            and ku in other.upper().strip()
            for _j, other, _l in matched
        )
        if not shadowed:
            survivors.append((i, lbl))
    out: List[str] = []
    for _i, lbl in sorted(survivors):
        if lbl and lbl not in out:
            out.append(lbl)
    return " ".join(out)


def _naming_cleanups(text: str) -> str:
    """90s normalization and the KHLOE accent fix."""
    if not text:
        return ""
    out = re.sub(r"'?90'?[sS]\b", "90s", text)
    out = out.replace("90S", "90s")
    out = out.replace("KHLOÉ", "KHLOE").replace("KHLOé", "KHLOE")
    return clean_text(out)


def _join_pieces(pieces: List[str]) -> str:
    return clean_text(" ".join(p for p in pieces if p))


LENGTH_WORDS_IN_TITLE = ["LONG INSEAM", "PETITE", "REGULAR", "SHORT",
                         "EXTENDED", "TALL", "LONG"]


def _strip_length_words(title: str) -> str:
    """Remove the length word so it cannot feed the other categories.

    The length belongs in its own segment, and leaving it in makes the
    two-letter rise keyword "LO" match inside "LONG" and stamp a spurious
    LOW RISE onto every long-inseam style.
    """
    out = title
    for word in LENGTH_WORDS_IN_TITLE:
        out = re.sub(rf"\b{re.escape(word)}\b", " ", out, flags=re.IGNORECASE)
    return clean_text(out)


def compute_naming(product_title: str, jean_style_first_word: str = "",
                   handle: str = "") -> Dict[str, str]:
    """Steps 1-7. Returns every intermediate label plus the final outputs."""
    raw_title = _clean_naming_title(product_title)
    # The length keyword is read from the raw title; everything else is read
    # from the title with the length word taken out.
    inseam_label_kw = mode2_maximal_join(raw_title, INSEAM_LABEL_KEYWORDS)
    title = _strip_length_words(raw_title)

    # Step 1 — Mode 2 over each category
    jean_style_label = mode2_maximal_join(title, JEAN_STYLE_KEYWORDS)
    product_line_label = mode2_maximal_join(title, PRODUCT_LINE_KEYWORDS)
    # The line can live only in the handle; slot it in so the Step 4/5 order
    # places it exactly as a title-sourced product line would be placed.
    if not product_line_label and "soft-tech" in (handle or "").lower():
        product_line_label = "SOFT TECH"
    pullon_label = mode2_maximal_join(title, PULLON_KEYWORDS)
    type2_label = mode2_maximal_join(title, TYPE2_KEYWORDS)
    fabric_label = mode2_maximal_join(title, FABRIC_KEYWORDS)
    # Whole-word: otherwise "LO" matches inside FLOCKED and stamps LOW RISE.
    rise_label_kw = mode2_maximal_join(title, RISE_KEYWORDS, whole_word=True)
    inseam_style_label = mode2_maximal_join(title, INSEAM_STYLE_KEYWORDS)
    type_label = mode2_maximal_join(title, TYPE_KEYWORDS)
    styling_label = mode2_maximal_join(title, STYLING_KEYWORDS)

    # Step 2 — JEAN_STYLE_ADJ special case
    adj_first = mode1_first_match(title, JEAN_STYLE_ADJ_KEYWORDS)
    if adj_first.upper() == "WIDE" and jean_style_label == "PALAZZO":
        jean_style_adj = "WIDE"
    elif adj_first.upper() == "WIDE":
        jean_style_adj = mode2_maximal_join(title, JEAN_STYLE_ADJ_KEYWORDS[:-1])
    else:
        jean_style_adj = mode2_maximal_join(title, JEAN_STYLE_ADJ_KEYWORDS)

    # Step 3 — WHATS_LEFT
    removal_bits = [
        mode1_first_match(title, JEAN_STYLE_ADJ_KEYWORDS),
        mode1_first_match(title, JEAN_STYLE_KEYWORDS),
        mode1_first_match(title, PRODUCT_LINE_KEYWORDS),
        mode1_first_match(title, PULLON_KEYWORDS),
        mode1_first_match(title, TYPE2_KEYWORDS),
        mode1_first_match(title, FABRIC_KEYWORDS),
        mode1_first_match(title, INSEAM_LABEL_KEYWORDS),
        mode1_first_match(title, RISE_KEYWORDS, whole_word=True),
        mode1_first_match(title, INSEAM_STYLE_KEYWORDS),
        mode1_first_match(title, TYPE_KEYWORDS),
        mode1_first_match(title, STYLING_KEYWORDS),
        jean_style_adj, jean_style_label, product_line_label, pullon_label,
        type2_label, fabric_label, inseam_label_kw, rise_label_kw,
        inseam_style_label, type_label, styling_label,
    ]
    removal_words = {w.upper() for w in " ".join(removal_bits).split() if w}
    whats_left = " ".join(w for w in title.split()
                          if w.upper() not in removal_words)
    whats_left = clean_text(whats_left)

    # Step 4 — VARIANT_TITLE_PRE
    is_always_dolly = (product_line_label == "ALWAYS FITS"
                       or "DOLLY" in product_line_label)
    if is_always_dolly:
        vt_pieces = [product_line_label, whats_left, jean_style_adj,
                     jean_style_label, pullon_label, rise_label_kw,
                     styling_label, fabric_label, inseam_style_label,
                     type2_label, type_label, inseam_label_kw]
    elif not whats_left and not product_line_label:
        vt_pieces = [pullon_label, jean_style_adj, jean_style_label,
                     rise_label_kw, styling_label, fabric_label,
                     inseam_style_label, type2_label, type_label]
    elif not whats_left:
        vt_pieces = [product_line_label, jean_style_adj, jean_style_label,
                     pullon_label, rise_label_kw, styling_label, fabric_label,
                     inseam_style_label, type2_label, type_label]
    else:
        vt_pieces = [whats_left, jean_style_adj, jean_style_label,
                     product_line_label, pullon_label, rise_label_kw,
                     styling_label, fabric_label, inseam_style_label,
                     type2_label, type_label]
    variant_title_pre = _naming_cleanups(_join_pieces(vt_pieces))

    # Step 5 — STYLE_NAME_DRAFT
    js_for_draft = jean_style_label or jean_style_first_word
    if is_always_dolly:
        sn_pieces = [product_line_label, whats_left, jean_style_adj,
                     js_for_draft, pullon_label, type2_label]
    elif not whats_left and not product_line_label:
        sn_pieces = [pullon_label, jean_style_adj, js_for_draft, type2_label]
    elif not whats_left:
        sn_pieces = [product_line_label, jean_style_adj, js_for_draft,
                     pullon_label, type2_label]
    else:
        sn_pieces = [whats_left, jean_style_adj, js_for_draft,
                     product_line_label, pullon_label, type2_label]
    style_name_draft = _naming_cleanups(_join_pieces(sn_pieces))

    # Step 6 — STYLE NAME
    draft = style_name_draft
    only_jean_style = draft and draft in {jean_style_label,
                                          jean_style_first_word}
    if not draft:
        style_name = _join_pieces([styling_label or fabric_label,
                                   jean_style_first_word])
    elif only_jean_style and not fabric_label and not styling_label:
        style_name = _join_pieces([draft, type_label])
    elif only_jean_style:
        style_name = _join_pieces([styling_label or fabric_label, draft])
    elif " " not in draft:
        style_name = _join_pieces([styling_label or fabric_label, draft])
    else:
        style_name = draft
    if "GOOD SKATE" in draft.upper():
        after = draft.upper().split("GOOD SKATE", 1)[1].strip()
        if not after.startswith("WIDE LEG"):
            style_name = re.sub(r"GOOD SKATE(\s+WIDE)?", "GOOD SKATE WIDE LEG",
                                style_name, count=1, flags=re.IGNORECASE)
    style_name = _naming_cleanups(style_name)

    # Step 7 — PRODUCT LINE
    vt_up = variant_title_pre.upper()
    product_line = ""
    for needle, label in PRODUCT_LINE_CONTAINS_RULES:
        if needle in vt_up:
            product_line = label
            break
    if not product_line:
        wl_up = whats_left.upper()
        adj_up = jean_style_adj.upper()
        if "GOOD" in wl_up and "BOOTCUT" in jean_style_label.upper():
            product_line = _join_pieces(
                ["GOOD", jean_style_adj if adj_up == "SLIM" else "",
                 jean_style_label])
        elif "GOOD " in wl_up:
            # The adjective list can match more than one word ("STANDARD KICK");
            # only the KICK/TRUE part belongs in the product line.
            keep = next((w for w in ("KICK", "TRUE") if w in adj_up.split()), "")
            product_line = _join_pieces(
                [whats_left.split()[0], keep, jean_style_label])
        elif "GOOD" in wl_up:
            keep = "KICK" if "KICK" in adj_up.split() else ""
            product_line = _join_pieces([whats_left, keep, jean_style_label])
        elif "VINTAGE" in vt_up:
            product_line = "VINTAGE"
        elif "POWER STRETCH" in vt_up and "PULL ON" in vt_up:
            product_line = "POWER STRETCH PULL ON"
        elif "PULL ON" in vt_up:
            product_line = "PULL ON"
        elif product_line_label:
            product_line = product_line_label

    if is_always_dolly:
        sn_branch = "dolly"
    elif not whats_left and not product_line_label:
        sn_branch = "bare"
    elif not whats_left:
        sn_branch = "no_whats_left"
    else:
        sn_branch = "default"

    return {
        "sn_branch": sn_branch,
        "jean_style_label": jean_style_label,
        "product_line_label": product_line_label,
        "pullon_label": pullon_label,
        "type2_label": type2_label,
        "fabric_label": fabric_label,
        "inseam_label_kw": inseam_label_kw,
        "rise_label_kw": rise_label_kw,
        "inseam_style_label": inseam_style_label,
        "type_label": type_label,
        "styling_label": styling_label,
        "jean_style_adj": jean_style_adj,
        "whats_left": whats_left,
        "variant_title_pre": variant_title_pre,
        "style_name_draft": style_name_draft,
        "style_name": style_name,
        "product_line": product_line,
    }


# ---------------------------------------------------------------------------
# Jean Style
# ---------------------------------------------------------------------------
NON_TAPER_STYLES = {"Straight From Knee/Thigh", "Bootcut", "Wide Leg",
                    "Boyfriend", "Baggy", "Flare", "Straight From Thigh"}
TAPER_STYLES = {"Taper", "Tapered", "Skinny", "Barrel", "Straight From Knee"}


def _kn(value: str) -> str:
    """Lowercase, hyphens to spaces, collapse whitespace — for keyword tests."""
    text = (value or "").lower().replace("-", " ")
    return re.sub(r"\s+", " ", text).strip()


def jean_style_from_title(title: str) -> str:
    t = " " + _kn(title) + " "

    def has(*needles: str) -> bool:
        return any(_kn(n) in t for n in needles)

    if has("barrel"):
        return "Barrel"
    if has("boot", "bootcut"):
        return "Bootcut"
    if "flare " in t or has("flares"):
        return "Flare"
    if "legging " in t or has("leggings", "skinny"):
        return "Skinny"
    if has("good skate", "palazzo", "wide", "wide leg"):
        return "Wide Leg"
    if has("tapered", "relaxed skinny") or " mom " in t:
        return "Tapered"
    if has("good icon"):
        return "Straight From Knee/Thigh"
    if (has("cigarette", "slim straight", "soft stretch ponte", "good boy")
            or (has("compression") and has("straight"))
            or (has("curve") and has("straight"))):
        return "Straight From Knee"
    if has("good icon", "good true straight", "vintage straight"):
        return "Straight From Knee/Thigh"
    if has("good 90", "oversized straight", "relaxed straight",
           "standard straight", "the khloe"):
        return "Straight From Thigh"
    if has("baggy"):
        return "Baggy"
    if has("straight"):
        return "Straight From Knee"
    return ""


def jean_style_from_title_and_desc(title: str, description: str) -> str:
    t, d = _kn(title), _kn(description)
    if "straight" in t and any(k in d for k in
                               ("slim straight", "slim, straight", "curve")):
        return "Straight From Knee"
    return ""


def jean_style_from_desc(description: str) -> str:
    d = " " + _kn(description) + " "

    def has(*needles: str) -> bool:
        return any(_kn(n) in d for n in needles)

    if has("flare"):
        return "Flare"
    if has("skinny"):
        return "Skinny"
    if has("relaxed wide legs", "wide relaxed legs", "wide leg", "wide legs"):
        return "Wide Leg"
    if has("vintage inspired straight legs"):
        return "Straight From Knee"
    if (has("relaxed") and has("straight")) or has("straight leg from thigh"):
        return "Straight From Thigh"
    if has("baggy"):
        return "Baggy"
    if has("taper", "tapering", "tapered"):
        return "Tapered"
    return ""


# ---------------------------------------------------------------------------
# Rise Label — title, then description, then tags
# ---------------------------------------------------------------------------
RISE_BARE_KEYWORDS = {"high", "low", "mid", "lo"}

RISE_LABEL_RULES: List[Tuple[str, List[str]]] = [
    ("Ultra High", ["SUPER HIGH WAIST", "SUPER HIGH-WAIST", "ULTRA HIGH WAIST",
                    "ULTRA HIGH-WAIST", "SUPER HIGH RISE", "SUPER HIGH-RISE",
                    "ULTRA HIGH RISE", "ULTRA HIGH-RISE", "SUPER HIGH"]),
    ("Ultra Low",  ["SUPER LOW WAIST", "SUPER LOW-WAIST", "ULTRA LOW WAIST",
                    "ULTRA LOW-WAIST", "SUPER LOW RISE", "SUPER LOW-RISE",
                    "ULTRA LOW RISE", "ULTRA LOW-RISE", "SUPER LOW"]),
    ("High",       ["STACKED WAIST", "HIGH WAISTED", "HIGH-WAISTED",
                    "V-HIGH RISE", "HIGH WAIST", "HIGH-WAIST", "HIGH RISE",
                    "HIGHRISE", "HIGH-RISE", "HIGH"]),
    ("Low",        ["LOW WAISTED", "LOW-WAISTED", "LOW WAISED", "LOW WAIST",
                    "LOW-WAIST", "LOW RISE", "LOW-RISE", "LOW", "LO"]),
    ("Mid",        ["MID WAISTED", "MID RISE", "MID-RISE", "MID"]),
]


def determine_rise_label_v2(title: str, description: str, tags_str: str) -> str:
    """Title, then description, then tags; longest matching keyword wins.

    Within a source the most specific keyword is taken rather than the first
    category in list order, so "MID RISE" beats a stray "LOW" in the care
    instructions ("tumble dry on low"). The bare one-word keywords (high, low,
    mid, lo) only count in the title: in body copy "high" matches inside
    "thigh" and a care line like "tumble dry on low" is not a rise.
    """
    for index, source in enumerate((title, description, tags_str)):
        hay = _kn(source)
        if not hay:
            continue
        is_title = index == 0
        best = None
        for order, (label, keywords) in enumerate(RISE_LABEL_RULES):
            for keyword in keywords:
                token = _kn(keyword)
                if token in RISE_BARE_KEYWORDS and not is_title:
                    continue
                if re.search(r"\b" + re.escape(token) + r"\b", hay):
                    candidate = (len(token), -order)
                    if best is None or candidate > best[0]:
                        best = (candidate, label)
        if best:
            return best[1]
    return ""


# ---------------------------------------------------------------------------
# Inseam Label
# ---------------------------------------------------------------------------
HANDLE_LENGTH_WORDS = [("petite", "Petite"), ("short", "Petite"),
                       ("long", "Long"), ("tall", "Long"),
                       ("regular", "Regular")]


def handle_length_label(handle: str) -> str:
    """A length spelled out in the handle wins over the variant options."""
    tokens = [t for t in (handle or "").lower().split("-") if t]
    for token, label in HANDLE_LENGTH_WORDS:
        if token in tokens:
            return label
    return ""


def option_attribute_label(option2: str, option3: str) -> str:
    """Regular / Long / Petite from the option attributes, else ''."""
    values = {_kn(option2), _kn(option3)}
    if values & {"regular", "standard"}:
        return "Regular"
    if values & {"long", "tall"}:
        return "Long"
    if values & {"short", "petite"}:
        return "Petite"
    return ""


def determine_inseam_label_v2(option2: str, option3: str, title: str,
                              inseam: str, handle: str = "") -> str:
    """Handle first, then the variant options, then the title, then inseam.

    A length spelled out in the handle outranks the options, the same
    precedence the Product and Variant Title fields use: a petite handle whose
    options still read Regular is a petite sku.
    """
    label = handle_length_label(handle) or option_attribute_label(option2, option3)
    if label:
        return label
    t = _kn(title)
    if "petite" in t:
        return "Petite"
    if "long" in t or "tall" in t:
        return "Long"
    if "regular" in t or "standard" in t:
        return "Regular"
    try:
        value = float(inseam)
    except (TypeError, ValueError):
        value = None
    if value is not None and value >= 33 and "petite" not in t and "regular" not in t:
        return "Long"
    return "Regular"


# ---------------------------------------------------------------------------
# Inseam
# ---------------------------------------------------------------------------
_NUM_WITH_FRACTION = r"\d+(?:\.\d+)?(?:\s+\d+\s*/\s*\d+)?"


def _parse_inseam_number(text: str) -> Optional[float]:
    """'32 1/2' -> 32.5, '33.5' -> 33.5."""
    if not text:
        return None
    parts = text.strip().split()
    total = 0.0
    for part in parts:
        try:
            total += float(Fraction(part)) if "/" in part else float(part)
        except (ValueError, ZeroDivisionError):
            return None
    return total


def _round_inseam(value: float) -> str:
    out = f"{round(value, 3):g}"
    return out


def extract_inseam_v2(description: str, option2: str, option3: str,
                      sku_no_size: str) -> str:
    """PDP description first, then the trailing number in sku_brand_no_size."""
    desc = description or ""
    attr = option_attribute_label(option2, option3)
    found = ""

    # Paired forms, e.g. "Inseam Regular: 29 Inseam Long: 32"
    pairs: Dict[str, str] = {}
    for m in re.finditer(
            rf"Inseam\s+(Regular|Long|Short|Petite|Tall)\s*:\s*({_NUM_WITH_FRACTION})",
            desc, re.IGNORECASE):
        pairs[m.group(1).lower()] = m.group(2)
    # "Inseam: Short 27" | Regular 29"" / "Inseam: Regular 29" | Long 35""
    for m in re.finditer(
            rf"(Regular|Long|Short|Petite|Tall)\s*({_NUM_WITH_FRACTION})",
            desc, re.IGNORECASE):
        pairs.setdefault(m.group(1).lower(), m.group(2))
    if pairs:
        wanted = {"Regular": ["regular"], "Long": ["long", "tall"],
                  "Petite": ["short", "petite"]}.get(attr, [])
        for key in wanted:
            if key in pairs:
                found = pairs[key]
                break

    if not found:
        m = re.search(rf"Inseam\s*:?\s*({_NUM_WITH_FRACTION})", desc, re.IGNORECASE)
        if m:
            found = m.group(1)
    if not found:
        m = re.search(rf"({_NUM_WITH_FRACTION})\s*[\"”]?\s*inseam", desc,
                      re.IGNORECASE)
        if m:
            found = m.group(1)

    # sku_brand_no_size: two dashes ending in a 2-digit number 20-40
    if sku_no_size and sku_no_size.count("-") == 2:
        m = re.search(r"-(\d{2})$", sku_no_size)
        if m and 20 <= int(m.group(1)) <= 40:
            sku_val = m.group(1)
            current = _parse_inseam_number(found) if found else None
            if current is None or float(sku_val) != current:
                found = sku_val

    if not found:
        return ""
    value = _parse_inseam_number(found)
    return _round_inseam(value) if value is not None else ""


# ---------------------------------------------------------------------------
# Quantity of style — sku_brand minus its size segment
# ---------------------------------------------------------------------------
def build_sku_no_size(sku_brand: str, size: str) -> str:
    """Strip the trailing size (or its first two characters) from the SKU.

    Only the LAST occurrence is removed, so GAGL873CE-B004-26-26 with size
    "26 PLUS" becomes GAGL873CE-B004-26.
    """
    sku = (sku_brand or "").strip()
    if not sku:
        return ""
    size_clean = (size or "").strip()
    if not size_clean:
        return sku
    # A size can span several dash-separated segments ("14-18 PLUS" appears in
    # the SKU as "-14-18"), so peel the trailing segments that belong to it.
    size_tokens = [t for t in re.split(r"[\s-]+", size_clean.upper()) if t]
    trimmed = sku
    for token in reversed(size_tokens):
        m = re.search(r"-" + re.escape(token) + r"$", trimmed, re.IGNORECASE)
        if m:
            trimmed = trimmed[:m.start()]
    if trimmed != sku:
        return trimmed
    head = size_tokens[0][:2] if size_tokens else ""
    if head:
        m = None
        for m in re.finditer(r"-" + re.escape(head) + r"$", sku, re.IGNORECASE):
            pass
        if m:
            return sku[:m.start()]
    return sku


# ---------------------------------------------------------------------------
# Inseam Style
# ---------------------------------------------------------------------------
def determine_inseam_style_v2(jean_style: str, inseam_label: str, inseam: str,
                              keyword_fallback: str) -> str:
    try:
        value = float(inseam)
    except (TypeError, ValueError):
        value = None
    if value is None:
        return (keyword_fallback or "").replace("Crop", "Cropped").replace(
            "Croppedped", "Cropped")

    petite = inseam_label == "Petite"
    if jean_style in NON_TAPER_STYLES:
        if petite:
            if value <= 25:
                return "Cropped"
            return "Ankle" if value < 28 else "Full Length"
        if value <= 27:
            return "Cropped"
        return "Ankle" if value < 30 else "Full Length"
    if jean_style in TAPER_STYLES:
        if petite:
            if value < 25.5:
                return "Cropped"
            return "Ankle" if value <= 27 else "Full Length"
        if value < 27:
            return "Cropped"
        return "Ankle" if value <= 28.5 else "Full Length"
    return (keyword_fallback or "").replace("Crop", "Cropped").replace(
        "Croppedped", "Cropped")


# ---------------------------------------------------------------------------
# Product field
# ---------------------------------------------------------------------------
def _strip_accents_specials(text: str) -> str:
    out = unicodedata.normalize("NFKD", text or "")
    out = "".join(c for c in out if not unicodedata.combining(c))
    out = out.replace("-", " ")
    out = re.sub(r"[^\w\s|'&]", " ", out)
    out = re.sub(r"[ \t]+", " ", out).strip()
    return re.sub(r"\s*\|\s*", " | ", out)


def _place_before_pipe(product: str, word: str) -> str:
    """Ensure `word` sits immediately before the ' | ' separator."""
    if "|" not in product:
        base, rest = product, ""
    else:
        base, rest = product.split("|", 1)
        rest = "|" + rest
    base = re.sub(rf"\b{word}\b", " ", base, flags=re.IGNORECASE)
    base = re.sub(r"\s+", " ", base).strip()
    return clean_text(f"{base} {word} {rest}") if rest else clean_text(f"{base} {word}")


def build_product_field(product_title: str, length_label: str) -> str:
    """length_label is the variant's own length attribute, not the resolved
    Inseam Label: a style whose options carry no length must not pick up a
    REGULAR just because Inseam Label defaults to Regular."""
    product = normalize_output_text(product_title or "")
    vt = _kn(length_label)
    upper = product.upper()

    if "long" in vt and " LONG " not in f" {upper} ":
        product = _place_before_pipe(product, "LONG")
    elif "tall" in vt and " LONG " not in f" {upper} " and " TALL " not in f" {upper} ":
        product = _place_before_pipe(product, "LONG")
    if "petite" in vt and "PETITE" not in product.upper():
        product = _place_before_pipe(product, "PETITE")
    if "regular" in vt and "REGULAR" not in product.upper():
        product = _place_before_pipe(product, "REGULAR")

    # Tall anywhere becomes Long, parked before the pipe
    if re.search(r"\bTALL\b", product, re.IGNORECASE):
        product = re.sub(r"\bTALL\b", " ", product, flags=re.IGNORECASE)
        product = _place_before_pipe(product, "LONG")
    for word in ("LONG", "PETITE", "REGULAR"):
        if re.search(rf"\b{word}\b", product, re.IGNORECASE):
            product = _place_before_pipe(product, word)

    product = _strip_accents_specials(product)
    product = re.sub(r"'?90'?[sS]\b", "90s", product)
    product = product.replace("90S", "90s")
    if re.search(r"\bBOOT\b", product, re.IGNORECASE):
        product = re.sub(r"\bBOOT\b", "BOOTCUT", product, flags=re.IGNORECASE)
    return clean_text(product)



# ---------------------------------------------------------------------------
# Color - Standardized / Simplified
# Ordered rules, first match wins, whole words only: "Standard" must not
# register as "Tan", and "Stone" must not match inside "Stonewashed".
# ---------------------------------------------------------------------------
COLOR_STD_BY_COLOR: List[Tuple[str, List[str]]] = [
    ("Animal Print", ["animal print", "leopard", "snake", "camo"]),
    ("Print",        ["print", "stripes"]),
    ("Blue",         ["blue", "bleu", "blues", "navy", "indigo"]),
    ("Tan",          ["tan", "sand", "buff", "cement", "ginger", "sable",
                      "beige", "khaki"]),
    ("Brown",        ["brown", "cinnamon", "camel", "chocolate", "pecan",
                      "oak", "coffee", "espresso"]),
    ("Green",        ["green", "olive", "cypress", "moss", "sage"]),
    ("Gray",         ["grey", "gray", "stone"]),
    ("Orange",       ["orange"]),
    ("Pink",         ["pink", "blush", "coral"]),
    ("Purple",       ["purple", "violet"]),
    ("Red",          ["red", "wine", "cherry", "burgundy"]),
    ("White",        ["white", "ecru", "egret", "cream", "creme", "blizzard",
                      "ivory", "parchment", "blanc"]),
]
COLOR_STD_YELLOW_TITLE = ["yellow", "sunny"]
COLOR_STD_BLACK = ["black", "noir", "onyx", "raven"]
COLOR_STD_TONE_TO_BLUE = ["dark", "medium", "light"]

COLOR_STD_BY_DESC: List[Tuple[str, List[str]]] = [
    ("Animal Print", ["animal print", "leopard", "snake"]),
    ("Brown",        ["brown"]),
    ("Green",        ["green", "olive"]),
    ("Gray",         ["grey", "gray", "smoke"]),
    ("Orange",       ["orange"]),
    ("Pink",         ["pink"]),
    ("Print",        ["print", "stripes"]),
    ("Green",        ["green", "olive", "cypress", "sage"]),
    ("Purple",       ["purple", "maroon", "violet"]),
    ("Red",          ["red", "wine", "burgundy"]),
    ("Tan",          ["tan", "beige", "khaki"]),
    ("White",        ["white", "ecru", "pearly", "cream"]),
    ("Yellow",       ["yellow"]),
    ("Blue",         ["blue", "navy", "indigo"]),
    ("Black",        ["black", "washed-black"]),
]
# Wash phrases that all imply a blue denim. "<tone> <colour> wash" is matched
# with a wildcard so "dark indigo wash" counts too.
COLOR_STD_WASH_PHRASES = [
    "dark base", "acid wash", "dark rinse", "dark stretch denim", "dark wash",
    "dark washed", "dark vintage wash", "dark vintage inspired wash",
    "rich dark base", "rich, dark base", "medium base", "medium wash",
    "medium vintage wash", "medium rinse", "medium washed",
    "medium vintage inspired wash", "light wash", "light vintage wash",
    "light rinse", "light washed", "light vintage inspired wash",
    "season-ready wash", "medium-dark", "medium-light",
]
COLOR_STD_WASH_REGEX = re.compile(
    r"\b(?:dark|medium|light)\s+\w+\s+wash\b", re.IGNORECASE)


def _has_whole(text: str, phrase: str) -> bool:
    return bool(re.search(r"\b" + re.escape(phrase) + r"\b", text))


def _match_rules(text: str, rules: List[Tuple[str, List[str]]]) -> str:
    for label, phrases in rules:
        if any(_has_whole(text, p) for p in phrases):
            return label
    return ""


def _color_words(color: str) -> str:
    """Colour names carry a wash number (INDIGO964); drop it so whole-word
    matching can still see the colour."""
    return _kn(re.sub(r"(?<=[A-Za-z])\d+", "", color or ""))


def color_standardized_v2(color: str, description: str, title: str) -> str:
    """Steps 1-2: the colour name, then description cues."""
    c = _color_words(color)
    if c:
        hit = _match_rules(c, COLOR_STD_BY_COLOR)
        if hit:
            return hit
        if any(_has_whole(_kn(title), p) for p in COLOR_STD_YELLOW_TITLE):
            return "Yellow"
        if any(_has_whole(c, p) for p in COLOR_STD_BLACK):
            return "Black"
        if any(_has_whole(c, p) for p in COLOR_STD_TONE_TO_BLUE):
            return "Blue"

    d = _kn(description)
    if d:
        hit = _match_rules(d, COLOR_STD_BY_DESC)
        if hit:
            return hit
        if any(_has_whole(d, p) for p in COLOR_STD_WASH_PHRASES) or \
                COLOR_STD_WASH_REGEX.search(d):
            return "Blue"
    return ""


COLOR_SIMPLE_BY_COLOR: List[Tuple[str, List[str]]] = [
    ("Dark",   ["wine", "burgundy", "navy", "dark", "hunter green", "deep",
                "midnight"]),
    ("Light",  ["pastel", "cream", "blush", "icy", "moonwashed", "light"]),
    ("Medium", ["medium", "mid"]),
]
COLOR_SIMPLE_LIGHT_TO_MEDIUM = [
    "medium light", "light to medium", "medium to light", "light-to-medium",
    "medium-to-light", "medium-light", "light-medium", "light/medium",
    "medium/light", "light medium",
]
COLOR_SIMPLE_MEDIUM_TO_DARK = [
    "medium to dark", "dark to medium", "dark-to-medium", "medium-to-dark",
    "dark medium", "medium/dark", "dark/medium", "medium-dark", "dark-medium",
]
COLOR_SIMPLE_DARK = [
    "dark", "deep", "black", "wine", "burgundy", "midnight blue",
    "forest green", "navy", "complex wash", "darker",
    "deep yet tranquil hue", "deep, luxurious wash", "deep, rich hue",
    "rich yet subtle", "rich, deep blue", "urbane grey wash",
]
COLOR_SIMPLE_LIGHT = [
    "light blue", "pale blue", "light vintage", "soft blue", "soft pink",
    "ecru", "white", "acid wash", "acid-wash", "light", "khaki", "tan",
    "ivory", "light gray wash", "light silver-blue", "light wash",
    "lighter accents",
]
COLOR_SIMPLE_MEDIUM = [
    "mid blue", "mid-blue", "medium stone wash", "classic stone washed blue",
    "vintage washed blue", "classic vintage blue", "medium blue",
    "medium wash", "classic blue", "medium-blue wash", "mid-tone blue wash",
    "perfectly blended wash",
]


def color_simplified_v2(color: str, color_standardized: str,
                        description: str) -> str:
    """Steps 1-3: standardized colour, then the colour name, then description."""
    std = (color_standardized or "").strip().lower()
    if std in {"black", "brown"}:
        return "Dark"
    if std in {"white", "tan"}:
        return "Light"

    c = _color_words(color)
    if c:
        hit = _match_rules(c, COLOR_SIMPLE_BY_COLOR)
        if hit:
            return hit

    d = _kn(description)
    if not d:
        return ""
    has_medium = _has_whole(d, "medium")
    has_light = _has_whole(d, "light")
    has_dark = _has_whole(d, "dark")
    if any(_has_whole(d, p) for p in COLOR_SIMPLE_LIGHT_TO_MEDIUM) or \
            (has_medium and has_light):
        return "Light to Medium"
    if any(_has_whole(d, p) for p in COLOR_SIMPLE_MEDIUM_TO_DARK) or \
            (has_dark and has_medium):
        return "Medium to Dark"
    if any(_has_whole(d, p) for p in COLOR_SIMPLE_DARK):
        return "Dark"
    if any(_has_whole(d, p) for p in COLOR_SIMPLE_LIGHT):
        return "Light"
    if any(_has_whole(d, p) for p in COLOR_SIMPLE_MEDIUM):
        return "Medium"
    return ""


def apply_color_fallbacks(rows: List[Dict[str, str]]) -> None:
    """Step 3/4: inherit from skus of the same colour, then the legacy rules."""
    for field in ("Color - Standardized", "Color - Simplified"):
        by_color: Dict[str, List[str]] = {}
        for row in rows:
            key = _kn(row.get("Color", ""))
            if key and row.get(field):
                by_color.setdefault(key, []).append(row[field])
        for row in rows:
            if row.get(field):
                continue
            found = by_color.get(_kn(row.get("Color", "")))
            if found:
                row[field] = Counter(found).most_common(1)[0][0]

    for row in rows:
        if not row.get("Color - Standardized"):
            row["Color - Standardized"] = determine_color_standardized(
                [t.strip() for t in row.get("Tags", "").split(",")],
                row.get("Description", ""))
        if not row.get("Color - Simplified"):
            row["Color - Simplified"] = determine_color_simplified(
                [t.strip() for t in row.get("Tags", "").split(",")],
                row.get("Description", ""), row.get("Color - Standardized", ""))

    # The legacy rules spell it "Grey"; the output standardises on "Gray".
    for row in rows:
        if row.get("Color - Standardized", "").strip().lower() == "grey":
            row["Color - Standardized"] = "Gray"


# ---------------------------------------------------------------------------
# Size guide fallback for Inseam
# ---------------------------------------------------------------------------
SIZE_GUIDE_SECTION = "product-ssr-size-guide"
_SIZE_GUIDE_CACHE: Dict[str, str] = {}
_PDP_TITLE_CACHE: Dict[str, Optional[str]] = {}


def fetch_size_guide_title(session, handle: str) -> Optional[str]:
    """The heading shown at the top of the PDP's Size Guide drawer.

    Returned so it can be checked against the product title before the guide
    is trusted; None means the drawer could not be read.
    """
    if handle in _PDP_TITLE_CACHE:
        return _PDP_TITLE_CACHE[handle]
    title = None
    try:
        response = session.get(
            f"https://www.goodamerican.com/products/{handle}", timeout=30)
        if response.status_code == 200:
            marker = response.text.find('data-size-guide-loader-target="content"')
            if marker > 0:
                window = response.text[max(0, marker - 4000):marker]
                headings = re.findall(r"<h2[^>]*>(.*?)</h2>", window, re.DOTALL)
                if headings:
                    text = re.sub(r"<[^>]+>", " ", headings[-1])
                    title = re.sub(r"\s+", " ", text).strip()
    except Exception as exc:  # noqa: BLE001
        logging.debug("size-guide title lookup failed for %s: %s", handle, exc)
    _PDP_TITLE_CACHE[handle] = title
    return title


def _inseam_from_size_guide_html(html: str) -> str:
    """First value under the Inseam column of a size-guide table."""
    soup = BeautifulSoup(html or "", "html.parser")
    for table in soup.find_all("table"):
        table_rows = table.find_all("tr")
        if len(table_rows) < 2:
            continue
        headers = [c.get_text(" ", strip=True).lower()
                   for c in table_rows[0].find_all(["th", "td"])]
        if "inseam" not in headers:
            continue
        column = headers.index("inseam")
        for data_row in table_rows[1:]:
            cells = data_row.find_all(["th", "td"])
            if len(cells) <= column:
                continue
            m = re.search(_NUM_WITH_FRACTION,
                          cells[column].get_text(" ", strip=True))
            if m:
                number = _parse_inseam_number(m.group(0))
                if number is not None:
                    return _round_inseam(number)
    return ""


def fetch_size_guide_inseam(session, handle: str, product_title: str) -> str:
    """First Inseam value from the PDP's Size Guide drawer.

    The drawer is lazy-loaded, so the table comes from the same section
    rendering endpoint the page itself calls. The drawer heading is compared
    with the product title first and a guide belonging to another product is
    ignored; when the heading cannot be read the guide is still used, because
    the request is already scoped to this product's URL.

    Failures are logged at warning level: this depends on the live site, and a
    silent empty result is indistinguishable from "no guide exists".
    """
    if handle in _SIZE_GUIDE_CACHE:
        return _SIZE_GUIDE_CACHE[handle]
    value = ""
    url = f"https://www.goodamerican.com/products/{handle}"
    try:
        drawer_title = fetch_size_guide_title(session, handle)
        expected = normalize_key(product_title.split("|")[0])
        if drawer_title and expected and normalize_key(drawer_title) != expected:
            logging.info("Size guide skipped for %s: drawer titled %r, product %r",
                         handle, drawer_title, expected)
            _SIZE_GUIDE_CACHE[handle] = ""
            return ""

        # Do NOT send an Accept: application/json header here. Shopify
        # answers that with an empty section body; the browser's default
        # Accept is what returns the rendered table.
        response = session.get(url, params={"sections": SIZE_GUIDE_SECTION},
                               timeout=30, allow_redirects=True)
        if response.status_code != 200:
            logging.warning("Size guide HTTP %s for %s", response.status_code, handle)
            _SIZE_GUIDE_CACHE[handle] = ""
            return ""

        html = ""
        try:
            payload = response.json()
            if isinstance(payload, dict):
                html = payload.get(SIZE_GUIDE_SECTION) or ""
        except ValueError:
            html = response.text      # section markup served directly
        value = _inseam_from_size_guide_html(html or response.text)

        if not value:
            # Last resort: some responses render the guide into the PDP body
            # instead of answering the section request.
            page = session.get(url, timeout=30)
            if page.status_code == 200:
                value = _inseam_from_size_guide_html(page.text)
            if value:
                logging.info("Size guide for %s read from the PDP body", handle)
        if not value:
            logging.warning(
                "Size guide for %s returned no Inseam column (%s chars)",
                handle, len(html or ""))
    except Exception as exc:  # noqa: BLE001
        logging.warning("Size guide lookup failed for %s: %s", handle, exc)
    _SIZE_GUIDE_CACHE[handle] = value
    return value


def apply_size_guide_inseam(rows: List[Dict[str, str]], session) -> None:
    """Fill a still-blank Inseam from the PDP size guide, then re-derive the
    label and style that depend on it."""
    if session is None:
        return
    pending = sorted({row["Handle"] for row in rows if not row["Inseam"].strip()})
    if not pending:
        return
    logging.info("Size guide lookup for %s handles with no inseam", len(pending))
    filled = 0
    for row in rows:
        if row["Inseam"].strip():
            continue
        value = fetch_size_guide_inseam(session, row["Handle"],
                                        row.get("_raw_title", ""))
        if not value:
            continue
        row["Inseam"] = value
        filled += 1
        row["Inseam Label"] = determine_inseam_label_v2(
            row.get("_opt2", ""), row.get("_opt3", ""),
            row.get("_raw_title", ""), value, row["Handle"])
    logging.info("Size guide filled inseam on %s rows", filled)


def build_rows(
    products: List[Dict[str, object]],
    searchspring_map: Dict[str, Dict[str, str]],
    max_products: Optional[int],
    max_variants: Optional[int],
    session=None,
) -> List[Dict[str, str]]:
    seen_products = 0
    staged_rows: List[Dict[str, str]] = []

    for product in products:
        if max_products is not None and seen_products >= max_products:
            break
        title = normalize_output_text(product.get("title", ""))
        if not title or has_excluded_title(title):
            continue
        product_type = normalize_output_text(product.get("productType") or "")
        if product_type and is_excluded_product_type(product_type):
            continue

        handle = product.get("handle", "")
        style_id = extract_gid_suffix(product.get("id"))
        description = clean_description_text(
            normalize_output_text(product.get("description") or ""))
        tags = product.get("tags") or []
        tags = [normalize_output_text(tag) for tag in tags] if isinstance(tags, list) else []
        tags_str = ", ".join(tags)
        online_store_url = product.get("onlineStoreUrl") or ""
        pdp_active = bool(online_store_url)
        if not online_store_url:
            online_store_url = f"https://www.goodamerican.com/products/{handle}"

        # VARIANT_TITLE_PRE does not depend on Jean Style, so build it first
        # and read the Jean Style keywords off it: it has the length word
        # moved out, so phrases like "GOOD 90" stay contiguous.
        naming = compute_naming(title, "", handle)
        jean_style = jean_style_from_title(naming["variant_title_pre"])
        naming = compute_naming(title, jean_style.split()[0] if jean_style else "", handle)

        rise_label = determine_rise_label_v2(title, description, tags_str)
        hem_style = determine_hem_style(description)

        images = product.get("images", {}).get("nodes", []) if isinstance(product.get("images"), dict) else []
        product_image = images[0].get("url") if images else ""
        searchspring = searchspring_map.get(handle, {})
        image_url = searchspring.get("image_url", "") or product_image

        variants = product.get("variants", {}).get("nodes", [])
        if not variants:
            continue
        if max_variants is not None:
            variants = variants[:max_variants]
        product_options = product.get("options") or []

        for variant in variants:
            option1, option2, option3 = determine_variant_options(
                product_options,
                variant.get("selectedOptions") or [],
            )
            size_value, _length_value = parse_size_and_length(option2, option3)
            sku_brand = variant.get("sku", "") or ""
            sku_no_size = build_sku_no_size(sku_brand, size_value)

            inseam_value = extract_inseam_v2(description, option2, option3, sku_no_size)
            inseam_label = determine_inseam_label_v2(
                option2, option3, title, inseam_value, handle)
            attr_label = handle_length_label(handle) or option_attribute_label(option2, option3)

            color_value = normalize_output_text(option1)
            title_parts = [clean_text(part) for part in normalize_output_text(title).split("|")]
            color_code = title_parts[1] if len(title_parts) > 1 and title_parts[1] else color_value

            # The length segment comes from the option attributes when the
            # variant carries them, otherwise from the title's own keyword.
            length_segment = (attr_label or naming["inseam_label_kw"]).upper()
            if length_segment == "STANDARD":
                length_segment = "REGULAR"
            alt_parts = [naming["variant_title_pre"], color_code]
            if length_segment:
                alt_parts.append(length_segment)
            product_title_alt = " | ".join([p for p in alt_parts if p])
            variant_title = " | ".join(
                [p for p in alt_parts + ([size_value] if size_value else []) if p])
            length_segment_store = length_segment

            keyword_inseam_style = determine_inseam_style(title, handle, description, inseam_value)
            inseam_style = determine_inseam_style_v2(
                jean_style, inseam_label, inseam_value, keyword_inseam_style)

            color_std = color_standardized_v2(color_value, description, title)
            color_simple = color_simplified_v2(color_value, color_std, description)
            staged_rows.append({
                "Style Id": style_id,
                "Handle": handle,
                "Published At": parse_date(product.get("publishedAt")),
                "Created At": parse_date(product.get("createdAt")),
                "Updated At": parse_datetime_central(product.get("updatedAt")),
                "Product": build_product_field(title, attr_label),
                "Product Title Alt": product_title_alt,
                "Style Name": naming["style_name"],
                "Product Type": product_type,
                "Tags": tags_str,
                "Vendor": normalize_output_text(product.get("vendor") or ""),
                "Description": description,
                "Variant Title": variant_title,
                "Color": color_value,
                "Size": size_value,
                "Inseam": inseam_value,
                "Price": (variant.get("price") or {}).get("amount", ""),
                "Compare at Price": (variant.get("compareAtPrice") or {}).get("amount", ""),
                "Promo": extract_promo(tags),
                "Available for Sale": str(variant.get("availableForSale", "")),
                "Quantity Available": str(variant.get("quantityAvailable", "")),
                "Quantity of style": "",
                "SKU - Shopify": extract_gid_suffix(variant.get("id")),
                "SKU - Brand": sku_brand,
                "Barcode": variant.get("barcode", ""),
                "Image URL": image_url,
                "SKU URL": online_store_url,
                "Jean Style": jean_style,
                "Product Line": naming["product_line"],
                "Inseam Label": inseam_label,
                "Rise Label": rise_label,
                "Hem Style": hem_style,
                "Inseam Style": inseam_style,
                "Color - Simplified": color_simple,
                "Color - Standardized": color_std,
                "Stretch": determine_stretch(description),
                "_style_name_draft": naming["style_name_draft"],
                "_jean_style_label": naming["jean_style_label"],
                "_sn_branch": naming["sn_branch"],
                "_product_line_label": naming["product_line_label"],
                "_pullon_label": naming["pullon_label"],
                "_type2_label": naming["type2_label"],
                "_vt_pre": naming["variant_title_pre"],
                "_inseam_label_kw": naming["inseam_label_kw"],
                "_color_code": color_code,
                "_attr_label": length_segment_store,
                "_variant_length": attr_label,
                "_raw_title": title,
                "_opt2": option2,
                "_opt3": option3,
                "_sku_no_size": sku_no_size,
                "_pdp_active": "1" if pdp_active else "",
            })

        seen_products += 1

    apply_good_insert_normalization(staged_rows, session)
    apply_good_palazzo_waist(staged_rows)
    rebuild_piped_titles(staged_rows)
    apply_jean_style_draft_fill(staged_rows)
    fill_jean_style_from_text(staged_rows, stage="title_desc")
    apply_jean_style_draft_fill(staged_rows)
    fill_jean_style_from_text(staged_rows, stage="desc")
    apply_jean_style_draft_fill(staged_rows)
    fill_jean_style_from_handle(staged_rows)
    fill_jean_style_from_style_name(staged_rows)
    fill_jean_style_from_tags(staged_rows)
    fill_jean_style_from_style_name(staged_rows)
    apply_jean_style_word_to_style_name(staged_rows)
    apply_size_guide_inseam(staged_rows, session)
    refresh_inseam_style(staged_rows)
    apply_color_fallbacks(staged_rows)
    apply_quantity_of_style(staged_rows)
    apply_duplicate_old_marker(staged_rows)

    for row in staged_rows:
        row.pop("_vt_pre", None)
        row.pop("_inseam_label_kw", None)
        row.pop("_color_code", None)
        row.pop("_attr_label", None)
        row.pop("_variant_length", None)
        row.pop("_good_insert_word", None)
        row.pop("_opt2", None)
        row.pop("_opt3", None)
        row.pop("_raw_title", None)
        row.pop("_jean_style_label", None)
        row.pop("_sn_branch", None)
        row.pop("_product_line_label", None)
        row.pop("_pullon_label", None)
        row.pop("_type2_label", None)
        row.pop("_style_name_draft", None)
        row.pop("_sku_no_size", None)
        row.pop("_pdp_active", None)
    return staged_rows


_SISTER_LINK_CACHE: Dict[str, List[str]] = {}
_INSEAM_CONTAINER = "artificialVariantsInseamContainer"
_PRODUCT_HREF = re.compile(r'href="/products/([^"?#]+)')


def fetch_sister_variant_handles(session, handle: str) -> List[str]:
    """Handles linked from the PDP's artificialVariantsInseam block.

    Good American links a style to its other-inseam siblings there, so a
    petite title points straight at the collection it belongs to. The links
    are read from inside that container rather than by matching attributes on
    the anchor: sisterVariantLink is also used by the colour swatches, which
    would otherwise be picked up instead.
    """
    if handle in _SISTER_LINK_CACHE:
        return _SISTER_LINK_CACHE[handle]
    found: List[str] = []
    try:
        response = session.get(
            f"https://www.goodamerican.com/products/{handle}", timeout=30)
        if response.status_code == 200:
            start = response.text.find(_INSEAM_CONTAINER)
            if start >= 0:
                block = response.text[start:start + 4000]
                found = _PRODUCT_HREF.findall(block)
    except Exception as exc:  # noqa: BLE001
        logging.debug("sister-variant lookup failed for %s: %s", handle, exc)
    found = [h for h in dict.fromkeys(found) if h != handle]
    _SISTER_LINK_CACHE[handle] = found
    return found


def _description_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, normalize_key(left or ""),
                           normalize_key(right or "")).ratio()


GOOD_INSERT_WORDS = ["LEGS", "CLASSIC", "WAIST", "CURVE", "BOY"]


def _color_exact(row: Dict[str, str]) -> str:
    return (row.get("_color_code") or "").strip().upper()


def _color_base(row: Dict[str, str]) -> str:
    """Digit-stripped colour, so BLACK001 can fall back to BLACK."""
    return re.sub(r"\d+$", "", _color_exact(row))


GOOD_INSERT_WORDS = ["LEGS", "CLASSIC", "WAIST", "CURVE", "BOY"]


def _build_color_key_map(rows: List[Dict[str, str]]) -> Dict[str, str]:
    """Fold BLACK001 into BLACK, but keep INDIGO254 apart from INDIGO271.

    A numbered colour is only merged into its bare form when that bare form is
    itself a colour in the data; otherwise stripping digits would lump every
    shade of a wash together.
    """
    colors = {(row.get("_color_code") or "").strip().upper() for row in rows}
    colors.discard("")
    mapping: Dict[str, str] = {}
    for color in colors:
        base = re.sub(r"\d+$", "", color)
        mapping[color] = base if base and base != color and base in colors else color
    return mapping


def apply_good_insert_normalization(rows: List[Dict[str, str]],
                                    session=None) -> None:
    """Re-attach the collection word that a length-specific title drops.

    A petite/long title such as "ALWAYS FITS GOOD PETITE BOOTCUT JEANS" names
    the same style as its regular sibling "ALWAYS FITS GOOD CLASSIC BOOTCUT
    JEANS". The length word belongs in the option-attribute segment, so it is
    stripped here and LEGS/CLASSIC/WAIST/CURVE/BOY is tried after "GOOD";
    the insert is kept only when it matches a style name that already exists
    for the same colour.
    """
    # Colours are compared exactly first. INDIGO254 and INDIGO271 are
    # different washes, so digits cannot simply be dropped; the digit-stripped
    # form is only consulted as a fallback, which is what lets BLACK001 find
    # a sibling listed under plain BLACK.
    names_by_color: Dict[str, Set[str]] = {}
    names_by_color_base: Dict[str, Set[str]] = {}
    for row in rows:
        if row["Style Name"]:
            names_by_color.setdefault(_color_exact(row), set()).add(
                row["Style Name"].upper())
            names_by_color_base.setdefault(_color_base(row), set()).add(
                row["Style Name"].upper())

    rows_by_name: Dict[str, List[Dict[str, str]]] = {}
    rows_by_handle: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        rows_by_name.setdefault(row["Style Name"].upper(), []).append(row)
        rows_by_handle.setdefault(row["Handle"], []).append(row)

    for row in rows:
        kw = row.get("_inseam_label_kw", "")
        if not kw:
            continue
        color_key = _color_exact(row)
        # Only rescue a name that stands alone, or whose every sku is a
        # length-specific one; a name shared with regular skus is already right.
        # Scope the "all skus are length-specific" test to this colour: the
        # whole rule is colour-based, and a same-named style in another colour
        # should not block the rescue.
        peers = [r for r in rows_by_name.get(row["Style Name"].upper(), [])
                 if _color_exact(r) == color_key]
        all_length_specific = all(r.get("_inseam_label_kw") for r in peers)
        if not all_length_specific:
            continue
        stripped_sn = clean_text(re.sub(rf"\b{re.escape(kw)}\b", " ",
                                        row["Style Name"], flags=re.IGNORECASE))
        stripped_vt = clean_text(re.sub(rf"\b{re.escape(kw)}\b", " ",
                                        row.get("_vt_pre", ""), flags=re.IGNORECASE))
        if not stripped_sn:
            continue
        siblings = names_by_color.get(color_key, set())

        def collect(pool: Set[str]) -> List[Tuple[str, str]]:
            out = []
            for word in GOOD_INSERT_WORDS:
                cand = re.sub(r"\bGOOD\b", f"GOOD {word}", stripped_sn, count=1,
                              flags=re.IGNORECASE)
                if cand.upper() in pool and cand.upper() != row["Style Name"].upper():
                    out.append((word, cand))
            return out

        # The PDP's sister-variant link is the only hard evidence of which
        # collection a length-only title belongs to, so it is consulted first
        # and may name a style outside this colour's pool. Failing that, only
        # the EXACT colour is trusted: falling back to the digit-stripped
        # colour pulls in unrelated washes (BLACK284 has no siblings of its
        # own, and borrowing BLACK's would invent a collection for it).
        candidates = collect(siblings)
        chosen_sn, chosen_vt = "", ""

        if session is not None and re.search(r"\bGOOD\b", stripped_sn, re.IGNORECASE):
            sister_names = {
                r["Style Name"].upper()
                for sister in fetch_sister_variant_handles(session, row["Handle"])
                for r in rows_by_handle.get(sister, [])
            }
            if sister_names:
                for word in GOOD_INSERT_WORDS:
                    cand = re.sub(r"\bGOOD\b", f"GOOD {word}", stripped_sn,
                                  count=1, flags=re.IGNORECASE)
                    if cand.upper() in sister_names:
                        candidates = [(word, cand)]
                        break

        if len(candidates) > 1:
            # Fall back to whichever candidate's copy reads most like this one.
            best_score = -1.0
            best = candidates[0]
            for word, cand_sn in candidates:
                peers = rows_by_name.get(cand_sn.upper(), [])
                score = max((_description_similarity(row["Description"],
                                                     peer["Description"])
                             for peer in peers), default=0.0)
                if score > best_score:
                    best_score, best = score, (word, cand_sn)
            candidates = [best]
        if candidates:
            word, chosen_sn = candidates[0]
            chosen_vt = re.sub(r"\bGOOD\b", f"GOOD {word}", stripped_vt,
                               count=1, flags=re.IGNORECASE)
        if not chosen_sn:
            # Nothing corroborates a collection word; just move the length
            # word out of the name rather than guessing one.
            chosen_sn, chosen_vt = stripped_sn, stripped_vt
        row["Style Name"] = chosen_sn
        row["_vt_pre"] = chosen_vt
        if candidates:
            row["_good_insert_word"] = candidates[0][0]

    for row in rows:
        # Product Line reads VARIANT_TITLE_PRE, so it has to be recomputed
        # after the collection word has been re-attached above.
        vt_up = row.get("_vt_pre", "").upper()
        for needle, label in PRODUCT_LINE_CONTAINS_RULES:
            if needle in vt_up:
                row["Product Line"] = label
                break
        parts = [row.get("_vt_pre", ""), row.get("_color_code", "")]
        if row.get("_attr_label"):
            parts.append(row["_attr_label"].upper())
        row["Product Title Alt"] = " | ".join([p for p in parts if p])
        size = row.get("Size", "")
        row["Variant Title"] = " | ".join(
            [p for p in parts + ([size] if size else []) if p])
        row["Product Title Alt"] = dedupe_trailing_length(row["Product Title Alt"])
        row["Variant Title"] = dedupe_trailing_length(row["Variant Title"])
        # Product keeps the brand's own title wording; the collection word is
        # only re-attached in the derived Style Name / piped titles.
        row["Product"] = build_product_field(row.get("_raw_title", ""),
                                             row.get("_variant_length", ""))


def rebuild_piped_titles(rows: List[Dict[str, str]]) -> None:
    """Re-assemble Product Title Alt / Variant Title from the current pieces.

    Later passes (the Palazzo rule) edit VARIANT_TITLE_PRE, so the piped
    titles and the Product Line that reads them have to be rebuilt.
    """
    for row in rows:
        parts = [row.get("_vt_pre", ""), row.get("_color_code", "")]
        if row.get("_attr_label"):
            parts.append(row["_attr_label"].upper())
        parts = [p for p in parts if p]
        size = row.get("Size", "")
        row["Product Title Alt"] = dedupe_trailing_length(" | ".join(parts))
        row["Variant Title"] = dedupe_trailing_length(
            " | ".join(parts + ([size] if size else [])))
        vt_up = row.get("_vt_pre", "").upper()
        for needle, label in PRODUCT_LINE_CONTAINS_RULES:
            if needle in vt_up:
                row["Product Line"] = label
                break


def apply_good_palazzo_waist(rows: List[Dict[str, str]]) -> None:
    """A solitary "GOOD PALAZZO" is really "GOOD WAIST PALAZZO"."""
    pattern = re.compile(r"\bGOOD\s+PALAZZO\b", re.IGNORECASE)
    bare = {row["Style Name"] for row in rows
            if row["Style Name"] and pattern.search(row["Style Name"])}
    if len(bare) != 1:
        return
    for row in rows:
        for field in ("Style Name", "_vt_pre"):
            if row.get(field) and pattern.search(row[field]):
                row[field] = pattern.sub("GOOD WAIST PALAZZO", row[field], count=1)


def apply_jean_style_draft_fill(rows: List[Dict[str, str]]) -> None:
    """Blank Jean Style inherits from rows sharing a STYLE_NAME_DRAFT."""
    groups: Dict[str, Set[str]] = {}
    for row in rows:
        draft = row.get("_style_name_draft", "")
        if draft and row["Jean Style"]:
            groups.setdefault(draft, set()).add(row["Jean Style"])
    for row in rows:
        if row["Jean Style"]:
            continue
        values = groups.get(row.get("_style_name_draft", "")) or set()
        if len(values) == 1:
            row["Jean Style"] = next(iter(values))


def fill_jean_style_from_text(rows: List[Dict[str, str]], stage: str) -> None:
    for row in rows:
        if row["Jean Style"]:
            continue
        if stage == "title_desc":
            row["Jean Style"] = jean_style_from_title_and_desc(
                row["Product"], row["Description"])
        else:
            row["Jean Style"] = jean_style_from_desc(row["Description"])


def fill_jean_style_from_handle(rows: List[Dict[str, str]]) -> None:
    """Last resort before sibling matching: the handle often names the fit."""
    for row in rows:
        if row["Jean Style"]:
            continue
        row["Jean Style"] = jean_style_from_title(row["Handle"])


def fill_jean_style_from_style_name(rows: List[Dict[str, str]]) -> None:
    """Inherit Jean Style from other skus carrying the same Style Name."""
    by_name: Dict[str, List[str]] = {}
    for row in rows:
        if row["Style Name"] and row["Jean Style"]:
            by_name.setdefault(row["Style Name"].upper(), []).append(
                row["Jean Style"])
    for row in rows:
        if row["Jean Style"]:
            continue
        found = by_name.get((row["Style Name"] or "").upper())
        if found:
            row["Jean Style"] = Counter(found).most_common(1)[0][0]


def fill_jean_style_from_tags(rows: List[Dict[str, str]]) -> None:
    """Final fallback: run the keyword ladder over the product tags."""
    for row in rows:
        if row["Jean Style"]:
            continue
        row["Jean Style"] = jean_style_from_title(row["Tags"])


def apply_jean_style_word_to_style_name(rows: List[Dict[str, str]]) -> None:
    """Fill the Style Name's jean-style slot from the final Jean Style.

    Step 5 uses JEAN_STYLE_LABEL when the title supplies one and otherwise
    falls back to the first word of Jean Style. That fallback is only useful
    once Jean Style is settled, which happens after the description and
    sibling passes have run, so the slot is filled here rather than while the
    name is first assembled.
    """
    for row in rows:
        if row.get("_jean_style_label"):
            continue
        jean_style = row.get("Jean Style") or ""
        if not jean_style:
            continue
        word = jean_style.split()[0].upper()
        name = row.get("Style Name") or ""
        if not name or re.search(rf"\b{re.escape(word)}\b", name, re.IGNORECASE):
            continue
        # Where the jean-style slot sits depends on the Step 5 branch. Only
        # the default branch puts the product line after it; the DOLLY and
        # ALWAYS FITS branch leads with the product line, and the branch with
        # no WHATS_LEFT leads with the pull-on label.
        branch = row.get("_sn_branch", "default")
        if branch == "dolly":
            tail_labels = [row.get("_pullon_label", ""), row.get("_type2_label", "")]
        elif branch == "bare":
            tail_labels = [row.get("_type2_label", "")]
        elif branch == "no_whats_left":
            tail_labels = [row.get("_pullon_label", ""), row.get("_type2_label", "")]
        else:
            tail_labels = [row.get("_product_line_label", ""),
                           row.get("_pullon_label", ""),
                           row.get("_type2_label", "")]
        inserted = ""
        for label in tail_labels:
            if not label:
                continue
            m = re.search(rf"\b{re.escape(label)}\b", name, re.IGNORECASE)
            if m:
                inserted = clean_text(
                    f"{name[:m.start()].strip()} {word} {name[m.start():].strip()}")
                break
        row["Style Name"] = inserted or clean_text(f"{name} {word}")


def refresh_inseam_style(rows: List[Dict[str, str]]) -> None:
    for row in rows:
        keyword_fallback = determine_inseam_style(
            row["Product"], row["Handle"], row["Description"], row["Inseam"])
        row["Inseam Style"] = determine_inseam_style_v2(
            row["Jean Style"], row["Inseam Label"], row["Inseam"], keyword_fallback)


def apply_quantity_of_style(rows: List[Dict[str, str]]) -> None:
    """Sum quantityAvailable across every row sharing a sku_brand_no_size."""
    totals: Dict[str, int] = {}
    for row in rows:
        key = row.get("_sku_no_size", "")
        if not key:
            continue
        try:
            qty = int(float(row.get("Quantity Available") or 0))
        except (TypeError, ValueError):
            qty = 0
        totals[key] = totals.get(key, 0) + qty
    for row in rows:
        key = row.get("_sku_no_size", "")
        row["Quantity of style"] = str(totals.get(key, "")) if key in totals else ""


LENGTH_SEGMENT_WORDS = ["LONG", "PETITE", "REGULAR"]


def dedupe_trailing_length(text: str) -> str:
    """Drop the length word from the name when it is also its own segment.

    "ALWAYS FITS ... JEANS LONG | INDIGO446 | LONG" carries LONG twice. The
    segment is authoritative, so the copy in the name goes. In Variant Title
    the length sits before the size rather than last, so every segment after
    the first is checked, not just the final one.
    """
    if not text or "|" not in text:
        return text
    segments = [p.strip().upper() for p in text.split("|")[1:]]
    word = next((w for w in LENGTH_SEGMENT_WORDS
                 for seg in segments
                 if seg == w or seg.startswith(w + " ")), "")
    if not word:
        return text
    head, rest = text.split("|", 1)
    cleaned = re.sub(rf"\b{word}\b", " ", head, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return f"{cleaned} |{rest}" if cleaned else text


def apply_duplicate_old_marker(rows: List[Dict[str, str]]) -> None:
    """Mark the superseded style behind a duplicated Product title.

    Grouping is by Product, which already carries the length word, so a legacy
    handle holding two inseams only gets OLD on the length that has since been
    split out into its own style. A style is marked when it is fully dead
    (nothing for sale, PDP gone, no inventory); otherwise the oldest Created At
    among the colliding styles is marked, so newer sales can be tracked
    separately from the style they replaced.
    """
    by_product: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    for row in rows:
        by_product.setdefault(row["Product"], {}).setdefault(
            row["Style Id"], []).append(row)

    for _product, styles in by_product.items():
        if len(styles) < 2:
            continue
        dead = []
        for style_id, style_rows in styles.items():
            for_sale = any(r["Available for Sale"].lower() == "true"
                           for r in style_rows)
            pdp_live = any(r.get("_pdp_active") for r in style_rows)
            inventory = 0
            for r in style_rows:
                try:
                    inventory += int(float(r.get("Quantity Available") or 0))
                except (TypeError, ValueError):
                    pass
            if not for_sale and not pdp_live and inventory <= 0:
                dead.append(style_id)

        if dead:
            targets = dead
        else:
            def created(style_id: str) -> str:
                raw = styles[style_id][0].get("Created At", "")
                try:
                    return datetime.strptime(raw, "%m/%d/%Y").isoformat()
                except ValueError:
                    return raw
            targets = [min(styles, key=created)]

        for style_id in targets:
            for row in styles[style_id]:
                row["Product"] = _insert_old_marker(row["Product"])
                for field in ("Product Title Alt", "Variant Title"):
                    row[field] = _append_old_segment(row[field])


def _append_old_segment(text: str) -> str:
    """Mark the superseded style in the piped titles.

    OLD follows the trailing length word when there is one, otherwise the
    colour, so "... | BLUE004 | LONG" becomes "... | BLUE004 | LONG OLD".
    """
    if not text or re.search(r"\bOLD\b", text, re.IGNORECASE):
        return text
    parts = [p.strip() for p in text.split("|")]
    for index in range(len(parts) - 1, -1, -1):
        upper = parts[index].upper()
        if any(upper == w or upper.startswith(w + " ")
               for w in LENGTH_SEGMENT_WORDS):
            parts[index] = f"{parts[index]} OLD"
            return " | ".join(parts)
    parts[-1] = f"{parts[-1]} OLD"
    return " | ".join(parts)


def _insert_old_marker(product: str) -> str:
    if re.search(r"OLD", product, re.IGNORECASE):
        return product
    if "|" in product:
        base, rest = product.split("|", 1)
        return clean_text(f"{clean_text(base)} OLD | {clean_text(rest)}")
    return clean_text(f"{product} OLD")


def write_csv(rows: List[Dict[str, str]]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"{BRAND}_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info("CSV written: %s", output_path.resolve())
    return output_path


def main() -> None:
    configure_logging()
    args = parse_args()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    logging.info("Fetching Searchspring data")
    searchspring_map = fetch_searchspring_data(session)

    product_map: Dict[str, Dict[str, object]] = {}
    for handle in COLLECTION_HANDLES:
        logging.info("Fetching collection: %s", handle)
        products = fetch_collection_products(session, handle)
        for product in products:
            product_map[product["id"]] = product
        logging.info("Products in %s: %s", handle, len(products))

    combined_products = list(product_map.values())
    logging.info("Unique products: %s", len(combined_products))
    rows = build_rows(
        combined_products,
        searchspring_map,
        args.max_products,
        args.max_variants,
        session,
    )
    logging.info("Rows prepared: %s", len(rows))
    write_csv(rows)


if __name__ == "__main__":
    main()
