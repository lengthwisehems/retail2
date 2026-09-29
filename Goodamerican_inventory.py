import argparse
import csv
import logging
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests

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


def has_excluded_title(title: str) -> bool:
    normalized = normalize_key(title)
    return any(term in normalized for term in EXCLUDED_TITLE_TERMS)


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


def mode1_first_match(title: str, pairs: List[Tuple[str, str]]) -> str:
    """Return the first RAW keyword found anywhere in the title, else ''."""
    hay = (title or "").upper()
    for keyword, _label in pairs:
        if keyword.upper() in hay:
            return keyword
    return ""


def mode2_maximal_join(title: str, pairs: List[Tuple[str, str]]) -> str:
    """Keep only the longest/most specific matches, join their labels.

    A matched keyword is dropped when it is a substring of another, longer
    matched keyword ("DOLLY" loses to "DOLLY PARTON"). Surviving labels are
    emitted in keyword-list order.
    """
    hay = (title or "").upper()
    matched = [(i, kw, lbl) for i, (kw, lbl) in enumerate(pairs)
               if kw.upper() in hay]
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


def compute_naming(product_title: str, jean_style_first_word: str = "") -> Dict[str, str]:
    """Steps 1-7. Returns every intermediate label plus the final outputs."""
    title = _clean_naming_title(product_title)

    # Step 1 — Mode 2 over each category
    jean_style_label = mode2_maximal_join(title, JEAN_STYLE_KEYWORDS)
    product_line_label = mode2_maximal_join(title, PRODUCT_LINE_KEYWORDS)
    pullon_label = mode2_maximal_join(title, PULLON_KEYWORDS)
    type2_label = mode2_maximal_join(title, TYPE2_KEYWORDS)
    fabric_label = mode2_maximal_join(title, FABRIC_KEYWORDS)
    inseam_label_kw = mode2_maximal_join(title, INSEAM_LABEL_KEYWORDS)
    rise_label_kw = mode2_maximal_join(title, RISE_KEYWORDS)
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
        mode1_first_match(title, RISE_KEYWORDS),
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
                     inseam_style_label, type2_label, type_label,
                     inseam_label_kw]
    elif not whats_left:
        vt_pieces = [product_line_label, jean_style_adj, jean_style_label,
                     pullon_label, rise_label_kw, styling_label, fabric_label,
                     inseam_style_label, type2_label, type_label,
                     inseam_label_kw]
    else:
        vt_pieces = [whats_left, jean_style_adj, jean_style_label,
                     product_line_label, pullon_label, rise_label_kw,
                     styling_label, fabric_label, inseam_style_label,
                     type2_label, type_label, inseam_label_kw]
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
            product_line = _join_pieces(
                [whats_left.split()[0],
                 jean_style_adj if adj_up in {"KICK", "TRUE"} else "",
                 jean_style_label])
        elif "GOOD" in wl_up:
            product_line = _join_pieces(
                [whats_left, jean_style_adj if adj_up == "KICK" else "",
                 jean_style_label])
        elif "VINTAGE" in vt_up:
            product_line = "VINTAGE"
        elif "POWER STRETCH" in vt_up and "PULL ON" in vt_up:
            product_line = "POWER STRETCH PULL ON"
        elif "PULL ON" in vt_up:
            product_line = "PULL ON"
        elif product_line_label:
            product_line = product_line_label

    return {
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
    if (has("cigarette", "good icon", "slim straight", "soft stretch point")
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
                              inseam: str) -> str:
    label = option_attribute_label(option2, option3)
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
            r"Inseam\s+(Regular|Long|Short|Petite|Tall)\s*:\s*(\d+(?:\.\d+)?)",
            desc, re.IGNORECASE):
        pairs[m.group(1).lower()] = m.group(2)
    # "Inseam: Short 27" | Regular 29"" / "Inseam: Regular 29" | Long 35""
    for m in re.finditer(
            r"(Regular|Long|Short|Petite|Tall)\s*(\d+(?:\.\d+)?)",
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
        m = re.search(r"Inseam\s*:\s*(\d+(?:\.\d+)?)", desc, re.IGNORECASE)
        if m:
            found = m.group(1)
    if not found:
        m = re.search(r"(\d+(?:\.\d+)?)\s*[\"”]?\s*inseam", desc, re.IGNORECASE)
        if m:
            found = m.group(1)

    # sku_brand_no_size: two dashes ending in a 2-digit number 20-40
    if sku_no_size and sku_no_size.count("-") == 2:
        m = re.search(r"-(\d{2})$", sku_no_size)
        if m and 20 <= int(m.group(1)) <= 40:
            sku_val = m.group(1)
            if not found or float(sku_val) != float(found):
                found = sku_val

    if not found:
        return ""
    try:
        return _round_inseam(float(found))
    except ValueError:
        return ""


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
    for candidate in (size_clean, size_clean.replace(" ", "-"),
                      size_clean[:2]):
        if not candidate:
            continue
        pattern = re.compile(r"-" + re.escape(candidate) + r"(?=$)",
                             re.IGNORECASE)
        m = None
        for m in pattern.finditer(sku):
            pass
        if m:
            return sku[:m.start()] + sku[m.end():]
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
    out = re.sub(r"[^\w\s|']", " ", out)
    return re.sub(r"[ \t]+", " ", out).strip()


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


def build_product_field(product_title: str, variant_title: str) -> str:
    product = normalize_output_text(product_title or "")
    vt = _kn(variant_title)
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


def build_rows(
    products: List[Dict[str, object]],
    searchspring_map: Dict[str, Dict[str, str]],
    max_products: Optional[int],
    max_variants: Optional[int],
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
        description = normalize_output_text(product.get("description") or "")
        tags = product.get("tags") or []
        tags = [normalize_output_text(tag) for tag in tags] if isinstance(tags, list) else []
        tags_str = ", ".join(tags)
        online_store_url = product.get("onlineStoreUrl") or ""
        pdp_active = bool(online_store_url)
        if not online_store_url:
            online_store_url = f"https://www.goodamerican.com/products/{handle}"

        # Jean Style step 1 (title keywords) seeds the naming engine.
        jean_style = jean_style_from_title(title)
        naming = compute_naming(title, jean_style.split()[0] if jean_style else "")

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
            inseam_label = determine_inseam_label_v2(option2, option3, title, inseam_value)
            attr_label = option_attribute_label(option2, option3)

            color_value = normalize_output_text(option1)
            title_parts = [clean_text(part) for part in normalize_output_text(title).split("|")]
            color_code = title_parts[1] if len(title_parts) > 1 and title_parts[1] else color_value

            # The length segment comes from the option attributes when the
            # variant carries them, otherwise from the title's own keyword.
            length_segment = (attr_label or naming["inseam_label_kw"]).upper()
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

            staged_rows.append({
                "Style Id": style_id,
                "Handle": handle,
                "Published At": parse_date(product.get("publishedAt")),
                "Created At": parse_date(product.get("createdAt")),
                "Product": build_product_field(title, variant_title),
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
                "Color - Simplified": determine_color_simplified(
                    tags, description, determine_color_standardized(tags, description)),
                "Color - Standardized": determine_color_standardized(tags, description),
                "Stretch": determine_stretch(description),
                "_style_name_draft": naming["style_name_draft"],
                "_vt_pre": naming["variant_title_pre"],
                "_inseam_label_kw": naming["inseam_label_kw"],
                "_color_code": color_code,
                "_attr_label": length_segment_store,
                "_raw_title": title,
                "_sku_no_size": sku_no_size,
                "_pdp_active": "1" if pdp_active else "",
            })

        seen_products += 1

    apply_good_insert_normalization(staged_rows)
    apply_jean_style_draft_fill(staged_rows)
    fill_jean_style_from_text(staged_rows, stage="title_desc")
    apply_jean_style_draft_fill(staged_rows)
    fill_jean_style_from_text(staged_rows, stage="desc")
    apply_jean_style_draft_fill(staged_rows)
    refresh_inseam_style(staged_rows)
    apply_quantity_of_style(staged_rows)
    apply_duplicate_old_marker(staged_rows)

    for row in staged_rows:
        row.pop("_vt_pre", None)
        row.pop("_inseam_label_kw", None)
        row.pop("_color_code", None)
        row.pop("_attr_label", None)
        row.pop("_raw_title", None)
        row.pop("_style_name_draft", None)
        row.pop("_sku_no_size", None)
        row.pop("_pdp_active", None)
    return staged_rows


GOOD_INSERT_WORDS = ["LEGS", "CLASSIC", "WAIST", "CURVE", "BOY"]


def _normalize_color_key(color: str) -> str:
    """BLACK001 and BLACK are the same colour for matching purposes."""
    return re.sub(r"\d+$", "", (color or "").strip().upper())


def apply_good_insert_normalization(rows: List[Dict[str, str]]) -> None:
    """Re-attach the collection word that a length-specific title drops.

    A petite/long title such as "ALWAYS FITS GOOD PETITE BOOTCUT JEANS" names
    the same style as its regular sibling "ALWAYS FITS GOOD CLASSIC BOOTCUT
    JEANS". The length word belongs in the option-attribute segment, so it is
    stripped here and LEGS/CLASSIC/WAIST/CURVE/BOY is tried after "GOOD";
    the insert is kept only when it matches a style name that already exists
    for the same colour.
    """
    names_by_color: Dict[str, Set[str]] = {}
    for row in rows:
        key = _normalize_color_key(row.get("_color_code", ""))
        if row["Style Name"]:
            names_by_color.setdefault(key, set()).add(row["Style Name"].upper())

    for row in rows:
        kw = row.get("_inseam_label_kw", "")
        if not kw:
            continue
        stripped_sn = clean_text(re.sub(rf"\b{re.escape(kw)}\b", " ",
                                        row["Style Name"], flags=re.IGNORECASE))
        stripped_vt = clean_text(re.sub(rf"\b{re.escape(kw)}\b", " ",
                                        row.get("_vt_pre", ""), flags=re.IGNORECASE))
        if not stripped_sn:
            continue
        color_key = _normalize_color_key(row.get("_color_code", ""))
        siblings = names_by_color.get(color_key, set())

        chosen_sn, chosen_vt = "", ""
        for word in GOOD_INSERT_WORDS:
            cand_sn = re.sub(r"\bGOOD\b", f"GOOD {word}", stripped_sn, count=1,
                             flags=re.IGNORECASE)
            if cand_sn.upper() in siblings and cand_sn.upper() != row["Style Name"].upper():
                chosen_sn = cand_sn
                chosen_vt = re.sub(r"\bGOOD\b", f"GOOD {word}", stripped_vt,
                                   count=1, flags=re.IGNORECASE)
                break
        if not chosen_sn:
            # No sibling to match: still move the length word out of the name.
            chosen_sn, chosen_vt = stripped_sn, stripped_vt
        row["Style Name"] = chosen_sn
        row["_vt_pre"] = chosen_vt

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
        row["Product"] = build_product_field(row.get("_raw_title", ""),
                                             row["Variant Title"])


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


def apply_duplicate_old_marker(rows: List[Dict[str, str]]) -> None:
    """Mark the superseded style when two Style Ids share a Product title.

    The older style gets "OLD" appended after LONG/PETITE/REGULAR but before
    the " | " separator. A style counts as superseded when none of its
    variants are for sale and its PDP is gone; otherwise the one with the
    oldest Created At date is marked.
    """
    by_product: Dict[str, Dict[str, List[Dict[str, str]]]] = {}
    for row in rows:
        by_product.setdefault(row["Product"], {}).setdefault(
            row["Style Id"], []).append(row)

    for _product, styles in by_product.items():
        if len(styles) < 2:
            continue
        inactive = []
        for style_id, style_rows in styles.items():
            for_sale = any(r["Available for Sale"].lower() == "true" for r in style_rows)
            pdp = any(r.get("_pdp_active") for r in style_rows)
            if not for_sale and not pdp:
                inactive.append(style_id)
        if inactive:
            targets = inactive
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
    )
    logging.info("Rows prepared: %s", len(rows))
    write_csv(rows)


if __name__ == "__main__":
    main()
