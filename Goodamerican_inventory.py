"""Brand-agnostic probe that inspects Shopify collection feeds and Storefront APIs."""

from __future__ import annotations

import argparse
import html
import json
import logging
import re
import time
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from requests.adapters import HTTPAdapter, Retry

# ---------------------------------------------------------------------------
# Brand-specific configuration
# ---------------------------------------------------------------------------
BRAND = "GOODAMERICAN"
COLLECTION_URL = [
    "https://www.goodamerican.com/collections/womens-jeans","https://www.goodamerican.com/collections/sale",
]
MYSHOPIFY = "good-american.myshopify.com"
GRAPHQL = "https://good-american.myshopify.com/api/unstable/graphql.json"
X_SHOPIFY_STOREFRONT_ACCESS_TOKEN = ["45b215a31a1aa259d1e128badf92328a"]
GRAPHQL_FILTER_TAG = ""
STOREFRONT_COLLECTION_HANDLES: List[str] = ["womens-jeans","sale"]
SEARCHSPRING_SITE_ID = "5ojqb3"
SEARCHSPRING_URL = "https://5ojqb3.a.searchspring.io/api/search/autocomplete.json"
SEARCHSPRING_EXTRA_PARAMS: Dict[str, Any] = {}
METAFIELD_IDENTIFIERS: List[Tuple[str, str]] = [("metafields", "composition"),("custom", "range"),("custom", "color"),("custom", "collection"),("custom", "wash"),("custom", "rise"),("custom", "mill")]
COLLECTION_TITLE_MAP: Dict[str, str] = {}
VIEW_JSON_ENRICHMENT_ENABLED = False
VIEW_JSON_FIELDS = [
    "metafields.0.product_measurements",
    "metafields.0.origin",
    "metafields.0.fabric",
    "metafields.0.color",
    "metafields.0.color_file",
    "metafields.0.details",
]
VIEW_JSON_PROBE_LIMIT = 6

# ---------------------------------------------------------------------------
# Derived paths and constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND_SLUG = BRAND.lower().replace(" ", "_") or "brand"
LOG_PATH = BASE_DIR / f"{BRAND_SLUG}_probe_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / f"{BRAND_SLUG}_probe_run.log"

REQUEST_TIMEOUT = 30
TRANSIENT_STATUS = {429, 500, 502, 503, 504}
GRAPHQL_PAGE_SIZE = 100
MAX_SCRIPT_FETCHES = 25
TOKEN_REGEX = re.compile(r"\b[0-9a-f]{32}\b", re.IGNORECASE)

DEFAULT_GRAPHQL_VERSIONS = [

    "api/unstable/graphql.json",
]

COLUMN_ORDER_BASE: Tuple[str, ...] = (
    "product.id",
    "product.handle",
    "product.published_at",
    "product.created_at",
    "product.title",
    "product.productType",
    "product.tags_all",
    "product.vendor",
    "product.description",
    "product.descriptionHtml",
    "variant.title",
    "variant.option1",
    "variant.option2",
    "variant.option3",
    "variant.price",
    "variant.compare_at_price",
    "variant.available",
    "variant.quantityAvailable",
    "product.totalInventory",
    "variant.id",
    "variant.sku",
    "variant.barcode",
    "product.images[0].src",
    "product.onlineStoreUrl",
)

DEFAULT_FORBIDDEN_FIELDS: Dict[str, Set[str]] = {
    "ProductVariant": {
        "components",
        "groupedBy",
        "quantityPriceBreaks",
        "sellingPlanAllocations",
        "sellingPlanGroups",
    }
}

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


def parse_metafield_identifiers(raw: str) -> List[Tuple[str, str]]:
    identifiers: List[Tuple[str, str]] = []
    if not raw:
        return identifiers
    parts = [part.strip() for part in raw.split(",") if part.strip()]
    seen: Set[Tuple[str, str]] = set()
    for part in parts:
        if ":" not in part:
            continue
        namespace, key = part.split(":", 1)
        namespace = namespace.strip()
        key = key.strip()
        if not namespace or not key:
            continue
        tup = (namespace, key)
        if tup not in seen:
            identifiers.append(tup)
            seen.add(tup)
    return identifiers


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_view_json_fields(raw: str) -> List[str]:
    if not raw:
        return []
    fields: List[str] = []
    seen: Set[str] = set()
    for part in raw.split(","):
        key = part.strip()
        if not key or key in seen:
            continue
        fields.append(key)
        seen.add(key)
    return fields


def normalize_tokens(value: Any) -> List[str]:
    """Return an ordered list of unique, non-empty tokens."""

    if not value:
        return []

    tokens: List[str] = []
    if isinstance(value, str):
        token = value.strip()
        if token:
            tokens.append(token)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            if not isinstance(item, str):
                continue
            token = item.strip()
            if token:
                tokens.append(token)

    seen: Set[str] = set()
    ordered: List[str] = []
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        ordered.append(token)
    return ordered


def format_error_note(errors: Optional[List[Dict[str, Any]]]) -> str:
    """Summarize GraphQL errors for logging and the Storefront_access sheet.

    This keeps the count while appending the first error's path/message so
    entries like "errors:1" have immediate context when a probe returns HTTP 200
    but Shopify still reports GraphQL errors.
    """

    if not errors:
        return "errors:0"

    first = errors[0] or {}
    path = first.get("path") or []
    message = first.get("message") or first.get("error") or ""
    path_str = ".".join(str(p) for p in path if p is not None)

    details: List[str] = []
    if path_str:
        details.append(f"path={path_str}")
    if message:
        details.append(f"msg={message}")

    suffix = f":{' | '.join(details)}" if details else ""
    return f"errors:{len(errors)}{suffix}"

FALLBACK_COLLECTION_QUERY = """
query CollectionFallback($handle: String!, $cursor: String, $pageSize: Int!) {
  collection(handle: $handle) {
    id
    handle
    title
    products(first: $pageSize, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        cursor
        node {
          id
          handle
          title
          productType
          tags
          vendor
          onlineStoreUrl
          createdAt
          updatedAt
          publishedAt
          variants(first: 100) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              cursor
              node {
                id
                title
                sku
                availableForSale
                price {
                  amount
                  currencyCode
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

def build_metafields_selection() -> str:
    if not METAFIELD_IDENTIFIERS:
        return ""
    identifiers_literal = ", ".join(
        f'{{namespace: "{ns}", key: "{key}"}}' for ns, key in METAFIELD_IDENTIFIERS
    )
    return (
        "metafields(identifiers: ["
        + identifiers_literal
        + "]) {\n  namespace\n  key\n  type\n  value\n}"
    )


def build_fallback_products_query() -> str:
    metafields_selection = build_metafields_selection()
    metafields_block = f"\n        {metafields_selection}" if metafields_selection else ""
    return f"""
query ProductsFallback($cursor: String, $pageSize: Int!, $query: String) {{
  products(first: $pageSize, after: $cursor, query: $query) {{
    pageInfo {{
      hasNextPage
      endCursor
    }}
    edges {{
      cursor
      node {{
        id
        handle
        title
        description
        productType
        tags
        vendor
        onlineStoreUrl
        createdAt
        updatedAt
        publishedAt
        collections(first: 50) {{
          edges {{
            node {{
              id
              handle
              title
            }}
          }}
        }}
        options {{
          name
          values
        }}{metafields_block}
        variants(first: 100) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          edges {{
            cursor
            node {{
              id
              title
              sku
              availableForSale
              price {{
                amount
                currencyCode
              }}
            }}
          }}
        }}
      }}
    }}
  }}
}}
"""

SHOP_PROBE_QUERY = "query { shop { name primaryDomain { url } } }"

INTROSPECTION_QUERY = """
query ($typeName: String!) {
  __type(name: $typeName) {
    name
    fields {
      name
      args {
        name
        defaultValue
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
              }
            }
          }
        }
      }
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
    }
  }
}
"""

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

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _primary_collection_url() -> Optional[str]:
    if isinstance(COLLECTION_URL, (list, tuple)):
        return COLLECTION_URL[0] if COLLECTION_URL else None
    return COLLECTION_URL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def configure_logging() -> logging.Logger:
    logger = logging.getLogger(f"retail_probe_{BRAND_SLUG}")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    handler: logging.Handler
    try:
        handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
    except OSError as exc:
        fallback = FALLBACK_LOG_PATH
        handler = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        logger.warning(
            "Primary log path %s unavailable (%s); using %s", LOG_PATH, exc, fallback
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=TRANSIENT_STATUS,
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    session.verify = False
    return session


def normalize_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def flatten_value(value: Any, prefix: str) -> Dict[str, Any]:
    items: Dict[str, Any] = {}
    if isinstance(value, dict):
        for key, inner in value.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            items.update(flatten_value(inner, new_prefix))
    elif isinstance(value, list):
        for index, inner in enumerate(value):
            new_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            items.update(flatten_value(inner, new_prefix))
    else:
        items[prefix] = value
    return items


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in record.items():
        flat.update(flatten_value(value, key))
    return flat


def extract_graphql_variant_entries(
    variants_connection: Any,
) -> List[Dict[str, Any]]:
    if not isinstance(variants_connection, dict):
        return []

    entries: List[Dict[str, Any]] = []
    seen_ids: Set[Any] = set()

    edges = variants_connection.get("edges") or []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        node = edge.get("node")
        if not isinstance(node, dict):
            continue
        vid = node.get("id")
        if vid is not None:
            if vid in seen_ids:
                continue
            seen_ids.add(vid)
        entries.append({"cursor": edge.get("cursor", ""), "node": node})

    nodes = variants_connection.get("nodes") or []
    if isinstance(nodes, list):
        for node in nodes:
            if not isinstance(node, dict):
                continue
            vid = node.get("id")
            if vid is not None and vid in seen_ids:
                continue
            if vid is not None:
                seen_ids.add(vid)
            entries.append({"cursor": "", "node": node})

    return entries


def build_option_columns(options: Sequence[Dict[str, Any]]) -> Dict[str, str]:
    columns: Dict[str, str] = {}
    aggregate_values: List[str] = []
    for option in options or []:
        if not isinstance(option, dict):
            continue
        name = str(option.get("name") or "").strip()
        values = [str(v).strip() for v in option.get("values") or [] if str(v).strip()]
        if not values:
            continue
        joined = ", ".join(values)
        if name:
            columns[f"product.options.{name}"] = joined
        else:
            aggregate_values.append(joined)
    if aggregate_values and "product.options" not in columns:
        columns["product.options"] = ", ".join(aggregate_values)
    return columns


def sanitize_dynamic_header(value: str) -> str:
    cleaned = re.sub(r"[^0-9A-Za-z]+", "_", str(value).strip()).strip("_")
    return cleaned or "value"


def apply_name_value_columns(row: Dict[str, Any]) -> None:
    replacements: Dict[str, Any] = {}
    to_remove: List[str] = []
    for key, value in list(row.items()):
        if not key.endswith(".name"):
            continue
        prefix = key[:-5]
        name_value = str(value).strip()
        value_key = f"{prefix}.value"
        if not name_value or value_key not in row:
            continue
        new_key = f"{prefix}.{sanitize_dynamic_header(name_value)}"
        replacements[new_key] = row[value_key]
        to_remove.extend([key, value_key])
    for key in to_remove:
        row.pop(key, None)
    row.update(replacements)


def extract_first_image_src(product: Dict[str, Any]) -> Optional[str]:
    images = product.get("images")
    if isinstance(images, list):
        for item in images:
            if isinstance(item, dict):
                src = item.get("src") or item.get("url") or item.get("originalSrc")
                if src:
                    return src
    elif isinstance(images, dict):
        edges = images.get("edges") or []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if isinstance(node, dict):
                src = node.get("src") or node.get("url") or node.get("originalSrc")
                if src:
                    return src
    return None


def normalize_money_field(row: Dict[str, Any], base_key: str) -> None:
    amount_key = f"{base_key}.amount"
    if base_key not in row and amount_key in row:
        row[base_key] = row.pop(amount_key)
    elif amount_key in row and row.get(base_key) == row.get(amount_key):
        row.pop(amount_key, None)


def remove_matching_keys(
    row: Dict[str, Any], prefixes: Sequence[str], *, allowed: Optional[Sequence[str]] = None
) -> None:
    allowed_set = set(allowed or [])
    for key in list(row.keys()):
        lowered = key.lower()
        if "position" in lowered:
            row.pop(key, None)
            continue
        for prefix in prefixes:
            if key.startswith(prefix) and key not in allowed_set:
                row.pop(key, None)
                break


def extract_field_from_error_path(path: Sequence[Any]) -> Optional[str]:
    for segment in reversed(path or []):
        if isinstance(segment, str):
            return segment
    return None


def infer_error_target_type(path: Sequence[Any]) -> str:
    string_segments = [segment for segment in path if isinstance(segment, str)]
    return "ProductVariant" if "variants" in string_segments else "Product"


def populate_variant_options(row: Dict[str, Any], variant: Optional[Dict[str, Any]]) -> None:
    if variant is None:
        return
    selected = variant.get("selectedOptions") or []
    for index, option in enumerate(selected):
        if index >= 3 or not isinstance(option, dict):
            continue
        value = option.get("value")
        if value and not row.get(f"variant.option{index + 1}"):
            row[f"variant.option{index + 1}"] = value


def finalize_common_row(
    row: Dict[str, Any],
    product: Dict[str, Any],
    variant: Optional[Dict[str, Any]],
    *,
    source: str,
) -> None:
    tags = product.get("tags") or []
    if isinstance(tags, list) and tags:
        row["product.tags_all"] = ", ".join(str(tag) for tag in tags if str(tag))
    for key in list(row.keys()):
        if key.startswith("product.tags["):
            row.pop(key, None)

    option_columns = build_option_columns(product.get("options") or [])
    for key, value in option_columns.items():
        row[key] = value

    image_src = extract_first_image_src(product)
    if image_src:
        row["product.images[0].src"] = image_src

    remove_matching_keys(
        row,
        [
            "product.images[",
            "product.images.edges",
            "product.media.edges",
            "product.collections.edges",
            "product.options[",
            "variant.selectedOptions[",
            "variant.featured_image",
        ],
        allowed=["product.images[0].src", "variant.featured_image.src"],
    )

    normalize_money_field(row, "variant.price")
    normalize_money_field(row, "variant.compare_at_price")

    remap_candidates: Dict[str, Sequence[str]] = {
        "product.totalInventory": (
            "product.ss_available_qty",
            "product.total_inventory",
            "product.ss_inventory_count",
        ),
        "product.onlineStoreUrl": (
            "product.ss_url",
            "product.url",
        ),
        "product.id": ("product.ss_id",),
        "product.title": ("product.name",),
        "product.vendor": ("product.brand",),
        "variant.id": ("variant.variant_id",),
    }
    for target, candidates in remap_candidates.items():
        if row.get(target) not in (None, ""):
            continue
        for candidate in candidates:
            if row.get(candidate) in (None, ""):
                continue
            row[target] = row[candidate]
            break

    if "variant.available" not in row:
        for candidate in (
            "variant.availableForSale",
            "variant.available_for_sale",
            "variant.available_for_sale?",
        ):
            if candidate in row:
                row["variant.available"] = row.pop(candidate)
                break

    if "variant.quantityAvailable" not in row:
        for candidate in (
            "variant.quantity_available",
            "variant.inventory_quantity",
        ):
            if candidate in row:
                row["variant.quantityAvailable"] = row.pop(candidate)
                break

    populate_variant_options(row, variant)

    apply_name_value_columns(row)

    if source == "storefront":
        if "product.publishedAt" in row and "product.published_at" not in row:
            row["product.published_at"] = row.pop("product.publishedAt")
        if "product.createdAt" in row and "product.created_at" not in row:
            row["product.created_at"] = row.pop("product.createdAt")
        if variant and "availableForSale" in variant and "variant.available" not in row:
            row["variant.available"] = variant.get("availableForSale")
    else:
        if "product.published_at" not in row and "product_published_at" in row:
            row["product.published_at"] = row.get("product_published_at")
        if "product_published_at" in row:
            row.pop("product_published_at", None)
        if "product.productType" not in row and "product.product_type" in row:
            row["product.productType"] = row.pop("product.product_type")
        if "product.body_html" in row:
            row.setdefault("product.descriptionHtml", row["product.body_html"])
            row.setdefault("product.description", row["product.body_html"])
            row.pop("product.body_html", None)

    if "variant.compare_at_price" not in row and variant is not None:
        compare_candidates = (
            variant.get("compareAtPrice"),
            variant.get("compare_at_price"),
        )
        for candidate in compare_candidates:
            if isinstance(candidate, dict):
                amount = candidate.get("amount")
                if amount is not None:
                    row["variant.compare_at_price"] = amount
                    break
            elif candidate not in (None, ""):
                row["variant.compare_at_price"] = candidate
                break

    if "variant.price" not in row and variant is not None:
        price_candidates = (
            variant.get("price"),
            variant.get("priceV2"),
        )
        for candidate in price_candidates:
            if isinstance(candidate, dict):
                amount = candidate.get("amount")
                if amount is not None:
                    row["variant.price"] = amount
                    break
            elif candidate not in (None, ""):
                row["variant.price"] = candidate
                break

    if variant is not None:
        for idx in range(1, 4):
            option_key = f"option{idx}"
            alt_key = f"variant.{option_key}"
            if alt_key not in row and option_key in variant:
                row[alt_key] = variant.get(option_key)

    for forbidden in list(EXTRA_FORBIDDEN_COLUMNS):
        row.pop(forbidden, None)


def finalize_json_row(row: Dict[str, Any], product: Dict[str, Any], variant: Optional[Dict[str, Any]]) -> None:
    finalize_common_row(row, product, variant, source="json")


def finalize_storefront_row(
    row: Dict[str, Any], product: Dict[str, Any], variant: Optional[Dict[str, Any]]
) -> None:
    finalize_common_row(row, product, variant, source="storefront")


def extract_collections(product: Dict[str, Any], collection_info: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    handles: List[str] = []
    titles: List[str] = []
    collections = product.get("collections")
    if isinstance(collections, dict):
        edges = collections.get("edges") or []
        nodes = collections.get("nodes") or []
        if nodes:
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                handle = node.get("handle")
                title = node.get("title")
                if handle:
                    handles.append(str(handle))
                if title:
                    titles.append(str(title))
        elif edges:
            for edge in edges:
                node = edge.get("node") if isinstance(edge, dict) else None
                if not isinstance(node, dict):
                    continue
                handle = node.get("handle")
                title = node.get("title")
                if handle:
                    handles.append(str(handle))
                if title:
                    titles.append(str(title))

    fallback_handle = collection_info.get("collection_handle")
    fallback_title = collection_info.get("collection_title")
    if fallback_handle and fallback_handle not in handles:
        handles.append(fallback_handle)
    if fallback_title and fallback_title not in titles:
        titles.append(fallback_title)
    if COLLECTION_TITLE_MAP and handles:
        for h in handles:
            if h in COLLECTION_TITLE_MAP and COLLECTION_TITLE_MAP[h] not in titles:
                titles.append(COLLECTION_TITLE_MAP[h])
    return handles, titles


def collect_metafields(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    metafields: List[Dict[str, Any]] = []
    raw_metafields = product.get("metafields")
    if isinstance(raw_metafields, list):
        for mf in raw_metafields:
            if isinstance(mf, dict):
                metafields.append(
                    {
                        "namespace": mf.get("namespace"),
                        "key": mf.get("key"),
                        "type": mf.get("type"),
                        "value": mf.get("value"),
                    }
                )
    elif isinstance(raw_metafields, dict):
        # Support alias-based selections like mf_0: metafield(...)
        for value in raw_metafields.values():
            if isinstance(value, dict):
                metafields.append(
                    {
                        "namespace": value.get("namespace"),
                        "key": value.get("key"),
                        "type": value.get("type"),
                        "value": value.get("value"),
                    }
                )
    return metafields


def derive_filter_values(product: Dict[str, Any], metafields: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    filters: Dict[str, Set[str]] = defaultdict(set)
    product_type = product.get("productType")
    vendor = product.get("vendor")
    tags = product.get("tags") or []
    options = product.get("options") or []

    if product_type:
        filters["productType"].add(str(product_type))
    if vendor:
        filters["vendor"].add(str(vendor))
    if isinstance(tags, list):
        for tag in tags:
            if tag:
                filters["tags"].add(str(tag))

    if isinstance(options, list):
        for opt in options:
            if not isinstance(opt, dict):
                continue
            name = opt.get("name") or opt.get("title")
            values = opt.get("values") or []
            if not name:
                continue
            for val in values:
                if val:
                    filters[str(name)].add(str(val))

    for mf in metafields:
        ns = mf.get("namespace")
        key = mf.get("key")
        value = mf.get("value")
        if ns and key and value not in (None, ""):
            filters[f"{ns}:{key}"].add(str(value))

    return {k: sorted(v) for k, v in filters.items() if v}


def normalize_filter_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_")
    return cleaned or "unnamed"


FILTER_COLUMN_SKIP = {"producttype", "vendor", "tags"}


def build_filter_corpus(product: Dict[str, Any]) -> Tuple[str, Set[str]]:
    parts: List[str] = []
    for field in ("handle", "title", "productType", "vendor"):
        val = product.get(field)
        if isinstance(val, str):
            parts.append(val)
    tags = product.get("tags") or []
    if isinstance(tags, list):
        parts.extend([t for t in tags if isinstance(t, str)])
    options = product.get("options") or []
    if isinstance(options, list):
        for opt in options:
            if not isinstance(opt, dict):
                continue
            values = opt.get("values") or []
            for val in values:
                if val:
                    parts.append(str(val))
    combined = " ".join(parts).lower()
    normalized_text = re.sub(r"[^a-z0-9]+", " ", combined)
    tokens = {tok for tok in normalized_text.split() if tok}
    return normalized_text, tokens


def select_filters_for_product(
    collection_filters: Dict[str, List[str]],
    product: Dict[str, Any],
    derived_filters: Dict[str, List[str]],
) -> Dict[str, List[str]]:
    final_filters: Dict[str, List[str]] = {}
    normalized_text, tokens = build_filter_corpus(product)

    for key, values in derived_filters.items():
        if not values:
            continue
        final_filters[key] = list(values)

    for key, candidates in (collection_filters or {}).items():
        if key in final_filters:
            continue
        matches: List[str] = []
        for candidate in candidates or []:
            cand_str = str(candidate)
            cand_norm = re.sub(r"[^a-z0-9]+", " ", cand_str.lower()).strip()
            if not cand_norm:
                continue
            cand_tokens = {tok for tok in cand_norm.split() if tok}
            if cand_norm in normalized_text or cand_tokens.issubset(tokens):
                matches.append(cand_str)
        if not matches and candidates and len(candidates) == 1:
            matches = [str(candidates[0])]
        if matches:
            final_filters[key] = matches

    return final_filters


def apply_filter_columns(row: Dict[str, Any], filter_values: Dict[str, List[str]]) -> None:
    for raw_key, values in (filter_values or {}).items():
        norm_key = normalize_filter_name(raw_key)
        if norm_key in FILTER_COLUMN_SKIP:
            continue
        if not values:
            continue
        column = f"filter.{norm_key}"
        row[column] = ", ".join(values)


def build_column_order(
    rows: List[Dict[str, Any]],
    *,
    extra_priority: Optional[Sequence[str]] = None,
) -> List[str]:
    all_columns = {key for row in rows for key in row.keys()}
    ordered = list(COLUMN_ORDER_BASE)
    priority: List[str] = []
    if extra_priority:
        for column in extra_priority:
            if column not in ordered and column in all_columns:
                priority.append(column)
    extras = [col for col in all_columns if col not in COLUMN_ORDER_BASE and col not in priority]
    extras.sort()
    return ordered + priority + extras


def unwrap_type(type_info: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str], Tuple[str, ...]]:
    wrappers: List[str] = []
    current = type_info
    while current and current.get("kind") in {"NON_NULL", "LIST"}:
        wrappers.append(current["kind"])
        current = current.get("ofType")
    kind = current.get("kind") if current else None
    name = current.get("name") if current else None
    return kind, name, tuple(wrappers)


def field_has_required_args(field: Dict[str, Any]) -> bool:
    for arg in field.get("args", []):
        kind, _name, wrappers = unwrap_type(arg.get("type"))
        if "NON_NULL" in wrappers and arg.get("defaultValue") in (None, "null"):
            return True
        if kind == "NON_NULL" and arg.get("defaultValue") in (None, "null"):
            return True
    return False


def write_sheet(
    sheet,
    rows: List[Dict[str, Any]],
    *,
    column_order: Optional[Sequence[str]] = None,
):
    if not rows:
        sheet.append(["No data"])
        return
    if column_order is None:
        columns = sorted({key for row in rows for key in row.keys()})
    else:
        columns = list(column_order)
    sheet.append(columns)
    for row in rows:
        sheet.append([normalize_cell(row.get(column)) for column in columns])


def group_tag_columns(sheet, columns: Sequence[str]) -> None:
    tag_indexes = [index for index, name in enumerate(columns, start=1) if name.startswith("tags_group_")]
    if not tag_indexes:
        return

    start = tag_indexes[0]
    end = start
    for index in tag_indexes[1:]:
        if index == end + 1:
            end = index
            continue
        sheet.column_dimensions.group(
            get_column_letter(start), get_column_letter(end), outline_level=1, hidden=False
        )
        start = index
        end = index
    sheet.column_dimensions.group(
        get_column_letter(start), get_column_letter(end), outline_level=1, hidden=False
    )


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
        target = f"{base}/collections.json"
        params = {"page": page, "limit": 250}
        try:
            resp = session.get(target, params=params, timeout=REQUEST_TIMEOUT, verify=False)
        except requests.RequestException as exc:
            logger.debug("Failed to fetch collections.json: %s", exc)
            break
        if not resp.ok:
            break
        try:
            payload = resp.json()
        except ValueError:
            break
        collections = payload.get("collections") if isinstance(payload, dict) else None
        if not collections:
            break
        for coll in collections:
            if not isinstance(coll, dict):
                continue
            handle = coll.get("handle")
            title = coll.get("title")
            if handle and title:
                titles[str(handle)] = str(title)
        if len(collections) < 250:
            break
        page += 1
        time.sleep(0.25)
    if titles:
        logger.info("Discovered %s collections from collections.json", len(titles))
    return titles


def derive_tag_group_key(tag: str) -> str:
    normalized = str(tag or "").strip().lower()
    if not normalized:
        return "misc"
    prefix = normalized
    for separator in ("-", "_", " "):
        if separator in normalized:
            prefix = normalized.split(separator, 1)[0]
            break
    prefix = re.sub(r"[^a-z0-9]+", "_", prefix).strip("_")
    return prefix or "misc"


def group_tags_for_columns(tags: Sequence[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, List[str]] = {}
    seen: Dict[str, set] = defaultdict(set)
    for raw_tag in tags:
        if not isinstance(raw_tag, str):
            continue
        tag = raw_tag.strip()
        if not tag:
            continue
        group_key = derive_tag_group_key(tag)
        column_name = f"tags_group_{group_key}"
        bucket = grouped.setdefault(column_name, [])
        if tag not in seen[column_name]:
            bucket.append(tag)
            seen[column_name].add(tag)
    return grouped


def collect_tag_values(record: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    for key, value in record.items():
        if "tag" not in key.lower():
            continue
        if isinstance(value, list):
            str_items = [str(item).strip() for item in value if isinstance(item, str) and str(item).strip()]
            tags.extend(str_items)
        elif isinstance(value, str):
            pieces = [part.strip() for part in value.split(",")]
            tags.extend([piece for piece in pieces if piece])
    return tags


def fetch_collection_json(
    session: requests.Session, logger: logging.Logger
) -> Tuple[List[Dict[str, Any]], List[str]]:
    products_json_urls = build_products_json_urls()
    if not products_json_urls:
        logger.info("No collection JSON URL computed; skipping JSON extraction")
        return [], []

    all_products: List[Dict[str, Any]] = []
    for products_json_url in products_json_urls:
        page = 1
        while True:
            params = {"limit": 250, "page": page}
            logger.info("Fetching collection JSON page %s from %s", page, products_json_url)
            try:
                response = session.get(
                    products_json_url, params=params, timeout=REQUEST_TIMEOUT, verify=False
                )
            except requests.RequestException as exc:
                logger.warning("Collection JSON request failed: %s", exc)
                break

            if not response.ok:
                logger.warning(
                    "Collection JSON request returned status %s", response.status_code
                )
                break

            try:
                data = response.json()
            except ValueError:
                logger.warning("Collection JSON response was not valid JSON")
                break

            products = data.get("products") or []
            if not products:
                logger.info("No products found on page %s; stopping pagination", page)
                break

            all_products.extend(products)
            if len(products) < 250:
                break
            page += 1
            time.sleep(0.5)

    logger.info("Collected %s products from collection JSON", len(all_products))
    rows: List[Dict[str, Any]] = []
    tag_group_counts: Counter[str] = Counter()
    for product in all_products:
        if not isinstance(product, dict):
            continue
        product_copy = dict(product)
        tags = list(product_copy.pop("tags", []) or [])
        tag_set = {tag for tag in tags if isinstance(tag, str)}
        for extra_tag in collect_tag_values(product):
            if extra_tag and extra_tag not in tag_set:
                tags.append(extra_tag)
                tag_set.add(extra_tag)
        variants = list(product_copy.get("variants", []) or [])
        product_copy.pop("variants", None)

        options = list(product.get("options") or [])
        option_columns = build_option_columns(options)

        derived_filters = derive_filter_values(product, [])
        filter_values = select_filters_for_product({}, product, derived_filters)

        images = product_copy.get("images") or []
        first_image_src = None
        if isinstance(images, list) and images:
            first_image = images[0]
            if isinstance(first_image, dict):
                first_image_src = (
                    first_image.get("src")
                    or first_image.get("url")
                    or first_image.get("originalSrc")
                )
        if first_image_src:
            product_copy["images"] = [{"src": first_image_src}]
        elif "images" in product_copy:
            product_copy["images"] = []

        flat_product = flatten_record({"product": product_copy})
        base_row = dict(flat_product)
        if tags:
            base_row["product.tags_all"] = ", ".join(tags)
        apply_filter_columns(base_row, filter_values)
        for key, value in option_columns.items():
            base_row[key] = value

        tag_groups = group_tags_for_columns(tags)

        def attach_tag_groups(target_row: Dict[str, Any]) -> None:
            for column_name, tag_values in tag_groups.items():
                joined = ", ".join(tag_values)
                target_row[column_name] = joined
                if joined:
                    tag_group_counts[column_name] += 1

        if not variants:
            row = dict(base_row)
            attach_tag_groups(row)
            finalize_json_row(row, product, None)
            rows.append(row)
            continue

        for variant in variants:
            if not isinstance(variant, dict):
                continue
            variant_copy = dict(variant)
            featured = variant_copy.get("featured_image")
            if isinstance(featured, dict):
                src = featured.get("src") or featured.get("url")
                variant_copy["featured_image"] = {"src": src} if src else {}
            flat_variant = flatten_record({"variant": variant_copy})
            row = dict(base_row)
            row.update(flat_variant)
            attach_tag_groups(row)
            finalize_json_row(row, product, variant)
            rows.append(row)

    if not rows:
        return [], []

    columns = {key for row in rows for key in row.keys()}
    tag_group_columns = [col for col in columns if col.startswith("tags_group_")]
    tag_group_columns.sort(key=lambda col: (-tag_group_counts.get(col, 0), col))

    return rows, tag_group_columns


def extract_searchspring_results(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, dict):
        ss_data = payload.get("ssData")
        if isinstance(ss_data, dict):
            nested_results = extract_searchspring_results(ss_data)
            if nested_results:
                return nested_results
        primary = payload.get("results")
        if isinstance(primary, list):
            return [item for item in primary if isinstance(item, dict)]
        if isinstance(primary, dict):
            aggregated: List[Dict[str, Any]] = []
            for value in primary.values():
                if isinstance(value, list):
                    aggregated.extend([item for item in value if isinstance(item, dict)])
            if aggregated:
                return aggregated
        for key, value in payload.items():
            if isinstance(value, list):
                candidates = [item for item in value if isinstance(item, dict)]
                if candidates:
                    return candidates
    return []


def extract_searchspring_variants(product: Dict[str, Any]) -> List[Dict[str, Any]]:
    def parse_ss_sizes_payload(raw_value: Any) -> List[Dict[str, Any]]:
        parsed_variants: List[Dict[str, Any]] = []

        def append_variant(candidate: Dict[str, Any]) -> None:
            if not isinstance(candidate, dict):
                return

            normalized: Dict[str, Any] = {}

            label = candidate.get("label")
            if label not in (None, ""):
                normalized["option1"] = str(label)

            variant_id = candidate.get("variant_id")
            if variant_id in (None, ""):
                variant_id = candidate.get("id")
            if variant_id not in (None, ""):
                normalized["id"] = str(variant_id)

            available = candidate.get("available")
            if available in (None, ""):
                available = candidate.get("quantityAvailable")
            if available not in (None, ""):
                normalized["quantityAvailable"] = available

            if normalized:
                parsed_variants.append(normalized)

        def parse_text_blob(text: str) -> None:
            if not text:
                return

            decoded = html.unescape(text).strip()
            if not decoded:
                return

            try:
                loaded = json.loads(decoded)
            except ValueError:
                loaded = None

            if isinstance(loaded, list):
                for item in loaded:
                    if isinstance(item, dict):
                        append_variant(item)
                if parsed_variants:
                    return

            for block in re.findall(r"\{[^{}]*\}", decoded):
                label_match = re.search(r'"?label"?\s*:\s*"([^\"]+)"', block)
                vid_match = re.search(r'"?(?:variant_id|id)"?\s*:\s*"?(\d+)"?', block)
                qty_match = re.search(r'"?available"?\s*:\s*(-?\d+)', block)

                variant: Dict[str, Any] = {}
                if label_match:
                    variant["option1"] = label_match.group(1)
                if vid_match:
                    variant["id"] = vid_match.group(1)
                if qty_match:
                    variant["quantityAvailable"] = int(qty_match.group(1))
                if variant:
                    parsed_variants.append(variant)

        if isinstance(raw_value, list):
            string_parts: List[str] = []
            for entry in raw_value:
                if isinstance(entry, dict):
                    append_variant(entry)
                elif isinstance(entry, str):
                    string_parts.append(entry)
                    parse_text_blob(entry)

            if string_parts:
                parse_text_blob(",".join(string_parts))
            return parsed_variants

        if isinstance(raw_value, dict):
            append_variant(raw_value)
            return parsed_variants

        if isinstance(raw_value, str):
            parse_text_blob(raw_value)

        return parsed_variants

    variants: List[Dict[str, Any]] = []
    seen_ids: Set[Any] = set()
    candidate_keys = [
        key
        for key in list(product.keys())
        if key.lower()
        in {
            "variants",
            "variant_list",
            "variantlist",
            "skus",
            "sku_list",
            "ss_variants",
        }
    ]
    for key in candidate_keys:
        value = product.pop(key, None)
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, dict):
                    continue
                variant = dict(item)
                vid = variant.get("id")
                dedupe_key = vid if vid not in (None, "") else json.dumps(variant, sort_keys=True)
                if dedupe_key in seen_ids:
                    continue
                seen_ids.add(dedupe_key)
                variants.append(variant)
        elif isinstance(value, dict):
            nodes = value.get("nodes") if isinstance(value.get("nodes"), list) else None
            if nodes is not None:
                for item in nodes:
                    if not isinstance(item, dict):
                        continue
                    variant = dict(item)
                    vid = variant.get("id")
                    dedupe_key = vid if vid not in (None, "") else json.dumps(variant, sort_keys=True)
                    if dedupe_key in seen_ids:
                        continue
                    seen_ids.add(dedupe_key)
                    variants.append(variant)
            else:
                variant = dict(value)
                vid = variant.get("id")
                dedupe_key = vid if vid not in (None, "") else json.dumps(variant, sort_keys=True)
                if dedupe_key in seen_ids:
                    continue
                seen_ids.add(dedupe_key)
                variants.append(variant)

    for size_key in ("ss_size_json", "ss_sizes_json", "ss_sizes"):
        raw_value = product.pop(size_key, None)
        if raw_value in (None, ""):
            continue

        for item in parse_ss_sizes_payload(raw_value):
            variant = dict(item)
            vid = variant.get("id")
            dedupe_key = vid if vid not in (None, "") else json.dumps(variant, sort_keys=True)
            if dedupe_key in seen_ids:
                continue
            seen_ids.add(dedupe_key)
            variants.append(variant)

    return variants


def fetch_searchspring_data(
    session: requests.Session, logger: logging.Logger
) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not SEARCHSPRING_SITE_ID or not SEARCHSPRING_URL:
        return [], []

    page = 1
    rows: List[Dict[str, Any]] = []
    tag_group_counts: Counter[str] = Counter()
    base_url = SEARCHSPRING_URL.strip()

    parsed = urlsplit(base_url)
    base_query: Dict[str, Any] = {
        key: value for key, value in parse_qsl(parsed.query, keep_blank_values=True)
    }
    endpoint = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", parsed.fragment))
    is_searchspring_host = "searchspring" in (parsed.netloc or "")

    while True:
        params: Dict[str, Any] = dict(base_query)
        params.update(SEARCHSPRING_EXTRA_PARAMS or {})

        if is_searchspring_host:
            if SEARCHSPRING_SITE_ID and not params.get("siteId"):
                params["siteId"] = SEARCHSPRING_SITE_ID
            params.setdefault("resultsFormat", "json")
            params.setdefault("resultsPerPage", 250)
            primary_url = _primary_collection_url()
            if primary_url and not params.get("domain"):
                params["domain"] = primary_url
        elif SEARCHSPRING_SITE_ID and not params.get("siteId"):
            params["siteId"] = SEARCHSPRING_SITE_ID

        params["page"] = page

        logger.info("Fetching Searchspring page %s", page)
        try:
            response = session.get(endpoint, params=params, timeout=REQUEST_TIMEOUT, verify=False)
        except requests.RequestException as exc:
            logger.warning("Searchspring request failed on page %s: %s", page, exc)
            break

        if not response.ok:
            logger.warning(
                "Searchspring request returned status %s on page %s", response.status_code, page
            )
            break

        try:
            payload = response.json()
        except ValueError:
            logger.warning("Searchspring response on page %s was not valid JSON", page)
            break

        results = extract_searchspring_results(payload)
        if not results:
            logger.info("Searchspring page %s returned no results; stopping", page)
            break

        for product in results:
            if not isinstance(product, dict):
                continue
            product_copy = dict(product)
            variants = extract_searchspring_variants(product_copy)

            tags = collect_tag_values(product)
            tag_groups = group_tags_for_columns(tags)

            def attach_tag_groups(target_row: Dict[str, Any]) -> None:
                for column_name, tag_values in tag_groups.items():
                    joined = ", ".join(tag_values)
                    target_row[column_name] = joined
                    tag_group_counts[column_name] += 1

            image_candidates = [
                product_copy.get(key)
                for key in (
                    "image",
                    "image_url",
                    "imageUrl",
                    "image_link",
                    "thumbnail",
                    "thumbnail_url",
                    "thumbnailImageUrl",
                )
            ]
            image_src = next((candidate for candidate in image_candidates if candidate), None)
            if image_src:
                product_copy.setdefault("images", [{"src": image_src}])

            flat_product = flatten_record({"product": product_copy})
            base_row = dict(flat_product)
            if tags:
                base_row["product.tags_all"] = ", ".join(tags)

            for key in (
                "product.image",
                "product.image_url",
                "product.imageUrl",
                "product.image_link",
                "product.thumbnail",
                "product.thumbnail_url",
                "product.thumbnailImageUrl",
            ):
                if key in base_row and not base_row.get("product.images[0].src"):
                    base_row["product.images[0].src"] = base_row[key]

            if not variants:
                attach_tag_groups(base_row)
                finalize_json_row(base_row, product, None)
                rows.append(base_row)
                continue

            for variant in variants:
                if not isinstance(variant, dict):
                    continue
                variant_copy = dict(variant)
                if "inventory_quantity" not in variant_copy:
                    for candidate in (
                        "inventory_quantity",
                        "inventoryQuantity",
                        "inventory",
                        "qty",
                        "quantity",
                        "available_quantity",
                    ):
                        value = variant_copy.get(candidate)
                        if value not in (None, ""):
                            variant_copy["inventory_quantity"] = value
                            break
                if "availableForSale" not in variant_copy and isinstance(
                    variant_copy.get("available"), bool
                ):
                    variant_copy["availableForSale"] = variant_copy.get("available")

                flat_variant = flatten_record({"variant": variant_copy})
                row = dict(base_row)
                row.update(flat_variant)
                attach_tag_groups(row)
                finalize_json_row(row, product, variant_copy)
                rows.append(row)

        pagination = payload.get("pagination") if isinstance(payload, dict) else None
        next_page: Optional[int] = None
        if isinstance(pagination, dict):
            candidate = pagination.get("nextPage")
            if isinstance(candidate, int):
                next_page = candidate
            elif isinstance(candidate, str) and candidate.isdigit():
                next_page = int(candidate)
            elif pagination.get("page") and pagination.get("totalPages"):
                try:
                    current_page = int(pagination.get("page"))
                    total_pages = int(pagination.get("totalPages"))
                    if current_page < total_pages:
                        next_page = current_page + 1
                except (TypeError, ValueError):
                    next_page = None

        if next_page:
            page = next_page
            time.sleep(0.5)
            continue

        per_page_param = params.get("resultsPerPage")
        try:
            per_page_int = int(per_page_param)
        except (TypeError, ValueError):
            per_page_int = None
        if per_page_int and len(results) >= per_page_int:
            page += 1
            time.sleep(0.5)
            continue

        break

    if not rows:
        return [], []

    columns = {key for row in rows for key in row.keys()}
    tag_group_columns = [col for col in columns if col.startswith("tags_group_")]
    tag_group_columns.sort(key=lambda col: (-tag_group_counts.get(col, 0), col))

    return rows, tag_group_columns


def make_absolute(url: str, base: Any) -> str:
    if not url:
        return url
    if isinstance(base, (list, tuple)):
        base = base[0] if base else ""
    if not isinstance(base, str) or not base:
        primary = _primary_collection_url()
        base = primary or ""
    return urljoin(base, url)


def discover_tokens(
    session: requests.Session, html_blobs: List[Tuple[str, str]], logger: logging.Logger
) -> List[Tuple[str, str]]:
    tokens: Dict[str, str] = {}
    for base_url, html in html_blobs:
        if not html:
            continue
        for token in set(TOKEN_REGEX.findall(html)):
            tokens.setdefault(token, "collection_html")

        soup = BeautifulSoup(html, "html.parser")
        script_urls: List[str] = []
        for script in soup.find_all("script"):
            src = script.get("src")
            if src:
                absolute = make_absolute(src, base_url)
                script_urls.append(absolute)
                for token in set(TOKEN_REGEX.findall(absolute)):
                    tokens.setdefault(token, f"script_url:{absolute}")
            if script.string:
                for token in set(TOKEN_REGEX.findall(script.string)):
                    tokens.setdefault(token, "inline_script")

        for index, script_url in enumerate(script_urls[:MAX_SCRIPT_FETCHES]):
            logger.info(
                "Fetching script %s/%s for token discovery: %s",
                index + 1,
                min(len(script_urls), MAX_SCRIPT_FETCHES),
                script_url,
            )
            try:
                response = session.get(script_url, timeout=REQUEST_TIMEOUT, verify=False)
            except requests.RequestException as exc:
                logger.debug("Failed to fetch script %s: %s", script_url, exc)
                continue
            if not response.ok:
                logger.debug(
                    "Script %s returned status %s", script_url, response.status_code
                )
                continue
            for token in set(TOKEN_REGEX.findall(response.text)):
                tokens.setdefault(token, f"script_body:{script_url}")

    logger.info("Discovered %s potential tokens", len(tokens))
    return [(token, source) for token, source in tokens.items()]


def determine_graphql_endpoints() -> List[str]:
    endpoints: List[str] = []
    if GRAPHQL:
        endpoints.append(GRAPHQL.strip())
    if MYSHOPIFY:
        base = MYSHOPIFY.rstrip("/") + "/"
        for version in DEFAULT_GRAPHQL_VERSIONS:
            endpoints.append(urljoin(base, version))
    return list(dict.fromkeys(endpoint for endpoint in endpoints if endpoint))


def perform_graphql_request(
    session: requests.Session,
    endpoint: str,
    payload: Dict[str, Any],
    token: Optional[str],
) -> Tuple[Optional[requests.Response], Optional[Dict[str, Any]]]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Shopify-Storefront-Access-Token"] = token
    try:
        response = session.post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )
    except requests.RequestException:
        return None, None

    try:
        data = response.json()
    except ValueError:
        data = None
    return response, data


class GraphQLIntrospectionError(RuntimeError):
    pass


class GraphQLSchema:
    def __init__(
        self,
        session: requests.Session,
        endpoint: str,
        token: Optional[str],
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.endpoint = endpoint
        self.token = token
        self.logger = logger
        self._cache: Dict[str, Dict[str, Any]] = {}

    def get_type(self, type_name: Optional[str]) -> Optional[Dict[str, Any]]:
        if not type_name:
            return None
        if type_name in self._cache:
            return self._cache[type_name]

        payload = {"query": INTROSPECTION_QUERY, "variables": {"typeName": type_name}}
        response, data = perform_graphql_request(
            self.session, self.endpoint, payload, self.token
        )
        if response is None or not response.ok:
            raise GraphQLIntrospectionError(
                f"Introspection request failed for {type_name}: {getattr(response, 'status_code', 'error')}"
            )
        type_info = ((data or {}).get("data") or {}).get("__type") if data else None
        if not type_info:
            raise GraphQLIntrospectionError(f"Type {type_name} not found during introspection")
        self._cache[type_name] = type_info
        return type_info


class GraphQLQueryBuilder:
    DEFAULT_CONNECTION_LIMITS: Dict[str, int] = {
        "variants": GRAPHQL_PAGE_SIZE,
        "images": 50,
        "media": 50,
        "collections": 50,
        "components": 100,
        "groupedBy": 100,
        "quantityPriceBreaks": 100,
        "sellingPlanAllocations": 100,
        "sellingPlanGroups": 50,
        "storeAvailability": 100,
    }

    def __init__(
        self,
        session: requests.Session,
        endpoint: str,
        token: Optional[str],
        logger: logging.Logger,
        *,
        max_depth: int = 3,
        forbidden_fields: Optional[Dict[str, Sequence[str]]] = None,
        metafield_identifiers: Optional[Sequence[Tuple[str, str]]] = None,
    ) -> None:
        self.session = session
        self.endpoint = endpoint
        self.token = token
        self.logger = logger
        self.max_depth = max_depth
        self.metafield_identifiers = list(metafield_identifiers or METAFIELD_IDENTIFIERS)
        self.forbidden_fields: Dict[str, Set[str]] = defaultdict(set)
        for parent, names in DEFAULT_FORBIDDEN_FIELDS.items():
            self.forbidden_fields[parent].update(names)
        if forbidden_fields:
            for parent, names in forbidden_fields.items():
                self.forbidden_fields[parent].update(names)
        self.schema = GraphQLSchema(session, endpoint, token, logger)
        self.variant_selection = self._build_type_selection(
            "ProductVariant", max(1, max_depth - 1)
        )
        if not self.variant_selection:
            self.variant_selection = self._build_type_selection("ProductVariant", max_depth)
        if not self.variant_selection:
            raise GraphQLIntrospectionError("Unable to build variant selection set")
        self.product_selection = self._build_type_selection("Product", max_depth)
        if not self.product_selection:
            raise GraphQLIntrospectionError("Unable to build product selection set")
        metafields_selection = self._build_metafields_selection()
        collections_selection = self._build_collections_selection()
        if metafields_selection:
            self.product_selection = "\n".join(
                part for part in [self.product_selection, metafields_selection] if part
            )
        if collections_selection:
            self.product_selection = "\n".join(
                part for part in [self.product_selection, collections_selection] if part
            )
        self.collection_query = self._build_collection_query()
        self.products_query = self._build_products_query()

    def _indent(self, text: str, spaces: int = 2) -> str:
        pad = " " * spaces
        return "\n".join(f"{pad}{line}" if line else pad for line in text.splitlines())

    def _should_include_field(self, parent_type: str, field: Dict[str, Any]) -> bool:
        name = field.get("name")
        if not name or name.startswith("__"):
            return False
        if field_has_required_args(field):
            return False
        if name in self.forbidden_fields.get(parent_type, set()):
            return False
        if parent_type == "ProductVariant" and name == "product":
            return False
        if name in {"sellingPlanGroups", "sellingPlanAllocations"}:
            return False
        return True

    def _build_field_args(self, field: Dict[str, Any]) -> str:
        args = []
        arg_index = {arg.get("name"): arg for arg in field.get("args", [])}
        if "first" in arg_index:
            limit = self.DEFAULT_CONNECTION_LIMITS.get(field.get("name", ""), GRAPHQL_PAGE_SIZE)
            args.append(f"first: {limit}")
        return f"({', '.join(args)})" if args else ""

    def _build_scalar_snapshot(self, type_name: str) -> str:
        type_info = self.schema.get_type(type_name)
        if not type_info:
            return ""
        scalars: List[str] = []
        for field in type_info.get("fields", []):
            if not self._should_include_field(type_name, field):
                continue
            kind, _name, _wrappers = unwrap_type(field.get("type"))
            if kind in {"SCALAR", "ENUM"}:
                scalars.append(field.get("name"))
        return "\n".join(scalars)

    def _build_connection_body(
        self,
        connection_name: str,
        depth: int,
        visited: Sequence[str],
        parent_type: Optional[str],
    ) -> str:
        type_info = self.schema.get_type(connection_name)
        if not type_info or depth <= 0:
            return ""

        lines: List[str] = []
        for field in type_info.get("fields", []):
            fname = field.get("name")
            if fname == "pageInfo":
                lines.append("pageInfo {\n  hasNextPage\n  endCursor\n}")
            elif fname == "edges":
                base_kind, edge_type_name, _ = unwrap_type(field.get("type"))
                if base_kind != "OBJECT" or not edge_type_name:
                    continue
                edge_info = self.schema.get_type(edge_type_name)
                if not edge_info:
                    continue
                edge_lines: List[str] = []
                for edge_field in edge_info.get("fields", []):
                    ename = edge_field.get("name")
                    if ename == "cursor":
                        edge_lines.append("cursor")
                    elif ename == "node":
                        node_kind, node_type_name, _ = unwrap_type(edge_field.get("type"))
                        if node_kind != "OBJECT" or not node_type_name:
                            continue
                        if node_type_name in visited:
                            node_body = self._build_scalar_snapshot(node_type_name)
                        else:
                            node_body = self._build_type_selection(
                                node_type_name,
                                depth - 1,
                                visited=tuple(visited) + (node_type_name,),
                            )
                        if not node_body and node_type_name == parent_type:
                            node_body = self._build_scalar_snapshot(node_type_name)
                        if node_body:
                            edge_lines.append(
                                f"node {{\n{self._indent(node_body)}\n}}"
                            )
                if edge_lines:
                    lines.append(
                        f"edges {{\n{self._indent('\n'.join(edge_lines))}\n}}"
                    )
        return "\n".join(lines)

    def _build_field_selection(
        self,
        parent_type: str,
        field: Dict[str, Any],
        depth: int,
        visited: Sequence[str],
    ) -> Optional[str]:
        name = field.get("name")
        if not self._should_include_field(parent_type, field):
            return None

        if parent_type == "Product" and name == "variants":
            return self._build_variants_field(field)

        base_kind, base_name, wrappers = unwrap_type(field.get("type"))
        if base_kind in {"SCALAR", "ENUM"}:
            return name
        if base_kind == "LIST" and not base_name and wrappers:
            # List ultimately resolves to another type stored deeper in ofType.
            inner = field.get("type", {})
            while inner and inner.get("kind") == "LIST":
                inner = inner.get("ofType")
            base_kind, base_name, _ = unwrap_type(inner)
        if base_kind == "OBJECT" and base_name:
            if base_name in visited or depth <= 0:
                return None
            new_visited = tuple(visited) + (base_name,)
            if base_name.endswith("Connection"):
                body = self._build_connection_body(
                    base_name, depth, new_visited, parent_type=parent_type
                )
            else:
                body = self._build_type_selection(
                    base_name, depth, visited=tuple(visited)
                )
            if not body:
                return None
            args = self._build_field_args(field)
            return f"{name}{args} {{\n{self._indent(body)}\n}}"
        return None

    def _build_metafields_selection(self) -> str:
        if not self.metafield_identifiers:
            return ""

        product_type = self.schema.get_type("Product") or {}
        fields = product_type.get("fields", [])
        metafields_field = next(
            (field for field in fields if field.get("name") == "metafields"), None
        )
        metafield_field = next(
            (field for field in fields if field.get("name") == "metafield"), None
        )

        selection_body = "namespace\nkey\ntype\nvalue"
        identifiers_literal = ", ".join(
            f'{{namespace: "{ns}", key: "{key}"}}'
            for ns, key in self.metafield_identifiers
        )

        if metafields_field and any(arg.get("name") == "identifiers" for arg in metafields_field.get("args", [])):
            return (
                "metafields(identifiers: ["
                + identifiers_literal
                + f"]) {{\n  {selection_body}\n}}"
            )

        if metafield_field and all(
            any(arg.get("name") == name for arg in metafield_field.get("args", []))
            for name in ("namespace", "key")
        ):
            lines: List[str] = []
            for idx, (ns, key) in enumerate(self.metafield_identifiers):
                alias = f"mf_{idx}"
                lines.append(
                    f"{alias}: metafield(namespace: \"{ns}\", key: \"{key}\") {{\n  {selection_body}\n}}"
                )
            return "\n".join(lines)

        self.logger.debug(
            "Metafields selection not added; schema lacks identifiers/namespace+key support"
        )
        return ""

    def _build_collections_selection(self) -> str:
        product_type = self.schema.get_type("Product") or {}
        fields = product_type.get("fields", [])
        collections_field = next(
            (field for field in fields if field.get("name") == "collections"), None
        )
        if not collections_field:
            return ""
        args = self._build_field_args(collections_field)
        body = (
            "edges {\n"
            "  node {\n"
            "    id\n"
            "    handle\n"
            "    title\n"
            "  }\n"
            "}"
        )
        return f"collections{args} {{\n{self._indent(body)}\n}}"

    def _build_variants_field(self, field: Dict[str, Any]) -> Optional[str]:
        args = self._build_field_args(field)
        body = (
            "pageInfo {\n  hasNextPage\n  endCursor\n}\n"
            "edges {\n"
            "  cursor\n"
            "  node {\n"
            f"{self._indent(self.variant_selection, 4)}\n"
            "  }\n"
            "}"
        )
        return f"variants{args} {{\n{self._indent(body)}\n}}"

    def _build_type_selection(
        self,
        type_name: str,
        depth: int,
        *,
        visited: Sequence[str] = (),
    ) -> str:
        if depth <= 0 or type_name in visited:
            return ""
        type_info = self.schema.get_type(type_name)
        if not type_info:
            return ""

        new_visited = tuple(visited) + (type_name,)
        selections: List[str] = []
        for field in type_info.get("fields", []):
            selection = self._build_field_selection(type_name, field, depth - 1, new_visited)
            if selection:
                selections.append(selection)
        return "\n".join(selections)

    def _build_collection_query(self) -> str:
        product_block = self._indent(self.product_selection)
        return (
            "query CollectionProducts($handle: String!, $cursor: String, $pageSize: Int!) {\n"
            "  collection(handle: $handle) {\n"
            "    id\n"
            "    handle\n"
            "    title\n"
            "    products(first: $pageSize, after: $cursor) {\n"
            "      pageInfo {\n"
            "        hasNextPage\n"
            "        endCursor\n"
            "      }\n"
            "      edges {\n"
            "        cursor\n"
            "        node {\n"
            f"{product_block}\n"
            "        }\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}"
        )

    def _build_products_query(self) -> str:
        product_block = self._indent(self.product_selection)
        return (
            "query ProductsProbe($cursor: String, $pageSize: Int!, $query: String) {\n"
            "  products(first: $pageSize, after: $cursor, query: $query) {\n"
            "    pageInfo {\n"
            "      hasNextPage\n"
            "      endCursor\n"
            "    }\n"
            "    edges {\n"
            "      cursor\n"
            "      node {\n"
            f"{product_block}\n"
            "      }\n"
            "    }\n"
            "  }\n"
            "}"
        )

def probe_graphql_endpoints(
    session: requests.Session,
    endpoints: Sequence[str],
    tokens_with_source: Sequence[Tuple[Optional[str], str]],
    logger: logging.Logger,
    *,
    include_unauthenticated: bool = True,
) -> Tuple[List[Dict[str, Any]], List[str], Dict[str, Set[Optional[str]]]]:
    access_rows: List[Dict[str, Any]] = []
    operational: List[str] = []
    success_map: Dict[str, Set[Optional[str]]] = {endpoint: set() for endpoint in endpoints}

    deduped_tokens: List[Tuple[Optional[str], str]] = []
    seen_keys: Set[Tuple[Optional[str], str]] = set()
    for token, source in tokens_with_source:
        normalized = token.strip() if isinstance(token, str) else token
        normalized = normalized or None
        key = (normalized, source)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped_tokens.append((normalized, source))

    for endpoint in endpoints:
        attempts: List[Tuple[Optional[str], str]] = list(deduped_tokens)
        if include_unauthenticated:
            attempts.append((None, "unauthenticated"))

        for token, token_source in attempts:
            payload = {"query": SHOP_PROBE_QUERY}
            response, data = perform_graphql_request(session, endpoint, payload, token)
            entry: Dict[str, Any] = {
                "endpoint": endpoint,
                "token": token or "",
                "token_source": token_source,
                "status_code": getattr(response, "status_code", ""),
                "ok": bool(response and response.ok),
            }
            if response is None:
                entry["note"] = "request_exception"
            elif not response.ok:
                entry["note"] = f"HTTP_{response.status_code}"
            else:
                shop = ((data or {}).get("data") or {}).get("shop") if data else None
                if shop:
                    entry["shop_name"] = shop.get("name")
                    entry["primary_domain"] = (shop.get("primaryDomain") or {}).get("url")
                    entry["note"] = "success"
                    success_map.setdefault(endpoint, set()).add(token)
                    if token is None and endpoint not in operational:
                        operational.append(endpoint)
                else:
                    errors = (data or {}).get("errors") if data else None
                    entry["note"] = format_error_note(errors) if errors else "no_shop_data"
            access_rows.append(entry)
    return access_rows, operational, success_map


def apply_tag_filter(product: Dict[str, Any]) -> bool:
    if not GRAPHQL_FILTER_TAG:
        return True
    tags = product.get("tags") or []
    lowered = {str(tag).lower() for tag in tags}
    return GRAPHQL_FILTER_TAG.lower() in lowered


def probe_collection_filters(
    session: requests.Session,
    endpoint: str,
    token: Optional[str],
    handle: str,
    logger: logging.Logger,
) -> Dict[str, List[str]]:
    for query in FILTER_PROBE_QUERIES:
        payload = {"query": query, "variables": {"handle": handle}}
        response, data = perform_graphql_request(session, endpoint, payload, token)
        if response is None or not response.ok:
            continue

        filters_block = None
        collection = ((data or {}).get("data") or {}).get("collection") if data else None
        if isinstance(collection, dict):
            products = collection.get("products")
            if isinstance(products, dict):
                filters_block = products.get("filters") or products.get("productFilters")

        if not filters_block:
            continue

        filters: Dict[str, List[str]] = {}
        for fil in filters_block:
            if not isinstance(fil, dict):
                continue
            label = fil.get("label") or fil.get("id")
            values = fil.get("values") or []
            if not label:
                continue
            val_labels: List[str] = []
            for val in values:
                if isinstance(val, dict):
                    if val.get("label"):
                        val_labels.append(str(val.get("label")))
                    elif val.get("id"):
                        val_labels.append(str(val.get("id")))
            if val_labels:
                filters[str(label)] = val_labels
        if filters:
            logger.info("Discovered %s filter groups for collection %s", len(filters), handle)
            return filters

    logger.debug("No filters discovered for collection %s", handle)
    return {}


class ViewJSONEnrichmentState:
    def __init__(self, enabled: bool, fields: Sequence[str], probe_limit: int) -> None:
        self.enabled = bool(enabled)
        self.fields = [field.strip() for field in fields if str(field).strip()]
        self.probe_limit = max(int(probe_limit), 0)
        self.probe_attempts = 0
        self.probe_hits = 0
        self.disabled_after_probe = False
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.warned_urls: Set[str] = set()


def _normalize_view_json_url(online_store_url: str) -> str:
    parsed = urlsplit(online_store_url)
    params = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != "view"]
    params.append(("view", "json"))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(params), parsed.fragment))


def _lookup_view_json_field(payload: Any, field: str) -> Any:
    current: Any = payload
    for part in field.split("."):
        key = part.strip()
        if not key:
            return None
        if isinstance(current, dict):
            current = current.get(key)
            continue
        if isinstance(current, list) and key.isdigit():
            idx = int(key)
            if 0 <= idx < len(current):
                current = current[idx]
                continue
        return None
    return current


def _extract_view_json_values(payload: Dict[str, Any], fields: Sequence[str]) -> Dict[str, Any]:
    extracted: Dict[str, Any] = {}
    for field in fields:
        value = _lookup_view_json_field(payload, field)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (dict, list)):
            extracted[f"viewjson.{field}"] = json.dumps(value, ensure_ascii=False)
        else:
            extracted[f"viewjson.{field}"] = str(value)
    return extracted


def _get_view_json_enrichment(
    session: requests.Session,
    product: Dict[str, Any],
    logger: logging.Logger,
    state: Optional[ViewJSONEnrichmentState],
) -> Dict[str, Any]:
    if state is None or not state.enabled or not state.fields:
        return {}
    if state.probe_limit and state.probe_attempts >= state.probe_limit and state.probe_hits == 0:
        state.enabled = False
        state.disabled_after_probe = True
        logger.info(
            "View JSON enrichment disabled after %s probe attempts with no useful fields.",
            state.probe_attempts,
        )
        return {}

    cache_key = str(product.get("id") or product.get("handle") or "")
    if cache_key and cache_key in state.cache:
        return dict(state.cache[cache_key])

    online_store_url = str(product.get("onlineStoreUrl") or "").strip()
    if not online_store_url:
        if cache_key:
            state.cache[cache_key] = {}
        return {}

    view_url = _normalize_view_json_url(online_store_url)
    response: Optional[requests.Response] = None
    try:
        response = session.get(view_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        if view_url not in state.warned_urls:
            state.warned_urls.add(view_url)
            logger.warning("View JSON enrichment failed for %s -> %s", view_url, exc)
        if cache_key:
            state.cache[cache_key] = {}
        if state.probe_attempts < state.probe_limit:
            state.probe_attempts += 1
        return {}

    if not isinstance(payload, dict):
        if view_url not in state.warned_urls:
            state.warned_urls.add(view_url)
            logger.warning("View JSON enrichment returned non-object JSON for %s", view_url)
        if cache_key:
            state.cache[cache_key] = {}
        if state.probe_attempts < state.probe_limit:
            state.probe_attempts += 1
        return {}

    extracted = _extract_view_json_values(payload, state.fields)
    if state.probe_attempts < state.probe_limit:
        state.probe_attempts += 1
        if extracted:
            state.probe_hits += 1
    if cache_key:
        state.cache[cache_key] = extracted
    return dict(extracted)


def flatten_graphql_product(
    collection_info: Dict[str, Any],
    edge_cursor: str,
    product: Dict[str, Any],
    variant_edge: Optional[Dict[str, Any]],
    *,
    session: Optional[requests.Session] = None,
    logger: Optional[logging.Logger] = None,
    view_json_state: Optional[ViewJSONEnrichmentState] = None,
) -> Dict[str, Any]:
    row: Dict[str, Any] = dict(collection_info)
    collections_handles, collections_titles = extract_collections(product, collection_info)
    if collections_handles:
        row["collections.handle"] = ",".join(collections_handles)
    if collections_titles:
        row["collections.title"] = ",".join(collections_titles)

    metafields = collect_metafields(product)
    if metafields:
        row["metafields"] = json.dumps(metafields)

    product_copy = dict(product)
    for drop_key in (
        "encodedVariantAvailability",
        "encodedVariantExistence",
        "featuredImage",
        "images",
        "media",
        "isGiftCard",
    ):
        product_copy.pop(drop_key, None)
    product_copy.pop("collections", None)
    variants = product_copy.pop("variants", None)
    flat_product = flatten_record({"product": product_copy})
    row.update(flat_product)

    if session is not None and logger is not None:
        row.update(_get_view_json_enrichment(session, product, logger, view_json_state))

    option_columns = build_option_columns(product.get("options") or [])
    for key, value in option_columns.items():
        row[key] = value

    collection_filters = collection_info.get("collection_filters") if isinstance(collection_info, dict) else {}
    derived_filters = derive_filter_values(product, metafields)
    filter_values = select_filters_for_product(collection_filters, product, derived_filters)
    apply_filter_columns(row, filter_values)

    if variant_edge is None:
        finalize_storefront_row(row, product, None)
        return row

    variant = dict(variant_edge.get("node") or {})
    for drop_key in ("quantityRule", "image"):
        variant.pop(drop_key, None)
    flat_variant = flatten_record({"variant": variant})
    row.update(flat_variant)
    finalize_storefront_row(row, product, variant)
    return row


def collect_storefront_from_collections(
    session: requests.Session,
    endpoint: str,
    token: Optional[str],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Optional[int], str]:
    forbidden: Dict[str, Set[str]] = defaultdict(set)
    for parent, names in DEFAULT_FORBIDDEN_FIELDS.items():
        forbidden[parent].update(names)

    first_status: Optional[int] = None
    view_json_state = ViewJSONEnrichmentState(
        VIEW_JSON_ENRICHMENT_ENABLED,
        VIEW_JSON_FIELDS,
        VIEW_JSON_PROBE_LIMIT,
    )

    while True:
        try:
            builder = GraphQLQueryBuilder(
                session,
                endpoint,
                token,
                logger,
                forbidden_fields=forbidden,
                metafield_identifiers=METAFIELD_IDENTIFIERS,
            )
        except GraphQLIntrospectionError as exc:
            logger.debug("Unable to build collection query for %s: %s", endpoint, exc)
            return [], None, "builder_error"

        query_text = builder.collection_query
        rows: List[Dict[str, Any]] = []
        need_retry = False
        newly_blocked: Dict[str, Set[str]] = defaultdict(set)
        collection_filters_cache: Dict[str, Dict[str, List[str]]] = {}

        for handle in STOREFRONT_COLLECTION_HANDLES:
            if handle not in collection_filters_cache:
                collection_filters_cache[handle] = probe_collection_filters(
                    session, endpoint, token, handle, logger
                )
            handle_filters = collection_filters_cache.get(handle) or {}
            cursor: Optional[str] = None
            while True:
                payload = {
                    "query": query_text,
                    "variables": {
                        "handle": handle,
                        "cursor": cursor,
                        "pageSize": GRAPHQL_PAGE_SIZE,
                    },
                }
                response, data = perform_graphql_request(
                    session, endpoint, payload, token
                )
                if first_status is None and response is not None:
                    first_status = response.status_code
                if response is None:
                    return [], first_status, "request_exception"
                if not response.ok:
                    return [], first_status, f"HTTP_{response.status_code}"

                payload_data = (data or {}).get("data") if data else None
                collection = (payload_data or {}).get("collection") if payload_data else None
                errors = (data or {}).get("errors") if data else None

                if not collection:
                    if errors:
                        unrecoverable = True
                        for error in errors:
                            path = error.get("path") or []
                            field_name = extract_field_from_error_path(path)
                            if not field_name:
                                continue
                            unrecoverable = False
                            target_type = infer_error_target_type(path)
                            if field_name not in forbidden[target_type]:
                                forbidden[target_type].add(field_name)
                                newly_blocked[target_type].add(field_name)
                                need_retry = True
                        if unrecoverable:
                            return [], first_status, format_error_note(errors)
                        break
                    return [], first_status, "no_collection_data"

                if errors:
                    logger.debug(
                        "Collection query returned %s errors for handle %s on %s",
                        len(errors),
                        handle,
                        endpoint,
                    )
                    new_field_added = False
                    for error in errors:
                        path = error.get("path") or []
                        field_name = extract_field_from_error_path(path)
                        if not field_name:
                            continue
                        target_type = infer_error_target_type(path)
                        if field_name not in forbidden[target_type]:
                            forbidden[target_type].add(field_name)
                            newly_blocked[target_type].add(field_name)
                            need_retry = True
                            new_field_added = True
                    if need_retry:
                        break
                    if not new_field_added:
                        return [], first_status, format_error_note(errors)

                collection_info = {
                    "collection_id": collection.get("id"),
                    "collection_handle": collection.get("handle"),
                    "collection_title": collection.get("title"),
                    "collection_filters": handle_filters,
                }
                products_connection = collection.get("products") or {}
                edges: Iterable[Dict[str, Any]] = products_connection.get("edges") or []
                for edge in edges:
                    product = edge.get("node") or {}
                    if not apply_tag_filter(product):
                        continue
                    variants_connection = product.get("variants") or {}
                    variant_entries = extract_graphql_variant_entries(
                        variants_connection
                    )
                    if not variant_entries:
                        rows.append(
                            flatten_graphql_product(
                                collection_info,
                                edge.get("cursor", ""),
                                product,
                                None,
                                session=session,
                                logger=logger,
                                view_json_state=view_json_state,
                            )
                        )
                    else:
                        for variant_edge in variant_entries:
                            rows.append(
                                flatten_graphql_product(
                                    collection_info,
                                    edge.get("cursor", ""),
                                    product,
                                    variant_edge,
                                    session=session,
                                    logger=logger,
                                    view_json_state=view_json_state,
                                )
                            )
                page_info = products_connection.get("pageInfo") or {}
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                    logger.info(
                        "Collection %s has additional Storefront pages; continuing",
                        handle,
                    )
                    time.sleep(0.5)
                else:
                    break

            if need_retry:
                break

        if need_retry:
            blocked_summary = {
                parent: sorted(fields)
                for parent, fields in newly_blocked.items()
                if fields
            }
            if blocked_summary:
                logger.info(
                    "Retrying collection query without restricted fields: %s",
                    blocked_summary,
                )
            else:
                logger.debug(
                    "Encountered errors but no removable fields; aborting with failure"
                )
                return [], first_status, "errors"
            continue

        note = "success" if rows else "no_rows"
        return rows, first_status, note


def build_product_query_string() -> Optional[str]:
    query_parts: List[str] = []
    if GRAPHQL_FILTER_TAG:
        if " " in GRAPHQL_FILTER_TAG:
            query_parts.append(f'tag:"{GRAPHQL_FILTER_TAG}"')
        else:
            query_parts.append(f"tag:{GRAPHQL_FILTER_TAG}")
    for handle in STOREFRONT_COLLECTION_HANDLES:
        query_parts.append(f"collection:{handle}")
    return " ".join(query_parts) if query_parts else None


def collect_storefront_from_products(
    session: requests.Session,
    endpoint: str,
    token: Optional[str],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Optional[int], str]:
    rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    query_string = build_product_query_string()
    first_status: Optional[int] = None
    view_json_state = ViewJSONEnrichmentState(
        VIEW_JSON_ENRICHMENT_ENABLED,
        VIEW_JSON_FIELDS,
        VIEW_JSON_PROBE_LIMIT,
    )
    forbidden: Dict[str, Set[str]] = defaultdict(set)
    for parent, names in DEFAULT_FORBIDDEN_FIELDS.items():
        forbidden[parent].update(names)

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


def compute_naming(product_title: str, jean_style_first_word: str = "") -> Dict[str, str]:
    """Steps 1-7. Returns every intermediate label plus the final outputs."""
    raw_title = _clean_naming_title(product_title)
    # The length keyword is read from the raw title; everything else is read
    # from the title with the length word taken out.
    inseam_label_kw = mode2_maximal_join(raw_title, INSEAM_LABEL_KEYWORDS)
    title = _strip_length_words(raw_title)

    # Step 1 — Mode 2 over each category
    jean_style_label = mode2_maximal_join(title, JEAN_STYLE_KEYWORDS)
    product_line_label = mode2_maximal_join(title, PRODUCT_LINE_KEYWORDS)
    pullon_label = mode2_maximal_join(title, PULLON_KEYWORDS)
    type2_label = mode2_maximal_join(title, TYPE2_KEYWORDS)
    fabric_label = mode2_maximal_join(title, FABRIC_KEYWORDS)
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
    if (has("cigarette", "slim straight", "soft stretch point")
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
        m = re.search(r"Inseam\s*:?\s*(\d+(?:\.\d+)?)", desc, re.IGNORECASE)
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
    note = "success" if rows else "no_rows"
    return rows, first_status, note


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

        # VARIANT_TITLE_PRE does not depend on Jean Style, so build it first
        # and read the Jean Style keywords off it: it has the length word
        # moved out, so phrases like "GOOD 90" stay contiguous.
        naming = compute_naming(title, "")
        jean_style = jean_style_from_title(naming["variant_title_pre"])
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

            staged_rows.append({
                "Style Id": style_id,
                "Handle": handle,
                "Published At": parse_date(product.get("publishedAt")),
                "Created At": parse_date(product.get("createdAt")),
                "Updated At": parse_date(product.get("updatedAt")),
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
                "Color - Simplified": determine_color_simplified(
                    tags, description, determine_color_standardized(tags, description)),
                "Color - Standardized": determine_color_standardized(tags, description),
                "Stretch": determine_stretch(description),
                "_style_name_draft": naming["style_name_draft"],
                "_vt_pre": naming["variant_title_pre"],
                "_inseam_label_kw": naming["inseam_label_kw"],
                "_color_code": color_code,
                "_attr_label": length_segment_store,
                "_variant_length": attr_label,
                "_raw_title": title,
                "_sku_no_size": sku_no_size,
                "_pdp_active": "1" if pdp_active else "",
            })

    attempted_sources: set = set()

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
        row.pop("_variant_length", None)
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

    rows_by_name: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        rows_by_name.setdefault(row["Style Name"].upper(), []).append(row)

    for row in rows:
        kw = row.get("_inseam_label_kw", "")
        if not kw:
            continue
        # Only rescue a name that stands alone, or whose every sku is a
        # length-specific one; a name shared with regular skus is already right.
        peers = rows_by_name.get(row["Style Name"].upper(), [])
        all_length_specific = all(r.get("_inseam_label_kw") for r in peers)
        if not all_length_specific:
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
                                             row.get("_variant_length", ""))


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


def _insert_old_marker(product: str) -> str:
    if re.search(r"OLD", product, re.IGNORECASE):
        return product
    if "|" in product:
        base, rest = product.split("|", 1)
        return clean_text(f"{clean_text(base)} OLD | {clean_text(rest)}")
    return clean_text(f"{product} OLD")

    access_sheet = workbook.create_sheet("Storefront_access")
    write_sheet(access_sheet, access_rows)

    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"{BRAND_SLUG}_probe_{timestamp}.xlsx"
    workbook.save(output_path)
    return output_path


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Retail data probe")
    parser.add_argument(
        "--metafield",
        dest="metafields",
        help="Comma-separated list of namespace:key metafields to request via Storefront API",
        default="",
    )
    args = parser.parse_args()

    global METAFIELD_IDENTIFIERS
    METAFIELD_IDENTIFIERS = parse_metafield_identifiers(args.metafields)

    logger = configure_logging()
    session = build_session()
    global COLLECTION_TITLE_MAP
    COLLECTION_TITLE_MAP = fetch_collection_titles(session, logger)
    html_blobs = fetch_collection_html(session, logger)
    json_rows, tag_group_columns = fetch_collection_json(session, logger)
    if SEARCHSPRING_SITE_ID and SEARCHSPRING_URL:
        searchspring_rows, searchspring_tag_columns = fetch_searchspring_data(session, logger)
    else:
        logger.info("Searchspring configuration missing; skipping Searchspring extraction")
        searchspring_rows, searchspring_tag_columns = [], []
    storefront_rows, access_rows = gather_storefront_data(session, html_blobs, logger)
    output_path = export_workbook(
        json_rows,
        storefront_rows,
        access_rows,
        searchspring_rows,
        json_priority_columns=tag_group_columns,
        searchspring_priority_columns=searchspring_tag_columns,
    )
    logger.info("Workbook written to %s", output_path.as_posix())


if __name__ == "__main__":
    main()
