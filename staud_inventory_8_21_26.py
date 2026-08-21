#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import requests
from bs4 import BeautifulSoup

BRAND = "STAUD"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / f"{BRAND}_run.log"

HOST_ROTATION = [
    "https://staud.clothing",
    "https://www.staud.clothing",
    "https://staud-clothing.myshopify.com",
]

COLLECTION_HANDLES = ["staud-jeans", "sale","denim"]
GRAPHQL_ENDPOINT = "https://staud-clothing.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN_ENV_VARS = [
    "X_SHOPIFY_STOREFRONT_ACCESS_TOKEN",
    "SHOPIFY_STOREFRONT_ACCESS_TOKEN",
    "STAUD_STOREFRONT_TOKEN",
    "x-shopify-storefront-access-token",
]
HARDCODED_TOKENS = ["ff6b027455fec0e6fcaad09c69088b1e"]

FILTER_HANDLE_SUBSTRINGS = [
    "dress", "sweater", "jacket", "shoe", "heel", "sandal", "ankle-boot",
    "over-the-knee-boot", "platform", "pump", "wedge", "sling-back", "pullover",
    "romper", "bag", "blazer", "bodysuit", "capri", "carryall", "carry-all", "coat",
    "combat-boot", "skirt", "accessories", "rompers", "shorts", "ballet-flat", "hat",
    "scarf", "scrunchie", "tank", "tee", "thong", "unitard", "culotte","short","vest","shirt",
    "Bermuda", "Bermudas", "Blazer", "Blazers", "Blouse", "Blouses", "Bodysuit", "Bodysuits",
    "Button-Up", "Capri", "Cardigan", "Cardigans", "Coat", "Coats", "Corset", "Corsets", "Crop-Top", 
    "Crop-Tops", "croptop", "Denim-Short", "Denimshort", "Donation", "Dress", "Dresses",
    "Handbag", "Heel", "Heels", "Hoodie", "Hoodies", "Jacket", "Jackets", "Jogger-Short", "Joggershort", "Jort", 
    "Jumpsuit", "Jumpsuits", "Neck", "Onepiece", "One-Piece", "One-Pieces", "Outerwear", "Pant-Suit", "Pantsuits", 
    "Purse", "Romper", "Rompers", "Sandel", "Sandle", "Shacket", "Shipping", "Shipping-Protection", "Shirt", "Shirts", 
    "Shoe", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Sleeve", "Sleeves", "Suit", "Suits", "Sweater", "Sweaters", "Sweatpant", 
    "Sweatpants", "Sweatshirt", "Sweatshirts", "Swim", "TShirt", "T-Shirts", "Tank", "Tank-Tops", "Tee", "Tees", "Tops", "Tote",
]
FILTER_TITLE_WORDS = {"short", "skirt", "capri", "romper", "jacket", "culotte","shirt","vest"}
ALLOWED_PRODUCT_TYPE = "denim"
FILTER_PRODUCT_TYPE_WORDS = {
    "Accessories", "Accessory", "Bermuda", "Bermudas", "Blazer", "Blazers", "Blouse", "Blouses", "Bodysuit", "Bodysuits", 
    "Button Up", "Button-Up", "Capri", "Cardigan", "Cardigans", "Clothing Top", "Clothing Tops", "Coat", "Coats", "Coats & Jackets", 
    "Core Handbags", "Corset", "Corsets", "Crop Top", "Crop Tops", "Denim Short", "Denim Shorts", "Donation", "Dress", "Dresses", 
    "Fashion Core Handbag", "Fashion Core Handbags", "Fashion Handbag", "Fashion Handbags", "Gift Wrap", "Goodies Accessories", 
    "Goodies Accessory", "Handbag", "Heel", "Heels", "Hoodie", "Hoodies", "Jacket", "Jackets", "Jogger Short", "Jogger Shorts", "Jort", 
    "Jumpsuit", "Jumpsuits", "Neck", "One Piece", "One Pieces", "One-Piece", "One-Pieces", "Outerwear", "Pant Suit", "Pant Suits", 
    "Purse", "Romper", "Rompers", "Sandel", "Sandle", "Shacket", "Shipping", "Shipping Protection", "Shirt", "Shirts", "Shirts & Tops", 
    "Shoe", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Sleeve", "Sleeves", "Suit", "Suits", "Sweat", "Sweater", "Sweaters", "Sweatpant", 
    "Sweatpants", "Sweats", "Sweatshirt", "Sweatshirts", "Swim", "T Shirt", "T Shirts", "Tank", "Tank Tops", "Tee", "Tees", "Top", "Tops", "Tote"
}

STYLE_REMOVE_TERMS = [
    # Original list (kept — none of these conflict with the new spec below)
    "Accent Hardware", "Beaded", "Corduroy", "Cuff", "Cuffed", "Darted", "Destroyed", "Distressed",
    "Flap Pocket", "Flap", "Frayed Seam", "Front Yoke", "w/ Slit Hem", "Leather", "Lightweight",
    "low and loose", "patch", "petite", "Pintucked", "Raw Hem", "Seam", "Seamed Front Yoke",
    "Seamed", "Side Seam Snaps", "Split", "Snake Print", "Track Pant", "Vegan Leather", "vent",
    "slit", "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "W/ Stud Detailing", "W/ Wide Cuff",
    "W/Flap", "Welt Pocket", "With Cuff", "With Frayed Seam", "Zipper", "BELTED", "CROP", "CUTOFF",
    "FLAG", "FLIP", "KRYSTAL", "CRYSTAL", "PANEL", "PLAID", "RENAISSANCE", "RINSE", "RIPPED",
    "SADDLE", "SEQUIN", "SOTT", "SPARK", "SPARKLE", "SPLICED", "STUDDED", "TRASHED", "WAX",
    "COATED", "FRONTIER", "LO", "SELVEDGE", "CRUSHED", "REPAIR", "SLICE", "PLUS", "ANKLE", "crease",
    # 2026-08-03 spec Step 3 additions (union with the above; matching is
    # already case-insensitive via re.IGNORECASE, so casing here is
    # cosmetic only)
    "Cargo", "Chap", "Constructed", "Cropped", "Cut-out", "Drawstring", "Extra", "Fit",
    "High Rise", "High-Rise", "Inch", "Inset", "Jean", "Jeans", "Kick", "Long", "Low Rise",
    "Low-Rise", "Mid Rise", "Mid-Rise", "Ms.", "Pant", "Pants", "Pleated", "Pocket", "Poplin",
    "Retro", "Rolled Hem", "Rolled Up", "stretch", "Slouchy", "Stacked Waist", "Stacked",
    "Stoned", "Super", "The", "Trouser", "Ultra", "Utility", "V-High Rise", "Vintage", "with", "seams", "tall"
]


def style_name(title: str) -> str:
    """
    Style Name derivation (2026-08-03 spec):
      Step 1: everything before " | "
      Step 2: "-" -> " " (e.g. "Wide-Leg" -> "Wide Leg")
      Step 3: strip numbers, quotation marks, and all STYLE_REMOVE_TERMS
    """
    s = (title or "").split("|")[0]
    s = s.replace("-", " ")
    s = re.sub(r"\d+", " ", s)          # any numbers
    s = s.replace('"', " ").replace("\u201c", " ").replace("\u201d", " ")  # quotation marks
    for term in sorted(STYLE_REMOVE_TERMS, key=len, reverse=True):
        s = re.sub(r"\b" + re.escape(term).replace(r"\ ", r"\s+") + r"\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()

CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size", "Rise", "Inseam", "Price", "Compare at Price",
    "Available for Sale", "Quantity Available", "Quantity of style", "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL", "Jean Style", "Inseam Style", "Inseam Label", "Rise Label", "Color - Standardized", "Stretch",
]

GRAPHQL_COLLECTION_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 100, after: $cursor) {
      nodes {
        id
        handle
        title
        productType
        tags
        vendor
        description
        publishedAt
        createdAt
        totalInventory
        onlineStoreUrl
        featuredImage { url }
        variants(first: 100) {
          nodes {
            id
            title
            sku
            barcode
            availableForSale
            quantityAvailable
            price { amount }
            compareAtPrice { amount }
            selectedOptions { name value }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

GRAPHQL_PRODUCT_QUERY = """
query ProductByHandle($handle: String!) {
  product(handle: $handle) {
    id
    handle
    title
    productType
    tags
    vendor
    description
    publishedAt
    createdAt
    totalInventory
    onlineStoreUrl
    featuredImage { url }
    variants(first: 100) {
      nodes {
        id
        title
        sku
        barcode
        availableForSale
        quantityAvailable
        price { amount }
        compareAtPrice { amount }
        selectedOptions { name value }
      }
    }
  }
}
"""

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def configure_logging() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
    )


def graphql_token() -> str:
    for key in GRAPHQL_TOKEN_ENV_VARS:
        value = os.getenv(key)
        if value:
            return value.strip()
    return HARDCODED_TOKENS[0]


def normalize_text(text: str) -> str:
    t = (text or "").lower().replace("-", " ")
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def contains_any(text: str, phrases: Sequence[str]) -> bool:
    n = normalize_text(text)
    return any(normalize_text(p) in n for p in phrases)


def parse_number_with_fraction(text: str) -> str:
    raw = (text or "").replace("½", " 1/2").replace("¼", " 1/4").replace("¾", " 3/4").replace('"', " ")
    total = 0.0
    for tok in re.split(r"\s+", raw.strip()):
        if not tok:
            continue
        if re.fullmatch(r"\d+/\d+", tok):
            total += float(Fraction(tok))
        else:
            try:
                total += float(tok)
            except Exception:
                pass
    if not total:
        return ""
    rounded = Decimal(str(total)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    if rounded == rounded.to_integral():
        return str(int(rounded))
    return format(rounded.normalize(), "f").rstrip("0").rstrip(".")


def normalize_measurement(value: str) -> str:
    parsed = parse_number_with_fraction(value)
    if not parsed:
        return ""
    try:
        numeric = float(parsed)
    except Exception:
        return parsed
    if numeric > 38:
        return f"{numeric / 2.54:.3f}".rstrip("0").rstrip(".")
    return parsed


def to_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        num = float(str(value).replace(",", "").strip())
    except Exception:
        return ""
    if abs(num - int(num)) < 1e-9:
        return f"${int(num):,}"
    return f"${num:,.2f}"


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except Exception:
        return ""


def strip_gid(value: str, prefix: str) -> str:
    if not value:
        return ""
    return value.replace(prefix, "") if value.startswith(prefix) else value.split("/")[-1]


def request_with_rotation(path_or_url: str, *, method: str = "GET", headers: Optional[dict] = None, payload: Optional[dict] = None, params: Optional[dict] = None, timeout: int = 45) -> requests.Response:
    urls: List[str] = []
    if path_or_url.startswith("http"):
        urls = [path_or_url]
    else:
        urls = [f"{host.rstrip('/')}/{path_or_url.lstrip('/')}" for host in HOST_ROTATION]
    last: Optional[Exception] = None
    for url in urls:
        try:
            if method == "POST":
                resp = SESSION.post(url, headers=headers, json=payload, params=params, timeout=timeout)
            else:
                resp = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code in {404, 410}:
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last = exc
            logging.warning("Request failed %s: %s", url, exc)
            time.sleep(0.4)
    raise RuntimeError(f"Failed request: {path_or_url} ({last})")


def fetch_collection_handles_json(collection: str) -> List[str]:
    out: List[str] = []
    page = 1
    while True:
        products: List[dict] = []
        for path in (f"/collections/{collection}.json", f"/collections/{collection}/products.json"):
            try:
                resp = request_with_rotation(path, params={"limit": 250, "page": page})
                products = (resp.json() or {}).get("products") or []
                break
            except Exception:
                continue
        if not products:
            break
        out.extend([str(p.get("handle") or "") for p in products if p.get("handle")])
        page += 1
    return out


def fetch_collection_products_graphql(collection: str) -> List[dict]:
    headers = {"X-Shopify-Storefront-Access-Token": graphql_token(), "Content-Type": "application/json"}
    cursor: Optional[str] = None
    out: List[dict] = []
    while True:
        data = request_with_rotation(
            GRAPHQL_ENDPOINT, method="POST", headers=headers,
            payload={"query": GRAPHQL_COLLECTION_QUERY, "variables": {"handle": collection, "cursor": cursor}},
        ).json()
        if data.get("errors"):
            raise RuntimeError(data["errors"])
        node = (((data.get("data") or {}).get("collection") or {}).get("products") or {})
        batch = node.get("nodes") or []
        out.extend(batch)
        info = node.get("pageInfo") or {}
        if not info.get("hasNextPage"):
            break
        cursor = info.get("endCursor")
        if not cursor:
            break
    return out


def fetch_product_by_handle(handle: str) -> Optional[dict]:
    headers = {"X-Shopify-Storefront-Access-Token": graphql_token(), "Content-Type": "application/json"}
    data = request_with_rotation(
        GRAPHQL_ENDPOINT, method="POST", headers=headers,
        payload={"query": GRAPHQL_PRODUCT_QUERY, "variables": {"handle": handle}},
    ).json()
    return ((data.get("data") or {}).get("product") or None)


def option_value(variant: dict, keys: Sequence[str]) -> str:
    for opt in variant.get("selectedOptions") or []:
        name = normalize_text(str(opt.get("name") or ""))
        if name in {normalize_text(k) for k in keys}:
            return str(opt.get("value") or "")
    return ""


def parse_pdp_details(handle: str) -> Dict[str, Any]:
    resp = request_with_rotation(f"/products/{handle}")
    html = resp.text
    soup = BeautifulSoup(html, "lxml")
    if not soup.find():
        soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("[id^='Details-Content-content_block_'] > div")
    parts: List[str] = []
    for block in blocks:
        lis = block.find_all("li")
        if lis:
            parts.extend([li.get_text(" ", strip=True) for li in lis if li.get_text(" ", strip=True)])
        else:
            txt = block.get_text(" ", strip=True)
            if txt:
                parts.append(txt)
    txt = " ".join(parts)
    txt = re.sub(r"\s+", " ", txt).strip()
    kiwi_fields = extract_kiwi_fields(html)
    rise_map: Dict[str, str] = {}
    inseam_map: Dict[str, str] = {}
    if kiwi_fields:
        rise_map, inseam_map = fetch_kiwi_measurements(kiwi_fields, handle)
    return {"details_text": txt, "rise_map": rise_map, "inseam_map": inseam_map}


def extract_kiwi_fields(html: str) -> Optional[Dict[str, str]]:
    block = re.search(r"KiwiSizing\.data\s*=\s*\{(.*?)\};\s*</script>", html, flags=re.IGNORECASE | re.DOTALL)
    if not block:
        return None
    section = block.group(1)
    out: Dict[str, str] = {}
    for key in ("collections", "tags", "product", "vendor", "type", "title"):
        match = re.search(rf"{key}:\s*\"(.*?)\"", section, flags=re.IGNORECASE | re.DOTALL)
        if match:
            out[key] = match.group(1).replace("\\/", "/")
    if not out.get("product"):
        return None
    out["shop"] = "staud-clothing.myshopify.com"
    return out


def fetch_kiwi_measurements(kiwi_fields: Dict[str, str], handle: str) -> tuple[Dict[str, str], Dict[str, str]]:
    try:
        resp = SESSION.get(
            "https://app.kiwisizing.com/kiwiSizing/api/getSizingChart",
            params={**kiwi_fields, "url": f"https://staud.clothing/products/{handle}"},
            timeout=45,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        logging.warning("Kiwi sizing request failed for %s: %s", handle, exc)
        return {}, {}

    first_word = normalize_text(handle).split()[0] if normalize_text(handle) else ""
    rise_by_size: Dict[str, str] = {}
    inseam_by_size: Dict[str, str] = {}
    for sizing in payload.get("sizings") or []:
        for table in (sizing.get("tables") or {}).values():
            rows = table.get("data") or []
            size_cols = find_size_row(rows, first_word)
            if not size_cols:
                continue
            rise_vals = find_measurement_row(rows, "RISE")
            inseam_vals = find_measurement_row(rows, "INSEAM")
            if rise_vals:
                for size, raw in zip(size_cols, rise_vals):
                    value = normalize_measurement(raw)
                    n_size = normalize_size_token(size)
                    if n_size and value and n_size not in rise_by_size:
                        rise_by_size[n_size] = value
            if inseam_vals:
                for size, raw in zip(size_cols, inseam_vals):
                    value = normalize_measurement(raw)
                    n_size = normalize_size_token(size)
                    if n_size and value and n_size not in inseam_by_size:
                        inseam_by_size[n_size] = value
    return rise_by_size, inseam_by_size


def find_size_row(rows: Sequence[Any], first_word: str) -> List[str]:
    for row in rows:
        values = [str((cell or {}).get("value") or "").strip() for cell in row]
        if not values:
            continue
        header = normalize_text(values[0])
        data = [v.strip() for v in values[1:] if str(v).strip()]
        if not data:
            continue
        numeric_like = sum(1 for v in data if re.fullmatch(r"\d{1,2}", v))
        if not first_word or header.split()[:1] == [first_word]:
            if numeric_like >= max(4, len(data) // 2):
                return data
    for row in rows:
        values = [str((cell or {}).get("value") or "").strip() for cell in row]
        data = [v.strip() for v in values[1:] if str(v).strip()]
        if data and sum(1 for v in data if re.fullmatch(r"\d{1,2}", v)) >= max(4, len(data) // 2):
            return data
    return []


def find_measurement_row(rows: Sequence[Any], label: str) -> List[str]:
    for row in rows:
        values = [str((cell or {}).get("value") or "").strip() for cell in row]
        if not values:
            continue
        header = normalize_text(values[0])
        if header == normalize_text(label):
            return [v.strip() for v in values[1:]]
    return []


def normalize_size_token(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d+\.0+", raw):
        return raw.split(".", 1)[0]
    return raw


def is_filtered(handle: str, title: str, product_type: str) -> bool:
    h = normalize_text(handle)
    if "wally" in h and "boot" in h:
        return True
    if any(normalize_text(x) in h for x in FILTER_HANDLE_SUBSTRINGS):
        return True
    t = normalize_text(title)
    if any(word in t.split() for word in FILTER_TITLE_WORDS):
        return True
    pt = normalize_text(product_type)
    if pt != ALLOWED_PRODUCT_TYPE:
        return True
    if any(bad in pt for bad in FILTER_PRODUCT_TYPE_WORDS):
        return True
    return False


def parse_inseam(desc: str, option3: str) -> str:
    if option3:
        num = parse_number_with_fraction(option3)
        if num:
            v = float(num)
            if v > 38:
                return f"{v / 2.54:.3f}".rstrip("0").rstrip(".")
            return num
    opt_match = re.search(r"inseam\s*options?\s*[:\-]?", desc, flags=re.IGNORECASE)
    if opt_match:
        snippet = desc[opt_match.end(): opt_match.end() + 120]
        stop = re.search(r"(?:\bfit\b|\bmodel\b|\bcontact\b|\bcare\b|\bfabrication\b|\brise\b|[.;])", snippet, flags=re.IGNORECASE)
        if stop:
            snippet = snippet[:stop.start()]
        tokens = re.findall(r"\d+(?:\.\d+)?(?:\s+\d+/\d+|/[0-9]+|\s*[¼½¾])?", snippet)
        values = [normalize_measurement(tok) for tok in tokens if normalize_measurement(tok)]
        numeric_values: List[float] = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except Exception:
                pass
        if numeric_values:
            return str(int(min(numeric_values))) if min(numeric_values).is_integer() else str(min(numeric_values))

    patterns = [
        r"(?:inseam options|inseam|inleg length|inleg)\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+|\s*[¼½¾])?)\s*\"?",
        r"([0-9]+(?:\.[0-9]+)?)\s*cm\s*/\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+|\s*[¼½¾])?)\s*\"",
    ]
    for pat in patterns:
        m = re.search(pat, desc, flags=re.IGNORECASE)
        if m:
            if len(m.groups()) > 1:
                return parse_number_with_fraction(m.group(2))
            return parse_number_with_fraction(m.group(1))
    return ""


def parse_rise(desc: str) -> str:
    m = re.search(r"rise\s*[:\-]?\s*(?:mid|low|high|ultra high|ultra low)?\s*[,\-(]?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+|\s*[¼½¾])?)", desc, flags=re.IGNORECASE)
    return parse_number_with_fraction(m.group(1)) if m else ""


def determine_rise_label(title: str, handle: str, desc: str, tags: str) -> str:
    if contains_any(title, ["super low rise", "ultra low rise", "super low waist", "ultra low waist"]): return "Ultra Low"
    if contains_any(title, ["super high rise", "ultra high rise", "super high waist", "ultra high waist"]): return "Ultra High"
    if contains_any(title, ["mid rise"]): return "Mid"
    if contains_any(title, ["low rise"]): return "Low"
    if contains_any(title, ["high rise"]): return "High"
    if contains_any(handle, ["slr", "ulr", "slw", "ulw", "superlowrise", "ultralowrise"]): return "Ultra Low"
    if contains_any(handle, ["shr", "uhr", "shw", "uhw", "superhighrise", "ultrahighrise"]): return "Ultra High"
    if contains_any(handle, ["-mr-", "-mw-", "mid rise", "mid waist"]): return "Mid"
    if contains_any(handle, ["-lr-", "-lw-", "low rise", "low waist"]): return "Low"
    if contains_any(handle, ["-hr-", "-hw-", "high rise", "high waist"]): return "High"
    if contains_any(desc, ["rise: super low", "rise: ultra low", "super low rise", "ultra low rise"]): return "Ultra Low"
    if contains_any(desc, ["rise: super high", "rise: ultra high", "super high rise", "ultra high rise"]): return "Ultra High"
    if contains_any(desc, ["rise: mid", "mid rise"]): return "Mid"
    if contains_any(desc, ["rise: low", "low rise", "low on the hip", "low on the waist"]): return "Low"
    if contains_any(desc, ["rise: high", "high rise", "high waist", "elevated waistline"]): return "High"
    if contains_any(tags, ["filter_rise_mid"]): return "Mid"
    if contains_any(tags, ["filter_rise_low"]): return "Low"
    if contains_any(tags, ["filter_rise_high"]): return "High"
    t = normalize_text(tags)
    if "mid" in t and "low" not in t and "high" not in t: return "Mid"
    if "low" in t and "mid" not in t and "high" not in t: return "Low"
    if "high" in t and "mid" not in t and "low" not in t: return "High"
    return ""


def determine_jean_style(title: str, desc: str, tags: str) -> str:
    if contains_any(title, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(title, ["tapered", "mom"]): return "Tapered"
    if contains_any(title, ["baggy"]): return "Baggy"
    if contains_any(title, ["flare"]): return "Flare"
    if contains_any(title, ["bootcut", "boot"]): return "Bootcut"
    if contains_any(title, ["skinny"]): return "Skinny"
    if contains_any(title, ["wide leg"]): return "Wide Leg"
    if contains_any(title, ["cigarette"]): return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["classic straight-leg", "slim straight", "classic straight fit", "cigarette"]): return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["relaxed straight-leg", "loose", "relaxed straight"]): return "Straight from Thigh"
    if contains_any(desc, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(desc, ["skinny"]): return "Skinny"
    if contains_any(desc, ["flare"]): return "Flare"
    if contains_any(desc, ["bootcut"]): return "Bootcut"
    if contains_any(desc, ["taper"]): return "Tapered"
    if contains_any(desc, ["wide leg", "palazzo"]): return "Wide Leg"
    if contains_any(desc, ["classic straight-leg", "slim straight", "classic straight fit", "cigarette"]): return "Straight from Knee"
    if contains_any(desc, ["relaxed fit with a 90s inspired"]): return "Straight from Thigh"
    if contains_any(desc, ["baggy", "relaxed jean", "relaxed fit", "loose fit"]): return "Baggy"
    if contains_any(tags, ["filter_style_barrel", "barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(tags, ["filter_style_skinny", "filter_style_superskinny", "skinny"]): return "Skinny"
    if contains_any(tags, ["filter_style_flare", "flare"]): return "Flare"
    if contains_any(tags, ["filter_style_boot", "bootcut", "boot"]): return "Bootcut"
    if contains_any(tags, ["taper"]): return "Tapered"
    if contains_any(tags, ["filter_style_wide", "wide leg", "wide", "palazzo"]): return "Wide Leg"
    if contains_any(tags, ["filter_style_cigarette", "cigarette"]): return "Straight from Knee"
    if contains_any(tags, ["straight"]) and contains_any(tags, ["baggy", "relaxed", "loose"]): return "Straight from Thigh"
    if contains_any(tags, ["baggy", "loose", "relaxed"]): return "Baggy"
    return ""


def determine_inseam_style(title: str, handle: str, desc: str, tags: str, jean_style: str, inseam: str) -> str:
    if jean_style in {"Skinny", "Tapered"} and contains_any(title, ["ankle"]): return "Full Length"
    if contains_any(title, ["ankle"]): return "Ankle"
    if contains_any(title, ["crop"]): return "Cropped"
    if jean_style in {"Skinny", "Tapered"} and contains_any(handle, ["ankle"]): return "Full Length"
    if contains_any(handle, ["ankle"]): return "Ankle"
    if contains_any(handle, ["crop"]): return "Cropped"
    if contains_any(handle, ["full length"]): return "Full Length"
    if jean_style in {"Skinny", "Tapered", "Straight from Knee"} and contains_any(desc, ["ankle-grazing", "ankle grazing length", "cropped at the ankle", "drape around the ankle", "hit at the ankle bone", "ankle length", "ankle jean", "tapers to a slim ankle"]): return "Full Length"
    if contains_any(desc, ["ankle-grazing", "ankle grazing length", "cropped at the ankle", "drape around the ankle", "hit at the ankle bone", "ankle length", "ankle jean"]): return "Ankle"
    if contains_any(desc, ["stack stylishly above the ankle", "stack at the ankle", "full length", "full-length", "full length at the ankles", "extended inseam", "stack at the hem", "pooling over"]): return "Full Length"
    if contains_any(desc, ["crop length", "crop-length", "cropped"]): return "Cropped"
    try:
        inseam_num = float(inseam) if inseam else None
    except Exception:
        inseam_num = None
    n = normalize_text(tags)
    if ("full" in n and "ankle" in n) or ("full" in n and "crop" in n and inseam_num and inseam_num >= 30): return "Full Length"
    if "ankle" in n and jean_style in {"Skinny", "Tapered", "Straight from Knee"}: return "Full Length"
    if "full" in n and "ankle" not in n and "crop" not in n: return "Full Length"
    if "ankle" in n and "full" not in n and "crop" not in n: return "Ankle"
    if "crop" in n and "full" not in n and "ankle" not in n: return "Cropped"
    return ""


# Jean Style groupings used by the Inseam Label rules. "Straight from
# Knee/Thigh" and "Boyfriend" are included per the rule table even though
# determine_jean_style() above doesn't currently produce them (it only
# emits "Straight from Knee" and "Straight from Thigh" separately, and
# never "Boyfriend") -- kept for forward-compatibility, effectively
# unreachable today. "Taper" (table spelling) and "Tapered" (this
# scraper's actual output) are both included defensively.
INSEAM_LABEL_WIDE_STYLES = {
    "Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
    "Boyfriend", "Baggy", "Flare", "Straight from Thigh",
}
INSEAM_LABEL_NARROW_STYLES = {"Taper", "Tapered", "Skinny", "Straight from Knee"}


def determine_inseam_label_single(product_title: str, size: str, jean_style: str, inseam: str) -> str:
    """
    Inseam Label rule for styles with only ONE inseam option (no genuine
    Regular/Long split to detect). Per spec:
      1. Product title contains "Petite", or size ends in "P" -> Petite
      2. Wide-leg-family jean style + inseam >= 33 -> Long
      3. Narrow-family jean style + inseam >= 30 -> Long
      4. Otherwise -> Regular
    """
    if contains_any(product_title, ["petite"]) or (size or "").strip().upper().endswith("P"):
        return "Petite"

    if contains_any(product_title, ["long"]) or (size or "").strip().upper().endswith("L"):
        return "Long"

    if contains_any(product_title, ["Tall"]) or (size or "").strip().upper().endswith("T"):
        return "Long"
    try:
        n = float(inseam) if inseam else None
    except (TypeError, ValueError):
        n = None
    if n is not None:
        if jean_style in INSEAM_LABEL_WIDE_STYLES and n >= 33:
            return "Long"
        if jean_style in INSEAM_LABEL_NARROW_STYLES and n >= 30:
            return "Long"
    return "Regular"


def determine_inseam_label_multi(inseam_style: str, jean_style: str, inseam: str) -> str:
    """
    Inseam Label rule for styles with MULTIPLE distinct inseam options
    (a genuine Regular/Long — or Petite/Regular/Long — split exists).
    Returns "" (never guesses) when:
      - inseam is missing/unparseable,
      - jean_style isn't in either defined group, or
      - the (inseam_style, jean_style-group) combination has no rule.
        Narrow styles (Taper/Skinny/Straight from Knee) intentionally have
        no "Ankle" row in the spec -- Carrie confirmed narrow styles should
        only ever resolve to Cropped or Full Length, so this case is
        expected to be unreachable in practice, not a gap to guess around.
    """
    try:
        n = float(inseam) if inseam else None
    except (TypeError, ValueError):
        n = None
    if n is None or not inseam_style:
        return ""

    if jean_style in INSEAM_LABEL_WIDE_STYLES:
        if inseam_style == "Cropped":
            if n <= 25: return "Petite"
            if n <= 27: return "Regular"
            return "Long"
        if inseam_style == "Ankle":
            if 25 < n < 28: return "Petite"
            if 28 <= n < 30: return "Regular"
            if n >= 30: return "Long"
            return ""
        if inseam_style == "Full Length":
            if 28 <= n < 30: return "Petite"
            if 30 <= n < 33: return "Regular"
            if n >= 33: return "Long"
            return ""
        return ""

    if jean_style in INSEAM_LABEL_NARROW_STYLES:
        if inseam_style == "Cropped":
            if n <= 26: return "Petite"
            if n <= 27: return "Regular"
            return "Long"
        if inseam_style == "Full Length":
            if 26 < n <= 27: return "Petite"
            if 27 < n <= 29: return "Regular"
            if n > 29: return "Long"
            return ""
        # Intentionally no "Ankle" rule for narrow styles -- see docstring.
        return ""

    return ""


def apply_style_name_editing_rules(rows: List[Dict[str, Any]]) -> None:
    """
    Style Name editing rules, Step 4 (applied in order, 2026-08-03 spec).
    Mutates `rows` in place.

    Rule 1: consolidate near-duplicate Style Names -- e.g. the brand
    slightly renamed a style over time so it no longer matches its
    siblings. Within products sharing the same first Style-Name word,
    if names disagree, adopt the MOST FREQUENT name for any product
    whose Jean Style matches the dominant Jean Style of that most-
    frequent name's group. Products with "MATERNITY" in the title are
    exempt (their Style Name is never touched by this rule).

    Rule 2: if a Style Name resolved to a single word:
      a. look for other (post-Rule-1) Style Names starting with that
         word whose Jean Style matches this product's -- if found,
         adopt the most common such name;
      b. else if no other Style Name starts with that word, append
         the first word of this product's Jean Style;
      c. else (Jean Style is blank) leave the single word as-is.
      NOTE: if candidates exist in (a) but none share this product's
      Jean Style, we leave the name as-is rather than falling through
      to (b) -- the spec doesn't define that case, and guessing here
      risked bolting an unrelated Jean Style word onto a real match
      that just didn't clear the Jean Style bar.

    Both rules operate once per PRODUCT (Style Id), then the resolved
    name is propagated to every variant row of that product.
    """
    by_product_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_product_id[r["Style Id"]].append(r)
    reps: Dict[str, Dict[str, Any]] = {pid: grp[0] for pid, grp in by_product_id.items()}

    # Rule 1 — only considers MULTI-word names. Single-word names are
    # Rule 2's job; if Rule 1 grouped by first word without this
    # restriction, a genuine single-word style like "FELIX" would get
    # swallowed into a merge with an unrelated "FELIX WIDE" simply for
    # sharing a first word, before Rule 2 ever got a chance to run its
    # own (different) single-word logic on it.
    first_word_groups: Dict[str, List[str]] = defaultdict(list)
    for pid, rep in reps.items():
        sn = rep["Style Name"]
        if sn and " " in sn:
            first_word_groups[sn.split(" ")[0]].append(pid)

    for fw, pids in first_word_groups.items():
        names = [reps[pid]["Style Name"] for pid in pids]
        if len(set(names)) <= 1:
            continue  # already consistent, nothing to consolidate
        most_common_name, _ = Counter(names).most_common(1)[0]
        dominant_js_counts = Counter(
            reps[pid]["Jean Style"] for pid in pids
            if reps[pid]["Style Name"] == most_common_name and reps[pid]["Jean Style"]
        )
        dominant_js = dominant_js_counts.most_common(1)[0][0] if dominant_js_counts else None
        for pid in pids:
            rep = reps[pid]
            if rep["Style Name"] == most_common_name:
                continue
            if "MATERNITY" in (rep["Product"] or "").upper():
                continue
            if dominant_js and rep["Jean Style"] == dominant_js:
                rep["Style Name"] = most_common_name

    # Rule 2 (recompute candidates AFTER Rule 1's consolidation)
    all_names_after_rule1 = [rep["Style Name"] for rep in reps.values() if rep["Style Name"]]
    for pid, rep in reps.items():
        sn = (rep["Style Name"] or "").strip()
        if not sn or " " in sn:
            continue  # not a single word
        candidates = [n for n in all_names_after_rule1 if n != sn and n.split(" ")[0] == sn]
        if candidates:
            candidate_pids = [p for p in reps
                              if reps[p]["Style Name"] in candidates
                              and reps[p]["Jean Style"] == rep["Jean Style"]]
            if candidate_pids:
                matched_names = Counter(reps[p]["Style Name"] for p in candidate_pids)
                rep["Style Name"] = matched_names.most_common(1)[0][0]
            # else: candidates exist but none share this Jean Style --
            # leave as-is (see note above).
        elif rep["Jean Style"]:
            rep["Style Name"] = f"{sn} {rep['Jean Style'].split(' ')[0]}".upper()
        # else: Jean Style blank -> leave the single word as-is.

    for pid, grp in by_product_id.items():
        resolved = reps[pid]["Style Name"]
        for r in grp:
            r["Style Name"] = resolved


def determine_color_standardized(tags: str, color: str) -> str:
    t = normalize_text(f"{tags} {color}")
    if "animal print" in t or "leopard" in t or "snake" in t: return "Animal Print"
    if "colorgroup blue" in t or "filter color blue" in t or "blue" in t: return "Blue"
    if "colorgroup black" in t or "filter color black" in t or "black" in t: return "Black"
    if "white" in t or "ecru" in t: return "White"
    if "grey" in t or "gray" in t: return "Grey"
    if "green" in t: return "Green"
    if "brown" in t or "tan" in t or "beige" in t or "khaki" in t: return "Tan"
    if "red" in t or "wine" in t or "burgundy" in t: return "Red"
    if "pink" in t: return "Pink"
    if "purple" in t: return "Purple"
    if "yellow" in t: return "Yellow"
    return ""


def determine_stretch(desc: str) -> str:
    if contains_any(desc, ["rigid"]): return "Rigid"
    if contains_any(desc, ["super stretch"]): return "High Stretch"
    if contains_any(desc, ["hint of stretch"]): return "Low Stretch"
    if contains_any(desc, ["stretch", "some stretch"]): return "Medium Stretch"
    return ""


def main() -> None:
    configure_logging()
    products_by_handle: Dict[str, dict] = {}
    json_handles: set[str] = set()
    for c in COLLECTION_HANDLES:
        for p in fetch_collection_products_graphql(c):
            h = str(p.get("handle") or "")
            if h:
                products_by_handle[h] = p
        json_handles.update(fetch_collection_handles_json(c))

    missing = sorted(h for h in json_handles if h and h not in products_by_handle)
    for h in missing:
        p = fetch_product_by_handle(h)
        if p:
            products_by_handle[h] = p

    rows: List[Dict[str, Any]] = []
    for handle, product in products_by_handle.items():
        title = str(product.get("title") or "")
        ptype = str(product.get("productType") or "")
        if is_filtered(handle, title, ptype):
            continue

        graph_desc = str(product.get("description") or "")
        pdp = parse_pdp_details(handle)
        details_text = pdp.get("details_text") or ""
        rise_by_size = pdp.get("rise_map") or {}
        inseam_by_size = pdp.get("inseam_map") or {}
        inseam_defaults = {v for v in inseam_by_size.values() if v}
        inseam_default = next(iter(inseam_defaults)) if len(inseam_defaults) == 1 else ""
        inseam_mode = ""
        if inseam_by_size:
            counts = Counter(v for v in inseam_by_size.values() if v)
            if counts:
                mode_value, mode_count = counts.most_common(1)[0]
                if mode_count / max(1, sum(counts.values())) >= 0.7:
                    inseam_mode = mode_value
        description = " ".join([x for x in [graph_desc, details_text] if x]).strip()
        tags = ", ".join(product.get("tags") or [])
        style = style_name(title)
        rise = parse_rise(description)
        for variant in ((product.get("variants") or {}).get("nodes") or []):
            option1 = option_value(variant, ["color", "option1"]) or ""
            option2 = option_value(variant, ["size", "option2"]) or ""
            option3 = option_value(variant, ["inseam", "length", "option3"]) or ""
            option2_key = normalize_size_token(option2)
            inseam = parse_inseam(description, option3)
            if option2_key in inseam_by_size:
                inseam = inseam_by_size[option2_key]
            elif inseam_default:
                inseam = inseam_default
            if inseam_mode:
                inseam = inseam_mode
            jean_style = determine_jean_style(title, description, tags)
            inseam_style = determine_inseam_style(title, handle, description, tags, jean_style, inseam)
            rise_label = determine_rise_label(title, handle, description, tags)
            sku_brand = str(variant.get("sku") or "")
            rise_for_row = rise_by_size.get(option2_key, rise)
            row = {
                "Style Id": strip_gid(str(product.get("id") or ""), "gid://shopify/Product/"),
                "Handle": handle,
                "Published At": format_date(product.get("publishedAt")),
                "Created At": format_date(product.get("createdAt")),
                "Product": title.upper(),
                "Style Name": style,
                "Product Type": ptype.title() if ptype else "",
                "Tags": tags,
                "Vendor": product.get("vendor") or "",
                "Description": description,
                "Variant Title": f"{title.split('|')[0].strip().upper()} / {str(variant.get('title') or '').replace(' / ', ' / ')}".strip(" /"),
                "Color": option1,
                "Size": option2,
                "Rise": rise_for_row,
                "Inseam": inseam,
                "Price": to_price((variant.get("price") or {}).get("amount")),
                "Compare at Price": to_price((variant.get("compareAtPrice") or {}).get("amount")),
                "Available for Sale": str(bool(variant.get("availableForSale"))).upper(),
                "Quantity Available": variant.get("quantityAvailable") if variant.get("quantityAvailable") is not None else "",
                "Quantity of style": product.get("totalInventory") if product.get("totalInventory") is not None else "",
                "SKU - Shopify": strip_gid(str(variant.get("id") or ""), "gid://shopify/ProductVariant/"),
                "SKU - Brand": sku_brand,
                "Barcode": variant.get("barcode") or "",
                "Image URL": ((product.get("featuredImage") or {}).get("url") or ""),
                "SKU URL": product.get("onlineStoreUrl") or f"https://staud.clothing/products/{handle}",
                "Jean Style": jean_style,
                "Inseam Style": inseam_style,
                "Inseam Label": "",  # computed later, after Inseam Style gaps are filled
                "Rise Label": rise_label,
                "Color - Standardized": determine_color_standardized(tags, option1),
                "Stretch": determine_stretch(description),
            }
            rows.append(row)

    apply_style_name_editing_rules(rows)

    # style-level infer for missing rise/inseam style
    by_style: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_style[r["Style Name"]].append(r)
    for r in rows:
        if not r["Inseam Style"]:
            peers = [x for x in by_style[r["Style Name"]] if x["Inseam Style"] and x["Inseam"] == r["Inseam"]]
            if peers and len({x["Inseam Style"] for x in peers}) == 1:
                r["Inseam Style"] = peers[0]["Inseam Style"]
        if not r["Rise Label"]:
            peers = [x for x in by_style[r["Style Name"]] if x["Rise Label"] and x["Rise"] == r["Rise"]]
            if peers and len({x["Rise Label"] for x in peers}) == 1:
                r["Rise Label"] = peers[0]["Rise Label"]

    # fill remaining gaps for required downstream columns
    by_style_size_rise: Dict[tuple[str, str], str] = {}
    for r in rows:
        key = (r["Style Name"], r["Size"])
        if r["Rise"] and key not in by_style_size_rise:
            by_style_size_rise[key] = r["Rise"]

    # Determine, per style, how many DISTINCT inseam values genuinely
    # exist -- this decides which Inseam Label rule set applies (single-
    # inseam styles use title/size/jean-style heuristics; multi-inseam
    # styles use the Inseam Style + Inseam-value table).
    style_inseam_values: Dict[str, set] = defaultdict(set)
    for r in rows:
        try:
            v = float(r["Inseam"]) if r["Inseam"] else None
        except (TypeError, ValueError):
            v = None
        if v is not None:
            style_inseam_values[r["Style Name"]].add(v)

    for r in rows:
        if not r["Rise"]:
            r["Rise"] = by_style_size_rise.get((r["Style Name"], r["Size"]), "")
        if not r["Inseam Style"]:
            try:
                inseam_num = float(r["Inseam"]) if r["Inseam"] else None
            except Exception:
                inseam_num = None
            if inseam_num is not None:
                if inseam_num >= 30:
                    r["Inseam Style"] = "Full Length"
                elif inseam_num <= 28:
                    r["Inseam Style"] = "Cropped"
                else:
                    r["Inseam Style"] = "Ankle"
        if not r["Compare at Price"]:
            r["Compare at Price"] = r["Price"]
        if len(style_inseam_values.get(r["Style Name"], set())) > 1:
            r["Inseam Label"] = determine_inseam_label_multi(r["Inseam Style"], r["Jean Style"], r["Inseam"])
        else:
            r["Inseam Label"] = determine_inseam_label_single(r["Product"], r["Size"], r["Jean Style"], r["Inseam"])

    # dedupe
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for r in rows:
        key = (r["Handle"], r["SKU - Shopify"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = OUTPUT_DIR / f"{BRAND}_{ts}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(deduped)
    logging.info("Rows written: %s", len(deduped))
    logging.info("CSV written: %s", out)


if __name__ == "__main__":
    main()
