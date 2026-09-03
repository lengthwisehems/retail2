#!/usr/bin/env python3
"""Inventory exporter for AG Jeans (Storefront GraphQL + Constructor + PDP HTML)."""
from __future__ import annotations

import csv
import json
import logging
import re
import time
import unicodedata
from collections import Counter
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/json,*/*",
})
SESSION.verify = False
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
BRAND = "AGJEANS"
LOG_PATH = BASE_DIR / f"{BRAND}_run.log"

HOST_ROTATION = [
    "https://www.agjeans.com",
    "https://agjeans.com",
    "https://agjeans-store.myshopify.com",
]
GRAPHQL_HEADERS = {
    "X-Shopify-Storefront-Access-Token": "ffae8e47a84566aa6fa059dfc56c7c56",
    "Content-Type": "application/json",
}
COLLECTION_HANDLES = ["womens-denim", "womens-sale"]
CONSTRUCTOR_API_KEY = "key_Ai9lmSZcQbh1bfYa"
CONSTRUCTOR_CLIENT_ID = "124bb124-e8d8-444b-9186-eaae4100af9f"
CONSTRUCTOR_SESSION = "20"
CONSTRUCTOR_RESULTS_PER_PAGE = 26

# Swym engagement (wishlist counts)
SWYM_PID       = "SI9v3T9CMo9ezO5IZeTm2QqzgPDs0rp7a35myMnO1vU="
SWYM_API_BASE  = "https://swymstore-v3pro-01.swymrelay.com"
SWYM_STORE_URL = "https://www.agjeans.com/"
ET_WISHLIST    = 4

# PDP politeness — AG rate-limits aggressively, so keep a floor delay between
# PDP hits and back off hard on 429 instead of hammering through the retries.
PDP_DELAY        = 0.6
PDP_MAX_RETRIES  = 5
PDP_BACKOFF_BASE = 2.0
PDP_429_SLEEP    = 15.0

CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Updated At",
    "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size",
    "Rise", "Knee", "Inseam", "Leg Opening",
    "Price", "Compare at Price", "Available for Sale",
    "Quantity Available", "Quantity Available (Online)",
    "Quantity Available (Instore Inventory)",
    "Quantity of style", "Wishlist Count",
    "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL",
    "Jean Style", "Hem Style", "Inseam Label", "Inseam Style", "Rise Label",
    "Color - Simplified", "Color - Standardized", "Fabric Source", "Stretch",
]

# ---------------------------------------------------------------------------
# Filter words — product dropped if title OR productType/filtercategory OR
# handle matches any of these. Applied before any PDP is visited.
# ---------------------------------------------------------------------------
FILTER_WORDS: List[str] = [
    "Accessories", "Accessory", "Bermuda", "Bermudas", "Blazer", "Blazers",
    "Blouse", "Blouses", "Bodysuit", "Bodysuits", "Button Up", "Button-Up",
    "Capri", "Cardigan", "Cardigans", "Clothing Top", "Clothing Tops", "Coat",
    "Coats", "Coats & Jackets", "Core Handbags", "Corset", "Corsets",
    "Crop Top", "Crop Tops", "Denim Short", "Denim Shorts", "Donation",
    "Dress", "Dresses", "Fashion Core Handbag", "Fashion Core Handbags",
    "Fashion Handbag", "Fashion Handbags", "Gift Wrap", "Goodies Accessories",
    "Goodies Accessory", "Handbag", "Heel", "Heels", "Hoodie", "Hoodies",
    "Jacket", "Jackets", "Jogger Short", "Jogger Shorts", "Jort", "Jumpsuit",
    "Jumpsuits", "Neck", "One Piece", "One Pieces", "One-Piece", "One-Pieces",
    "Outerwear", "Pant Suit", "Pant Suits", "Purse", "Romper", "Rompers",
    "Sandel", "Sandle", "Shacket", "Shipping", "Shipping Protection", "Shirt",
    "Shirts", "Shirts & Tops", "Shoe", "Shoes", "Short", "Shorts", "Skirt",
    "Skirts", "Sleeve", "Sleeves", "Suit", "Suits", "Sweat", "Sweater",
    "Sweaters", "Sweatpant", "Sweatpants", "Sweats", "Sweatshirt",
    "Sweatshirts", "Swim", "T Shirt", "T Shirts", "Tank", "Tank Tops", "Tee",
    "Tees", "Top", "Tops", "Tote", "Trench", "T-shirt", "T-Shirts", "Vest",
    "Vests",
]

# ---------------------------------------------------------------------------
# Style Name — styling words stripped from the cleaned handle
# ---------------------------------------------------------------------------
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "1999", "360", "5-Pocket", "Accent Hardware", "Accent", "Ag Cloud Soft",
    "Ag ED", "AGed", "AG-ed", "AG-ed™", "Ankle", "Art Project", "Beaded",
    "Bee's Knees", "Belted", "Braided", "Button", "Cadet", "Capsule", "Cargo",
    "Carpenter", "Chap", "Checkered", "Chew", "Cinched", "Cloud Soft",
    "Coated", "Collection", "Color block", "colorblock", "Constructed",
    "Contrast", "Corduroy", "Crochet", "Crop", "Cropped", "Crushed",
    "Crystal", "Cuff", "Cuffed", "Cutoff", "Cut-Out", "Darted", "Denim",
    "Destroyed", "detail", "Diamond Cut", "Diamond", "Distressed",
    "Double Flood", "Double Heel", "Double Prep", "Double Sneak", "Drawn",
    "Drawstring", "Earth Day", "Elastic Waist", "Embellished",
    "Embellishment", "Embroidery", "Exposed", "Extended", "Faux", "Fisherman",
    "Fit", "Flag", "Flannel", "Flap Pocket", "Flap", "Flip", "Flocked",
    "Flood", "Floral", "Fray", "Frayed Seam", "Fringe", "Front Yoke",
    "Frontier", "Graffitimetalik", "Hardware", "Heel", "Hem", "Heyday",
    "High Rise", "high waist", "High Waisted", "High-Rise", "high-waist",
    "high-waisted", "Hover cuff", "Hover", "Inch", "Inset",
    "Jean W/ Slit Hem", "Jean", "Krushed", "Krystal", "Leather", "Leatherette", "Leg",
    "Lightweight", "Lil", "Linen", "linnen", "Lo", "Long", "low and loose",
    "Low Rise", "Low Slung", "Low Waised", "low waist", "low waisted",
    "Low-Rise", "low-slung", "low-waist", "low-waisted", "Mid Rise",
    "Mid Waisted", "Mid-Rise", "Moto", "Ms.", "Nacho", "Nerdy", "Panel",
    "Paneled", "panelled", "Panneled", "Pant", "Pants", "Patch Pocket",
    "Patch", "Patchwork", "petite", "PETITES The Lil", "Pintucked", "Plaid",
    "Pleated", "Pleaty", "Plus", "Pocket Pant", "Pocket", "Poplin", "Prep",
    "Pressed", "Print", "Printed", "Pull On", "Raw Hem", "Raw", "Regular",
    "Renaissance", "Repair", "Retro", "Rinse", "Ripped", "Rolled Hem",
    "Rolled", "Saddle", "Sailor", "Seam", "Seamed Front Yoke", "Seamed",
    "Selvage", "Selvedge", "Sequin", "Side Seam Snaps", "Side Zip", "Silk",
    "Skimp", "Slice", "Slit", "Slouchy", "SNACKS!", "Snake Print", "Snake",
    "Snap", "Sneak", "Sneaker Length", "Sott", "Spark", "SPARKLE", "Splatter",
    "Spliced", "Split", "Stacked Waist", "Stacked", "Step Fray", "Stitched",
    "Stone", "Stoned", "Stripe", "striped", "Studded", "Stunner Zip", "Suede",
    "super high rise", "super high waist", "super high-rise",
    "super high-waist", "super low rise", "super low waist", "super low-rise",
    "super low-waist", "Swisher", "Tailored", "Tall", "The Laced Up",
    "The Side Zip Slung", "The", "Track Pant", "Trashed", "Trim",
    "Trouser Jean", "Trouser", "Tune Up", "Tux", "Twill", "Twisted", "Ultra",
    "ultra high rise", "ultra high waist", "ultra high-rise",
    "ultra high-waist", "ultra low rise", "ultra low waist", "ultra low-rise",
    "ultra low-waist", "Utility", "Vapor", "Vegan Leather", "vegan", "Velvet",
    "Vent", "V-High Rise", "Vintage", "W/ Contrast Front Panel", "w/ Cuff",
    "w/ Flap Jean", "w/ Slit Hem", "W/ Stud Detailing", "W/ Wide Cuff",
    "W/Flap", "Wash", "Wax", "Welt Pocket", "Wide Hem", "With Cuff",
    "With Frayed Seam", "With", "Wool", "Zip", "Zipper",
]

# Handle phrases that are their own style name, so the words inside them are
# not treated as repeats of the same word appearing loose elsewhere in the
# handle ("ex-boyfriend" is distinct from a later bare "boyfriend").
PROTECTED_COMPOUNDS: List[str] = ["ex boyfriend"]

# ---------------------------------------------------------------------------
# Rise Label phrase lists — description (fallback 1) and tags (fallback 2)
# ---------------------------------------------------------------------------
RISE_DESC_ULTRA_LOW: List[str] = [
    "Rise: Super Low", "Rise: Ultra Low", "Rise - Super Low",
    "Rise - Ultra Low", "super low rise", "super low-rise", "ultra low rise",
    "ultra low-rise", "super low waist", "super low-waist", "ultra low waist",
    "ultra low-waist",
]
RISE_DESC_ULTRA_HIGH: List[str] = [
    "Rise: Super High", "Rise: Ultra High", "Rise - Super High",
    "Rise - Ultra High", "super high rise", "super high-rise",
    "ultra high rise", "ultra high-rise", "super high waist",
    "super high-waist", "ultra high waist", "ultra high-waist",
]
RISE_DESC_MID: List[str] = [
    "Rise: Mid", "Rise - Mid", "Mid-Rise", "Mid Rise", "Mid waist",
]
RISE_DESC_LOW: List[str] = [
    "Rise: Low", "Rise - Low", "Low-Rise", "Low Rise", "hip-hugging fit",
    "sit comfortably on your hips", "low on the hip", "low on the waist",
    "Low waist", "low slung",
]
RISE_DESC_HIGH: List[str] = [
    "Rise: High", "Rise - High", "High-Rise", "High Rise", "High Waist",
    "High-Waist", "High Waisted", "High-Waisted", "high on the hip",
    "high on the waist",
]
RISE_TAG_RULES: List[Tuple[str, List[str]]] = [
    ("Ultra Low",  ["Rise: Super Low", "Rise: Super-Low", "Rise: Ultra Low",
                    "Rise: Ultra-Low"]),
    ("Ultra High", ["Rise: Super High", "Rise: Super-High",
                    "Rise: Ultra Rise", "Rise: Ultra-High"]),
    ("High",       ["Rise: High", "Rise: High Rise", "Rise: High-Rise"]),
    ("Mid",        ["Rise: Mid", "Rise: Mid Rise", "Rise: Mid-Rise"]),
    ("Low",        ["Rise: Low", "Rise: Low Rise", "Rise: Low-Rise"]),
]

# ---------------------------------------------------------------------------
# Product build (PT1) keyword lists — Steps 5-10
# ---------------------------------------------------------------------------
PT_RISE_KEYWORDS: List[str] = [
    "super low rise", "super low-rise", "ultra low rise", "ultra low-rise",
    "super low waist", "super low-waist", "ultra low waist",
    "ultra low-waist", "super high rise", "super high-rise",
    "ultra high rise", "ultra high-rise", "super high waist",
    "super high-waist", "ultra high waist", "ultra high-waist", "mid-rise",
    "mid rise", "low-rise", "Low Rise", "low slung", "low-slung", "low waist",
    "low waisted", "low-waist", "low-waisted", "high-rise", "high rise",
    "high waist", "high waisted", "high-waist", "high-waisted",
]

PT_JEAN_STYLE_KEYWORDS: List[str] = [
    "Relaxed", "Slim", "Kick", "Ultra", "Super", "culotte", "Wide", "Flare",
    "barrel", "barrell", "bow", "bowed", "Boyfriend", "Cigarette", "bootcut",
    "boot-cut", "horseshoe", "mom", "palazzo", "stovepipe", "stove-pipe",
    "straight", "Skinny", "Slouchy", "Baggy", "tapered", "taper", "Utility",
    "leg", "fit",
]

PT_INSEAM_STYLE_KEYWORDS_1 = ["Ankle", "Stacked", "Crop", "Flood", "Hover",
                              "Sneaker Length"]
PT_INSEAM_STYLE_KEYWORDS_2 = ["Ankle", "Stacked", "Cropped", "Flood", "Hover",
                              "Sneaker Length"]
PT_INSEAM_LABEL_KEYWORDS = ["Petite", "Extended", "Long", "Tall", "Regular"]
PT_TYPE_KEYWORDS = ["Jean", "Trouser", "Pant"]

PT_STYLING_KEYWORDS: List[str] = [
    "With", "360", "Inset", "Panel", "paneled", "panelled", "Panneled",
    "Colorblock", "Color Block", "Embellished", "Studded", "Patchwork",
    "Pleated", "Vapor", "Seam", "Flocked", "Crushed", "5-Pocket", "Beaded",
    "Braided", "Button", "Contrast", "Crystal", "Diamond", "Embroidery",
    "Flap", "Flip", "Splatter", "Repair", "Cut-Out", "Distressed",
    "Destroyed", "Patch", "Ripped", "Frayed Seam", "Krushed", "Krystal",
    "Pocket", "Pintucked", "Retro", "Saddle", "Sequin", "Snap", "Spark",
    "Stitched", "Stone", "Stoned", "Fisherman", "Twisted", "Tailored",
    "Pressed", "Constructed", "Carpenter", "Sailor", "Moto", "Front Yoke",
    "Chap", "Side Zip", "Zipper", "detail", "vintage", "Rinse", "Wash",
    "Soft", "trim", "Fringe", "Cut", "Accent", "Hardware", "AGed", "Ag ed",
    "Ag-ed", "AG-ed™", "Art Project", "Earth Day", "Collection", "Capsule",
    "Coated", "vegan", "linnen", "linen", "denim", "Ag Cloud Soft",
    "Corduroy", "Silk", "Twill", "Velvet", "Selvage", "Leather", "Leatherette", "Wool",
    "Flannel", "Crochet", "Faux", "Lightweight", "Plaid", "Poplin", "Wax",
    "Suede", "Floral", "Flag", "Stripe", "Checkered", "Printed", "Snake",
    "Print", "Cinched", "Cargo", "Cadet", "pull on", "Belted",
    "Elastic Waist", "Drawstring", "Spliced", "Slice", "Split", "Cuffed",
    "Chew", "Cutoff", "Darted", "Exposed", "Fray", "Slit", "Raw", "Rolled",
    "Wide Hem", "Hem",
]

NON_TAPER_STYLES = {"Straight from Knee/Thigh", "Bootcut", "Wide Leg",
                    "Boyfriend", "Barrel", "Baggy", "Flare",
                    "Straight from Thigh"}
TAPER_STYLES = {"Taper", "Tapered", "Skinny", "Straight from Knee"}

GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 100, after: $cursor) {
      nodes {
        id handle title productType tags vendor description
        publishedAt createdAt updatedAt totalInventory onlineStoreUrl
        featuredImage { url }
        images(first: 30) { nodes { url } }
        variants(first: 100) {
          nodes {
            id title sku barcode availableForSale quantityAvailable
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


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------
def configure_logging() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    try:
        handlers = [logging.FileHandler(LOG_PATH, encoding="utf-8"),
                    logging.StreamHandler()]
    except OSError:
        fallback = OUTPUT_DIR / f"{BRAND}_run.log"
        handlers = [logging.FileHandler(fallback, encoding="utf-8"),
                    logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=handlers)


def normalize_text(text: str) -> str:
    value = (text or "").lower().replace("-", " ")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def contains_any(text: str, phrases: Sequence[str]) -> bool:
    normalized = normalize_text(text)
    return any(normalize_text(p) in normalized for p in phrases)


def find_word(text: str, word: str) -> bool:
    n = normalize_text(text)
    w = normalize_text(word)
    return bool(re.search(rf"(^|\s){re.escape(w)}(\s|$)", n))


def sanitize_text(raw: str) -> str:
    """Straighten quotes, drop registration marks, strip accents."""
    if not raw:
        return ""
    s = (raw.replace("‘", "'").replace("’", "'")
            .replace("“", '"').replace("”", '"')
            .replace("–", "-").replace("—", "-"))
    s = s.replace("®", "").replace("™", "").replace("℗", "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", s).strip()


def clean_description_text(raw: str) -> str:
    """Sanitize, then AG-ed -> AGed, then de-hyphenate tight hyphens."""
    s = sanitize_text(raw)
    s = re.sub(r"AG-ed", "AGed", s, flags=re.IGNORECASE)
    # Replace hyphens that have no space on either side with a space
    s = re.sub(r"(?<=\S)-(?=\S)", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return value


def strip_gid(value: str, prefix: str) -> str:
    if not value:
        return ""
    return value.replace(prefix, "") if value.startswith(prefix) else value.split("/")[-1]


def parse_number_with_fraction(raw: str) -> str:
    text = (raw or "").replace('"', "").strip()
    if not text:
        return ""
    tokens = [t for t in re.split(r"\s+", text) if t]
    total = 0.0
    for token in tokens:
        try:
            total += float(Fraction(token)) if "/" in token else float(token)
        except (ValueError, ZeroDivisionError):
            return ""
    out = f"{total:.4f}".rstrip("0").rstrip(".")
    return out if "." in out else f"{out}.0"


def extract_measurement(description_text: str, labels: Sequence[str]) -> str:
    text = description_text or ""
    for label in labels:
        pattern = rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?)"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            return parse_number_with_fraction(m.group(1))
    return ""


def to_float(value) -> Optional[float]:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def to_price(value: Optional[str]) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"${float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def clean_tags(tags: Any) -> str:
    if isinstance(tags, list):
        return ", ".join(str(t) for t in tags)
    return str(tags or "")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
def request_with_rotation(session, path, *, method="GET", headers=None,
                          json_payload=None, params=None, timeout=40):
    last_error: Optional[Exception] = None
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/{path.lstrip('/')}"
        try:
            if method.upper() == "POST":
                response = session.post(url, headers=headers, json=json_payload,
                                        params=params, timeout=timeout)
            else:
                response = session.get(url, headers=headers, params=params,
                                       timeout=timeout)
            if response.status_code in {404, 410}:
                continue
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logging.warning("Request failed for %s: %s", url, exc)
            time.sleep(0.5)
    raise RuntimeError(f"Unable to request {path}: {last_error}")


def fetch_graphql_products(session, handle: str) -> List[dict]:
    products: List[dict] = []
    cursor: Optional[str] = None
    while True:
        payload = {"query": GRAPHQL_QUERY,
                   "variables": {"handle": handle, "cursor": cursor}}
        resp = request_with_rotation(session, "/api/unstable/graphql.json",
                                     method="POST", headers=GRAPHQL_HEADERS,
                                     json_payload=payload)
        data = resp.json()
        if data.get("errors"):
            logging.warning("GraphQL errors for %s: %s", handle, data["errors"])
        node = (((data.get("data") or {}).get("collection") or {})
                .get("products") or {})
        batch = node.get("nodes") or []
        products.extend(batch)
        page_info = node.get("pageInfo") or {}
        logging.info("GraphQL %s fetched %s products (total %s)", handle,
                     len(batch), len(products))
        if not page_info.get("hasNextPage") or not page_info.get("endCursor"):
            break
        cursor = page_info["endCursor"]
    return products


def fetch_constructor_collection(session, group_id: str) -> List[dict]:
    out: List[dict] = []
    page = 1
    while True:
        params = {"key": CONSTRUCTOR_API_KEY, "i": CONSTRUCTOR_CLIENT_ID,
                  "s": CONSTRUCTOR_SESSION,
                  "num_results_per_page": CONSTRUCTOR_RESULTS_PER_PAGE,
                  "page": page}
        try:
            resp = session.get(f"https://ac.cnstrc.com/browse/group_id/{group_id}",
                               params=params, timeout=40)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logging.warning("Constructor %s page %s failed: %s", group_id, page, exc)
            break
        response = (resp.json().get("response") or {})
        results = response.get("results") or []
        if not results:
            break
        out.extend(results)
        total = int(response.get("total_num_results") or 0)
        if page * CONSTRUCTOR_RESULTS_PER_PAGE >= total:
            break
        page += 1
    logging.info("Constructor %s fetched %s results", group_id, len(out))
    return out


def build_constructor_maps(results: Iterable[dict]):
    by_handle: Dict[str, dict] = {}
    by_variant_id: Dict[str, dict] = {}
    by_sku: Dict[str, dict] = {}
    for result in results:
        data = result.get("data") or {}
        handle = data.get("handle") or ""
        if handle:
            by_handle[handle] = result
        for var in result.get("variations") or []:
            vdata = var.get("data") or {}
            variation_id = str(vdata.get("variation_id") or "")
            sku = str(vdata.get("sku") or "").upper().strip()
            if variation_id:
                by_variant_id[variation_id] = {"result": result, "variation": var}
            if sku:
                by_sku[sku] = {"result": result, "variation": var}
    return by_handle, by_variant_id, by_sku


# ---------------------------------------------------------------------------
# PDP fetching — throttled, with explicit 429 backoff
# ---------------------------------------------------------------------------
def get_pdp_html(session, handle: str) -> str:
    """Fetch a PDP, rotating hosts and backing off properly on 429."""
    delay = PDP_BACKOFF_BASE
    for attempt in range(1, PDP_MAX_RETRIES + 1):
        for host in HOST_ROTATION:
            url = f"{host.rstrip('/')}/products/{handle}"
            try:
                resp = session.get(url, timeout=40)
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After")
                    wait = float(retry_after) if retry_after and retry_after.isdigit() else PDP_429_SLEEP
                    logging.warning("429 for %s; sleeping %.1fs", url, wait)
                    time.sleep(wait)
                    continue
                if resp.status_code in {404, 410}:
                    continue
                if resp.status_code in {500, 502, 503, 504}:
                    raise requests.HTTPError(f"status {resp.status_code}")
                resp.raise_for_status()
                return resp.text
            except Exception as exc:  # noqa: BLE001
                logging.warning("PDP fetch failed %s (attempt %s): %s", url,
                                attempt, exc)
        time.sleep(delay)
        delay *= 2
    return ""


def parse_pdp(html: str) -> dict:
    """Description (tab children 2-4) + measurements + stretch."""
    blank = {"description": "", "rise": "", "knee": "", "inseam": "",
             "leg_opening": "", "stretch": ""}
    if not html:
        return blank
    soup = BeautifulSoup(html, "html.parser")

    info = soup.select_one("div.product-main__info")
    block = None
    if info:
        block = info.select_one("div.tab__wrapper") or info
    if block is None:
        block = soup.select_one("div.tab__wrapper")
    if block is None:
        return blank

    children = [c for c in block.find_all(recursive=False)]
    # Child 1 is the tab header row; children 2-4 carry description,
    # measurements and fabric.
    parts = []
    for child in children[1:4]:
        text = re.sub(r"\s+", " ", child.get_text(" ", strip=True)).strip()
        if text:
            parts.append(text)
    description = clean_description_text(" ".join(parts))

    measure_text = " ".join(parts)
    rise = extract_measurement(measure_text, ["Front Rise", "Rise"])
    knee = extract_measurement(measure_text, ["Knee Opening", "Knee"])
    inseam = extract_measurement(measure_text, ["Inseam"])
    leg_opening = extract_measurement(measure_text, ["Bottom Opening",
                                                    "Leg Opening", "Bottom"])

    stretch = ""
    stretch_el = soup.select_one(".stretch-icons .icon-wrapper.active span")
    if stretch_el:
        raw = stretch_el.get_text(" ", strip=True)
        for phrases, label in [
            (["non stretch"], "Rigid"),
            (["comfort stretch"], "Low Stretch"),
            (["power stretch"], "Medium Stretch"),
            (["super stretch"], "Medium to High Stretch"),
            (["ultimate stretch"], "High Stretch"),
        ]:
            if contains_any(raw, phrases):
                stretch = label
                break
    return {"description": description, "rise": rise, "knee": knee,
            "inseam": inseam, "leg_opening": leg_opening, "stretch": stretch}


def fetch_pdp_cache(session, handles: Iterable[str]) -> Dict[str, dict]:
    cache: Dict[str, dict] = {}
    unique = sorted({h for h in handles if h})
    total = len(unique)
    for i, handle in enumerate(unique, start=1):
        cache[handle] = parse_pdp(get_pdp_html(session, handle))
        time.sleep(PDP_DELAY)
        if i % 25 == 0 or i == total:
            logging.info("PDP fetched %s/%s", i, total)
    return cache


def fetch_wishlist_counts(session, products: Dict[str, dict]) -> Dict[str, int]:
    """Swym public social-count / eventcount, keyed by Shopify product id."""
    counts: Dict[str, int] = {}
    for handle, product in products.items():
        empi = strip_gid(product.get("id") or "", "gid://shopify/Product/")
        if not empi:
            continue
        du = f"{SWYM_STORE_URL}/products/{handle}"
        value = 0
        try:
            r = session.get(f"{SWYM_API_BASE}/api/v3/product/social-count",
                            params={"pid": SWYM_PID, "du": du, "empi": empi,
                                    "topic": "addToWishlist"},
                            verify=False, timeout=15)
            if r.ok:
                value = int((r.json().get("data") or {}).get("count") or 0)
        except Exception:  # noqa: BLE001
            value = 0
        if not value:
            try:
                r = session.get(f"{SWYM_API_BASE}/api/v2/provider/eventcount",
                                params={"pid": SWYM_PID, "du": du,
                                        "et": ET_WISHLIST, "empi": empi},
                                verify=False, timeout=15)
                if r.ok:
                    value = int(r.json().get("count") or 0)
            except Exception:  # noqa: BLE001
                value = 0
        counts[empi] = value
        time.sleep(0.1)
    logging.info("Swym wishlist counts fetched for %s products", len(counts))
    return counts


# ---------------------------------------------------------------------------
# Product — Steps 1-13
# ---------------------------------------------------------------------------
def clean_title_step1(title: str) -> str:
    """Only the first matching rule applies."""
    t = sanitize_text(title)
    if "Boot " in t:
        return re.sub(r"\s+", " ", t.replace("Boot ", "Bootcut ")).strip()
    if "-" in t:
        return re.sub(r"\s+", " ", t.replace("-", " ")).strip()
    if "°" in t:
        return re.sub(r"\s+", " ", t.replace("°", "")).strip()
    return t


def clean_title_v(title: str) -> str:
    """Steps 2/3 — eight rules, first match wins."""
    t = sanitize_text(title)
    if not t:
        return ""
    # "Boot" must match as a whole word in rules 1-4, otherwise a subtitle that
    # already reads "Low-Rise Bootcut" matches the "-Rise Boot" rule and becomes
    # "Low Rise Bootcutcut", which no longer matches the Bootcut keyword search.
    if re.search(r"-Rise\s+Boot\s+Cut\b", t, re.IGNORECASE):
        return re.sub(r"\s+", " ", re.sub(r"-Rise\s+Boot\s+Cut\b", " Rise Bootcut",
                                          t, flags=re.IGNORECASE)).strip()
    if re.search(r"-Rise\s+Boot\b", t, re.IGNORECASE):
        return re.sub(r"\s+", " ", re.sub(r"-Rise\s+Boot\b", " Rise Bootcut",
                                          t, flags=re.IGNORECASE)).strip()
    if re.search(r"\bBoot\s+Cut\b", t, re.IGNORECASE):
        return re.sub(r"\s+", " ", re.sub(r"\bBoot\s+Cut\b", "Bootcut", t,
                                          flags=re.IGNORECASE)).strip()
    if re.search(r"\bBoot\b", t, re.IGNORECASE):
        return re.sub(r"\s+", " ", re.sub(r"\bBoot\b", "Bootcut", t,
                                          flags=re.IGNORECASE)).strip()
    if "AG-ed" in t:
        return re.sub(r"\s+", " ", t.replace("AG-ed", "AGed")).strip()
    if "-" in t:
        return re.sub(r"\s+", " ", t.replace("-", " ")).strip()
    if "°" in t:
        return re.sub(r"\s+", " ", t.replace("°", "")).strip()
    if t.endswith("Boot"):
        return re.sub(r"\s+", " ", t[:-4] + "Bootcut ").strip() + " "
    return t


def build_pt1(title_clean: str, v2_clean: str, v3_clean: str) -> str:
    """Step 4 — v3 + v2 + whatever trails v3 inside the cleaned title."""
    leftover = ""
    if v3_clean:
        idx = title_clean.lower().find(v3_clean.lower())
        if idx >= 0:
            leftover = title_clean[idx + len(v3_clean):]
        else:
            first = v3_clean.split()[0] if v3_clean.split() else ""
            if first:
                m = re.search(rf"\b{re.escape(first)}\b", title_clean, re.IGNORECASE)
                if m:
                    leftover = title_clean[m.end():]
    combined = f"{v3_clean} {v2_clean} {leftover}"
    seen: set[str] = set()
    words: List[str] = []
    for word in combined.split():
        key = re.sub(r"[^a-z0-9]", "", word.lower())
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        words.append(word)
    return " ".join(words)


def _match_keyword(pt1: str, keyword: str) -> str:
    """Return the keyword as it appears in PT1 (preserving PT1's casing)."""
    pattern = r"\b" + re.escape(keyword).replace(r"\ ", r"\s+") + r"\b"
    m = re.search(pattern, pt1, flags=re.IGNORECASE)
    return m.group(0) if m else ""


def first_keyword(pt1: str, keywords: Sequence[str]) -> str:
    for kw in keywords:
        hit = _match_keyword(pt1, kw)
        if hit:
            return hit
    return ""


def all_keywords(pt1: str, keywords: Sequence[str]) -> str:
    """Join every match in keyword-list order.

    Matching runs longest-first so a longer keyword claims its span before a
    shorter one nested inside it ('Paneled' wins over 'Panel'), but the output
    is emitted in the order the keywords appear in the list, not the order they
    appear in PT1 — so 'Slim Slouchy' stays in list order. Casing is taken from
    PT1 so 'Wide Leg' keeps the source capitalization.
    """
    used_spans: List[Tuple[int, int]] = []
    hits: Dict[int, str] = {}
    order = {kw: i for i, kw in enumerate(keywords)}
    for kw in sorted(keywords, key=len, reverse=True):
        pattern = r"\b" + re.escape(kw).replace(r"\ ", r"\s+") + r"\b"
        for m in re.finditer(pattern, pt1, flags=re.IGNORECASE):
            if any(m.start() < e and s < m.end() for s, e in used_spans):
                continue
            used_spans.append((m.start(), m.end()))
            hits[order[kw]] = m.group(0)
            break
    return " ".join(hits[i] for i in sorted(hits))


def handle_descriptor(handle: str) -> str:
    """Handle minus its trailing style code, as Title Case words."""
    if not handle:
        return ""
    base = handle.rsplit("-", 1)[0] if "-" in handle else handle
    return " ".join(w.capitalize() for w in base.replace("-", " ").split())


def build_product_title(title: str, v2: str, v3: str,
                        handle: str = "") -> Tuple[str, str, str]:
    """Steps 1-12. Returns (final title, PT1, jean-style keywords)."""
    title_clean = clean_title_step1(title)
    v2_clean = clean_title_v(v2)
    v3_clean = clean_title_v(v3)
    if not v2_clean and not v3_clean:
        # No Constructor record for this handle — fall back to the handle so
        # the rise/style words still make it into PT1.
        v3_clean = title_clean
        v2_clean = clean_title_v(handle_descriptor(handle))
    pt1 = build_pt1(title_clean, v2_clean, v3_clean)

    rise_kw = first_keyword(pt1, PT_RISE_KEYWORDS)
    jean_kw = all_keywords(pt1, PT_JEAN_STYLE_KEYWORDS)
    if re.match(r"^\s*(Ex|The)\s+Boyfriend", v3_clean, flags=re.IGNORECASE):
        jean_kw = re.sub(r"\bBoyfriend\b", " ", jean_kw, flags=re.IGNORECASE)
        jean_kw = re.sub(r"\s+", " ", jean_kw).strip()

    inseam_list = (PT_INSEAM_STYLE_KEYWORDS_2
                   if re.search(r"\bcropped\b", pt1, re.IGNORECASE)
                   else PT_INSEAM_STYLE_KEYWORDS_1)
    inseam_style_kw = all_keywords(pt1, inseam_list)
    inseam_label_kw = all_keywords(pt1, PT_INSEAM_LABEL_KEYWORDS)
    type_kw = all_keywords(pt1, PT_TYPE_KEYWORDS)
    styling_kw = all_keywords(pt1, PT_STYLING_KEYWORDS)

    # Step 11 — leftover name from the original title
    pool = set()
    for chunk in (rise_kw, jean_kw, inseam_style_kw, inseam_label_kw,
                  type_kw, styling_kw):
        for word in chunk.split():
            key = re.sub(r"[^a-z0-9]", "", word.lower())
            if key:
                pool.add(key)
    name_words = []
    for word in title_clean.split():
        key = re.sub(r"[^a-z0-9]", "", word.lower())
        if key and key in pool:
            continue
        name_words.append(word)
    name_kw = " ".join(name_words)

    final = " ".join(p for p in (name_kw, rise_kw, jean_kw, inseam_style_kw,
                                 styling_kw, inseam_label_kw, type_kw) if p)
    return re.sub(r"\s+", " ", final).strip(), pt1, jean_kw


# ---------------------------------------------------------------------------
# Style Name
# ---------------------------------------------------------------------------
def clean_handle_for_style_name(handle: str) -> str:
    """Step 1 — drop the trailing style code, normalize Boot, de-duplicate."""
    if not handle:
        return ""
    base = handle.rsplit("-", 1)[0] if "-" in handle else handle
    text = base.replace("-", " ").strip()

    has_boot = re.search(r"\bboot\b", text, re.IGNORECASE)
    has_bootcut = re.search(r"\bbootcut\b", text, re.IGNORECASE)
    if has_boot and has_bootcut:
        text = re.sub(r"\bboot\b(?!cut)", " ", text, flags=re.IGNORECASE)
    else:
        text = re.sub(r"\bboot\s+cut\b", "Bootcut", text, flags=re.IGNORECASE)
        text = re.sub(r"\bboot\b(?!cut)", "Bootcut", text, flags=re.IGNORECASE)

    # Keep only the last occurrence of any repeated word, except that words
    # belonging to a protected compound ("ex boyfriend" is its own style, not
    # a repeat of "boyfriend") are always kept and suppress the loose copies.
    words = text.split()
    protected_idx: set[int] = set()
    protected_words: set[str] = set()
    lowered = [w.lower() for w in words]
    for compound in PROTECTED_COMPOUNDS:
        parts = compound.lower().split()
        for i in range(len(lowered) - len(parts) + 1):
            if lowered[i:i + len(parts)] == parts:
                for j in range(i, i + len(parts)):
                    protected_idx.add(j)
                    protected_words.add(lowered[j])

    keep = []
    for i, word in enumerate(words):
        key = word.lower()
        if i in protected_idx:
            keep.append(word)
            continue
        if key in protected_words:
            continue
        if any(w == key for w in lowered[i + 1:]):
            continue
        keep.append(word)
    return re.sub(r"\s+", " ", " ".join(keep)).strip()


def strip_style_words(text: str) -> str:
    """Step 2 — remove styling words, numbers, punctuation."""
    out = text
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        if phrase.lower() == "jean":
            out = re.sub(r"\bJeans?\b", " ", out, flags=re.IGNORECASE)
            continue
        pattern = r"\b" + re.escape(phrase).replace(r"\ ", r"\s+") + r"\b"
        out = re.sub(pattern, " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\b\d+\b", " ", out)
    out = re.sub(r"[^\w\s]", " ", out)
    return re.sub(r"\s+", " ", out).strip()


def build_style_name(handle: str) -> str:
    cleaned = clean_handle_for_style_name(handle)
    stripped = strip_style_words(cleaned)
    return " ".join(w.capitalize() for w in stripped.split())


# ---------------------------------------------------------------------------
# Jean Style
# ---------------------------------------------------------------------------
def _straight_bucket(leg_opening) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def jean_style_from_source(source: str, leg_opening, *, is_description=False) -> str:
    """Shared ordered keyword ladder used for Product, Handle and Description."""
    if not source:
        return ""
    n = normalize_text(source)

    def has(*phrases) -> bool:
        return any(normalize_text(p) in n for p in phrases)

    if has("wide leg", "palazzo"):
        return "Wide Leg"
    if has("tapered", "relaxed skinny"):
        return "Tapered"
    if has("skinny"):
        return "Skinny"
    if has("bootcut", "boot cut", "boot"):
        return "Bootcut"
    if has("flare"):
        return "Flare"
    if has("mom", "boyfriend", "barrel", "barrell", "bowed", "bow leg",
            "stovepipe", "stove pipe", "horseshoe"):
        lo = to_float(leg_opening)
        if lo is not None:
            return "Tapered" if lo < 15.5 else "Barrel"
    if has("cigarette", "slim straight"):
        return "Straight from Knee"
    if is_description and has("slim") and has("straight"):
        return "Straight from Knee"
    if has("straight"):
        bucket = _straight_bucket(leg_opening)
        if bucket:
            return bucket
    if has("baggy"):
        return "Baggy"
    if is_description:
        if has("straight") and (has("relaxed") or has("loose fitting")):
            return "Straight from Thigh"
        if has("straight"):
            return "Straight from Knee/Thigh"
    return ""


# ---------------------------------------------------------------------------
# Inseam Label / Inseam Style
# ---------------------------------------------------------------------------
def determine_inseam_label(product_base: str, description: str, size: str) -> str:
    """Keyword-driven only.

    The measurement rule ("Inseam is 30 or more") fills Inseam *Style*, not
    Inseam Label — inseam alone cannot separate the two here, since Long and
    Regular both occur at 32" and 33". Long comes from the Product or the
    description wording; everything else is Regular.
    """
    if re.search(r"\bpetite\b", product_base, re.IGNORECASE) or \
            re.fullmatch(r"\d+\s*P", (size or "").strip(), re.IGNORECASE):
        return "Petite"
    if re.search(r"\b(extended|long|tall)\b", product_base, re.IGNORECASE):
        return "Long"
    if contains_any(description, ["longer inseam", "extra long inseam",
                                  "extra-long inseam",
                                  "ideal for taller frames", "longest inseam"]):
        return "Long"
    return "Regular"


def measurement_inseam_style(jean_style: str, inseam, rise, label: str,
                             style_name: str = "") -> str:
    ins = to_float(inseam)
    if ins is None:
        return ""
    r = to_float(rise)
    ir = (ins + r) if r is not None else None

    if jean_style in NON_TAPER_STYLES:
        if label == "Petite":
            if ins < 26:
                if ir is None:
                    return ""
                if ir <= 37:
                    return "Cropped"
                return "Ankle" if ir < 38 else "Full Length"
            if ins < 28:
                if ir is None:
                    return ""
                if ir < 35.5:
                    return "Cropped"
                return "Ankle" if ir < 38 else "Full Length"
            if ins < 30:
                if ir is None:
                    return ""
                return "Ankle" if ir < 38 else "Full Length"
            return "Full Length"
        if label == "Regular":
            if ins < 27:
                if ir is None:
                    return ""
                if ir <= 39:
                    return "Cropped"
                return "Ankle" if ir < 40 else "Full Length"
            if ins < 29:
                if ir is None:
                    return ""
                # Mercer Barrel reaches full length earlier than the rest of
                # the non-taper group at this inseam.
                if style_name.strip().lower() == "mercer barrel" and ir >= 39:
                    return "Full Length"
                if ir < 37.5:
                    return "Cropped"
                return "Ankle" if ir < 40 else "Full Length"
            if ins < 31:
                if ir is None:
                    return ""
                return "Ankle" if ir < 40 else "Full Length"
            return "Full Length"
        if label == "Long":
            if ins < 28:
                return "Cropped"
            return "Ankle" if ins <= 31 else "Full Length"

    if jean_style in TAPER_STYLES:
        if label == "Petite":
            if ins < 25:
                return "Cropped"
            return "Ankle" if ins <= 27 else "Full Length"
        if label == "Regular":
            if ins <= 27:
                if ir is None:
                    return ""
                if ir <= 37:
                    return "Cropped"
                return "Ankle" if ir < 40 else "Full Length"
            if ins < 28.5:
                if ir is None:
                    return ""
                return "Ankle" if ir < 40 else "Full Length"
            if ins <= 29:
                if ir is None:
                    return ""
                return "Ankle" if ir < 38.75 else "Full Length"
            return "Full Length"
        if label == "Long":
            if ins >= 30 or (ir is not None and ir >= 41):
                return "Full Length"
            if ins < 27:
                return "Cropped"
            return "Ankle"
    return ""


def keyword_inseam_style(description: str) -> str:
    d = normalize_text(description)

    def has(*phrases) -> bool:
        return any(normalize_text(p) in d for p in phrases)

    if has("full length", "full-length", "long inseam", "long-inseam",
           "full inseam", "full-inseam", "floor-length", "floor length",
           "slight amount of stacking", "stack at the", "full dramatic length",
           "puddles at the hem", "puddle at the hem"):
        return "Full Length"
    if has("slightly cropped"):
        return "Ankle"
    if has("at the ankle"):
        return "Ankle"
    if has("at the ankles", "ankle length", "ankle-length", "ankle-skimming"):
        return "Ankle"
    if has("cropped"):
        return "Cropped"
    if has("hits just above the ankle"):
        return "Cropped"
    return ""


def tags_inseam_style(tags: str) -> str:
    t = (tags or "").lower()
    if "length:crop" in t:
        return "Cropped"
    if "length:ankle" in t:
        return "Ankle"
    if "length:full" in t:
        return "Full Length"
    return ""


def rise_label_from_description(description: str, rise) -> str:
    """Fallback 1 — explicit description phrases."""
    if contains_any(description, RISE_DESC_ULTRA_LOW):
        return "Ultra Low"
    if contains_any(description, RISE_DESC_ULTRA_HIGH):
        return "Ultra High"
    has_mid = contains_any(description, RISE_DESC_MID)
    has_high = contains_any(description, RISE_DESC_HIGH)
    if has_mid and has_high:
        # Both present — defer to the measured Rise.
        r = to_float(rise)
        if r is not None:
            return "High" if r >= 12 else "Mid"
    if has_mid:
        return "Mid"
    if contains_any(description, RISE_DESC_LOW):
        return "Low"
    if has_high:
        return "High"
    return ""


def rise_labels_from_tags(tags: str) -> List[str]:
    """Fallback 2 — distinct rise labels present in the Rise: tags."""
    found: List[str] = []
    for raw in (tags or "").split(","):
        entry = raw.strip()
        if not entry.lower().startswith("rise"):
            continue
        n = normalize_text(entry)
        for label, phrases in RISE_TAG_RULES:
            if any(normalize_text(p) in n for p in phrases):
                if label not in found:
                    found.append(label)
                break
    return found


def determine_rise_label(product_base: str, description: str,
                         handle: str = "", rise=None) -> str:
    # Handle sits between Product and Description: when Constructor has no
    # subtitle the rise only survives in the handle (e.g. sydney-*-high-rise-*).
    for src in (product_base, handle.replace("-", " ")):
        n = normalize_text(src)
        if not n:
            continue
        if "mid rise" in n or re.search(r"(^|\s)mid(\s|$)", n):
            return "Mid"
        if "low rise" in n or re.search(r"(^|\s)low(\s|$)", n):
            return "Low"
        if "high rise" in n or re.search(r"(^|\s)high(\s|$)", n):
            return "High"
    return rise_label_from_description(description, rise)


def determine_stretch_from_description(description: str) -> str:
    d = normalize_text(description)

    def has(*phrases) -> bool:
        return any(normalize_text(p) in d for p in phrases)

    if has("non stretch", "nonstretch", "non-stretch", "rigid"):
        return "Rigid"
    if has("comfort denim", "daytripper stretch denim"):
        return "Low Stretch"
    if has("power stretch", "power-stretch", "power denim"):
        return "Medium Stretch"
    if has("super stretch", "super denim"):
        return "Medium to High Stretch"
    if has("superior stretch denim", "premiere stretch", "ultimate stretch"):
        return "High Stretch"
    return ""


def pick_image_url(product: dict, constructor_data: dict) -> str:
    """Skip images containing '_9.jpg' or '_1_'; take the next available."""
    urls: List[str] = []
    for node in ((product.get("images") or {}).get("nodes") or []):
        url = node.get("url") or ""
        if url:
            urls.append(url)
    featured = (product.get("featuredImage") or {}).get("url") or ""
    if featured and featured not in urls:
        urls.insert(0, featured)
    cimg = constructor_data.get("image_url") or ""
    if cimg and cimg not in urls:
        urls.append(cimg)
    for url in urls:
        name = url.split("/")[-1]
        if "_9.jpg" in name or "_1_" in name:
            continue
        return url
    return urls[0] if urls else ""


def get_option(variant: dict, names: Sequence[str]) -> str:
    for option in (variant.get("selectedOptions") or []):
        if (option.get("name") or "").strip().lower() in names:
            return option.get("value") or ""
    return ""


# ---------------------------------------------------------------------------
# Color / Hem classifiers (unchanged behaviour)
# ---------------------------------------------------------------------------
def classify_color_standardized(color: str, description: str, tags: str) -> str:
    def classify(source: str) -> str:
        rules = [
            (["animal print", "leopard", "snake"], "Animal Print"),
            (["black"], "Black"), (["blue", "indigo"], "Blue"),
            (["brown"], "Brown"),
            (["tan", "taupe", "beige", "khaki", "canvas"], "Tan"),
            (["white", "ecru", "off white wash"], "White"),
            (["green", "olive", "sage"], "Green"),
            (["grey", "smoke"], "Grey"), (["orange"], "Orange"),
            (["pink"], "Pink"), (["print"], "Print"), (["purple"], "Purple"),
            (["red"], "Red"), (["yellow"], "Yellow"),
        ]
        for phrases, label in rules:
            if any(find_word(source, p) for p in phrases):
                return label
        return ""
    out = classify(color) or classify(description)
    if out:
        return out
    m = re.search(r"(?:^|,)\s*Color\s*:\s*([^,]+)", tags, flags=re.IGNORECASE)
    return (m.group(1).strip() if m else "")


def classify_color_simplified(color_standardized: str, description: str,
                              tags: str) -> str:
    if color_standardized.lower() in {"grey", "white", "tan"}:
        return "Light"
    if contains_any(description, ["light to medium", "medium to light",
                                  "medium light", "light medium"]):
        return "Light to Medium"
    if contains_any(description, ["medium to dark", "dark to medium",
                                  "medium dark", "dark medium"]):
        return "Medium to Dark"
    if contains_any(description, ["dark", "black", "navy"]):
        return "Dark"
    if contains_any(description, ["light wash", "light denim", "light indigo",
                                  "light blue"]):
        return "Light"
    if contains_any(description, ["medium", "mid blue", "classic blue"]):
        return "Medium"
    wash_values = [m.strip() for m in re.findall(
        r"(?:^|,)\s*Wash\s*:\s*([^,]+)", tags, flags=re.IGNORECASE)]
    if not wash_values:
        return ""
    normalized = set()
    for wash in wash_values:
        cleaned = re.sub(r"\bwash\b", "", wash, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if cleaned:
            normalized.add(cleaned.lower())
    if "medium" in normalized and "light" in normalized:
        return "Light to Medium"
    if "medium" in normalized and "dark" in normalized:
        return "Medium to Dark"
    if "black" in normalized:
        return "Dark"
    if "white" in normalized:
        return "Light"
    if "color" in normalized:
        return ""
    first = re.sub(r"\bwash\b", "", wash_values[0], flags=re.IGNORECASE)
    first = re.sub(r"\s+", " ", first).strip()
    return first.title() if first else ""


def determine_hem_style(description: str, tags: str) -> str:
    rules = [
        (["frayed hem", "frayed hems", "fraying at the hems"], "Frayed Hem"),
        (["cuffed hem", "double cuffed hems", "cuffs at the hem"], "Cuffed Hem"),
        (["raw hem", "raw hems", "raw cut hems", "raw cut hem"], "Raw Hem"),
        (["clean hems"], "Clean Hems"),
        (["released hem"], "Released Hem"),
        (["split hem"], "Split Hem"),
        (["distressed hem", "distressing at the hem",
          "light distressing at the hem", "busted hems", "well worn hems"],
         "Distressed Hem"),
    ]
    for phrases, label in rules:
        if contains_any(description, phrases):
            return label
    m = re.search(r"(?:^|,)\s*Hem\s*:\s*([^,]+)", tags, flags=re.IGNORECASE)
    if not m:
        return ""
    hem = m.group(1).strip()
    return {"Busted Hem": "Distressed Hem", "Vintage Raw Hem": "Raw Hem",
            "Cuff": "Cuffed Hem"}.get(hem, hem)


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------
def matches_filter_words(*sources: str) -> bool:
    hay = " " + normalize_text(" ".join(s or "" for s in sources)) + " "
    for word in FILTER_WORDS:
        w = normalize_text(word)
        if w and re.search(rf"(^|\s){re.escape(w)}(\s|$)", hay):
            return True
    return False


def filter_products(products_by_handle: Dict[str, dict]) -> Dict[str, dict]:
    kept: Dict[str, dict] = {}
    for handle, product in products_by_handle.items():
        tags = clean_tags(product.get("tags"))
        filtercats = " ".join(
            t.split(":", 1)[1] for t in tags.split(",")
            if ":" in t and t.strip().lower().startswith(
                ("filtercategory", "category")))
        if matches_filter_words(product.get("title") or "",
                                product.get("productType") or "",
                                filtercats, handle.replace("-", " ")):
            continue
        kept[handle] = product
    logging.info("Products retained after filtering: %s/%s", len(kept),
                 len(products_by_handle))
    return kept


def dedupe_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for row in rows:
        key = (row.get("Style Id"), row.get("SKU - Shopify"),
               row.get("SKU - Brand"))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------
def apply_style_name_rules(rows: List[dict]) -> None:
    """Step 3 rule 1 (sibling unification) and rule 2 (one-word names)."""
    by_first: Dict[str, List[dict]] = {}
    for row in rows:
        fw = (row["Style Name"].split()[:1] or [""])[0].lower()
        if fw:
            by_first.setdefault(fw, []).append(row)

    for _, group in by_first.items():
        non_mat = [r for r in group
                   if "maternity" not in (r["Product"] or "").lower()]
        if len(non_mat) < 2:
            continue
        legs = [to_float(r["Leg Opening"]) for r in non_mat]
        targets: List[Optional[str]] = []
        for i, _r in enumerate(non_mat):
            lo_i = legs[i]
            if lo_i is None or len(non_mat[i]["Style Name"].split()) > 1:
                targets.append(None)
                continue
            compat = [non_mat[j]["Style Name"] for j, lo_j in enumerate(legs)
                      if lo_j is not None and abs(lo_j - lo_i) <= 1.5
                      and non_mat[j]["Style Name"]]
            if not compat or len(set(compat)) <= 1:
                targets.append(None)
                continue
            multi = [s for s in compat if len(s.split()) > 1]
            targets.append(max(set(multi), key=multi.count) if multi
                           else max(set(compat), key=compat.count))
        for row, tgt in zip(non_mat, targets):
            if tgt:
                row["Style Name"] = tgt

    for row in rows:
        sn = (row["Style Name"] or "").strip()
        if not sn or len(sn.split()) != 1:
            continue
        lo_val = to_float(row["Leg Opening"])
        cands = [r["Style Name"] for r in rows
                 if r["Style Name"].split(" ", 1)[0].lower() == sn.lower()
                 and len(r["Style Name"].split()) > 1
                 and (lo_val is None
                      or (to_float(r["Leg Opening"]) is not None
                          and abs(to_float(r["Leg Opening"]) - lo_val) <= 1.5))]
        if cands:
            row["Style Name"] = max(set(cands), key=cands.count)
            continue
        js = row.get("Jean Style") or ""
        if js:
            row["Style Name"] = f"{sn} {js.split()[0]}".strip()


def apply_jean_style_by_style_name(rows: List[dict]) -> None:
    """Fill blanks from siblings sharing Style Name and Leg Opening."""
    groups: Dict[str, List[dict]] = {}
    for row in rows:
        if row["Style Name"]:
            groups.setdefault(row["Style Name"], []).append(row)
    for row in rows:
        if row["Jean Style"]:
            continue
        sibs = [r for r in groups.get(row["Style Name"], []) if r["Jean Style"]]
        if not sibs:
            continue
        lo = to_float(row["Leg Opening"])
        matching = {r["Jean Style"] for r in sibs
                    if lo is None or to_float(r["Leg Opening"]) == lo}
        if len(matching) == 1:
            row["Jean Style"] = next(iter(matching))


def unify_jean_style_by_style_name(rows: List[dict]) -> None:
    """Majority Jean Style wins across rows sharing a Style Name."""
    groups: Dict[str, List[dict]] = {}
    for row in rows:
        if row["Style Name"]:
            groups.setdefault(row["Style Name"], []).append(row)
    for _, group in groups.items():
        styles = [r["Jean Style"] for r in group if r["Jean Style"]]
        if not styles:
            continue
        counts = Counter(styles)
        if len(counts) <= 1 and len(styles) == len(group):
            continue
        majority = counts.most_common(1)[0][0]
        for row in group:
            if not row["Jean Style"]:
                row["Jean Style"] = majority


def apply_inseam_style(rows: List[dict]) -> None:
    for row in rows:
        if row["Inseam Style"]:
            continue
        style = measurement_inseam_style(row["Jean Style"], row["Inseam"],
                                         row["Rise"], row["Inseam Label"],
                                         row["Style Name"])
        if not style:
            style = keyword_inseam_style(row["Description"])
        if not style:
            style = tags_inseam_style(row["Tags"])
        row["Inseam Style"] = style

    # Style Name + measurement matching for rows still missing a value
    groups: Dict[str, List[dict]] = {}
    for row in rows:
        if row["Style Name"]:
            groups.setdefault(row["Style Name"], []).append(row)
    for row in rows:
        if row["Inseam Style"]:
            continue
        sibs = [r for r in groups.get(row["Style Name"], [])
                if r is not row and r["Inseam Style"]]
        if not sibs:
            continue
        matches = []
        for sib in sibs:
            same = True
            for field in ("Rise", "Knee", "Inseam", "Leg Opening"):
                a, b = to_float(row[field]), to_float(sib[field])
                if a is not None and b is not None and a != b:
                    same = False
                    break
            if same:
                matches.append(sib["Inseam Style"])
        if matches:
            row["Inseam Style"] = Counter(matches).most_common(1)[0][0]


def apply_rise_label_fallbacks(rows: List[dict]) -> None:
    """Rise Label fallback 2 (tags) then fallback 3 (matching Style Name)."""
    def closest_sibling_label(row: dict, within: Optional[float]) -> str:
        mine = to_float(row["Rise"])
        if mine is None:
            return ""
        best: Optional[Tuple[float, str]] = None
        for other in rows:
            if other is row or not other["Rise Label"]:
                continue
            if other["Style Name"] != row["Style Name"]:
                continue
            theirs = to_float(other["Rise"])
            if theirs is None:
                continue
            gap = abs(theirs - mine)
            if within is not None and gap > within:
                continue
            if best is None or gap < best[0]:
                best = (gap, other["Rise Label"])
        return best[1] if best else ""

    # Fallback 2 — tags
    for row in rows:
        if row["Rise Label"]:
            continue
        labels = rise_labels_from_tags(row["Tags"])
        if not labels:
            continue
        if len(labels) == 1:
            row["Rise Label"] = labels[0]
            continue
        # Multiple rise tags: use the same-Style-Name item whose Rise is
        # closest, and only when it is within 1 inch.
        row["Rise Label"] = closest_sibling_label(row, 1.0)

    # Fallback 3 — matching Style Name
    by_style: Dict[str, List[dict]] = {}
    for row in rows:
        if row["Style Name"] and row["Rise Label"]:
            by_style.setdefault(row["Style Name"], []).append(row)
    for row in rows:
        if row["Rise Label"]:
            continue
        sibs = by_style.get(row["Style Name"]) or []
        if not sibs:
            continue
        labels = {s["Rise Label"] for s in sibs}
        if len(labels) == 1:
            row["Rise Label"] = next(iter(labels))
            continue
        row["Rise Label"] = closest_sibling_label(row, None)


def main() -> None:
    configure_logging()
    session = SESSION

    products_by_handle: Dict[str, dict] = {}
    for collection in COLLECTION_HANDLES:
        for product in fetch_graphql_products(session, collection):
            handle = product.get("handle") or ""
            if handle:
                products_by_handle[handle] = product

    constructor_results = []
    for group in COLLECTION_HANDLES:
        constructor_results.extend(fetch_constructor_collection(session, group))
    by_handle, by_variant_id, by_sku = build_constructor_maps(constructor_results)

    products_by_handle = filter_products(products_by_handle)
    pdp_cache = fetch_pdp_cache(session, products_by_handle.keys())
    wishlist_counts = fetch_wishlist_counts(session, products_by_handle)

    rows: List[dict] = []
    for handle, product in products_by_handle.items():
        tags = clean_tags(product.get("tags"))
        pdp = pdp_cache.get(handle, {})
        cresult = by_handle.get(handle, {})
        cdata = cresult.get("data") or {}

        product_title = product.get("title") or ""
        title_v2 = cdata.get("subtitle") or ""
        title_v3 = cresult.get("value") or ""

        graphql_desc = clean_description_text(
            BeautifulSoup(product.get("description") or "", "html.parser")
            .get_text(" ", strip=True))
        description = pdp.get("description") or graphql_desc

        product_base, _pt1, _jkw = build_product_title(product_title, title_v2,
                                                       title_v3, handle)
        style_name = build_style_name(handle)
        style_id = strip_gid(product.get("id") or "", "gid://shopify/Product/")
        image_url = pick_image_url(product, cdata)

        rise = pdp.get("rise") or ""
        knee = pdp.get("knee") or ""
        leg_opening = pdp.get("leg_opening") or ""
        stretch = pdp.get("stretch") or determine_stretch_from_description(description)

        jean_style = (jean_style_from_source(product_base, leg_opening)
                      or jean_style_from_source(handle.replace("-", " "),
                                                leg_opening))
        rise_label = determine_rise_label(product_base, description, handle, rise)
        hem_style = determine_hem_style(description, tags)

        for variant in ((product.get("variants") or {}).get("nodes") or []):
            variant_id = strip_gid(variant.get("id") or "",
                                   "gid://shopify/ProductVariant/")
            sku_brand = (variant.get("sku") or "").strip()
            hit = by_variant_id.get(variant_id) or by_sku.get(sku_brand.upper()) or {}
            cvar_data = (hit.get("variation") or {}).get("data") or {}

            color = get_option(variant, ["color", "option1"]) or ""
            size = get_option(variant, ["size", "option2"]) or ""
            inseam_opt = get_option(variant, ["inseam", "option3"]) or ""
            inseam = pdp.get("inseam") or (
                parse_number_with_fraction(inseam_opt) if inseam_opt else "")

            inventory_list = cvar_data.get("inventory") or []
            instore_qty = sum(int(x.get("available") or 0)
                              for x in inventory_list if isinstance(x, dict))

            inseam_label = determine_inseam_label(product_base, description, size)
            color_std = classify_color_standardized(color, description, tags)
            color_simple = classify_color_simplified(color_std, description, tags)

            fabric_source = ""
            mill = cdata.get("mill")
            if isinstance(mill, list) and mill:
                fabric_source = str(mill[0])
            elif isinstance(mill, str):
                fabric_source = mill

            product_display = (f"{product_base} - {color.title()}"
                               if color else product_base)
            qty = variant.get("quantityAvailable")

            rows.append({
                "Style Id": style_id,
                "Handle": handle,
                "Published At": format_date(product.get("publishedAt")),
                "Created At": format_date(product.get("createdAt")),
                "Updated At": format_date(product.get("updatedAt")),
                "Product": product_display,
                "Style Name": style_name,
                "Product Type": "Jeans",
                "Tags": tags,
                "Vendor": product.get("vendor") or "",
                "Description": description,
                "Variant Title": f"{product_title} - {variant.get('title') or ''}".strip(" -"),
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Knee": knee,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": to_price((variant.get("price") or {}).get("amount")),
                "Compare at Price": to_price((variant.get("compareAtPrice") or {}).get("amount")),
                "Available for Sale": str(bool(variant.get("availableForSale"))).upper(),
                "Quantity Available": qty if qty is not None else "",
                "Quantity Available (Online)": qty if qty is not None else "",
                "Quantity Available (Instore Inventory)": instore_qty,
                "Quantity of style": product.get("totalInventory")
                                     if product.get("totalInventory") is not None else "",
                "Wishlist Count": wishlist_counts.get(style_id, ""),
                "SKU - Shopify": variant_id,
                "SKU - Brand": sku_brand,
                "Barcode": variant.get("barcode") or "",
                "Image URL": image_url,
                "SKU URL": f"https://www.agjeans.com/products/{handle}",
                "Jean Style": jean_style,
                "Hem Style": hem_style,
                "Inseam Label": inseam_label,
                "Inseam Style": "",
                "Rise Label": rise_label,
                "Color - Simplified": color_simple,
                "Color - Standardized": color_std,
                "Fabric Source": fabric_source,
                "Stretch": stretch,
            })

    rows = dedupe_rows(rows)

    apply_jean_style_by_style_name(rows)
    for row in rows:
        if not row["Jean Style"]:
            row["Jean Style"] = jean_style_from_source(
                row["Description"], row["Leg Opening"], is_description=True)
    apply_jean_style_by_style_name(rows)
    unify_jean_style_by_style_name(rows)
    apply_style_name_rules(rows)
    apply_inseam_style(rows)

    apply_rise_label_fallbacks(rows)

    # Fabric Source fallback within a style
    style_fabric: Dict[str, str] = {}
    for row in rows:
        sid, fab = str(row.get("Style Id") or ""), str(row.get("Fabric Source") or "")
        if sid and fab and sid not in style_fabric:
            style_fabric[sid] = fab
    for row in rows:
        if not str(row.get("Fabric Source") or "").strip():
            row["Fabric Source"] = style_fabric.get(str(row.get("Style Id") or ""), "")

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"AGJEANS_{ts}.csv"
    with out_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info("Rows written: %s", len(rows))
    logging.info("CSV written: %s", out_path.resolve())


if __name__ == "__main__":
    main()
