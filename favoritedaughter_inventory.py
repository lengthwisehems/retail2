#!/usr/bin/env python3
"""Favorite Daughter denim inventory scraper."""
from __future__ import annotations

import csv
import html
import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from itertools import cycle
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import requests
import urllib3

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "favoritedaughter_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "favoritedaughter_run.log"
OUTPUT_DIR.mkdir(exist_ok=True)

SHOPIFY_HOSTS = [
    "https://shopfavoritedaughter.com",
    "https://shopfavoritedaughter-com.myshopify.com",
]
GRAPHQL_PATH = "/api/2025-07/graphql.json"
GRAPHQL_TOKENS = [
    "30041a91612d5ff0e8a0848541bfc099",
    "b0c329456216781c8595f96380386f2d",
]

COLLECTION_HANDLES = ["denim", "sale-denim", "petite-denim"]
PRODUCT_FEED_COLLECTIONS = ["denim", "sale-denim", "petite-denim"]

EXCLUDED_TITLE_KEYWORDS = {
    "skirt",
    "shirt",
    "jacket",
    "trench",
    "bermuda",
    "short",
    "shorts",
    "dress",
    "shacket",
    "vest",
    "tee",
}

EXCLUDED_PRODUCT_TYPES = {
    "tops",
    "shirt",
    "jackets",
    "jacket",
    "outerware",
    "bermuda",
    "short",
    "shorts",
    "dress",
    "shacket",
    "vest",
    "tee",
}

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Created At",
    "Product",
    "Style Name",
    "Product Type",
    "Tags",
    "Vendor",
    "Description",
    "Variant Title",
    "Color",
    "Size",
    "Rise",
    "Inseam",
    "Knee",
    "Leg Opening",
    "Price",
    "Compare at Price",
    "Available for Sale",
    "Quantity Available",
    "Old Quantity Available",
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
]

REQUEST_TIMEOUT = 40
TRANSIENT_STATUS = {429, 500, 502, 503, 504}
REBUY_WIDGET_ID = "183060"
REBUY_BASE = "https://rebuyengine.com"
REBUY_SHOP = "shopfavoritedaughter-com.myshopify.com"
REBUY_CHUNK_SIZE = 20

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/128.0.0.0 Safari/537.36"
)

MEASUREMENT_REPLACEMENTS = {
    "\u201c": '"',
    "\u201d": '"',
    "\u00e2\u20ac\u0153": '"',
    "\u00e2\u20ac\u009d": '"',
    "\u2033": '"',
    "\uff02": '"',
    "\u2018": "'",
    "\u2019": "'",
    "\u00e2\u20ac\u02dc": "'",
    "\u00e2\u20ac\u2122": "'",
    "\u00a0": " ",
}
MEASUREMENT_UNIT_PATTERN = r"(?:['\"′″”]|inches?|in\.)"
MEASUREMENT_VALUE_PATTERN = r"\d+(?:\.\d+)?(?:\s+\d/\d+)?"

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})
session.verify = False

rebuy_session = requests.Session()
rebuy_session.headers.update({"User-Agent": USER_AGENT})
rebuy_session.verify = False

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

token_cycle = cycle(GRAPHQL_TOKENS)

logger = logging.getLogger("favoritedaughter_inventory")
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def configure_logging() -> None:
    handlers: List[logging.Handler] = []
    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path, encoding="utf-8")
            handlers.append(handler)
            if path != LOG_PATH:
                print(
                    f"WARNING: Primary log path unavailable. Using fallback log at {path}.",
                    flush=True,
                )
            break
        except OSError as exc:
            print(f"WARNING: Unable to open log file {path}: {exc}", flush=True)
    if not handlers:
        handlers.append(logging.StreamHandler())
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.info("Logging initialized")


def host_candidates() -> Iterable[str]:
    seen = set()
    for host in SHOPIFY_HOSTS:
        if host not in seen:
            seen.add(host)
            yield host


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def perform_shopify_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    json_payload: Any = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT,
    add_token: bool = False,
) -> requests.Response:
    if not path.startswith("http"):
        path = path if path.startswith("/") else f"/{path}"
    last_error: Optional[Exception] = None
    for host in host_candidates():
        url = f"{host}{path}"
        for attempt in range(5):
            try:
                req_headers = dict(headers or {})
                if add_token:
                    req_headers.setdefault(
                        "X-Shopify-Storefront-Access-Token", next(token_cycle)
                    )
                    req_headers.setdefault("Content-Type", "application/json")
                resp = session.request(
                    method,
                    url,
                    params=params,
                    json=json_payload,
                    headers=req_headers or None,
                    timeout=timeout,
                    verify=False,
                )
                if resp.status_code in TRANSIENT_STATUS:
                    raise requests.HTTPError(f"transient status {resp.status_code}")
                resp.raise_for_status()
                if host != SHOPIFY_HOSTS[0]:
                    logger.info("Switched host to %s", host)
                return resp
            except Exception as exc:  # pragma: no cover - network path
                last_error = exc
                sleep_for = min(8.0, 1.0 * (2 ** attempt))
                logger.warning("%s %s failed (%s); sleeping %.1fs", method, url, exc, sleep_for)
                time.sleep(sleep_for)
        logger.error("Giving up on host %s after retries", host)
    if last_error:
        raise last_error
    raise RuntimeError("perform_shopify_request exhausted hosts")


def execute_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = perform_shopify_request(
        "POST",
        GRAPHQL_PATH,
        json_payload={"query": query, "variables": variables or {}},
        add_token=True,
    )
    payload = resp.json()
    if payload.get("errors"):
        logger.error("GraphQL errors: %s", payload["errors"])
        raise RuntimeError("GraphQL request failed")
    return payload.get("data", {})


COLLECTION_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    handle
    products(first: 100, after: $cursor) {
      edges {
        cursor
        node {
          id
          handle
          title
          description
          tags
          vendor
          productType
          createdAt
          publishedAt
          onlineStoreUrl
          featuredImage { url }
          variants(first: 250) {
            edges {
              node {
                id
                title
                sku
                barcode
                availableForSale
                price { amount }
                compareAtPrice { amount }
                image { url }
                selectedOptions { name value }
              }
            }
          }
        }
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""

PRODUCT_BY_HANDLE_QUERY = """
query ProductByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    id
    handle
    title
    description
    tags
    vendor
    productType
    createdAt
    publishedAt
    onlineStoreUrl
    featuredImage { url }
    variants(first: 250) {
      edges {
        node {
          id
          title
          sku
          barcode
          availableForSale
          price { amount }
          compareAtPrice { amount }
          image { url }
          selectedOptions { name value }
        }
      }
    }
  }
}
"""

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def clean_text(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip()


def contains_keyword(text: str, keyword: str) -> bool:
    if not text or not keyword:
        return False
    lowered = text.lower()
    needle = keyword.lower().strip()
    if not needle:
        return False
    if re.search(r"\s", needle):
        return needle in lowered
    pattern = rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])"
    return re.search(pattern, lowered) is not None


def contains_any_keyword(text: str, keywords: Set[str]) -> bool:
    return any(contains_keyword(text, keyword) for keyword in keywords)


def parse_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except Exception:
        try:
            dt = datetime.strptime(value[:10], "%Y-%m-%d")
            return dt.strftime("%m/%d/%y")
        except Exception:
            return ""


def should_exclude(product: Dict[str, Any]) -> bool:
    title = (product.get("title") or "").lower()
    product_type = (
        product.get("productType")
        or product.get("product_type")
        or ""
    ).lower()
    if contains_any_keyword(title, EXCLUDED_TITLE_KEYWORDS):
        return True
    if contains_any_keyword(product_type, EXCLUDED_PRODUCT_TYPES):
        return True
    return False


def normalize_gid(gid: Optional[str]) -> str:
    if not gid:
        return ""
    return gid.split("/")[-1]


def normalize_measurement_text(text: Optional[str]) -> str:
    if not text:
        return ""
    normalized = html.unescape(text)
    normalized = re.sub(r"<[^>]+>", " ", normalized)
    for bad, replacement in MEASUREMENT_REPLACEMENTS.items():
        normalized = normalized.replace(bad, replacement)
    normalized = normalized.replace("\\\"", '"')
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def format_measure_value(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    if "/" in value and " " in value:
        whole, frac = value.split(" ", 1)
        try:
            num, denom = frac.split("/", 1)
            computed = float(whole) + float(num) / float(denom)
            value = str(computed)
        except Exception:
            pass
    if value.count("/") == 1 and value.replace("/", "").isdigit():
        num, denom = value.split("/", 1)
        try:
            value = str(float(num) / float(denom))
        except Exception:
            pass
    if "." in value:
        value = value.rstrip("0").rstrip(".")
    return value


def extract_numbers_before(label: str, text: Optional[str]) -> List[str]:
    normalized = normalize_measurement_text(text)
    if not normalized:
        return []
    label_pattern = re.escape(label)
    colon_pattern = (
        rf"{label_pattern}\s*:\s*({MEASUREMENT_VALUE_PATTERN})"
        rf"\s*(?:{MEASUREMENT_UNIT_PATTERN})?"
    )
    colon_matches: List[str] = []
    for match in re.finditer(colon_pattern, normalized, flags=re.IGNORECASE):
        value = format_measure_value(match.group(1))
        if value:
            colon_matches.append(value)
    if colon_matches:
        return colon_matches
    matches_with_pos: List[Tuple[int, str]] = []
    after_pattern = (
        rf"{label_pattern}\s*(?:is|=|-)?\s*({MEASUREMENT_VALUE_PATTERN})"
        rf"\s*(?:{MEASUREMENT_UNIT_PATTERN})?"
    )
    for match in re.finditer(after_pattern, normalized, flags=re.IGNORECASE):
        value = format_measure_value(match.group(1))
        if value:
            matches_with_pos.append((match.start(1), value))
    before_pattern = rf"({MEASUREMENT_VALUE_PATTERN})\s*(?:{MEASUREMENT_UNIT_PATTERN})?\s+{label_pattern}"
    for match in re.finditer(before_pattern, normalized, flags=re.IGNORECASE):
        value = format_measure_value(match.group(1))
        if value:
            matches_with_pos.append((match.start(1), value))
    matches_with_pos.sort(key=lambda item: item[0])
    return [value for _, value in matches_with_pos]


def extract_first_number(label: str, text: Optional[str]) -> str:
    matches = extract_numbers_before(label, text)
    return matches[0] if matches else ""


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def determine_jean_style(description: str) -> str:
    desc = (description or "").lower()
    bootcut_phrases = [
        "easy at the knee with a kick flare hem",
        "bootcut",
        "narrow mini boot",
        "slim mini boot",
    ]
    if any(phrase in desc for phrase in bootcut_phrases):
        return "Bootcut"
    if "flare" in desc:
        return "Flare"
    if "skinny" in desc:
        return "Skinny"
    straight_knee_phrases = [
        "slim straight leg",
        "long slim straight",
        "straight leg",
        "wide relaxed straight leg",
    ]
    if any(phrase in desc for phrase in straight_knee_phrases):
        return "Straight from knee"
    straight_thigh_phrases = [
        "loose straight from thigh to hem",
        "wide straight leg",
    ]
    if any(phrase in desc for phrase in straight_thigh_phrases):
        return "Straight from thigh"
    wide_leg_phrases = [
        "wide leg",
        "wide leg from thigh to hem",
        "wide straight ankle",
    ]
    if any(phrase in desc for phrase in wide_leg_phrases):
        return "Wide Leg"
    match = re.search(r"leg:\s*([^\n\r]+)", description or "", re.IGNORECASE)
    if match:
        leg_phrase = match.group(1)
        leg_phrase = leg_phrase.split("Average Height")[0]
        return clean_text(leg_phrase)
    return ""


def determine_product_line(title: str, tags: Sequence[str]) -> str:
    lowered = (title or "").lower()
    tags_lower = [t.lower() for t in tags]
    if "mama" in lowered:
        return "Maternity"
    if "shortie" in lowered:
        return "Petite"
    if any(tag == "core" for tag in tags_lower):
        return "Core"
    return ""


def determine_inseam_label(title: str, description: str, tags: Sequence[str]) -> str:
    title_lower = (title or "").lower()
    desc_lower = (description or "").lower()
    tags_lower = [t.lower() for t in tags]
    if "petite" in desc_lower or "shortie" in title_lower:
        return "Petite"
    if "long" in title_lower or "leg-lengthening" in desc_lower or any("34" in tag for tag in tags_lower):
        return "Long"
    if "average height" in desc_lower:
        return "Regular"
    return ""


def determine_rise_label(description: str) -> str:
    desc = (description or "").lower()
    ultra_phrases = ["extreme high rise", "super high rise", "ultimate high rise"]
    high_phrases = ["regular high rise", "high rise", "high-rise"]
    low_phrases = ["low slung rise", "low rise", "low-rise"]
    mid_phrases = ["mid to low rise", "high or low", "mid rise", "mid-rise"]
    if any(phrase in desc for phrase in ultra_phrases):
        return "Ultra High"
    if any(phrase in desc for phrase in high_phrases):
        return "High"
    if any(phrase in desc for phrase in low_phrases):
        return "Low"
    if any(phrase in desc for phrase in mid_phrases):
        return "Mid"
    return ""


def determine_hem_style(description: str) -> str:
    desc = (description or "").lower()
    if any(phrase in desc for phrase in ("cut hem", "cut hems", "raw hems")):
        return "Raw Hem"
    if any(phrase in desc for phrase in ("finished hems", "finished hem", "clean hem")):
        return "Clean Hem"
    if any(phrase in desc for phrase in ("distressed hem", "grinding along the hems")):
        return "Distressed Hem"
    if "wide hem" in desc:
        return "Wide Hem"
    return ""


def determine_inseam_style(title: str, description: str) -> str:
    title_lower = (title or "").lower()
    if "ankle" in title_lower:
        return "Ankle"
    desc = description or ""
    match = re.search(r"Average Height:\s*([^:]+?)\s*length", desc, re.IGNORECASE)
    if match:
        phrase = clean_text(match.group(1)).lower()
        if phrase in {"full", "below ankle"}:
            return "Full Length"
        if phrase in {"cropped", "crop"}:
            return "Cropped"
        if phrase == "ankle":
            return "Ankle"
        return "Full Length"
    return "Full Length"


def determine_color_simplified(tags: Sequence[str]) -> str:
    tags_lower = [t.lower() for t in tags]
    if any("dark" in tag or "black" in tag for tag in tags_lower):
        return "Dark"
    if any("light" in tag or "white" in tag for tag in tags_lower):
        return "Light"
    if any("medium" in tag for tag in tags_lower):
        return "Medium"
    return ""


def determine_color_standardized(tags: Sequence[str]) -> str:
    tags_lower = [t.lower() for t in tags]
    if any("indigo" in tag or "blue" in tag for tag in tags_lower):
        return "Blue"
    for tag in tags:
        lower = tag.lower()
        if lower.startswith("filter_color_"):
            return tag.split("filter_color_", 1)[1]
    return ""


def extract_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    s = str(value)
    match = re.search(r"[-+]?\d[\d,]*(?:\.\d+)?", s)
    if match:
        return match.group(0).replace(",", "")
    return ""


def chunked(seq: Sequence[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

def fetch_product_feed_handles() -> Set[str]:
    handles: Set[str] = set()
    for collection in PRODUCT_FEED_COLLECTIONS:
        page = 1
        while True:
            params = {"limit": 250, "page": page}
            try:
                resp = perform_shopify_request(
                    "GET", f"/collections/{collection}/products.json", params=params
                )
            except Exception as exc:  # pragma: no cover - network
                logger.warning(
                    "Failed to fetch %s products.json page %s (%s)", collection, page, exc
                )
                break
            payload = resp.json()
            products = payload.get("products") or []
            logger.info(
                "products.json %s page %s returned %s products",
                collection,
                page,
                len(products),
            )
            if not products:
                break
            for product in products:
                if should_exclude(product):
                    continue
                handle = product.get("handle")
                if handle:
                    handles.add(handle)
            page += 1
            time.sleep(0.2)
    logger.info("Discovered %s handles via product feeds", len(handles))
    return handles


def fetch_product_by_handle(handle: str) -> Optional[Dict[str, Any]]:
    data = execute_graphql(PRODUCT_BY_HANDLE_QUERY, {"handle": handle})
    product = data.get("productByHandle")
    if not product:
        logger.warning("productByHandle returned no data for %s", handle)
        return None
    if should_exclude(product):
        logger.info("Skipping handle %s due to exclusion filters", handle)
        return None
    return product


def fetch_collection_products(required_handles: Optional[Set[str]] = None) -> List[Dict[str, Any]]:
    products: Dict[str, Dict[str, Any]] = {}
    seen_handles: Set[str] = set()
    for handle in COLLECTION_HANDLES:
        logger.info("Fetching collection %s", handle)
        cursor: Optional[str] = None
        while True:
            data = execute_graphql(COLLECTION_QUERY, {"handle": handle, "cursor": cursor})
            collection = data.get("collection") or {}
            connection = collection.get("products") or {}
            edges = connection.get("edges") or []
            for edge in edges:
                node = edge.get("node") or {}
                if should_exclude(node):
                    continue
                gid = normalize_gid(node.get("id"))
                if not gid:
                    continue
                prod_handle = node.get("handle")
                if prod_handle:
                    seen_handles.add(prod_handle)
                products[gid] = node
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.2)
    if required_handles:
        missing_handles = sorted(h for h in required_handles if h not in seen_handles)
        if missing_handles:
            logger.info("Fetching %s handles missing from collections", len(missing_handles))
            for handle in missing_handles:
                product = fetch_product_by_handle(handle)
                if not product:
                    continue
                gid = normalize_gid(product.get("id"))
                if not gid:
                    continue
                products[gid] = product
                seen_handles.add(product.get("handle") or "")
    logger.info("Fetched %s unique products", len(products))
    return list(products.values())


def fetch_rebuy_settings() -> Tuple[str, str]:
    params = {"shop": REBUY_SHOP, "id": REBUY_WIDGET_ID}
    for attempt in range(5):
        try:
            resp = rebuy_session.get(
                f"{REBUY_BASE}/api/v1/widgets/settings",
                params=params,
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )
            if resp.status_code in TRANSIENT_STATUS:
                raise requests.HTTPError(f"transient status {resp.status_code}")
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            key = data.get("key")
            endpoint = data.get("endpoint")
            if not key or not endpoint:
                raise RuntimeError("Rebuy settings missing key or endpoint")
            return key, endpoint
        except Exception as exc:  # pragma: no cover - network path
            sleep_for = min(8.0, 1.5 * (2 ** attempt))
            logger.warning("Rebuy settings fetch failed (%s); sleeping %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError("Unable to fetch Rebuy settings")


def fetch_rebuy_inventory(product_ids: Sequence[str]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    if not product_ids:
        return {}, {}
    key, endpoint = fetch_rebuy_settings()
    variant_map: Dict[str, Dict[str, Any]] = {}
    product_totals: Dict[str, int] = defaultdict(int)
    for chunk in chunked(list(product_ids), REBUY_CHUNK_SIZE):
        params = {
            "shop": REBUY_SHOP,
            "key": key,
            "shopify_product_ids": ",".join(chunk),
        }
        for attempt in range(5):
            try:
                resp = rebuy_session.get(
                    f"{REBUY_BASE}/api/v1{endpoint}",
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                    verify=False,
                )
                if resp.status_code in TRANSIENT_STATUS:
                    raise requests.HTTPError(f"transient status {resp.status_code}")
                resp.raise_for_status()
                payload = resp.json()
                meta = payload.get("metadata") or {}
                for product in meta.get("input_products", []):
                    pid = str(product.get("id") or product.get("product_id") or "")
                    total = 0
                    for variant in product.get("variants", []):
                        vid = str(variant.get("id"))
                        qty = variant.get("inventory_quantity")
                        old_qty = variant.get("old_inventory_quantity")
                        if qty is not None:
                            try:
                                qty_int = int(qty)
                            except (TypeError, ValueError):
                                qty_int = None
                        else:
                            qty_int = None
                        if qty_int is not None:
                            total += qty_int
                        variant_map[vid] = {
                            "qty": qty_int,
                            "old_qty": old_qty,
                            "product_id": pid,
                        }
                    if pid:
                        product_totals[pid] = max(product_totals.get(pid, 0), total)
                break
            except Exception as exc:  # pragma: no cover - network path
                sleep_for = min(8.0, 1.5 * (2 ** attempt))
                logger.warning(
                    "Rebuy inventory chunk %s failed (%s); sleeping %.1fs", chunk, exc, sleep_for
                )
                time.sleep(sleep_for)
        else:
            logger.error("Failed to fetch Rebuy inventory for chunk %s", chunk)
    return variant_map, product_totals


# ---------------------------------------------------------------------------
# Row building
# ---------------------------------------------------------------------------

def build_rows(products: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    product_ids = sorted({normalize_gid(p.get("id")) for p in products if p.get("id")})
    rebuy_variants, rebuy_totals = fetch_rebuy_inventory(product_ids)
    rows: List[Dict[str, str]] = []
    for product in products:
        product_gid = normalize_gid(product.get("id"))
        if not product_gid:
            continue
        title = product.get("title") or ""
        description = product.get("description") or ""
        tags = product.get("tags") or []
        vendor = product.get("vendor") or ""
        product_type = product.get("productType") or ""
        handle = product.get("handle") or ""
        product_url = product.get("onlineStoreUrl") or ""
        featured_image = ((product.get("featuredImage") or {}).get("url")) or ""
        published_at = parse_date(product.get("publishedAt"))
        created_at = parse_date(product.get("createdAt"))
        tags_joined = ", ".join(tags)
        rise = extract_first_number("rise", description)
        inseam = extract_first_number("inseam", description)
        knee = extract_first_number("knee", description)
        leg_matches = extract_numbers_before("leg opening", description)
        leg_opening = leg_matches[0] if leg_matches else ""
        if not knee and len(leg_matches) >= 2:
            try:
                nums = sorted(float(val) for val in leg_matches)
                knee = str(nums[0]).rstrip("0").rstrip(".") if nums else ""
                leg_opening = str(nums[-1]).rstrip("0").rstrip(".") if nums else leg_opening
            except ValueError:
                pass
        inseam_value = parse_float(inseam)
        product_type_output = (
            "Shorts" if inseam_value is not None and inseam_value < 8 else product_type
        )
        jean_style = determine_jean_style(description)
        product_line = determine_product_line(title, tags)
        inseam_label = determine_inseam_label(title, description, tags)
        rise_label = determine_rise_label(description)
        hem_style = determine_hem_style(description)
        inseam_style = determine_inseam_style(title, description)
        color_simplified = determine_color_simplified(tags)
        color_standardized = determine_color_standardized(tags)

        variants = (product.get("variants") or {}).get("edges") or []
        style_total = rebuy_totals.get(product_gid)
        if style_total is None:
            total = 0
            for edge in variants:
                node = edge.get("node") or {}
                qty = node.get("quantityAvailable")
                if qty is None:
                    continue
                try:
                    total += int(qty)
                except (TypeError, ValueError):
                    continue
            style_total = total if total else 0
        for edge in variants:
            node = edge.get("node") or {}
            variant_gid = normalize_gid(node.get("id"))
            sku = node.get("sku") or ""
            barcode = node.get("barcode") or ""
            available = node.get("availableForSale")
            price = extract_price(((node.get("price") or {}).get("amount")))
            compare_at = extract_price(((node.get("compareAtPrice") or {}).get("amount")))
            image_url = (node.get("image") or {}).get("url") or featured_image
            selected_options = node.get("selectedOptions") or []
            color = ""
            size = ""
            for option in selected_options:
                name = (option.get("name") or "").lower()
                value = option.get("value") or ""
                if name == "color":
                    color = value
                elif name == "size":
                    size = value
            if not color:
                color = next((opt.get("value") for opt in selected_options if opt.get("value")), "")
            rebuy_entry = rebuy_variants.get(variant_gid)
            qty_available = rebuy_entry.get("qty") if rebuy_entry else None
            old_qty = rebuy_entry.get("old_qty") if rebuy_entry else None
            if qty_available is None:
                qty_available = node.get("quantityAvailable")
            row = {
                "Style Id": product_gid,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": clean_text(f"{title} / {color.upper()}" if color else title),
                "Style Name": title,
                "Product Type": product_type_output,
                "Tags": tags_joined,
                "Vendor": vendor,
                "Description": clean_text(description),
                "Variant Title": clean_text(
                    f"{title} / {color.upper()} / {size}".rstrip(" /") if color or size else f"{title} / {node.get('title', '')}"
                ),
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Knee": knee,
                "Leg Opening": leg_opening,
                "Price": price,
                "Compare at Price": compare_at,
                "Available for Sale": "TRUE" if available else "FALSE",
                "Quantity Available": "" if qty_available is None else str(qty_available),
                "Old Quantity Available": "" if old_qty in (None, "") else str(old_qty),
                "Quantity of style": str(style_total or 0),
                "SKU - Shopify": variant_gid,
                "SKU - Brand": sku,
                "Barcode": barcode,
                "Image URL": image_url,
                "SKU URL": product_url,
                "Jean Style": jean_style,
                "Product Line": product_line,
                "Inseam Label": inseam_label,
                "Rise Label": rise_label,
                "Hem Style": hem_style,
                "Inseam Style": inseam_style,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
            }
            rows.append(row)
    logger.info("Prepared %s rows", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    configure_logging()
    feed_handles = fetch_product_feed_handles()
    products = fetch_collection_products(feed_handles or None)
    rows = build_rows(products)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"FAVORITEDAUGHTER_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logger.info("CSV written: %s", output_path.resolve())
    print(f"CSV written: {output_path.resolve()}")


if __name__ == "__main__":
    main()
