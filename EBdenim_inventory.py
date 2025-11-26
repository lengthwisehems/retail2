import csv
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import uuid
import time
import warnings

import requests
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from urllib3.exceptions import InsecureRequestWarning

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND = "EBdenim"
LOG_FILE = BASE_DIR / f"{BRAND.lower()}_run.log"

BASE_HOSTS = [
    "https://www.ebdenim.com",
    "https://ebdenim.com",
]
STORE_HOST = "https://ebdenim-com.myshopify.com"
COLLECTION_HANDLES = ["pants", "archive-sale"]
GRAPHQL_ENDPOINTS = [f"{STORE_HOST}/api/unstable/graphql.json"]
GRAPHQL_TOKEN = "c23e8a3393b4e6bdd4dbd9eede820604"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}

BANNED_TITLE_WORDS = [
    "dress",
    "shorts",
    "skirt",
    "jacket",
    "shirt",
    "vest",
    "tee",
    "tank",
    "neck",
    "sweatpant",
    "sweater",
    "long sleeve",
    "zip up",
    "corset",
    "trench",
]
BANNED_CATEGORIES = {
    "T-Shirts",
    "Tank Tops",
    "Shorts",
    "Outerwear",
    "Shirts & Tops",
    "Dresses",
    "Shirts",
    "Skirts",
    "Sweatpants",
    "Sweaters",
    "Hoodies",
    "Blouses",
    "Coats & Jackets",
}

STYLE_NAME_CUTOFFS = [
    "Pant",
    "Inseam",
    "Relaxed",
    "Baggy",
    "Barrel",
    "Bootcut",
    "Bowed",
    "Carpenter",
    "Cigarette",
    "Jegging",
    "Straight",
]
COLOR_STARTERS = [
    "Pant",
    "Inseam",
    "Relaxed",
    "Baggy",
    "Barrel",
    "Bootcut",
    "Bowed",
    "Carpenter",
    "Cigarette",
    "Jegging",
    "Straight",
]
TOKEN_REGEX = re.compile(r"\b[0-9a-f]{32}\b")
FLOAT_REGEX = re.compile(r"([-+]?[0-9]*\.?[0-9]+)")
REBUY_BASE = "https://rebuyengine.com"
REBUY_SHOP = "ebdenim-com.myshopify.com"
REBUY_WIDGET_ID = "199459"
REBUY_API_KEY_FALLBACK = "1b2815331f7a0f6b6adec0f17736d2b7dc52cc8d"
REBUY_CHUNK_SIZE = 20
REQUEST_TIMEOUT = 40
TRANSIENT_STATUS = {429, 500, 502, 503, 504}

@dataclass
class Product:
    id: str
    handle: str
    title: str
    published_at: str
    created_at: str
    description: str
    tags: List[str]
    vendor: str
    product_type: str
    category_name: Optional[str]
    seo_title: Optional[str]
    featured_alt: Optional[str]
    featured_url: Optional[str]
    online_url: Optional[str]

@dataclass
class Variant:
    product_id: str
    variant_id: str
    title: str
    sku: str
    barcode: str
    available: bool
    price: str
    compare_at: str
    size: str
    image_url: Optional[str]


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(BRAND)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    log_path = LOG_FILE
    try:
        fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        fallback = OUTPUT_DIR / f"{BRAND.lower()}_run.log"
        logger.warning("Primary log path unavailable, falling back to %s", fallback)
        fh = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def build_session() -> Session:
    session = requests.Session()
    warnings.simplefilter("ignore", InsecureRequestWarning)
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    session.verify = False
    return session


def clean_html(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def to_mmddyy(dt_str: str) -> str:
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except Exception:
        return dt_str


def parse_decimal(val: Optional[str]) -> str:
    if not val:
        return ""
    try:
        return str(Decimal(val))
    except InvalidOperation:
        match = FLOAT_REGEX.search(val)
        if match:
            try:
                return str(Decimal(match.group(1)))
            except InvalidOperation:
                return match.group(1)
    return ""


def chunked(seq: Sequence[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), size):
        yield list(seq[i : i + size])


def discover_rebuy_credentials(session: Session, logger: logging.Logger) -> Optional[Tuple[str, str]]:
    pattern = re.compile(r"https://rebuyengine\.com/api/v1/custom/id/(\d+)\?key=([0-9a-f]{40})", re.I)
    for handle in COLLECTION_HANDLES:
        for host in BASE_HOSTS:
            url = f"{host}/collections/{handle}"
            try:
                resp = session.get(url, timeout=REQUEST_TIMEOUT, verify=False)
                resp.raise_for_status()
            except Exception:
                continue
            match = pattern.search(resp.text)
            if match:
                widget_id, key = match.group(1), match.group(2)
                logger.info("Discovered Rebuy widget %s from %s", widget_id, url)
                return key, f"/custom/id/{widget_id}"
    return None


def extract_after(label: str, text: str) -> str:
    pattern = re.compile(rf"{re.escape(label)}\s*([0-9]+(?:\.[0-9]+)?)", re.I)
    m = pattern.search(text)
    if m:
        return m.group(1)
    return ""


def determine_style_name(product: Product) -> str:
    if product.seo_title:
        name = product.seo_title.split("|")[0].strip()
    title = product.title
    for marker in STYLE_NAME_CUTOFFS:
        idx = title.lower().find(marker.lower())
        if idx != -1:
            end = idx + len(marker)
            name = title[:end].strip()
            break
    else:
        name = title.strip()
    name = re.sub(r"\bEagle\b", "", name, flags=re.I).strip()
    name = re.sub(r"\s{2,}", " ", name)
    return name


def determine_color(product: Product) -> str:
    if product.featured_alt:
        return product.featured_alt.strip()
    title = product.title
    for marker in COLOR_STARTERS:
        idx = title.lower().find(marker.lower())
        if idx != -1:
            return title[idx + len(marker):].strip().lstrip("- ")
    return ""


def determine_jean_style(title: str, description: str) -> str:
    t = title.lower()
    d = description.lower()
    if "capri" in t:
        return "Capri"
    if "straight" in d and ("baggy" in d or "relax" in d):
        return "Straight From Thigh"
    if ("straight" in d and "slim" in d) or "cigarette" in t:
        return "Straight From Knee"
    if "straight" in t:
        return "Straight"
    if "wide" in t:
        return "Wide Leg"
    if "flare" in t:
        return "Flare"
    if "bootcut" in t:
        return "Bootcut"
    if "skinny" in t:
        return "Skinny"
    if "barrel" in t or "bowed" in t or "carpenter" in d:
        return "Barrel"
    if "baggy" in t or "work pant" in t:
        return "Baggy"
    if "straight" in d:
        return "Straight"
    if "wide" in d:
        return "Wide Leg"
    if "flare" in d:
        return "Flare"
    if "bootcut" in d or "boot-cut" in d:
        return "Bootcut"
    if "skinny" in d:
        return "Skinny"
    if "barrel" in d or "bowed" in d:
        return "Barrel"
    if "baggy" in d:
        return "Baggy"
    return ""


def determine_inseam_label(title: str) -> str:
    t = title.lower()
    if "29\" inseam" in t or "petite" in t:
        return "Petite"
    return ""


def determine_inseam_style(title: str, description: str) -> str:
    t = title.lower()
    d = description.lower()
    if "cropped" in t or "crop" in t or "cropped" in d or "crop" in d:
        return "Cropped"
    if "ankle" in t or "ankle" in d:
        return "Ankle"
    if "capri" in t or "capri" in d:
        return "Capri"
    return "Full Length"


def determine_rise_label(title: str, description: str, tags: List[str]) -> str:
    t = title.lower()
    d = description.lower()
    tags_lower = [x.lower() for x in tags]
    if "high rise" in t:
        return "High"
    if "mid rise" in t:
        return "Mid"
    if "low rise" in t or "slouchy" in t:
        return "Low"
    if "high-" in d:
        return "High"
    if "mid-" in d:
        return "Mid"
    if "low-" in d or "loose on" in d:
        return "Low"
    if any("high rise" in tg for tg in tags_lower):
        return "High"
    if any("mid rise" in tg for tg in tags_lower):
        return "Mid"
    if any("low rise" in tg or "low waist" in tg for tg in tags_lower):
        return "Low"
    return ""


def determine_hem_style(description: str) -> str:
    d = description.lower()
    if any(k in d for k in ["cut hem", "cut hems", "raw hems"]):
        return "Raw Hem"
    if any(k in d for k in ["finished hems", "finished hem", "clean hem"]):
        return "Clean Hem"
    if any(k in d for k in ["distressed hem", "grinding along the hems"]):
        return "Distressed Hem"
    if "wide hem" in d:
        return "Wide Hem"
    if "split hem" in d:
        return "Split Hem"
    if "adjustable tie hem" in d:
        return "Drawstring Hem"
    return ""


def determine_color_simplified(tags: List[str]) -> Tuple[str, str]:
    simplified = ""
    standardized = ""
    tgs = [t.lower() for t in tags]
    if any("black" in t for t in tgs):
        simplified = simplified or "Dark"
        standardized = standardized or "Black"
    if any("dark blue" in t or "dark wash" in t for t in tgs):
        simplified = simplified or "Dark"
    if any("white" in t or "light blue" in t or "light wash" in t for t in tgs):
        simplified = simplified or "Light"
        if not standardized:
            standardized = "White"
    if any("medium wash" in t for t in tgs):
        simplified = simplified or "Medium"
    color_map = {
        "blue": "Blue",
        "grey": "Grey",
        "gray": "Grey",
        "white": "White",
        "yellow": "Yellow",
        "purple": "Purple",
        "green": "Green",
        "red": "Red",
    }
    for key, val in color_map.items():
        if any(key in t for t in tgs):
            standardized = standardized or val
            if key == "blue" and not simplified:
                simplified = ""
            break
    return simplified, standardized


def should_exclude(product: Product) -> bool:
    title_lower = product.title.lower()
    if any(word in title_lower for word in BANNED_TITLE_WORDS):
        return True
    cat = (product.category_name or "").strip()
    if cat in BANNED_CATEGORIES:
        return True
    return False


def request_json(session: Session, url: str, logger: logging.Logger, method: str = "get", **kwargs) -> Dict[str, Any]:
    kwargs.setdefault("verify", False)
    resp = session.request(method.upper(), url, **kwargs)
    resp.raise_for_status()
    return resp.json()


def fetch_graphql_products(session: Session, logger: logging.Logger) -> Tuple[Dict[str, Product], Dict[str, List[Variant]]]:
    products: Dict[str, Product] = {}
    variants: Dict[str, List[Variant]] = {}

    query = """
    query CollectionProducts($handle: String!, $cursor: String) {
      collection(handle: $handle) {
        products(first: 100, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id
              handle
              title
              description
              vendor
              tags
              productType
              category { name }
              seo { title }
              onlineStoreUrl
              featuredImage { url altText }
              createdAt
              publishedAt
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
                    selectedOptions { name value }
                    image { url }
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    headers = {**HEADERS, "X-Shopify-Storefront-Access-Token": GRAPHQL_TOKEN}

    for collection_handle in COLLECTION_HANDLES:
        cursor: Optional[str] = None
        logger.info("Fetching collection %s via GraphQL", collection_handle)
        while True:
            payload = {"query": query, "variables": {"handle": collection_handle, "cursor": cursor}}
            resp = session.post(GRAPHQL_ENDPOINTS[0], json=payload, headers=headers, verify=False)
            if resp.status_code != 200:
                logger.error("GraphQL request failed: %s", resp.text)
                break
            data = resp.json()
            prod_edges = data.get("data", {}).get("collection", {}) or {}
            prod_edges = prod_edges.get("products", {}) if prod_edges else {}
            edges = prod_edges.get("edges", []) if prod_edges else []
            for edge in edges:
                node = edge.get("node", {})
                prod_id = node.get("id", "")
                product = Product(
                    id=prod_id,
                    handle=node.get("handle", ""),
                    title=node.get("title", ""),
                    description=clean_html(node.get("description", "")),
                    vendor=node.get("vendor", ""),
                    tags=node.get("tags", []) or [],
                    product_type=node.get("productType", ""),
                    category_name=(node.get("category") or {}).get("name"),
                    seo_title=(node.get("seo") or {}).get("title"),
                    featured_alt=(node.get("featuredImage") or {}).get("altText"),
                    featured_url=(node.get("featuredImage") or {}).get("url"),
                    online_url=node.get("onlineStoreUrl"),
                    created_at=node.get("createdAt", ""),
                    published_at=node.get("publishedAt", ""),
                )
                if should_exclude(product):
                    continue
                products[prod_id] = product
                var_edges = (node.get("variants") or {}).get("edges", [])
                var_list: List[Variant] = []
                for vedge in var_edges:
                    vnode = vedge.get("node", {})
                    selected = vnode.get("selectedOptions") or []
                    size_val = ""
                    for opt in selected:
                        if opt.get("name", "").lower() in {"size", "option1"}:
                            size_val = opt.get("value", "")
                            break
                    variant = Variant(
                        product_id=prod_id,
                        variant_id=vnode.get("id", ""),
                        title=vnode.get("title", ""),
                        sku=vnode.get("sku", "") or "",
                        barcode=vnode.get("barcode", "") or "",
                        available=bool(vnode.get("availableForSale")),
                        price=parse_decimal(((vnode.get("price") or {}).get("amount"))),
                        compare_at=parse_decimal(((vnode.get("compareAtPrice") or {}).get("amount"))),
                        size=size_val or vnode.get("title", ""),
                        image_url=(vnode.get("image") or {}).get("url"),
                    )
                    var_list.append(variant)
                variants[prod_id] = var_list
            page_info = prod_edges.get("pageInfo") if prod_edges else None
            if page_info and page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break
    return products, variants


def fetch_rebuy_settings(session: Session, logger: logging.Logger) -> Tuple[str, str]:
    params = {"shop": REBUY_SHOP, "id": REBUY_WIDGET_ID}
    for attempt in range(5):
        try:
            resp = session.get(
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
        except Exception as exc:
            sleep_for = min(8.0, 1.5 * (2**attempt))
            logger.warning("Rebuy settings fetch failed (%s); sleeping %.1fs", exc, sleep_for)
            time.sleep(sleep_for)
    raise RuntimeError("Unable to fetch Rebuy settings")


def fetch_rebuy_inventory(
    session: Session, products: Dict[str, Product], logger: logging.Logger
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
    if not products:
        return {}, {}
    credentials: List[Tuple[str, str]] = []
    discovered = discover_rebuy_credentials(session, logger)
    if discovered:
        credentials.append(discovered)
    try:
        credentials.append(fetch_rebuy_settings(session, logger))
    except Exception as exc:
        logger.warning("Rebuy settings unavailable (%s)", exc)
    if REBUY_API_KEY_FALLBACK:
        credentials.append((REBUY_API_KEY_FALLBACK, f"/custom/id/{REBUY_WIDGET_ID}"))

    variant_map: Dict[str, Dict[str, Any]] = {}
    product_totals: Dict[str, int] = defaultdict(int)

    product_ids = [p.id.split("/")[-1] for p in products.values()]
    collection_url = f"{BASE_HOSTS[0]}/collections/{COLLECTION_HANDLES[0]}"
    for key, endpoint in credentials:
        variant_map.clear()
        product_totals.clear()
        for chunk in chunked(product_ids, REBUY_CHUNK_SIZE):
            params = {
                "shop": REBUY_SHOP,
                "key": key,
                "shopify_product_ids": ",".join(chunk),
                "product_groups": "yes",
                "limit": 250,
                "uuid": str(uuid.uuid4()),
                "url": collection_url,
            }
            for attempt in range(5):
                try:
                    resp = session.get(
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
                    for prod in meta.get("input_products", []):
                        rebuy_pid = str(prod.get("id") or prod.get("product_id") or "")
                        total = 0
                        for variant in prod.get("variants", []):
                            sku = (variant.get("sku") or "").strip().upper()
                            if sku == "ROUTEINS":
                                continue
                            vid = str(variant.get("id"))
                            qty_raw = variant.get("inventory_quantity")
                            old_qty_raw = variant.get("old_inventory_quantity")
                            qty_int: Optional[int]
                            try:
                                qty_int = int(qty_raw)
                            except Exception:
                                qty_int = None
                            if qty_int is not None and qty_int > 0:
                                total += qty_int
                            variant_map[vid] = {
                                "qty": qty_int,
                                "old_qty": old_qty_raw,
                                "product_id": rebuy_pid,
                            }
                        if rebuy_pid:
                            product_totals[rebuy_pid] = max(product_totals.get(rebuy_pid, 0), total)
                    break
                except Exception as exc:
                    sleep_for = min(8.0, 1.5 * (2**attempt))
                    logger.warning(
                        "Rebuy inventory chunk %s failed (%s); sleeping %.1fs", chunk, exc, sleep_for
                    )
                    time.sleep(sleep_for)
            else:
                logger.error("Failed to fetch Rebuy inventory for chunk %s", chunk)
        if variant_map:
            break
        logger.info("Rebuy attempt with endpoint %s returned no variants; trying next credential", endpoint)
    return variant_map.copy(), product_totals.copy()


def build_rows(
    products: Dict[str, Product],
    variants: Dict[str, List[Variant]],
    rebuy_variants: Dict[str, Dict[str, Any]],
    rebuy_totals: Dict[str, int],
) -> List[List[str]]:
    rows: List[List[str]] = []
    for pid, product in products.items():
        var_list = variants.get(pid, [])
        product_gid = pid.split("/")[-1]
        style_total = rebuy_totals.get(product_gid, 0)
        for var in var_list:
            vid = var.variant_id.split("/")[-1]
            qty_data = rebuy_variants.get(vid, {})
            qty_raw = qty_data.get("qty")
            old_raw = qty_data.get("old_qty")
            qty_available: Optional[int] = None
            notify = 0
            if qty_raw is not None:
                try:
                    qty_int = int(qty_raw)
                    if qty_int < 0:
                        notify = abs(qty_int)
                        qty_available = 0
                    else:
                        qty_available = qty_int
                except Exception:
                    qty_available = None
            old_qty = None
            if old_raw is not None:
                try:
                    old_int = int(old_raw)
                    if old_int >= 0:
                        old_qty = old_int
                except Exception:
                    old_qty = None
            rise = extract_after("Rise:", product.description)
            inseam = extract_after("Inseam:", product.description)
            leg_opening = extract_after("Leg Opening:", product.description)
            size = var.size
            product_title = product.title
            variant_title = f"{product_title} - {size}".strip(" -")
            style_name = determine_style_name(product)
            color = determine_color(product)
            jean_style = determine_jean_style(product.title, product.description)
            inseam_label = determine_inseam_label(product.title)
            inseam_style = determine_inseam_style(product.title, product.description)
            rise_label = determine_rise_label(product.title, product.description, product.tags)
            hem_style = determine_hem_style(product.description)
            color_simple, color_std = determine_color_simplified(product.tags)
            row = [
                pid.split("/")[-1],
                product.handle,
                to_mmddyy(product.published_at),
                to_mmddyy(product.created_at),
                product.title,
                style_name,
                product.category_name or product.product_type,
                ", ".join(product.tags),
                product.vendor,
                product.description,
                variant_title,
                color,
                size,
                rise,
                inseam,
                leg_opening,
                var.price,
                var.compare_at,
                "TRUE" if var.available else "FALSE",
                "" if qty_available is None else str(qty_available),
                "" if old_qty is None else str(old_qty),
                str(notify),
                str(style_total),
                var.variant_id.split("/")[-1],
                var.sku,
                var.barcode,
                var.image_url or product.featured_url or "",
                product.online_url or f"{BASE_HOSTS[0]}/products/{product.handle}",
                jean_style,
                inseam_label,
                rise_label,
                hem_style,
                inseam_style,
                color_simple,
                color_std,
                "Stretch" if any("stretch" in t.lower() for t in product.tags) else "",
            ]
            rows.append(row)
    return rows


def write_csv(rows: List[List[str]], logger: logging.Logger) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"EBdenim_{timestamp}.csv"
    headers = [
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
        "Leg Opening",
        "Price",
        "Compare at Price",
        "Available for Sale",
        "Quantity Available",
        "Old Quantity Available",
        "Notify Me Signups",
        "Quantity of style",
        "SKU - Shopify",
        "SKU - Brand",
        "Barcode",
        "Image URL",
        "SKU URL",
        "Jean Style",
        "Inseam Label",
        "Rise Label",
        "Hem Style",
        "Inseam Style",
        "Color - Simplified",
        "Color - Standardized",
        "Stretch",
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    logger.info("CSV written: %s", out_path.resolve())


def main():
    logger = setup_logger()
    session = build_session()
    try:
        products, variants = fetch_graphql_products(session, logger)
    except Exception as exc:
        logger.error("GraphQL fetch failed: %s", exc)
        return
    rebuy_variants, rebuy_totals = fetch_rebuy_inventory(session, products, logger)
    rows = build_rows(products, variants, rebuy_variants, rebuy_totals)
    write_csv(rows, logger)


if __name__ == "__main__":
    main()
