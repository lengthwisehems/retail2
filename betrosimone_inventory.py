import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH = BASE_DIR / "betrosimone_run.log"

PRIMARY_LOG_PATH = LOG_PATH
FALLBACK_LOG_PATH = OUTPUT_DIR / "betrosimone_run.log"

try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(PRIMARY_LOG_PATH, mode="a", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
except Exception as exc:  # noqa: BLE001
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(FALLBACK_LOG_PATH, mode="a", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    logging.warning("Falling back to fallback log path due to: %s", exc)

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
})
# Some endpoints present incomplete certificate chains; disable verification to keep runs stable.
SESSION.verify = False
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

HOSTS = ["https://betrosimone.com", "https://betro-simone.myshopify.com"]
API_ENDPOINTS = [
    "https://betrosimone.com/api/unstable/graphql.json",
    "https://betro-simone.myshopify.com/api/unstable/graphql.json",
]
API_TOKEN = "c47591a44ab2eae6135584afff99dddf"

GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 100, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          id
          handle
          title
          productType
          tags
          vendor
          description
          publishedAt
          createdAt
          onlineStoreUrl
          variants(first: 100) {
            edges {
              node {
                id
                title
                sku
                barcode
                availableForSale
                price {
                  amount
                }
                compareAtPrice {
                  amount
                }
                image {
                  url
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

VARIANT_BLOCK_RE = re.compile(r"variants\s*:\s*\[(?P<body>.*?)\]\s*,?\s*\}", re.S)
VARIANT_OBJ_RE = re.compile(r"\{[^{}]*?\bid\s*:\s*(?P<id>\d+)[^{}]*?\bquantity\s*:\s*(?P<qty>-?\d+)[^{}]*?\}", re.S)

TITLE_FILTER_WORDS = ["dress", "short", "skirt", "jacket", "shirt", "vest", "tee"]
PRODUCT_TYPE_EXCLUDE = {"Jackets", "Shirts & Tops", "Skirts", "Shorts"}
TAG_EXCLUDE = {
    "buttoned vest",
    "crop vest",
    "jacket",
    "jackets",
    "slim vest",
    "tops",
    "vest",
    "blazer",
    "coat",
    "fall jacket",
    "long sleeve top",
    "long sleeve tops",
    "skirts",
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
    "Price",
    "Compare at Price",
    "Available for Sale",
    "Quantity Available",
    "Notify Me Signups",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Rise Label",
]


def parse_shopify_id(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]


def format_date(date_str: Optional[str]) -> str:
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%-m/%-d/%Y")
    except Exception:  # noqa: BLE001
        return date_str


def clean_html(text: str) -> str:
    soup = BeautifulSoup(text or "", "html.parser")
    return soup.get_text(" ", strip=True)


def graphql_request(variables: Dict[str, Optional[str]]) -> Dict:
    for endpoint in API_ENDPOINTS:
        for attempt in range(5):
            try:
                resp = SESSION.post(
                    endpoint,
                    headers={"X-Shopify-Storefront-Access-Token": API_TOKEN},
                    json={"query": GRAPHQL_QUERY, "variables": variables},
                    timeout=30,
                    verify=False,
                )
                if resp.status_code in {429, 500, 502, 503, 504}:
                    sleep = 2 ** attempt
                    logger.warning("GraphQL %s returned %s; sleeping %s", endpoint, resp.status_code, sleep)
                    time.sleep(sleep)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if "errors" in data:
                    logger.error("GraphQL errors from %s: %s", endpoint, data["errors"])
                    break
                return data
            except Exception as exc:  # noqa: BLE001
                sleep = 2 ** attempt
                logger.warning("GraphQL request to %s failed (%s); retrying in %s", endpoint, exc, sleep)
                time.sleep(sleep)
    raise RuntimeError("GraphQL requests failed for all endpoints")


def fetch_collection_products() -> List[Dict]:
    products: List[Dict] = []
    cursor = None
    page = 0
    while True:
        page += 1
        logger.info("Fetching collection page %s", page)
        data = graphql_request({"handle": "denim", "cursor": cursor})
        collection = data.get("data", {}).get("collection")
        if not collection:
            break
        prod_edges = collection.get("products", {}).get("edges", [])
        for edge in prod_edges:
            products.append(edge["node"])
        page_info = collection.get("products", {}).get("pageInfo", {})
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break
    logger.info("Total products fetched: %s", len(products))
    return products


def should_exclude(title: str, product_type: str, tags: List[str]) -> bool:
    lt = title.lower()
    if any(word in lt for word in TITLE_FILTER_WORDS):
        return True
    if product_type in PRODUCT_TYPE_EXCLUDE:
        return True
    lowered_tags = [t.lower() for t in tags]
    for tag in lowered_tags:
        if any(ex in tag for ex in TAG_EXCLUDE):
            return True
    return False


def extract_restock_quantities_from_html(html_text: str) -> Dict[str, int]:
    if "_ReStockConfig.product" not in html_text:
        return {}
    m = VARIANT_BLOCK_RE.search(html_text)
    if not m:
        return {}
    body = m.group("body")
    out: Dict[str, int] = {}
    for mo in VARIANT_OBJ_RE.finditer(body):
        vid = mo.group("id")
        qty = int(mo.group("qty"))
        out[vid] = qty
    return out


def fetch_restock_quantities(handle: str) -> Dict[str, int]:
    for host in HOSTS:
        url = f"{host}/products/{handle}"
        try:
            resp = SESSION.get(url, timeout=30, verify=False)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            return extract_restock_quantities_from_html(resp.text)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch Restock config from %s (%s)", url, exc)
    return {}


def derive_jean_style(title: str, description: str) -> str:
    ttl = title.lower()
    desc = description.lower()
    if "barrel" in ttl:
        return "Barrel"
    if "wide" in ttl:
        return "Wide Leg"
    if "baggy" in ttl:
        return "Baggy"
    if "flare" in ttl:
        return "Flare"
    if "skinny" in ttl:
        return "Skinny"
    if "straight" in ttl and "slim" not in desc:
        return "Straight"
    if "slim" in desc and "straight" in desc:
        return "Straight from knee"
    return ""


def derive_rise_label(tags: List[str], description: str, title: str) -> str:
    desc = description.lower()
    lowered_tags = {t.lower() for t in tags}
    title_lower = title.lower()
    options: List[Tuple[str, str]] = []
    high_tokens = {"high-rise", "highrise", "rise:high", "rise:ultrahighrise", "ultrahighrise"}
    low_tokens = {"low-rise", "lowrise", "rise:low"}
    mid_tokens = {"mid-rise", "rise:mid", "midrise"}
    if any(tok in lowered_tags or tok in desc for tok in high_tokens):
        options.append(("high", "High"))
    if any(tok in lowered_tags or tok in desc for tok in low_tokens):
        options.append(("low", "Low"))
    if any(tok in lowered_tags or tok in desc for tok in mid_tokens):
        options.append(("mid", "Mid"))
    if not options:
        return ""
    # Prefer the one whose keyword appears closest to title content
    title_pref = title_lower
    for key, label in options:
        if key in title_pref:
            return label
    return options[0][1]


def write_csv(rows: List[Dict[str, str]]) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"BETROSIMONE_{timestamp}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("CSV written: %s", out_path.resolve())


def build_rows(products: List[Dict]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for product in products:
        title = product.get("title", "")
        product_type = product.get("productType", "")
        tags = product.get("tags", []) or []
        if should_exclude(title, product_type, tags):
            continue
        description_raw = product.get("description", "")
        description = clean_html(description_raw)
        style_id = parse_shopify_id(product.get("id", ""))
        handle = product.get("handle", "")
        published_at = format_date(product.get("publishedAt"))
        created_at = format_date(product.get("createdAt"))
        vendor = product.get("vendor", "")
        variants = product.get("variants", {}).get("edges", [])
        restock_map = fetch_restock_quantities(handle)
        variant_quantities: Dict[str, int] = {}
        for vid, qty in restock_map.items():
            variant_quantities[vid] = qty
        style_total = sum(q for q in variant_quantities.values() if q > 0)
        jean_style = derive_jean_style(title, description)
        rise_label = derive_rise_label(tags, description, title)
        for edge in variants:
            node = edge["node"]
            variant_gid = node.get("id", "")
            variant_id = parse_shopify_id(variant_gid)
            variant_title = node.get("title", "")
            # Variant title is in format "Color / Size"
            color = variant_title.split("/")[0].strip() if "/" in variant_title else variant_title.strip()
            size = variant_title.split("/")[-1].strip() if "/" in variant_title else ""
            product_name = f"{title} - {color}" if color else title
            style_name = title.split()[0] if title.split() else title
            variant_title_out = f"{product_name} - {size}" if size else product_name
            price_data = node.get("price") or {}
            compare_data = node.get("compareAtPrice") or {}
            price_amount = price_data.get("amount") if isinstance(price_data, dict) else ""
            compare_amount = compare_data.get("amount") if isinstance(compare_data, dict) else ""
            available = node.get("availableForSale", False)
            quantity_raw = variant_quantities.get(variant_id)
            quantity_available = quantity_raw if quantity_raw is not None and quantity_raw > 0 else 0
            notify_signups = abs(quantity_raw) if quantity_raw is not None and quantity_raw < 0 else 0
            barcode = node.get("barcode") or ""
            image_url = node.get("image", {}) or {}
            image_url_str = image_url.get("url", "") if isinstance(image_url, dict) else ""
            sku_brand = node.get("sku", "") or ""
            sku_shopify = parse_shopify_id(variant_gid)
            product_type_out = product_type if product_type not in PRODUCT_TYPE_EXCLUDE and product_type else "Jeans"
            tags_out = ", ".join(tags)
            rows.append({
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": product_name,
                "Style Name": style_name,
                "Product Type": product_type_out,
                "Tags": tags_out,
                "Vendor": vendor,
                "Description": description,
                "Variant Title": variant_title_out,
                "Color": color,
                "Size": size,
                "Price": price_amount or "",
                "Compare at Price": compare_amount or "",
                "Available for Sale": str(available).upper(),
                "Quantity Available": quantity_available,
                "Notify Me Signups": notify_signups,
                "Quantity of style": style_total,
                "SKU - Shopify": sku_shopify,
                "SKU - Brand": sku_brand,
                "Barcode": barcode,
                "Image URL": image_url_str,
                "SKU URL": product.get("onlineStoreUrl", ""),
                "Jean Style": jean_style,
                "Rise Label": rise_label,
            })
    return rows


def main() -> None:
    products = fetch_collection_products()
    rows = build_rows(products)
    write_csv(rows)


if __name__ == "__main__":
    main()
