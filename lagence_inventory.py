"""L'Agence denim inventory scraper."""
from __future__ import annotations

import csv
import json
import logging
import math
import re
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import urllib3
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "lagence_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "lagence_run.log"

OUTPUT_DIR.mkdir(exist_ok=True)

HOSTS = [
    "https://lagence-fashion.myshopify.com",
    "https://www.lagence.com",
    "https://lagence.com",
]
SHOPIFY_COLLECTIONS: Tuple[Tuple[str, str], ...] = (
    ("/collections/jeans/products.json", "Jeans"),
    ("/collections/sale/products.json", "Sale"),
)
NOSTO_ENDPOINT = "https://search.nosto.com/v1/graphql"
NOSTO_ACCOUNT_ID = "shopify-5439520871"
NOSTO_CATEGORY_IDS = ("626045911412", "160218808423")
NOSTO_PAGE_SIZE = 100
SKU_URL_TEMPLATE = "https://lagence.com/products/{handle}"

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
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
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Label",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
    "Site Exclusive",
]


def configure_logging() -> logging.Logger:
    handlers: List[logging.Handler] = []
    selected_path: Optional[Path] = None
    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path)
            handlers.append(handler)
            selected_path = path
            if path != LOG_PATH:
                print(
                    f"WARNING: Primary log path {LOG_PATH} unavailable. Using fallback log at {path}.",
                    flush=True,
                )
            break
        except (OSError, PermissionError) as exc:
            print(
                f"WARNING: Unable to open log file {path}: {exc}. Continuing without this destination.",
                flush=True,
            )
    handlers.append(logging.StreamHandler())
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )
    logger = logging.getLogger(__name__)
    if selected_path is None:
        logger.warning("File logging disabled; continuing with console logging only.")
    return logger


LOGGER = configure_logging()

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
    }
)
retry_strategy = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)
SESSION.verify = False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None


def format_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    text = str(value).strip().replace("$", "")
    if not text:
        return ""
    try:
        return f"{float(text):.2f}"
    except ValueError:
        return text


def format_published_at(date_str: Optional[str]) -> str:
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return date_str


def clean_html(html_text: str) -> str:
    if not html_text:
        return ""
    soup = BeautifulSoup(unescape(html_text), "html.parser")
    text = soup.get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_custom_value(value: Any) -> Any:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                return parsed
            except json.JSONDecodeError:
                return text
        return text
    return value


def to_key_value_map(items: Optional[Iterable[Dict[str, Any]]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not items:
        return result
    for item in items:
        key = item.get("key")
        if not key:
            continue
        result[key] = normalize_custom_value(item.get("value"))
    return result


def first_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        for entry in value:
            if entry not in (None, ""):
                return str(entry).strip()
        return ""
    return str(value).strip()


def measurement_to_decimal(raw_value: Optional[str]) -> str:
    if not raw_value:
        return ""
    value = raw_value
    if ":" in value:
        value = value.split(":", 1)[1]
    value = value.replace("\u00bd", " 1/2")
    value = re.sub(r"[^0-9./\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    if not value:
        return ""
    match = re.search(r"(\d+\s+\d/\d|\d+/\d|\d+\.\d+|\d+)", value)
    if not match:
        return ""
    token = match.group(1)
    total = 0.0
    if " " in token:
        whole, frac = token.split(" ", 1)
        total += float(whole)
        token = frac
    if "/" in token:
        num, denom = token.split("/", 1)
        try:
            total += float(num) / float(denom)
        except (TypeError, ValueError, ZeroDivisionError):
            return ""
    else:
        try:
            total += float(token)
        except ValueError:
            return ""
    return (
        f"{total:.2f}".rstrip("0").rstrip(".")
        if not math.isclose(total, 0.0)
        else ""
    )


def iter_shopify_hosts(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    last_error: Optional[Exception] = None
    for host in HOSTS:
        url = f"{host}{path}"
        try:
            response = SESSION.get(url, params=params, timeout=30, verify=False)
            response.raise_for_status()
            LOGGER.debug("Fetched %s with params %s via %s", path, params, host)
            return response.json()
        except requests.RequestException as exc:
            LOGGER.warning("Request to %s via %s failed: %s", path, host, exc)
            last_error = exc
            time.sleep(1)
            continue
    raise RuntimeError(f"Unable to fetch {path} after host rotation: {last_error}")


def fetch_shopify_catalog() -> Dict[str, Dict[str, Any]]:
    products: Dict[str, Dict[str, Any]] = {}
    for path, label in SHOPIFY_COLLECTIONS:
        LOGGER.info("Fetching Shopify collection: %s", label)
        page = 1
        while True:
            params = {"limit": 250, "page": page}
            data = iter_shopify_hosts(path, params)
            batch = data.get("products", [])
            if not batch:
                LOGGER.info("%s page %s returned no products", label, page)
                break
            LOGGER.info("%s page %s -> %s products", label, page, len(batch))
            for product in batch:
                product_type = (product.get("product_type") or "").strip().lower()
                if "jean" not in product_type:
                    continue
                handle = product.get("handle")
                if not handle:
                    continue
                existing = products.get(handle)
                if existing:
                    merge_shopify_product(existing, product)
                else:
                    products[handle] = product
            page += 1
    LOGGER.info("Collected %s unique Shopify products", len(products))
    return products


def merge_shopify_product(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    target_variants = {str(v.get("id")): v for v in target.get("variants", [])}
    for variant in source.get("variants", []):
        variant_id = str(variant.get("id"))
        if variant_id not in target_variants:
            target.setdefault("variants", []).append(variant)
    target_tags = set(filter(None, (target.get("tags") or "").split(",")))
    source_tags = set(filter(None, (source.get("tags") or "").split(",")))
    merged_tags = ", ".join(sorted({tag.strip() for tag in target_tags | source_tags if tag.strip()}))
    target["tags"] = merged_tags
    published_at = source.get("published_at")
    target_published_at = target.get("published_at")
    if published_at and (
        not target_published_at or published_at < target_published_at
    ):
        target["published_at"] = published_at


NOSTO_QUERY = """
query FetchProducts($account: String!, $category: String!, $size: Int!, $from: Int!) {
  search(
    accountId: $account
    products: { categoryId: $category, size: $size, from: $from }
  ) {
    products {
      total
      from
      size
      hits {
        productId
        pid
        name
        brand
        description
        url
        imageUrl
        price
        listPrice
        priceCurrencyCode
        availability
        available
        onDiscount
        inventoryLevel
        categories
        customFields { key value }
        extra { key value }
        skus {
          id
          name
          url
          imageUrl
          price
          listPrice
          inventoryLevel
          availability
          customFields { key value }
        }
      }
    }
  }
}
"""


def fetch_nosto_data() -> Dict[str, Dict[str, Any]]:
    handle_map: Dict[str, Dict[str, Any]] = {}
    for category_id in NOSTO_CATEGORY_IDS:
        LOGGER.info("Fetching Nosto category %s", category_id)
        fetched = 0
        total = math.inf
        offset = 0
        while fetched < total:
            payload = {
                "query": NOSTO_QUERY,
                "variables": {
                    "account": NOSTO_ACCOUNT_ID,
                    "category": category_id,
                    "size": NOSTO_PAGE_SIZE,
                    "from": offset,
                },
            }
            response = SESSION.post(NOSTO_ENDPOINT, json=payload, timeout=30, verify=False)
            response.raise_for_status()
            data = response.json()
            if data.get("errors"):
                raise RuntimeError(f"Nosto error: {data['errors']}")
            products_node = data.get("data", {}).get("search", {}).get("products", {})
            hits = products_node.get("hits", [])
            total = products_node.get("total", 0) or 0
            LOGGER.info(
                "Nosto category %s from %s -> %s hits (total %s)",
                category_id,
                offset,
                len(hits),
                total,
            )
            if not hits:
                break
            for hit in hits:
                handle = extract_handle(hit.get("url"))
                if not handle:
                    continue
                entry = handle_map.setdefault(handle, {
                    "product": hit,
                    "skus": {},
                })
                if entry["product"].get("inventoryLevel") is None and hit.get("inventoryLevel") is not None:
                    entry["product"] = hit
                entry_skus = entry.setdefault("skus", {})
                for sku in hit.get("skus", []) or []:
                    sku_id = str(sku.get("id"))
                    if not sku_id:
                        continue
                    entry_skus[sku_id] = sku
            fetched += len(hits)
            offset += NOSTO_PAGE_SIZE
            if len(hits) < NOSTO_PAGE_SIZE:
                break
            time.sleep(0.5)
    LOGGER.info("Collected Nosto data for %s handles", len(handle_map))
    return handle_map


def extract_handle(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    match = re.search(r"/products/([^/?#]+)", url)
    if not match:
        return None
    return match.group(1)


def determine_inseam_label(length: str, size_type: str) -> str:
    length_clean = (length or "").strip()
    size_type_clean = (size_type or "").strip().title()
    if size_type_clean == "Petite":
        if not length_clean:
            return "Petite"
        if length_clean.lower() == "full length":
            return "Petite"
        return f"Petite - {length_clean}"
    if size_type_clean == "Extra Long" and length_clean.lower() == "full length":
        return "Extra Long"
    return length_clean


def determine_site_exclusive(categories: Iterable[str]) -> str:
    for category in categories or []:
        if not isinstance(category, str):
            continue
        normalized = category.strip().lower()
        if normalized in {"online exclusive", "online exclusives"}:
            return "Online Exclusives"
    return ""


def choose_image(product: Dict[str, Any], variant: Dict[str, Any]) -> str:
    variant_id = variant.get("id")
    for image in product.get("images", []) or []:
        variant_ids = image.get("variant_ids") or []
        if variant_id and variant_id in variant_ids:
            return image.get("src", "")
    featured = variant.get("featured_image") or {}
    if isinstance(featured, dict):
        url = featured.get("src")
        if url:
            return url
    primary_image = product.get("image")
    if isinstance(primary_image, dict):
        url = primary_image.get("src")
        if url:
            return url
    images = product.get("images") or []
    if images:
        first_url = images[0].get("src")
        if first_url:
            return first_url
    return ""


def assemble_rows(
    shopify_products: Dict[str, Dict[str, Any]],
    nosto_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    total = len(shopify_products)
    for index, (handle, product) in enumerate(shopify_products.items(), start=1):
        variants = product.get("variants", []) or []
        if not variants:
            LOGGER.warning("Product %s has no variants", handle)
            continue
        nosto_entry = nosto_map.get(handle, {})
        nosto_product = nosto_entry.get("product", {}) if nosto_entry else {}
        nosto_skus = nosto_entry.get("skus", {}) if nosto_entry else {}
        product_custom_fields = to_key_value_map(nosto_product.get("customFields"))
        product_extra = to_key_value_map(nosto_product.get("extra"))
        rise = measurement_to_decimal(product_custom_fields.get("custom-detail_spec_2"))
        inseam = measurement_to_decimal(product_custom_fields.get("custom-detail_spec_3"))
        leg_opening = measurement_to_decimal(product_custom_fields.get("custom-detail_spec_4"))
        style_quantity = safe_int(nosto_product.get("inventoryLevel"))
        tags_value = product.get("tags") or ""
        if isinstance(tags_value, list):
            tags = [str(tag).strip() for tag in tags_value if str(tag).strip()]
        else:
            tags = [tag.strip() for tag in str(tags_value).split(",") if tag.strip()]
        description = clean_html(product.get("body_html", ""))
        product_title = product.get("title", "")
        published_at = format_published_at(product.get("published_at"))
        vendor = product.get("vendor", "")
        product_type = product.get("product_type", "")
        jean_style = first_value(
            product_custom_fields.get("custom-fit_pants_denim")
            or product_extra.get("custom-fit_pants_denim")
        )
        length_value = first_value(
            product_custom_fields.get("custom-standard_product_length")
            or product_extra.get("custom-standard_product_length")
        )
        size_type_value = first_value(
            product_custom_fields.get("mm-google-shopping-size_type")
            or product_custom_fields.get("custom-size_type")
            or product_extra.get("sizeType")
        )
        inseam_label = determine_inseam_label(length_value, size_type_value)
        rise_label = first_value(
            product_custom_fields.get("custom-rise_pants_denim")
            or product_extra.get("custom-rise_pants_denim")
        )
        color_simplified = first_value(
            product_extra.get("wash") or product_custom_fields.get("wash")
        )
        color_standardized = first_value(
            product_custom_fields.get("custom-main_color_family")
            or product_extra.get("custom-main_color_family")
        )
        stretch = first_value(product_extra.get("group") or product_custom_fields.get("group"))
        site_exclusive = determine_site_exclusive(nosto_product.get("categories") or [])
        jean_style = jean_style.strip()
        inseam_label = inseam_label.strip()
        rise_label = rise_label.strip()
        color_simplified = color_simplified.strip().title() if color_simplified else ""
        color_standardized = color_standardized.strip().title() if color_standardized else ""
        stretch = stretch.replace("_", " ").strip().title() if stretch else ""
        if style_quantity is None:
            style_quantity = sum(
                safe_int(v.get("inventory_quantity")) or 0 for v in variants
            )
        for variant in variants:
            variant_id = str(variant.get("id"))
            sku_data = nosto_skus.get(variant_id, {})
            sku_custom_fields = to_key_value_map(sku_data.get("customFields"))
            variant_title_part = variant.get("title", "")
            color = variant.get("option1", "")
            size = variant.get("option2", "")
            product_display = (
                f"{product_title} - {color}".strip(" -") if color else product_title
            )
            variant_title = (
                f"{product_title} - {variant_title_part}".strip(" -")
                if variant_title_part
                else product_title
            )
            quantity_available = safe_int(sku_data.get("inventoryLevel"))
            if quantity_available is None:
                quantity_available = safe_int(variant.get("inventory_quantity")) or 0
            barcode = first_value(
                sku_custom_fields.get("gtin")
                or sku_custom_fields.get("barcode")
                or variant.get("barcode")
            )
            row = {
                "Style Id": product.get("id", ""),
                "Handle": handle,
                "Published At": published_at,
                "Product": product_display,
                "Style Name": product_title,
                "Product Type": product_type,
                "Tags": ", ".join(tags),
                "Vendor": vendor,
                "Description": description,
                "Variant Title": variant_title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": format_price(variant.get("price")),
                "Compare at Price": format_price(variant.get("compare_at_price")),
                "Available for Sale": "TRUE" if variant.get("available") else "FALSE",
                "Quantity Available": quantity_available,
                "Quantity of style": style_quantity if style_quantity is not None else "",
                "SKU - Shopify": variant.get("id", ""),
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": barcode,
                "Image URL": choose_image(product, variant),
                "SKU URL": SKU_URL_TEMPLATE.format(handle=handle),
                "Jean Style": jean_style,
                "Inseam Label": inseam_label,
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
                "Site Exclusive": site_exclusive,
            }
            rows.append(row)
        if index % 25 == 0 or index == total:
            LOGGER.info("Processed %s/%s products", index, total)
    LOGGER.info("Prepared %s variant rows", len(rows))
    return rows


def write_csv(rows: List[Dict[str, Any]]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"LAGENCE_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    LOGGER.info("Wrote CSV to %s", output_path)
    return output_path


def main() -> None:
    LOGGER.info("Starting L'Agence inventory export")
    shopify_products = fetch_shopify_catalog()
    nosto_data = fetch_nosto_data()
    rows = assemble_rows(shopify_products, nosto_data)
    if not rows:
        raise SystemExit("No rows generated; aborting")
    write_csv(rows)
    LOGGER.info("Completed export")


if __name__ == "__main__":
    main()
