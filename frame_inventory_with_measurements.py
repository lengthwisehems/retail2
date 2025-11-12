"""Frame denim inventory scraper with monthly measurement export."""
from __future__ import annotations

import csv
import json
import logging
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import urllib3
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "frame_measurements_run.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler(),
    ],
)
LOGGER = logging.getLogger(__name__)

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
    allowed_methods=["GET"],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)
SESSION.verify = False  # frame-store.com presents an incomplete certificate chain
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHOPIFY_COLLECTIONS = [
    "https://frame-store.com/collections/sale-denim/products.json",
    "https://frame-store.com/collections/denim-women/products.json",
]
SEARCHSPRING_URL = "https://v1j77y.a.searchspring.io/api/search/search.json"
DETAILS_TEMPLATE = "https://frame-store.com/products/{handle}?modals=details_modal"
SKU_URL_TEMPLATE = "https://frame-store.com/products/{handle}"

ALLOWED_SIZES = [
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "XXS",
    "XXS/XS",
    "XS",
    "XS/S",
    "S",
    "M",
    "M/L",
    "L",
    "L/XL",
    "XL",
    "XXL",
    "00",
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "ONSZ",
    "0-REGULAR",
    "2-REGULAR",
    "4-REGULAR",
    "6-REGULAR",
    "8-REGULAR",
    "10-REGULAR",
]
ALLOWED_SIZE_LOOKUP = {value.upper(): value for value in ALLOWED_SIZES}

SIMPLIFIED_COLOR_MAP = {
    "darkwash": "Dark wash",
    "lightwash": "Light wash",
    "mediumwash": "Medium wash",
}
STANDARD_COLOR_OPTIONS = {
    "BLUE",
    "BLACK",
    "WHITE",
    "BEIGE",
    "BROWN",
    "RED",
    "GREEN",
    "GREY",
    "PINK",
    "PURPLE",
    "MULTI",
    "YELLOW",
    "ORANGE",
    "PRINT",
}

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
    "Back Rise",
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
]


def fetch_json(url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    for attempt in range(5):
        try:
            response = SESSION.get(url, params=params, timeout=30, verify=False)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:  # pragma: no cover - network retry
            wait = 2 ** attempt
            LOGGER.warning("JSON request failed (%s): %s", url, exc)
            time.sleep(wait)
    raise RuntimeError(f"Failed to fetch JSON after retries: {url}")


def fetch_html(url: str) -> str:
    for attempt in range(5):
        try:
            response = SESSION.get(url, timeout=30, verify=False)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:  # pragma: no cover - network retry
            wait = 2 ** attempt
            LOGGER.warning("HTML request failed (%s): %s", url, exc)
            time.sleep(wait)
    LOGGER.error("Giving up on HTML request for %s", url)
    return ""


def paginate_shopify_collections() -> Dict[str, Dict[str, Any]]:
    products: Dict[str, Dict[str, Any]] = {}
    for base_url in SHOPIFY_COLLECTIONS:
        page = 1
        while True:
            url = f"{base_url}?limit=250&page={page}"
            data = fetch_json(url)
            page_products = data.get("products", [])
            if not page_products:
                LOGGER.info("%s page %s returned 0 products", base_url, page)
                break
            LOGGER.info("Fetched %s products from %s page %s", len(page_products), base_url, page)
            for product in page_products:
                handle = product.get("handle")
                if not handle:
                    continue
                existing = products.get(handle)
                if existing:
                    existing_date = existing.get("published_at")
                    new_date = product.get("published_at")
                    if new_date and (not existing_date or new_date < existing_date):
                        products[handle] = product
                else:
                    products[handle] = product
            page += 1
    LOGGER.info("Collected %s unique products from Shopify", len(products))
    return products


def parse_measurements(handle: str) -> Tuple[str, str, str, str]:
    url = DETAILS_TEMPLATE.format(handle=handle)
    html = fetch_html(url)
    if not html:
        return "", "", "", ""
    soup = BeautifulSoup(html, "html.parser")
    list_node = soup.select_one("div.measurement-image__text-wrapper ul.measurement-image__list")
    rise = back_rise = inseam = leg_opening = ""
    if not list_node:
        return rise, back_rise, inseam, leg_opening
    for item in list_node.select("li"):
        label = item.get_text(" ", strip=True).lower()
        span = item.find("span", attrs={"data-size-in-inches": True})
        value = span["data-size-in-inches"].strip() if span else ""
        if not value:
            continue
        if "back rise" in label and not back_rise:
            back_rise = value
        elif "leg opening" in label and not leg_opening:
            leg_opening = value
        elif "inseam" in label and not inseam:
            inseam = value
        elif "rise" in label and not rise:
            rise = value
    return rise, back_rise, inseam, leg_opening


def clean_description(body_html: str) -> str:
    if not body_html:
        return ""
    soup = BeautifulSoup(body_html, "html.parser")
    return soup.get_text(" ", strip=True)


def format_published_at(date_str: Optional[str]) -> str:
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return date_str


def derive_style_name(title: str) -> str:
    if not title:
        return ""
    if "--" in title:
        return title.split("--", 1)[0].strip(" -")
    if "-" in title:
        return title.split("-", 1)[0].strip()
    return title


def derive_color(title: str) -> str:
    if not title:
        return ""
    if "--" in title:
        return title.split("--", 1)[1].strip()
    return ""


def normalize_product_type(product_type: str, tags: Iterable[str]) -> str:
    if product_type == "Jeans" and any(tag.lower() == "collection::skirts & shorts" for tag in tags):
        return "Skirt/Short"
    return product_type


def determine_size(variant: Dict[str, Any]) -> str:
    options = [variant.get("option1"), variant.get("option2"), variant.get("option3"), variant.get("title")]
    for option in options:
        if not option:
            continue
        normalized = option.strip().upper()
        if normalized in ALLOWED_SIZE_LOOKUP:
            return ALLOWED_SIZE_LOOKUP[normalized]
        if " / " in option:
            first_part = option.split(" / ", 1)[0].strip().upper()
            if first_part in ALLOWED_SIZE_LOOKUP:
                return ALLOWED_SIZE_LOOKUP[first_part]
        if " - " in option:
            first_part = option.split(" - ", 1)[0].strip().upper()
            if first_part in ALLOWED_SIZE_LOOKUP:
                return ALLOWED_SIZE_LOOKUP[first_part]
    return variant.get("option1") or variant.get("title", "")


def format_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = text.replace("$", "")
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return text
    return f"{amount:.2f}"


def resolve_price(*candidates: Any) -> str:
    for candidate in candidates:
        formatted = format_price(candidate)
        if formatted:
            return formatted
    return ""


def parse_searchspring() -> Dict[str, Dict[str, Any]]:
    handle_map: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        params = {
            "siteId": "v1j77y",
            "resultsFormat": "json",
            "q": "women",
            "ss_category": "Jeans",
            "resultsPerPage": 100,
            "page": page,
            "redirectResponse": "minimal",
        }
        data = fetch_json(SEARCHSPRING_URL, params=params)
        results = data.get("results", [])
        LOGGER.info("Fetched %s Searchspring hits on page %s", len(results), page)
        for result in results:
            handle = result.get("handle")
            if not handle:
                continue
            raw_variants = result.get("variants") or []
            if isinstance(raw_variants, str):
                try:
                    variants = json.loads(unescape(raw_variants))
                except json.JSONDecodeError:
                    LOGGER.warning("Failed to parse variants for handle %s", handle)
                    variants = []
            else:
                variants = raw_variants
            variant_map = {str(variant.get("id")): variant for variant in variants if variant.get("id")}
            handle_map[handle] = {
                "quantity_of_style": safe_int(result.get("ss_inventory_count")),
                "tags": result.get("ss_tags", []),
                "variants": variant_map,
            }
        pagination = data.get("pagination", {})
        if page >= pagination.get("totalPages", page):
            break
        page += 1
    LOGGER.info("Collected Searchspring metadata for %s handles", len(handle_map))
    return handle_map


def safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def determine_jean_style(tags: Iterable[str]) -> str:
    values: List[str] = []
    seen: set[str] = set()
    pixie_present = False
    for tag in tags:
        normalized = tag.lower().replace("filter-", "filter")
        if normalized.startswith("filterleg::"):
            raw_value = tag.split("::", 1)[1].strip()
            cleaned_value = raw_value.replace("_", " ")
            key = cleaned_value.lower()
            if key == "pixie":
                pixie_present = True
                continue
            if key and key not in seen:
                seen.add(key)
                values.append(cleaned_value)
    if not values and pixie_present:
        values.append("pixie")
    return "/".join(values)


def determine_inseam_label(tags: Iterable[str], title: str) -> str:
    length_values = [
        tag.split("::", 1)[1].strip()
        for tag in tags
        if tag.lower().replace("filter-", "filter").startswith("filterlength::")
    ]
    if not length_values:
        return ""
    value = length_values[0].replace("_", " ").title()
    pixie = "pixie" in title.lower() or any(
        tag.lower().replace("filter-", "filter") == "filterleg::pixie" for tag in tags
    )
    if pixie:
        return f"Petite {value}"
    return value


def determine_rise_label(tags: Iterable[str]) -> str:
    for tag in tags:
        if tag.lower().replace("filter-", "filter").startswith("filterrise::"):
            return tag.split("::", 1)[1].replace("_", " ").title()
    return ""


def determine_stretch(tags: Iterable[str]) -> str:
    for tag in tags:
        if tag.lower().replace("filter-", "filter").startswith("filterstretch::"):
            return tag.split("::", 1)[1].replace("_", " ").title()
    return ""


def determine_color_fields(tags: Iterable[str]) -> Tuple[str, str]:
    simplified = ""
    standardized = ""
    for tag in tags:
        if not tag.lower().replace("filter-", "filter").startswith("filtercolor::"):
            continue
        value = tag.split("::", 1)[1].strip()
        normalized = value.replace(" ", "").replace("-", "").lower()
        if not simplified and normalized in SIMPLIFIED_COLOR_MAP:
            simplified = SIMPLIFIED_COLOR_MAP[normalized]
        upper_value = value.replace("-", " ").strip().upper()
        if not standardized and upper_value in STANDARD_COLOR_OPTIONS:
            standardized = upper_value.title()
    return simplified, standardized


def choose_image(product: Dict[str, Any], variant: Dict[str, Any]) -> str:
    featured = variant.get("featured_image") or {}
    src = featured.get("src")
    if src:
        return src
    images = product.get("images") or []
    if images:
        return images[0].get("src", "")
    return ""


def build_rows() -> List[Dict[str, Any]]:
    products = paginate_shopify_collections()
    searchspring_map = parse_searchspring()
    rows: List[Dict[str, Any]] = []
    measurement_cache: Dict[str, Tuple[str, str, str, str]] = {}

    total_handles = len(products)
    for index, (handle, product) in enumerate(products.items(), start=1):
        variants = product.get("variants", [])
        if not variants:
            continue
        searchspring_entry = searchspring_map.get(handle, {})
        ss_variants: Dict[str, Any] = searchspring_entry.get("variants", {})
        tags = product.get("tags", [])
        ss_tags = searchspring_entry.get("tags", [])
        combined_tags = list(tags)
        if ss_tags:
            combined_tags = list(dict.fromkeys(list(tags) + ss_tags))
        rise, back_rise, inseam, leg_opening = measurement_cache.get(handle, ("", "", "", ""))
        if not rise and not back_rise and not inseam and not leg_opening:
            rise, back_rise, inseam, leg_opening = parse_measurements(handle)
            measurement_cache[handle] = (rise, back_rise, inseam, leg_opening)
        style_quantity = searchspring_entry.get("quantity_of_style")
        if style_quantity is None and ss_variants:
            ss_total = sum(
                safe_int(str(variant.get("inventory_quantity", 0))) or 0
                for variant in ss_variants.values()
            )
            style_quantity = ss_total
        if style_quantity is None:
            shopify_total = sum(
                safe_int(variant.get("inventory_quantity")) or 0 for variant in variants
            )
            style_quantity = shopify_total
        product_title = product.get("title", "")
        color = derive_color(product_title)
        style_name = derive_style_name(product_title)
        product_type = normalize_product_type(product.get("product_type", ""), tags)
        description = clean_description(product.get("body_html", ""))
        published_at = format_published_at(product.get("published_at"))
        jean_style = determine_jean_style(combined_tags)
        inseam_label = determine_inseam_label(combined_tags, product_title)
        rise_label = determine_rise_label(combined_tags)
        stretch = determine_stretch(combined_tags)
        color_simplified, color_standardized = determine_color_fields(combined_tags)

        for variant in variants:
            variant_id = str(variant.get("id"))
            ss_variant = ss_variants.get(variant_id, {})
            size = determine_size(variant)
            variant_title = f"{product_title} - {size}" if size else product_title
            quantity_available = safe_int(ss_variant.get("inventory_quantity"))
            if quantity_available is None:
                quantity_available = safe_int(variant.get("inventory_quantity")) or 0
            barcode = ss_variant.get("barcode") or variant.get("barcode", "")
            sku_shopify = variant.get("id", "")
            sku_brand = variant.get("sku", "") or ""
            if sku_brand and size:
                sku_brand = f"{sku_brand}-{size}"
            price = resolve_price(ss_variant.get("price"), variant.get("price"))
            compare_at_price = resolve_price(
                ss_variant.get("compare_at_price"), variant.get("compare_at_price")
            )
            row = {
                "Style Id": product.get("id", ""),
                "Handle": handle,
                "Published At": published_at,
                "Product": product_title,
                "Style Name": style_name,
                "Product Type": product_type,
                "Tags": ", ".join(tags),
                "Vendor": product.get("vendor", ""),
                "Description": description,
                "Variant Title": variant_title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Back Rise": back_rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": price,
                "Compare at Price": compare_at_price,
                "Available for Sale": "TRUE" if variant.get("available") else "FALSE",
                "Quantity Available": quantity_available,
                "Quantity of style": style_quantity if style_quantity is not None else 0,
                "SKU - Shopify": sku_shopify,
                "SKU - Brand": sku_brand,
                "Barcode": barcode,
                "Image URL": choose_image(product, variant),
                "SKU URL": SKU_URL_TEMPLATE.format(handle=handle),
                "Jean Style": jean_style,
                "Inseam Label": inseam_label,
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
            }
            rows.append(row)
        if index % 25 == 0 or index == total_handles:
            LOGGER.info("Processed %s/%s product handles", index, total_handles)
    LOGGER.info("Prepared %s variant rows", len(rows))
    return rows


def write_csv(rows: List[Dict[str, Any]]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"FRAME_Measurements_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    LOGGER.info("Wrote CSV to %s", output_path)
    return output_path


def main() -> None:
    rows = build_rows()
    if not rows:
        LOGGER.warning("No rows were generated.")
        return
    write_csv(rows)


if __name__ == "__main__":
    main()
