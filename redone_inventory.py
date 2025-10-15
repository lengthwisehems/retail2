import csv
import html
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "shopredone_inventory.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / LOG_PATH.name

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)

REQUEST_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/json,text/html,application/xhtml+xml",
}


COLLECTION_URLS = [
    "https://shopredone.com/collections/denim/products.json",
    "https://shopredone.com/collections/sale-denim-all/products.json",
]

SEARCHSPRING_URL = "https://w7x7sx.a.searchspring.io/api/search/search.json"

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


_RESOLVED_LOG_PATH: Optional[Path] = None


def ensure_directories() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)


def resolve_log_path() -> Path:
    global _RESOLVED_LOG_PATH
    if _RESOLVED_LOG_PATH is not None:
        return _RESOLVED_LOG_PATH

    ensure_directories()
    try:
        with LOG_PATH.open("a", encoding="utf-8"):
            pass
        _RESOLVED_LOG_PATH = LOG_PATH
    except PermissionError:
        FALLBACK_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with FALLBACK_LOG_PATH.open("a", encoding="utf-8"):
            pass
        print(
            f"WARNING: Unable to write to {LOG_PATH}. Using fallback log at {FALLBACK_LOG_PATH}.",
            flush=True,
        )
        _RESOLVED_LOG_PATH = FALLBACK_LOG_PATH

    return _RESOLVED_LOG_PATH


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_path = resolve_log_path()
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{timestamp}] {message}\n")


def clean_html(value: str) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    return " ".join(text.split())


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    iso_value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_value)
    except ValueError:
        try:
            dt = datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return value
    return dt.strftime("%m/%d/%Y")


def first_unique(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        candidates = value
    else:
        candidates = str(value).split(",")
    seen: List[str] = []
    for candidate in candidates:
        candidate = html.unescape(candidate).strip()
        if candidate and candidate not in seen:
            seen.append(candidate)
    return seen[0] if seen else ""


def join_unique(values: Iterable[str], separator: str = ", ") -> str:
    seen: List[str] = []
    for value in values:
        if not value:
            continue
        cleaned = html.unescape(str(value)).strip()
        if cleaned and cleaned not in seen:
            seen.append(cleaned)
    return separator.join(seen)


def determine_product_type(product_type_unigram: Optional[str]) -> str:
    if not product_type_unigram:
        return "Jeans"
    value = product_type_unigram.strip().lower()
    mapping = {
        "short": "Short",
        "shorts": "Short",
        "skirt": "Skirt",
        "jacket": "Jacket",
        "shirt": "Shirt",
        "dress": "Dress",
    }
    if value in mapping:
        return mapping[value]
    return "Jeans"


def normalize_list(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        items = value
    else:
        items = str(value).split(",")
    return [html.unescape(item).strip() for item in items if item and item.strip()]


def derive_jean_style(category_filter: Optional[str], fit_filter: Optional[List[str]]) -> str:
    categories = normalize_list(category_filter)
    fits = normalize_list(fit_filter or [])

    categories = list(dict.fromkeys(categories))
    fits = list(dict.fromkeys(fits))

    lower_categories = [c.lower() for c in categories]
    lower_fits = [f.lower() for f in fits]

    if "shorts & skirts" in lower_categories and "skirts" in lower_categories:
        return "Skirt"

    if categories:
        for cat in categories:
            cat_low = cat.lower()
            if "short" in cat_low and ("short" in lower_fits or "shorts" in lower_fits):
                return "Short"
            if "skirt" in cat_low and ("skirt" in lower_fits or "shorts & skirts" in lower_fits):
                return "Skirt"
            for fit in fits:
                fit_low = fit.lower()
                if fit_low and fit_low in cat_low:
                    if fit_low == "flare" and "wide" in cat_low:
                        return "Flare"
                    return cat

    if categories and fits:
        suffixes = {cat.split()[-1] for cat in categories if " " in cat}
        if len(categories) > 1 and len(fits) > 1 and len(suffixes) == 1:
            suffix = suffixes.pop()
            combined = " ".join(dict.fromkeys([fit.split()[-1] for fit in fits]))
            combined = combined.strip()
            if combined:
                return f"{combined} {suffix}".strip()

    if categories:
        first_category = categories[0]
        if "straight leg" in first_category.lower() and any("short" in f.lower() for f in fits):
            return "Capri"
        if "wide & flare leg" in first_category.lower() and any("flare" in f.lower() for f in fits):
            return "Flare"
        return first_category

    if fits:
        fit_choice = fits[0]
        if "short" in fit_choice.lower() and "skirt" in fit_choice.lower():
            return "Short/Skirt"
        return fit_choice

    return "Jeans"


def derive_inseam_label(
    tags_inseam: Optional[str],
    tags_length: Optional[str],
    ss_tags: Optional[Iterable[str] | str],
    jean_style: str,
) -> str:
    candidates = normalize_list(tags_inseam)
    if not candidates:
        candidates = normalize_list(tags_length)
    if not candidates and ss_tags:
        if isinstance(ss_tags, str):
            tag_source: Iterable[str] = ss_tags.split(",")
        else:
            tag_source = ss_tags
        extracted = []
        for part in tag_source:
            part = html.unescape(part).strip()
            if part.lower().startswith("inseam:"):
                extracted.append(part.split(":", 1)[-1].strip())
        candidates = [c for c in extracted if c]

    label = candidates[0] if candidates else ""
    jean_style_lower = jean_style.lower()
    if jean_style_lower in {"capri", "short", "skirt", "short/skirt"}:
        if label.lower() not in {"capri", "short", "skirt", "short/skirt"}:
            return jean_style
    return label


class ShopRedoneScraper:
    def __init__(self) -> None:
        ensure_directories()
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> dict:
        for attempt in range(5):
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    return response.json()
                log(f"HTTP {response.status_code} for {url} (attempt {attempt + 1})")
            except requests.RequestException as exc:
                log(f"Request error for {url}: {exc}")
            time.sleep(2 ** attempt * 0.5)
        raise RuntimeError(f"Failed to load JSON from {url}")

    def fetch_shopify_products(self) -> Dict[str, dict]:
        products: Dict[str, dict] = {}
        for base_url in COLLECTION_URLS:
            page = 1
            while True:
                params = {"limit": 250, "page": page}
                data = self.get_json(base_url, params=params)
                items = data.get("products", [])
                if not items:
                    break
                for product in items:
                    handle = product.get("handle")
                    if handle and handle not in products:
                        products[handle] = product
                page += 1
        log(f"Collected {len(products)} unique products from Shopify collections")
        return products

    def fetch_searchspring(self) -> Dict[str, dict]:
        handle_map: Dict[str, dict] = {}
        page = 1
        while True:
            params = {
                "siteId": "w7x7sx",
                "bgfilter.collection_handle": ["denim", "sale-denim-all"],
                "redirectResponse": "full",
                "noBeacon": "true",
                "ajaxCatalog": "Snap",
                "resultsFormat": "native",
                "resultsPerPage": "250",
                "page": str(page),
            }
            data = self.get_json(SEARCHSPRING_URL, params=params)
            results = data.get("results", [])
            for item in results:
                handle = item.get("handle")
                if not handle or handle in handle_map:
                    continue
                ss_tags = item.get("ss_tags", "")
                category_filter = item.get("tags_categoryfilter")
                fit_filter = item.get("tags_fitfilter") or []
                jean_style = derive_jean_style(category_filter, fit_filter)
                inseam_label = derive_inseam_label(
                    item.get("tags_inseam"), item.get("tags_length"), ss_tags, jean_style
                )
                rise_label = first_unique(item.get("tags_rise"))
                color_simplified = first_unique(item.get("tags_wash"))
                color_standardized = first_unique(item.get("tags_colorfilter"))
                stretch_value = first_unique(item.get("tags_stretch"))
                product_type = determine_product_type(item.get("product_type_unigram"))
                quantity_of_style = item.get("ss_inventory_count")
                try:
                    quantity_of_style = str(int(quantity_of_style))
                except (TypeError, ValueError):
                    quantity_of_style = ""

                variants_raw = item.get("variants") or "[]"
                try:
                    variants_list = json.loads(html.unescape(variants_raw))
                except json.JSONDecodeError:
                    variants_list = []
                variant_map = {
                    str(variant.get("id")): dict(variant)
                    for variant in variants_list
                    if variant.get("id")
                }

                size_json_raw = item.get("ss_size_json") or "[]"
                try:
                    size_entries = json.loads(html.unescape(size_json_raw))
                except json.JSONDecodeError:
                    size_entries = []

                for entry in size_entries:
                    variant_id = str(entry.get("id"))
                    if not variant_id:
                        continue
                    quantity_value = entry.get("available")
                    try:
                        quantity_value = int(quantity_value)
                    except (TypeError, ValueError):
                        quantity_value = None

                    if variant_id not in variant_map:
                        variant_map[variant_id] = {}
                    if quantity_value is not None:
                        variant_map[variant_id]["inventory_quantity"] = quantity_value

                handle_map[handle] = {
                    "product_type": product_type,
                    "quantity_of_style": quantity_of_style,
                    "jean_style": jean_style,
                    "inseam_label": inseam_label,
                    "rise_label": rise_label,
                    "color_simplified": color_simplified,
                    "color_standardized": color_standardized,
                    "stretch": stretch_value,
                    "variants": variant_map,
                    "ss_tags": ss_tags,
                }
            total_pages = int(data.get("pagination", {}).get("totalPages", 1))
            if page >= total_pages:
                break
            page += 1
        log(f"Collected Searchspring metadata for {len(handle_map)} products")
        return handle_map

    def fetch_product_detail(self, handle: str) -> Dict[str, dict]:
        url = f"https://shopredone.com/products/{handle}.json"
        try:
            data = self.get_json(url)
        except RuntimeError:
            log(f"Failed to load product detail for {handle}")
            return {}
        variant_map = {}
        for variant in data.get("product", {}).get("variants", []):
            variant_map[str(variant.get("id"))] = variant
        return variant_map

    def build_variant_image_map(self, product: dict) -> Dict[str, str]:
        image_map: Dict[str, str] = {}
        for image in product.get("images", []):
            src = image.get("src", "")
            for variant_id in image.get("variant_ids", []) or []:
                image_map[str(variant_id)] = src
        return image_map

    def assemble_rows(self) -> List[Dict[str, str]]:
        shopify_products = self.fetch_shopify_products()
        searchspring_data = self.fetch_searchspring()
        rows: List[Dict[str, str]] = []

        for handle, product in shopify_products.items():
            search_data = searchspring_data.get(handle, {})
            if not search_data:
                log(f"Missing Searchspring data for handle {handle}")

            detail_variants = self.fetch_product_detail(handle)
            image_map = self.build_variant_image_map(product)
            default_image = product.get("images", [{}])[0].get("src", "") if product.get("images") else ""

            style_id = str(product.get("id", ""))
            published_at = format_date(product.get("published_at"))
            product_title = product.get("title", "")
            style_name = product.get("product_type", "")
            tags_value = join_unique(product.get("tags", []))
            vendor = product.get("vendor", "")
            description = clean_html(product.get("body_html", ""))
            sku_url = f"https://shopredone.com/products/{handle}"
            product_type = search_data.get("product_type", "Jeans")
            quantity_of_style = search_data.get("quantity_of_style", "")
            jean_style = search_data.get("jean_style", "") or "Jeans"
            inseam_label = search_data.get("inseam_label", "")
            rise_label = search_data.get("rise_label", "")
            color_simplified = search_data.get("color_simplified", "")
            color_standardized = search_data.get("color_standardized", "")
            stretch_value = search_data.get("stretch", "")
            ss_tags = search_data.get("ss_tags", "")

            if not inseam_label:
                inseam_label = derive_inseam_label("", "", ss_tags, jean_style)

            for variant in product.get("variants", []):
                variant_id = str(variant.get("id"))
                variant_detail = detail_variants.get(variant_id, {})
                variant_search = search_data.get("variants", {}).get(variant_id, {})

                color = variant.get("option1", "")
                size = variant.get("option2", "")
                price = variant.get("price", "")
                compare_at_price = variant.get("compare_at_price", "")
                available = "TRUE" if variant.get("available") else "FALSE"
                sku_shopify = variant_id
                sku_brand = variant.get("sku", "")
                image_url = image_map.get(variant_id, default_image)

                quantity_available = None
                if variant_search:
                    quantity_available = variant_search.get("inventory_quantity")
                if quantity_available is None:
                    quantity_available = variant_detail.get("inventory_quantity")
                if quantity_available is None:
                    quantity_available = ""
                
                barcode = variant_detail.get("barcode")
                if not barcode and variant_search:
                    barcode = variant_search.get("barcode")
                barcode = barcode or ""

                variant_title = product_title if not size else f"{product_title} - {size}"

                label = inseam_label
                if jean_style.lower() in {"capri", "short", "skirt", "short/skirt"} and label.lower() not in {
                    "capri",
                    "short",
                    "skirt",
                    "short/skirt",
                }:
                    label = jean_style

                row = {
                    "Style Id": style_id,
                    "Handle": handle,
                    "Published At": published_at,
                    "Product": product_title,
                    "Style Name": style_name,
                    "Product Type": product_type,
                    "Tags": tags_value,
                    "Vendor": vendor,
                    "Description": description,
                    "Variant Title": variant_title,
                    "Color": color,
                    "Size": size,
                    "Price": str(price),
                    "Compare at Price": str(compare_at_price),
                    "Available for Sale": available,
                    "Quantity Available": str(quantity_available),
                    "Quantity of style": quantity_of_style,
                    "SKU - Shopify": sku_shopify,
                    "SKU - Brand": sku_brand,
                    "Barcode": str(barcode),
                    "Image URL": image_url,
                    "SKU URL": sku_url,
                    "Jean Style": jean_style,
                    "Inseam Label": label,
                    "Rise Label": rise_label,
                    "Color - Simplified": color_simplified,
                    "Color - Standardized": color_standardized,
                    "Stretch": stretch_value,
                }

                rows.append(row)

        return rows

    def write_csv(self, rows: List[Dict[str, str]]) -> Path:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = OUTPUT_DIR / f"REDONE_{timestamp}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        return output_path


def main() -> None:
    log("Starting ShopRedone scrape")
    scraper = ShopRedoneScraper()
    rows = scraper.assemble_rows()
    csv_path = scraper.write_csv(rows)
    log(f"Wrote {len(rows)} rows to {csv_path}")
    print("Done.")


if __name__ == "__main__":
    main()