import csv
import html
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE = BASE_DIR / "paige_inventory.log"

ALGOLIA_APP_ID = "DK4YY42827"
ALGOLIA_API_KEY = "333da36aea28227274c0ad598d0fbdb0"
ALGOLIA_INDEX = "production_products"
ALGOLIA_SEARCH_URL = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

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
    "Price Range",
    "Available for Sale",
    "Quantity Available",
    "Google Analytics Purchases",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Product Line",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Label",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Country Produced",
    "Stretch",
    "Production Cost",
    "Site Exclusive",
]


def ensure_directories() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        LOG_FILE.touch(exist_ok=True)
    except PermissionError:
        pass


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(f"[{timestamp}] {message}\n")
    except PermissionError:
        print(f"[{timestamp}] {message}")


def format_price(value: Optional[float]) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"${numeric:.2f}"


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


def join_tags(tags: Iterable[str]) -> str:
    cleaned: List[str] = []
    for tag in tags:
        if not tag:
            continue
        text = html.unescape(str(tag)).strip()
        if text:
            cleaned.append(text)
    return ", ".join(cleaned)


def safe_string(value: Optional[object]) -> str:
    if value is None:
        return ""
    return str(value)


def extract_tag_value(tags: Iterable[str], prefix: str) -> str:
    prefix_lower = prefix.lower()
    for tag in tags:
        if not tag:
            continue
        tag_text = str(tag)
        if tag_text.lower().startswith(prefix_lower):
            return html.unescape(tag_text[len(prefix):]).strip()
    return ""


def derive_inseam_label(length: str, size_type: str) -> str:
    length_clean = (length or "").strip()
    size_type_clean = (size_type or "").strip()
    if not size_type_clean:
        return length_clean

    size_type_upper = size_type_clean.lower()
    length_upper = length_clean.lower()

    if size_type_upper == "petite":
        if length_upper in {"full length", "full"}:
            return "Petite"
        if length_clean:
            return f"Petite - {length_clean}"
        return "Petite"

    if size_type_upper == "extra long":
        if length_upper in {"full length", "full"}:
            return "Extra Long"
        if length_clean:
            return f"Extra Long - {length_clean}"
        return "Extra Long"

    return length_clean


class PaigeScraper:
    def __init__(self) -> None:
        ensure_directories()
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/123.0.0.0 Safari/537.36",
            }
        )

    def algolia_request(self, params: Dict[str, str]) -> Dict:
        query_string = "&".join(
            f"{key}={requests.utils.quote(str(value))}" for key, value in params.items()
        )
        payload = {"params": query_string}
        response = self.session.post(
            ALGOLIA_SEARCH_URL,
            headers={
                "X-Algolia-Application-Id": ALGOLIA_APP_ID,
                "X-Algolia-API-Key": ALGOLIA_API_KEY,
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def fetch_styles(self) -> List[Dict]:
        styles: List[Dict] = []
        page = 0
        while True:
            response = self.algolia_request(
                {
                    "filters": "collections:women-denim",
                    "distinct": "true",
                    "hitsPerPage": 1000,
                    "page": page,
                }
            )
            hits = response.get("hits", [])
            styles.extend(hits)
            nb_pages = response.get("nbPages", 0)
            page += 1
            if page >= nb_pages:
                break
        return styles

    def fetch_variants(self, style_id: str) -> List[Dict]:
        variants: List[Dict] = []
        page = 0
        while True:
            response = self.algolia_request(
                {
                    "filters": f"id={style_id} AND collections:women-denim",
                    "distinct": "false",
                    "hitsPerPage": 1000,
                    "page": page,
                }
            )
            hits = response.get("hits", [])
            if not hits:
                break
            variants.extend(hits)
            nb_pages = response.get("nbPages", 0)
            page += 1
            if page >= nb_pages:
                break
        return variants

    def build_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []
        styles = self.fetch_styles()
        log(f"Fetched {len(styles)} women denim styles from Algolia")

        for idx, style in enumerate(styles, start=1):
            style_id = str(style.get("id"))
            handle = style.get("handle", "")
            if not handle:
                continue

            tags = style.get("tags", [])
            meta_attrs = ((style.get("meta") or {}).get("attributes") or {})

            variants = self.fetch_variants(style_id)
            if not variants:
                log(f"No variants returned for style {style_id} ({handle})")
                continue

            style_name = extract_tag_value(tags, "styleGroup:") or meta_attrs.get(
                "styleGroup", ""
            )
            product_type = extract_tag_value(tags, "clothingType:") or meta_attrs.get(
                "clothingType", ""
            )
            country = extract_tag_value(tags, "country:")
            production_cost = extract_tag_value(tags, "productionCost:")
            site_exclusive = extract_tag_value(tags, "productType:")

            product_line = meta_attrs.get("sizeType", "")
            base_length = meta_attrs.get("length", "")

            for variant in variants:
                raw_size = safe_string(
                    variant.get("option1")
                    or (variant.get("options") or {}).get("size", "")
                )
                size_value = raw_size.rstrip("Pp") if raw_size else ""

                variant_size_type = product_line or ""
                if raw_size.endswith(("P", "p")) and variant_size_type.lower() != "petite":
                    variant_size_type = "Petite"
                inseam_value = derive_inseam_label(base_length, variant_size_type)

                sku_brand = safe_string(variant.get("sku"))
                sku_shopify = safe_string(variant.get("objectID"))
                variant_title = f"{style.get('title', '')} - {size_value}".strip(" -")
                published_at = format_date(
                    variant.get("published_at") or style.get("published_at")
                )
                color_value = extract_tag_value(variant.get("tags", tags), "styleColor:")

                available_raw = variant.get("inventory_available")
                available_bool = bool(available_raw)
                if isinstance(available_raw, str):
                    available_bool = available_raw.lower() == "true"

                row = [
                    style_id,
                    handle,
                    published_at,
                    style.get("title", ""),
                    style_name,
                    product_type,
                    join_tags(tags),
                    style.get("vendor", ""),
                    (style.get("body_html_safe") or "").strip(),
                    variant_title,
                    color_value,
                    size_value,
                    format_price(style.get("price")),
                    format_price(style.get("compare_at_price")),
                    style.get("price_range", ""),
                    "TRUE" if available_bool else "FALSE",
                    safe_string(variant.get("inventory_quantity")),
                    safe_string(variant.get("recently_ordered_count")),
                    safe_string(style.get("variants_inventory_count")),
                    sku_shopify,
                    sku_brand,
                    safe_string(variant.get("barcode")),
                    variant_size_type or product_line,
                    style.get("product_image", ""),
                    f"https://paige.com/products/{handle}",
                    meta_attrs.get("fit", ""),
                    inseam_value,
                    meta_attrs.get("rise", ""),
                    meta_attrs.get("wash", ""),
                    meta_attrs.get("colorCategory", ""),
                    country,
                    meta_attrs.get("stretch", ""),
                    production_cost,
                    site_exclusive,
                ]

                rows.append(row)

            log(f"Processed style {idx}/{len(styles)}: {handle} -> {len(variants)} variants")
            time.sleep(0.5)

        return rows

    def write_csv(self, rows: List[List[str]]) -> Path:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = OUTPUT_DIR / f"PAIGE_{timestamp}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADERS)
            writer.writerows(rows)
        log(f"Wrote {len(rows)} rows to {output_path}")
        return output_path

    def run(self) -> Path:
        log("Starting Paige scrape")
        rows = self.build_rows()
        output_path = self.write_csv(rows)
        log("Scrape complete")
        print("Done.")
        return output_path


def main() -> None:
    scraper = PaigeScraper()
    scraper.run()


if __name__ == "__main__":
    main()