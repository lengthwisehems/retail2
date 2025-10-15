#!/usr/bin/env python3
"""Scrape Haikure women's denim collection into a CSV export.

This script paginates the public Shopify collection feed, enriches the
variant records with PDP measurements and inventory details, and writes a
timestamped CSV alongside a lightweight run log.
"""
from __future__ import annotations

import csv
import datetime as dt
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DOMAIN = "https://haikure.com"
COLLECTION_ENDPOINT = (
    "https://haikure.com/collections/denim/products.json"
)
PRODUCT_PAGE_TEMPLATE = "https://haikure.com/products/{handle}"
PRODUCT_JSON_TEMPLATE = "https://haikure.com/products/{handle}.json"
BASE_OUTPUT_DIR = Path(__file__).resolve().parent
CSV_FOLDER = BASE_OUTPUT_DIR / "Output"
LOG_FILE = BASE_OUTPUT_DIR / "haikure_inventory.log"
CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Product",
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
    "Next Shipment",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)


@dataclass
class ProductMeta:
    description: str
    color: str
    sku_brand: str
    variant_quantity: Dict[str, int]
    variant_next_incoming: Dict[str, str]
    style_quantity_total: Optional[int]


@dataclass
class VariantJson:
    barcode: Optional[str]


class HaikureScraper:
    def __init__(self) -> None:
        self.session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST"),
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update({"User-Agent": USER_AGENT})
        CSV_FOLDER.mkdir(parents=True, exist_ok=True)
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        timestamp = dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        self.csv_path = CSV_FOLDER / f"HAIKURE_{timestamp}.csv"
        self.log_path = LOG_FILE
        self.log_lines: List[str] = []

    # ------------------------------------------------------------------
    # Logging helpers
    def log(self, message: str) -> None:
        stamp = dt.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        line = f"{stamp} {message}"
        print(line)
        self.log_lines.append(line)

    def flush_log(self) -> None:
        if self.log_lines:
            with self.log_path.open("a", encoding="utf-8") as log_file:
                log_file.write("\n".join(self.log_lines) + "\n")

    # ------------------------------------------------------------------
    # HTTP utilities
    def get_json(self, url: str, **params) -> dict:
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_html(self, url: str) -> str:
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text

    # ------------------------------------------------------------------
    def paginate_collection(self) -> Iterable[dict]:
        page = 1
        total = 0
        while True:
            self.log(f"[collection] page {page}")
            data = self.get_json(COLLECTION_ENDPOINT, limit=250, page=page)
            products = data.get("products") or []
            if not products:
                break
            for product in products:
                total += 1
                yield product
            page += 1
        self.log(f"[collection] total products: {total}")

    # ------------------------------------------------------------------
    def fetch_product_meta(self, handle: str) -> ProductMeta:
        page_url = PRODUCT_PAGE_TEMPLATE.format(handle=handle)
        html = self.get_html(page_url)
        soup = BeautifulSoup(html, "html.parser")
        description_block = soup.select_one("div.product-block__description")
        description = (
            normalize_whitespace(description_block.get_text(" ", strip=True))
            if description_block
            else ""
        )

        color = ""
        sku_brand = ""
        if description_block:
            block_text = description_block.get_text(" \n", strip=True)
            color = extract_color(block_text)
            if not color:
                first_li = description_block.find("li")
                if first_li:
                    color = extract_color(first_li.get_text(" ", strip=True))
            sku_brand = extract_sku_brand(block_text)

        inventory_script_text = None
        for script in soup.find_all("script"):
            script_text = script.string or script.get_text()
            if script_text and "window.inventories" in script_text:
                inventory_script_text = script_text
                break

        variant_quantity: Dict[str, int] = {}
        variant_next: Dict[str, str] = {}
        if inventory_script_text:
            variant_quantity, variant_next = parse_inventory_script(inventory_script_text)

        style_quantity_total = sum(variant_quantity.values()) if variant_quantity else None

        return ProductMeta(
            description=description,
            color=color,
            sku_brand=sku_brand,
            variant_quantity=variant_quantity,
            variant_next_incoming=variant_next,
            style_quantity_total=style_quantity_total,
        )

    def fetch_variant_barcodes(self, handle: str) -> Dict[str, VariantJson]:
        data = self.get_json(PRODUCT_JSON_TEMPLATE.format(handle=handle))
        variants = data.get("product", {}).get("variants", [])
        result: Dict[str, VariantJson] = {}
        for variant in variants:
            vid = str(variant.get("id"))
            barcode = variant.get("barcode")
            result[vid] = VariantJson(barcode=barcode if barcode else None)
        return result

    # ------------------------------------------------------------------
    def run(self) -> None:
        try:
            with self.csv_path.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=CSV_HEADERS)
                writer.writeheader()
                product_count = 0
                variant_count = 0
                for product in self.paginate_collection():
                    product_count += 1
                    handle = product.get("handle", "")
                    self.log(f"[product] {handle}")
                    try:
                        meta = self.fetch_product_meta(handle)
                    except Exception as exc:  # pragma: no cover - logging failure path
                        self.log(f"[warning] failed to parse PDP for {handle}: {exc}")
                        meta = ProductMeta("", "", "", {}, {}, None)
                    try:
                        barcode_map = self.fetch_variant_barcodes(handle)
                    except Exception as exc:  # pragma: no cover
                        self.log(f"[warning] failed to fetch barcode JSON for {handle}: {exc}")
                        barcode_map = {}

                    for variant in product.get("variants", []):
                        row = self.build_row(product, variant, meta, barcode_map)
                        writer.writerow(row)
                        variant_count += 1
                self.log(f"[summary] wrote {variant_count} variants from {product_count} products")
        finally:
            self.flush_log()

    # ------------------------------------------------------------------
    def build_row(
        self,
        product: dict,
        variant: dict,
        meta: ProductMeta,
        barcode_map: Dict[str, VariantJson],
    ) -> Dict[str, str]:
        variant_id = str(variant.get("id", ""))
        price = normalize_price(variant.get("price"))
        compare_at = normalize_price(variant.get("compare_at_price"))
        sku_shopify = variant_id
        sku_brand = meta.sku_brand
        barcode = barcode_map.get(variant_id).barcode if barcode_map.get(variant_id) else ""
        quantity = meta.variant_quantity.get(variant_id)
        next_ship = meta.variant_next_incoming.get(variant_id)
        size_value = extract_size(variant)

        row = {
            "Style Id": str(product.get("id", "")),
            "Handle": product.get("handle", ""),
            "Published At": format_shopify_date(product.get("published_at")),
            "Product": product.get("title", ""),
            "Product Type": product.get("product_type", ""),
            "Tags": ", ".join(product.get("tags", [])) if isinstance(product.get("tags"), list) else product.get("tags", ""),
            "Vendor": product.get("vendor", ""),
            "Description": meta.description,
            "Variant Title": f"{product.get('title', '')} // {variant.get('title', '')}",
            "Color": meta.color,
            "Size": size_value,
            "Price": price,
            "Compare at Price": compare_at,
            "Available for Sale": str(bool(variant.get("available", False))).upper(),
            "Quantity Available": format_quantity(quantity),
            "Quantity of style": format_quantity(meta.style_quantity_total),
            "Next Shipment": format_next_shipment(next_ship),
            "SKU - Shopify": sku_shopify,
            "SKU - Brand": sku_brand,
            "Barcode": barcode or "",
            "Image URL": extract_variant_image(variant, product),
            "SKU URL": f"{BASE_DOMAIN}/products/{product.get('handle', '')}",
        }
        return row


# ----------------------------------------------------------------------
# Helper functions

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def extract_size(variant: dict) -> str:
    for key in ("option1", "option2", "option3"):
        value = variant.get(key)
        if value and value.strip().lower() not in {"default title"}:
            return value.strip()
    title = variant.get("title")
    if title and title.strip().lower() not in {"default title"}:
        return title.strip()
    return ""


def extract_color(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"COLOR\s*[-:]\s*([^\n]+)", text, flags=re.I)
    if match:
        return normalize_whitespace(match.group(1))
    match = re.search(r"Colour\s*[-:]\s*([^\n]+)", text, flags=re.I)
    if match:
        return normalize_whitespace(match.group(1))
    # Fallback: first uppercase word sequence that looks like a color entry
    fallback = re.search(r"\b([A-Z][A-Z0-9\s]{2,})\b", text)
    return normalize_whitespace(fallback.group(1)) if fallback else ""


def extract_sku_brand(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"\b([A-Z0-9]{18})\b", text)
    if match:
        return match.group(1)
    return ""


def parse_inventory_script(script_text: str) -> Tuple[Dict[str, int], Dict[str, str]]:
    quantities: Dict[str, int] = {}
    next_dates: Dict[str, str] = {}
    if not script_text:
        return quantities, next_dates

    assignment_pattern = re.compile(
        r"window\.inventories\[['\"]\d+['\"]\]\[(?P<vid>\d+)\]\s*=\s*{(?P<body>[^{}]*)}",
        re.S,
    )

    for match in assignment_pattern.finditer(script_text):
        vid = match.group("vid")
        body = match.group("body")
        qty_match = re.search(r"['\"]quantity['\"]\s*:\s*(-?\d+)", body)
        if qty_match:
            quantities[vid] = int(qty_match.group(1))
        else:
            quantities.setdefault(vid, 0)

        next_match = re.search(
            r"['\"]next_incoming_date['\"]\s*:\s*(null|['\"]([^'\"]*)['\"])",
            body,
        )
        if next_match:
            if next_match.group(1).lower() == "null":
                next_dates[vid] = "null"
            elif next_match.group(2):
                next_dates[vid] = next_match.group(2)

    if not quantities:
        # Fallback to generic pattern
        pair_pattern = re.compile(
            r"\[(?P<vid>\d+)\]\s*=\s*{[^{}]*?['\"]quantity['\"]\s*:\s*(-?\d+)",
            re.S,
        )
        for match in pair_pattern.finditer(script_text):
            quantities[match.group("vid")] = int(match.group(2))

    return quantities, next_dates


def normalize_price(raw: Optional[str]) -> str:
    if raw is None or raw == "":
        return ""
    try:
        value = int(raw)
        return f"{value / 100:.2f}"
    except (TypeError, ValueError):
        try:
            value = float(raw)
            return f"{value:.2f}"
        except (TypeError, ValueError):
            return str(raw)


def format_shopify_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%m/%d/%y")
    except ValueError:
        return value


def format_next_shipment(value: Optional[str]) -> str:
    if value is None:
        return ""
    value = value.strip()
    if not value:
        return ""
    if value.lower() in {"null", "none", "undefined"}:
        return "null"
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%m/%d/%y")
    except ValueError:
        return value


def format_quantity(value: Optional[int]) -> str:
    if value is None:
        return ""
    return str(value)


def extract_variant_image(variant: dict, product: dict) -> str:
    featured = variant.get("featured_image")
    if isinstance(featured, dict) and featured.get("src"):
        return ensure_absolute_url(featured["src"])
    if product.get("image") and isinstance(product["image"], dict) and product["image"].get("src"):
        return ensure_absolute_url(product["image"]["src"])
    images = product.get("images", [])
    if images:
        first = images[0]
        if isinstance(first, dict) and first.get("src"):
            return ensure_absolute_url(first["src"])
        if isinstance(first, str):
            return ensure_absolute_url(first)
    return ""


def ensure_absolute_url(src: str) -> str:
    if not src:
        return ""
    if src.startswith("http://") or src.startswith("https://"):
        return src
    return f"https:{src}" if src.startswith("//") else f"{BASE_DOMAIN.rstrip('/')}/{src.lstrip('/')}"


# ----------------------------------------------------------------------

def main() -> None:
    scraper = HaikureScraper()
    scraper.run()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
