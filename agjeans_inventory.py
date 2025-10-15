#!/usr/bin/env python3
"""Variant inventory exporter for AG Jeans women's denim."""

from __future__ import annotations

import csv
import os
import re
import time
from datetime import datetime
from fractions import Fraction
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.agjeans.com"
COLLECTION_PATH = "/collections/womens-jeans/products.json"
ALGOLIA_OBJECTS_URL = "https://ao8siisku6-dsn.algolia.net/1/indexes/*/objects"
ALGOLIA_HEADERS = {
    "X-Algolia-API-Key": "765ac45b27cdb1c591c28fd00d1bdf70",
    "X-Algolia-Application-Id": "AO8SIISKU6",
    "Content-Type": "application/json",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "Output")
LOG_PATH = os.path.join(OUTPUT_DIR, "agjeans_run.log")

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
    "Inseam",
    "Rise",
    "Leg Opening",
    "Price",
    "Compare at Price",
    "Price Range",
    "Available for Sale",
    "Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Hem Style",
    "Inseam Label",
    "Rise Label",
    "Color - Simplified",
    "Fabric Source",
    "Stretch",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        s.trust_env = False
        _session = s
    return _session


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except OSError:
        pass


TRANSIENT_STATUSES = {429, 500, 502, 503, 504}


def get_with_retry(url: str, *, params: Optional[Dict[str, Any]] = None, expect_json: bool = True) -> Any:
    session = get_session()
    for attempt in range(5):
        try:
            resp = session.get(url, params=params, timeout=40)
        except requests.RequestException as exc:
            if attempt == 4:
                raise
            sleep_for = 1.5 * (attempt + 1)
            log(f"[retry] GET {url} ({exc!r}) -> sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue

        if resp.status_code in TRANSIENT_STATUSES:
            if attempt == 4:
                resp.raise_for_status()
            sleep_for = 1.5 * (attempt + 1)
            log(f"[retry] GET {url} status {resp.status_code} -> sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue

        resp.raise_for_status()
        return resp.json() if expect_json else resp.text

    raise RuntimeError("GET retry loop exhausted")


def post_with_retry(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    session = get_session()
    for attempt in range(5):
        try:
            resp = session.post(url, headers=ALGOLIA_HEADERS, json=payload, timeout=40)
        except requests.RequestException as exc:
            if attempt == 4:
                raise
            sleep_for = 1.5 * (attempt + 1)
            log(f"[retry] POST {url} ({exc!r}) -> sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue

        if resp.status_code in TRANSIENT_STATUSES:
            if attempt == 4:
                resp.raise_for_status()
            sleep_for = 1.5 * (attempt + 1)
            log(f"[retry] POST {url} status {resp.status_code} -> sleep {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue

        resp.raise_for_status()
        return resp.json()

    raise RuntimeError("POST retry loop exhausted")


def parse_published_at(raw: Optional[str]) -> str:
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except Exception:
        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
        if match:
            year, month, day = match.groups()
            return f"{month}/{day}/{year[-2:]}"
    return ""


def format_price(raw: Any) -> str:
    if raw in (None, ""):
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    if text.startswith("$"):
        text = text[1:]
    text = text.replace(",", "")
    try:
        value = float(text)
    except ValueError:
        try:
            value = float(int(text))
        except Exception:
            return str(raw)
    if value > 999 and value % 100 == 0 and len(text) > 4:
        value = value / 100
    return f"${value:.2f}"


def clean_html_text(raw_html: Optional[str]) -> str:
    if not raw_html:
        return ""
    soup = BeautifulSoup(unescape(raw_html), "html.parser")
    return soup.get_text(" ", strip=True)


def stringify_identifier(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return format(value, "f").rstrip("0").rstrip(".")
    return str(value)


MEASURE_CACHE: Dict[str, Tuple[str, str]] = {}
MEASURE_PATTERN_RISE = re.compile(r"(front\s+)?rise\s*:\s*([^•\n]+)", re.IGNORECASE)
MEASURE_PATTERN_OPENING = re.compile(r"(bottom|leg)\s+opening\s*:\s*([^•\n]+)", re.IGNORECASE)


def parse_measure_value(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace("\u2033", "").replace("\u2032", "").replace("\"", "")
    cleaned = cleaned.replace("inches", "").replace("inch", "")
    cleaned = re.sub(r"[^0-9./\s]", " ", cleaned)
    tokens = [tok for tok in cleaned.split() if tok]
    if not tokens:
        return ""
    total = 0.0
    consumed = False
    for tok in tokens:
        try:
            if "/" in tok:
                total += float(Fraction(tok))
            else:
                total += float(tok)
            consumed = True
        except Exception:
            continue
    if not consumed:
        return ""
    if abs(total - round(total)) < 1e-6:
        return str(int(round(total)))
    return f"{total:.2f}".rstrip("0").rstrip(".")


def get_measurements(handle: str) -> Tuple[str, str]:
    if handle in MEASURE_CACHE:
        return MEASURE_CACHE[handle]

    url = f"{BASE_URL}/products/{handle}"
    try:
        html = get_with_retry(url, expect_json=False)
    except Exception as exc:
        log(f"[warn] failed to pull measurements for {handle}: {exc!r}")
        MEASURE_CACHE[handle] = ("", "")
        return MEASURE_CACHE[handle]

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" \n")
    rise = ""
    opening = ""

    for line in text.splitlines():
        stripped = line.strip()
        if not rise:
            match = MEASURE_PATTERN_RISE.search(stripped)
            if match:
                rise = parse_measure_value(match.group(2))
        if not opening:
            match = MEASURE_PATTERN_OPENING.search(stripped)
            if match:
                opening = parse_measure_value(match.group(2))
        if rise and opening:
            break

    MEASURE_CACHE[handle] = (rise, opening)
    return MEASURE_CACHE[handle]


def fetch_collection_products() -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    seen: set[str] = set()
    page = 1

    while True:
        params = {"limit": 250, "page": page}
        url = f"{BASE_URL}{COLLECTION_PATH}"
        log(f"[collection] page {page}")
        data = get_with_retry(url, params=params)
        page_products = data.get("products") or []
        if not page_products:
            log("[collection] no products returned -> stop")
            break

        new_items = [p for p in page_products if p.get("handle") not in seen]
        for prod in new_items:
            seen.add(prod.get("handle", ""))
            products.append(prod)
        log(f"[collection] added {len(new_items)} / {len(page_products)} items")
        if not new_items:
            break
        page += 1
        time.sleep(0.4)

    log(f"[collection] total unique products: {len(products)}")
    return products


def normalize_sku(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip().upper()


def fetch_algolia_variants(
    variant_ids: Iterable[Any],
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Return Algolia records keyed by SKU and variant (object) ID."""

    sku_map: Dict[str, Dict[str, Any]] = {}
    id_map: Dict[str, Dict[str, Any]] = {}
    chunk: List[str] = []

    def flush_chunk(requests_chunk: List[str]) -> None:
        if not requests_chunk:
            return

        payload = {
            "requests": [
                {"indexName": "shopify_main_products", "objectID": object_id}
                for object_id in requests_chunk
            ]
        }

        response = post_with_retry(ALGOLIA_OBJECTS_URL, payload)
        for result in response.get("results", []):
            if not isinstance(result, dict):
                continue
            if result.get("status") == 404:
                # Missing object
                continue
            object_id = stringify_identifier(result.get("objectID"))
            if object_id:
                id_map[object_id] = result
            sku_key = normalize_sku(result.get("sku"))
            if sku_key:
                sku_map[sku_key] = result

    for variant_id in variant_ids:
        object_id = stringify_identifier(variant_id)
        if not object_id:
            continue
        chunk.append(object_id)
        if len(chunk) >= 50:
            flush_chunk(chunk)
            chunk = []

    if chunk:
        flush_chunk(chunk)

    log(
        f"[algolia] hydrated {len(id_map)} variant records via direct object lookups"
    )
    return sku_map, id_map


def coerce_tags(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        return ", ".join(str(v) for v in value)
    return str(value)


def named_tag_value(hit: Dict[str, Any], key: str) -> str:
    tags = hit.get("named_tags") or {}
    value = tags.get(key)
    if isinstance(value, dict):
        if "value" in value:
            return stringify_identifier(value.get("value"))
        return ", ".join(str(v) for v in value.values() if v not in (None, ""))
    if isinstance(value, list):
        return ", ".join(str(v) for v in value if v is not None)
    return str(value) if value not in (None, "") else ""


def determine_style_total(
    variants: Iterable[Dict[str, Any]],
    algolia_id_map: Dict[str, Dict[str, Any]],
) -> str:
    totals: List[str] = []
    for variant in variants:
        variant_id = stringify_identifier((variant or {}).get("id"))
        if not variant_id:
            continue
        hit = algolia_id_map.get(variant_id)
        if not hit:
            continue
        total = hit.get("variants_inventory_count")
        if isinstance(total, (int, float)):
            totals.append(str(int(total)))
        elif isinstance(total, str) and total.strip():
            totals.append(total.strip())
    if totals:
        # values should match across variants; return the first non-empty entry
        return totals[0]

    summed = 0
    found_any = False
    for variant in variants:
        variant_id = stringify_identifier((variant or {}).get("id"))
        if not variant_id:
            continue
        hit = algolia_id_map.get(variant_id)
        if not hit:
            continue
        qty = hit.get("inventory_quantity")
        if isinstance(qty, (int, float)):
            summed += int(qty)
            found_any = True
        elif isinstance(qty, str) and qty.strip().isdigit():
            summed += int(qty.strip())
            found_any = True
    if found_any:
        return str(summed)
    return ""


def get_variant_image_url(variant: Dict[str, Any], product_images: List[Dict[str, Any]]) -> str:
    featured = variant.get("featured_image") or {}
    src = featured.get("src")
    if src:
        return src
    if product_images:
        first = product_images[0]
        if isinstance(first, dict):
            return first.get("src", "")
    return ""


def collect_algolia_fields(
    variant: Dict[str, Any],
    sku_map: Dict[str, Dict[str, Any]],
    id_map: Dict[str, Dict[str, Any]],
) -> Dict[str, str]:
    sku_value = normalize_sku((variant or {}).get("sku"))
    variant_id = stringify_identifier((variant or {}).get("id"))
    hit = {}
    if sku_value:
        hit = sku_map.get(sku_value, {})
    if not hit and variant_id:
        hit = id_map.get(variant_id, {})

    fields = {
        "Style Name": named_tag_value(hit, "Product"),
        "Product Type": named_tag_value(hit, "Category"),
        "Price Range": stringify_identifier(hit.get("price_range")),
        "Quantity Available": stringify_identifier(hit.get("inventory_quantity")),
        "Quantity of style": stringify_identifier(hit.get("variants_inventory_count")),
        "Barcode": stringify_identifier(hit.get("barcode")),
        "Jean Style": named_tag_value(hit, "Fit"),
        "Hem Style": named_tag_value(hit, "Hem"),
        "Inseam Label": named_tag_value(hit, "Length"),
        "Rise Label": named_tag_value(hit, "Rise"),
        "Color - Simplified": named_tag_value(hit, "Wash"),
        "Fabric Source": named_tag_value(hit, "Mill"),
        "Stretch": named_tag_value(hit, "Stretch"),
    }

    return fields


def assemble_rows(
    products: List[Dict[str, Any]],
    algolia_map: Dict[str, Dict[str, Any]],
    algolia_id_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product in products:
        handle = product.get("handle", "")
        rise, leg_opening = get_measurements(handle)
        published_at = parse_published_at(product.get("published_at"))
        description = clean_html_text(product.get("body_html"))
        tags = coerce_tags(product.get("tags"))
        style_total = determine_style_total(
            product.get("variants", []), algolia_id_map
        )

        for variant in product.get("variants", []):
            variant_id = stringify_identifier(variant.get("id"))
            algolia_fields = collect_algolia_fields(
                variant, algolia_map, algolia_id_map
            )

            product_title = product.get("title", "")
            color = variant.get("option1") or ""
            product_name = f"{product_title} - {color}" if color else product_title
            variant_title = variant.get("title", "")
            full_variant_title = (
                f"{product_title} - {variant_title}" if variant_title else product_title
            )

            row = {
                "Style Id": stringify_identifier(product.get("id")),
                "Handle": handle,
                "Published At": published_at,
                "Product": product_name,
                "Style Name": algolia_fields["Style Name"],
                "Product Type": algolia_fields["Product Type"],
                "Tags": tags,
                "Vendor": product.get("vendor", ""),
                "Description": description,
                "Variant Title": full_variant_title,
                "Color": color,
                "Size": variant.get("option2", ""),
                "Inseam": variant.get("option3", ""),
                "Rise": rise,
                "Leg Opening": leg_opening,
                "Price": format_price(variant.get("price")),
                "Compare at Price": format_price(variant.get("compare_at_price")),
                "Price Range": algolia_fields["Price Range"],
                "Available for Sale": "TRUE" if variant.get("available") else "FALSE",
                "Quantity Available": algolia_fields["Quantity Available"],
                "Quantity of style": algolia_fields["Quantity of style"] or style_total,
                "SKU - Shopify": variant_id,
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": algolia_fields["Barcode"],
                "Image URL": get_variant_image_url(variant, product.get("images", [])),
                "SKU URL": f"{BASE_URL}/products/{handle}",
                "Jean Style": algolia_fields["Jean Style"],
                "Hem Style": algolia_fields["Hem Style"],
                "Inseam Label": algolia_fields["Inseam Label"],
                "Rise Label": algolia_fields["Rise Label"],
                "Color - Simplified": algolia_fields["Color - Simplified"],
                "Fabric Source": algolia_fields["Fabric Source"],
                "Stretch": algolia_fields["Stretch"],
            }

            rows.append(row)

    return rows


def write_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("No data rows to write")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"AGJEANS_{timestamp}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


def main() -> None:
    log("[start] AG Jeans inventory export")
    products = fetch_collection_products()
    variant_ids: List[str] = []
    seen_ids: set[str] = set()
    for product in products:
        for variant in product.get("variants", []):
            object_id = stringify_identifier(variant.get("id"))
            if not object_id or object_id in seen_ids:
                continue
            seen_ids.add(object_id)
            variant_ids.append(object_id)

    sku_map, id_map = fetch_algolia_variants(variant_ids)
    rows = assemble_rows(products, sku_map, id_map)
    csv_path = write_csv(rows)
    log(f"[done] wrote {len(rows)} rows -> {csv_path}")


if __name__ == "__main__":
    main()
