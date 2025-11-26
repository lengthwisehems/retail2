"""Favorite Daughter Rebuy data explorer.

Fetches every denim product from the Shopify collection feed and queries the
Rebuy custom widget for each product id, exporting both the product-level input
payload and the variant-level rows into a timestamped Excel workbook.
"""
from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from openpyxl import Workbook
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BRAND = "FAVORITEDAUGHTER"
COLLECTION_URL = "https://shopfavoritedaughter.com/collections/denim"
REBUY_WIDGET_ID = "193536"
REBUY_API_KEY = "f6f12c47c1ce0364a01d215c6b9ae5f235b37392"
REBUY_DOMAIN = "shopfavoritedaughter.com"

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH = OUTPUT_DIR / f"{BRAND.lower()}_rebuy_probe.log"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
    )
}

# Disable TLS warnings because Rebuy's certificate chain often breaks in CI.
disable_warnings(InsecureRequestWarning)


@dataclass
class ProductHandle:
    product_id: int
    handle: str
    title: str
    url: str


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def configure_logging() -> logging.Logger:
    logger = logging.getLogger("favoritedaughter_rebuy")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    except OSError as exc:  # pragma: no cover - fallback path
        fallback = OUTPUT_DIR / "favoritedaughter_rebuy_fallback.log"
        fh = logging.FileHandler(fallback, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.warning("Primary log unavailable (%s); using fallback %s", exc, fallback)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    session.verify = False
    return session


def request_json(
    session: requests.Session,
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None,
    retries: int = 3,
) -> Dict[str, Any]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=30, verify=False)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # pragma: no cover - network failure path
            last_exc = exc
            if logger:
                logger.warning("GET %s failed on attempt %s/%s: %s", url, attempt, retries, exc)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {url}: {last_exc}")


# ---------------------------------------------------------------------------
# Collection + Rebuy fetches
# ---------------------------------------------------------------------------
def fetch_collection_products(session: requests.Session, logger: logging.Logger) -> List[ProductHandle]:
    products: List[ProductHandle] = []
    page = 1
    while True:
        params = {"limit": 250, "page": page}
        url = f"{COLLECTION_URL.rstrip('/')}/products.json"
        logger.info("Fetching collection JSON page %s", page)
        data = request_json(session, url, params=params, logger=logger)
        entries = data.get("products", [])
        if not entries:
            break
        for prod in entries:
            product_id = int(prod["id"])
            handle = prod["handle"]
            title = prod.get("title", "")
            url = f"https://{REBUY_DOMAIN}/products/{handle}"
            products.append(ProductHandle(product_id, handle, title, url))
        page += 1
    logger.info("Collected %s products from collection feed", len(products))
    return products


def fetch_rebuy_input_products(
    session: requests.Session,
    product: ProductHandle,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    params = {
        "key": REBUY_API_KEY,
        "shopify_product_ids": str(product.product_id),
        "limit": 50,
        "product_groups": "yes",
        "uuid": str(uuid.uuid4()),
        "url": product.url,
    }
    base_url = f"https://rebuyengine.com/api/v1/custom/id/{REBUY_WIDGET_ID}"
    data = request_json(session, base_url, params=params, logger=logger)
    metadata = data.get("metadata", {})
    input_products = metadata.get("input_products", [])
    if not input_products:
        logger.warning("No input_products returned for %s (%s)", product.handle, product.product_id)
    return input_products


# ---------------------------------------------------------------------------
# Flattening helpers
# ---------------------------------------------------------------------------
def serialize_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def build_product_row(prod: Dict[str, Any]) -> Dict[str, Any]:
    row: Dict[str, Any] = {}
    for key, value in prod.items():
        if key == "variants":
            continue
        row[f"product.{key}"] = serialize_value(value)
    row["product.variant_count"] = len(prod.get("variants", []))
    return row


def build_variant_rows(prod: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    variants = prod.get("variants", [])
    for idx, variant in enumerate(variants, start=1):
        row: Dict[str, Any] = {
            "product.id": prod.get("id"),
            "product.handle": prod.get("handle"),
            "product.title": prod.get("title"),
            "variant.index": idx,
        }
        for key, value in variant.items():
            row[f"variant.{key}"] = serialize_value(value)
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Workbook output
# ---------------------------------------------------------------------------
def write_sheet(ws, rows: List[Dict[str, Any]], title: str) -> None:
    if not rows:
        ws.title = title
        ws.append(["No data"])
        return
    columns = sorted(rows[0].keys())
    # Ensure union of keys
    all_keys = set(columns)
    for row in rows:
        all_keys.update(row.keys())
    ordered_columns = sorted(all_keys)
    ws.title = title
    ws.append(ordered_columns)
    for row in rows:
        ws.append([row.get(col, "") for col in ordered_columns])


def export_workbook(products_rows: List[Dict[str, Any]], variant_rows: List[Dict[str, Any]]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"{BRAND}_rebuy_{timestamp}.xlsx"
    wb = Workbook()
    write_sheet(wb.active, products_rows, "InputProducts")
    write_sheet(wb.create_sheet(), variant_rows, "Variants")
    wb.save(output_path)
    return output_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    logger = configure_logging()
    session = build_session()
    logger.info("Starting Favorite Daughter Rebuy probe")
    products = fetch_collection_products(session, logger)
    product_rows: List[Dict[str, Any]] = []
    variant_rows: List[Dict[str, Any]] = []

    for prod in products:
        input_products = fetch_rebuy_input_products(session, prod, logger)
        for payload in input_products:
            product_rows.append(build_product_row(payload))
            variant_rows.extend(build_variant_rows(payload))

    output_path = export_workbook(product_rows, variant_rows)
    logger.info("Workbook written to %s", output_path)


if __name__ == "__main__":
    main()
