"""Utility to export Fidelity Denim Globo filter payloads to Excel."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import requests
import urllib3
from openpyxl import Workbook
from requests.adapters import HTTPAdapter, Retry

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "fidelitydenim_globo_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "fidelitydenim_globo_run.log"

OUTPUT_DIR.mkdir(exist_ok=True)

REQUEST_TIMEOUT = 30
GLOBO_ENDPOINT = "https://filter-x3.globo.io/api/apiFilter"
DEFAULT_GLOBO_PARAMS = {
    "callback": "jQuery37106542333122937476_1761684550758",
    "filter_id": "9062",
    "shop": "fidelitydenim.myshopify.com",
    "collection": "273471111204",
    "sort_by": "created-descending",
    "country": "US",
    "limit": "100",
    "event": "init",
    "cid": "a1aa7f84-0adc-4acf-aaca-f60f84f06fb6",
    "did": "629a04a1-5b50-45f0-bf2e-2eab7defb407",
    "page_type": "collection",
    "ncp": "60",
}

GLOBO_COLLECTIONS = [
    (
        "fidelity_women",
        DEFAULT_GLOBO_PARAMS,
    ),
    (
        "modern_american",
        {
            "callback": "jQuery37108292485561334944_1761760733001",
            "filter_id": "9062",
            "shop": "fidelitydenim.myshopify.com",
            "collection": "273462034468",
            "country": "US",
            "event": "init",
            "cid": "32fd0d14-d567-43f9-b4f1-b02d752ae06d",
            "did": "38601c22-daba-49d5-b8ef-f28d245aab2a",
            "page_type": "collection",
            "ncp": "19",
        },
    ),
    (
        "fidelity_womens_sale",
        {
            "callback": "jQuery37109486356583540366_1761761266979",
            "filter_id": "9062",
            "shop": "fidelitydenim.myshopify.com",
            "collection": "273470652452",
            "country": "US",
            "limit": "48",
            "event": "init",
            "cid": "dcd12cde-ec2d-4045-8574-fb32c382e757",
            "did": "8cd8eb6d-1cc1-4de1-b041-2a5868b51403",
            "page_type": "collection",
            "ncp": "24",
        },
    ),
]

STOREFRONT_ENDPOINT = "https://fidelitydenim.com/api/2025-07/graphql.json"
STOREFRONT_TOKEN = "51cab5df1462f88a7245a3066803b9c1"

COLLECTION_HANDLES = [
    "fidelity-women",
    "modern-american",
    "fidelity-womens-sale",
]

COLLECTION_PRODUCT_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    id
    handle
    title
    products(first: 100, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        cursor
        node {
          id
          handle
          title
          description
          descriptionHtml
          productType
          tags
          vendor
          onlineStoreUrl
          createdAt
          updatedAt
          publishedAt
          totalInventory
          seo {
            title
            description
          }
          featuredImage {
            url
            altText
          }
          priceRange {
            minVariantPrice {
              amount
              currencyCode
            }
            maxVariantPrice {
              amount
              currencyCode
            }
          }
          compareAtPriceRange {
            minVariantPrice {
              amount
              currencyCode
            }
            maxVariantPrice {
              amount
              currencyCode
            }
          }
          options {
            id
            name
            values
          }
          collections(first: 10) {
            edges {
              node {
                id
                handle
                title
              }
            }
          }
          images(first: 10) {
            edges {
              node {
                url
                altText
              }
            }
          }
          variants(first: 250) {
            edges {
              cursor
              node {
                id
                title
                sku
                barcode
                availableForSale
                quantityAvailable
                requiresShipping
                weight
                weightUnit
                selectedOptions {
                  name
                  value
                }
                price {
                  amount
                  currencyCode
                }
                compareAtPrice {
                  amount
                  currencyCode
                }
                image {
                  url
                  altText
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


def configure_logging() -> logging.Logger:
    handlers: List[logging.Handler] = []
    selected_path: Path | None = None

    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(path)
            handlers.append(file_handler)
            selected_path = path
            if path != LOG_PATH:
                print(
                    "WARNING: Primary log path %s unavailable. Using fallback log at %s." % (LOG_PATH, path),
                    flush=True,
                )
            break
        except (OSError, PermissionError) as exc:
            print(
                f"WARNING: Unable to open log file {path}: {exc}. Continuing without this path.",
                flush=True,
            )

    handlers.append(logging.StreamHandler())

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )

    logger = logging.getLogger("fidelitydenim_globo_dump")
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
        "Accept": "application/json, text/javascript, */*; q=0.01",
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
SESSION.verify = False  # filter-x3.globo.io serves an incomplete certificate chain in this environment
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def strip_jsonp(payload: str) -> str:
    start = payload.find("(")
    end = payload.rfind(")")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("Unexpected JSONP payload format; unable to locate parentheses boundary.")
    return payload[start + 1 : end]


def fetch_globo_payload(params: Dict[str, Any]) -> Dict[str, Any]:
    query = dict(DEFAULT_GLOBO_PARAMS)
    query.update(params)

    LOGGER.info(
        "Requesting globo.io payload from %s with collection %s",
        GLOBO_ENDPOINT,
        query.get("collection"),
    )
    response = SESSION.get(
        GLOBO_ENDPOINT,
        params=query,
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    response.raise_for_status()
    raw_text = response.text
    try:
        json_text = strip_jsonp(raw_text)
        data = json.loads(json_text)
    except (ValueError, json.JSONDecodeError) as exc:
        LOGGER.error("Failed to parse Globo JSONP payload: %s", exc)
        LOGGER.debug("Globo response sample: %s", raw_text[:4000])
        raise
    LOGGER.info(
        "Received Globo payload with %d products and %d filters.",
        len(data.get("products", []) or []),
        len(data.get("filters", []) or []),
    )
    return data


def normalise_field_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def flatten_products(data: Dict[str, Any], source_label: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    products: Iterable[Dict[str, Any]] = data.get("products", []) or []

    for product in products:
        base: Dict[str, Any] = {}
        base["source_collection"] = source_label
        for key, value in product.items():
            if key == "variants":
                continue
            column_name = f"product_{key}"
            base[column_name] = normalise_field_value(value)

        variants: Iterable[Dict[str, Any]] = product.get("variants", []) or []
        if not variants:
            rows.append(dict(base))
            continue
        for variant in variants:
            row = dict(base)
            for key, value in variant.items():
                column_name = f"variant_{key}"
                row[column_name] = normalise_field_value(value)
            rows.append(row)
    return rows


def execute_storefront_query(variables: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
    }
    response = SESSION.post(
        STOREFRONT_ENDPOINT,
        json={"query": COLLECTION_PRODUCT_QUERY, "variables": variables},
        headers=headers,
        timeout=REQUEST_TIMEOUT,
        verify=False,
    )
    response.raise_for_status()
    payload = response.json()
    if "errors" in payload:
        raise RuntimeError(f"Storefront API returned errors: {payload['errors']}")
    return payload


def flatten_storefront_rows(handles: Sequence[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for handle in handles:
        LOGGER.info("Fetching Storefront API products for collection handle '%s'", handle)
        cursor: str | None = None
        while True:
            payload = execute_storefront_query({"handle": handle, "cursor": cursor})
            collection = (
                payload.get("data", {})
                .get("collection")
            )
            if not collection:
                LOGGER.warning("No collection returned for handle '%s'", handle)
                break
            collection_info = {
                "collection_id": normalise_field_value(collection.get("id")),
                "collection_handle": normalise_field_value(collection.get("handle")),
                "collection_title": normalise_field_value(collection.get("title")),
            }
            products_connection = collection.get("products") or {}
            product_edges: Iterable[Dict[str, Any]] = products_connection.get("edges", []) or []
            for edge in product_edges:
                product = edge.get("node", {})
                base: Dict[str, Any] = dict(collection_info)
                base["products_edge_cursor"] = edge.get("cursor", "")
                for key, value in product.items():
                    if key == "variants":
                        continue
                    column_name = f"product_{key}"
                    base[column_name] = normalise_field_value(value)
                variants_connection = product.get("variants") or {}
                variant_edges: Iterable[Dict[str, Any]] = variants_connection.get("edges", []) or []
                if not variant_edges:
                    rows.append(dict(base))
                    continue
                for variant_edge in variant_edges:
                    variant = variant_edge.get("node", {})
                    row = dict(base)
                    row["variant_edge_cursor"] = variant_edge.get("cursor", "")
                    for key, value in variant.items():
                        column_name = f"variant_{key}"
                        row[column_name] = normalise_field_value(value)
                    rows.append(row)
            page_info = products_connection.get("pageInfo") or {}
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
                LOGGER.info(
                    "Collection '%s' has additional Storefront pages; continuing with cursor %s",
                    handle,
                    cursor,
                )
                if not cursor:
                    LOGGER.warning(
                        "Missing endCursor for collection '%s' despite hasNextPage; aborting pagination.",
                        handle,
                    )
                    break
            else:
                break
    LOGGER.info("Flattened %d rows from the Storefront API", len(rows))
    return rows


def determine_column_order(rows: Sequence[Dict[str, Any]]) -> List[str]:
    order: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in order:
                order.append(key)
    return order


def write_excel_sheets(
    sheet_rows: Dict[str, Dict[str, Any]],
    output_path: Path,
) -> None:
    workbook = Workbook()
    first_sheet = True

    for sheet_name, data in sheet_rows.items():
        columns: Sequence[str] = data["columns"]
        rows: Sequence[Dict[str, Any]] = data["rows"]
        if first_sheet:
            sheet = workbook.active
            sheet.title = sheet_name
            first_sheet = False
        else:
            sheet = workbook.create_sheet(title=sheet_name)
        sheet.append(list(columns))
        for row in rows:
            sheet.append([row.get(col, "") for col in columns])

    workbook.save(output_path)
    LOGGER.info("Exported workbook to %s", output_path)


def main() -> None:
    globo_rows: List[Dict[str, Any]] = []
    for label, params in GLOBO_COLLECTIONS:
        data = fetch_globo_payload(params)
        subset = flatten_products(data, label)
        LOGGER.info(
            "Flattened %d rows from Globo collection '%s'",
            len(subset),
            label,
        )
        globo_rows.extend(subset)

    if not globo_rows:
        LOGGER.warning("No rows were generated from any Globo payload.")
    globo_columns = determine_column_order(globo_rows)

    storefront_rows = flatten_storefront_rows(COLLECTION_HANDLES)
    storefront_columns = determine_column_order(storefront_rows)

    sheets: Dict[str, Dict[str, Any]] = {}
    if globo_rows:
        sheets["globo_raw"] = {"columns": globo_columns, "rows": globo_rows}
    if storefront_rows:
        sheets["storefront_raw"] = {
            "columns": storefront_columns,
            "rows": storefront_rows,
        }

    if not sheets:
        LOGGER.warning("No data collected; skipping workbook write.")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"FIDELITYDENIM_GLOBO_{timestamp}.xlsx"
    write_excel_sheets(sheets, output_path)


if __name__ == "__main__":
    main()
