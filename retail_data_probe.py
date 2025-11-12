"""Brand-agnostic probe that inspects Shopify collection feeds and Storefront APIs."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
import urllib3
from bs4 import BeautifulSoup
from openpyxl import Workbook
from requests.adapters import HTTPAdapter, Retry

# ---------------------------------------------------------------------------
# Brand-specific configuration
# ---------------------------------------------------------------------------
BRAND = "AMO"
COLLECTION_URL = "https://www.amodenim.com/collections/denim"
MYSHOPIFY = "https://amo-denim.myshopify.com"
GRAPHQL = ""
X_SHOPIFY_STOREFRONT_ACCESS_TOKEN = ""
GRAPHQL_FILTER_TAG = ""
STOREFRONT_COLLECTION_HANDLES: List[str] = ["denim"]

# ---------------------------------------------------------------------------
# Derived paths and constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND_SLUG = BRAND.lower().replace(" ", "_") or "brand"
LOG_PATH = BASE_DIR / f"{BRAND_SLUG}_probe_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / f"{BRAND_SLUG}_probe_run.log"

REQUEST_TIMEOUT = 30
TRANSIENT_STATUS = {429, 500, 502, 503, 504}
GRAPHQL_PAGE_SIZE = 100
MAX_SCRIPT_FETCHES = 25
TOKEN_REGEX = re.compile(r"\b[0-9a-f]{32}\b", re.IGNORECASE)

DEFAULT_GRAPHQL_VERSIONS = [
    "api/2025-10/graphql.json",
    "api/2024-01/graphql.json",
    "api/2025-01/graphql.json",
    "api/2025-07/graphql.json",
    "api/2025-04/graphql.json",
    "api/unstable/graphql.json",
    "api/2024-04/graphql.json",
    "api/2023-01/graphql.json",
    "api/2023-04/graphql.json",
]

COLLECTION_QUERY = """
query CollectionProducts($handle: String!, $cursor: String, $pageSize: Int!) {
  collection(handle: $handle) {
    id
    handle
    title
    products(first: $pageSize, after: $cursor) {
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
          featuredImage {
            url
            altText
          }
          images(first: 20) {
            edges {
              cursor
              node {
                url
                altText
              }
            }
          }
          options {
            id
            name
            values
          }
          collections(first: 10) {
            edges {
              cursor
              node {
                id
                handle
                title
              }
            }
          }
          metafields(first: 20) {
            edges {
              cursor
              node {
                namespace
                key
                value
                type
              }
            }
          }
          variants(first: 100) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              cursor
              node {
                id
                title
                sku
                barcode
                availableForSale
                currentlyNotInStock
                quantityAvailable
                requiresShipping
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
                metafields(first: 10) {
                  edges {
                    cursor
                    node {
                      namespace
                      key
                      value
                      type
                    }
                  }
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

PRODUCTS_QUERY = """
query ProductsProbe($cursor: String, $pageSize: Int!, $query: String) {
  products(first: $pageSize, after: $cursor, query: $query) {
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
        featuredImage {
          url
          altText
        }
        images(first: 20) {
          edges {
            cursor
            node {
              url
              altText
            }
          }
        }
        options {
          id
          name
          values
        }
        collections(first: 10) {
          edges {
            cursor
            node {
              id
              handle
              title
            }
          }
        }
        metafields(first: 20) {
          edges {
            cursor
            node {
              namespace
              key
              value
              type
            }
          }
        }
        variants(first: 100) {
          pageInfo {
            hasNextPage
            endCursor
          }
          edges {
            cursor
            node {
              id
              title
              sku
              barcode
              availableForSale
              currentlyNotInStock
              quantityAvailable
              requiresShipping
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
              metafields(first: 10) {
                edges {
                  cursor
                  node {
                    namespace
                    key
                    value
                    type
                  }
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

SHOP_PROBE_QUERY = "query { shop { name primaryDomain { url } } }"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def configure_logging() -> logging.Logger:
    logger = logging.getLogger(f"retail_probe_{BRAND_SLUG}")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    handler: logging.Handler
    try:
        handler = logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8")
    except OSError as exc:
        fallback = FALLBACK_LOG_PATH
        handler = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        logger.warning(
            "Primary log path %s unavailable (%s); using %s", LOG_PATH, exc, fallback
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=TRANSIENT_STATUS,
        allowed_methods=("GET", "POST"),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    session.verify = False
    return session


def normalize_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return value.as_posix()
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def flatten_value(value: Any, prefix: str) -> Dict[str, Any]:
    items: Dict[str, Any] = {}
    if isinstance(value, dict):
        for key, inner in value.items():
            new_prefix = f"{prefix}.{key}" if prefix else key
            items.update(flatten_value(inner, new_prefix))
    elif isinstance(value, list):
        for index, inner in enumerate(value):
            new_prefix = f"{prefix}[{index}]" if prefix else f"[{index}]"
            items.update(flatten_value(inner, new_prefix))
    else:
        items[prefix] = value
    return items


def flatten_record(record: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in record.items():
        flat.update(flatten_value(value, key))
    return flat


def write_sheet(sheet, rows: List[Dict[str, Any]]):
    if not rows:
        sheet.append(["No data"])
        return
    columns = sorted({key for row in rows for key in row.keys()})
    sheet.append(columns)
    for row in rows:
        sheet.append([normalize_cell(row.get(column)) for column in columns])


def fetch_collection_html(session: requests.Session, logger: logging.Logger) -> str:
    if not COLLECTION_URL:
        logger.info("No COLLECTION_URL configured; skipping HTML fetch")
        return ""
    logger.info("Fetching collection HTML from %s", COLLECTION_URL)
    try:
        response = session.get(COLLECTION_URL, timeout=REQUEST_TIMEOUT, verify=False)
        response.raise_for_status()
        return response.text
    except requests.RequestException as exc:
        logger.warning("Failed to fetch collection HTML: %s", exc)
        return ""


def build_products_json_url() -> Optional[str]:
    if not COLLECTION_URL:
        return None
    parts = urlsplit(COLLECTION_URL)
    path = parts.path.rstrip("/") + "/products.json"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def fetch_collection_json(session: requests.Session, logger: logging.Logger) -> List[Dict[str, Any]]:
    products_json_url = build_products_json_url()
    if not products_json_url:
        logger.info("No collection JSON URL computed; skipping JSON extraction")
        return []

    page = 1
    all_products: List[Dict[str, Any]] = []
    while True:
        params = {"limit": 250, "page": page}
        logger.info("Fetching collection JSON page %s", page)
        try:
            response = session.get(
                products_json_url, params=params, timeout=REQUEST_TIMEOUT, verify=False
            )
        except requests.RequestException as exc:
            logger.warning("Collection JSON request failed: %s", exc)
            break

        if not response.ok:
            logger.warning(
                "Collection JSON request returned status %s", response.status_code
            )
            break

        try:
            data = response.json()
        except ValueError:
            logger.warning("Collection JSON response was not valid JSON")
            break

        products = data.get("products") or []
        if not products:
            logger.info("No products found on page %s; stopping pagination", page)
            break

        all_products.extend(products)
        if len(products) < 250:
            break
        page += 1
        time.sleep(0.5)

    logger.info("Collected %s products from collection JSON", len(all_products))
    rows: List[Dict[str, Any]] = []
    for product in all_products:
        flat = flatten_record({"product": product})
        rows.append(flat)
    return rows


def make_absolute(url: str, base: str) -> str:
    if not url:
        return url
    return urljoin(base, url)


def discover_tokens(
    session: requests.Session, html: str, logger: logging.Logger
) -> List[Tuple[str, str]]:
    tokens: Dict[str, str] = {}
    if html:
        for token in set(TOKEN_REGEX.findall(html)):
            tokens.setdefault(token, "collection_html")

        soup = BeautifulSoup(html, "html.parser")
        script_urls: List[str] = []
        for script in soup.find_all("script"):
            src = script.get("src")
            if src:
                absolute = make_absolute(src, COLLECTION_URL)
                script_urls.append(absolute)
                for token in set(TOKEN_REGEX.findall(absolute)):
                    tokens.setdefault(token, f"script_url:{absolute}")
            if script.string:
                for token in set(TOKEN_REGEX.findall(script.string)):
                    tokens.setdefault(token, "inline_script")

        for index, script_url in enumerate(script_urls[:MAX_SCRIPT_FETCHES]):
            logger.info(
                "Fetching script %s/%s for token discovery: %s",
                index + 1,
                min(len(script_urls), MAX_SCRIPT_FETCHES),
                script_url,
            )
            try:
                response = session.get(script_url, timeout=REQUEST_TIMEOUT, verify=False)
            except requests.RequestException as exc:
                logger.debug("Failed to fetch script %s: %s", script_url, exc)
                continue
            if not response.ok:
                logger.debug(
                    "Script %s returned status %s", script_url, response.status_code
                )
                continue
            for token in set(TOKEN_REGEX.findall(response.text)):
                tokens.setdefault(token, f"script_body:{script_url}")

    logger.info("Discovered %s potential tokens", len(tokens))
    return [(token, source) for token, source in tokens.items()]


def determine_graphql_endpoints() -> List[str]:
    endpoints: List[str] = []
    if GRAPHQL:
        endpoints.append(GRAPHQL.strip())
    if MYSHOPIFY:
        base = MYSHOPIFY.rstrip("/") + "/"
        for version in DEFAULT_GRAPHQL_VERSIONS:
            endpoints.append(urljoin(base, version))
    return list(dict.fromkeys(endpoint for endpoint in endpoints if endpoint))


def perform_graphql_request(
    session: requests.Session,
    endpoint: str,
    payload: Dict[str, Any],
    token: Optional[str],
) -> Tuple[Optional[requests.Response], Optional[Dict[str, Any]]]:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Shopify-Storefront-Access-Token"] = token
    try:
        response = session.post(
            endpoint,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )
    except requests.RequestException:
        return None, None

    try:
        data = response.json()
    except ValueError:
        data = None
    return response, data


def probe_graphql_endpoints(
    session: requests.Session,
    endpoints: Sequence[str],
    provided_token: Optional[str],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    access_rows: List[Dict[str, Any]] = []
    operational: List[str] = []

    for endpoint in endpoints:
        attempts: List[Tuple[Optional[str], str]] = []
        if provided_token:
            attempts.append((provided_token, "provided_token"))
        attempts.append((None, "unauthenticated"))

        for token, token_source in attempts:
            payload = {"query": SHOP_PROBE_QUERY}
            response, data = perform_graphql_request(session, endpoint, payload, token)
            entry: Dict[str, Any] = {
                "endpoint": endpoint,
                "token": token or "",
                "token_source": token_source,
                "status_code": getattr(response, "status_code", ""),
                "ok": bool(response and response.ok),
            }
            if response is None:
                entry["note"] = "request_exception"
            elif not response.ok:
                entry["note"] = f"HTTP_{response.status_code}"
            else:
                shop = ((data or {}).get("data") or {}).get("shop") if data else None
                if shop:
                    entry["shop_name"] = shop.get("name")
                    entry["primary_domain"] = (shop.get("primaryDomain") or {}).get("url")
                    entry["note"] = "success"
                else:
                    errors = (data or {}).get("errors") if data else None
                    entry["note"] = f"errors:{len(errors)}" if errors else "no_shop_data"
            access_rows.append(entry)
            if entry["ok"] and not token and endpoint not in operational:
                operational.append(endpoint)
    return access_rows, operational


def apply_tag_filter(product: Dict[str, Any]) -> bool:
    if not GRAPHQL_FILTER_TAG:
        return True
    tags = product.get("tags") or []
    lowered = {str(tag).lower() for tag in tags}
    return GRAPHQL_FILTER_TAG.lower() in lowered


def flatten_graphql_product(
    collection_info: Dict[str, Any],
    edge_cursor: str,
    product: Dict[str, Any],
    variant_edge: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    row: Dict[str, Any] = dict(collection_info)
    row["products_edge_cursor"] = edge_cursor

    product_copy = dict(product)
    variants = product_copy.pop("variants", None)
    if isinstance(variants, dict):
        page_info = variants.get("pageInfo") or {}
        row["variants_hasNextPage"] = page_info.get("hasNextPage")
        row["variants_endCursor"] = page_info.get("endCursor")
    flat_product = flatten_record({"product": product_copy})
    row.update(flat_product)

    if variant_edge is None:
        return row

    variant = dict(variant_edge.get("node") or {})
    row["variant_edge_cursor"] = variant_edge.get("cursor", "")
    flat_variant = flatten_record({"variant": variant})
    row.update(flat_variant)
    return row


def collect_storefront_from_collections(
    session: requests.Session,
    endpoint: str,
    token: Optional[str],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Optional[int], str]:
    rows: List[Dict[str, Any]] = []
    first_status: Optional[int] = None
    note = ""

    for handle in STOREFRONT_COLLECTION_HANDLES:
        cursor: Optional[str] = None
        while True:
            payload = {
                "query": COLLECTION_QUERY,
                "variables": {
                    "handle": handle,
                    "cursor": cursor,
                    "pageSize": GRAPHQL_PAGE_SIZE,
                },
            }
            response, data = perform_graphql_request(session, endpoint, payload, token)
            if first_status is None and response is not None:
                first_status = response.status_code
            if response is None:
                return [], first_status, "request_exception"
            if not response.ok:
                return [], first_status, f"HTTP_{response.status_code}"

            collection = ((data or {}).get("data") or {}).get("collection") if data else None
            if not collection:
                errors = (data or {}).get("errors") if data else None
                return [], first_status, (
                    f"no_collection_data:{len(errors)}" if errors else "no_collection_data"
                )

            collection_info = {
                "collection_id": collection.get("id"),
                "collection_handle": collection.get("handle"),
                "collection_title": collection.get("title"),
            }
            products_connection = collection.get("products") or {}
            edges: Iterable[Dict[str, Any]] = products_connection.get("edges") or []
            for edge in edges:
                product = edge.get("node") or {}
                if not apply_tag_filter(product):
                    continue
                variants_connection = product.get("variants") or {}
                variant_edges: Iterable[Dict[str, Any]] = variants_connection.get("edges") or []
                if not variant_edges:
                    rows.append(
                        flatten_graphql_product(
                            collection_info, edge.get("cursor", ""), product, None
                        )
                    )
                else:
                    for variant_edge in variant_edges:
                        rows.append(
                            flatten_graphql_product(
                                collection_info, edge.get("cursor", ""), product, variant_edge
                            )
                        )
            page_info = products_connection.get("pageInfo") or {}
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
                logger.info(
                    "Collection %s has additional Storefront pages; continuing", handle
                )
                time.sleep(0.5)
            else:
                break
    note = "success" if rows else "no_rows"
    return rows, first_status, note


def build_product_query_string() -> Optional[str]:
    query_parts: List[str] = []
    if GRAPHQL_FILTER_TAG:
        if " " in GRAPHQL_FILTER_TAG:
            query_parts.append(f'tag:"{GRAPHQL_FILTER_TAG}"')
        else:
            query_parts.append(f"tag:{GRAPHQL_FILTER_TAG}")
    for handle in STOREFRONT_COLLECTION_HANDLES:
        query_parts.append(f"collection:{handle}")
    return " ".join(query_parts) if query_parts else None


def collect_storefront_from_products(
    session: requests.Session,
    endpoint: str,
    token: Optional[str],
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], Optional[int], str]:
    rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    query_string = build_product_query_string()
    first_status: Optional[int] = None

    while True:
        payload = {
            "query": PRODUCTS_QUERY,
            "variables": {
                "cursor": cursor,
                "pageSize": GRAPHQL_PAGE_SIZE,
                "query": query_string,
            },
        }
        response, data = perform_graphql_request(session, endpoint, payload, token)
        if first_status is None and response is not None:
            first_status = response.status_code
        if response is None:
            return [], first_status, "request_exception"
        if not response.ok:
            return [], first_status, f"HTTP_{response.status_code}"

        products_connection = ((data or {}).get("data") or {}).get("products") if data else None
        if not products_connection:
            errors = (data or {}).get("errors") if data else None
            return [], first_status, (
                f"no_products_data:{len(errors)}" if errors else "no_products_data"
            )

        edges: Iterable[Dict[str, Any]] = products_connection.get("edges") or []
        for edge in edges:
            product = edge.get("node") or {}
            if not apply_tag_filter(product):
                continue
            variants_connection = product.get("variants") or {}
            variant_edges: Iterable[Dict[str, Any]] = variants_connection.get("edges") or []
            if not variant_edges:
                rows.append(
                    flatten_graphql_product(
                        {"collection_handle": ""}, edge.get("cursor", ""), product, None
                    )
                )
            else:
                for variant_edge in variant_edges:
                    rows.append(
                        flatten_graphql_product(
                            {"collection_handle": ""}, edge.get("cursor", ""), product, variant_edge
                        )
                    )
        page_info = products_connection.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
            logger.info("Products query returned more pages; continuing")
            time.sleep(0.5)
        else:
            break
    note = "success" if rows else "no_rows"
    return rows, first_status, note


def gather_storefront_data(
    session: requests.Session,
    html: str,
    logger: logging.Logger,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    endpoints = determine_graphql_endpoints()
    if not endpoints:
        logger.info("No GraphQL endpoints configured; skipping Storefront extraction")
        return [], []

    provided_token = X_SHOPIFY_STOREFRONT_ACCESS_TOKEN or None
    access_rows, operational = probe_graphql_endpoints(
        session, endpoints, provided_token, logger
    )

    candidate_tokens: List[Tuple[Optional[str], str]] = []
    if provided_token:
        candidate_tokens.append((provided_token, "provided_token"))

    discovered = discover_tokens(session, html, logger)
    for token, source in discovered:
        candidate_tokens.append((token, source))

    candidate_tokens.append((None, "no_token"))

    tried: set = set()
    endpoints_to_use = operational or list(endpoints)

    for token, source in candidate_tokens:
        if (token, source) in tried:
            continue
        tried.add((token, source))

        for endpoint in endpoints_to_use:
            if STOREFRONT_COLLECTION_HANDLES:
                rows, status, note = collect_storefront_from_collections(
                    session, endpoint, token, logger
                )
            else:
                rows, status, note = collect_storefront_from_products(
                    session, endpoint, token, logger
                )

            access_rows.append(
                {
                    "endpoint": endpoint,
                    "token": token or "",
                    "token_source": source,
                    "status_code": status or "",
                    "ok": note == "success",
                    "note": note,
                }
            )

            if rows:
                logger.info(
                    "Storefront extraction succeeded with endpoint %s using token source %s",
                    endpoint,
                    source,
                )
                return rows, access_rows
    logger.warning("Storefront extraction did not return any rows")
    return [], access_rows


def export_workbook(
    json_rows: List[Dict[str, Any]],
    storefront_rows: List[Dict[str, Any]],
    access_rows: List[Dict[str, Any]],
) -> Path:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "JSON"
    write_sheet(sheet, json_rows)

    storefront_sheet = workbook.create_sheet("Storefront")
    write_sheet(storefront_sheet, storefront_rows)

    access_sheet = workbook.create_sheet("Storefront_access")
    write_sheet(access_sheet, access_rows)

    timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"{BRAND_SLUG}_probe_{timestamp}.xlsx"
    workbook.save(output_path)
    return output_path


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main() -> None:
    logger = configure_logging()
    session = build_session()
    html = fetch_collection_html(session, logger)
    json_rows = fetch_collection_json(session, logger)
    storefront_rows, access_rows = gather_storefront_data(session, html, logger)
    output_path = export_workbook(json_rows, storefront_rows, access_rows)
    logger.info("Workbook written to %s", output_path.as_posix())


if __name__ == "__main__":
    main()
