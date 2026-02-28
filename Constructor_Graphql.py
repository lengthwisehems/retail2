"""Constructor + Shopify Storefront GraphQL workbook probe.

Outputs one Excel file with two tabs:
- Constructor: one row per variant with product-level + variant-level Constructor fields
- GraphQL: product/variant rows with custom filter columns and ordered base columns
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

import requests
from openpyxl import Workbook
from requests.adapters import HTTPAdapter, Retry

# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
BRAND = "AG Jeans"
COLLECTION_URL = [
    "https://www.agjeans.com/collections/womens-denim",
    "https://www.agjeans.com/collections/womens-sale",
]
MYSHOPIFY = "agjeans-store.myshopify.com"
GRAPHQL = "https://www.agjeans.com/api/unstable/graphql.json"
X_SHOPIFY_STOREFRONT_ACCESS_TOKEN = "ffae8e47a84566aa6fa059dfc56c7c56"

CATEGORY_FILTER = ["Jeans"]
PRODUCT_TYPE_FILTER = ["WOMENS BOTTOMS"]

# Constructor inputs
CONSTRUCTOR_API_KEY = "key_Ai9lmSZcQbh1bfYa"
CONSTRUCTOR_CLIENT_ID = "124bb124-e8d8-444b-9186-eaae4100af9f"
CONSTRUCTOR_SESSION = "20"
CONSTRUCTOR_BROWSE_ENDPOINT = "https://ac.cnstrc.com/browse/group_id"
CONSTRUCTOR_RESULTS_PER_PAGE = 26
CONSTRUCTOR_HIDDEN_FIELDS = ["prices.price_US", "compareAtPrices.compareprice_US"]
CONSTRUCTOR_EXTRA_PARAMS: Dict[str, str] = {}

# ---------------------------------------------------------------------------
# GraphQL output ordering / skip rules
# ---------------------------------------------------------------------------
COLUMN_ORDER_BASE: Tuple[str, ...] = (
    "product.id",
    "product.handle",
    "product.published_at",
    "product.created_at",
    "product.title",
    "product.productType",
    "product.category.name",
    "product.tags_all",
    "product.vendor",
    "product.description",
    "product.descriptionHtml",
    "variant.title",
    "variant.option1",
    "variant.option2",
    "variant.option3",
    "variant.price",
    "variant.compare_at_price",
    "product.priceRange",
    "variant.available",
    "variant.quantityAvailable",
    "product.totalInventory",
    "variant.id",
    "variant.sku",
    "variant.barcode",
    "product.featuredImage",
    "product.onlineStoreUrl",
)

DEFAULT_FORBIDDEN_FIELDS: Dict[str, Set[str]] = {
    "ProductVariant": {
        "components",
        "groupedBy",
        "quantityPriceBreaks",
        "sellingPlanAllocations",
        "sellingPlanGroups",
        "storeAvailability",
    }
}


CONSTRUCTOR_COLUMN_ORDER: Tuple[str, ...] = (
    "product.id",
    "product.handle",
    "product.title",
    "product.title.v2",
    "product.title.v3",
    "product.title.v4",
    "product.productType",
    "product.sort",
    "product.labels",
    "product.matchedTerms",
    "product.tags_all",
    "product.description",
    "product.color",
    "product.rise",
    "product.closure",
    "product.onlineStoreUrl",
    "variant.price",
    "variant.compare_at_price",
    "product.notifyBIS",
    "variant.quantityAvailable.Instore",
    "variant.id",
    "variant.sku",
    "product.material",
    "product.fabric",
    "product.mill",
    "product.country",
    "collection.handles",
    "collection.url",
    "collection.handle",
    "product.highlight",
    "product.capsule",
    "product.image",
    "product.images.v2",
    "varient.raw",
)

EXTRA_FORBIDDEN_COLUMNS: Set[str] = {
    "product.collections.pageInfo.endCursor",
    "product.collections.pageInfo.hasNextPage",
    "product.encodedVariantAvailability",
    "product.encodedVariantExistence",
    "product.featuredImage.height",
    "product.featuredImage.thumbhash",
    "product.featuredImage.width",
    "product.images.pageInfo.endCursor",
    "product.images.pageInfo.hasNextPage",
    "product.isGiftCard",
    "product.media.pageInfo.endCursor",
    "product.media.pageInfo.hasNextPage",
    "products_edge_cursor",
    "variant.currentlyNotInStock",
    "variant.image.height",
    "variant.image.id",
    "variant.image.thumbhash",
    "variant.image.width",
    "variant.quantityRule.minimum",
    "variant_edge_cursor",
    "variants_endCursor",
    "variants_hasNextPage",
}

# ---------------------------------------------------------------------------
# Paths / logging
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND_SLUG = re.sub(r"[^a-z0-9]+", "_", BRAND.lower()).strip("_") or "brand"
LOG_PATH = OUTPUT_DIR / f"{BRAND_SLUG}_constructor_graphql.log"

REQUEST_TIMEOUT = 30


def configure_logger() -> logging.Logger:
    logger = logging.getLogger("constructor_graphql")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/127.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def collection_handle_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2 and parts[0] == "collections":
        return parts[1]
    return parts[-1] if parts else ""


def list_to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        cleaned = []
        for item in value:
            if isinstance(item, (dict, list)):
                cleaned.append(json.dumps(item, ensure_ascii=False))
            else:
                cleaned.append(str(item))
        return ", ".join(cleaned)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def maybe_number(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        m = re.search(r"-?\d+(?:\.\d+)?", value)
        return m.group(0) if m else ""
    if isinstance(value, dict):
        for key in ("price_US", "compareprice_US", "amount"):
            if key in value:
                return maybe_number(value.get(key))
        for v in value.values():
            got = maybe_number(v)
            if got:
                return got
        return ""
    if isinstance(value, list):
        for item in value:
            got = maybe_number(item)
            if got:
                return got
        return ""
    return ""


def sum_inventory(value: Any) -> str:
    if value is None:
        return ""
    total = 0

    def walk(node: Any) -> None:
        nonlocal total
        if isinstance(node, dict):
            for k, v in node.items():
                if k.lower() == "available" and isinstance(v, (int, float)):
                    total += int(v)
                else:
                    walk(v)
        elif isinstance(node, list):
            for i in node:
                walk(i)

    walk(value)
    return str(total)


# ---------------------------------------------------------------------------
# Constructor collection -> merged variant rows
# ---------------------------------------------------------------------------
CONSTRUCTOR_REMOVE_HEADERS = {
    "constructor.data.prices",
    "constructor.data.inventory",
    "constructor.data.compareAtPrices",
    "constructor.data.productmedia_v1",
    "constructor.is_slotted",
    "constructor.parent_sku",
    "constructor.parent_value",
    "constructor.variation_index",
    "row_type",
    "source",
}


def fetch_constructor_results_for_collection(
    session: requests.Session,
    collection_url: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    handle = collection_handle_from_url(collection_url)
    if not handle:
        logger.warning("Could not derive collection handle from URL: %s", collection_url)
        return []

    base_params: List[Tuple[str, str]] = [
        ("c", "cio-ui-plp-1.6.2"),
        ("key", CONSTRUCTOR_API_KEY),
        ("i", CONSTRUCTOR_CLIENT_ID),
        ("s", CONSTRUCTOR_SESSION),
        ("num_results_per_page", str(CONSTRUCTOR_RESULTS_PER_PAGE)),
    ]
    for hf in CONSTRUCTOR_HIDDEN_FIELDS:
        base_params.append(("fmt_options[hidden_fields]", hf))
    for k, v in CONSTRUCTOR_EXTRA_PARAMS.items():
        base_params.append((k, str(v)))

    page = 1
    total_pages: Optional[int] = None
    all_results: List[Dict[str, Any]] = []

    while True:
        url = f"{CONSTRUCTOR_BROWSE_ENDPOINT}/{handle}"
        params = base_params + [("page", str(page))]
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        response = payload.get("response") or {}
        results = response.get("results") or []

        if total_pages is None:
            total = int(response.get("total_num_results") or 0)
            per_page = int(response.get("num_results_per_page") or CONSTRUCTOR_RESULTS_PER_PAGE)
            total_pages = max((total + per_page - 1) // per_page, 1)
            logger.info("Constructor %s: total=%s pages=%s", handle, total, total_pages)

        if not results:
            break
        all_results.extend(results)

        if total_pages is not None and page >= total_pages:
            break
        page += 1

    return all_results


def merge_constructor_rows(results: List[Dict[str, Any]], collection_url: str) -> List[Dict[str, str]]:
    handle = collection_handle_from_url(collection_url)
    rows: List[Dict[str, str]] = []

    for result in results:
        data = result.get("data") if isinstance(result.get("data"), dict) else {}
        variations = result.get("variations") or []
        if not variations:
            variations = [{"value": None, "data": {}}]

        for idx, variation in enumerate(variations):
            variation = variation if isinstance(variation, dict) else {}
            v_data = variation.get("data") if isinstance(variation.get("data"), dict) else {}
            row: Dict[str, str] = {
                "collection.url": collection_url,
                "collection.handle": handle,
                "product.id": list_to_text(data.get("id")),
                "product.handle": list_to_text(data.get("handle")),
                "product.title": list_to_text(result.get("value")),
                "product.title.v2": list_to_text(data.get("subtitle")),
                "product.title.v3": list_to_text(data.get("product")),
                "product.title.v4": list_to_text(variation.get("value")),
                "product.productType": list_to_text(data.get("product_type")),
                "product.sort": list_to_text(data.get("featured")),
                "product.labels": list_to_text(result.get("labels")),
                "product.matchedTerms": list_to_text(result.get("matched_terms")),
                "product.tags_all": list_to_text(data.get("keywords")),
                "product.description": list_to_text(data.get("description")),
                "product.color": list_to_text(data.get("color_code")),
                "product.rise": list_to_text(data.get("rise")),
                "product.closure": list_to_text(data.get("closure")),
                "product.onlineStoreUrl": list_to_text(data.get("url")),
                "variant.price": maybe_number(v_data.get("prices")),
                "variant.compare_at_price": maybe_number(v_data.get("compareAtPrices")),
                "product.notifyBIS": list_to_text(data.get("show_klaviyo_bis")),
                "variant.quantityAvailable.Instore": sum_inventory(v_data.get("inventory")),
                "variant.id": list_to_text(v_data.get("variation_id") or data.get("variation_id")),
                "variant.sku": list_to_text(v_data.get("sku") or data.get("sku")),
                "product.material": list_to_text(data.get("material")),
                "product.fabric": list_to_text(data.get("fabric")),
                "product.mill": list_to_text(data.get("mill")),
                "product.country": list_to_text(data.get("coo")),
                "collection.handles": list_to_text(data.get("group_ids")),
                "product.highlight": list_to_text(data.get("highlight")),
                "product.capsule": list_to_text(data.get("capsule")),
                "product.image": list_to_text(data.get("image_url")),
                "product.images.v2": "" if data.get("image_url") else list_to_text(data.get("mediaImages")),
                "varient.raw": list_to_text(result.get("variations")),
            }

            # carry unmapped fields as extras with [level].[label] naming
            mapped_levels = {
                "collection": {"url", "handle", "handles"},
                "product": {
                    "id",
                    "handle",
                    "title",
                    "title.v2",
                    "title.v3",
                    "title.v4",
                    "productType",
                    "sort",
                    "labels",
                    "matchedTerms",
                    "tags_all",
                    "description",
                    "color",
                    "rise",
                    "closure",
                    "onlineStoreUrl",
                    "notifyBIS",
                    "material",
                    "fabric",
                    "mill",
                    "country",
                    "highlight",
                    "capsule",
                    "image",
                    "images.v2",
                },
                "variant": {"price", "compare_at_price", "quantityAvailable.Instore", "id", "sku"},
            }

            for key, value in data.items():
                source_header = f"constructor.data.{key}"
                if source_header in CONSTRUCTOR_REMOVE_HEADERS:
                    continue
                label = key
                new_col = f"product.{label}"
                if label not in mapped_levels["product"] and new_col not in row:
                    row[new_col] = list_to_text(value)

            for key, value in v_data.items():
                label = key
                new_col = f"variant.{label}"
                if label not in mapped_levels["variant"] and new_col not in row:
                    row[new_col] = list_to_text(value)

            rows.append(row)

    # Post-pass column rules
    def all_same(col: str, baseline_col: str) -> bool:
        non_blank = [r.get(col, "") for r in rows if r.get(col, "") != ""]
        baseline = [r.get(baseline_col, "") for r in rows if r.get(col, "") != ""]
        if not non_blank:
            return True
        return all(a == b for a, b in zip(non_blank, baseline))

    drop_cols: Set[str] = set()
    if all_same("product.title.v3", "product.title"):
        drop_cols.add("product.title.v3")
    if all_same("product.title.v4", "product.title"):
        drop_cols.add("product.title.v4")

    for c in ("product.labels", "product.matchedTerms"):
        if all((r.get(c, "") == "" or r.get(c, "") == "{}" or r.get(c, "") == "[]") for r in rows):
            drop_cols.add(c)

    if drop_cols:
        for r in rows:
            for c in drop_cols:
                r.pop(c, None)

    return rows


def fetch_constructor_rows(session: requests.Session, logger: logging.Logger) -> List[Dict[str, str]]:
    all_rows: List[Dict[str, str]] = []
    for collection_url in COLLECTION_URL:
        try:
            results = fetch_constructor_results_for_collection(session, collection_url, logger)
            all_rows.extend(merge_constructor_rows(results, collection_url))
        except requests.RequestException as exc:
            logger.warning("Constructor fetch failed for %s -> %s", collection_url, exc)
    logger.info("Constructor rows collected: %s", len(all_rows))
    return all_rows


# ---------------------------------------------------------------------------
# GraphQL (with introspection pass first)
# ---------------------------------------------------------------------------
INTROSPECTION_TYPE_FIELDS_QUERY = """
query($t: String!) {
  __type(name: $t) {
    name
    fields {
      name
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
          }
        }
      }
      args {
        name
        type {
          kind
          name
          ofType { kind name }
        }
      }
    }
  }
}
"""

COLLECTION_PRODUCTS_QUERY = """
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
        node {
          id
          handle
          title
          publishedAt
          createdAt
          productType
          tags
          vendor
          onlineStoreUrl
          description
          descriptionHtml
          totalInventory
          category {
            name
          }
          featuredImage {
            url
            altText
          }
          priceRange {
            minVariantPrice { amount currencyCode }
            maxVariantPrice { amount currencyCode }
          }
          options {
            name
            values
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
                quantityAvailable
                price {
                  amount
                  currencyCode
                }
                compareAtPrice {
                  amount
                  currencyCode
                }
                selectedOptions {
                  name
                  value
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

FILTER_PROBE_QUERIES = [
    """
query FilterProbeA($handle: String!) {
  collection(handle: $handle) {
    products(first: 1) {
      filters {
        id
        label
        type
        values {
          id
          label
          count
        }
      }
    }
  }
}
""",
    """
query FilterProbeB($handle: String!) {
  collection(handle: $handle) {
    products(first: 1) {
      productFilters {
        id
        label
        type
        values {
          id
          label
          count
        }
      }
    }
  }
}
""",
]


def perform_graphql_request(
    session: requests.Session,
    endpoint: str,
    query: str,
    variables: Dict[str, Any],
) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": X_SHOPIFY_STOREFRONT_ACCESS_TOKEN,
    }
    resp = session.post(
        endpoint,
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("errors"):
        raise requests.RequestException(f"GraphQL errors: {payload['errors']}")
    return payload


def graphql_introspection_probe(session: requests.Session, endpoint: str, logger: logging.Logger) -> None:
    for type_name in ("Product", "ProductVariant"):
        try:
            data = perform_graphql_request(
                session,
                endpoint,
                INTROSPECTION_TYPE_FIELDS_QUERY,
                {"t": type_name},
            )
        except requests.RequestException as exc:
            logger.warning("Introspection failed for %s -> %s", type_name, exc)
            continue
        fields = (((data.get("data") or {}).get("__type") or {}).get("fields") or [])
        logger.info("%s field count: %s", type_name, len(fields))
        for f in fields:
            type_obj = f.get("type") or {}
            type_name_guess = type_obj.get("name") or ((type_obj.get("ofType") or {}).get("name")) or type_obj.get("kind")
            logger.info("%s.%s -> %s", type_name, f.get("name"), type_name_guess)


def discover_collection_filters(
    session: requests.Session,
    endpoint: str,
    handle: str,
    logger: logging.Logger,
) -> Dict[str, List[str]]:
    for query in FILTER_PROBE_QUERIES:
        try:
            data = perform_graphql_request(session, endpoint, query, {"handle": handle})
        except requests.RequestException:
            continue
        collection = ((data.get("data") or {}).get("collection") or {})
        products = collection.get("products") or {}
        filters_block = products.get("filters") or products.get("productFilters")
        if not filters_block:
            continue
        parsed: Dict[str, List[str]] = {}
        for fil in filters_block:
            if not isinstance(fil, dict):
                continue
            label = str(fil.get("label") or fil.get("id") or "").strip()
            if not label:
                continue
            values = []
            for item in fil.get("values") or []:
                if isinstance(item, dict):
                    candidate = item.get("label") or item.get("id")
                    if candidate:
                        values.append(str(candidate))
            if values:
                parsed[label] = values
        if parsed:
            logger.info("GraphQL filter groups for %s: %s", handle, len(parsed))
            return parsed
    return {}


def product_matches_filters(product: Dict[str, Any]) -> bool:
    if PRODUCT_TYPE_FILTER:
        ptype = str(product.get("productType") or "").strip().lower()
        allowed = {x.strip().lower() for x in PRODUCT_TYPE_FILTER if x.strip()}
        if ptype not in allowed:
            return False

    if CATEGORY_FILTER:
        tags = [str(t) for t in (product.get("tags") or [])]
        corpus = " ".join(
            [
                str(product.get("title") or ""),
                str(product.get("productType") or ""),
                " ".join(tags),
            ]
        ).lower()
        if not any(cat.strip().lower() in corpus for cat in CATEGORY_FILTER if cat.strip()):
            return False

    return True


def normalize_filter_name(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(name).lower()).strip("_") or "unnamed"


def flatten_graphql_row(
    collection_url: str,
    collection_handle: str,
    collection_title: str,
    filters: Dict[str, List[str]],
    product: Dict[str, Any],
    variant: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    row: Dict[str, str] = {
        "collection.url": collection_url,
        "collection.handle": collection_handle,
        "collection.title": collection_title,
        "product.id": list_to_text(product.get("id")),
        "product.handle": list_to_text(product.get("handle")),
        "product.published_at": list_to_text(product.get("publishedAt")),
        "product.created_at": list_to_text(product.get("createdAt")),
        "product.title": list_to_text(product.get("title")),
        "product.productType": list_to_text(product.get("productType")),
        "product.category.name": list_to_text((product.get("category") or {}).get("name")),
        "product.tags_all": list_to_text(product.get("tags")),
        "product.vendor": list_to_text(product.get("vendor")),
        "product.description": list_to_text(product.get("description")),
        "product.descriptionHtml": list_to_text(product.get("descriptionHtml")),
        "product.priceRange": list_to_text(product.get("priceRange")),
        "product.totalInventory": list_to_text(product.get("totalInventory")),
        "product.featuredImage": list_to_text(product.get("featuredImage")),
        "product.onlineStoreUrl": list_to_text(product.get("onlineStoreUrl")),
    }

    corpus = " ".join(
        [
            row.get("product.title", ""),
            row.get("product.productType", ""),
            row.get("product.tags_all", ""),
        ]
    ).lower()
    for name, vals in filters.items():
        matches = [v for v in vals if str(v).lower() in corpus]
        if matches:
            row[f"filter.{normalize_filter_name(name)}"] = ", ".join(sorted(set(matches)))

    if variant:
        row["variant.id"] = list_to_text(variant.get("id"))
        row["variant.title"] = list_to_text(variant.get("title"))
        row["variant.sku"] = list_to_text(variant.get("sku"))
        row["variant.barcode"] = list_to_text(variant.get("barcode"))
        row["variant.available"] = list_to_text(variant.get("availableForSale"))
        row["variant.quantityAvailable"] = list_to_text(variant.get("quantityAvailable"))
        row["variant.price"] = maybe_number(variant.get("price"))
        row["variant.compare_at_price"] = maybe_number(variant.get("compareAtPrice"))
        selected = variant.get("selectedOptions") or []
        for opt in selected:
            if not isinstance(opt, dict):
                continue
            oname = str(opt.get("name") or "").strip().lower()
            oval = list_to_text(opt.get("value"))
            if oname in {"size", "option1"}:
                row["variant.option1"] = oval
            elif oname in {"color", "option2"}:
                row["variant.option2"] = oval
            else:
                if "variant.option3" not in row:
                    row["variant.option3"] = oval

    return row


def build_column_order(rows: List[Dict[str, str]]) -> List[str]:
    all_cols = {k for r in rows for k in r.keys()}
    base = [c for c in COLUMN_ORDER_BASE if c in all_cols]
    extras = []
    for col in sorted(all_cols):
        if col in base:
            continue
        if col in EXTRA_FORBIDDEN_COLUMNS:
            continue
        extras.append(col)
    return base + extras


def build_constructor_column_order(rows: List[Dict[str, str]]) -> List[str]:
    all_cols = {k for r in rows for k in r.keys()}
    base = [c for c in CONSTRUCTOR_COLUMN_ORDER if c in all_cols]
    extras = [c for c in sorted(all_cols) if c not in base]
    return base + extras


def fetch_graphql_rows(session: requests.Session, logger: logging.Logger) -> List[Dict[str, str]]:
    endpoint = GRAPHQL
    graphql_introspection_probe(session, endpoint, logger)

    rows: List[Dict[str, str]] = []

    for collection_url in COLLECTION_URL:
        handle = collection_handle_from_url(collection_url)
        if not handle:
            continue
        filters = discover_collection_filters(session, endpoint, handle, logger)
        cursor: Optional[str] = None

        while True:
            payload = perform_graphql_request(
                session,
                endpoint,
                COLLECTION_PRODUCTS_QUERY,
                {"handle": handle, "cursor": cursor, "pageSize": 100},
            )
            collection = ((payload.get("data") or {}).get("collection") or {})
            products_conn = collection.get("products") or {}
            edges = products_conn.get("edges") or []

            for edge in edges:
                product = (edge or {}).get("node") or {}
                if not product_matches_filters(product):
                    continue

                variants = ((product.get("variants") or {}).get("edges") or [])
                if not variants:
                    rows.append(
                        flatten_graphql_row(
                            collection_url,
                            handle,
                            list_to_text(collection.get("title")),
                            filters,
                            product,
                            None,
                        )
                    )
                else:
                    for v_edge in variants:
                        variant = (v_edge or {}).get("node") or {}
                        rows.append(
                            flatten_graphql_row(
                                collection_url,
                                handle,
                                list_to_text(collection.get("title")),
                                filters,
                                product,
                                variant,
                            )
                        )

            page_info = products_conn.get("pageInfo") or {}
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break

    logger.info("GraphQL rows collected: %s", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Excel
# ---------------------------------------------------------------------------
def write_sheet(ws, rows: List[Dict[str, str]], ordered_columns: Optional[List[str]] = None) -> None:
    if not rows:
        ws.append(["note"])
        ws.append(["No rows collected"])
        return

    columns = ordered_columns or sorted({k for row in rows for k in row.keys()})
    ws.append(columns)
    for row in rows:
        ws.append([row.get(col, "") for col in columns])


def write_workbook(
    constructor_rows: List[Dict[str, str]],
    graphql_rows: List[Dict[str, str]],
    logger: logging.Logger,
) -> Path:
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output = OUTPUT_DIR / f"{BRAND_SLUG}_Constructor_Graphql_{ts}.xlsx"

    wb = Workbook()
    wb.remove(wb.active)

    ws_constructor = wb.create_sheet("Constructor")
    write_sheet(
        ws_constructor,
        constructor_rows,
        ordered_columns=build_constructor_column_order(constructor_rows),
    )

    ws_graphql = wb.create_sheet("GraphQL")
    write_sheet(ws_graphql, graphql_rows, ordered_columns=build_column_order(graphql_rows))

    wb.save(output)
    logger.info("Workbook written: %s", output.resolve())
    return output


def main() -> None:
    logger = configure_logger()
    session = build_session()

    constructor_rows = fetch_constructor_rows(session, logger)
    graphql_rows = fetch_graphql_rows(session, logger)

    output = write_workbook(constructor_rows, graphql_rows, logger)

    if not constructor_rows:
        logger.warning("Constructor tab is blank.")
    if not graphql_rows:
        logger.warning("GraphQL tab is blank.")
    logger.info("Done. Output: %s", output)


if __name__ == "__main__":
    main()
