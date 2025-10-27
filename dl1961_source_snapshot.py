"""Utility script to snapshot DL1961 product data from multiple sources.

This helper pulls three feeds (Searchspring, Shopify Storefront GraphQL, and
Shopify collection JSON) and writes them to an Excel workbook with one sheet per
source so the mapping work can happen side-by-side.

Run via: ``python dl1961_source_snapshot.py``.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_PATH = BASE_DIR / "Output"
OUTPUT_PATH.mkdir(exist_ok=True)

LOG_PATH = BASE_DIR / "dl1961_snapshot.log"
LOG_FALLBACK_PATH = OUTPUT_PATH / "dl1961_snapshot.log"


def configure_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    try:
        handlers.append(logging.FileHandler(LOG_PATH))
    except OSError:
        handlers.append(logging.FileHandler(LOG_FALLBACK_PATH))
        handlers[1].setLevel(logging.INFO)
        logging.getLogger(__name__).warning(
            "Falling back to %s for logging", LOG_FALLBACK_PATH
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


configure_logging()


@dataclass
class FetchContext:
    session: requests.Session

    def get_json(self, url: str, *, params: dict[str, Any] | None = None) -> Any:
        logging.debug("GET %s", url)
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def post_json(self, url: str, *, json_body: dict[str, Any]) -> Any:
        logging.debug("POST %s", url)
        resp = self.session.post(url, json=json_body, timeout=30)
        resp.raise_for_status()
        return resp.json()


SEARCHSPRING_SITE_ID = "8176gy"
SEARCHSPRING_URL = (
    "https://8176gy.a.searchspring.io/api/search/autocomplete.json"
)

STOREFRONT_URL = "https://dl1961trial.myshopify.com/api/2023-04/graphql.json"
STOREFRONT_TOKEN = "d66ac22abacd5c3978abe95b55eaa3df"

SHOPIFY_COLLECTION_URL = (
    "https://dl1961.com/collections/women-view-all-fits/products.json"
)


def fetch_searchspring(ctx: FetchContext) -> list[dict[str, Any]]:
    logging.info("Fetching Searchspring autocomplete results")
    page = 1
    hits: list[dict[str, Any]] = []
    while True:
        params = {
            "siteId": SEARCHSPRING_SITE_ID,
            "resultsFormat": "json",
            "resultsPerPage": 250,
            "q": "women jean",
            "page": page,
        }
        payload = ctx.get_json(SEARCHSPRING_URL, params=params)
        raw_results = payload.get("results", [])
        if isinstance(raw_results, dict):
            result_hits = raw_results.get("results", [])
        elif isinstance(raw_results, list):
            result_hits = raw_results
        else:
            result_hits = []
        logging.info("Searchspring page %s -> %s hits", page, len(result_hits))
        if not result_hits:
            break
        hits.extend(result_hits)
        page += 1
    return hits


def fetch_storefront(ctx: FetchContext) -> list[dict[str, Any]]:
    logging.info("Fetching Shopify Storefront products")
    headers = {
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    query = """
    query PaginatedProducts($cursor: String) {
      products(
        first: 200
        after: $cursor
        query: "collection_handle:women-view-all-fits tag:'women'"
      ) {
        edges {
          cursor
          node {
            id
            handle
            title
            description
            tags
            vendor
            productType
            createdAt
            updatedAt
            publishedAt
            onlineStoreUrl
            totalInventory
            collections(first: 10) { edges { node { id handle title } } }
            options { id name values }
            variants(first: 250) {
              edges {
                node {
                  id
                  title
                  sku
                  price { amount currencyCode }
                  compareAtPrice { amount currencyCode }
                  selectedOptions { name value }
                  availableForSale
                  quantityAvailable
                  barcode
                }
              }
            }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
    """

    products: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        data = ctx.session.post(
            STOREFRONT_URL,
            headers=headers,
            json={"query": query, "variables": {"cursor": cursor}},
            timeout=30,
        )
        data.raise_for_status()
        payload = data.json()
        if payload.get("errors"):
            raise RuntimeError(json.dumps(payload["errors"], indent=2))
        connection = payload["data"].get("products")
        if not connection:
            logging.info("Storefront response missing products connection")
            break
        for edge in connection["edges"]:
            products.append(edge["node"])
        logging.info("Storefront page fetched -> total %s products", len(products))
        if not connection["pageInfo"]["hasNextPage"]:
            break
        cursor = connection["pageInfo"]["endCursor"]
    return products


def fetch_collection_json(ctx: FetchContext) -> list[dict[str, Any]]:
    logging.info("Fetching Shopify collection JSON")
    page = 1
    products: list[dict[str, Any]] = []
    while True:
        payload = ctx.get_json(SHOPIFY_COLLECTION_URL, params={"limit": 250, "page": page})
        page_products = payload.get("products", [])
        logging.info("Collection page %s -> %s products", page, len(page_products))
        if not page_products:
            break
        products.extend(page_products)
        page += 1
    return products


def flatten_records(records: Iterable[dict[str, Any]]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    return pd.json_normalize(records)


def flatten_storefront_products(products: Iterable[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for product in products:
        base: dict[str, Any] = {}

        for key, value in product.items():
            if key in {"collections", "variants"}:
                continue
            if key == "options":
                base[key] = json.dumps(value)
            else:
                base[key] = value

        collections = product.get("collections", {}).get("edges", [])
        for idx, edge in enumerate(collections):
            node = edge.get("node", {}) if isinstance(edge, dict) else {}
            for node_key, node_value in node.items():
                base[f"collections_{idx}_{node_key}"] = node_value

        variants = product.get("variants", {}).get("edges", [])
        for idx, edge in enumerate(variants):
            node = edge.get("node", {}) if isinstance(edge, dict) else {}
            for node_key, node_value in node.items():
                column_prefix = f"variants_{idx}_{node_key}"
                if isinstance(node_value, dict):
                    for sub_key, sub_value in node_value.items():
                        base[f"{column_prefix}_{sub_key}"] = sub_value
                elif isinstance(node_value, list):
                    base[column_prefix] = json.dumps(node_value)
                else:
                    base[column_prefix] = node_value

        rows.append(base)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def main() -> None:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/120.0.0.0 Safari/537.36"
    })
    ctx = FetchContext(session=session)

    searchspring_hits = fetch_searchspring(ctx)
    storefront_products = fetch_storefront(ctx)
    collection_products = fetch_collection_json(ctx)

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    output_file = OUTPUT_PATH / f"DL1961_snapshot_{timestamp}.xlsx"

    logging.info("Writing workbook to %s", output_file)
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        flatten_records(searchspring_hits).to_excel(writer, sheet_name="Searchspring", index=False)
        flatten_storefront_products(storefront_products).to_excel(
            writer, sheet_name="Storefront", index=False
        )
        flatten_records(collection_products).to_excel(writer, sheet_name="CollectionJSON", index=False)

    logging.info("Done")


if __name__ == "__main__":
    main()

