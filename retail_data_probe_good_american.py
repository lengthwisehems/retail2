"""
Good American (good-american.myshopify.com) product probe.

Data sources:
  1. Shopify Storefront API — product info, inventory, pricing for
     collections: womens-jeans, sale
  2. Appmate (api.appmate.io/v2) — per-session wishlist items
     NOTE: The public frontend token only exposes the current anonymous
     session's wishlist. Aggregate per-product wishlist/cart counts
     require merchant-level API credentials not available here.
  3. Swish / Wishlist King — saved-items analytics
     NOTE: The analytics endpoint requires merchant auth (returns 401
     with only the public storefront token). Counts are left blank.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ── Config ────────────────────────────────────────────────────────────────
MYSHOPIFY_DOMAIN = "good-american.myshopify.com"
STOREFRONT_GRAPHQL = f"https://{MYSHOPIFY_DOMAIN}/api/unstable/graphql.json"
STOREFRONT_TOKEN = "45b215a31a1aa259d1e128badf92328a"

COLLECTIONS = ["womens-jeans", "sale"]

APPMATE_API_BASE = "https://api.appmate.io/v2"
APPMATE_TOKEN = "fcae45665f84046bedf794dab56e4e2ddb7243151f05dcbf6cb585dca2a0737b"

SWISH_API_BASE = "https://swish.app/api/2025-04"

OUTPUT_FILE = "good_american_probe.xlsx"
PAGE_SIZE = 250
RATE_LIMIT_DELAY = 0.3

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Storefront query ───────────────────────────────────────────────────────
PRODUCTS_QUERY = """
query CollectionProducts($handle: String!, $first: Int!, $after: String) {
  collection(handle: $handle) {
    id
    title
    products(first: $first, after: $after) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          id
          title
          handle
          vendor
          productType
          tags
          totalInventory
          availableForSale
          description
          compareAtPriceRange {
            minVariantPrice { amount currencyCode }
            maxVariantPrice { amount currencyCode }
          }
          priceRange {
            minVariantPrice { amount currencyCode }
            maxVariantPrice { amount currencyCode }
          }
          variants(first: 100) {
            edges {
              node {
                id
                title
                sku
                availableForSale
                quantityAvailable
                price { amount currencyCode }
                compareAtPrice { amount currencyCode }
                selectedOptions { name value }
              }
            }
          }
        }
      }
    }
  }
}
"""


def _gid_to_id(gid: str) -> str:
    """'gid://shopify/Product/1234' → '1234'"""
    return gid.rsplit("/", 1)[-1]


def fetch_collection_products(session: requests.Session, handle: str) -> List[Dict[str, Any]]:
    """Paginate through all products in a Storefront collection."""
    products: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 0
    while True:
        page += 1
        variables: Dict[str, Any] = {"handle": handle, "first": PAGE_SIZE}
        if cursor:
            variables["after"] = cursor
        try:
            resp = session.post(
                STOREFRONT_GRAPHQL,
                json={"query": PRODUCTS_QUERY, "variables": variables},
                timeout=30,
            )
            resp.raise_for_status()
            body = resp.json()
        except Exception as exc:
            logger.error("Storefront request failed (page %d, %s): %s", page, handle, exc)
            break

        errors = body.get("errors")
        if errors:
            logger.warning("GraphQL errors for %s page %d: %s", handle, page, errors)

        collection = (body.get("data") or {}).get("collection")
        if not collection:
            logger.warning("No collection data for handle=%s", handle)
            break

        edges = collection["products"]["edges"]
        for edge in edges:
            node = edge["node"]
            variants = [v["node"] for v in node.pop("variants", {}).get("edges", [])]
            node["variants"] = variants
            node["collection_handle"] = handle
            node["collection_title"] = collection["title"]
            products.append(node)

        page_info = collection["products"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
        logger.info("  %s: fetched %d products so far (page %d)…", handle, len(products), page)
        time.sleep(RATE_LIMIT_DELAY)

    logger.info("Collection '%s': %d products total", handle, len(products))
    return products


def fetch_appmate_session_wishlist(session: requests.Session) -> Dict[str, Any]:
    """
    Fetch the current anonymous session's Appmate wishlist.
    Returns the wishlist payload (numItems, items[]).
    Aggregate per-product counts are not exposed by the public token.
    """
    try:
        resp = session.get(
            f"{APPMATE_API_BASE}/wishlists/mine",
            headers={"x-appmate-shp": MYSHOPIFY_DOMAIN},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("wishlist", {})
    except Exception as exc:
        logger.warning("Appmate wishlist fetch failed: %s", exc)
        return {}


def fetch_swish_saved_count(
    session: requests.Session, product_gid: str
) -> Optional[int]:
    """
    Attempt to fetch Wishlist King saved-items count for a product.
    Returns None if the endpoint is unavailable (requires merchant auth).
    """
    try:
        resp = session.get(
            f"{SWISH_API_BASE}/analytics/saved-items",
            params={"merchandiseId": product_gid},
            timeout=10,
        )
        if resp.status_code == 401:
            return None
        resp.raise_for_status()
        data = resp.json()
        return data.get("count") or data.get("savedCount") or data.get("total")
    except Exception:
        return None


# ── Row builder ────────────────────────────────────────────────────────────
def build_rows(
    products: List[Dict[str, Any]],
    appmate_wishlist: Dict[str, Any],
    swish_available: bool,
) -> List[Dict[str, Any]]:
    """Flatten product+variant data into one row per variant."""
    # Index Appmate session wishlist items by product numeric ID
    appmate_items: Dict[str, Any] = {}
    for item in appmate_wishlist.get("items", []):
        pid = str(item.get("productId", ""))
        appmate_items[pid] = item

    rows = []
    for product in products:
        p_gid = product["id"]
        p_numeric_id = _gid_to_id(p_gid)

        price_min = product.get("priceRange", {}).get("minVariantPrice", {}).get("amount")
        price_max = product.get("priceRange", {}).get("maxVariantPrice", {}).get("amount")
        compare_min = product.get("compareAtPriceRange", {}).get("minVariantPrice", {}).get("amount")
        compare_max = product.get("compareAtPriceRange", {}).get("maxVariantPrice", {}).get("amount")

        appmate_in_session = p_numeric_id in appmate_items

        variants = product.get("variants") or []
        if not variants:
            rows.append({
                "collection": product.get("collection_handle"),
                "collection_title": product.get("collection_title"),
                "product_id": p_numeric_id,
                "product_title": product.get("title"),
                "handle": product.get("handle"),
                "vendor": product.get("vendor"),
                "product_type": product.get("productType"),
                "tags": ", ".join(product.get("tags") or []),
                "product_available": product.get("availableForSale"),
                "total_inventory": product.get("totalInventory"),
                "price_min": price_min,
                "price_max": price_max,
                "compare_at_min": compare_min,
                "compare_at_max": compare_max,
                "variant_id": None,
                "variant_title": None,
                "sku": None,
                "variant_available": None,
                "qty_available": None,
                "variant_price": None,
                "variant_compare_at": None,
                "options": None,
                "appmate_in_session_wishlist": appmate_in_session,
                "swish_wishlist_count": "N/A (merchant auth required)",
            })
        else:
            for v in variants:
                v_numeric_id = _gid_to_id(v["id"])
                options_str = "; ".join(
                    f"{o['name']}: {o['value']}" for o in (v.get("selectedOptions") or [])
                )
                rows.append({
                    "collection": product.get("collection_handle"),
                    "collection_title": product.get("collection_title"),
                    "product_id": p_numeric_id,
                    "product_title": product.get("title"),
                    "handle": product.get("handle"),
                    "vendor": product.get("vendor"),
                    "product_type": product.get("productType"),
                    "tags": ", ".join(product.get("tags") or []),
                    "product_available": product.get("availableForSale"),
                    "total_inventory": product.get("totalInventory"),
                    "price_min": price_min,
                    "price_max": price_max,
                    "compare_at_min": compare_min,
                    "compare_at_max": compare_max,
                    "variant_id": v_numeric_id,
                    "variant_title": v.get("title"),
                    "sku": v.get("sku"),
                    "variant_available": v.get("availableForSale"),
                    "qty_available": v.get("quantityAvailable"),
                    "variant_price": v.get("price", {}).get("amount"),
                    "variant_compare_at": (v.get("compareAtPrice") or {}).get("amount"),
                    "options": options_str,
                    "appmate_in_session_wishlist": appmate_in_session,
                    "swish_wishlist_count": "N/A (merchant auth required)",
                })
    return rows


# ── Excel writer ───────────────────────────────────────────────────────────
COLUMNS = [
    ("collection", "Collection Handle"),
    ("collection_title", "Collection Title"),
    ("product_id", "Product ID"),
    ("product_title", "Product Title"),
    ("handle", "Handle"),
    ("vendor", "Vendor"),
    ("product_type", "Product Type"),
    ("tags", "Tags"),
    ("product_available", "Product Available"),
    ("total_inventory", "Total Inventory"),
    ("price_min", "Price Min"),
    ("price_max", "Price Max"),
    ("compare_at_min", "Compare At Min"),
    ("compare_at_max", "Compare At Max"),
    ("variant_id", "Variant ID"),
    ("variant_title", "Variant Title"),
    ("sku", "SKU"),
    ("variant_available", "Variant Available"),
    ("qty_available", "Qty Available"),
    ("variant_price", "Variant Price"),
    ("variant_compare_at", "Variant Compare At"),
    ("options", "Options"),
    ("appmate_in_session_wishlist", "Appmate: In Session Wishlist"),
    ("swish_wishlist_count", "Swish: Wishlist Count"),
]


def write_excel(rows: List[Dict[str, Any]], output_path: str) -> None:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Products"

    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(color="FFFFFF", bold=True)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for col_idx, (_, header_label) in enumerate(COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=header_label)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = header_align

    for row_idx, row_data in enumerate(rows, start=2):
        for col_idx, (key, _) in enumerate(COLUMNS, start=1):
            ws.cell(row=row_idx, column=col_idx, value=row_data.get(key))

    ws.freeze_panes = "A2"
    ws.row_dimensions[1].height = 30
    for col_idx, _ in enumerate(COLUMNS, start=1):
        col_letter = get_column_letter(col_idx)
        ws.column_dimensions[col_letter].width = 20

    wb.save(output_path)
    logger.info("Saved %d rows to %s", len(rows), output_path)


# ── Main ──────────────────────────────────────────────────────────────────
def main() -> None:
    session = requests.Session()
    session.headers.update({
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (retail-probe/1.0)",
        "Accept": "application/json",
    })

    # 1. Appmate session wishlist (one call for the anonymous session)
    logger.info("Fetching Appmate session wishlist…")
    appmate_wishlist = fetch_appmate_session_wishlist(session)
    logger.info(
        "Appmate session: numItems=%s (aggregate product-level counts require merchant auth)",
        appmate_wishlist.get("numItems", 0),
    )

    # 2. Swish probe (test once; if 401 skip per-product)
    logger.info("Testing Swish analytics endpoint…")
    test_product_gid = "gid://shopify/Product/7939573710931"
    swish_test = fetch_swish_saved_count(session, test_product_gid)
    swish_available = swish_test is not None
    if not swish_available:
        logger.info("Swish analytics endpoint requires merchant auth — counts will be N/A")

    # 3. Storefront products
    all_products: List[Dict[str, Any]] = []
    seen_ids: set = set()
    for handle in COLLECTIONS:
        logger.info("Fetching collection: %s", handle)
        prods = fetch_collection_products(session, handle)
        for p in prods:
            if p["id"] not in seen_ids:
                seen_ids.add(p["id"])
                all_products.append(p)

    logger.info("Total unique products across all collections: %d", len(all_products))

    # 4. Build rows and write Excel
    rows = build_rows(all_products, appmate_wishlist, swish_available)
    write_excel(rows, OUTPUT_FILE)
    logger.info("Done. Output: %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
