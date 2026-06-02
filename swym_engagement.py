"""
Swym engagement data fetcher for ramybrook.com.

No admin API key required. Pulls product-level counts from public Swym endpoints:
  - Back-in-stock (BIS) notification signups  (topic=backinstock)
  - Wishlist additions                          (topic=addToWishlist)

Counts are product-level (all variants combined). Per-variant BIS breakdown
requires the Swym admin API key (Swym Dashboard → Settings → API).

Usage:
  python swym_engagement.py                  # all jeans, export CSV
  python swym_engagement.py --empi 7097674367040  # single product lookup
  python swym_engagement.py --handle cindy-high-rise-wide-leg-jean

Notes:
  - "Added to cart" and "Recently Viewed" in Swym's relayfilters are UI tab IDs
    (3 and 1), not per-product analytics — no public API exists for these counts.
  - "Added to cart" events (et=3) do exist in the eventcount API but return 0
    for this store, likely because Swym's cart tracking is not enabled.
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configuration ──────────────────────────────────────────────────────────────

PID       = "JYxW0bO//HIl29BRB2i1vARfPY5YSr+7Xdr/iqq8FgE="
API_BASE  = "https://swymstore-v3premium-01.swymrelay.com"
STORE_URL = "https://www.ramybrook.com"
OUTPUT_CSV = "swym_engagement.csv"
REQUEST_TIMEOUT = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Swym event type constants (from SDK source)
ET_ADD_TO_CART = 3
ET_WISHLIST    = 4
ET_BIS         = 8


# ── Session setup ──────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, */*",
        "Origin": STORE_URL,
        "Referer": STORE_URL + "/",
    })
    return s


# ── Public Swym API (no admin key needed) ─────────────────────────────────────

def social_count(
    session: requests.Session,
    empi: int,
    du: str,
    topic: str,
) -> int:
    """
    Return the social count for a product via the public social-count endpoint.
    topic: 'backinstock' | 'addToWishlist'
    Returns product-level count (all variants combined).
    """
    r = session.get(
        f"{API_BASE}/api/v3/product/social-count",
        params={"pid": PID, "du": du, "empi": empi, "topic": topic},
        verify=False,
        timeout=REQUEST_TIMEOUT,
    )
    if r.ok:
        return int((r.json().get("data") or {}).get("count") or 0)
    logger.warning("social-count [empi=%s topic=%s]: HTTP %s", empi, topic, r.status_code)
    return 0


def event_count(
    session: requests.Session,
    empi: int,
    du: str,
    et: int,
) -> int:
    """
    Return the event count via the public eventcount endpoint (v2).
    et=4 → wishlist, et=3 → add-to-cart, et=8 → watchlist/BIS
    Returns product-level count.
    """
    r = session.get(
        f"{API_BASE}/api/v2/provider/eventcount",
        params={"pid": PID, "du": du, "et": et, "empi": empi},
        verify=False,
        timeout=REQUEST_TIMEOUT,
    )
    if r.ok:
        return int(r.json().get("count") or 0)
    logger.warning("eventcount [empi=%s et=%s]: HTTP %s", empi, et, r.status_code)
    return 0


# ── Store product discovery ────────────────────────────────────────────────────

def fetch_collection_products(
    session: requests.Session,
    collection_handle: str = "jeans",
) -> List[Dict[str, Any]]:
    """
    Fetch all products from a Shopify collection via the store's products.json API.
    Returns list of {id, title, handle, variants: [{id, title}]}.
    """
    products: List[Dict[str, Any]] = []
    page = 1
    while True:
        url = f"{STORE_URL}/collections/{collection_handle}/products.json"
        r = session.get(url, params={"limit": 250, "page": page},
                        verify=False, timeout=REQUEST_TIMEOUT)
        if not r.ok:
            logger.warning("products.json [page=%d]: HTTP %s", page, r.status_code)
            break
        batch = r.json().get("products", [])
        if not batch:
            break
        products.extend(batch)
        logger.info("Collection '%s': fetched %d products (page %d)", collection_handle, len(products), page)
        if len(batch) < 250:
            break
        page += 1
        time.sleep(0.3)
    return products


def parse_empi_from_page(session: requests.Session, handle: str) -> Optional[int]:
    """Fetch a product page and extract its Shopify product ID (empi)."""
    r = session.get(f"{STORE_URL}/products/{handle}", verify=False, timeout=REQUEST_TIMEOUT)
    if not r.ok:
        return None
    m = re.search(r'SwymProductInfo\.product\s*=\s*\{"id"\s*:\s*(\d+)', r.text)
    if m:
        return int(m.group(1))
    m = re.search(r'"product_id"\s*:\s*(\d+)', r.text)
    return int(m.group(1)) if m else None


# ── Fetch engagement counts ────────────────────────────────────────────────────

def fetch_engagement(
    session: requests.Session,
    products: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    For each product, fetch BIS and wishlist counts.
    Returns list of rows sorted by bis_count descending.
    """
    rows = []
    total = len(products)
    for i, prod in enumerate(products):
        empi = prod["id"]
        handle = prod["handle"]
        title = prod["title"]
        du = f"{STORE_URL}/products/{handle}"

        logger.info("[%d/%d] %s (empi=%s)", i + 1, total, title, empi)

        bis   = social_count(session, empi, du, "backinstock")
        wl    = social_count(session, empi, du, "addToWishlist")
        # eventcount wishlist as a cross-check (et=4)
        wl_ev = event_count(session, empi, du, ET_WISHLIST)

        rows.append({
            "empi": empi,
            "handle": handle,
            "product_title": title,
            "product_url": du,
            "bis_signups": bis,
            "wishlist_count": wl or wl_ev,
            "variant_count": len(prod.get("variants", [])),
        })
        time.sleep(0.15)

    rows.sort(key=lambda r: r["bis_signups"], reverse=True)
    return rows


# ── CSV export ─────────────────────────────────────────────────────────────────

def export_csv(rows: List[Dict[str, Any]], path: str) -> None:
    fieldnames = [
        "empi", "handle", "product_title", "product_url",
        "bis_signups", "wishlist_count", "variant_count",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Exported %d rows → %s", len(rows), path)


# ── Pretty print ───────────────────────────────────────────────────────────────

def print_table(rows: List[Dict[str, Any]]) -> None:
    print(f"\n{'EMPI':<14} {'BIS':>5} {'WL':>5}  Product")
    print("-" * 75)
    for r in rows:
        print(f"{r['empi']:<14} {r['bis_signups']:>5} {r['wishlist_count']:>5}  {r['product_title'][:50]}")
    print(
        "\nCounts are product-level (all variants combined).\n"
        "Per-variant BIS breakdown requires the Swym admin API key\n"
        "(Swym Dashboard → Settings → API).\n"
        "\n'Added to cart' / 'Recently Viewed' from relayfilters are Swym UI tab IDs,\n"
        "not per-product analytics — no public count API exists for these."
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Swym BIS + wishlist counts (no admin key)")
    parser.add_argument("--collection", default="jeans",
                        help="Shopify collection handle (default: jeans)")
    parser.add_argument("--empi", type=int, default=None,
                        help="Look up a single product by Shopify product ID")
    parser.add_argument("--handle", default=None,
                        help="Look up a single product by handle")
    args = parser.parse_args()

    session = make_session()

    if args.empi or args.handle:
        # Single product lookup
        if args.handle and not args.empi:
            args.empi = parse_empi_from_page(session, args.handle)
            if not args.empi:
                print(f"Could not resolve empi for handle '{args.handle}'")
                sys.exit(1)
        handle = args.handle or str(args.empi)
        du = f"{STORE_URL}/products/{handle}"
        bis = social_count(session, args.empi, du, "backinstock")
        wl  = social_count(session, args.empi, du, "addToWishlist")
        print(f"\nProduct empi={args.empi}  ({handle})")
        print(f"  Back-in-stock signups : {bis}  (product level, all variants)")
        print(f"  Wishlist additions    : {wl}")
        return

    # Full collection run
    logger.info("Fetching collection '%s'…", args.collection)
    products = fetch_collection_products(session, args.collection)
    if not products:
        print(f"No products found in collection '{args.collection}'")
        sys.exit(1)

    rows = fetch_engagement(session, products)
    print_table(rows)
    export_csv(rows, OUTPUT_CSV)


if __name__ == "__main__":
    main()
