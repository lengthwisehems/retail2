"""
Swym engagement data fetcher for ramybrook.com.

Pulls per-variant counts for:
  - Back-in-stock (BIS) notification signups
  - Wishlist social counts (per product/empi)

Usage:
  python swym_engagement.py               # fetch all, export CSV
  python swym_engagement.py --epi 40522216964160  # single variant lookup
"""

import argparse
import base64
import csv
import json
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Configuration ──────────────────────────────────────────────────────────────

PID = "JYxW0bO//HIl29BRB2i1vARfPY5YSr+7Xdr/iqq8FgE="
API_BASE = "https://swymstore-v3premium-01.swymrelay.com"

# Candidate API keys extracted from network traffic (tested in order)
CANDIDATE_KEYS: List[str] = [
    "1784dfa5ea23575f610283cb6f728bba",   # checkAndGet response
    "sqGIZvaiYpLHmh32F1oIS1CMyLCK38He",   # collect + checkAndGet responses
    "1Iqla4FsBbAuVJhWugvpQNIYAGIgTAMZ",   # storefront-layout-components.js
    "D41Go086i5RsNouuri4UCgXpKHS4Dyml",   # storefront-layout-components.js
    "SkdJBa017rjjDmzcuBGvfvWrsWDBAulI",   # storefront-layout-components.js
    "kNr7pmIJ4DdbPrAWph6vOa9JZWgXlvoq",   # storefront-layout-components.js
    "um4D8HEA3ykWi2GT4qUKC0ACuK57fL1e",   # storefront-layout-components.js
    "vls5hOq4CSIaqg48A0jSwALSA53mf58x",   # storefront-layout-components.js
]

REQUEST_TIMEOUT = 20
OUTPUT_CSV = "swym_engagement.csv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ── Auth helpers ───────────────────────────────────────────────────────────────

def _make_admin_headers(api_key: str) -> Dict[str, str]:
    token = base64.b64encode(f"{PID}:{api_key}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def probe_admin_key(session: requests.Session, key: str) -> bool:
    """Return True if key authenticates against the storeadmin BIS endpoint."""
    url = f"{API_BASE}/storeadmin/bispa/subscriptions/fetch"
    headers = _make_admin_headers(key)
    try:
        resp = session.post(
            url,
            json={"pid": PID, "limit": 1, "offset": 0},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )
        masked = f"...{key[-4:]}"
        logger.info("Key probe [%s]: HTTP %s", masked, resp.status_code)
        return resp.status_code == 200
    except requests.RequestException as exc:
        logger.warning("Key probe failed: %s", exc)
        return False


def find_working_key(session: requests.Session) -> Optional[str]:
    """Test each candidate key and return the first that works, or None."""
    logger.info("Testing %d candidate API keys…", len(CANDIDATE_KEYS))
    for key in CANDIDATE_KEYS:
        if probe_admin_key(session, key):
            logger.info("Found working key: ...%s", key[-4:])
            return key
    return None


# ── Back-in-Stock ──────────────────────────────────────────────────────────────

_BIS_ENDPOINTS = [
    ("POST", "/storeadmin/bispa/subscriptions/fetch"),
    ("GET",  "/storeadmin/bispa/subscriptions"),
    ("POST", "/storeadmin/v3/bispa/subscriptions/fetch"),
]


def _paginate_bis(
    session: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
) -> Optional[List[Dict[str, Any]]]:
    """Paginate a BIS endpoint; return records or None if endpoint is unusable."""
    collected: List[Dict[str, Any]] = []
    offset = 0
    limit = 100
    while True:
        payload = {"pid": PID, "limit": limit, "offset": offset}
        try:
            if method == "POST":
                resp = session.post(url, json=payload, headers=headers,
                                    timeout=REQUEST_TIMEOUT, verify=False)
            else:
                resp = session.get(url, params=payload, headers=headers,
                                   timeout=REQUEST_TIMEOUT, verify=False)
        except requests.RequestException as exc:
            logger.warning("BIS request error [%s %s]: %s", method, url, exc)
            return None

        if resp.status_code == 404:
            return None
        if not resp.ok:
            logger.warning("BIS endpoint [%s %s]: HTTP %s", method, url, resp.status_code)
            return None

        try:
            data = resp.json()
        except ValueError:
            logger.warning("BIS endpoint [%s]: non-JSON response", url)
            return None

        results: List[Any] = (
            data.get("subscriptions")
            or data.get("results")
            or (data if isinstance(data, list) else [])
        )
        if not isinstance(results, list):
            return collected if collected else None

        collected.extend(results)
        logger.info("BIS [%s]: fetched %d records (offset=%d)", url, len(collected), offset)

        if len(results) < limit:
            break
        offset += limit
        time.sleep(0.3)

    return collected


def fetch_all_bis_subscriptions(
    session: requests.Session, api_key: str
) -> List[Dict[str, Any]]:
    """Fetch all BIS subscriptions across all pages, trying fallback endpoints."""
    headers = _make_admin_headers(api_key)
    for method, path in _BIS_ENDPOINTS:
        url = f"{API_BASE}{path}"
        logger.info("Trying BIS endpoint: %s %s", method, url)
        records = _paginate_bis(session, method, url, headers)
        if records is not None:
            logger.info("BIS: collected %d total subscriptions", len(records))
            return records
        logger.info("BIS endpoint unavailable, trying next…")

    logger.warning("All BIS endpoints failed — no subscription data retrieved")
    return []


# ── Wishlist ───────────────────────────────────────────────────────────────────

def generate_regid(
    session: requests.Session, api_key: str
) -> Tuple[Optional[str], Optional[str]]:
    """Generate a temporary regid+sessionid via the storeadmin API."""
    url = f"{API_BASE}/storeadmin/v3/user/generate-regid"
    headers = _make_admin_headers(api_key)
    try:
        resp = session.post(
            url,
            json={"pid": PID},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
            verify=False,
        )
        if not resp.ok:
            logger.warning("generate-regid: HTTP %s", resp.status_code)
            return None, None
        data = resp.json()
        regid = data.get("regid")
        sessionid = data.get("sessionid") or data.get("session_id")
        if regid:
            logger.info("Generated regid: %s…", str(regid)[:12])
        return regid, sessionid
    except requests.RequestException as exc:
        logger.warning("generate-regid failed: %s", exc)
        return None, None


def fetch_wishlist_counts(
    session: requests.Session,
    regid: str,
    sessionid: str,
    empis: List[int],
) -> Dict[int, int]:
    """Return {empi: wishlist_count} for the given product IDs."""
    url = f"{API_BASE}/api/v3/product/wishlist/social-count"
    counts: Dict[int, int] = {}
    batch_size = 10

    for i in range(0, len(empis), batch_size):
        batch = empis[i : i + batch_size]
        for empi in batch:
            payload = {
                "pid": PID,
                "regid": regid,
                "sessionid": sessionid,
                "empi": empi,
            }
            try:
                resp = session.post(
                    url,
                    data=payload,
                    timeout=REQUEST_TIMEOUT,
                    verify=False,
                )
                if resp.ok:
                    data = resp.json()
                    count = (data.get("data") or {}).get("count", 0)
                    counts[empi] = int(count or 0)
                else:
                    logger.warning("Wishlist count [empi=%s]: HTTP %s", empi, resp.status_code)
                    counts[empi] = 0
            except requests.RequestException as exc:
                logger.warning("Wishlist count [empi=%s] failed: %s", empi, exc)
                counts[empi] = 0
            time.sleep(0.15)

    return counts


# ── Aggregation ────────────────────────────────────────────────────────────────

def aggregate_bis(
    subscriptions: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Group BIS subscription records by epi (variant ID); count per variant."""
    by_epi: Dict[str, Dict[str, Any]] = {}
    for sub in subscriptions:
        epi = str(sub.get("epi") or sub.get("variant_id") or "")
        if not epi:
            continue
        if epi not in by_epi:
            by_epi[epi] = {
                "epi": epi,
                "empi": sub.get("empi") or sub.get("product_id") or "",
                "product_title": sub.get("dt") or sub.get("product_title") or "",
                "variant_url": sub.get("du") or sub.get("product_url") or "",
                "bis_signup_count": 0,
            }
        by_epi[epi]["bis_signup_count"] += 1
    return by_epi


# ── CSV export ─────────────────────────────────────────────────────────────────

def export_csv(
    bis_data: Dict[str, Dict[str, Any]],
    wishlist_counts: Dict[int, int],
    path: str,
) -> None:
    rows = sorted(
        bis_data.values(),
        key=lambda r: r["bis_signup_count"],
        reverse=True,
    )
    fieldnames = [
        "epi", "empi", "product_title", "variant_url",
        "bis_signup_count", "wishlist_count",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            empi_int = int(row["empi"]) if str(row["empi"]).isdigit() else 0
            writer.writerow({
                "epi": row["epi"],
                "empi": row["empi"],
                "product_title": row["product_title"],
                "variant_url": row["variant_url"],
                "bis_signup_count": row["bis_signup_count"],
                "wishlist_count": wishlist_counts.get(empi_int, ""),
            })
    logger.info("Exported %d rows → %s", len(rows), path)


# ── Pretty print ───────────────────────────────────────────────────────────────

def print_table(
    bis_data: Dict[str, Dict[str, Any]],
    wishlist_counts: Dict[int, int],
    epi_filter: Optional[str] = None,
) -> None:
    rows = sorted(
        bis_data.values(),
        key=lambda r: r["bis_signup_count"],
        reverse=True,
    )
    if epi_filter:
        rows = [r for r in rows if r["epi"] == epi_filter]
        if not rows:
            print(f"\nNo BIS subscription data found for epi={epi_filter}")
            return

    print(f"\n{'EPI':<20} {'EMPI':<14} {'BIS Signups':>11} {'Wishlist':>9}  Product")
    print("-" * 90)
    for row in rows:
        empi_int = int(row["empi"]) if str(row["empi"]).isdigit() else 0
        wl = wishlist_counts.get(empi_int, "—")
        title = (row["product_title"] or row["variant_url"] or "")[:45]
        print(
            f"{row['epi']:<20} {str(row['empi']):<14} "
            f"{row['bis_signup_count']:>11} {str(wl):>9}  {title}"
        )

    print(
        "\nNote: 'Added to cart' and 'Recently Viewed' values in Swym's "
        "relayfilters (3 and 1) are UI tab IDs, not per-variant analytics — "
        "no per-variant count API exists for these."
    )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Swym engagement data")
    parser.add_argument(
        "--epi",
        help="Show counts for a single variant ID (epi) only",
        default=None,
    )
    args = parser.parse_args()

    session = requests.Session()
    session.headers.update({"User-Agent": "SwymEngagementFetcher/1.0"})

    # 1. Find a working API key
    api_key = find_working_key(session)
    if not api_key:
        print(
            "\nNo working API key found among candidates.\n"
            "To get the real Swym API key:\n"
            "  Swym Admin Dashboard → Settings → API → copy the API key\n"
            "Then add it to CANDIDATE_KEYS at the top of this script."
        )
        sys.exit(1)

    print(f"\nUsing API key: ...{api_key[-4:]}")

    # 2. Fetch BIS subscriptions
    subscriptions = fetch_all_bis_subscriptions(session, api_key)
    bis_data = aggregate_bis(subscriptions)
    print(f"Back-in-stock: {len(subscriptions)} subscriptions across {len(bis_data)} variants")

    # 3. Generate regid for wishlist calls
    wishlist_counts: Dict[int, int] = {}
    regid, sessionid = generate_regid(session, api_key)
    if regid and sessionid:
        empis = list({
            int(v["empi"]) for v in bis_data.values()
            if str(v.get("empi", "")).isdigit()
        })
        if empis:
            logger.info("Fetching wishlist counts for %d products…", len(empis))
            wishlist_counts = fetch_wishlist_counts(session, regid, sessionid, empis)
    else:
        logger.warning("Could not generate regid; skipping wishlist counts")

    # 4. Display and export
    print_table(bis_data, wishlist_counts, epi_filter=args.epi)

    if not args.epi:
        export_csv(bis_data, wishlist_counts, OUTPUT_CSV)


if __name__ == "__main__":
    main()
