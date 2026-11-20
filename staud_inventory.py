#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import requests
from bs4 import BeautifulSoup

BRAND = "STAUD"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / f"{BRAND}_run.log"

# Product pages and collection .json. www.staud.clothing and
# staud-clothing.myshopify.com both redirect here, so all three addresses share
# one rate limiter: trying another address after a 429 only adds blocked
# requests (same finding as Good American and AYR). One address only.
SITE_HOST = "https://staud.clothing"

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
# Which headers to send. False = simple headers (default, as in AYR: a full fake
# set of Chrome headers drew a hard 403 there where simple ones drew a softer
# 429). True = the full Chrome set Good American sends.
USE_FULL_BROWSER_HEADERS = False
SIMPLE_HEADERS = {"User-Agent": USER_AGENT, "Accept": "application/json,text/html,*/*"}
BROWSER_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "sec-ch-ua": '"Chromium";v="120", "Not(A:Brand";v="24", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}
HEADERS = BROWSER_HEADERS if USE_FULL_BROWSER_HEADERS else SIMPLE_HEADERS

# Site pacing - the Good American / AYR method. A 429 from staud.clothing is
# Cloudflare, and waiting does not clear it. So every site request waits its
# turn at a shared pace; a block slows the pace, clean responses speed it back
# up; a wait is capped; a page gets at most 2 tries in the main pass and one
# more at the end of the run.
PDP_MIN_INTERVAL = 0.5      # floor: seconds between any two site requests
PDP_MAX_INTERVAL = 6.0      # ceiling once blocks have forced the pace down
PDP_INTERVAL_STEP = 0.75    # added to the interval after each block
PDP_INTERVAL_DECAY = 0.9    # multiplier applied after each clean response
PDP_BLOCK_MAX_WAIT = 15.0   # cap: a challenge outlasts any wait worth making
PDP_BLOCK_SLEEP = 5.0       # wait when the server gives no Retry-After
PDP_GETS_PER_TRY = 2        # HTTP GETs per try
PDP_TRIES_PER_PAGE = 2      # tries per page across the whole run
BLOCK_STATUSES = {403, 429}
TRANSIENT_STATUSES = {500, 502, 503, 504}

# Real-browser fallback (same as AYR). When the site blocks plain requests from
# this computer, a real Chrome reads the pages instead. It starts hidden; if that
# is blocked too it reopens in a VISIBLE Chrome window - if a "verify you are
# human" box shows there, click it (the run waits up to 60s).
BROWSER_FALLBACK_ENABLED = True
BLOCKS_BEFORE_BROWSER = 2       # blocked pages in a row before switching to the browser
BROWSER_HEADLESS = True
BROWSER_RETRY_VISIBLE = True
BROWSER_CHALLENGE_WAIT_MS = 20000
BROWSER_VISIBLE_WAIT_MS = 60000
BROWSER_RENDER_TIMEOUT_MS = 45000
# Only needed when Chrome / Playwright's Chromium can't be found (left blank normally).
BROWSER_EXECUTABLE_PATH = os.environ.get("STAUD_BROWSER_EXECUTABLE", "")
# Backup: every product page read successfully is saved here. If a page can't be
# read today, the last saved copy is used for Description / Rise / Inseam.
# Quantities come from GraphQL, never from these pages.
PDP_SAVE_DIR = OUTPUT_DIR / "STAUD_saved_product_pages"

COLLECTION_HANDLES = ["staud-jeans", "sale","denim"]
GRAPHQL_ENDPOINT = "https://staud-clothing.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN_ENV_VARS = [
    "X_SHOPIFY_STOREFRONT_ACCESS_TOKEN",
    "SHOPIFY_STOREFRONT_ACCESS_TOKEN",
    "STAUD_STOREFRONT_TOKEN",
    "x-shopify-storefront-access-token",
]
# Tried in order; the first one Shopify accepts is used for the whole run.
# 1fd95522... sees the most denim (15 more jeans than 17902caa...), so it goes first.
# ff6b0274... stopped working (401) on 2026-11-19 and was removed.
HARDCODED_TOKENS = [
    "1fd9552249820c33f49d877c0d68e87c",
    "17902caa8051c3291a095a5d505d7811",
]

FILTER_HANDLE_SUBSTRINGS = [
    "dress", "sweater", "jacket", "shoe", "heel", "sandal", "ankle-boot",
    "over-the-knee-boot", "platform", "pump", "wedge", "sling-back", "pullover",
    "romper", "bag", "blazer", "bodysuit", "capri", "carryall", "carry-all", "coat",
    "combat-boot", "skirt", "accessories", "rompers", "shorts", "ballet-flat", "hat",
    "scarf", "scrunchie", "tank", "tee", "thong", "unitard", "culotte","short","vest","shirt",
    "Bermuda", "Bermudas", "Blazer", "Blazers", "Blouse", "Blouses", "Bodysuit", "Bodysuits",
    "Button-Up", "Capri", "Cardigan", "Cardigans", "Coat", "Coats", "Corset", "Corsets", "Crop-Top", 
    "Crop-Tops", "croptop", "Denim-Short", "Denimshort", "Donation", "Dress", "Dresses",
    "Handbag", "Heel", "Heels", "Hoodie", "Hoodies", "Jacket", "Jackets", "Jogger-Short", "Joggershort", "Jort", 
    "Jumpsuit", "Jumpsuits", "Neck", "Onepiece", "One-Piece", "One-Pieces", "Outerwear", "Pant-Suit", "Pantsuits", 
    "Purse", "Romper", "Rompers", "Sandel", "Sandle", "Shacket", "Shipping", "Shipping-Protection", "Shirt", "Shirts", 
    "Shoe", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Sleeve", "Sleeves", "Suit", "Suits", "Sweater", "Sweaters", "Sweatpant", 
    "Sweatpants", "Sweatshirt", "Sweatshirts", "Swim", "TShirt", "T-Shirts", "Tank", "Tank-Tops", "Tee", "Tees", "Tops", "Tote",
]
FILTER_TITLE_WORDS = {"short", "skirt", "capri", "romper", "jacket", "culotte","shirt","vest"}
ALLOWED_PRODUCT_TYPE = "denim"
FILTER_PRODUCT_TYPE_WORDS = {
    "Accessories", "Accessory", "Bermuda", "Bermudas", "Blazer", "Blazers", "Blouse", "Blouses", "Bodysuit", "Bodysuits", 
    "Button Up", "Button-Up", "Capri", "Cardigan", "Cardigans", "Clothing Top", "Clothing Tops", "Coat", "Coats", "Coats & Jackets", 
    "Core Handbags", "Corset", "Corsets", "Crop Top", "Crop Tops", "Denim Short", "Denim Shorts", "Donation", "Dress", "Dresses", 
    "Fashion Core Handbag", "Fashion Core Handbags", "Fashion Handbag", "Fashion Handbags", "Gift Wrap", "Goodies Accessories", 
    "Goodies Accessory", "Handbag", "Heel", "Heels", "Hoodie", "Hoodies", "Jacket", "Jackets", "Jogger Short", "Jogger Shorts", "Jort", 
    "Jumpsuit", "Jumpsuits", "Neck", "One Piece", "One Pieces", "One-Piece", "One-Pieces", "Outerwear", "Pant Suit", "Pant Suits", 
    "Purse", "Romper", "Rompers", "Sandel", "Sandle", "Shacket", "Shipping", "Shipping Protection", "Shirt", "Shirts", "Shirts & Tops", 
    "Shoe", "Shoes", "Short", "Shorts", "Skirt", "Skirts", "Sleeve", "Sleeves", "Suit", "Suits", "Sweat", "Sweater", "Sweaters", "Sweatpant", 
    "Sweatpants", "Sweats", "Sweatshirt", "Sweatshirts", "Swim", "T Shirt", "T Shirts", "Tank", "Tank Tops", "Tee", "Tees", "Top", "Tops", "Tote"
}

STYLE_REMOVE_TERMS = [
    # Original list (kept — none of these conflict with the new spec below)
    "Accent Hardware", "Beaded", "Corduroy", "Cuff", "Cuffed", "Darted", "Destroyed", "Distressed",
    "Flap Pocket", "Flap", "Frayed Seam", "Front Yoke", "w/ Slit Hem", "Leather", "Lightweight",
    "low and loose", "patch", "petite", "Pintucked", "Raw Hem", "Seam", "Seamed Front Yoke",
    "Seamed", "Side Seam Snaps", "Split", "Snake Print", "Track Pant", "Vegan Leather", "vent",
    "slit", "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "W/ Stud Detailing", "W/ Wide Cuff",
    "W/Flap", "Welt Pocket", "With Cuff", "With Frayed Seam", "Zipper", "BELTED", "CROP", "CUTOFF",
    "FLAG", "FLIP", "KRYSTAL", "CRYSTAL", "PANEL", "PLAID", "RENAISSANCE", "RINSE", "RIPPED",
    "SADDLE", "SEQUIN", "SOTT", "SPARK", "SPARKLE", "SPLICED", "STUDDED", "TRASHED", "WAX",
    "COATED", "FRONTIER", "LO", "SELVEDGE", "CRUSHED", "REPAIR", "SLICE", "PLUS", "ANKLE", "crease",
    # 2026-08-03 spec Step 3 additions (union with the above; matching is
    # already case-insensitive via re.IGNORECASE, so casing here is
    # cosmetic only)
    "Cargo", "Chap", "Constructed", "Cropped", "Cut-out", "Drawstring", "Extra", "Fit",
    "High Rise", "High-Rise", "Inch", "Inset", "Jean", "Jeans", "Kick", "Long", "Low Rise",
    "Low-Rise", "Mid Rise", "Mid-Rise", "Ms.", "Pant", "Pants", "Pleated", "Pocket", "Poplin",
    "Retro", "Rolled Hem", "Rolled Up", "stretch", "Slouchy", "Stacked Waist", "Stacked",
    "Stoned", "Super", "The", "Trouser", "Ultra", "Utility", "V-High Rise", "Vintage", "with", "seams", "tall"
]


def style_name(title: str) -> str:
    """
    Style Name derivation (2026-08-03 spec):
      Step 1: everything before " | "
      Step 2: "-" -> " " (e.g. "Wide-Leg" -> "Wide Leg")
      Step 3: strip numbers, quotation marks, and all STYLE_REMOVE_TERMS
    """
    s = (title or "").split("|")[0]
    s = s.replace("-", " ")
    s = re.sub(r"\d+", " ", s)          # any numbers
    s = s.replace('"', " ").replace("\u201c", " ").replace("\u201d", " ")  # quotation marks
    for term in sorted(STYLE_REMOVE_TERMS, key=len, reverse=True):
        s = re.sub(r"\b" + re.escape(term).replace(r"\ ", r"\s+") + r"\b", " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()

CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size", "Rise", "Inseam", "Price", "Compare at Price",
    "Available for Sale", "Quantity Available", "Quantity of style", "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL", "Jean Style", "Inseam Style", "Inseam Label", "Rise Label", "Color - Standardized", "Stretch",
]

GRAPHQL_COLLECTION_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 100, after: $cursor) {
      nodes {
        id
        handle
        title
        productType
        tags
        vendor
        description
        publishedAt
        createdAt
        totalInventory
        onlineStoreUrl
        featuredImage { url }
        variants(first: 100) {
          nodes {
            id
            title
            sku
            barcode
            availableForSale
            quantityAvailable
            price { amount }
            compareAtPrice { amount }
            selectedOptions { name value }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

GRAPHQL_PRODUCT_QUERY = """
query ProductByHandle($handle: String!) {
  product(handle: $handle) {
    id
    handle
    title
    productType
    tags
    vendor
    description
    publishedAt
    createdAt
    totalInventory
    onlineStoreUrl
    featuredImage { url }
    variants(first: 100) {
      nodes {
        id
        title
        sku
        barcode
        availableForSale
        quantityAvailable
        price { amount }
        compareAtPrice { amount }
        selectedOptions { name value }
      }
    }
  }
}
"""

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def configure_logging() -> None:
    # Windows Command Prompt is not UTF-8 by default; never crash on a curly quote.
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except Exception:
            pass
    OUTPUT_DIR.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
    )


_WORKING_TOKEN: Optional[str] = None


def _token_candidates() -> List[str]:
    out: List[str] = []
    for key in GRAPHQL_TOKEN_ENV_VARS:
        value = (os.getenv(key) or "").strip()
        if value and value not in out:
            out.append(value)
    out.extend(t for t in HARDCODED_TOKENS if t not in out)
    return out


def graphql_token() -> str:
    """
    Return the first storefront token Shopify accepts (checked once, then
    cached). Env-var tokens are tried before the hardcoded list. An expired
    token returns 401, so we just move on to the next one.
    """
    global _WORKING_TOKEN
    if _WORKING_TOKEN:
        return _WORKING_TOKEN
    candidates = _token_candidates()
    for tok in candidates:
        try:
            resp = post_with_retry(
                GRAPHQL_ENDPOINT,
                headers={"X-Shopify-Storefront-Access-Token": tok, "Content-Type": "application/json"},
                payload={"query": "{ shop { name } }"},
            )
            if resp.status_code == 200 and ((resp.json().get("data") or {}).get("shop")):
                logging.info("Using storefront token %s...", tok[:8])
                _WORKING_TOKEN = tok
                return tok
            logging.warning("Storefront token %s... rejected (HTTP %s)", tok[:8], resp.status_code)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Storefront token %s... check failed: %s", tok[:8], exc)
    raise RuntimeError(
        "No working Shopify storefront token. All of these were rejected: "
        + ", ".join(t[:8] + "..." for t in candidates)
        + ". Find a new one in the staud.clothing page source and add it to HARDCODED_TOKENS."
    )


def normalize_text(text: str) -> str:
    t = (text or "").lower().replace("-", " ")
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def contains_any(text: str, phrases: Sequence[str]) -> bool:
    n = normalize_text(text)
    return any(normalize_text(p) in n for p in phrases)


def parse_number_with_fraction(text: str) -> str:
    raw = (text or "").replace("½", " 1/2").replace("¼", " 1/4").replace("¾", " 3/4").replace('"', " ")
    total = 0.0
    for tok in re.split(r"\s+", raw.strip()):
        if not tok:
            continue
        if re.fullmatch(r"\d+/\d+", tok):
            total += float(Fraction(tok))
        else:
            try:
                total += float(tok)
            except Exception:
                pass
    if not total:
        return ""
    rounded = Decimal(str(total)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    if rounded == rounded.to_integral():
        return str(int(rounded))
    return format(rounded.normalize(), "f").rstrip("0").rstrip(".")


def normalize_measurement(value: str) -> str:
    parsed = parse_number_with_fraction(value)
    if not parsed:
        return ""
    try:
        numeric = float(parsed)
    except Exception:
        return parsed
    if numeric > 38:
        return f"{numeric / 2.54:.3f}".rstrip("0").rstrip(".")
    return parsed


def to_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        num = float(str(value).replace(",", "").strip())
    except Exception:
        return ""
    if abs(num - int(num)) < 1e-9:
        return f"${int(num):,}"
    return f"${num:,.2f}"


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except Exception:
        return ""


def strip_gid(value: str, prefix: str) -> str:
    if not value:
        return ""
    return value.replace(prefix, "") if value.startswith(prefix) else value.split("/")[-1]


def post_with_retry(url: str, *, headers: dict, payload: dict, max_retries: int = 3, timeout: int = 45) -> requests.Response:
    """GraphQL calls (Good American's request_with_retry): 429/5xx and
    connection errors retried after 1s, 2s, 4s."""
    resp: Optional[requests.Response] = None
    for attempt in range(max_retries):
        try:
            resp = SESSION.post(url, headers=headers, json=payload, timeout=timeout)
        except requests.RequestException as exc:
            if attempt + 1 >= max_retries:
                raise
            logging.warning("Retrying POST %s (%s)", url, exc)
            time.sleep(2 ** attempt)
            continue
        if resp.status_code in {429} | TRANSIENT_STATUSES and attempt + 1 < max_retries:
            logging.warning("Retrying POST %s (HTTP %s)", url, resp.status_code)
            time.sleep(2 ** attempt)
            continue
        break
    return resp


# ---- site fetching (Good American / AYR method) ---------------------------
_PACE: Dict[str, float] = {"interval": PDP_MIN_INTERVAL, "last": 0.0}
_PAGE_CACHE: Dict[str, Optional[str]] = {}
_PAGE_TRIES: Dict[Tuple[str, ...], int] = {}
_PAGES_BLOCKED: Set[str] = set()   # paths never read because of a block / error
_REQUEST_COUNTS: Dict[str, int] = {"http_gets": 0, "blocked": 0, "browser_reads": 0}
_STATE: Dict[str, Any] = {"use_browser": False, "consecutive_blocks": 0, "browser": None}

CHALLENGE_MARKERS = ("verifying your connection", "just a moment", "checking your browser", "cf-chl-")
BLOCK_TITLE_MARKERS = (
    "access denied", "attention required", "you have been blocked", "forbidden",
    "too many requests", "error 1020", "request blocked", "verify you are human",
)


def fmt_duration(seconds: float) -> str:
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def page_title(html: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", html or "", re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s+", " ", match.group(1)).strip()[:120] if match else ""


def html_looks_blocked(html: str) -> bool:
    head = (html or "")[:5000].lower()
    title = page_title(html).lower()
    return any(m in head for m in CHALLENGE_MARKERS) or any(m in title for m in BLOCK_TITLE_MARKERS)


def _pace_request() -> None:
    """Hold every site request to the current shared interval."""
    wait = _PACE["last"] + _PACE["interval"] - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    _PACE["last"] = time.monotonic()


def _is_bot_challenge(response: requests.Response) -> bool:
    return any(m in (response.text or "")[:5000].lower() for m in CHALLENGE_MARKERS)


def _get_with_backoff(url: str) -> Optional[requests.Response]:
    """Paced GET. A block (403/429) or server error slows the pace for every
    later request and gets ONE capped wait and retry - never a long sleep."""
    response = None
    for attempt in range(PDP_GETS_PER_TRY):
        _pace_request()
        _REQUEST_COUNTS["http_gets"] += 1
        try:
            response = SESSION.get(url, timeout=30)
        except requests.RequestException as exc:
            logging.warning("Request error for %s: %s", url, exc)
            response = None
        status = response.status_code if response is not None else None
        if status is not None and status not in BLOCK_STATUSES and status not in TRANSIENT_STATUSES:
            _PACE["interval"] = max(PDP_MIN_INTERVAL, _PACE["interval"] * PDP_INTERVAL_DECAY)
            return response
        if status in BLOCK_STATUSES:
            _REQUEST_COUNTS["blocked"] += 1
        _PACE["interval"] = min(PDP_MAX_INTERVAL, _PACE["interval"] + PDP_INTERVAL_STEP)
        if attempt + 1 >= PDP_GETS_PER_TRY:
            break
        retry_after = (response.headers.get("Retry-After", "") if response is not None else "").strip()
        wait = float(retry_after) if retry_after.replace(".", "", 1).isdigit() else PDP_BLOCK_SLEEP * (attempt + 1)
        wait = min(wait, PDP_BLOCK_MAX_WAIT)
        kind = ("Bot challenge" if response is not None and _is_bot_challenge(response) else
                "Blocked" if status in BLOCK_STATUSES else "Server error" if status else "Connection error")
        logging.warning("%s (HTTP %s) on %s; waiting %.0fs (pace now %.1fs)", kind, status, url, wait, _PACE["interval"])
        time.sleep(wait)
    return response


def switch_to_browser(reason: str) -> bool:
    """Stop sending plain requests (each blocked one can extend the block) and
    read the remaining pages through a real browser."""
    if _STATE["use_browser"]:
        return True
    if not BROWSER_FALLBACK_ENABLED:
        return False
    logging.warning("%s Switching to a real browser for the rest of the run (no more plain requests).", reason)
    _STATE["use_browser"] = True
    if _STATE["browser"] is None:
        _STATE["browser"] = BrowserSession()
    return True


def fetch_page(path: str, kind: str) -> Optional[str]:
    """A site path (product page "html" or collection "json"), fetched once and
    cached. A blocked or failed page is NOT cached, so a later pass can try
    again; a page that was really answered (200, 404...) is cached for the
    whole run. Plain requests first; after BLOCKS_BEFORE_BROWSER blocked pages
    in a row the run switches to the real browser."""
    if path in _PAGE_CACHE:
        return _PAGE_CACHE[path]
    url = f"{SITE_HOST}{path}"

    if _STATE["use_browser"]:
        browser = _STATE["browser"]
        tries_key = ("browser", path)
        if browser is None or browser.unavailable or _PAGE_TRIES.get(tries_key, 0) >= 1:
            # Not read (browser could not start, or already tried): it still
            # counts as unread, so its saved copy is used at the end.
            _PAGES_BLOCKED.add(path)
            return None
        _PAGE_TRIES[tries_key] = 1
        text, status = browser.fetch_text(url, f"{SITE_HOST}/")
        _REQUEST_COUNTS["browser_reads"] += 1
    else:
        if _PAGE_TRIES.get((path,), 0) >= PDP_TRIES_PER_PAGE:
            _PAGES_BLOCKED.add(path)
            return None
        _PAGE_TRIES[(path,)] = _PAGE_TRIES.get((path,), 0) + 1
        response = _get_with_backoff(url)
        status = response.status_code if response is not None else None
        text = response.text if response is not None else None

    blocked = status is None or status in BLOCK_STATUSES or status in TRANSIENT_STATUSES
    if status == 200 and kind == "html" and html_looks_blocked(text or ""):
        blocked = True
    if blocked:
        _PAGES_BLOCKED.add(path)
        logging.warning("Could not read %s (HTTP %s)%s", path, status,
                        "" if _STATE["use_browser"] else "; will try again later")
        if not _STATE["use_browser"]:
            _STATE["consecutive_blocks"] += 1
            if _STATE["consecutive_blocks"] >= BLOCKS_BEFORE_BROWSER:
                switch_to_browser(f"STAUD blocked {_STATE['consecutive_blocks']} pages in a row from this computer.")
        return None
    _STATE["consecutive_blocks"] = 0
    _PAGES_BLOCKED.discard(path)
    if status != 200:
        if status != 404:
            logging.warning("%s returned HTTP %s", path, status)
        text = None
    _PAGE_CACHE[path] = text
    return text


def product_path(handle: str) -> str:
    return f"/products/{handle}"


def save_product_page(handle: str, html: str) -> None:
    try:
        PDP_SAVE_DIR.mkdir(parents=True, exist_ok=True)
        (PDP_SAVE_DIR / f"{handle}.html").write_text(html, encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logging.info("Could not save product page for %s: %s", handle, exc)


def load_saved_product_page(handle: str) -> Tuple[str, str]:
    """(html, saved date) of the last good copy of a product page, or ("", "")."""
    path = PDP_SAVE_DIR / f"{handle}.html"
    try:
        html = path.read_text(encoding="utf-8")
        saved = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        return html, saved
    except Exception:  # noqa: BLE001
        return "", ""


# ---- real browser (from AYR / retail_PDP_probe.py) -------------------------
def load_sync_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:  # noqa: BLE001
        logging.warning(
            "Playwright is not installed, so the browser fallback is unavailable (%s). "
            "Install it with: pip install playwright  then: playwright install chromium", exc)
        return None
    return sync_playwright


class BrowserSession:
    """One real browser shared by the whole run (same as AYR).

    A bot check passed once stays passed (its cookies carry over). Pages and
    .json are read with fetch() from inside an open STAUD page, so every
    request carries the browser's cookies. Starts hidden; if the hidden browser
    is blocked it reopens visibly so a person can click "verify you are human".
    """

    def __init__(self) -> None:
        self._playwright = None
        self._browser = None
        self.context = None
        self.visible = False
        self.force_visible = not BROWSER_HEADLESS
        self.unavailable = False
        self.anchor = None

    def _launch(self, visible: bool) -> bool:
        if self.context is not None and self.visible == visible:
            return True
        self._shutdown()
        sync_playwright = load_sync_playwright()
        if sync_playwright is None:
            self.unavailable = True
            return False
        self._playwright = sync_playwright().start()
        mode = "visible window" if visible else "headless"
        attempts = ([("custom", BROWSER_EXECUTABLE_PATH)] if BROWSER_EXECUTABLE_PATH else []) + [("chrome", None), (None, None)]
        for channel, exe in attempts:
            name = "Chrome at STAUD_BROWSER_EXECUTABLE" if exe else ("Google Chrome" if channel else "Playwright Chromium")
            kwargs: Dict[str, Any] = {"headless": not visible, "args": ["--disable-blink-features=AutomationControlled"]}
            if exe:
                kwargs["executable_path"] = exe
            elif channel:
                kwargs["channel"] = channel
            try:
                self._browser = self._playwright.chromium.launch(**kwargs)
            except Exception as exc:  # noqa: BLE001
                logging.info("Could not start %s (%s): %s", name, mode, str(exc).strip().splitlines()[0])
                continue
            logging.info("Browser started: %s (%s)", name, mode)
            break
        if self._browser is None:
            self._shutdown()
            self.unavailable = True
            return False
        major = (self._browser.version or "124").split(".")[0]
        user_agent = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      f"(KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36")
        extra_headers = {
            "sec-ch-ua": f'"Google Chrome";v="{major}", "Chromium";v="{major}", "Not_A Brand";v="24"',
            "sec-ch-ua-platform": '"Windows"',
        }
        self.context = self._browser.new_context(user_agent=user_agent, locale="en-US",
                                                 viewport={"width": 1366, "height": 900},
                                                 extra_http_headers=extra_headers)
        self.context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.visible = visible
        return True

    def _shutdown(self) -> None:
        for closer in (self._browser, self._playwright):
            if closer is None:
                continue
            try:
                closer.close() if closer is self._browser else closer.stop()
            except Exception:  # noqa: BLE001
                pass
        self._browser = None
        self._playwright = None
        self.context = None
        self.anchor = None

    def close(self) -> None:
        self._shutdown()

    def _wait_until_clear(self, page, nav_status: Dict[str, Optional[int]], label: str, wait_ms: int) -> bool:
        deadline = time.monotonic() + wait_ms / 1000
        announced = False
        while True:
            try:
                html = page.content()
            except Exception:  # noqa: BLE001
                html = ""
            status = nav_status.get("status")
            blocked = not html or html_looks_blocked(html) or (status is not None and status in BLOCK_STATUSES)
            if not blocked:
                if announced:
                    logging.info("Browser got past the block for %s", label)
                return True
            if time.monotonic() > deadline:
                logging.warning("Browser still blocked after %.0fs (HTTP %s, page title: '%s'): %s",
                                wait_ms / 1000, status, page_title(html), label)
                return False
            if not announced:
                logging.info("Browser is blocked for %s (HTTP %s, page title: '%s'); waiting up to %.0fs for it to clear",
                             label, status, page_title(html), wait_ms / 1000)
                announced = True
            page.wait_for_timeout(1000)

    def open_page(self, url: str):
        """Open url and wait out any block. Returns the page, or None if still blocked."""
        if self.unavailable:
            return None
        modes = [True] if self.force_visible else ([False, True] if BROWSER_RETRY_VISIBLE else [False])
        for visible in modes:
            if not self._launch(visible):
                if self.unavailable:
                    return None
                continue
            page = self.context.new_page()
            nav_status: Dict[str, Optional[int]] = {"status": None}

            def on_response(response, page=page, nav_status=nav_status) -> None:
                try:
                    if response.request.is_navigation_request() and response.frame == page.main_frame:
                        nav_status["status"] = response.status
                except Exception:  # noqa: BLE001
                    pass

            page.on("response", on_response)
            try:
                _pace_request()
                page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_RENDER_TIMEOUT_MS)
                wait_ms = BROWSER_VISIBLE_WAIT_MS if visible else BROWSER_CHALLENGE_WAIT_MS
                if self._wait_until_clear(page, nav_status, url, wait_ms):
                    return page
            except Exception as exc:  # noqa: BLE001
                logging.warning("Browser could not load %s: %s", url, str(exc).strip().splitlines()[0])
            try:
                page.close()
            except Exception:  # noqa: BLE001
                pass
            if not visible and True in modes:
                logging.warning("The hidden browser was blocked; reopening in a visible Chrome window. "
                                "If a 'verify you are human' box appears there, click it.")
                self.force_visible = True
        return None

    def _fetch_in_page(self, url: str) -> Tuple[Optional[str], Optional[int]]:
        _pace_request()
        try:
            result = self.anchor.evaluate(
                """async (url) => {
                    const r = await fetch(url, {credentials: 'same-origin'});
                    return {status: r.status, text: await r.text()};
                }""", url)
            return result.get("text"), result.get("status")
        except Exception as exc:  # noqa: BLE001
            logging.warning("Browser fetch failed for %s: %s", url, str(exc).strip().splitlines()[0])
            return None, None

    def fetch_text(self, url: str, anchor_url: str) -> Tuple[Optional[str], Optional[int]]:
        """(text, HTTP status) of url read through the browser. On a block the
        STAUD page is reopened once (waiting out the check, visibly if needed)."""
        text, status = None, None
        for attempt in range(2):
            if self.anchor is None:
                self.anchor = self.open_page(anchor_url)
                if self.anchor is None:
                    return None, None
            text, status = self._fetch_in_page(url)
            blocked = status is None or status in BLOCK_STATUSES or status in TRANSIENT_STATUSES or \
                (status == 200 and ".json" not in url and html_looks_blocked(text or ""))
            if not blocked:
                _PACE["interval"] = max(PDP_MIN_INTERVAL, _PACE["interval"] * PDP_INTERVAL_DECAY)
                return text, status
            _PACE["interval"] = min(PDP_MAX_INTERVAL, _PACE["interval"] + PDP_INTERVAL_STEP)
            logging.warning("Browser read of %s was blocked (HTTP %s)%s", url, status,
                            "; reopening the STAUD page" if attempt == 0 else "")
            try:
                self.anchor.close()
            except Exception:  # noqa: BLE001
                pass
            self.anchor = None
        return text, status


def close_browser() -> None:
    if _STATE["browser"] is not None:
        _STATE["browser"].close()
        _STATE["browser"] = None


def fetch_all_product_pages(handles: List[str], use_browser: bool = False) -> Dict[str, str]:
    """Product page HTML for every handle (AYR's fetch_all_pages, pages only -
    STAUD quantities come from GraphQL, not the product .json).

    1. Plain requests, Good American pacing. After BLOCKS_BEFORE_BROWSER blocked
       pages in a row, the rest of the run uses a real browser.
    2. Anything still unread gets one more try (through the browser if the run
       has switched, otherwise plain requests once the pace has backed off; if
       that still fails, through the browser).
    3. A product page that still can't be read uses its last saved copy.
    The run never stops over a product page.
    """
    pages: Dict[str, str] = {}
    if use_browser:
        switch_to_browser("--browser was given.")

    def run_pass(todo: List[str], label: str) -> None:
        start = time.monotonic()
        for i, handle in enumerate(todo, start=1):
            if not pages.get(handle):
                html = fetch_page(product_path(handle), "html") or ""
                pages[handle] = html
                if html:
                    save_product_page(handle, html)
            elapsed = time.monotonic() - start
            eta = elapsed / i * (len(todo) - i)
            logging.info("%s %s/%s (%s) - elapsed %s, ETA %s", label, i, len(todo), handle,
                         fmt_duration(elapsed), fmt_duration(eta))

    def unread() -> List[str]:
        return [h for h in handles if product_path(h) in _PAGES_BLOCKED]

    try:
        run_pass(handles, "Product pages")
        for attempt in (1, 2):
            todo = unread()
            if not todo:
                break
            if attempt == 2 and not _STATE["use_browser"]:
                if not switch_to_browser("Some pages are still blocked after the second try."):
                    break
            logging.info("Retrying %s product page(s) the site blocked%s", len(todo),
                         " (real browser)" if _STATE["use_browser"] else "")
            run_pass(todo, "Retry product pages")
    finally:
        close_browser()

    logging.info("Site requests: %s plain (%s blocked), %s read through the browser; final pace %.1fs",
                 _REQUEST_COUNTS["http_gets"], _REQUEST_COUNTS["blocked"], _REQUEST_COUNTS["browser_reads"],
                 _PACE["interval"])

    for handle in unread():
        html, saved = load_saved_product_page(handle)
        if html:
            pages[handle] = html
            logging.warning("Product page for %s could not be read today; using the copy saved %s "
                            "(Description / Rise / Inseam only - quantities always come fresh from GraphQL)",
                            handle, saved)
        else:
            logging.warning("Product page for %s could not be read and there is no saved copy: its Description "
                            "falls back to GraphQL and its Rise / Inseam come from the description only", handle)
    return pages


def fetch_collection_handles_json(collection: str) -> List[str]:
    """Backup list of a collection's handles from /collections/<c>/products.json
    (catches anything GraphQL misses). Paced like every site request. The old
    /collections/<c>.json returns the collection, not its products, so this
    step used to find nothing. Blocked pages are skipped, never fatal."""
    out: List[str] = []
    page = 1
    while True:
        path = f"/collections/{collection}/products.json?limit=250&page={page}"
        text = fetch_page(path, "json")
        if not text:
            if path in _PAGES_BLOCKED:
                logging.warning("Skipping the rest of the %s backup list (blocked); GraphQL still has its products",
                                collection)
            break
        try:
            products = (json.loads(text) or {}).get("products") or []
        except ValueError:
            logging.warning("%s was not JSON", path)
            break
        if not products:
            break
        out.extend([str(p.get("handle") or "") for p in products if p.get("handle")])
        page += 1
    return out


def fetch_collection_products_graphql(collection: str) -> List[dict]:
    headers = {"X-Shopify-Storefront-Access-Token": graphql_token(), "Content-Type": "application/json"}
    cursor: Optional[str] = None
    out: List[dict] = []
    while True:
        resp = post_with_retry(
            GRAPHQL_ENDPOINT, headers=headers,
            payload={"query": GRAPHQL_COLLECTION_QUERY, "variables": {"handle": collection, "cursor": cursor}},
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(data["errors"])
        node = (((data.get("data") or {}).get("collection") or {}).get("products") or {})
        batch = node.get("nodes") or []
        out.extend(batch)
        info = node.get("pageInfo") or {}
        if not info.get("hasNextPage"):
            break
        cursor = info.get("endCursor")
        if not cursor:
            break
    return out


def fetch_product_by_handle(handle: str) -> Optional[dict]:
    headers = {"X-Shopify-Storefront-Access-Token": graphql_token(), "Content-Type": "application/json"}
    resp = post_with_retry(
        GRAPHQL_ENDPOINT, headers=headers,
        payload={"query": GRAPHQL_PRODUCT_QUERY, "variables": {"handle": handle}},
    )
    resp.raise_for_status()
    data = resp.json()
    return ((data.get("data") or {}).get("product") or None)


def option_value(variant: dict, keys: Sequence[str]) -> str:
    for opt in variant.get("selectedOptions") or []:
        name = normalize_text(str(opt.get("name") or ""))
        if name in {normalize_text(k) for k in keys}:
            return str(opt.get("value") or "")
    return ""


def parse_pdp_details(handle: str, html: str) -> Dict[str, Any]:
    """Details text + Kiwi size-chart Rise/Inseam from a product page already
    fetched by fetch_all_product_pages(). Empty html -> empty results."""
    if not html:
        return {"details_text": "", "rise_map": {}, "inseam_map": {}}
    soup = BeautifulSoup(html, "lxml")
    if not soup.find():
        soup = BeautifulSoup(html, "html.parser")
    blocks = soup.select("[id^='Details-Content-content_block_'] > div")
    parts: List[str] = []
    for block in blocks:
        lis = block.find_all("li")
        if lis:
            parts.extend([li.get_text(" ", strip=True) for li in lis if li.get_text(" ", strip=True)])
        else:
            txt = block.get_text(" ", strip=True)
            if txt:
                parts.append(txt)
    txt = " ".join(parts)
    txt = re.sub(r"\s+", " ", txt).strip()
    kiwi_fields = extract_kiwi_fields(html)
    rise_map: Dict[str, str] = {}
    inseam_map: Dict[str, str] = {}
    if kiwi_fields:
        rise_map, inseam_map = fetch_kiwi_measurements(kiwi_fields, handle)
    return {"details_text": txt, "rise_map": rise_map, "inseam_map": inseam_map}


def extract_kiwi_fields(html: str) -> Optional[Dict[str, str]]:
    block = re.search(r"KiwiSizing\.data\s*=\s*\{(.*?)\};\s*</script>", html, flags=re.IGNORECASE | re.DOTALL)
    if not block:
        return None
    section = block.group(1)
    out: Dict[str, str] = {}
    for key in ("collections", "tags", "product", "vendor", "type", "title"):
        match = re.search(rf"{key}:\s*\"(.*?)\"", section, flags=re.IGNORECASE | re.DOTALL)
        if match:
            out[key] = match.group(1).replace("\\/", "/")
    if not out.get("product"):
        return None
    out["shop"] = "staud-clothing.myshopify.com"
    return out


def fetch_kiwi_measurements(kiwi_fields: Dict[str, str], handle: str) -> tuple[Dict[str, str], Dict[str, str]]:
    try:
        resp = None
        for attempt in range(3):
            resp = SESSION.get(
                "https://app.kiwisizing.com/kiwiSizing/api/getSizingChart",
                params={**kiwi_fields, "url": f"https://staud.clothing/products/{handle}"},
                timeout=45,
            )
            if resp.status_code in {429} | TRANSIENT_STATUSES and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            break
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        logging.warning("Kiwi sizing request failed for %s: %s", handle, exc)
        return {}, {}

    first_word = normalize_text(handle).split()[0] if normalize_text(handle) else ""
    rise_by_size: Dict[str, str] = {}
    inseam_by_size: Dict[str, str] = {}
    for sizing in payload.get("sizings") or []:
        for table in (sizing.get("tables") or {}).values():
            rows = table.get("data") or []
            size_cols = find_size_row(rows, first_word)
            if not size_cols:
                continue
            rise_vals = find_measurement_row(rows, "RISE")
            inseam_vals = find_measurement_row(rows, "INSEAM")
            if rise_vals:
                for size, raw in zip(size_cols, rise_vals):
                    value = normalize_measurement(raw)
                    n_size = normalize_size_token(size)
                    if n_size and value and n_size not in rise_by_size:
                        rise_by_size[n_size] = value
            if inseam_vals:
                for size, raw in zip(size_cols, inseam_vals):
                    value = normalize_measurement(raw)
                    n_size = normalize_size_token(size)
                    if n_size and value and n_size not in inseam_by_size:
                        inseam_by_size[n_size] = value
    return rise_by_size, inseam_by_size


def find_size_row(rows: Sequence[Any], first_word: str) -> List[str]:
    for row in rows:
        values = [str((cell or {}).get("value") or "").strip() for cell in row]
        if not values:
            continue
        header = normalize_text(values[0])
        data = [v.strip() for v in values[1:] if str(v).strip()]
        if not data:
            continue
        numeric_like = sum(1 for v in data if re.fullmatch(r"\d{1,2}", v))
        if not first_word or header.split()[:1] == [first_word]:
            if numeric_like >= max(4, len(data) // 2):
                return data
    for row in rows:
        values = [str((cell or {}).get("value") or "").strip() for cell in row]
        data = [v.strip() for v in values[1:] if str(v).strip()]
        if data and sum(1 for v in data if re.fullmatch(r"\d{1,2}", v)) >= max(4, len(data) // 2):
            return data
    return []


def find_measurement_row(rows: Sequence[Any], label: str) -> List[str]:
    for row in rows:
        values = [str((cell or {}).get("value") or "").strip() for cell in row]
        if not values:
            continue
        header = normalize_text(values[0])
        if header == normalize_text(label):
            return [v.strip() for v in values[1:]]
    return []


def normalize_size_token(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if re.fullmatch(r"\d+\.0+", raw):
        return raw.split(".", 1)[0]
    return raw


def is_filtered(handle: str, title: str, product_type: str) -> bool:
    h = normalize_text(handle)
    if "wally" in h and "boot" in h:
        return True
    if any(normalize_text(x) in h for x in FILTER_HANDLE_SUBSTRINGS):
        return True
    t = normalize_text(title)
    if any(word in t.split() for word in FILTER_TITLE_WORDS):
        return True
    pt = normalize_text(product_type)
    if pt != ALLOWED_PRODUCT_TYPE:
        return True
    if any(bad in pt for bad in FILTER_PRODUCT_TYPE_WORDS):
        return True
    return False


def parse_inseam(desc: str, option3: str) -> str:
    if option3:
        num = parse_number_with_fraction(option3)
        if num:
            v = float(num)
            if v > 38:
                return f"{v / 2.54:.3f}".rstrip("0").rstrip(".")
            return num
    opt_match = re.search(r"inseam\s*options?\s*[:\-]?", desc, flags=re.IGNORECASE)
    if opt_match:
        snippet = desc[opt_match.end(): opt_match.end() + 120]
        stop = re.search(r"(?:\bfit\b|\bmodel\b|\bcontact\b|\bcare\b|\bfabrication\b|\brise\b|[.;])", snippet, flags=re.IGNORECASE)
        if stop:
            snippet = snippet[:stop.start()]
        tokens = re.findall(r"\d+(?:\.\d+)?(?:\s+\d+/\d+|/[0-9]+|\s*[¼½¾])?", snippet)
        values = [normalize_measurement(tok) for tok in tokens if normalize_measurement(tok)]
        numeric_values: List[float] = []
        for v in values:
            try:
                numeric_values.append(float(v))
            except Exception:
                pass
        if numeric_values:
            return str(int(min(numeric_values))) if min(numeric_values).is_integer() else str(min(numeric_values))

    patterns = [
        r"(?:inseam options|inseam|inleg length|inleg)\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+|\s*[¼½¾])?)\s*\"?",
        r"([0-9]+(?:\.[0-9]+)?)\s*cm\s*/\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+|\s*[¼½¾])?)\s*\"",
    ]
    for pat in patterns:
        m = re.search(pat, desc, flags=re.IGNORECASE)
        if m:
            if len(m.groups()) > 1:
                return parse_number_with_fraction(m.group(2))
            return parse_number_with_fraction(m.group(1))
    return ""


def parse_rise(desc: str) -> str:
    m = re.search(r"rise\s*[:\-]?\s*(?:mid|low|high|ultra high|ultra low)?\s*[,\-(]?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+|\s*[¼½¾])?)", desc, flags=re.IGNORECASE)
    return parse_number_with_fraction(m.group(1)) if m else ""


def determine_rise_label(title: str, handle: str, desc: str, tags: str) -> str:
    if contains_any(title, ["super low rise", "ultra low rise", "super low waist", "ultra low waist"]): return "Ultra Low"
    if contains_any(title, ["super high rise", "ultra high rise", "super high waist", "ultra high waist"]): return "Ultra High"
    if contains_any(title, ["mid rise"]): return "Mid"
    if contains_any(title, ["low rise"]): return "Low"
    if contains_any(title, ["high rise"]): return "High"
    if contains_any(handle, ["slr", "ulr", "slw", "ulw", "superlowrise", "ultralowrise"]): return "Ultra Low"
    if contains_any(handle, ["shr", "uhr", "shw", "uhw", "superhighrise", "ultrahighrise"]): return "Ultra High"
    if contains_any(handle, ["-mr-", "-mw-", "mid rise", "mid waist"]): return "Mid"
    if contains_any(handle, ["-lr-", "-lw-", "low rise", "low waist"]): return "Low"
    if contains_any(handle, ["-hr-", "-hw-", "high rise", "high waist"]): return "High"
    if contains_any(desc, ["rise: super low", "rise: ultra low", "super low rise", "ultra low rise"]): return "Ultra Low"
    if contains_any(desc, ["rise: super high", "rise: ultra high", "super high rise", "ultra high rise"]): return "Ultra High"
    if contains_any(desc, ["rise: mid", "mid rise"]): return "Mid"
    if contains_any(desc, ["rise: low", "low rise", "low on the hip", "low on the waist"]): return "Low"
    if contains_any(desc, ["rise: high", "high rise", "high waist", "elevated waistline"]): return "High"
    if contains_any(tags, ["filter_rise_mid"]): return "Mid"
    if contains_any(tags, ["filter_rise_low"]): return "Low"
    if contains_any(tags, ["filter_rise_high"]): return "High"
    t = normalize_text(tags)
    if "mid" in t and "low" not in t and "high" not in t: return "Mid"
    if "low" in t and "mid" not in t and "high" not in t: return "Low"
    if "high" in t and "mid" not in t and "low" not in t: return "High"
    return ""


def determine_jean_style(title: str, desc: str, tags: str) -> str:
    if contains_any(title, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(title, ["tapered", "mom"]): return "Tapered"
    if contains_any(title, ["baggy"]): return "Baggy"
    if contains_any(title, ["flare"]): return "Flare"
    if contains_any(title, ["bootcut", "boot"]): return "Bootcut"
    if contains_any(title, ["skinny"]): return "Skinny"
    if contains_any(title, ["wide leg"]): return "Wide Leg"
    if contains_any(title, ["cigarette"]): return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["classic straight-leg", "slim straight", "classic straight fit", "cigarette"]): return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["relaxed straight-leg", "loose", "relaxed straight"]): return "Straight from Thigh"
    if contains_any(desc, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(desc, ["skinny"]): return "Skinny"
    if contains_any(desc, ["flare"]): return "Flare"
    if contains_any(desc, ["bootcut"]): return "Bootcut"
    if contains_any(desc, ["taper"]): return "Tapered"
    if contains_any(desc, ["wide leg", "palazzo"]): return "Wide Leg"
    if contains_any(desc, ["classic straight-leg", "slim straight", "classic straight fit", "cigarette"]): return "Straight from Knee"
    if contains_any(desc, ["relaxed fit with a 90s inspired"]): return "Straight from Thigh"
    if contains_any(desc, ["baggy", "relaxed jean", "relaxed fit", "loose fit"]): return "Baggy"
    if contains_any(tags, ["filter_style_barrel", "barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(tags, ["filter_style_skinny", "filter_style_superskinny", "skinny"]): return "Skinny"
    if contains_any(tags, ["filter_style_flare", "flare"]): return "Flare"
    if contains_any(tags, ["filter_style_boot", "bootcut", "boot"]): return "Bootcut"
    if contains_any(tags, ["taper"]): return "Tapered"
    if contains_any(tags, ["filter_style_wide", "wide leg", "wide", "palazzo"]): return "Wide Leg"
    if contains_any(tags, ["filter_style_cigarette", "cigarette"]): return "Straight from Knee"
    if contains_any(tags, ["straight"]) and contains_any(tags, ["baggy", "relaxed", "loose"]): return "Straight from Thigh"
    if contains_any(tags, ["baggy", "loose", "relaxed"]): return "Baggy"
    return ""


def determine_inseam_style(title: str, handle: str, desc: str, tags: str, jean_style: str, inseam: str) -> str:
    if jean_style in {"Skinny", "Tapered"} and contains_any(title, ["ankle"]): return "Full Length"
    if contains_any(title, ["ankle"]): return "Ankle"
    if contains_any(title, ["crop"]): return "Cropped"
    if jean_style in {"Skinny", "Tapered"} and contains_any(handle, ["ankle"]): return "Full Length"
    if contains_any(handle, ["ankle"]): return "Ankle"
    if contains_any(handle, ["crop"]): return "Cropped"
    if contains_any(handle, ["full length"]): return "Full Length"
    if jean_style in {"Skinny", "Tapered", "Straight from Knee"} and contains_any(desc, ["ankle-grazing", "ankle grazing length", "cropped at the ankle", "drape around the ankle", "hit at the ankle bone", "ankle length", "ankle jean", "tapers to a slim ankle"]): return "Full Length"
    if contains_any(desc, ["ankle-grazing", "ankle grazing length", "cropped at the ankle", "drape around the ankle", "hit at the ankle bone", "ankle length", "ankle jean"]): return "Ankle"
    if contains_any(desc, ["stack stylishly above the ankle", "stack at the ankle", "full length", "full-length", "full length at the ankles", "extended inseam", "stack at the hem", "pooling over"]): return "Full Length"
    if contains_any(desc, ["crop length", "crop-length", "cropped"]): return "Cropped"
    try:
        inseam_num = float(inseam) if inseam else None
    except Exception:
        inseam_num = None
    n = normalize_text(tags)
    if ("full" in n and "ankle" in n) or ("full" in n and "crop" in n and inseam_num and inseam_num >= 30): return "Full Length"
    if "ankle" in n and jean_style in {"Skinny", "Tapered", "Straight from Knee"}: return "Full Length"
    if "full" in n and "ankle" not in n and "crop" not in n: return "Full Length"
    if "ankle" in n and "full" not in n and "crop" not in n: return "Ankle"
    if "crop" in n and "full" not in n and "ankle" not in n: return "Cropped"
    return ""


# Jean Style groupings used by the Inseam Label rules. "Straight from
# Knee/Thigh" and "Boyfriend" are included per the rule table even though
# determine_jean_style() above doesn't currently produce them (it only
# emits "Straight from Knee" and "Straight from Thigh" separately, and
# never "Boyfriend") -- kept for forward-compatibility, effectively
# unreachable today. "Taper" (table spelling) and "Tapered" (this
# scraper's actual output) are both included defensively.
INSEAM_LABEL_WIDE_STYLES = {
    "Straight from Knee/Thigh", "Bootcut", "Barrel", "Wide Leg",
    "Boyfriend", "Baggy", "Flare", "Straight from Thigh",
}
INSEAM_LABEL_NARROW_STYLES = {"Taper", "Tapered", "Skinny", "Straight from Knee"}


def size_length_suffix(size: str) -> str:
    """
    The trailing LENGTH letter of a size, e.g. "30L" -> "L", "27P" -> "P",
    "33.5 T" -> "T". Returns "" when there is no length suffix.

    A digit is required immediately before the letter: STAUD also sells
    alpha-sized denim (XS/S/M/L/XL), where a trailing "L" is the size
    itself, not a length. Without that guard, size "L" and "XL" read as
    Long -- which mislabelled the alpha-sized NOAH JEAN styles, whose
    sizes all share one inseam.
    """
    m = re.fullmatch(r".*\d\s*([A-Za-z])", (size or "").strip())
    return m.group(1).upper() if m else ""


def determine_inseam_label_single(product_title: str, size: str, jean_style: str, inseam: str) -> str:
    """
    Inseam Label rule for styles with only ONE inseam option (no genuine
    Regular/Long split to detect). Per spec:
      1. Product title contains "Petite", or size ends in "P" -> Petite
      2. Wide-leg-family jean style + inseam >= 33 -> Long
      3. Narrow-family jean style + inseam >= 30 -> Long
      4. Otherwise -> Regular
    Size suffixes are read via size_length_suffix(), so an alpha size like
    "L"/"XL" is not mistaken for a length.
    """
    if contains_any(product_title, ["petite"]) or size_length_suffix(size) == "P":
        return "Petite"

    if contains_any(product_title, ["long"]) or size_length_suffix(size) == "L":
        return "Long"

    if contains_any(product_title, ["Tall"]) or size_length_suffix(size) == "T":
        return "Long"
    try:
        n = float(inseam) if inseam else None
    except (TypeError, ValueError):
        n = None
    if n is not None:
        if jean_style in INSEAM_LABEL_WIDE_STYLES and n >= 33:
            return "Long"
        if jean_style in INSEAM_LABEL_NARROW_STYLES and n >= 30:
            return "Long"
    return "Regular"


def determine_inseam_label_multi(inseam_style: str, jean_style: str, inseam: str) -> str:
    """
    Inseam Label rule for styles with MULTIPLE distinct inseam options
    (a genuine Regular/Long — or Petite/Regular/Long — split exists).
    Returns "" (never guesses) when:
      - inseam is missing/unparseable,
      - jean_style isn't in either defined group, or
      - the (inseam_style, jean_style-group) combination has no rule.
        Narrow styles (Taper/Skinny/Straight from Knee) intentionally have
        no "Ankle" row in the spec -- Carrie confirmed narrow styles should
        only ever resolve to Cropped or Full Length, so this case is
        expected to be unreachable in practice, not a gap to guess around.
    """
    try:
        n = float(inseam) if inseam else None
    except (TypeError, ValueError):
        n = None
    if n is None or not inseam_style:
        return ""

    if jean_style in INSEAM_LABEL_WIDE_STYLES:
        if inseam_style == "Cropped":
            if n <= 25: return "Petite"
            if n <= 27: return "Regular"
            return "Long"
        if inseam_style == "Ankle":
            if 25 < n < 28: return "Petite"
            if 28 <= n < 30: return "Regular"
            if n >= 30: return "Long"
            return ""
        if inseam_style == "Full Length":
            if 28 <= n < 30: return "Petite"
            if 30 <= n < 33: return "Regular"
            if n >= 33: return "Long"
            return ""
        return ""

    if jean_style in INSEAM_LABEL_NARROW_STYLES:
        if inseam_style == "Cropped":
            if n <= 26: return "Petite"
            if n <= 27: return "Regular"
            return "Long"
        if inseam_style == "Full Length":
            if 26 < n <= 27: return "Petite"
            if 27 < n <= 29: return "Regular"
            if n > 29: return "Long"
            return ""
        # Intentionally no "Ankle" rule for narrow styles -- see docstring.
        return ""

    return ""


def multi_inseam_check(style_id: str, inseam: str) -> str:
    """
    Step 1 of the multi-inseam spec: Multi_inseam_check is Style Id
    concatenated with Inseam (e.g. 8435253805229 + "30" ->
    "843525380522930"). Internal only -- never written to the CSV.
    """
    return f"{(style_id or '').strip()}{(inseam or '').strip()}"


def _check_sort_key(inseam: str) -> tuple:
    """
    Sort key ranking one style's Multi_inseam_check values smallest ->
    largest. Because every check in a style shares the same Style Id
    prefix, ranking the checks is ranking their inseams -- so we compare
    the inseam itself rather than the concatenated digits. Comparing the
    concatenation numerically would misrank a style mixing whole and
    fractional inseams (e.g. "...34" is 15 digits, "...33.5" is 16, so
    33.5 would sort above 34). Unparseable inseams sort last.
    """
    try:
        return (0, Decimal((inseam or "").strip()))
    except Exception:  # noqa: BLE001
        return (1, Decimal(0), (inseam or "").strip())


def add_product_descriptor(product: str, word: str) -> str:
    """
    Insert `word` ("PETITE"/"LONG") into Product just before the "|"
    separator, e.g. "LEO JEAN | BLUE" -> "LEO JEAN LONG | BLUE".
    No-op if the pre-"|" portion already carries that word, or if there is
    nothing to add. Products without a "|" get the word appended.
    """
    if not word or not product:
        return product
    head, sep, tail = product.partition("|")
    if contains_any(head, [word]):
        return product
    head = f"{head.strip()} {word}".strip()
    if not sep:
        return head
    return f"{head} | {tail.strip()}".strip()


def apply_multi_inseam_rules(rows: List[Dict[str, Any]]) -> None:
    """
    Multi-inseam Product / Inseam Label rules (2026-08-21 spec). Runs AFTER
    the existing Product and Inseam Label rules and revises their output in
    place.

    Step 1: build Multi_inseam_check (Style Id + Inseam) and reduce it to
            the unique values per Style Id. Internal only -- not a CSV
            column.
    Step 2: a Style Id with more than one unique Multi_inseam_check is
            multi_inseam. Rows with a blank Inseam don't count as an inseam
            option (their check would just be the bare Style Id).
    Step 3: revise Product -- add "PETITE"/"LONG" before the "|", first
            from Size (3A), otherwise from the row's rank among its style's
            inseams (3B).
    Step 4: revise Inseam Label -- Regular override (4A) and the
            multi_inseam rank labels (4B), but only for rows that did NOT
            already resolve as Petite/Long/Tall from the ORIGINAL product
            title or the size suffix, which stay locked.

    Styles with 4+ distinct inseams are left alone: the spec defines rank
    rules for 2 and 3 inseams only, and guessing a middle-rank scheme
    beyond that would invent behavior rather than implement it.
    """
    checks_by_style: Dict[str, Dict[str, str]] = defaultdict(dict)
    for r in rows:
        inseam = (r["Inseam"] or "").strip()
        if not inseam:
            continue
        # Step 1: keyed by check, so duplicates collapse to unique values.
        checks_by_style[r["Style Id"]][multi_inseam_check(r["Style Id"], inseam)] = inseam

    ranked_by_style: Dict[str, List[str]] = {
        sid: sorted(checks, key=lambda c: _check_sort_key(checks[c]))
        for sid, checks in checks_by_style.items()
    }

    for r in rows:
        original_product = r["Product"] or ""
        # Length suffix only -- an alpha size ("L", "XL") is not a length.
        size_suffix = size_length_suffix(r["Size"])
        ranked = ranked_by_style.get(r["Style Id"], [])
        multi_inseam = len(ranked) > 1  # Step 2
        check = multi_inseam_check(r["Style Id"], r["Inseam"]) if (r["Inseam"] or "").strip() else ""

        title_is_petite = contains_any(original_product, ["petite"])
        title_is_long = contains_any(original_product, ["long"])
        title_is_tall = contains_any(original_product, ["tall"])

        # ---- Step 3A: Product descriptor from Size ----
        descriptor = ""
        if size_suffix == "P":
            descriptor = "PETITE"
        elif size_suffix in {"L", "T"}:
            descriptor = "LONG"

        # ---- Step 3B: Product descriptor from multi_inseam rank ----
        # Size (3A) wins, and a title that already declares Petite/Long/Tall
        # is left alone rather than being given a contradicting descriptor.
        if not descriptor and multi_inseam and check and not (title_is_petite or title_is_long or title_is_tall):
            if len(ranked) == 2:
                if check == ranked[-1]:
                    descriptor = "LONG"
            elif len(ranked) == 3:
                if check == ranked[-1]:
                    descriptor = "LONG"
                elif check == ranked[0]:
                    descriptor = "PETITE"

        r["Product"] = add_product_descriptor(original_product, descriptor)

        # ---- Step 4: Inseam Label ----
        # Locked: anything that already resolved Petite/Long/Tall from the
        # ORIGINAL title or the size suffix (i.e. as evaluated before Step 3
        # rewrote Product) keeps whatever Inseam Label it has.
        locked = (
            title_is_petite or size_suffix == "P"
            or title_is_long or size_suffix == "L"
            or title_is_tall or size_suffix == "T"
        )
        if locked:
            continue

        # Part 4A
        if contains_any(original_product, ["regular"]) or size_suffix == "R":
            r["Inseam Label"] = "Regular"

        # Part 4B -- rank within the style overrides jean style / inseam value
        if multi_inseam and check:
            if len(ranked) == 2:
                r["Inseam Label"] = "Long" if check == ranked[-1] else "Regular"
            elif len(ranked) == 3:
                if check == ranked[-1]:
                    r["Inseam Label"] = "Long"
                elif check == ranked[0]:
                    r["Inseam Label"] = "Petite"
                else:
                    r["Inseam Label"] = "Regular"


def apply_style_name_editing_rules(rows: List[Dict[str, Any]]) -> None:
    """
    Style Name editing rules, Step 4 (applied in order, 2026-08-03 spec).
    Mutates `rows` in place.

    Rule 1: consolidate near-duplicate Style Names -- e.g. the brand
    slightly renamed a style over time so it no longer matches its
    siblings. Within products sharing the same first Style-Name word,
    if names disagree, adopt the MOST FREQUENT name for any product
    whose Jean Style matches the dominant Jean Style of that most-
    frequent name's group. Products with "MATERNITY" in the title are
    exempt (their Style Name is never touched by this rule).

    Rule 2: if a Style Name resolved to a single word:
      a. look for other (post-Rule-1) Style Names starting with that
         word whose Jean Style matches this product's -- if found,
         adopt the most common such name;
      b. else if no other Style Name starts with that word, append
         the first word of this product's Jean Style;
      c. else (Jean Style is blank) leave the single word as-is.
      NOTE: if candidates exist in (a) but none share this product's
      Jean Style, we leave the name as-is rather than falling through
      to (b) -- the spec doesn't define that case, and guessing here
      risked bolting an unrelated Jean Style word onto a real match
      that just didn't clear the Jean Style bar.

    Both rules operate once per PRODUCT (Style Id), then the resolved
    name is propagated to every variant row of that product.
    """
    by_product_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_product_id[r["Style Id"]].append(r)
    reps: Dict[str, Dict[str, Any]] = {pid: grp[0] for pid, grp in by_product_id.items()}

    # Rule 1 — only considers MULTI-word names. Single-word names are
    # Rule 2's job; if Rule 1 grouped by first word without this
    # restriction, a genuine single-word style like "FELIX" would get
    # swallowed into a merge with an unrelated "FELIX WIDE" simply for
    # sharing a first word, before Rule 2 ever got a chance to run its
    # own (different) single-word logic on it.
    first_word_groups: Dict[str, List[str]] = defaultdict(list)
    for pid, rep in reps.items():
        sn = rep["Style Name"]
        if sn and " " in sn:
            first_word_groups[sn.split(" ")[0]].append(pid)

    for fw, pids in first_word_groups.items():
        names = [reps[pid]["Style Name"] for pid in pids]
        if len(set(names)) <= 1:
            continue  # already consistent, nothing to consolidate
        most_common_name, _ = Counter(names).most_common(1)[0]
        dominant_js_counts = Counter(
            reps[pid]["Jean Style"] for pid in pids
            if reps[pid]["Style Name"] == most_common_name and reps[pid]["Jean Style"]
        )
        dominant_js = dominant_js_counts.most_common(1)[0][0] if dominant_js_counts else None
        for pid in pids:
            rep = reps[pid]
            if rep["Style Name"] == most_common_name:
                continue
            if "MATERNITY" in (rep["Product"] or "").upper():
                continue
            if dominant_js and rep["Jean Style"] == dominant_js:
                rep["Style Name"] = most_common_name

    # Rule 2 (recompute candidates AFTER Rule 1's consolidation)
    all_names_after_rule1 = [rep["Style Name"] for rep in reps.values() if rep["Style Name"]]
    for pid, rep in reps.items():
        sn = (rep["Style Name"] or "").strip()
        if not sn or " " in sn:
            continue  # not a single word
        candidates = [n for n in all_names_after_rule1 if n != sn and n.split(" ")[0] == sn]
        if candidates:
            candidate_pids = [p for p in reps
                              if reps[p]["Style Name"] in candidates
                              and reps[p]["Jean Style"] == rep["Jean Style"]]
            if candidate_pids:
                matched_names = Counter(reps[p]["Style Name"] for p in candidate_pids)
                rep["Style Name"] = matched_names.most_common(1)[0][0]
            # else: candidates exist but none share this Jean Style --
            # leave as-is (see note above).
        elif rep["Jean Style"]:
            rep["Style Name"] = f"{sn} {rep['Jean Style'].split(' ')[0]}".upper()
        # else: Jean Style blank -> leave the single word as-is.

    for pid, grp in by_product_id.items():
        resolved = reps[pid]["Style Name"]
        for r in grp:
            r["Style Name"] = resolved


def determine_color_standardized(tags: str, color: str) -> str:
    t = normalize_text(f"{tags} {color}")
    if "animal print" in t or "leopard" in t or "snake" in t: return "Animal Print"
    if "colorgroup blue" in t or "filter color blue" in t or "blue" in t: return "Blue"
    if "colorgroup black" in t or "filter color black" in t or "black" in t: return "Black"
    if "white" in t or "ecru" in t: return "White"
    if "grey" in t or "gray" in t: return "Grey"
    if "green" in t: return "Green"
    if "brown" in t or "tan" in t or "beige" in t or "khaki" in t: return "Tan"
    if "red" in t or "wine" in t or "burgundy" in t: return "Red"
    if "pink" in t: return "Pink"
    if "purple" in t: return "Purple"
    if "yellow" in t: return "Yellow"
    return ""


def determine_stretch(desc: str) -> str:
    if contains_any(desc, ["rigid"]): return "Rigid"
    if contains_any(desc, ["super stretch"]): return "High Stretch"
    if contains_any(desc, ["hint of stretch"]): return "Low Stretch"
    if contains_any(desc, ["stretch", "some stretch"]): return "Medium Stretch"
    return ""


def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="STAUD inventory scraper")
    parser.add_argument("--browser", action="store_true",
                        help="read product pages through a real browser from the start")
    args = parser.parse_args(argv)
    configure_logging()
    started = time.monotonic()
    products_by_handle: Dict[str, dict] = {}
    json_handles: set[str] = set()
    for c in COLLECTION_HANDLES:
        for p in fetch_collection_products_graphql(c):
            h = str(p.get("handle") or "")
            if h:
                products_by_handle[h] = p
        json_handles.update(fetch_collection_handles_json(c))

    missing = sorted(h for h in json_handles if h and h not in products_by_handle)
    for h in missing:
        p = fetch_product_by_handle(h)
        if p:
            products_by_handle[h] = p

    kept = [
        (handle, product) for handle, product in products_by_handle.items()
        if not is_filtered(handle, str(product.get("title") or ""), str(product.get("productType") or ""))
    ]
    logging.info("%s products found, %s jeans kept after filtering", len(products_by_handle), len(kept))
    pdp_pages = fetch_all_product_pages([h for h, _ in kept], use_browser=args.browser)

    rows: List[Dict[str, Any]] = []
    for handle, product in kept:
        title = str(product.get("title") or "")
        ptype = str(product.get("productType") or "")

        graph_desc = str(product.get("description") or "")
        pdp = parse_pdp_details(handle, pdp_pages.get(handle, ""))
        details_text = pdp.get("details_text") or ""
        rise_by_size = pdp.get("rise_map") or {}
        inseam_by_size = pdp.get("inseam_map") or {}
        inseam_defaults = {v for v in inseam_by_size.values() if v}
        inseam_default = next(iter(inseam_defaults)) if len(inseam_defaults) == 1 else ""
        inseam_mode = ""
        if inseam_by_size:
            counts = Counter(v for v in inseam_by_size.values() if v)
            if counts:
                mode_value, mode_count = counts.most_common(1)[0]
                if mode_count / max(1, sum(counts.values())) >= 0.7:
                    inseam_mode = mode_value
        description = " ".join([x for x in [graph_desc, details_text] if x]).strip()
        tags = ", ".join(product.get("tags") or [])
        style = style_name(title)
        rise = parse_rise(description)
        for variant in ((product.get("variants") or {}).get("nodes") or []):
            option1 = option_value(variant, ["color", "option1"]) or ""
            option2 = option_value(variant, ["size", "option2"]) or ""
            option3 = option_value(variant, ["inseam", "length", "option3"]) or ""
            option2_key = normalize_size_token(option2)
            inseam = parse_inseam(description, option3)
            if option2_key in inseam_by_size:
                inseam = inseam_by_size[option2_key]
            elif inseam_default:
                inseam = inseam_default
            if inseam_mode:
                inseam = inseam_mode
            jean_style = determine_jean_style(title, description, tags)
            inseam_style = determine_inseam_style(title, handle, description, tags, jean_style, inseam)
            rise_label = determine_rise_label(title, handle, description, tags)
            sku_brand = str(variant.get("sku") or "")
            rise_for_row = rise_by_size.get(option2_key, rise)
            row = {
                "Style Id": strip_gid(str(product.get("id") or ""), "gid://shopify/Product/"),
                "Handle": handle,
                "Published At": format_date(product.get("publishedAt")),
                "Created At": format_date(product.get("createdAt")),
                "Product": title.upper(),
                "Style Name": style,
                "Product Type": ptype.title() if ptype else "",
                "Tags": tags,
                "Vendor": product.get("vendor") or "",
                "Description": description,
                "Variant Title": f"{title.split('|')[0].strip().upper()} / {str(variant.get('title') or '').replace(' / ', ' / ')}".strip(" /"),
                "Color": option1,
                "Size": option2,
                "Rise": rise_for_row,
                "Inseam": inseam,
                "Price": to_price((variant.get("price") or {}).get("amount")),
                "Compare at Price": to_price((variant.get("compareAtPrice") or {}).get("amount")),
                "Available for Sale": str(bool(variant.get("availableForSale"))).upper(),
                "Quantity Available": variant.get("quantityAvailable") if variant.get("quantityAvailable") is not None else "",
                "Quantity of style": product.get("totalInventory") if product.get("totalInventory") is not None else "",
                "SKU - Shopify": strip_gid(str(variant.get("id") or ""), "gid://shopify/ProductVariant/"),
                "SKU - Brand": sku_brand,
                "Barcode": variant.get("barcode") or "",
                "Image URL": ((product.get("featuredImage") or {}).get("url") or ""),
                "SKU URL": product.get("onlineStoreUrl") or f"https://staud.clothing/products/{handle}",
                "Jean Style": jean_style,
                "Inseam Style": inseam_style,
                "Inseam Label": "",  # computed later, after Inseam Style gaps are filled
                "Rise Label": rise_label,
                "Color - Standardized": determine_color_standardized(tags, option1),
                "Stretch": determine_stretch(description),
            }
            rows.append(row)

    apply_style_name_editing_rules(rows)

    # style-level infer for missing rise/inseam style
    by_style: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_style[r["Style Name"]].append(r)
    for r in rows:
        if not r["Inseam Style"]:
            peers = [x for x in by_style[r["Style Name"]] if x["Inseam Style"] and x["Inseam"] == r["Inseam"]]
            if peers and len({x["Inseam Style"] for x in peers}) == 1:
                r["Inseam Style"] = peers[0]["Inseam Style"]
        if not r["Rise Label"]:
            peers = [x for x in by_style[r["Style Name"]] if x["Rise Label"] and x["Rise"] == r["Rise"]]
            if peers and len({x["Rise Label"] for x in peers}) == 1:
                r["Rise Label"] = peers[0]["Rise Label"]

    # fill remaining gaps for required downstream columns
    by_style_size_rise: Dict[tuple[str, str], str] = {}
    for r in rows:
        key = (r["Style Name"], r["Size"])
        if r["Rise"] and key not in by_style_size_rise:
            by_style_size_rise[key] = r["Rise"]

    # Determine, per style, how many DISTINCT inseam values genuinely
    # exist -- this decides which Inseam Label rule set applies (single-
    # inseam styles use title/size/jean-style heuristics; multi-inseam
    # styles use the Inseam Style + Inseam-value table).
    style_inseam_values: Dict[str, set] = defaultdict(set)
    for r in rows:
        try:
            v = float(r["Inseam"]) if r["Inseam"] else None
        except (TypeError, ValueError):
            v = None
        if v is not None:
            style_inseam_values[r["Style Name"]].add(v)

    for r in rows:
        if not r["Rise"]:
            r["Rise"] = by_style_size_rise.get((r["Style Name"], r["Size"]), "")
        if not r["Inseam Style"]:
            try:
                inseam_num = float(r["Inseam"]) if r["Inseam"] else None
            except Exception:
                inseam_num = None
            if inseam_num is not None:
                if inseam_num >= 30:
                    r["Inseam Style"] = "Full Length"
                elif inseam_num <= 28:
                    r["Inseam Style"] = "Cropped"
                else:
                    r["Inseam Style"] = "Ankle"
        if not r["Compare at Price"]:
            r["Compare at Price"] = r["Price"]
        if len(style_inseam_values.get(r["Style Name"], set())) > 1:
            r["Inseam Label"] = determine_inseam_label_multi(r["Inseam Style"], r["Jean Style"], r["Inseam"])
        else:
            r["Inseam Label"] = determine_inseam_label_single(r["Product"], r["Size"], r["Jean Style"], r["Inseam"])

    # Multi-inseam revisions to Product / Inseam Label (2026-08-21 spec).
    # Runs last so it revises the output of the rules above.
    apply_multi_inseam_rules(rows)

    # dedupe
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for r in rows:
        key = (r["Handle"], r["SKU - Shopify"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = OUTPUT_DIR / f"{BRAND}_{ts}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(deduped)
    logging.info("Rows written: %s", len(deduped))
    logging.info("CSV written: %s", out)
    logging.info("Finished in %s", fmt_duration(time.monotonic() - started))


if __name__ == "__main__":
    main()