#!/usr/bin/env python3
"""Heuristic Searchspring token probe.

Given a list of brands and their Searchspring bundle URLs, attempt to
surface X-Shopify-Storefront-Access-Token strings (32 hex chars) by
scanning the referenced JavaScript assets and any hinted chunk files.

The script logs candidate tokens with context so a human can decide
whether a match is truly a Storefront access token.
"""
from __future__ import annotations

import dataclasses
import logging
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import ssl

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH = OUTPUT_DIR / "searchspring_token_probe.log"

# Create an SSL context that matches the rest of the tooling (disable cert checks).
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

LOGGER = logging.getLogger("searchspring_token_probe")


@dataclasses.dataclass
class BundleTarget:
    url: str
    referer: Optional[str] = None
    note: Optional[str] = None
    chunk_prefix: Optional[str] = None
    chunk_ids: Optional[List[int]] = None


@dataclasses.dataclass
class BrandConfig:
    name: str
    targets: List[BundleTarget]


# Brand-specific bundle URLs gathered from the exploratory prompt.
BRANDS: List[BrandConfig] = [
    BrandConfig(
        name="WarpWeft-reference",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/dkc5xr/bundle.js",
                referer="https://warpandweftworld.com/collections/jeans",
                note="SnapUI runtime",
                chunk_prefix="https://snapui.searchspring.io/dkc5xr/bundle.chunk.5a55f52f",
                chunk_ids=[129, 954, 178, 770, 339, 476, 974, 39, 184, 343, 158, 505, 326, 371],
            )
        ],
    ),
    BrandConfig(
        name="Redone-reference",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/w7x7sx/bundle.js",
                referer="https://shopredone.com/collections/denim",
                chunk_prefix="https://snapui.searchspring.io/w7x7sx/bundle.chunk.73a6b0cc",
                chunk_ids=[954, 178, 92, 804, 172, 379, 467, 400, 693, 339, 818, 473, 129, 433, 995, 116],
            )
        ],
    ),
    BrandConfig(
        name="DL1961-reference",
        targets=[
            BundleTarget(
                url="https://snapui.searchspring.io/8176gy/bundle.js",
                referer="https://dl1961.com/collections/women-view-all-fits",
                chunk_prefix="https://snapui.searchspring.io/8176gy/bundle.chunk.68794b24",
                chunk_ids=[129, 954, 178, 770, 339, 519, 137, 44, 75, 158, 505, 446, 371],
            )
        ],
    ),
    BrandConfig(
        name="GoodAmerican",
        targets=[
            BundleTarget(
                url="https://cdn.shopify.com/oxygen-v2/26935/12013/24631/2597642/build/_shared/chunk-4BYWHZPF.js",
                note="Oxygen chunk (initiator for Searchspring)",
            ),
            BundleTarget(
                url="https://www.goodamerican.com/api/searchspring?bgfilter.collection_handle=womens-jeans",
                note="Searchspring API payload",
            ),
        ],
    ),
    BrandConfig(
        name="Mother",
        targets=[
            BundleTarget(
                url="https://www.motherdenim.com/cdn/shop/t/136/assets/collection.js?v=74035425146881058231757530505",
                note="Theme bundle (Searchspring initiator)",
            ),
            BundleTarget(
                url="https://00svms.a.searchspring.io/api/search/search.json?siteId=00svms&resultsFormat=native&resultsPerPage=40&bgfilter.collection_name=Denim&page=1",
                note="Searchspring API payload",
            ),
        ],
    ),
    BrandConfig(
        name="Frame",
        targets=[
            BundleTarget(
                url="https://frame-store.com/cdn/shop/t/1321/assets/snap-bundle.js?v=42413924495642258221762816318",
                note="Theme bundle",
            ),
            BundleTarget(
                url="https://v1j77y.a.searchspring.io/api/search/search.json?siteId=v1j77y&resultsFormat=native&resultsPerPage=40&bgfilter.collection_id=914751490&page=1",
                note="Searchspring API payload",
            ),
        ],
    ),
]

HEX32 = re.compile(r"[0-9a-f]{32}")
TOKEN_CONTEXT_CHARS = 120


def configure_logging() -> None:
    LOGGER.setLevel(logging.INFO)
    handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    LOGGER.addHandler(console)


def fetch_text(target: BundleTarget) -> Tuple[Optional[str], Optional[bytes]]:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
    if target.referer:
        headers["Referer"] = target.referer
    req = Request(target.url, headers=headers)
    try:
        with urlopen(req, context=CTX, timeout=6) as resp:
            data = resp.read()
    except HTTPError as err:
        LOGGER.warning("%s (%s) HTTP %s", target.url, target.note or "bundle", err.code)
        return None, None
    except URLError as err:
        LOGGER.warning("%s (%s) URLError %s", target.url, target.note or "bundle", err.reason)
        return None, None
    try:
        text = data.decode("utf-8", "replace")
    except Exception:
        text = None
    return text, data


def scan_for_tokens(name: str, target: BundleTarget, text: Optional[str], data: Optional[bytes]) -> None:
    if not text and not data:
        return
    LOGGER.info("Scanning %s - %s", name, target.url)
    candidates: List[str] = []
    if text:
        candidates.extend(HEX32.findall(text))
        if "storefront" in text.lower():
            LOGGER.info("  found 'storefront' keyword")
        for keyword in ("storefrontToken", "accessToken", "X-Shopify-Storefront-Access-Token"):
            idx = text.find(keyword)
            if idx != -1:
                snippet = text[max(0, idx - TOKEN_CONTEXT_CHARS): idx + TOKEN_CONTEXT_CHARS]
                LOGGER.info("  context for %s: %s", keyword, snippet)
    elif data:
        candidates.extend(match.decode("ascii") for match in HEX32.findall(data.decode("latin-1", "ignore")))

    seen = set()
    for token in candidates:
        if token in seen:
            continue
        seen.add(token)
        if token.isdigit():
            continue
        LOGGER.info("  candidate token: %s", token)

    if target.chunk_prefix and target.chunk_ids:
        fetch_chunks(name, target)


def fetch_chunks(name: str, target: BundleTarget) -> None:
    prefix = target.chunk_prefix.rstrip(".")
    headers = {"User-Agent": "Mozilla/5.0"}
    if target.referer:
        headers["Referer"] = target.referer
    for cid in target.chunk_ids:
        chunk_url = f"{prefix}.{cid}.js"
        try:
            with urlopen(Request(chunk_url, headers=headers), context=CTX, timeout=4) as resp:
                chunk = resp.read().decode("utf-8", "replace")
        except Exception:
            continue
        if "token" in chunk or "storefront" in chunk.lower():
            idx = chunk.find("token")
            snippet = chunk[max(0, idx - TOKEN_CONTEXT_CHARS): idx + TOKEN_CONTEXT_CHARS]
            LOGGER.info("  [%s] chunk %s contains token string: %s", name, cid, snippet)
        for token in set(HEX32.findall(chunk)):
            LOGGER.info("  [%s] chunk %s candidate: %s", name, cid, token)


def main() -> None:
    configure_logging()
    for brand in BRANDS:
        LOGGER.info("===== %s =====", brand.name)
        for target in brand.targets:
            text, data = fetch_text(target)
            scan_for_tokens(brand.name, target, text, data)


if __name__ == "__main__":
    main()
