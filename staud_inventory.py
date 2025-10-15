#!/usr/bin/env python3
"""
Inventory + catalog exporter for https://staud.clothing/ STAID denim collection.

Pulls the jeans collection feed for catalog level data and augments each variant
with inventory details from the per-product JSON endpoint.
Outputs a timestamped CSV alongside a simple run log.
"""

from __future__ import annotations

import csv
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from html import unescape
from typing import Any, Dict, List, Optional

import requests
from urllib.parse import urlparse, urlunparse

BASE_URL = "https://staud.clothing"
COLLECTION_PATH = "/collections/staud-jeans/products.json"

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR / "Output"
LOG_PATH = SCRIPT_DIR / "staud_run.log"

os.makedirs(OUTPUT_DIR, exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

HOST_FALLBACKS = {
    "staud.clothing": ["www.staud.clothing"],
    "www.staud.clothing": ["staud.clothing"],
}

TRANSIENT_STATUSES = {429, 500, 502, 503, 504}

DNS_ERROR_KEYWORDS = [
    "failed to resolve",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo failed",
    "nodename nor servname provided",
    "unreachable network",
    "connection aborted",
]

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Product",
    "Style Name",
    "Product Type",
    "Tags",
    "Vendor",
    "Description",
    "Variant Title",
    "Color",
    "Size",
    "Inseam",
    "Price",
    "Compare at Price",
    "Available for Sale",
    "Quantity Price Breaks",
    "Quantity Available",
    "Old Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
]


_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"User-Agent": USER_AGENT})
        # Ensure the container proxy does not interfere with outbound requests.
        s.trust_env = False
        _session = s
    return _session


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except OSError:
        pass


def iter_url_candidates(url: str) -> List[str]:
    parsed = urlparse(url)
    netloc = parsed.netloc
    candidates = [netloc]
    candidates.extend(HOST_FALLBACKS.get(netloc.lower(), []))
    seen: set[str] = set()
    results: List[str] = []
    for host in candidates:
        if not host:
            continue
        key = host.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(urlunparse(parsed._replace(netloc=host)))
    return results


def is_name_resolution_error(exc: Exception) -> bool:
    parts: List[str] = []
    current: Optional[BaseException] = exc
    visited: set[int] = set()
    while current and id(current) not in visited:
        visited.add(id(current))
        parts.append(str(current))
        parts.append(repr(current))
        current = getattr(current, "__cause__", None)
    combined = " ".join(parts).lower()
    return any(keyword in combined for keyword in DNS_ERROR_KEYWORDS)


def polite_get(
    url: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    backoff: float = 1.0,
) -> requests.Response:
    session = get_session()
    candidates = iter_url_candidates(url)
    last_exc: Optional[Exception] = None

    for idx, candidate in enumerate(candidates):
        for attempt in range(max_retries):
            try:
                resp = session.get(candidate, params=params, timeout=45)
            except requests.RequestException as exc:
                last_exc = exc
                if attempt == max_retries - 1:
                    log(f"[error] {candidate} exhausted retries due to {exc!r}")
                    break
                sleep_for = backoff * (2 ** attempt)
                log(
                    f"[retry] {candidate} due to error: {exc!r} -> sleeping {sleep_for:.1f}s"
                )
                time.sleep(sleep_for)
                continue

            if resp.status_code == 200:
                if candidate != url:
                    log(f"[fallback] succeeded via {candidate}")
                return resp

            if resp.status_code in TRANSIENT_STATUSES:
                sleep_for = backoff * (2 ** attempt)
                log(
                    f"[wait] {resp.status_code} on {candidate} -> sleeping {sleep_for:.1f}s"
                )
                time.sleep(sleep_for)
                continue

            resp.raise_for_status()

        if (
            idx + 1 < len(candidates)
            and last_exc is not None
            and is_name_resolution_error(last_exc)
        ):
            next_candidate = candidates[idx + 1]
            log(f"[retry] switching host for {url} -> {next_candidate}")
            continue
        break

    if last_exc:
        raise last_exc
    raise RuntimeError("polite_get retry loop exhausted without response")


def parse_collection_products() -> List[Dict[str, Any]]:
    all_products: List[Dict[str, Any]] = []
    seen_handles: set[str] = set()
    page = 1

    while True:
        params = {"limit": 250, "page": page}
        url = f"{BASE_URL}{COLLECTION_PATH}"
        log(f"[pull] collection page {page}")
        resp = polite_get(url, params=params)
        payload = resp.json()
        products = payload.get("products") or []

        if not products:
            log("[done] no products returned -> stop pagination")
            break

        new_products = [p for p in products if p.get("handle") not in seen_handles]
        for prod in new_products:
            seen_handles.add(prod.get("handle", ""))
        all_products.extend(new_products)

        log(
            f"[page] {page} -> {len(new_products)} new / {len(products)} total items"
        )

        if len(new_products) == 0:
            log("[done] all remaining handles were duplicates -> stop")
            break

        page += 1
        time.sleep(0.5)

    log(f"[summary] total unique products: {len(all_products)}")
    return all_products


def fetch_product_detail(handle: str) -> Dict[str, Any]:
    url = f"{BASE_URL}/products/{handle}.json"
    resp = polite_get(url)
    data = resp.json()
    product = data.get("product")
    if not isinstance(product, dict):
        raise ValueError(f"Unexpected product payload for handle {handle}")
    return product


def clean_html_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    unescaped = unescape(raw_html)
    # Remove script/style and HTML tags.
    unescaped = re.sub(r"<\s*(script|style)[^>]*>.*?<\s*/\s*\1\s*>", " ", unescaped, flags=re.I | re.S)
    text = re.sub(r"<[^>]+>", " ", unescaped)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def format_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (int, float)):
        number = float(value)
        if number > 999:  # treat as cents
            return f"${number / 100:.2f}"
        return f"${number:.2f}"

    text = str(value).strip()
    if not text:
        return ""
    if text.startswith("$"):
        return text

    cleaned = text.replace(",", "")
    try:
        number = float(cleaned)
    except Exception:
        return text

    if number > 999:  # treat as cents
        return f"${number / 100:.2f}"
    return f"${number:.2f}"


def parse_published_at(iso_string: Optional[str]) -> str:
    if not iso_string:
        return ""
    try:
        dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except Exception:
        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", iso_string)
        if match:
            y, m, d = match.groups()
            return f"{m}/{d}/{y[-2:]}"
        return ""


def style_name_from_title(title: str) -> str:
    if not title:
        return ""
    return title.split(" |")[0].strip()


def normalize_quantity_price_breaks(raw_value: Any) -> str:
    if raw_value in (None, ""):
        return ""
    if isinstance(raw_value, (list, dict)):
        try:
            return json.dumps(raw_value, ensure_ascii=False)
        except Exception:
            return str(raw_value)
    return str(raw_value)


def get_variant_image_url(
    variant: Dict[str, Any],
    product_images: List[Dict[str, Any]],
) -> str:
    featured = variant.get("featured_image")
    if isinstance(featured, dict) and featured.get("src"):
        return featured["src"]

    image_id = variant.get("image_id")
    if image_id and isinstance(product_images, list):
        for img in product_images:
            if str(img.get("id")) == str(image_id) and img.get("src"):
                return img["src"]

    if product_images:
        primary = product_images[0]
        if primary.get("src"):
            return primary["src"]
    return ""


def collect_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    products = parse_collection_products()

    for idx, product in enumerate(products, start=1):
        handle = product.get("handle", "")
        log(f"[detail] {idx}/{len(products)} handle={handle}")
        try:
            detail = fetch_product_detail(handle)
        except Exception as exc:
            log(f"[error] failed to fetch detail for {handle}: {exc!r}")
            continue

        variant_detail_map = {
            str(variant.get("id")): variant for variant in detail.get("variants", [])
        }
        images = detail.get("images") or []

        # Pre-compute style total quantity
        style_quantity_total = 0
        for v in variant_detail_map.values():
            qty = v.get("inventory_quantity")
            if isinstance(qty, (int, float)):
                style_quantity_total += int(qty)

        published_at = parse_published_at(product.get("published_at"))
        description = clean_html_text(product.get("body_html", ""))
        tags = product.get("tags")
        if isinstance(tags, list):
            tags_str = ", ".join(tags)
        else:
            tags_str = str(tags or "")

        for variant in product.get("variants", []):
            vid = str(variant.get("id"))
            detail_variant = variant_detail_map.get(vid, {})

            row = {
                "Style Id": product.get("id", ""),
                "Handle": handle,
                "Published At": published_at,
                "Product": product.get("title", ""),
                "Style Name": style_name_from_title(product.get("title", "")),
                "Product Type": product.get("product_type", ""),
                "Tags": tags_str,
                "Vendor": product.get("vendor", ""),
                "Description": description,
                "Variant Title": variant.get("title", ""),
                "Color": variant.get("option1", ""),
                "Size": variant.get("option2", ""),
                "Inseam": variant.get("option3", ""),
                "Price": format_price(variant.get("price")),
                "Compare at Price": format_price(variant.get("compare_at_price")),
                "Available for Sale": "TRUE" if variant.get("available") else "FALSE",
                "Quantity Price Breaks": normalize_quantity_price_breaks(
                    detail_variant.get("quantity_price_breaks")
                ),
                "Quantity Available": detail_variant.get("inventory_quantity", ""),
                "Old Quantity Available": detail_variant.get("old_inventory_quantity", ""),
                "Quantity of style": style_quantity_total,
                "SKU - Shopify": vid,
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": detail_variant.get("barcode", ""),
                "Image URL": get_variant_image_url(detail_variant, images),
                "SKU URL": f"{BASE_URL}/products/{handle}",
            }

            rows.append(row)

    return rows


def write_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("No data rows available to write")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"STAUD_{timestamp}.csv"
    path = os.path.join(OUTPUT_DIR, filename)

    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    log(f"[csv] wrote {len(rows)} rows to {path}")
    return path


def main() -> None:
    start = time.time()
    try:
        rows = collect_rows()
        if not rows:
            log("[warn] No rows collected. CSV will not be created.")
            return
        csv_path = write_csv(rows)
        elapsed = time.time() - start
        log(f"[done] Completed in {elapsed:.1f}s -> {csv_path}")
    except Exception as exc:
        log(f"[fatal] {exc!r}")
        raise


if __name__ == "__main__":
    main()