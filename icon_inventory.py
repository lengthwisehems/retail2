"""Icon Denim Los Angeles women's jeans inventory scraper."""

from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "icon_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "icon_run.log"

COLLECTION_URLS = [
    "https://icondenimlosangeles.com/collections/bottoms-women/products.json?sort_by=manual&filter.p.t.category=aa-1-12-4&limit=250&currency=USD",
    "https://icondenimlosangeles.com/collections/last-chance-women/products.json?sort_by=manual&filter.p.t.category=aa-1-12-4&limit=250&currency=USD",
]

HOST_FALLBACKS = (
    "https://icondenimlosangeles.com",
    "https://icondenim-los-angeles.myshopify.com",
)

GRAPHQL_HOSTS = HOST_FALLBACKS
GRAPHQL_PATH = "/api/2025-04/graphql.json"
STOREFRONT_TOKEN = "570f85534823ffaf20e1db06a130ff76"

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Created At",
    "Product",
    "Style Name",
    "Product Type",
    "Vendor",
    "Description",
    "Variant Title",
    "Color",
    "Size",
    "Price",
    "Compare at Price",
    "Available for Sale",
    "Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Inseam Style",
    "Stretch",
]

REQUEST_TIMEOUT = 30
MAX_RETRIES = 4


def configure_logging() -> logging.Logger:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    handlers: List[logging.Handler] = []
    selected_path: Optional[Path] = None

    try:
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        handlers.append(file_handler)
        selected_path = LOG_PATH
    except OSError:
        fallback_handler = logging.FileHandler(FALLBACK_LOG_PATH, encoding="utf-8")
        handlers.append(fallback_handler)
        selected_path = FALLBACK_LOG_PATH

    stream_handler = logging.StreamHandler()
    handlers.append(stream_handler)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )

    logger = logging.getLogger("icon_inventory")
    if selected_path and selected_path != LOG_PATH:
        logger.warning("Logging fell back to %s", selected_path)
    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return session


def iter_host_variants(url: str, hosts: Iterable[str]) -> Iterable[str]:
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(url)
    for host in hosts:
        base = urlparse(host)
        yield urlunparse(
            (
                parsed.scheme or base.scheme,
                base.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )


def request_with_backoff(
    session: requests.Session,
    url: str,
    method: str = "GET",
    **kwargs,
) -> requests.Response:
    backoff = 1.0
    for attempt in range(MAX_RETRIES):
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            if response.status_code in {429} or response.status_code >= 500:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            return response
        except requests.RequestException as exc:  # pragma: no cover - network
            logging.warning("Request %s %s failed (%s); retrying in %.1fs", method, url, exc, backoff)
            time.sleep(backoff)
            backoff *= 2
    raise RuntimeError(f"Failed to fetch {url}")


def fetch_json_with_hosts(session: requests.Session, url: str) -> Dict:
    for candidate in iter_host_variants(url, HOST_FALLBACKS):
        try:
            response = request_with_backoff(session, candidate)
            return response.json()
        except Exception as exc:  # pragma: no cover - network
            logging.warning("Error fetching %s: %s", candidate, exc)
    raise RuntimeError(f"Unable to fetch JSON for {url}")


def fetch_collection_products(session: requests.Session) -> Dict[int, Dict]:
    products: Dict[int, Dict] = {}
    for base_url in COLLECTION_URLS:
        page = 1
        while True:
            if "page=" in base_url:
                url = base_url
            else:
                sep = "&" if "?" in base_url else "?"
                url = f"{base_url}{sep}page={page}"
            data = fetch_json_with_hosts(session, url)
            batch = data.get("products", [])
            logging.info("Collection %s page %s -> %s products", base_url, page, len(batch))
            if not batch:
                break
            for product in batch:
                title = (product.get("title") or "").strip()
                if "jean" not in title.lower():
                    continue
                words = title.split()
                if not words or words[-1].lower() != "jeans":
                    continue
                pid = int(product["id"])
                existing = products.get(pid)
                if existing:
                    existing_published = existing.get("published_at") or existing.get("publishedAt")
                    new_published = product.get("published_at") or product.get("publishedAt")
                    if new_published and (not existing_published or new_published < existing_published):
                        products[pid] = product
                else:
                    products[pid] = product
            page += 1
            if len(batch) < data.get("limit", len(batch)):
                break
    logging.info("Total qualifying products: %s", len(products))
    return products


def fetch_storefront_details(session: requests.Session, handle: str) -> Optional[Dict]:
    query = """
    query ProductByHandle($handle: String!) {
      productByHandle(handle: $handle) {
        description
        totalInventory
        onlineStoreUrl
        variants(first: 250) {
          edges {
            node {
              id
              sku
              barcode
              availableForSale
              quantityAvailable
            }
          }
        }
      }
    }
    """
    variables = {"handle": handle}

    for host in GRAPHQL_HOSTS:
        url = host.rstrip("/") + GRAPHQL_PATH
        try:
            response = request_with_backoff(
                session,
                url,
                method="POST",
                json={"query": query, "variables": variables},
                headers={
                    "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            )
            payload = response.json()
            if payload.get("errors"):
                logging.warning("GraphQL errors for %s via %s: %s", handle, host, payload["errors"])
                continue
            return payload.get("data", {}).get("productByHandle")
        except Exception as exc:  # pragma: no cover - network
            logging.warning("GraphQL request for %s via %s failed: %s", handle, host, exc)
    logging.warning("No GraphQL data for handle %s", handle)
    return None


def clean_html_text(raw: Optional[str]) -> str:
    if not raw:
        return ""
    text = unescape(raw)
    result: List[str] = []
    inside = False
    for char in text:
        if char == "<":
            inside = True
            continue
        if char == ">":
            inside = False
            continue
        if not inside:
            result.append(char)
    return "".join(result).strip()


def extract_numeric_id(gid: Optional[str]) -> Optional[str]:
    if not gid:
        return None
    if gid.isdigit():
        return gid
    if "gid://" in gid:
        return gid.rsplit("/", 1)[-1]
    return gid


def pick_image(product: Dict, variant: Dict) -> str:
    featured = variant.get("featured_image") or {}
    src = featured.get("src")
    if src:
        return src
    images = product.get("images") or []
    if images:
        return images[0].get("src", "")
    return ""


def determine_stretch(description: str) -> str:
    if "rigid" in description.lower():
        return "Rigid"
    return ""


def determine_inseam_style(title: str) -> str:
    return "Cropped" if "crop" in title.lower() else ""


def _format_decimal(value: Decimal) -> str:
    quantized = value.quantize(Decimal("0.01"))
    result = format(quantized.normalize(), "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    return result


def format_price(raw: Optional[object]) -> str:
    if raw in (None, "", "null"):
        return ""

    if isinstance(raw, (int, float)):
        value = Decimal(str(raw))
        if isinstance(raw, int) and raw >= 100:
            # Shopify product JSON returns cents as integers.
            value = value / Decimal(100)
        return _format_decimal(value)

    text = str(raw).strip()
    if not text or text.lower() == "null":
        return ""

    try:
        value = Decimal(text)
    except InvalidOperation:
        return text

    if text.isdigit() and len(text) > 2:
        value = value / Decimal(100)
    return _format_decimal(value)


def format_date(raw: Optional[str]) -> str:
    if not raw:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue
    return raw


def assemble_rows(products: Dict[int, Dict], storefront_cache: Dict[str, Dict]) -> List[List[str]]:
    rows: List[List[str]] = []
    for product in products.values():
        handle = product.get("handle")
        if not handle:
            continue
        title = (product.get("title") or "").strip()
        style_name = title.split(" - ")[0].strip() if title else ""
        product_type = title.split()[-1] if title else ""
        vendor = product.get("vendor", "")
        published = product.get("published_at") or product.get("publishedAt")
        created = product.get("created_at") or product.get("createdAt")

        storefront = storefront_cache.get(handle) or {}
        description = clean_html_text(storefront.get("description"))
        quantity_style = storefront.get("totalInventory") or ""
        sku_url = storefront.get("onlineStoreUrl") or f"https://icondenimlosangeles.com/products/{handle}"
        stretch = determine_stretch(description)

        variant_lookup: Dict[str, Dict] = {}
        for edge in (storefront.get("variants") or {}).get("edges", []):
            node = edge.get("node", {})
            variant_id = extract_numeric_id(node.get("id"))
            if variant_id:
                variant_lookup[variant_id] = node

        for variant in product.get("variants", []):
            variant_id = str(variant.get("id"))
            gql_variant = variant_lookup.get(variant_id, {})
            sku_brand = gql_variant.get("sku", "")
            if sku_brand:
                sku_brand = " ".join(sku_brand.split())

            option1 = variant.get("option1")
            option2 = variant.get("option2")
            if option2 in (None, ""):
                color = ""
                size = option1 or ""
            else:
                color = option1 or ""
                size = option2 or ""

            row = [
                str(product.get("id", "")),
                handle,
                format_date(published),
                format_date(created),
                title,
                style_name,
                product_type,
                vendor,
                description,
                f"{title} - {variant.get('title', '')}",
                color,
                size,
                format_price(variant.get("price")),
                format_price(variant.get("compare_at_price")),
                str(variant.get("available", "")),
                str(gql_variant.get("quantityAvailable", "")),
                str(quantity_style),
                variant_id,
                sku_brand,
                str(gql_variant.get("barcode", "")),
                pick_image(product, variant),
                sku_url,
                determine_inseam_style(title),
                stretch,
            ]
            rows.append(row)
    return rows


def write_csv(rows: List[List[str]]) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"ICON_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)
    return output_path


def main() -> None:
    logger = configure_logging()
    session = build_session()

    products = fetch_collection_products(session)
    handles = sorted({(prod.get("handle") or "") for prod in products.values()})

    storefront_cache: Dict[str, Dict] = {}
    for handle in handles:
        if not handle:
            continue
        storefront_cache[handle] = fetch_storefront_details(session, handle) or {}
        time.sleep(0.25)

    rows = assemble_rows(products, storefront_cache)
    rows.sort(key=lambda r: (r[0], r[11]))

    output_path = write_csv(rows)
    logger.info("Wrote %s rows -> %s", len(rows), output_path)


if __name__ == "__main__":
    main()

