"""Self Contrast inventory scraper combining Storefront and Globo data."""

from __future__ import annotations

import csv
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
import urllib3
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "selfcontrast_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "selfcontrast_run.log"

OUTPUT_DIR.mkdir(exist_ok=True)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

HOSTS: Sequence[str] = (
    "https://selfcontrast.com",
    "https://www.selfcontrast.com",
    "https://self-contrast.myshopify.com",
)

GRAPHQL_VERSIONS: Sequence[str] = (
    "2025-10",
    "2025-07",
    "unstable",
)

COLLECTION_HANDLE = "denim-2"
COLLECTION_URLS: Sequence[str] = (
    "https://selfcontrast.com/collections/denim-2",
    "https://www.selfcontrast.com/collections/denim-2",
)

GRAPHQL_PAGE_SIZE = 100
REQUEST_TIMEOUT = 30
TRANSIENT_STATUS = {429, 500, 502, 503, 504}

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Created At",
    "Product",
    "Style Name",
    "Product Type",
    "Tags",
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
    "Jean Style",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
]

EXCLUDED_TITLE_KEYWORDS = {
    "dress",
    "short",
    "skirt",
    "jacket",
    "shirt",
    "vest",
    "tee",
}

EXCLUDED_PRODUCT_TYPES = {
    "jackets",
    "shirts & tops",
    "skirts",
    "shorts",
}

EXCLUDED_TAG_SUBSTRINGS = {
    "buttoned vest",
    "crop vest",
    "jacket",
    "jackets",
    "slim vest",
    "tops",
    "vest",
    "blazer",
    "coat",
    "fall jacket",
    "long sleeve top",
    "long sleeve tops",
    "skirts",
}

STYLE_KEYWORD_MAP: Sequence[Tuple[str, Tuple[str, ...]]] = (
    ("Barrel", ("barrel", "widebarrel")),
    ("Boot", ("boot", "bootcut", "boot cut", "bootflare", "filterbootflare")),
    ("Baggy", ("boyfriend", "boyfriendrelaxed")),
    ("Wide", ("filterwideleg", "wide", "wideleg", "widelegs")),
    ("Straight", ("filterstraight", "straight")),
    ("Flare", ("flare",)),
    ("Skinny", ("skinny",)),
    ("Straight from the Knee", ("cigarette", "slim", "slimstraight")),
)

RISE_KEYWORDS: Sequence[Tuple[str, Tuple[str, ...]]] = (
    ("High", ("rise:high", "rise:ultrahighrise", "ultrahighrise", "high", "highrise", "high rise")),
    ("Mid", ("rise:mid", "midrise", "mid rise", "mid")),
    ("Low", ("rise:low", "lowrise", "low rise", "low")),
)

COLOR_STANDARDIZED_KEYWORDS: Sequence[Tuple[str, Tuple[str, ...]]] = (
    ("Red", ("red",)),
    ("Yellow", ("yellow",)),
    ("Blue", ("blue",)),
    ("Green", ("green",)),
    ("Purple", ("purple", "palepurple")),
    ("Brown", ("brown",)),
    ("Beige", ("beige", "tan")),
    ("Cream", ("cream",)),
    ("White", ("white", "ivory")),
    ("Black", ("black",)),
    ("Grey", ("grey", "gray")),
    ("Pink", ("pink",)),
    ("Orange", ("orange",)),
)

MEDIUM_COLOR_KEYWORDS = {"midwash", "wash:mid", "medium blue"}
DARK_COLOR_KEYWORDS = {"dark", "darkindigo", "darkwash", "dark wash", "wash:black", "wash:dark", "tinteddark", "black"}
LIGHT_COLOR_KEYWORDS = {"white", "white pants", "cream", "wash:white", "wash:light", "wash:lightwash"}
OTHER_COLOR_KEYWORDS = {"wash:neutrals", "wash:other"}

STYLE_ORDER = {style: index for index, (style, _) in enumerate(STYLE_KEYWORD_MAP)}
RISE_ORDER = {label: index for index, (label, _) in enumerate(RISE_KEYWORDS)}

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GraphQLRequestError(RuntimeError):
    """Raised when Storefront requests repeatedly fail."""


def configure_logging() -> logging.Logger:
    logger = logging.getLogger("selfcontrast_inventory")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handlers: List[logging.Handler] = []
    added_file_handler = False
    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path, encoding="utf-8")
        except OSError as exc:
            print(f"WARNING: Unable to open log file {path}: {exc}", flush=True)
            continue
        else:
            handlers.append(handler)
            added_file_handler = True
            if path != LOG_PATH:
                print(
                    f"WARNING: Primary log path {LOG_PATH} unavailable. Using fallback log at {path}.",
                    flush=True,
                )
            break

    if not handlers:
        handlers.append(logging.StreamHandler())

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    for handler in handlers:
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    if added_file_handler:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    logger.info("Logging configured")
    return logger


def build_session() -> requests.Session:
    session = requests.Session()
    retries = urllib3.Retry(
        total=5,
        backoff_factor=1.2,
        status_forcelist=tuple(TRANSIENT_STATUS),
        allowed_methods=("GET", "HEAD", "POST"),
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": USER_AGENT})
    session.verify = False
    return session


def build_graphql_endpoints() -> List[str]:
    endpoints: List[str] = []
    for host in HOSTS:
        base = host.rstrip("/")
        for version in GRAPHQL_VERSIONS:
            endpoints.append(f"{base}/api/{version}/graphql.json")
    # remove duplicates preserving order
    return list(dict.fromkeys(endpoints))


def select_graphql_endpoint(session: requests.Session, logger: logging.Logger) -> List[str]:
    payload = {
        "query": GRAPHQL_QUERY,
        "variables": {"handle": COLLECTION_HANDLE, "cursor": None, "pageSize": 1},
    }
    headers = {"Content-Type": "application/json"}
    available: List[str] = []
    for endpoint in build_graphql_endpoints():
        logger.info("Probing Storefront endpoint %s", endpoint)
        try:
            response = session.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )
        except requests.RequestException as exc:
            logger.warning("Probe request to %s failed: %s", endpoint, exc)
            continue
        if response.status_code in TRANSIENT_STATUS:
            logger.info("Endpoint %s returned transient status %s", endpoint, response.status_code)
            continue
        if not response.ok:
            logger.info("Endpoint %s returned status %s", endpoint, response.status_code)
            continue
        try:
            data = response.json()
        except ValueError:
            logger.info("Endpoint %s returned non-JSON payload", endpoint)
            continue
        collection = ((data.get("data") or {}).get("collection") or {})
        edges = ((collection.get("products") or {}).get("edges")) or []
        if edges:
            logger.info("Endpoint %s produced collection data", endpoint)
            available.append(endpoint)
    if not available:
        raise GraphQLRequestError("No Storefront endpoint returned collection data")
    # ensure full endpoint list with available endpoints first
    all_candidates = build_graphql_endpoints()
    ordered: List[str] = []
    for endpoint in available:
        if endpoint not in ordered:
            ordered.append(endpoint)
    for endpoint in all_candidates:
        if endpoint not in ordered:
            ordered.append(endpoint)
    return ordered


def execute_graphql(
    session: requests.Session,
    payload: Dict[str, Any],
    endpoints: Sequence[str],
    logger: logging.Logger,
) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    last_error: Optional[Exception] = None

    for attempt, endpoint in enumerate(endpoints):
        try:
            response = session.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
                verify=False,
            )
        except requests.RequestException as exc:
            last_error = exc
            sleep_for = min(2 ** attempt, 30)
            logger.warning("POST %s failed (%s); sleeping %.1fs", endpoint, exc, sleep_for)
            time.sleep(sleep_for)
            continue

        if response.status_code in TRANSIENT_STATUS:
            last_error = RuntimeError(f"HTTP {response.status_code}")
            sleep_for = min(2 ** attempt, 30)
            logger.warning(
                "POST %s returned %s; sleeping %.1fs",
                endpoint,
                response.status_code,
                sleep_for,
            )
            time.sleep(sleep_for)
            continue

        try:
            data = response.json()
        except ValueError as exc:
            last_error = exc
            logger.warning("Invalid JSON from %s: %s", endpoint, exc)
            time.sleep(1.5)
            continue

        if data.get("errors"):
            logger.warning(
                "GraphQL response from %s contained %s errors",
                endpoint,
                len(data["errors"]),
            )
        return data

    raise GraphQLRequestError(f"GraphQL request failed after retries: {last_error}")


GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String, $pageSize: Int!) {
  collection(handle: $handle) {
    id
    handle
    title
    products(first: $pageSize, after: $cursor) {
      edges {
        cursor
        node {
          id
          handle
          title
          productType
          tags
          vendor
          description
          descriptionHtml
          onlineStoreUrl
          publishedAt
          createdAt
          updatedAt
          availableForSale
          featuredImage { url altText }
          images(first: 20) { edges { node { url altText } } }
          variants(first: 250) {
            edges {
              cursor
              node {
                id
                title
                sku
                barcode
                availableForSale
                price { amount currencyCode }
                compareAtPrice { amount currencyCode }
                quantityAvailable
                image { url altText }
              }
            }
          }
        }
      }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""


def fetch_collection_html(session: requests.Session, logger: logging.Logger) -> str:
    html_parts: List[str] = []
    for url in COLLECTION_URLS:
        logger.info("Fetching collection HTML from %s", url)
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT, verify=False)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Failed to fetch %s: %s", url, exc)
            continue
        html_parts.append(response.text)
    if not html_parts:
        raise RuntimeError("Unable to retrieve collection HTML for Globo inventory")
    return "\n".join(html_parts)


def extract_globo_script(html: str, logger: logging.Logger) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for script in soup.find_all("script"):
        if script.string and "GloboPreorderParams" in script.string:
            logger.info("Located Globo preorder script block")
            return script.string
    raise RuntimeError("Globo preorder script not found in HTML")


def _consume_block(text: str, start: int, opening: str = "[") -> Tuple[str, int]:
    closing = "]" if opening == "[" else "}"
    depth = 0
    for idx in range(start, len(text)):
        char = text[idx]
        if char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return text[start : idx + 1], idx + 1
    raise ValueError("Unbalanced brackets while parsing Globo payload")


def parse_globo_product_arrays(script_text: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    cursor = 0
    needle = ".concat("
    while True:
        idx = script_text.find(needle, cursor)
        if idx == -1:
            break
        start = script_text.find("[", idx)
        if start == -1:
            cursor = idx + len(needle)
            continue
        block, cursor = _consume_block(script_text, start, "[")
        try:
            chunk = json.loads(block)
        except json.JSONDecodeError as exc:
            logger.warning("Failed to parse Globo product chunk: %s", exc)
            continue
        products.extend(chunk)
    logger.info("Parsed %s Globo product entries", len(products))
    return products


def normalize_shopify_id(raw_id: Any, prefix: str) -> str:
    if not raw_id:
        return ""
    text = str(raw_id)
    if text.startswith(prefix):
        text = text[len(prefix) :]
    return text


def parse_iso_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    return dt.strftime("%m/%d/%Y")


def clean_description(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"\s+", " ", text.replace("\u00a0", " ").strip())
    return cleaned


def tags_to_list(tags: Optional[Iterable[str]]) -> List[str]:
    if not tags:
        return []
    return [str(tag) for tag in tags if tag]


def tags_to_string(tags: Iterable[str]) -> str:
    return ", ".join(tags)


def coerce_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None


def should_exclude_product(title: str, product_type: str, tags: List[str]) -> bool:
    lower_title = title.lower()
    for keyword in EXCLUDED_TITLE_KEYWORDS:
        if keyword in lower_title:
            return True

    if product_type and product_type.lower() in EXCLUDED_PRODUCT_TYPES:
        return True

    tags_lower = [tag.lower() for tag in tags]
    for forbidden in EXCLUDED_TAG_SUBSTRINGS:
        if any(forbidden in tag for tag in tags_lower):
            return True
    return False


def split_variant_title(title: str) -> Tuple[str, str]:
    if not title:
        return "", ""
    parts = [part.strip() for part in title.split("/")]
    if len(parts) >= 2:
        return parts[0], parts[1]
    if parts:
        return parts[0], ""
    return "", ""


def format_price(amount: Any) -> str:
    if not amount:
        return ""
    if isinstance(amount, dict):
        amount = amount.get("amount")
    if amount in (None, ""):
        return ""
    try:
        value = float(str(amount))
    except (TypeError, ValueError):
        return str(amount)
    return f"${value:.2f}"


def determine_rise_label(tags: List[str], title: str) -> str:
    tags_lower = [tag.lower() for tag in tags]
    title_lower = title.lower()
    candidates: List[Tuple[int, str]] = []
    for label, keywords in RISE_KEYWORDS:
        tag_match = any(keyword in tag for tag in tags_lower for keyword in keywords)
        title_match = any(keyword in title_lower for keyword in keywords)
        score = 0
        if tag_match:
            score += 2
        if title_match:
            score += 2
        if score:
            candidates.append((score, label))
    if not candidates:
        return ""
    candidates.sort(key=lambda item: (-item[0], RISE_ORDER[item[1]]))
    return candidates[0][1]


def determine_jean_style(tags: List[str], title: str, description: str) -> str:
    tags_lower = [tag.lower() for tag in tags]
    title_lower = title.lower()
    description_lower = description.lower()

    if "soft curve" in title_lower:
        return "Barrel"
    if "tapered" in title_lower:
        return "Tapered But Loose at the Knee"

    candidates: List[Tuple[int, str]] = []
    for style, keywords in STYLE_KEYWORD_MAP:
        tag_match = any(keyword in tag for tag in tags_lower for keyword in keywords)
        title_match = any(keyword in title_lower for keyword in keywords)
        description_match = any(keyword in description_lower for keyword in keywords)
        score = 0
        if tag_match:
            score += 3
        if title_match:
            score += 2
        if description_match:
            score += 1
        if score:
            candidates.append((score, style))

    if candidates:
        candidates.sort(key=lambda item: (-item[0], STYLE_ORDER[item[1]]))
        return candidates[0][1]

    # fallback to keyword search in text if no tag match but product type indicates jeans
    text = f"{title_lower} {description_lower}"
    for style, keywords in STYLE_KEYWORD_MAP:
        if any(keyword in text for keyword in keywords):
            return style

    return ""


def determine_color_standardized(tags: List[str], color: str) -> str:
    tags_lower = [tag.lower() for tag in tags]
    color_lower = color.lower()

    tag_label: Optional[str] = None
    for label, keywords in COLOR_STANDARDIZED_KEYWORDS:
        if any(keyword in tag for keyword in keywords for tag in tags_lower):
            tag_label = label
            break

    color_label: Optional[str] = None
    for label, keywords in COLOR_STANDARDIZED_KEYWORDS:
        if any(keyword in color_lower for keyword in keywords):
            color_label = label
            break

    if any("white" in tag for tag in tags_lower) and "black" in color_lower:
        return "Black"
    if tag_label:
        return tag_label
    if color_label:
        return color_label
    return ""


def determine_color_simplified(tags: List[str], color_standardized: str, color: str) -> str:
    tags_lower = [tag.lower() for tag in tags]
    if any(keyword in tag for tag in tags_lower for keyword in MEDIUM_COLOR_KEYWORDS):
        return "Medium"
    if color_standardized == "Black":
        return "Dark"
    if any(keyword in tag for tag in tags_lower for keyword in DARK_COLOR_KEYWORDS):
        return "Dark"
    if any(keyword in tag for tag in tags_lower for keyword in LIGHT_COLOR_KEYWORDS):
        return "Light"
    if any(keyword in tag for tag in tags_lower for keyword in OTHER_COLOR_KEYWORDS):
        return "Other"
    color_lower = color.lower()
    if "light" in color_lower:
        return "Light"
    if "medium" in color_lower:
        return "Medium"
    return ""


def build_globo_maps(globo_products: Iterable[Dict[str, Any]]) -> Tuple[Dict[str, str], Dict[str, str]]:
    variant_qty: Dict[str, str] = {}
    product_totals: Dict[str, int] = defaultdict(int)
    for product in globo_products:
        product_id = str(product.get("id", ""))
        variants = product.get("variants") or []
        style_total = 0
        for variant in variants:
            variant_id = str(variant.get("id", ""))
            qty = variant.get("inventory_quantity")
            if qty in (None, ""):
                continue
            try:
                qty_int = int(qty)
            except (TypeError, ValueError):
                qty_str = str(qty)
                variant_qty[variant_id] = qty_str
                continue
            style_total += qty_int
            variant_qty[variant_id] = str(qty_int)
        product_totals[product_id] += style_total
    totals_str = {key: str(value) for key, value in product_totals.items()}
    return variant_qty, totals_str


def extract_primary_image(product: Dict[str, Any], variant: Dict[str, Any]) -> str:
    variant_image = (variant.get("image") or {}).get("url")
    if variant_image:
        return variant_image
    featured = (product.get("featuredImage") or {}).get("url")
    if featured:
        return featured
    images = ((product.get("images") or {}).get("edges")) or []
    for edge in images:
        node = edge.get("node") or {}
        url = node.get("url")
        if url:
            return url
    return ""


def flatten_rows(
    storefront_products: List[Dict[str, Any]],
    globo_variant_qty: Dict[str, str],
    globo_product_totals: Dict[str, str],
    logger: logging.Logger,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    for edge in storefront_products:
        product = edge.get("node") or {}
        product_id = normalize_shopify_id(product.get("id"), "gid://shopify/Product/")
        handle = product.get("handle", "")
        title = product.get("title", "")
        product_type = product.get("productType", "")
        tags = tags_to_list(product.get("tags"))

        if should_exclude_product(title, product_type, tags):
            logger.info("Skipping product %s due to exclusion rules", handle)
            continue

        vendor = product.get("vendor", "")
        description = clean_description(product.get("description", ""))
        published_at = parse_iso_date(product.get("publishedAt"))
        created_at = parse_iso_date(product.get("createdAt"))
        style_name = title.split()[0] if title else ""
        product_type_value = "Jeans"
        tags_string = tags_to_string(tags)
        sku_url = product.get("onlineStoreUrl") or (
            f"https://selfcontrast.com/products/{handle}" if handle else ""
        )

        jean_style = determine_jean_style(tags, title, description)
        rise_label = determine_rise_label(tags, title)

        variants_connection = product.get("variants") or {}
        variant_edges = variants_connection.get("edges") or []
        if not variant_edges:
            logger.warning("Product %s has no variants", handle)
            continue

        processed_variants: List[Tuple[Dict[str, Any], str]] = []
        graphql_quantities: Dict[str, str] = {}
        graphql_total = 0
        for variant_edge in variant_edges:
            variant = variant_edge.get("node") or {}
            variant_id = normalize_shopify_id(variant.get("id"), "gid://shopify/ProductVariant/")
            processed_variants.append((variant, variant_id))
            qty_int = coerce_int(variant.get("quantityAvailable"))
            if qty_int is not None:
                graphql_quantities[variant_id] = str(qty_int)
                graphql_total += qty_int

        style_total = globo_product_totals.get(product_id, "")
        if not style_total and graphql_quantities:
            style_total = str(graphql_total)

        for variant, variant_id in processed_variants:
            variant_title = variant.get("title", "")
            color, size = split_variant_title(variant_title)
            variant_price = format_price(variant.get("price"))
            compare_at = format_price(variant.get("compareAtPrice"))
            available = "TRUE" if variant.get("availableForSale") else "FALSE"
            quantity_available = globo_variant_qty.get(variant_id)
            if quantity_available is None:
                quantity_available = graphql_quantities.get(variant_id, "")
            image_url = extract_primary_image(product, variant)
            color_standardized = determine_color_standardized(tags, color)
            color_simplified = determine_color_simplified(tags, color_standardized, color)

            row = {
                "Style Id": product_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": title,
                "Style Name": style_name,
                "Product Type": product_type_value,
                "Tags": tags_string,
                "Vendor": vendor,
                "Description": description,
                "Variant Title": f"{title} - {size}" if size else title,
                "Color": color,
                "Size": size,
                "Price": variant_price,
                "Compare at Price": compare_at,
                "Available for Sale": available,
                "Quantity Available": quantity_available,
                "Quantity of style": style_total,
                "SKU - Shopify": variant_id,
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": variant.get("barcode", ""),
                "Image URL": image_url,
                "SKU URL": sku_url,
                "Jean Style": jean_style,
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
            }
            rows.append(row)
    return rows


def fetch_storefront_data(session: requests.Session, logger: logging.Logger) -> List[Dict[str, Any]]:
    endpoints = select_graphql_endpoint(session, logger)
    rows: List[Dict[str, Any]] = []
    cursor: Optional[str] = None

    while True:
        payload = {
            "query": GRAPHQL_QUERY,
            "variables": {"handle": COLLECTION_HANDLE, "cursor": cursor, "pageSize": GRAPHQL_PAGE_SIZE},
        }
        data = execute_graphql(session, payload, endpoints, logger)
        collection = ((data.get("data") or {}).get("collection")) or {}
        products = (collection.get("products") or {})
        edges = products.get("edges") or []
        if not edges:
            break
        rows.extend(edges)
        page_info = products.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
            if not cursor:
                logger.warning("Missing endCursor despite hasNextPage; stopping pagination")
                break
        else:
            break
    logger.info("Fetched %s product edges from Storefront", len(rows))
    return rows


def write_csv(rows: List[Dict[str, str]], logger: logging.Logger) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"SELFCONTRAST_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logger.info("CSV written to %s", output_path.as_posix())
    return output_path


def main() -> None:
    logger = configure_logging()
    session = build_session()
    html = fetch_collection_html(session, logger)
    globo_script = extract_globo_script(html, logger)
    globo_products = parse_globo_product_arrays(globo_script, logger)
    globo_variant_qty, globo_product_totals = build_globo_maps(globo_products)

    storefront_edges = fetch_storefront_data(session, logger)
    rows = flatten_rows(storefront_edges, globo_variant_qty, globo_product_totals, logger)
    if not rows:
        logger.warning("No rows produced for CSV output")
    write_csv(rows, logger)


if __name__ == "__main__":
    main()
