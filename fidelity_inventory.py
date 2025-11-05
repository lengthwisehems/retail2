"""Fidelity Denim daily inventory scraper (GraphQL only)."""
from __future__ import annotations

import csv
import json
import logging
import re
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import urllib3

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "fidelity_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "fidelity_run.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}

HOSTS: List[str] = [
    "https://fidelitydenim.com",
    "https://fidelitydenim.myshopify.com",
]

GRAPHQL_PATH = "/api/2025-07/graphql.json"
STOREFRONT_TOKEN = "51cab5df1462f88a7245a3066803b9c1"

COLLECTION_HANDLES: List[str] = [
    "fidelity-women",
    "modern-american",
    "fidelity-womens-sale",
]

GLOBO_ENDPOINT = "https://filter-x3.globo.io/api/apiFilter"
GLOBO_COLLECTIONS: List[Tuple[str, Dict[str, str]]] = [
    (
        "fidelity_women",
        {
            "callback": "jQuery37106542333122937476_1761684550758",
            "filter_id": "9062",
            "shop": "fidelitydenim.myshopify.com",
            "collection": "273471111204",
            "sort_by": "created-descending",
            "country": "US",
            "limit": "100",
            "event": "init",
            "cid": "a1aa7f84-0adc-4acf-aaca-f60f84f06fb6",
            "did": "629a04a1-5b50-45f0-bf2e-2eab7defb407",
            "page_type": "collection",
            "ncp": "60",
        },
    ),
    (
        "modern_american",
        {
            "callback": "jQuery37108292485561334944_1761760733001",
            "filter_id": "9062",
            "shop": "fidelitydenim.myshopify.com",
            "collection": "273462034468",
            "country": "US",
            "event": "init",
            "cid": "32fd0d14-d567-43f9-b4f1-b02d752ae06d",
            "did": "38601c22-daba-49d5-b8ef-f28d245aab2a",
            "page_type": "collection",
            "ncp": "19",
        },
    ),
    (
        "fidelity_womens_sale",
        {
            "callback": "jQuery37109486356583540366_1761761266979",
            "filter_id": "9062",
            "shop": "fidelitydenim.myshopify.com",
            "collection": "273470652452",
            "country": "US",
            "limit": "48",
            "event": "init",
            "cid": "dcd12cde-ec2d-4045-8574-fb32c382e757",
            "did": "8cd8eb6d-1cc1-4de1-b041-2a5868b51403",
            "page_type": "collection",
            "ncp": "24",
        },
    ),
]

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
    "Rise",
    "Back Rise",
    "Inseam",
    "Leg Opening",
    "Price",
    "Compare at Price",
    "Available for Sale",
    "Quantity Available",
    "Old Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Product Line",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Style",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
]

EXCLUDED_TITLE_KEYWORDS = {
    "50-11",
    "dress",
    "skirt",
    "highway s",
    "ryder",
    "vest",
    "maxine",
}

TRANSIENT_STATUS = {429, 500, 502, 503, 504}
REQUEST_TIMEOUT = 30
GRAPHQL_PAGE_SIZE = 100

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SHOPIFY_SESSION = requests.Session()
SHOPIFY_SESSION.headers.update(DEFAULT_HEADERS)
SHOPIFY_SESSION.verify = False

GLOBO_SESSION = requests.Session()
GLOBO_SESSION.headers.update(DEFAULT_HEADERS)
GLOBO_SESSION.verify = False

LOGGER = logging.getLogger("fidelity_inventory")
LOGGER.setLevel(logging.INFO)


def configure_logging() -> logging.Logger:
    handlers: List[logging.Handler] = []
    selected_path: Optional[Path] = None
    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path)
            handlers.append(handler)
            selected_path = path
            if path != LOG_PATH:
                print(
                    f"WARNING: Primary log path {LOG_PATH} unavailable. Using fallback log at {path}.",
                    flush=True,
                )
            break
        except OSError as exc:
            print(f"WARNING: Unable to open log file {path}: {exc}", flush=True)
            continue

    if not handlers:
        handlers.append(logging.StreamHandler())

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)

    LOGGER.info("Logging initialized (path=%s)", selected_path or "stream-only")
    return LOGGER


def host_candidates() -> Iterable[str]:
    yielded: set[str] = set()
    for host in HOSTS:
        if host not in yielded:
            yielded.add(host)
            yield host


def perform_shopify_request(
    method: str,
    path: str,
    *,
    params: Optional[Dict[str, Any]] = None,
    data: Any = None,
    json_payload: Any = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT,
    add_token: bool = False,
    logger: Optional[logging.Logger] = None,
) -> requests.Response:
    logger = logger or LOGGER
    if not path.startswith("http"):
        path = path if path.startswith("/") else f"/{path}"
    last_error: Optional[Exception] = None
    for host in host_candidates():
        url = f"{host}{path}"
        for attempt in range(5):
            try:
                request_headers = dict(headers or {})
                if add_token:
                    request_headers.setdefault("X-Shopify-Storefront-Access-Token", STOREFRONT_TOKEN)
                    request_headers.setdefault("Content-Type", "application/json")
                response = SHOPIFY_SESSION.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    json=json_payload,
                    headers=request_headers or None,
                    timeout=timeout,
                    verify=False,
                )
                if response.status_code in TRANSIENT_STATUS:
                    raise requests.HTTPError(f"transient status {response.status_code}")
                response.raise_for_status()
                if host != HOSTS[0]:
                    logger.info("Switched host to %s", host)
                return response
            except Exception as exc:  # pragma: no cover - network path
                last_error = exc
                sleep_for = min(8.0, 1.0 * (2 ** attempt))
                logger.warning("%s %s failed (%s); sleeping %.1fs", method, url, exc, sleep_for)
                time.sleep(sleep_for)
        logger.error("Giving up on host %s after retries", host)
    if last_error:
        raise last_error
    raise RuntimeError("perform_shopify_request exhausted hosts without response")


def execute_graphql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    response = perform_shopify_request(
        "POST",
        GRAPHQL_PATH,
        json_payload={"query": query, "variables": variables or {}},
        add_token=True,
    )
    payload = response.json()
    if "errors" in payload:
        LOGGER.error("GraphQL errors: %s", payload["errors"])
        raise RuntimeError("GraphQL request failed")
    return payload.get("data", {})


COLLECTION_PRODUCTS_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    handle
    products(first: 100, after: $cursor) {
      edges {
        cursor
        node {
          id
          handle
          title
          tags
          vendor
          description
          productType
          onlineStoreUrl
          createdAt
          updatedAt
          publishedAt
          totalInventory
          featuredImage { url }
          variants(first: 250) {
            edges {
              node {
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
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
"""


def should_exclude(title: str) -> bool:
    lowered = title.lower()
    return any(keyword in lowered for keyword in EXCLUDED_TITLE_KEYWORDS)


def fetch_products(logger: Optional[logging.Logger] = None) -> List[Dict[str, Any]]:
    logger = logger or LOGGER
    products_by_id: Dict[str, Dict[str, Any]] = {}
    for handle in COLLECTION_HANDLES:
        logger.info("Fetching collection %s", handle)
        cursor: Optional[str] = None
        seen_count = 0
        while True:
            data = execute_graphql(
                COLLECTION_PRODUCTS_QUERY,
                {"handle": handle, "cursor": cursor},
            )
            collection = data.get("collection") or {}
            connection = collection.get("products") or {}
            edges = connection.get("edges") or []
            if not edges and cursor is None:
                logger.warning("Collection %s returned no products", handle)
                break
            for edge in edges:
                node = edge.get("node") or {}
                title = node.get("title", "")
                if not title or should_exclude(title):
                    continue
                style_id = normalize_shopify_id_length(
                    stringify_identifier(node.get("id"))
                )
                if not style_id:
                    continue
                if style_id not in products_by_id:
                    products_by_id[style_id] = node
                seen_count += 1
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.2)
        logger.info(
            "Collection %s yielded %s product edges", handle, seen_count
        )
    logger.info(
        "Fetched %s unique products across %s collections",
        len(products_by_id),
        len(COLLECTION_HANDLES),
    )
    return list(products_by_id.values())


def parse_jsonp(text: str) -> Dict[str, Any]:
    text = text.strip()
    match = re.search(r"^[^(]+\((.*)\)\s*;?\s*$", text, re.S)
    if not match:
        raise ValueError("Unable to parse Globo JSONP payload")
    return json.loads(match.group(1))


def fetch_globo_mappings(logger: Optional[logging.Logger] = None) -> Tuple[Dict[str, str], Dict[str, str]]:
    logger = logger or LOGGER
    variant_old_qty: Dict[str, str] = {}
    product_line_map: Dict[str, str] = {}
    for name, params in GLOBO_COLLECTIONS:
        for attempt in range(5):
            try:
                response = GLOBO_SESSION.get(
                    GLOBO_ENDPOINT,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                    verify=False,
                )
                if response.status_code in TRANSIENT_STATUS:
                    raise requests.HTTPError(f"transient status {response.status_code}")
                response.raise_for_status()
                payload = parse_jsonp(response.text)
                break
            except Exception as exc:  # pragma: no cover - network path
                sleep_for = min(8.0, 1.0 * (2 ** attempt))
                logger.warning("Globo request %s failed (%s); sleeping %.1fs", name, exc, sleep_for)
                time.sleep(sleep_for)
        else:
            logger.error("Globo request %s failed after retries", name)
            continue

        collection_id = str(params.get("collection", ""))
        collection_label = ""
        for gf_filter in payload.get("filters", []):
            if gf_filter.get("attribute") == "Collection":
                for value in gf_filter.get("values", []):
                    if str(value.get("value")) == collection_id:
                        label = value.get("label") or value.get("handle") or ""
                        collection_label = str(label).replace("_", " ")
                        break
            if collection_label:
                break
        if not collection_label:
            collection_label = name.replace("_", " ")

        for product in payload.get("products", []):
            style_id = normalize_shopify_id_length(
                stringify_identifier(product.get("id"))
            )
            if style_id and style_id not in product_line_map:
                product_line_map[style_id] = collection_label
            for variant in product.get("variants", []):
                variant_id = normalize_shopify_id_length(
                    stringify_identifier(variant.get("id"))
                )
                if not variant_id:
                    continue
                old_qty = variant.get("old_inventory_quantity")
                if old_qty not in (None, ""):
                    variant_old_qty[variant_id] = str(old_qty)
    logger.info(
        "Collected Globo metadata for %s variants across %s collections",
        len(variant_old_qty),
        len(GLOBO_COLLECTIONS),
    )
    return variant_old_qty, product_line_map


def stringify_identifier(raw: Any) -> str:
    if raw in (None, ""):
        return ""
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, float):
        try:
            decimal_value = Decimal(str(raw))
            if decimal_value == decimal_value.to_integral_value():
                return format(decimal_value.quantize(Decimal("1")), "f")
        except InvalidOperation:
            pass
        return str(raw)
    text = str(raw).strip()
    if text.startswith("gid://"):
        text = text.rsplit("/", 1)[-1]
    if text.isdigit():
        return text
    if re.fullmatch(r"[0-9eE+.-]+", text):
        try:
            decimal_value = Decimal(text)
            if decimal_value == decimal_value.to_integral_value():
                return format(decimal_value.quantize(Decimal("1")), "f")
        except InvalidOperation:
            pass
    digits = re.sub(r"\D", "", text)
    return digits or text


def normalize_shopify_id_length(value: str) -> str:
    if value and value.isdigit() and len(value) < 14:
        return value.ljust(14, "0")
    return value


def parse_date(raw: Optional[str]) -> str:
    if not raw:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%Y")
    except Exception:
        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", raw)
        if match:
            year, month, day = match.groups()
            return f"{month}/{day}/{year}"
    return ""


def format_price(amount: Any) -> str:
    if not amount:
        return ""
    if isinstance(amount, dict):
        amount = amount.get("amount")
    if amount in (None, ""):
        return ""
    try:
        value = float(str(amount))
    except ValueError:
        return str(amount)
    return f"${value:.2f}"


def coerce_tags(tags: Optional[Iterable[str]]) -> List[str]:
    if not tags:
        return []
    return [str(tag) for tag in tags if tag not in (None, "")]


def tags_to_string(tags: Iterable[str]) -> str:
    return ", ".join(tags)


def clean_description_text(text: str) -> str:
    if not text:
        return ""
    cleaned = unescape(text)
    cleaned = cleaned.replace("\u00a0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def clean_measurement_value(raw: Any) -> str:
    if raw in (None, ""):
        return ""
    match = re.search(r"\d+(?:\.\d+)?", str(raw))
    return match.group(0) if match else ""


def _measurement_regex(label: str) -> re.Pattern[str]:
    return re.compile(rf"{label}[^0-9]{{0,40}}(\d+(?:\.\d+)?)", re.IGNORECASE)


def extract_measurement_for_labels(text: str, labels: Iterable[str]) -> str:
    for label in labels:
        match = _measurement_regex(label).search(text)
        if match:
            return clean_measurement_value(match.group(1))
    return ""


def normalize_measurement_text(text: str) -> str:
    if not text:
        return ""
    normalized = unescape(text)
    normalized = normalized.replace("\u201d", "\"").replace("\u2033", "\"")
    normalized = normalized.replace("•", " ")
    normalized = normalized.replace("\u00a0", " ")
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def parse_measurements_from_text(text: str) -> Dict[str, str]:
    normalized = normalize_measurement_text(text)
    if not normalized:
        return {}
    measurements = {
        "rise": extract_measurement_for_labels(normalized, ["Front Rise", "Rise"]),
        "back_rise": extract_measurement_for_labels(normalized, ["Back Rise"]),
        "inseam": extract_measurement_for_labels(normalized, ["Inseam"]),
        "leg_opening": extract_measurement_for_labels(normalized, ["Leg Opening", "Opening", "Sweep"]),
    }
    return {key: value for key, value in measurements.items() if value}


def extract_style_name(title: str) -> str:
    parts = title.split()
    return parts[0] if parts else ""


def extract_variant_size_color(variant: Dict[str, Any]) -> Tuple[str, str]:
    selected_options = variant.get("selectedOptions") or []
    size = ""
    color = ""
    for option in selected_options:
        name = (option.get("name") or "").strip().lower()
        value = (option.get("value") or "").strip()
        if not value:
            continue
        if name == "size":
            size = value
        elif name in {"color", "colour", "wash"}:
            color = value
    title = variant.get("title", "")
    parts = [part.strip() for part in title.split("/") if part.strip()]
    if not size and parts:
        size = parts[0]
    if not color and len(parts) >= 2:
        color = parts[-1]
    return size, color


def determine_jean_style(tags: Iterable[str], description: str) -> str:
    tags_lower = [tag.lower() for tag in tags]
    description_lower = (description or "").lower()

    def contains(keyword: str) -> bool:
        return any(keyword in tag for tag in tags_lower) or keyword in description_lower

    has_straight = contains("straight")
    has_wide = contains("wide")
    if has_straight and has_wide:
        return "Straight From Thigh"
    if has_straight:
        return "Straight"
    if has_wide:
        return "Wide Leg"
    if contains("flare"):
        return "Flare"
    if contains("bootcut"):
        return "Bootcut"
    if contains("skinny"):
        return "Skinny"
    if contains("barrel"):
        return "Barrel"
    return ""


def determine_inseam_style(tags: Iterable[str]) -> str:
    tags_lower = [tag.lower() for tag in tags]
    if any(term in tag for tag in tags_lower for term in ("length:ankle", "filterwomenankle", "ankle")):
        return "Ankle"
    if any(term in tag for tag in tags_lower for term in ("length:crop", "filterwomencropped", "crop", "cropped")):
        return "Cropped"
    if any(term in tag for tag in tags_lower for term in ("length:capri", "length:knee", "capri")):
        return "Capri"
    return ""


def determine_rise_label(tags: Iterable[str], title: str) -> str:
    tags_lower = [tag.lower() for tag in tags]
    candidates: List[str] = []
    mapping = {
        "High": ["rise:high", "high rise", "highrise", "rise:ultrahighrise", "ultrahighrise"],
        "Mid": ["rise:mid", "mid rise", "midrise"],
        "Low": ["rise:low", "low rise", "lowrise"],
    }
    for label, keywords in mapping.items():
        if any(keyword in tag for keyword in keywords for tag in tags_lower):
            if label not in candidates:
                candidates.append(label)

    if not candidates:
        return ""
    if len(candidates) == 1:
        return candidates[0]

    title_lower = (title or "").lower()
    for label in candidates:
        if label.lower() in title_lower:
            return label

    for label in ("High", "Mid", "Low"):
        if label in candidates:
            return label
    return candidates[0]


def determine_color_standardized(tags: Iterable[str]) -> str:
    tags_upper = [tag.upper() for tag in tags if tag]
    checks = [
        ("GREY", ["GREY"]),
        ("PURPLE", ["PURPLE", "PALEPURPLE"]),
        ("IVORY", ["IVORY"]),
        ("BROWN", ["BROWN"]),
        ("BLUE", ["BLUE"]),
        ("WHITE", ["WHITE"]),
        ("BLACK", ["BLACK"]),
        ("GREEN", ["GREEN"]),
        ("RED", ["RED"]),
        ("TAN", ["TAN"]),
    ]
    for label, keywords in checks:
        if any(keyword in tag for tag in tags_upper for keyword in keywords):
            return label
    return ""


def determine_color_simplified(tags: Iterable[str], color_standardized: str) -> str:
    tags_upper = [tag.upper() for tag in tags if tag]
    text = " ".join(tags_upper)
    if any(keyword in text for keyword in ("MIDWASH", "WASH:MID", "MEDIUM BLUE")):
        return "Medium"
    if any(keyword in text for keyword in ("DARK", "DARKINDIGO", "DARKWASH", "WASH:BLACK", "WASH:DARK", "TINTEDDARK", "DARK BLUE", "DARK WASH", "BLACK")):
        return "Dark"
    if any(keyword in text for keyword in ("WASH:WHITE", "WASH:LIGHT", "WASH:LIGHTWASH", "IVORY", "WHITE", "LIGHT BLUE")):
        return "Light"
    if "WASH:OTHER" in text:
        return "Other"
    if color_standardized == "BLACK":
        return "Dark"
    return ""


def determine_stretch(tags: Iterable[str]) -> str:
    tags_lower = [tag.lower() for tag in tags]
    if any("performance stretch" in tag for tag in tags_lower):
        return "High Stretch"
    if any("comfort stretch" in tag for tag in tags_lower):
        return "Stretch"
    if any("no stretch" in tag or "traditional" in tag for tag in tags_lower):
        return "Rigid"
    return ""


def assemble_rows(
    products: List[Dict[str, Any]],
    variant_old_qty: Dict[str, str],
    product_line_map: Dict[str, str],
    *,
    measurement_overrides: Optional[Dict[str, Dict[str, str]]] = None,
    description_overrides: Optional[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    measurement_overrides = measurement_overrides or {}
    description_overrides = description_overrides or {}

    for product in products:
        style_id = normalize_shopify_id_length(
            stringify_identifier(product.get("id"))
        )
        handle = product.get("handle", "")
        title = product.get("title", "")
        tags = coerce_tags(product.get("tags"))
        vendor = product.get("vendor", "")
        description = clean_description_text(description_overrides.get(style_id, product.get("description", "")))
        measurements = parse_measurements_from_text(description)
        overrides = measurement_overrides.get(style_id, {})

        rise = measurements.get("rise") or overrides.get("rise") or ""
        back_rise = measurements.get("back_rise") or overrides.get("back_rise") or ""
        inseam = measurements.get("inseam") or overrides.get("inseam") or ""
        leg_opening = measurements.get("leg_opening") or overrides.get("leg_opening") or ""

        style_name = extract_style_name(title)
        product_type = "Jeans"
        published_at = parse_date(product.get("publishedAt"))
        created_at = parse_date(product.get("createdAt"))
        total_inventory = stringify_identifier(product.get("totalInventory"))
        image_url = (product.get("featuredImage") or {}).get("url") or ""
        sku_url = product.get("onlineStoreUrl") or (f"https://fidelitydenim.com/products/{handle}" if handle else "")
        jean_style = determine_jean_style(tags, description)
        inseam_style = determine_inseam_style(tags)
        rise_label = determine_rise_label(tags, title)
        color_standardized = determine_color_standardized(tags)
        color_simplified = determine_color_simplified(tags, color_standardized)
        stretch = determine_stretch(tags)
        product_line = product_line_map.get(style_id, "")
        if not product_line and "plus" in title.lower():
            product_line = "Plus"

        for variant_edge in (product.get("variants") or {}).get("edges", []):
            variant = variant_edge.get("node") or {}
            variant_id = normalize_shopify_id_length(
                stringify_identifier(variant.get("id"))
            )
            size, color = extract_variant_size_color(variant)
            price = format_price(variant.get("price"))
            compare_at = format_price(variant.get("compareAtPrice"))
            quantity_available = stringify_identifier(variant.get("quantityAvailable"))
            old_quantity = variant_old_qty.get(variant_id, "")

            row = {
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": title,
                "Style Name": style_name,
                "Product Type": product_type,
                "Tags": tags_to_string(tags),
                "Vendor": vendor,
                "Description": description,
                "Variant Title": f"{title} - {size}" if size else title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Back Rise": back_rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": price,
                "Compare at Price": compare_at,
                "Available for Sale": "TRUE" if variant.get("availableForSale") else "FALSE",
                "Quantity Available": quantity_available,
                "Old Quantity Available": old_quantity,
                "Quantity of style": total_inventory,
                "SKU - Shopify": variant_id,
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": variant.get("barcode", ""),
                "Product Line": product_line,
                "Image URL": image_url,
                "SKU URL": sku_url,
                "Jean Style": jean_style,
                "Inseam Style": inseam_style,
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
            }
            rows.append(row)
    return rows


def write_csv(rows: List[Dict[str, str]]) -> str:
    if not rows:
        raise ValueError("No rows to write")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"FIDELITY_{timestamp}.csv"
    output_path = OUTPUT_DIR / filename
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Wrote %s rows to %s", len(rows), output_path)
    return str(output_path)


def main() -> str:
    configure_logging()
    products = fetch_products()
    if not products:
        raise RuntimeError("No products returned from GraphQL")
    variant_old_qty, product_line_map = fetch_globo_mappings()
    rows = assemble_rows(products, variant_old_qty, product_line_map)
    if not rows:
        raise RuntimeError("No rows assembled for CSV output")
    return write_csv(rows)


if __name__ == "__main__":
    try:
        path = main()
        print(path)
    except Exception as exc:  # pragma: no cover - entry point logging
        LOGGER.exception("Run failed: %s", exc)
        raise

