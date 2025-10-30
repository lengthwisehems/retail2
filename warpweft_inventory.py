"""Warp + Weft women denim inventory scraper."""
from __future__ import annotations

import csv
import logging
import re
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from urllib3.exceptions import InsecureRequestWarning
import urllib3

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "warpweft_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "warpweft_run.log"

OUTPUT_DIR.mkdir(exist_ok=True)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

urllib3.disable_warnings(category=InsecureRequestWarning)

SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)
SESSION.verify = False

HOSTS: List[str] = [
    "https://warpweftworld.com",
    "https://warpandweft1.myshopify.com",
]

GRAPHQL_PATH = "/api/2024-04/graphql.json"
STOREFRONT_TOKEN = "8473dc07111af05dd96ecf3f061a64ac"
GRAPHQL_PAGE_SIZE = 100

SEARCHSPRING_URL = "https://dkc5xr.a.searchspring.io/api/search/autocomplete.json"
SEARCHSPRING_PARAMS = {
    "siteId": "dkc5xr",
    "resultsFormat": "json",
    "resultsPerPage": 250,
    "q": "women jean",
}

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
    "Inseam",
    "Leg Opening",
    "Price",
    "Compare at Price",
    "Promo",
    "Available for Sale",
    "Quantity Available",
    "Quantity of style",
    "Instock Percent",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Product Line",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Hem Style",
    "Inseam Style",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
]

TARGET_PRODUCT_TYPES = {
    "Women's Jeans",
    "Women's Plus Size Jeans",
    "Women's Regular Size Jeans",
}

EXCLUDED_TITLE_KEYWORDS = {"dress", "short", "skirt", "jacket", "shirt", "vest", "tee"}
ALLOWED_INSEAMS = {
    "25",
    "25.25",
    "25.5",
    "25.75",
    "26",
    "26.25",
    "26.5",
    "26.75",
    "27",
    "27.25",
    "27.5",
    "27.75",
    "28",
    "28.25",
    "28.5",
    "28.75",
    "29",
    "29.25",
    "29.5",
    "29.75",
    "30",
    "30.25",
    "30.5",
    "30.75",
    "31",
    "31.25",
    "31.5",
    "31.75",
    "32",
    "32.25",
    "32.5",
    "32.75",
    "33",
    "33.25",
    "33.5",
    "33.75",
    "34",
    "34.25",
}

TRANSIENT_STATUS = {429, 500, 502, 503, 504}


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
        except (OSError, PermissionError) as exc:
            print(
                f"WARNING: Unable to open log file {path}: {exc}. Continuing without this destination.",
                flush=True,
            )
    handlers.append(logging.StreamHandler())
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )
    logger = logging.getLogger(__name__)
    if selected_path is None:
        logger.warning("File logging disabled; continuing with console logging only.")
    return logger


LOGGER = configure_logging()


GRAPHQL_QUERY = """
query WomensJeans($cursor: String, $pageSize: Int!) {
  products(
    first: $pageSize,
    after: $cursor
  ) {
    edges {
      cursor
      node {
        id
        handle
        title
        description
        vendor
        productType
        tags
        createdAt
        publishedAt
        onlineStoreUrl
        totalInventory
        collections(first: 20) {
          edges {
            node { id handle title }
          }
        }
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
"""


class GraphQLRequestError(RuntimeError):
    """Raised when the GraphQL API repeatedly fails."""


def post_graphql(payload: Dict[str, Any]) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
    }
    last_error: Optional[Exception] = None
    for attempt in range(6):
        host = HOSTS[attempt % len(HOSTS)]
        url = f"{host}{GRAPHQL_PATH}"
        try:
            response = SESSION.post(
                url, json=payload, headers=headers, timeout=40, verify=False
            )
        except requests.RequestException as exc:
            last_error = exc
            sleep_for = 2 ** attempt * 0.5
            LOGGER.warning("POST %s failed (%s); sleeping %.1fs", url, exc, sleep_for)
            time.sleep(sleep_for)
            continue

        if response.status_code in TRANSIENT_STATUS:
            sleep_for = 2 ** attempt * 0.5
            LOGGER.warning(
                "POST %s returned %s; sleeping %.1fs",
                url,
                response.status_code,
                sleep_for,
            )
            time.sleep(sleep_for)
            last_error = RuntimeError(f"HTTP {response.status_code}")
            continue

        try:
            data = response.json()
        except ValueError as exc:
            last_error = exc
            LOGGER.warning("Invalid JSON from %s: %s", url, exc)
            time.sleep(1.5)
            continue

        if data.get("errors"):
            last_error = RuntimeError(str(data["errors"]))
            LOGGER.warning("GraphQL errors from %s: %s", url, data["errors"])
            time.sleep(1.5)
            continue

        return data

    raise GraphQLRequestError(f"GraphQL request failed after retries: {last_error}")


def fetch_products() -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        payload = {
            "query": GRAPHQL_QUERY,
            "variables": {"cursor": cursor, "pageSize": GRAPHQL_PAGE_SIZE},
        }
        LOGGER.info("Fetching GraphQL page with cursor %s", cursor)
        data = post_graphql(payload)
        product_edges = (
            data
            .get("data", {})
            .get("products", {})
            .get("edges", [])
        )
        for edge in product_edges:
            node = edge.get("node") or {}
            if not should_include_product(node.get("title"), node.get("productType")):
                continue
            products.append(node)
        page_info = (
            data
            .get("data", {})
            .get("products", {})
            .get("pageInfo", {})
        )
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break
    LOGGER.info("Fetched %s qualifying products", len(products))
    return products


def should_include_product(title: Optional[str], product_type: Optional[str]) -> bool:
    if not title or not product_type:
        return False
    if product_type not in TARGET_PRODUCT_TYPES:
        return False
    lowered = title.lower()
    if any(keyword in lowered for keyword in EXCLUDED_TITLE_KEYWORDS):
        return False
    return True


def fetch_searchspring_results() -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        params = {**SEARCHSPRING_PARAMS, "page": page}
        try:
            response = SESSION.get(
                SEARCHSPRING_URL, params=params, timeout=40, verify=False
            )
        except requests.RequestException as exc:
            if page > 5:
                raise
            sleep_for = 1.5 * page
            LOGGER.warning("GET %s page %s failed (%s); sleeping %.1fs", SEARCHSPRING_URL, page, exc, sleep_for)
            time.sleep(sleep_for)
            continue
        if response.status_code in TRANSIENT_STATUS:
            sleep_for = 1.5 * page
            LOGGER.warning(
                "GET %s page %s status %s; sleeping %.1fs",
                SEARCHSPRING_URL,
                page,
                response.status_code,
                sleep_for,
            )
            time.sleep(sleep_for)
            continue
        try:
            payload = response.json()
        except ValueError as exc:
            LOGGER.warning("Invalid JSON from Searchspring page %s: %s", page, exc)
            time.sleep(1.5)
            continue

        for item in payload.get("results", []):
            style_id = stringify_identifier(item.get("uid"))
            if style_id:
                results[style_id] = item
        pagination = payload.get("pagination") or {}
        total_pages = int(pagination.get("totalPages", 1) or 1)
        if page >= total_pages:
            break
        page += 1
    LOGGER.info("Fetched %s Searchspring styles", len(results))
    return results


def stringify_identifier(raw: Any) -> str:
    if raw in (None, ""):
        return ""
    text = str(raw)
    if text.startswith("gid://"):
        return text.rsplit("/", 1)[-1]
    return text


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
    if amount in (None, ""):
        return ""
    if isinstance(amount, dict):
        amount = amount.get("amount")
    text = str(amount).strip()
    if not text:
        return ""
    if text.startswith("$"):
        text = text[1:]
    text = text.replace(",", "")
    try:
        value = float(text)
    except ValueError:
        return str(amount)
    return f"${value:.2f}"


def extract_style_name(product_title: str) -> str:
    if not product_title:
        return ""
    return product_title.split("-")[0].strip()


def extract_color(product_title: str) -> str:
    if not product_title:
        return ""
    if "|" in product_title:
        return product_title.split("|")[-1].strip()
    return ""


def extract_size_and_inseam(variant_title: str, description: str) -> Tuple[str, str]:
    size = ""
    inseam = ""
    if variant_title:
        parts = [part.strip() for part in variant_title.split("/")]
        if parts:
            size = clean_size(parts[0])
        if len(parts) > 1:
            inseam_candidate = clean_measurement_value(parts[1])
            if inseam_candidate in ALLOWED_INSEAMS:
                inseam = inseam_candidate
    if not inseam:
        inseam = extract_measurement(description, "Inseam")
    return size, inseam


def clean_measurement_value(raw: Any) -> str:
    if raw in (None, ""):
        return ""
    text = str(raw)
    match = re.search(r"\d+(?:\.\d+)?", text)
    return match.group(0) if match else ""


def extract_measurement(description: str, label: str) -> str:
    if not description:
        return ""
    search_area = unescape(description)
    pattern = re.compile(
        rf"{label}\b[^0-9]{{0,40}}(\d+(?:\.\d+)?)",
        re.IGNORECASE,
    )
    match = pattern.search(search_area)
    if match:
        return clean_measurement_value(match.group(1))
    return ""


def clean_size(raw: Optional[str]) -> str:
    if raw in (None, ""):
        return ""
    return str(raw).replace("\"", "").replace("'", "").strip()


def derive_product_type(raw: Optional[str]) -> str:
    if not raw:
        return ""
    parts = raw.strip().split()
    return parts[-1] if parts else ""


def derive_product_line(collections: Iterable[Dict[str, Any]]) -> str:
    ids = {stringify_identifier(edge.get("node", {}).get("id")) for edge in collections}
    if "72340373561" in ids:
        return "Main"
    if "73661087801" in ids:
        return "Plus"
    return ""


def coerce_tags(tags: Optional[Iterable[str]]) -> List[str]:
    if not tags:
        return []
    return [str(tag) for tag in tags if tag not in (None, "")]


def tags_to_string(tags: Iterable[str]) -> str:
    return ", ".join(tags)


def determine_jean_style(title: str, tags: Iterable[str]) -> str:
    if title and "baggy" in title.lower():
        return "Baggy"
    for tag in tags:
        lowered = tag.lower()
        if lowered.startswith("fit:"):
            return tag.split(":", 1)[1].strip().title()
        if lowered.startswith("fit-"):
            return tag.split("-", 1)[1].strip().title()
    return ""


def determine_hem_style(description: str) -> str:
    if not description:
        return ""
    lowered = description.lower()
    if "1/2\" regular hem" in lowered or "0.5\" regular hem" in lowered:
        return "0.5 Regular Hem"
    if "regular hem" in lowered or "clean hem" in lowered:
        return "Regular Hem"
    if "raw hem" in lowered:
        if "released" in lowered:
            return "Released Raw Hem"
        return "Raw Hem"
    if "released" in lowered:
        return "Released Raw Hem"
    return ""


def determine_inseam_style(tags: Iterable[str]) -> str:
    lowered = [tag.lower() for tag in tags]
    if any("length:full" in tag for tag in lowered):
        return "Full"
    if any("length:long" in tag for tag in lowered):
        return "Long"
    if any(tag in {"ankle", "filterwomenankle"} for tag in lowered) or any(
        "length:ankle" in tag for tag in lowered
    ):
        return "Ankle"
    if any(tag in {"crop", "cropped", "filterwomencropped"} for tag in lowered) or any(
        "length:crop" in tag for tag in lowered
    ):
        return "Cropped"
    if any("length:capri" in tag for tag in lowered) or any("length:knee" in tag for tag in lowered) or any(
        "capri" == tag for tag in lowered
    ):
        return "Capri"
    return ""


def determine_rise_label(tags: Iterable[str], title: str) -> str:
    lowered = [tag.lower() for tag in tags]
    title_lower = title.lower() if title else ""
    options: List[Tuple[str, str]] = []
    if any(tag in {"filterwomenhighrise", "highrise"} for tag in lowered) or any(
        "rise:high" in tag or "rise:ultrahigh" in tag or "ultrahighrise" in tag for tag in lowered
    ):
        options.append(("High", "high"))
    if any(tag in {"filterwomenmidrise", "midrise"} for tag in lowered) or any(
        "rise:mid" in tag for tag in lowered
    ):
        options.append(("Mid", "mid"))
    if any(tag in {"filterwomenlowrise", "lowrise"} for tag in lowered) or any(
        "rise:low" in tag for tag in lowered
    ):
        options.append(("Low", "low"))
    if not options:
        return ""
    # choose the label whose keyword appears in the title, fallback to first option
    for label, keyword in options:
        if keyword in title_lower:
            return label
    return options[0][0]


def determine_color_simplified(tags: Iterable[str], color_standardized: str = "") -> str:
    lowered = [tag.lower() for tag in tags]
    if any("wash:mid" in tag or "midwash" in tag for tag in lowered):
        return "Medium"
    if any(
        "wash-black" == tag
        or "wash:black" in tag
        or "wash:dark" in tag
        or "wash:otherdark" in tag
        or "dark" == tag
        for tag in lowered
    ):
        return "Dark"
    if color_standardized and color_standardized.lower() == "black":
        return "Dark"
    if any("wash:white" in tag or "wash:light" in tag or "wash:lightwash" in tag for tag in lowered):
        return "Light"
    if any("wash:other" in tag or "wash:neutrals" in tag for tag in lowered):
        return "Other"
    return ""


COLOR_STANDARDIZED_KEYWORDS = {
    "Blue": [
        "blue",
        "hunt",  # covers Huntington
        "takeoff",
        "vroom",
        "long gone",
        "everafter",
        "far far away",
        "sweetest thing",
        "blue eyed",
        "knight",
        "twilight",
    ],
    "Black": ["black", "depths"],
    "White": ["white"],
    "Red": ["oxblood"],
    "Brown": ["brown", "americano"],
    "Green": ["clover"],
}


def determine_color_standardized(color: str, tags: Iterable[str]) -> str:
    combined = " ".join([color or ""] + list(tags)).lower()
    for label, keywords in COLOR_STANDARDIZED_KEYWORDS.items():
        if any(keyword in combined for keyword in keywords):
            return label
    return ""


def determine_stretch(tags: Iterable[str]) -> str:
    lowered = [tag.lower() for tag in tags]
    for tag in lowered:
        if "stretch-high" in tag or "stretch:high" in tag:
            return "High Stretch"
        if "stretch-low" in tag or "stretch:low" in tag:
            return "Low Stretch"
        if "stretch:rigid" in tag or tag == "rigid":
            return "Rigid"
    if any("sculpting denim" in tag for tag in lowered):
        return "Sculpting Denim"
    return ""


def searchspring_compare_at(item: Dict[str, Any]) -> str:
    msrp = item.get("msrp")
    if msrp in (None, ""):
        return ""
    return format_price(msrp)


def searchspring_promo(item: Dict[str, Any]) -> str:
    badges = item.get("badges") or []
    values = [badge.get("value") for badge in badges if badge.get("value")]
    return " | ".join(values)


def searchspring_instock_percent(item: Dict[str, Any]) -> str:
    pct = item.get("ss_instock_pct")
    if pct in (None, ""):
        return ""
    pct_text = str(pct).strip()
    if pct_text.endswith("%"):
        return pct_text
    return f"{pct_text}%"


def assemble_rows(
    products: List[Dict[str, Any]],
    searchspring_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product in products:
        product_id = stringify_identifier(product.get("id"))
        handle = product.get("handle", "")
        product_title = product.get("title", "")
        description = product.get("description", "")
        vendor = product.get("vendor", "")
        tags = coerce_tags(product.get("tags"))
        tags_string = tags_to_string(tags)
        color = extract_color(product_title)
        style_name = extract_style_name(product_title)
        product_type = derive_product_type(product.get("productType"))
        published_at = parse_date(product.get("publishedAt"))
        created_at = parse_date(product.get("createdAt"))
        product_line = derive_product_line(product.get("collections", {}).get("edges", []))
        if product_title and "PLUS" in product_title.upper():
            product_line = "Plus"
        rise = extract_measurement(description, "Rise")
        leg_opening = extract_measurement(description, "Opening")
        searchspring_item = searchspring_map.get(product_id, {})
        compare_at_price = searchspring_compare_at(searchspring_item)
        promo = searchspring_promo(searchspring_item)
        instock_percent = searchspring_instock_percent(searchspring_item)
        image_url = searchspring_item.get("imageUrl") or searchspring_item.get("thumbnailImageUrl") or ""
        sku_url = product.get("onlineStoreUrl") or (
            f"https://warpweftworld.com/products/{handle}" if handle else ""
        )
        jean_style = determine_jean_style(product_title, tags)
        hem_style = determine_hem_style(description)
        inseam_style = determine_inseam_style(tags)
        rise_label = determine_rise_label(tags, product_title)
        color_standardized = determine_color_standardized(color, tags)
        color_simplified = determine_color_simplified(tags, color_standardized)
        stretch = determine_stretch(tags)
        total_inventory = product.get("totalInventory")
        for variant_edge in product.get("variants", {}).get("edges", []):
            variant = variant_edge.get("node", {})
            variant_id = stringify_identifier(variant.get("id"))
            variant_title = variant.get("title", "")
            size, inseam = extract_size_and_inseam(variant_title, description)
            price = format_price(variant.get("price"))
            row = {
                "Style Id": product_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": product_title,
                "Style Name": style_name,
                "Product Type": product_type,
                "Tags": tags_string,
                "Vendor": vendor,
                "Description": description,
                "Variant Title": f"{product_title} - {size}" if size else product_title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": price,
                "Compare at Price": compare_at_price,
                "Promo": promo,
                "Available for Sale": "TRUE" if variant.get("availableForSale") else "FALSE",
                "Quantity Available": stringify_identifier(variant.get("quantityAvailable")),
                "Quantity of style": stringify_identifier(total_inventory),
                "Instock Percent": instock_percent,
                "SKU - Shopify": variant_id,
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": variant.get("barcode", ""),
                "Product Line": product_line,
                "Image URL": image_url,
                "SKU URL": sku_url,
                "Jean Style": jean_style,
                "Hem Style": hem_style,
                "Inseam Style": inseam_style,
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
            }
            rows.append(row)
    return rows


def write_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("No data rows to write")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"WARPWEFT_{timestamp}.csv"
    output_path = OUTPUT_DIR / filename
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Wrote %s rows to %s", len(rows), output_path)
    return str(output_path)


def main() -> str:
    products = fetch_products()
    if not products:
        raise RuntimeError("No products returned from GraphQL")
    searchspring_map = fetch_searchspring_results()
    rows = assemble_rows(products, searchspring_map)
    if not rows:
        raise RuntimeError("No rows assembled for CSV output")
    return write_csv(rows)


if __name__ == "__main__":
    main()
