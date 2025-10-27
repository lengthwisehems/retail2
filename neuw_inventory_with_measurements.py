"""Neuw Denim monthly scraper with HTML measurement fallback."""
from __future__ import annotations

import csv
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "neuw_measurements_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "neuw_measurements_run.log"

OUTPUT_DIR.mkdir(exist_ok=True)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(DEFAULT_HEADERS)

HOSTS: List[str] = [
    "https://neuw-denim-us.myshopify.com",
    "https://us.neuwdenim.com",
    "https://www.neuwdenim.com",
    "https://neuwdenim.com",
]

HTML_HOSTS: List[str] = [
    "https://us.neuwdenim.com",
    "https://www.neuwdenim.com",
    "https://neuwdenim.com",
]

STOREFRONT_TOKEN = "5de3871e151d2f64ba341e23f7feefd4"
GRAPHQL_PATH = "/api/2023-01/graphql.json"
GRAPHQL_PAGE_SIZE = 100

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
    "Available for Sale",
    "Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Label",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Country Produced",
    "Stretch",
    "Site Exclusive",
    "Gender",
]


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
    first: $pageSize
    after: $cursor
    query: "collection:womens AND tag:'category:Jeans' AND tag:'gender:Girls'"
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
        updatedAt
        publishedAt
        onlineStoreUrl
        totalInventory
        featuredImage { url altText }
        options { id name values }
        collections(first: 10) {
          edges { node { id handle title } }
        }
        variants(first: 250) {
          edges {
            node {
              id
              sku
              title
              availableForSale
              currentlyNotInStock
              quantityAvailable
              requiresShipping
              barcode
              selectedOptions { name value }
              priceV2 { amount currencyCode }
              compareAtPriceV2 { amount currencyCode }
              image { url altText }
            }
          }
        }
      }
    }
    pageInfo { hasNextPage endCursor }
  }
}
"""


def graphql_request(variables: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"query": GRAPHQL_QUERY, "variables": variables}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
    }
    last_error: Optional[Exception] = None
    for base in HOSTS:
        url = f"{base.rstrip('/')}{GRAPHQL_PATH}"
        try:
            response = SESSION.post(
                url,
                json=payload,
                headers=headers,
                timeout=30,
                proxies={},
            )
        except requests.RequestException as exc:
            last_error = exc
            LOGGER.warning("GraphQL request to %s failed: %s", url, exc)
            continue
        if response.status_code == 200:
            data = response.json()
            if data.get("errors"):
                raise RuntimeError(data["errors"])
            return data["data"]
        LOGGER.warning(
            "GraphQL request to %s returned %s: %s",
            url,
            response.status_code,
            response.text[:200],
        )
        last_error = RuntimeError(f"Status {response.status_code}")
    raise RuntimeError(f"All GraphQL hosts failed: {last_error}")


def parse_shopify_id(raw_id: str) -> str:
    if not raw_id:
        return ""
    return raw_id.split("/")[-1]


def format_iso_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return value


def format_price(value: Optional[str]) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return str(value)


def bool_to_str(flag: Optional[bool]) -> str:
    return "TRUE" if flag else "FALSE"


def clean_tags(tags: Iterable[str]) -> List[str]:
    result = []
    for tag in tags:
        cleaned = (tag or "").strip()
        if cleaned:
            result.append(cleaned)
    return result


def tag_value(tags: Iterable[str], prefix: str) -> Optional[str]:
    prefix_lower = prefix.lower()
    for tag in tags:
        if tag and tag.lower().startswith(prefix_lower):
            _, _, remainder = tag.partition(":")
            return remainder.strip()
    return None


def inches_from_cm(cm_value: float) -> str:
    inches = cm_value * 0.393700787
    return f"{round(inches + 1e-9, 2):.2f}"


def parse_numeric_text(text: str) -> Optional[float]:
    text = text.strip()
    if not text:
        return None
    if " " in text and "/" in text:
        whole, fraction = text.split(" ", 1)
        try:
            return float(whole) + parse_numeric_text(fraction)
        except (TypeError, ValueError):
            return None
    if "/" in text:
        num, _, denom = text.partition("/")
        try:
            return float(num) / float(denom)
        except (ValueError, ZeroDivisionError):
            return None
    try:
        return float(text)
    except ValueError:
        return None


def extract_measure(description: str, label: str, extra_labels: Optional[List[str]] = None) -> str:
    if not description:
        return ""
    candidates = [label]
    if extra_labels:
        candidates.extend(extra_labels)
    for candidate in candidates:
        pattern = rf"{candidate}\s*:?\s*([\d./\s]+)\s*cm"
        match = re.search(pattern, description, flags=re.IGNORECASE)
        if match:
            value = parse_numeric_text(match.group(1))
            if value is not None:
                return inches_from_cm(value)
    for candidate in candidates:
        label_match = re.search(candidate, description, flags=re.IGNORECASE)
        segment = description[label_match.start() :] if label_match else description
        inch_match = re.search(
            r"(\d[\d\s/\.]*)\s*(?:in|inch|inches)\b",
            segment,
            flags=re.IGNORECASE,
        )
        if inch_match:
            value_text = inch_match.group(1).strip()
            value = parse_numeric_text(value_text)
            if value is not None:
                return f"{value:.2f}"
    return ""


def derive_style_name(tags: Iterable[str], title: str) -> str:
    fit_swatch = tag_value(tags, "fit_swatch:")
    if fit_swatch:
        return fit_swatch
    fit = tag_value(tags, "fit:")
    if fit:
        return fit
    return title.split("-", 1)[0].strip()


COLOR_SPLIT_TOKENS = [
    "-",
    "Tall",
    "Skean",
    "Petite",
    "Jean",
    "Kick",
    "Flare",
    "Straight",
    "Slim",
    "Wide",
    "Skinny",
    "Barrel",
    "Baggy",
]


def derive_color(title: str) -> str:
    if not title:
        return ""
    for token in COLOR_SPLIT_TOKENS:
        pattern = re.compile(re.escape(token), re.IGNORECASE)
        match = pattern.search(title)
        if match:
            remainder = title[match.end():].strip(" -")
            if remainder:
                return remainder.strip()
    return ""


def derive_color_standardized(options: Iterable[Dict[str, Any]], selected: Dict[str, str]) -> str:
    colour = selected.get("colour") or selected.get("color")
    if colour:
        return colour
    for option in options:
        name = (option.get("name") or "").lower()
        if name in ("colour", "color"):
            values = option.get("values") or []
            if values:
                return str(values[0]).strip()
    return ""


def derive_color_simplified(tags: Iterable[str]) -> str:
    return tag_value(tags, "option_2_label:") or ""


def derive_site_exclusive(tags: Iterable[str]) -> str:
    for tag in tags:
        if tag and "exclusive" in tag.lower():
            return "Online Exclusives"
    return ""


def derive_jean_style(title: str) -> str:
    lowered = title.lower()
    if any(keyword in lowered for keyword in ("bell", "flare")):
        return "Flare"
    if any(keyword in lowered for keyword in ("straight", "relaxed", "boyfriend")):
        return "Straight Leg"
    if "slim" in lowered:
        return "Slim"
    if "wide" in lowered:
        return "Wide Leg"
    if "skinny" in lowered:
        return "Skinny"
    if "barrel" in lowered:
        return "Barrel"
    if "baggy" in lowered:
        return "Baggy"
    if "boot" in lowered:
        return "Boot"
    if "capri" in lowered:
        return "Capri"
    return ""


def parse_size_and_inseam_from_sku(sku: str) -> Tuple[str, Optional[str]]:
    if not sku:
        return "", None
    last_segment = sku.split("-")[-1].strip()
    if "/" in last_segment:
        size_part, _, inseam_part = last_segment.partition("/")
        return size_part.strip(), inseam_part.strip() or None
    return last_segment, None


def normalize_stretch_value(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    lowered = value.strip().lower()
    if not lowered:
        return None
    if "comfort" in lowered and "stretch" in lowered:
        return "comfort-stretch"
    if "rigid" in lowered:
        return "Rigid"
    if "stretch" in lowered:
        return "stretch"
    return None


def derive_stretch(tags: Iterable[str], description: str) -> str:
    for prefix in ("strech:", "stretch:", "fabric:"):
        candidate = normalize_stretch_value(tag_value(tags, prefix))
        if candidate:
            return candidate
    lowered = description.lower()
    if "comfort-stretch" in lowered:
        return "comfort-stretch"
    if "rigid" in lowered:
        return "Rigid"
    if "stretch" in lowered:
        return "stretch"
    return ""


INSEAM_LABEL_RULES = {
    ("skinny", 30): "Regular",
    ("skinny", 32): "Long",
    ("straight leg", 28): "Petite",
    ("straight leg", 30): "Regular",
    ("straight leg", 32): "Regular",
    ("straight leg", 34): "Long",
    ("baggy", 30): "Petite",
    ("baggy", 32): "Regular",
}


def determine_inseam_label(jean_style: str, inseam_value: str) -> str:
    if not jean_style or not inseam_value:
        return ""
    match = re.search(r"\d+(?:\.\d+)?", inseam_value)
    if not match:
        return ""
    try:
        inseam_num = float(match.group(0))
    except ValueError:
        return ""
    key = (jean_style.lower(), int(round(inseam_num)))
    return INSEAM_LABEL_RULES.get(key, "")


COLLECTION_RISE_MAP = {
    "gid://shopify/Collection/271375597703": "Low",
    "gid://shopify/Collection/298149380231": "High",
    "gid://shopify/Collection/278829334663": "Mid",
}


def derive_rise_label(description: str, collections: Iterable[Dict[str, Any]]) -> str:
    for edge in collections:
        node = edge.get("node") or {}
        collection_id = node.get("id")
        if collection_id in COLLECTION_RISE_MAP:
            return COLLECTION_RISE_MAP[collection_id]
    if not description:
        return ""
    lowered = description.lower()
    if "low rise" in lowered or "low-rise" in lowered:
        return "Low"
    if "mid rise" in lowered or "mid-rise" in lowered:
        return "Mid"
    if "high rise" in lowered or "high-rise" in lowered or "high waisted" in lowered:
        return "High"
    return ""


def normalize_quantity(value: Optional[int]) -> str:
    if value is None:
        return "0"
    return str(value)


MEASUREMENT_CACHE: Dict[str, Dict[str, str]] = {}


def fetch_measurements_from_html(handle: str) -> Dict[str, str]:
    if not handle:
        return {}
    if handle in MEASUREMENT_CACHE:
        return MEASUREMENT_CACHE[handle]

    for base in HTML_HOSTS:
        url = f"{base.rstrip('/')}/products/{handle}"
        try:
            response = SESSION.get(url, headers=DEFAULT_HEADERS, timeout=30)
        except requests.RequestException as exc:
            LOGGER.warning("Measurement fetch for %s failed: %s", url, exc)
            continue
        if response.status_code != 200:
            LOGGER.warning("Measurement fetch for %s returned %s", url, response.status_code)
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        measurement: Dict[str, str] = {}
        for li in soup.select("li"):
            text = li.get_text(" ", strip=True)
            if not text:
                continue
            lowered = text.lower()
            if "rise" in lowered:
                value = extract_measure(text, "Rise", ["Front Rise"])
                if value and "rise" not in measurement:
                    measurement["rise"] = value
            if any(keyword in lowered for keyword in ("inseam", "inner leg length", "inleg")):
                value = extract_measure(text, "Inseam", ["Inner Leg Length", "Inleg"])
                if value and "inseam" not in measurement:
                    measurement["inseam"] = value
            if any(keyword in lowered for keyword in ("hem", "leg opening")):
                value = extract_measure(text, "Hem", ["Leg Opening"])
                if value and "leg_opening" not in measurement:
                    measurement["leg_opening"] = value
        if measurement:
            MEASUREMENT_CACHE[handle] = measurement
            LOGGER.info("Measurements hydrated from HTML for %s", handle)
            return measurement
    MEASUREMENT_CACHE[handle] = {}
    return {}


def build_variant_rows(products: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for product in products:
        tags = clean_tags(product.get("tags", []))
        style_id = parse_shopify_id(product.get("id", ""))
        handle = product.get("handle", "")
        title = product.get("title", "")
        description = product.get("description", "")
        vendor = product.get("vendor", "")
        product_type = product.get("productType", "")
        published_at = format_iso_date(product.get("publishedAt"))
        created_at = format_iso_date(product.get("createdAt"))
        total_inventory = normalize_quantity(product.get("totalInventory"))
        online_url = product.get("onlineStoreUrl") or (
            f"https://us.neuwdenim.com/products/{handle}" if handle else ""
        )
        featured_image = (product.get("featuredImage") or {}).get("url", "")
        options = product.get("options", [])
        collections = (product.get("collections") or {}).get("edges", [])

        style_name = derive_style_name(tags, title)
        rise_value = extract_measure(description, "Rise", ["Front Rise"])
        inseam_value = extract_measure(description, "Inseam", ["Inner Leg Length", "Inleg"])
        leg_opening_value = extract_measure(description, "Hem", ["Leg Opening"])
        html_measurements: Dict[str, str] = {}

        if not rise_value or not inseam_value or not leg_opening_value:
            html_measurements = fetch_measurements_from_html(handle)
            if not rise_value:
                rise_value = html_measurements.get("rise", "")
            if not inseam_value:
                inseam_value = html_measurements.get("inseam", "")
            if not leg_opening_value:
                leg_opening_value = html_measurements.get("leg_opening", "")

        rise_label = derive_rise_label(description, collections)
        country = tag_value(tags, "country_of_origin:") or ""
        stretch = derive_stretch(tags, description)
        site_exclusive = derive_site_exclusive(tags)
        color_simplified = derive_color_simplified(tags)
        jean_style = derive_jean_style(title)
        gender = "Women"

        variants = product.get("variants", {}).get("edges", [])
        if not variants:
            LOGGER.warning("Product %s has no variants; skipping", handle)
            continue

        for edge in variants:
            node = edge.get("node") or {}
            selected_map = {
                opt.get("name", "").lower(): (opt.get("value") or "")
                for opt in node.get("selectedOptions", [])
            }
            sku = node.get("sku", "")
            size_from_sku, inseam_from_sku = parse_size_and_inseam_from_sku(sku)
            size = size_from_sku or selected_map.get("size") or selected_map.get("option2") or ""
            leg_option = selected_map.get("leg") or selected_map.get("inseam") or ""
            variant_inseam = inseam_from_sku or leg_option or inseam_value
            if not variant_inseam:
                variant_inseam = ""
            variant_color = (
                derive_color(title)
                or selected_map.get("colour")
                or selected_map.get("color")
                or ""
            )

            variant_id = parse_shopify_id(node.get("id", ""))
            barcode = node.get("barcode", "")
            available = bool_to_str(node.get("availableForSale"))
            quantity_available = normalize_quantity(node.get("quantityAvailable"))
            price = format_price((node.get("priceV2") or {}).get("amount"))
            compare_at = format_price((node.get("compareAtPriceV2") or {}).get("amount"))
            variant_image = (node.get("image") or {}).get("url")
            image_url = variant_image or featured_image

            variant_title = " - ".join(
                [value for value in (style_name or title, variant_color, size) if value]
            )

            inseam_label = determine_inseam_label(jean_style, variant_inseam or html_measurements.get("inseam", ""))

            row = {
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": title,
                "Style Name": style_name,
                "Product Type": product_type,
                "Tags": ", ".join(tags),
                "Vendor": vendor,
                "Description": description.strip(),
                "Variant Title": variant_title or title,
                "Color": variant_color,
                "Size": size,
                "Rise": rise_value,
                "Inseam": variant_inseam or html_measurements.get("inseam", ""),
                "Leg Opening": leg_opening_value,
                "Price": price,
                "Compare at Price": compare_at,
                "Available for Sale": available,
                "Quantity Available": quantity_available,
                "Quantity of style": total_inventory,
                "SKU - Shopify": variant_id,
                "SKU - Brand": sku,
                "Barcode": barcode,
                "Image URL": image_url or "",
                "SKU URL": online_url,
                "Jean Style": jean_style,
                "Inseam Label": inseam_label,
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": derive_color_standardized(options, selected_map),
                "Country Produced": country,
                "Stretch": stretch,
                "Site Exclusive": site_exclusive,
                "Gender": gender,
            }
            rows.append(row)
    return rows


def fetch_products() -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    page = 1
    while True:
        LOGGER.info("Requesting product page %s", page)
        data = graphql_request({"cursor": cursor, "pageSize": GRAPHQL_PAGE_SIZE})
        connection = data.get("products") or {}
        edges = connection.get("edges") or []
        if not edges:
            LOGGER.info("No edges returned; stopping pagination")
            break
        for edge in edges:
            node = edge.get("node")
            if node:
                products.append(node)
        page_info = connection.get("pageInfo") or {}
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
            page += 1
            continue
        break
    LOGGER.info("Fetched %s products", len(products))
    return products


def write_csv(rows: List[Dict[str, str]]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"NEUW_Measurements_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    LOGGER.info("CSV written: %s", output_path)
    return output_path


def main() -> None:
    LOGGER.info("Starting Neuw Denim measurement scrape")
    products = fetch_products()
    rows = build_variant_rows(products)
    LOGGER.info("Assembled %s rows", len(rows))
    if not rows:
        raise SystemExit("No rows generated; aborting")
    write_csv(rows)


if __name__ == "__main__":
    main()
