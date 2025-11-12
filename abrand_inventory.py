"""Abrand Jeans women denim inventory scraper."""
from __future__ import annotations

import csv
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import requests

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "abrand_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "abrand_run.log"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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
    "https://abrand-jeans-us.myshopify.com",
    "https://us.abrandjeans.com",
    "https://abrandjeans.com",
    "https://www.abrandjeans.com",
]

STOREFRONT_TOKEN = "0b6d0854f3c383eed95001cd65faa8c1"
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
            headers_with_default = {**headers}
            response = SESSION.post(
                url,
                json=payload,
                headers=headers_with_default,
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
            "GraphQL request to %s returned %s: %s", url, response.status_code, response.text[:200]
        )
        last_error = RuntimeError(f"Status {response.status_code}")
    raise RuntimeError(f"All GraphQL hosts failed: {last_error}")


def parse_shopify_id(raw_id: str) -> str:
    if not raw_id:
        return ""
    return raw_id.split("/")[-1]


def format_published_at(value: Optional[str]) -> str:
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
    # Mixed fraction e.g. "29 3/4"
    if " " in text and "/" in text:
        whole, fraction = text.split(" ", 1)
        try:
            return float(whole) + parse_numeric_text(fraction)
        except (ValueError, TypeError):
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
        inch_pattern = rf"{candidate}\s*:?[^\d]*(\d+(?:\.\d+)?)\s*(?:in\b|inch(?:es)?)"
        inch_match = re.search(inch_pattern, description, flags=re.IGNORECASE)
        if inch_match:
            try:
                return f"{float(inch_match.group(1)):.2f}"
            except ValueError:
                pass
    return ""


def derive_rise_label(description: str, tags: Iterable[str]) -> str:
    lowered = (description or "").lower()
    if "low rise" in lowered or "low-rise" in lowered:
        return "Low"
    if "mid rise" in lowered or "mid-rise" in lowered:
        return "Mid"
    if "high rise" in lowered or "high-rise" in lowered or "high waisted" in lowered:
        return "High"
    tagged = tag_value(tags, "rise:")
    if tagged:
        value = tagged.strip().title()
        if value.lower() in {"low", "mid", "high"}:
            return value
        return value
    return ""


def derive_jean_style(title: str, tags: Iterable[str]) -> str:
    lowered = (title or "").lower()
    if any(keyword in lowered for keyword in ("kick", "flare")):
        return "Flare"
    if "boot" in lowered:
        return "Boot"
    if "straight" in lowered:
        return "Straight Leg"
    if "slim" in lowered:
        return "Slim"
    if "wide" in lowered:
        return "Wide Leg"
    if "skinny" in lowered:
        return "Skinny"
    if "barrel" in lowered:
        return "Barrel"
    if "carrie" in lowered or "baggy" in lowered:
        return "Baggy"

    # Fallback to tag-driven hints
    leg_tag = tag_value(tags, "leg:")
    if leg_tag:
        leg_lower = leg_tag.lower()
        if "flare" in leg_lower:
            return "Flare"
        if "boot" in leg_lower:
            return "Boot"
        if "straight" in leg_lower:
            return "Straight Leg"
        if "slim" in leg_lower:
            return "Slim"
        if "wide" in leg_lower:
            return "Wide Leg"
        if "skinny" in leg_lower:
            return "Skinny"
        if "barrel" in leg_lower:
            return "Barrel"
        if "baggy" in leg_lower:
            return "Baggy"

    fit_swatch = (tag_value(tags, "fit_swatch:") or "").lower()
    if fit_swatch:
        if "flare" in fit_swatch:
            return "Flare"
        if "boot" in fit_swatch:
            return "Boot"
        if "straight" in fit_swatch:
            return "Straight Leg"
        if "slim" in fit_swatch:
            return "Slim"
        if "wide" in fit_swatch:
            return "Wide Leg"
        if "skinny" in fit_swatch:
            return "Skinny"
        if "barrel" in fit_swatch:
            return "Barrel"
        if "baggy" in fit_swatch or "carrie" in fit_swatch:
            return "Baggy"

    return ""


def derive_inseam_label(tags: Iterable[str]) -> str:
    length = tag_value(tags, "length:")
    if not length:
        return ""
    length_lower = length.lower()
    if length_lower == "petite":
        return "Petite"
    if length_lower == "tall":
        return "Long"
    if length_lower == "regular":
        return "Regular"
    return length


def color_from_title(title: str) -> str:
    if not title:
        return ""
    tokens = [
        "Tall",
        "Skean",
        "Petite",
        "-",
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
    for token in tokens:
        if token == "-":
            if "-" in title:
                return title.split("-", 1)[1].strip(" -")
            continue
        pattern = re.compile(rf"\b{re.escape(token)}\b", re.IGNORECASE)
        match = pattern.search(title)
        if match:
            color = title[match.end():].strip(" -")
            if color:
                return color
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
    for prefix in ("option_2_label:", "option_2:"):
        value = tag_value(tags, prefix)
        if value:
            return value
    return ""


def derive_site_exclusive(tags: Iterable[str]) -> str:
    for tag in tags:
        if tag and "exclusive" in tag.lower():
            return "Online Exclusives"
    return ""


def normalize_quantity(value: Optional[int]) -> str:
    if value is None:
        return "0"
    return str(value)


def build_variant_rows(products: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for product in products:
        tags = clean_tags(product.get("tags", []))
        if any(tag.lower().startswith("gender:guys") for tag in tags if tag):
            LOGGER.info("Skipping product %s because it is tagged for guys", product.get("handle"))
            continue

        style_id = parse_shopify_id(product.get("id", ""))
        handle = product.get("handle", "")
        title = product.get("title", "")
        published_at = format_published_at(product.get("publishedAt"))
        created_at = format_published_at(product.get("createdAt"))
        description = product.get("description", "")
        vendor = product.get("vendor", "")
        product_type = product.get("productType", "")
        total_inventory = normalize_quantity(product.get("totalInventory"))
        online_url = (
            product.get("onlineStoreUrl")
            or (f"https://us.abrandjeans.com/products/{handle}" if handle else "")
        )
        featured_image = (product.get("featuredImage") or {}).get("url", "")
        options = product.get("options", [])

        style_name = (
            tag_value(tags, "fit_swatch:")
            or tag_value(tags, "fit:")
            or title.split("-", 1)[0].strip()
        )

        rise_cm = extract_measure(description, "Rise", ["Front Rise"])
        inseam_cm = extract_measure(description, "Inseam", ["Inleg", "Inner Leg Length"])
        leg_opening_cm = extract_measure(description, "Hem", ["Leg Opening"])
        rise_label = derive_rise_label(description, tags)

        country = tag_value(tags, "country_of_origin:") or ""
        stretch = tag_value(tags, "stretch:") or ""
        site_exclusive = derive_site_exclusive(tags)
        jean_style = derive_jean_style(title, tags)
        inseam_label = derive_inseam_label(tags)
        color_simplified = derive_color_simplified(tags)

        gender = ""
        for tag in tags:
            lower = tag.lower()
            if lower.startswith("gender:girls") or "gender:gal" in lower or lower.startswith("gender:women"):
                gender = "Women"
                break

        variants = product.get("variants", {}).get("edges", [])
        if not variants:
            LOGGER.warning("Product %s has no variants; skipping", handle)
            continue

        for edge in variants:
            node = edge.get("node") or {}
            selected_map = {opt.get("name", "").lower(): (opt.get("value") or "") for opt in node.get("selectedOptions", [])}
            size = selected_map.get("size") or selected_map.get("option2") or ""
            variant_id = parse_shopify_id(node.get("id", ""))
            sku = node.get("sku", "")
            barcode = node.get("barcode", "")
            available = bool_to_str(node.get("availableForSale"))
            quantity_available = normalize_quantity(node.get("quantityAvailable"))
            price = format_price((node.get("priceV2") or {}).get("amount"))
            compare_at = format_price((node.get("compareAtPriceV2") or {}).get("amount"))
            variant_image = (node.get("image") or {}).get("url")
            image_url = variant_image or featured_image
            color = (
                color_from_title(title)
                or selected_map.get("colour")
                or selected_map.get("color")
                or ""
            )
            color_standardized = derive_color_standardized(options, selected_map)

            variant_title_parts = [style_name or title]
            if color:
                variant_title_parts.append(color)
            if size:
                variant_title_parts.append(size)
            variant_title = " - ".join(part for part in variant_title_parts if part)

            row = {
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": title,
                "Style Name": style_name or title,
                "Product Type": product_type,
                "Tags": ", ".join(tags),
                "Vendor": vendor,
                "Description": description.strip(),
                "Variant Title": variant_title,
                "Color": color,
                "Size": size,
                "Rise": rise_cm,
                "Inseam": inseam_cm,
                "Leg Opening": leg_opening_cm,
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
                "Color - Standardized": color_standardized,
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
    output_path = OUTPUT_DIR / f"ABRAND_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    LOGGER.info("CSV written: %s", output_path)
    return output_path


def main() -> None:
    LOGGER.info("Starting Abrand Jeans scrape")
    products = fetch_products()
    rows = build_variant_rows(products)
    LOGGER.info("Assembled %s rows", len(rows))
    if not rows:
        raise SystemExit("No rows generated; aborting")
    write_csv(rows)


if __name__ == "__main__":
    main()

