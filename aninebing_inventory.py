import csv
import logging
import re
import time
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "aninebing_inventory.log"

STORE_HEADERS = {
    "X-Shopify-Storefront-Access-Token": "31be1df77b861f6705faa6e72aae7711",
    "Content-Type": "application/json",
}

HOST_ROTATION = [
    "https://www.aninebing.com",
    "https://aninebing.com",
    "https://aninebing.myshopify.com",
]

COLLECTION_HANDLES = ["denim-1", "sale-denim"]
ALLOWED_PRODUCT_TYPES = {"pants", "jeans"}

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
    "Inseam Style",
    "Rise Label",
    "Hem Style",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
    "Country Produced",
]

COLLECTION_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    id
    handle
    title
    products(first: 80, after: $cursor) {
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
        featuredImage {
          url
        }
        variants(first: 100) {
          nodes {
            id
            title
            sku
            barcode
            availableForSale
            quantityAvailable
            price {
              amount
            }
            compareAtPrice {
              amount
            }
            selectedOptions {
              name
              value
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


def configure_logging() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    try:
        handlers = [logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()]
    except OSError:
        fallback = OUTPUT_DIR / "aninebing_inventory.log"
        handlers = [logging.FileHandler(fallback, encoding="utf-8"), logging.StreamHandler()]
        logging.warning("Primary log path locked; using fallback %s", fallback)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json,text/plain,*/*",
        }
    )
    return session


def request_with_rotation(
    session: requests.Session,
    path: str,
    *,
    method: str = "GET",
    json_payload: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = 30,
) -> requests.Response:
    last_error: Optional[Exception] = None
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/{path.lstrip('/')}"
        try:
            if method == "POST":
                response = session.post(url, headers=headers, json=json_payload, timeout=timeout)
            else:
                response = session.get(url, headers=headers, timeout=timeout)
            if response.status_code in {404, 410}:
                continue
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logging.warning("Request failed for %s: %s", url, exc)
            time.sleep(0.6)
    raise RuntimeError(f"Unable to request path: {path}. Last error: {last_error}")


def clean_text(html: str) -> str:
    return fix_mojibake(BeautifulSoup(html or "", "html.parser").get_text(" ", strip=True))


def fix_mojibake(text: str) -> str:
    if not text:
        return ""
    replacements = {
        "â€™": "’",
        "â€˜": "‘",
        "â€œ": "“",
        "â€\x9d": "”",
        "â€\x93": "–",
        "â€\x94": "—",
        "â€¦": "…",
        "Â": "",
    }
    fixed = text
    for bad, good in replacements.items():
        fixed = fixed.replace(bad, good)
    return fixed


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return value


def normalize_for_match(text: str) -> str:
    lowered = (text or "").lower()
    lowered = lowered.replace("-", " ")
    lowered = re.sub(r"[^a-z0-9\s]", " ", lowered)
    return re.sub(r"\s+", " ", lowered).strip()


def contains_any(text: str, phrases: Sequence[str]) -> bool:
    normalized = normalize_for_match(text)
    return any(normalize_for_match(phrase) in normalized for phrase in phrases)


def count_occurrences(text: str, phrases: Sequence[str]) -> int:
    normalized = normalize_for_match(text)
    total = 0
    for phrase in phrases:
        token = normalize_for_match(phrase)
        if not token:
            continue
        total += len(re.findall(rf"(?<!\w){re.escape(token)}(?!\w)", normalized))
    return total


def format_product_title(raw_title: str) -> str:
    title = re.sub(r"\s+", " ", raw_title or "").strip()
    match = re.search(r"\b(\d+\s*\")", title)
    if not match:
        return title
    size_token = match.group(1).replace(" ", "")
    without_size = (title[: match.start()] + title[match.end() :]).strip()
    without_size = re.sub(r"\s+", " ", without_size)
    without_size = re.sub(r"\s*-\s*", " - ", without_size).strip(" -")
    if without_size:
        return f"{without_size} - {size_token}"
    return size_token


def parse_style_name(title: str) -> str:
    return (title or "").split("-")[0].strip()


def parse_color(title: str) -> str:
    parts = [part.strip() for part in (title or "").split("-")]
    if len(parts) < 2:
        return ""
    return " - ".join(part for part in parts[1:] if part)


def parse_fractional_number(value: str) -> str:
    token = (value or "").strip().replace('"', "")
    if not token:
        return ""
    parts = token.split()
    total = 0.0
    try:
        for piece in parts:
            if "/" in piece:
                total += float(Fraction(piece))
            else:
                total += float(piece)
    except Exception:  # noqa: BLE001
        return token
    text = f"{total:.4f}".rstrip("0").rstrip(".")
    if "." not in text:
        text += ".0"
    return text


def extract_measurement(product_measurements: str, label: str) -> str:
    if not product_measurements:
        return ""
    pattern = rf"{re.escape(label)}\s*:\s*([^,]+)"
    match = re.search(pattern, product_measurements, flags=re.IGNORECASE)
    if not match:
        return ""
    return parse_fractional_number(match.group(1))


def parse_country(origin: str) -> str:
    if not origin:
        return ""
    country = re.sub(r"^made\s+in\s+", "", origin.strip(), flags=re.IGNORECASE).strip(" .")
    if country.lower() == "turkiye":
        return "Turkey"
    return country


def determine_jean_style(product: str, description: str) -> str:
    product_n = normalize_for_match(product)
    desc_n = normalize_for_match(description)

    step1 = [
        (["flare"], "Flare", product_n),
        (["bootcut", "boot"], "Bootcut", product_n),
        (["skinny"], "Skinny", product_n),
        (["barrel", "bowed", "bow leg", "horseshoe"], "Barrel", product_n),
        (["boyfriend"], "Boyfriend", product_n),
        (["barrel", "bowed", "bow leg", "horseshoe"], "Barrel", desc_n),
        (["flare"], "Flare", desc_n),
        (["bootcut"], "Bootcut", desc_n),
        (["skinny"], "Skinny", desc_n),
        (["relaxed straight", "straight relaxed"], "Straight from Thigh", desc_n),
        (["stright leg", "straight leg", "slim silhouette"], "Straight from Knee", desc_n),
    ]
    for phrases, label, source in step1:
        if any(normalize_for_match(p) in source for p in phrases):
            return label

    baggy_count = count_occurrences(description, ["baggy"])
    wide_count = count_occurrences(description, ["wide leg", "wide-leg"])
    if wide_count > 0 and baggy_count == 0:
        return "Wide Leg"
    if baggy_count > 0 and wide_count > 0:
        if baggy_count > wide_count:
            return "Baggy"
        return "Wide Leg"

    return ""


def determine_inseam_label(description: str, inseam: str) -> str:
    try:
        inseam_value = float(inseam)
    except (TypeError, ValueError):
        return ""
    desc_n = normalize_for_match(description)
    if "long" in desc_n and inseam_value > 32:
        return "Long"
    if "petite" in desc_n and inseam_value < 32:
        return "Petite"
    return ""


def determine_inseam_style(description: str, tags: str, inseam: str) -> str:
    desc_n = normalize_for_match(description)
    tags_n = normalize_for_match(tags)
    try:
        inseam_value = float(inseam)
    except (TypeError, ValueError):
        inseam_value = None

    if "crop" in desc_n:
        return "Crop"
    if "ankle length" in desc_n:
        return "Ankle"
    if contains_any(description, ["full length", "styled long"]):
        return "Full Length"
    if "crop" in desc_n:
        return "Crop"
    if "length ankle" in tags_n and inseam_value is not None and inseam_value < 32:
        return "Ankle"
    if "length crop" in tags_n and inseam_value is not None and inseam_value < 32:
        return "Crop"
    if contains_any(tags, ["full length", "length full"]):
        return "Full Length"
    return ""


def determine_rise_label(description: str) -> str:
    desc_n = normalize_for_match(description)
    if "mid to high rise" in desc_n:
        return "Mid to High"

    if "mid rise intended to be worn low on the hips" in desc_n:
        return "Mid"

    has_mid = "mid rise" in desc_n
    has_high = "high rise" in desc_n
    if has_mid and has_high:
        return "Mid to High"
    if "high rise" in desc_n:
        return "High"
    if "mid rise" in desc_n:
        return "Mid"
    if "low rise" in desc_n:
        return "Low"
    return ""


def determine_hem_style(description: str) -> str:
    rules = [
        (["split hem", "side slits", "side slits"], "Split Hem"),
        (["released hem"], "Released Hem"),
        (["raw hem", "raw edge hem"], "Raw Hem"),
        (["clean hem", "clean edge hem", "tacking detail at bottom hem"], "Clean Hem"),
        (["wide hem", "trouser hem"], "Wide Hem"),
        (["distressed hem"], "Distressed Hem"),
    ]
    for phrases, label in rules:
        if contains_any(description, phrases):
            return label
    return ""


def determine_color_simplified(product: str, description: str) -> str:
    light_medium = ["light to medium", "medium to light", "light medium", "medium light"]
    medium_dark = ["medium to dark", "dark to medium", "dark medium"]
    if contains_any(product, light_medium):
        return "Light to Medium"
    if contains_any(product, medium_dark):
        return "Medium to Dark"
    if contains_any(description, light_medium):
        return "Light to Medium"
    if contains_any(description, medium_dark):
        return "Medium to Dark"
    if contains_any(product, ["dark", "black", "navy"]):
        return "Dark"
    if contains_any(product, ["light", "khaki", "tan", "white", "ivory"]):
        return "Light"
    if contains_any(product, ["medium", "mid blue", "classic blue"]):
        return "Medium"
    if contains_any(description, ["dark", "black", "navy"]):
        return "Dark"
    if contains_any(description, ["light", "khaki", "tan", "white", "ivory"]):
        return "Light"
    if contains_any(description, ["medium", "mid blue", "classic blue"]):
        return "Medium"
    return ""


def determine_color_standardized(product: str, description: str) -> str:
    rules = [
        (["animal print", "leopard", "snake"], "Animal Print"),
        (["blue", "indigo"], "Blue"),
        (["black"], "Black"),
        (["brown"], "Brown"),
        (["green"], "Green"),
        (["grey", "smoke"], "Grey"),
        (["orange"], "Orange"),
        (["pink"], "Pink"),
        (["print"], "Print"),
        (["purple"], "Purple"),
        (["red"], "Red"),
        (["tan", "beige", "khaki"], "Tan"),
        (["white", "ecru"], "White"),
        (["yellow"], "Yellow"),
    ]
    for source in [product, description]:
        for phrases, label in rules:
            if contains_any(source, phrases):
                return label
    return ""


def determine_stretch(description: str) -> str:
    if contains_any(description, ["without stretch", "rigid", "non stretch", "non-stretch"]):
        return "Rigid"
    if contains_any(description, ["stretch"]):
        return "Stretch"
    return ""


def extract_variant_size(variant: dict) -> str:
    options = variant.get("selectedOptions") or []
    for option in options:
        name = (option.get("name") or "").strip().lower()
        if name in {"size", "waist", "option1"}:
            return option.get("value") or ""
    return variant.get("title") or ""


def extract_gid_tail(gid: str, prefix: str) -> str:
    if not gid:
        return ""
    return gid.replace(prefix, "") if gid.startswith(prefix) else gid.split("/")[-1]


def fetch_collection_active(session: requests.Session, handle: str) -> bool:
    for host in HOST_ROTATION:
        if "myshopify" in host:
            continue
        url = f"{host}/collections/{handle}.json"
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                payload = resp.json().get("collection") or {}
                logging.info("Collection %s active at %s (products_count=%s)", handle, host, payload.get("products_count"))
                return True
            if resp.status_code == 404:
                continue
        except Exception as exc:  # noqa: BLE001
            logging.warning("Collection activity check failed for %s: %s", url, exc)
    return False


def fetch_collection_products(session: requests.Session, handle: str) -> List[dict]:
    products: List[dict] = []
    cursor: Optional[str] = None
    while True:
        response = request_with_rotation(
            session,
            "/api/unstable/graphql.json",
            method="POST",
            headers=STORE_HEADERS,
            json_payload={"query": COLLECTION_QUERY, "variables": {"handle": handle, "cursor": cursor}},
        )
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(f"GraphQL errors for collection {handle}: {payload['errors']}")
        collection = ((payload.get("data") or {}).get("collection") or {})
        node = collection.get("products") or {}
        batch = node.get("nodes") or []
        products.extend(batch)
        page_info = node.get("pageInfo") or {}
        logging.info("GraphQL %s: fetched %s products (running total %s)", handle, len(batch), len(products))
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break
    return products


def fetch_view_json(session: requests.Session, handle: str) -> dict:
    resp = request_with_rotation(session, f"/products/{handle}?view=json")
    return resp.json()


def build_rows(products: Iterable[dict], session: requests.Session) -> List[dict]:
    rows: List[dict] = []
    seen_handles = set()
    for product in products:
        handle = product.get("handle") or ""
        if not handle or handle in seen_handles:
            continue
        seen_handles.add(handle)

        product_type = (product.get("productType") or "").strip()
        if product_type.lower() not in ALLOWED_PRODUCT_TYPES:
            continue

        view_json = fetch_view_json(session, handle)
        metafield = (view_json.get("metafields") or [{}])[0] or {}
        product_measurements = metafield.get("product_measurements") or ""

        description = clean_text(product.get("description") or "")
        tags = product.get("tags") or []
        tag_string = ", ".join(tags) if isinstance(tags, list) else str(tags)

        title = product.get("title") or ""
        product_title = format_product_title(title)
        style_name = parse_style_name(title)
        color = parse_color(title)

        rise = extract_measurement(product_measurements, "Rise")
        inseam = extract_measurement(product_measurements, "Inseam")
        leg_opening = extract_measurement(product_measurements, "Leg Opening")

        jean_style = determine_jean_style(product_title, description)
        inseam_label = determine_inseam_label(description, inseam)
        inseam_style = determine_inseam_style(description, tag_string, inseam)
        rise_label = determine_rise_label(description)
        hem_style = determine_hem_style(description)
        color_simplified = determine_color_simplified(product_title, description)
        color_standardized = determine_color_standardized(product_title, description)
        stretch = determine_stretch(description)
        country = parse_country(metafield.get("origin") or "")

        variants = ((product.get("variants") or {}).get("nodes") or [])
        for variant in variants:
            size = extract_variant_size(variant)
            row = {
                "Style Id": extract_gid_tail(product.get("id") or "", "gid://shopify/Product/"),
                "Handle": handle,
                "Published At": format_date(product.get("publishedAt")),
                "Created At": format_date(product.get("createdAt")),
                "Product": product_title,
                "Style Name": style_name,
                "Product Type": product_type.title(),
                "Tags": tag_string,
                "Vendor": product.get("vendor") or "",
                "Description": description,
                "Variant Title": f"{product_title} - {size}" if size else product_title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": variant.get("price", {}).get("amount") or "",
                "Compare at Price": (variant.get("compareAtPrice") or {}).get("amount") or "",
                "Available for Sale": str(bool(variant.get("availableForSale"))).upper(),
                "Quantity Available": variant.get("quantityAvailable") if variant.get("quantityAvailable") is not None else "",
                "Quantity of style": product.get("totalInventory") if product.get("totalInventory") is not None else "",
                "SKU - Shopify": extract_gid_tail(variant.get("id") or "", "gid://shopify/ProductVariant/"),
                "SKU - Brand": variant.get("sku") or "",
                "Barcode": variant.get("barcode") or "",
                "Image URL": (product.get("featuredImage") or {}).get("url") or "",
                "SKU URL": product.get("onlineStoreUrl") or f"https://www.aninebing.com/products/{handle}",
                "Jean Style": jean_style,
                "Inseam Label": inseam_label,
                "Inseam Style": inseam_style,
                "Rise Label": rise_label,
                "Hem Style": hem_style,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
                "Country Produced": country,
            }
            rows.append(row)

    return rows


def write_csv(rows: List[dict]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"ANINEBING_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logging.info("CSV written: %s", output_path.resolve())
    return output_path


def main() -> None:
    configure_logging()
    session = build_session()

    all_products: List[dict] = []
    for handle in COLLECTION_HANDLES:
        if handle == "sale-denim" and not fetch_collection_active(session, handle):
            logging.info("Skipping inactive collection: %s", handle)
            continue
        products = fetch_collection_products(session, handle)
        all_products.extend(products)

    rows = build_rows(all_products, session)
    logging.info("Total variant rows: %s", len(rows))
    write_csv(rows)


if __name__ == "__main__":
    main()
