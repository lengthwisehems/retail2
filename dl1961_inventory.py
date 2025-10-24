"""DL1961 women denim inventory scraper."""
from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "dl1961_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "dl1961_run.log"

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
    "https://dl1961trial.myshopify.com",
    "https://www.dl1961.com",
    "https://dl1961.com",
]

SEARCHSPRING_URL = "https://8176gy.a.searchspring.io/api/search/autocomplete.json"
SEARCHSPRING_PARAMS = {
    "siteId": "8176gy",
    "resultsFormat": "json",
    "resultsPerPage": 250,
    "q": "women jean",
}

STOREFRONT_TOKEN = "d66ac22abacd5c3978abe95b55eaa3df"
GRAPHQL_PATH = "/api/2023-04/graphql.json"
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
    "Promo",
    "Available for Sale",
    "Quantity Available",
    "Google Analytics Purchases",
    "Quantity of style",
    "Instock Percent",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Product Line",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Label",
    "Inseam Style",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
    "Gender",
]

EXCLUDED_PRODUCT_TYPES = {
    "short",
    "relaxed",
    "pleated",
    "trouser",
    "off",
    "top",
    "length",
    "sweater",
    "mini",
    "kneelength",
    "front",
    "jacket",
}

PRODUCT_TYPE_TO_KEEP = {
    "jeans",
    "pants",
    "maternity",
    "ankle",
    "crop",
    "instasculpt",
    "leg",
    "rise",
}

EXCLUDED_TAGS = {
    "men",
    "kids",
    "knit",
    "fabriccrochet",
    "30offmen",
    "discfullpricemen",
    "men_slim",
    "mendlultimate",
    "menexchanges",
    "mens",
    "mensblackdenim",
    "ymal|mennewarrivals]",
    "ymal|mensdarkwashcollection",
}

EXCLUDED_TITLE_KEYWORDS = ["short"]


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
  products(first: $pageSize, after: $cursor, query: "product_type:Women AND jeans") {
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
        collections(first: 20) {
          edges {
            node { id handle title }
          }
        }
        metafield(namespace: "custom", key: "fabricdescription") { value }
        variants(first: 250) {
          edges {
            node {
              id
              sku
              title
              availableForSale
              currentlyNotInStock
              quantityAvailable
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


@dataclass
class SearchspringHit:
    style_id: Optional[str]
    handle: str
    product_type_raw: str
    vendor: Optional[str]
    promo: Optional[str]
    ga_purchases: Optional[int]
    instock_pct: Optional[str]
    tags: List[str]


def parse_shopify_id(raw_id: str) -> str:
    return raw_id.rsplit("/", 1)[-1]


def money_amount(node: Dict[str, Any], key: str) -> str:
    value = node.get(key)
    if isinstance(value, dict):
        return value.get("amount") or ""
    return ""


def iso_to_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return dt.strftime("%m/%d/%Y")


def clean_tags(tags: Iterable[str]) -> List[str]:
    return [t.strip() for t in tags if t.strip()]


def fetch_searchspring() -> Dict[str, SearchspringHit]:
    LOGGER.info("Fetching Searchspring autocomplete results")
    hits: Dict[str, SearchspringHit] = {}
    page = 1
    while True:
        params = {**SEARCHSPRING_PARAMS, "page": page}
        try:
            resp = SESSION.get(SEARCHSPRING_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(f"Searchspring request failed: {exc}") from exc
        payload = resp.json()
        results = payload.get("results") or []
        LOGGER.info("Searchspring page %s -> %s hits", page, len(results))
        if not results:
            break
        for item in results:
            handle = (item.get("handle") or "").strip()
            if not handle:
                continue
            title = (item.get("title") or "").lower()
            product_type = (item.get("product_type_unigram") or "").lower()
            if product_type in EXCLUDED_PRODUCT_TYPES:
                continue
            if product_type == "rise" and "short" in title:
                continue
            if product_type and product_type not in PRODUCT_TYPE_TO_KEEP:
                continue
            if any(keyword in title for keyword in EXCLUDED_TITLE_KEYWORDS):
                continue
            tags = clean_tags(item.get("tags", []))
            if any(tag.lower() in EXCLUDED_TAGS for tag in tags):
                continue
            uid = item.get("uid")
            style_id = str(uid) if uid is not None else None
            badges = item.get("badges") or []
            promo_value = None
            if badges:
                values = [b.get("value") for b in badges if isinstance(b, dict)]
                promo_value = "; ".join([v for v in values if v]) or None
            ga_purchases = item.get("ga_unique_purchases")
            instock_pct = item.get("ss_instock_pct")
            if instock_pct not in (None, ""):
                try:
                    instock_pct = f"{float(instock_pct):.0f}%"
                except (TypeError, ValueError):
                    instock_pct = str(instock_pct)
                    if not instock_pct.endswith("%"):
                        instock_pct = f"{instock_pct}%"
            vendor = item.get("brand")
            hits[handle] = SearchspringHit(
                style_id=style_id,
                handle=handle,
                product_type_raw=product_type or "",
                vendor=vendor,
                promo=promo_value,
                ga_purchases=int(ga_purchases) if isinstance(ga_purchases, (int, float)) else None,
                instock_pct=instock_pct,
                tags=tags,
            )
        page += 1
    return hits


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
            response = SESSION.post(url, json=payload, headers=headers, timeout=30)
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


def fetch_storefront_products() -> List[Dict[str, Any]]:
    LOGGER.info("Fetching Shopify Storefront products")
    products: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        data = graphql_request({"cursor": cursor, "pageSize": GRAPHQL_PAGE_SIZE})
        connection = data["products"]
        edges = connection.get("edges", [])
        for edge in edges:
            products.append(edge["node"])
        LOGGER.info("Storefront page fetched -> total %s products", len(products))
        page_info = connection.get("pageInfo", {})
        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break
    return products


def slugify_fit(value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()
    return cleaned.title()


def extract_style_name(tags: Iterable[str]) -> str:
    for tag in tags:
        if tag.lower().startswith("fit:"):
            return slugify_fit(tag.split(":", 1)[1])
        if tag.lower().startswith("fit_swatch:"):
            return slugify_fit(tag.split(":", 1)[1])
    return ""


def determine_product_type(hit: Optional[SearchspringHit], title: str) -> Optional[str]:
    if hit is None:
        return "Jeans"
    raw = hit.product_type_raw
    if raw in EXCLUDED_PRODUCT_TYPES:
        return None
    if raw == "rise" and "short" in title.lower():
        return None
    if raw and raw not in PRODUCT_TYPE_TO_KEEP:
        return None
    return "Jeans"


def determine_jean_style(tags: Iterable[str], title: str, description: str) -> str:
    lower_tags = {t.lower() for t in tags}
    title_lower = title.lower()
    description_lower = description.lower()
    if any(word in lower_tags for word in {"barrel", "widebarrel"}):
        return "Barrel"
    if any(word in lower_tags for word in {"boot", "bootcut", "bootflare", "filterbootflare"}):
        return "Boot"
    if any(word in lower_tags for word in {"boyfriend", "boyfriendrelaxed"}):
        return "Baggy"
    if any(word in lower_tags for word in {"filterwideleg", "wide", "wideleg", "widelegs"}):
        return "Wide"
    if any(word in lower_tags for word in {"filterstraight", "straight"}):
        return "Straight"
    if any("flare" == tag or "flare" in tag for tag in lower_tags):
        return "Flare"
    if any("skinny" == tag or "skinny" in tag for tag in lower_tags):
        return "Skinny"
    if any(word in lower_tags for word in {"cigarette", "slim", "slimstraight"}):
        return "Straight from the Knee"
    if "soft curve" in title_lower:
        return "Barrel"
    if "tapered" in title_lower:
        return "Tapered But Loose at the Knee"
    if "barrel" in title_lower or "barrel" in description_lower:
        return "Barrel"
    if "boot" in title_lower or "boot" in description_lower:
        return "Boot"
    if "flare" in title_lower or "flare" in description_lower:
        return "Flare"
    if "skinny" in title_lower or "skinny" in description_lower:
        return "Skinny"
    if "wide" in title_lower or "wide" in description_lower:
        return "Wide"
    if "straight" in title_lower or "straight" in description_lower:
        return "Straight"
    if "baggy" in title_lower or "boyfriend" in title_lower:
        return "Baggy"
    if "capri" in title_lower:
        return "Capri"
    return ""


def determine_inseam_label(
    jean_style: str,
    inseam_value: Optional[float],
    tags: Iterable[str],
    product_title: str,
    size_value: str,
) -> str:
    lower_tags = {t.lower() for t in tags}
    title_lower = product_title.lower()
    if "petite" in title_lower or "petite" in lower_tags or any(part.endswith("p") for part in size_value.lower().split()):
        return "Petite"
    if "length:petite" in lower_tags:
        return "Petite"
    if "length:long" in lower_tags or "tall" in lower_tags:
        return "Long"
    if "regular" in lower_tags or "length:regular" in lower_tags:
        return "Regular"
    if jean_style == "Skinny" and inseam_value in {30}:
        return "Regular"
    if jean_style == "Skinny" and inseam_value in {32}:
        return "Long"
    if jean_style == "Straight" and inseam_value == 28:
        return "Petite"
    if jean_style == "Straight" and inseam_value in {30, 32}:
        return "Regular"
    if jean_style == "Straight" and inseam_value == 34:
        return "Long"
    if jean_style == "Baggy" and inseam_value == 30:
        return "Petite"
    if jean_style == "Baggy" and inseam_value == 32:
        return "Regular"
    return ""


def determine_inseam_style(tags: Iterable[str]) -> str:
    lower_tags = {t.lower() for t in tags}
    if any(tag in lower_tags for tag in {"ankle", "filterwomenankle", "length:ankle"}):
        return "Ankle"
    if any(tag in lower_tags for tag in {"crop", "cropped", "filterwomencropped", "length:crop"}):
        return "Cropped"
    if any(tag in lower_tags for tag in {"length:capri", "capri", "length:knee"}):
        return "Capri"
    return ""


LOW_RISE_COLLECTION = "gid://shopify/Collection/271375597703"
HIGH_RISE_COLLECTION = "gid://shopify/Collection/298149380231"
MID_RISE_COLLECTION = "gid://shopify/Collection/278829334663"


def determine_rise_label(tags: Iterable[str], collections: Iterable[str], description: str) -> str:
    lower_tags = {t.lower() for t in tags}
    collection_ids = set(collections)
    if LOW_RISE_COLLECTION in collection_ids:
        return "Low"
    if HIGH_RISE_COLLECTION in collection_ids:
        return "High"
    if MID_RISE_COLLECTION in collection_ids:
        return "Mid"
    if any(tag in lower_tags for tag in {"filterwomenhighrise", "highrise", "rise:high", "rise:ultrahighrise", "ultrahighrise"}):
        return "High"
    if any(tag in lower_tags for tag in {"filterwomenlowrise", "lowrise", "rise:low"}):
        return "Low"
    if any(tag in lower_tags for tag in {"filterwomenmidrise", "rise:mid", "midrise"}):
        return "Mid"
    desc_lower = description.lower()
    if "high-rise" in desc_lower or "high rise" in desc_lower or "high waisted" in desc_lower:
        return "High"
    if "mid rise" in desc_lower or "mid-rise" in desc_lower:
        return "Mid"
    if "low rise" in desc_lower or "low-rise" in desc_lower:
        return "Low"
    return ""


def determine_color_simplified(tags: Iterable[str]) -> str:
    lower_tags = {t.lower() for t in tags}
    if any(tag in lower_tags for tag in {"wash:mid", "midwash"}):
        return "Medium"
    if any(tag in lower_tags for tag in {"dark", "darkindigo", "darkwash", "wash:black", "wash:dark", "tinteddark"}):
        return "Dark"
    if any(tag in lower_tags for tag in {"wash:white", "wash:light", "wash:lightwash"}):
        return "Light"
    if any(tag in lower_tags for tag in {"wash:neutrals", "wash:other"}):
        return "Other"
    return ""


def determine_color_standardized(tags: Iterable[str]) -> str:
    for tag in tags:
        if tag.lower().startswith("color:") and "=" in tag:
            return tag.split("=", 1)[-1].strip()
    return ""


def determine_stretch(
    fabric_description: Optional[str], tags: Iterable[str], description: str
) -> str:
    candidates = []
    if fabric_description:
        candidates.append(fabric_description.lower())
    candidates.extend(tag.lower() for tag in tags)
    candidates.append(description.lower())
    if any("high stretch" in text for text in candidates):
        return "High Stretch"
    if any("low stretch" in text for text in candidates):
        return "Low Stretch"
    if any("rigid" in text for text in candidates):
        return "Rigid"
    return ""


def extract_measurements_from_description(description: str) -> Dict[str, Optional[float]]:
    desc = description.replace("\u201d", "\"")
    measurements: Dict[str, Optional[float]] = {"rise": None, "inseam": None, "leg_opening": None}
    pattern_map = {
        "rise": r"rise[:\s]*([0-9]+(?:\.[0-9]+)?)",
        "inseam": r"inseam[:\s]*([0-9]+(?:\.[0-9]+)?)",
        "leg_opening": r"leg opening[:\s]*([0-9]+(?:\.[0-9]+)?)",
    }
    desc_lower = desc.lower()
    for key, pattern in pattern_map.items():
        match = re.search(pattern, desc_lower)
        if match:
            try:
                measurements[key] = float(match.group(1))
            except ValueError:
                measurements[key] = None
    if measurements["inseam"] is None:
        title_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\"", desc)
        if title_match:
            try:
                measurements["inseam"] = float(title_match.group(1))
            except ValueError:
                pass
    return measurements


def extract_inseam_from_title(title: str) -> Optional[float]:
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\"", title)
    if match:
        try:
            value = float(match.group(1))
            return value
        except ValueError:
            return None
    return None


def should_skip_product(title: str, tags: Iterable[str]) -> bool:
    title_lower = title.lower()
    if any(keyword in title_lower for keyword in EXCLUDED_TITLE_KEYWORDS):
        return True
    lower_tags = {t.lower() for t in tags}
    if any(tag in lower_tags for tag in EXCLUDED_TAGS):
        return True
    if "kids" in lower_tags or "men" in lower_tags:
        return True
    return False


def derive_size_and_color(variant: Dict[str, Any]) -> Tuple[str, str]:
    title = variant.get("title") or ""
    if "/" in title:
        size_part, _, color_part = title.partition("/")
        return size_part.strip(), color_part.strip()
    options = {opt.get("name", "").lower(): opt.get("value", "") for opt in variant.get("selectedOptions", [])}
    size = options.get("size") or options.get("waist") or title.strip()
    color = options.get("color") or options.get("colour") or ""
    return size.strip(), color.strip()


def variant_measurements(
    measurements: Dict[str, Optional[float]],
    fallback_inseam: Optional[float],
) -> Tuple[str, str, str]:
    rise = f"{measurements['rise']}" if measurements.get("rise") is not None else ""
    inseam_val = measurements.get("inseam")
    if inseam_val is None and fallback_inseam is not None:
        inseam_val = fallback_inseam
    inseam = f"{inseam_val}" if inseam_val is not None else ""
    leg = f"{measurements['leg_opening']}" if measurements.get("leg_opening") is not None else ""
    return rise, inseam, leg


def assemble_rows(
    products: List[Dict[str, Any]],
    search_hits: Dict[str, SearchspringHit],
    measurement_fetcher: Optional[callable] = None,
) -> List[List[str]]:
    rows: List[List[str]] = []
    for product in products:
        handle = product.get("handle") or ""
        title = product.get("title") or ""
        description = product.get("description") or ""
        tags = clean_tags(product.get("tags", []))
        if should_skip_product(title, tags):
            LOGGER.info("Skipping product %s because of title/tags filters", handle)
            continue
        search_hit = search_hits.get(handle)
        product_type = determine_product_type(search_hit, title)
        if product_type is None:
            LOGGER.info("Skipping product %s due to product type filters", handle)
            continue
        style_name = extract_style_name(tags)
        vendor = (search_hit.vendor if search_hit else None) or product.get("vendor") or ""
        style_id = parse_shopify_id(product.get("id", "")) if product.get("id") else None
        if not style_id and search_hit and search_hit.style_id:
            style_id = search_hit.style_id
        product_line = "Maternity" if "maternity" in title.lower() else ""
        collections = [edge.get("node", {}).get("id", "") for edge in product.get("collections", {}).get("edges", [])]
        jean_style = determine_jean_style(tags, title, description)
        fabric_description = None
        metafield = product.get("metafield")
        if metafield and isinstance(metafield, dict):
            fabric_description = metafield.get("value")
        stretch = determine_stretch(fabric_description, tags, description)
        color_simplified = determine_color_simplified(tags)
        color_standardized = determine_color_standardized(tags)
        rise_label = determine_rise_label(tags, collections, description)
        inseam_style = determine_inseam_style(tags)
        measurements = extract_measurements_from_description(description)
        fallback_inseam = extract_inseam_from_title(title)
        if measurement_fetcher and (
            measurements.get("rise") is None
            or measurements.get("inseam") is None
            or measurements.get("leg_opening") is None
        ):
            fetched = measurement_fetcher(handle)
            if fetched:
                for key in ("rise", "inseam", "leg_opening"):
                    if measurements.get(key) is None and fetched.get(key) is not None:
                        measurements[key] = fetched[key]
        variants = product.get("variants", {}).get("edges", [])
        if not variants:
            LOGGER.warning("Product %s has no variants", handle)
            continue
        tags_joined = ", ".join(tags)
        published_at = iso_to_date(product.get("publishedAt"))
        created_at = iso_to_date(product.get("createdAt"))
        total_inventory = product.get("totalInventory")
        sku_url = product.get("onlineStoreUrl") or ""
        product_title = product.get("title") or ""
        fallback_image = product.get("featuredImage", {}).get("url") if product.get("featuredImage") else ""
        for edge in variants:
            variant = edge.get("node", {})
            if not variant:
                continue
            sku_brand = variant.get("sku") or ""
            size_value, color_value = derive_size_and_color(variant)
            rise, inseam, leg_opening = variant_measurements(measurements, fallback_inseam)
            inseam_float = None
            try:
                inseam_float = float(inseam) if inseam else None
            except ValueError:
                inseam_float = None
            inseam_label = determine_inseam_label(jean_style, inseam_float, tags, product_title, size_value)
            variant_title = f"{product_title} - {size_value}".strip()
            price = money_amount(variant, "priceV2")
            compare_at_price = money_amount(variant, "compareAtPriceV2")
            quantity_available = variant.get("quantityAvailable")
            available_for_sale = variant.get("availableForSale")
            barcode = variant.get("barcode") or ""
            image_url = variant.get("image", {}).get("url") if variant.get("image") else fallback_image
            sku_shopify = parse_shopify_id(variant.get("id", "")) if variant.get("id") else ""
            promo = search_hit.promo if search_hit else ""
            ga_purchases = search_hit.ga_purchases if search_hit else None
            instock_pct = search_hit.instock_pct if search_hit else ""
            row = [
                style_id or "",
                handle,
                published_at,
                created_at,
                product_title,
                style_name,
                product_type,
                tags_joined,
                vendor,
                description,
                variant_title,
                color_value,
                size_value,
                rise,
                inseam,
                leg_opening,
                price,
                compare_at_price,
                promo or "",
                "TRUE" if available_for_sale else "FALSE",
                str(quantity_available) if quantity_available is not None else "",
                str(ga_purchases) if ga_purchases is not None else "",
                str(total_inventory) if total_inventory is not None else "",
                instock_pct or "",
                sku_shopify,
                sku_brand,
                barcode,
                product_line,
                image_url or "",
                sku_url,
                jean_style,
                inseam_label,
                inseam_style,
                rise_label,
                color_simplified,
                color_standardized,
                stretch,
                "Women",
            ]
            rows.append(row)
    return rows


def write_csv(rows: List[List[str]], filename: str) -> None:
    path = OUTPUT_DIR / filename
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)


def main() -> None:
    search_hits = fetch_searchspring()
    products = fetch_storefront_products()
    rows = assemble_rows(products, search_hits)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"DL1961_{timestamp}.csv"
    write_csv(rows, filename)
    LOGGER.info("Wrote %s rows to %s", len(rows), filename)


if __name__ == "__main__":
    main()

