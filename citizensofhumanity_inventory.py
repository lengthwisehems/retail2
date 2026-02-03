import csv
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_PATH = BASE_DIR / "citizensofhumanity_run.log"

PRIMARY_LOG_PATH = LOG_PATH
FALLBACK_LOG_PATH = OUTPUT_DIR / "citizensofhumanity_run.log"

try:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(PRIMARY_LOG_PATH, mode="a", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
except Exception as exc:  # noqa: BLE001
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(FALLBACK_LOG_PATH, mode="a", encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    logging.warning("Falling back to fallback log path due to: %s", exc)

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
})
SESSION.verify = False
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

HOSTS = ["https://citizensofhumanity.com", "https://citizens-of-humanity.myshopify.com"]
API_ENDPOINTS = [
    "https://citizensofhumanity.com/api/unstable/graphql.json",
    "https://citizens-of-humanity.myshopify.com/api/unstable/graphql.json",
]
API_TOKENS = ["6e71093ff37f6adfc1bed4d89eca9a8f"]
COLLECTION_HANDLES = ["womens-jeans", "archive-denim"]

DETAILS_ID_PREFIX = "ProductAccordion-a847e329-5dcb-4a67-a07c-840def99d68a"

GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 100, after: $cursor) {
      pageInfo {
        hasNextPage
        endCursor
      }
      edges {
        node {
          id
          handle
          title
          productType
          tags
          vendor
          description
          publishedAt
          createdAt
          onlineStoreUrl
          featuredImage {
            url
          }
          filter_fit: metafield(namespace: "filter", key: "fit") {
            value
          }
          filter_rise: metafield(namespace: "filter", key: "rise") {
            value
          }
          filter_category: metafield(namespace: "filter", key: "category") {
            value
          }
          variants(first: 250) {
            edges {
              node {
                id
                title
                sku
                barcode
                availableForSale
                selectedOptions {
                  name
                  value
                }
                price {
                  amount
                }
                compareAtPrice {
                  amount
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

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
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
    "Country Produced",
    "Gender",
]

PRODUCT_TYPE_ALLOW = {"pants", "jeans"}


def parse_shopify_id(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]


def format_date(date_str: Optional[str]) -> str:
    if not date_str:
        return ""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%-m/%-d/%Y")
    except Exception:  # noqa: BLE001
        return date_str


def clean_html(text: str) -> str:
    soup = BeautifulSoup(text or "", "html.parser")
    return soup.get_text(" ", strip=True)


def request_json(url: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    for attempt in range(5):
        try:
            if payload is None:
                resp = SESSION.get(url, timeout=40, verify=False)
            else:
                resp = SESSION.post(url, json=payload, timeout=40, verify=False)
            if resp.status_code in {429, 500, 502, 503, 504}:
                sleep = 2 ** attempt
                logger.warning("Request %s returned %s; sleeping %s", url, resp.status_code, sleep)
                time.sleep(sleep)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            sleep = 2 ** attempt
            logger.warning("Request to %s failed (%s); retrying in %s", url, exc, sleep)
            time.sleep(sleep)
    raise RuntimeError(f"Request failed for {url}")


def graphql_request(variables: Dict[str, Optional[str]]) -> Dict[str, Any]:
    for endpoint in API_ENDPOINTS:
        for token in API_TOKENS:
            for attempt in range(4):
                try:
                    resp = SESSION.post(
                        endpoint,
                        headers={"X-Shopify-Storefront-Access-Token": token},
                        json={"query": GRAPHQL_QUERY, "variables": variables},
                        timeout=40,
                        verify=False,
                    )
                    if resp.status_code in {429, 500, 502, 503, 504}:
                        sleep = 2 ** attempt
                        logger.warning("GraphQL %s returned %s; sleeping %s", endpoint, resp.status_code, sleep)
                        time.sleep(sleep)
                        continue
                    resp.raise_for_status()
                    data = resp.json()
                    if "errors" in data:
                        logger.warning("GraphQL errors from %s: %s", endpoint, data["errors"])
                        break
                    return data
                except Exception as exc:  # noqa: BLE001
                    sleep = 2 ** attempt
                    logger.warning("GraphQL request to %s failed (%s); retrying in %s", endpoint, exc, sleep)
                    time.sleep(sleep)
    raise RuntimeError("GraphQL requests failed for all endpoints")


def fetch_collection_products() -> List[Dict[str, Any]]:
    collected: Dict[str, Dict[str, Any]] = {}
    for handle in COLLECTION_HANDLES:
        cursor = None
        page = 0
        while True:
            page += 1
            logger.info("Fetching collection %s page %s", handle, page)
            data = graphql_request({"handle": handle, "cursor": cursor})
            collection = data.get("data", {}).get("collection")
            if not collection:
                break
            edges = collection.get("products", {}).get("edges", [])
            for edge in edges:
                product = edge["node"]
                collected.setdefault(product.get("handle", ""), product)
            page_info = collection.get("products", {}).get("pageInfo", {})
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break
    logger.info("Total products fetched: %s", len(collected))
    return list(collected.values())


def extract_fraction(value: str) -> str:
    raw = value.replace('"', "").strip()
    if not raw:
        return ""
    if " " in raw and "/" in raw:
        whole, frac = raw.split(" ", 1)
        try:
            num, den = frac.split("/")
            number = float(whole) + float(num) / float(den)
            return str(round(number, 4)).rstrip("0").rstrip(".")
        except Exception:  # noqa: BLE001
            return raw
    if "/" in raw:
        try:
            num, den = raw.split("/")
            number = float(num) / float(den)
            return str(round(number, 4)).rstrip("0").rstrip(".")
        except Exception:  # noqa: BLE001
            return raw
    return raw


def extract_measurement(text: str, label: str) -> str:
    pattern = re.compile(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\s+[0-9]+/[0-9]+)?(?:\.[0-9]+)?)", re.I)
    match = pattern.search(text)
    if not match:
        return ""
    return extract_fraction(match.group(1))


def extract_country(text: str) -> str:
    match = re.search(r"Made in\s+([^\.]*?)(?:\s+with|$)", text, re.I)
    if not match:
        return ""
    return match.group(1).strip()


def extract_sku_brand(lines: List[str]) -> str:
    for line in reversed(lines):
        if re.search(r"[A-Za-z0-9]{3,}[-_][A-Za-z0-9]+", line):
            return line.strip()
    return ""


def parse_restock_quantities(html: str) -> Dict[str, int]:
    match = re.search(r"variantsInventoryQuantity\s*=\s*\{(?P<body>.*?)\};", html, re.S)
    if not match:
        return {}
    body = match.group("body")
    quantities: Dict[str, int] = {}
    for entry in re.finditer(r"(\d+)\s*:\s*parseInt\(\"?(-?\d*)\"?\)", body):
        variant_id = entry.group(1)
        qty_raw = entry.group(2)
        qty = int(qty_raw) if qty_raw not in {"", None} else 0
        quantities[variant_id] = qty
    if quantities:
        return quantities
    for entry in re.finditer(r"(\d+)\s*:\s*(-?\d+)", body):
        quantities[entry.group(1)] = int(entry.group(2))
    return quantities


def parse_restock_image(html: str) -> str:
    match = re.search(r"\"images\"\s*:\s*\[(?P<body>.*?)\]", html, re.S)
    if not match:
        return ""
    body = match.group("body")
    urls = re.findall(r"\"(//[^\"]+)\"", body)
    if not urls:
        return ""
    url = urls[0]
    return f"https:{url}" if url.startswith("//") else url


def fetch_pdp_details(handle: str) -> Dict[str, Any]:
    for host in HOSTS:
        url = f"{host}/products/{handle}"
        try:
            resp = SESSION.get(url, timeout=40, verify=False)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            details_div = soup.find("div", id=re.compile(rf"{re.escape(DETAILS_ID_PREFIX)}"))
            details_text = details_div.get_text("\n", strip=True) if details_div else ""
            lines = [line.strip() for line in details_text.splitlines() if line.strip()]
            rise = extract_measurement(details_text, "Rise")
            inseam = extract_measurement(details_text, "Inseam")
            leg_opening = extract_measurement(details_text, "Leg Opening")
            sku_brand = extract_sku_brand(lines)
            country = extract_country(details_text)
            restock_quantities = parse_restock_quantities(html)
            restock_image = parse_restock_image(html)
            return {
                "details_text": details_text,
                "rise": rise,
                "inseam": inseam,
                "leg_opening": leg_opening,
                "sku_brand": sku_brand,
                "country": country,
                "restock_quantities": restock_quantities,
                "restock_image": restock_image,
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch PDP %s (%s)", url, exc)
    return {
        "details_text": "",
        "rise": "",
        "inseam": "",
        "leg_opening": "",
        "sku_brand": "",
        "country": "",
        "restock_quantities": {},
        "restock_image": "",
    }


def normalize_product_title(title: str) -> str:
    lowered = title.lower()
    if " in " not in lowered:
        return title
    prefix, sep, suffix = title.partition(" in ")
    if not sep:
        return title
    tokens = prefix.split()
    movers = {"petite", "reworked", "rework"}
    moved = [token for token in tokens if token.lower() in movers]
    kept = [token for token in tokens if token.lower() not in movers]
    if not moved:
        return title
    moved_phrase = " ".join(moved)
    prefix_out = " ".join(kept + moved).strip()
    suffix_out = suffix.strip()
    return f"{prefix_out} in {suffix_out}"


def derive_style_name(product_title: str) -> str:
    lower = product_title.lower()
    if " in " in lower:
        base = product_title[: lower.index(" in ")].strip()
    else:
        base = product_title.strip()
    tokens = base.split()
    movers = {"petite", "reworked", "rework"}
    moved: List[str] = []
    kept: List[str] = []
    for token in tokens:
        if token.lower() in movers:
            moved.append(token)
        else:
            kept.append(token)
    style_name = " ".join(kept + moved).strip()
    return style_name


def extract_option(selected_options: List[Dict[str, str]], name: str) -> str:
    for opt in selected_options:
        if opt.get("name", "").lower() == name.lower():
            return opt.get("value", "")
    return ""


def normalize_tags(tags: Iterable[str]) -> List[str]:
    return [t.strip() for t in tags if t is not None]


def extract_segment(text: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}\s*:\s*(.*)", text, re.I)
    if not match:
        return ""
    segment = match.group(1)
    segment = re.split(r"Feels Like:|Looks Like:", segment, flags=re.I)[0]
    segment = segment.split(".")[0]
    return segment.strip(" .")


def derive_color_standardized(looks_like: str) -> str:
    ll = re.sub(r"\s+", " ", looks_like.lower())
    if "indigo" in ll:
        return "Blue"
    if "black" in ll:
        return "Black"
    if "white" in ll:
        return "White"
    if "grey" in ll:
        return "Grey"
    if "brown" in ll:
        return "Brown"
    if "khaki" in ll or "beige" in ll:
        return "Tan"
    if "green" in ll or "olive" in ll:
        return "Green"
    if "denim" in ll:
        return "Blue"
    return ""


def derive_color_simplified(looks_like: str, standardized: str, wash_phrase: str) -> str:
    ll = re.sub(r"\s+", " ", looks_like.lower())
    if standardized in {"White", "Tan"} or any(term in ll for term in [
        "light indigo",
        "light, vintage indigo",
        "light vintage indigo",
        "light washed",
    ]):
        return "Light"
    if standardized in {"Black", "Brown"} or any(term in ll for term in [
        "dark indigo",
        "dark deep",
        "deep indigo",
        "dark blue",
        "dark wash",
        "dark black",
        "dark rinse",
    ]):
        return "Dark"
    if "light indigo" in ll:
        return "Light"
    if "light to medium" in ll or "medium to light" in ll:
        return "Light to Medium"
    if "dark to medium" in ll or "medium to dark" in ll:
        return "Medium to Dark"
    if "medium indigo" in ll or " medium" in ll:
        return "Medium"
    if wash_phrase:
        return wash_phrase
    return ""


def derive_wash_phrase(looks_like: str) -> str:
    match = re.search(r"([A-Za-z\s]+)\s+wash", looks_like, re.I)
    if not match:
        return ""
    return match.group(1).strip().title()


def derive_stretch(feels_like: str) -> str:
    fl = feels_like.lower()
    if any(term in fl for term in ["rigid", "non stretch", "non-stretch"]):
        return "Rigid"
    if any(term in fl for term in [
        "comfort stretch",
        "stretch cotton",
        "perfect amount of stretch",
        "stretch denim",
        "soft stretch",
    ]):
        return "Stretch"
    if any(term in fl for term in ["slight stretch", "hint of stretch"]):
        return "Low Stretch"
    return ""


def derive_inseam_label(title: str, tags: List[str]) -> str:
    lowered_tags = {t.lower() for t in tags}
    if "petite" in title.lower():
        return "Petite"
    has_regular = any("regular" in t for t in lowered_tags)
    has_long = any("length:long" in t or "tall" in t for t in lowered_tags)
    if has_regular and has_long:
        return "Regular"
    if has_long:
        return "Long"
    if has_regular:
        return "Regular"
    return ""


def derive_rise_label(description: str, filter_rise: str) -> str:
    desc = description.lower()
    if any(token in desc for token in ["low on", "low-", "low rise", "relaxed at the natural waist", "low slung"]):
        return "Low"
    if any(token in desc for token in ["high on", "high-rise", "high rise"]):
        return "High"
    if any(token in desc for token in ["mid on", "mid-", "mid rise"]):
        return "Mid"
    if filter_rise and "low" in filter_rise.lower():
        return "Low"
    if filter_rise and "mid" in filter_rise.lower():
        return "Mid"
    if filter_rise and "high" in filter_rise.lower():
        return "High"
    return ""


def derive_jean_style_base(style_name: str) -> str:
    name = re.sub(r"[-\s]+", " ", style_name.lower())
    if "wide" in name:
        return "Wide Leg"
    if "flare" in name:
        return "Flare"
    if "bootcut" in name or "boot" in name:
        return "Bootcut"
    if "skinny" in name or "slim" in name:
        return "Skinny"
    if any(term in name for term in ["barrel", "bowed", "bow leg", "relaxed", "flight pant", "horseshoe"]):
        return "Barrel"
    if "baggy" in name or "work pant" in name:
        return "Baggy"
    if "boyfriend" in name:
        return "Boyfriend"
    return ""


def has_style_keyword(style_name: str, filter_fit: str) -> bool:
    text = f"{style_name} {filter_fit}".lower()
    keywords = [
        "wide",
        "flare",
        "boot",
        "skinny",
        "slim",
        "barrel",
        "bowed",
        "bow leg",
        "relaxed",
        "horseshoe",
        "baggy",
        "work pant",
        "boyfriend",
        "straight",
        "trouser",
    ]
    return any(keyword in text for keyword in keywords)


def infer_straight_style(leg_opening: Optional[float]) -> str:
    if leg_opening is None:
        return ""
    if leg_opening < 15.5:
        return "Straight from Knee"
    if 15.5 <= leg_opening <= 17:
        return "Straight from Knee/Thigh"
    if leg_opening > 17:
        return "Straight from Thigh"
    return ""


def derive_inseam_style_base(style_name: str, description: str, inseam: Optional[float]) -> str:
    name = style_name.lower()
    desc = description.lower()
    if "crop" in name or "kick" in name:
        return "Crop"
    if any(term in desc for term in [
        "cropped for a fresh",
        "cropped hems",
        "cropped inseam",
        "cropped interpretation",
        "cropped length",
        "cropped style",
    ]):
        return "Crop"
    if "ankle" in name:
        return "Ankle"
    if "long" in name:
        return "Full Length"
    if any(term in desc for term in [
        "ankle-grazing",
        "ankle-length",
        "ankle-skimming",
        "down to the ankle",
        "grazes the ankle",
        "hits right at the ankle",
        "tapers to the ankle",
        "to the ankle",
    ]):
        return "Ankle"
    if "long" in name or any(term in desc for term in ["full-length", "elong", "full  33", "full length"]):
        return "Full Length"
    if inseam is not None and inseam > 29.1:
        return "Full Length"
    return ""


def parse_float(value: str) -> Optional[float]:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def build_rows(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for product in products:
        title = product.get("title", "")
        product_type = product.get("productType", "")
        if product_type.lower() not in PRODUCT_TYPE_ALLOW:
            continue
        tags = normalize_tags(product.get("tags", []) or [])
        if any(re.search(r"\bmen\b", t, re.I) for t in tags):
            continue
        description = clean_html(product.get("description", ""))
        product_title = normalize_product_title(title)
        style_name = derive_style_name(product_title)
        handle = product.get("handle", "")
        published_at = format_date(product.get("publishedAt"))
        created_at = format_date(product.get("createdAt"))
        vendor = product.get("vendor", "")
        style_id = parse_shopify_id(product.get("id", ""))
        filter_fit = (product.get("filter_fit") or {}).get("value") or ""
        filter_rise = (product.get("filter_rise") or {}).get("value") or ""
        filter_category = (product.get("filter_category") or {}).get("value") or ""
        details = fetch_pdp_details(handle)
        restock_quantities = details.get("restock_quantities", {})
        style_total = sum(max(0, qty) for qty in restock_quantities.values())
        sku_brand = details.get("sku_brand", "")
        country = details.get("country", "")
        rise = details.get("rise", "")
        inseam = details.get("inseam", "")
        leg_opening = details.get("leg_opening", "")
        image_url = ""
        featured = product.get("featuredImage") or {}
        if isinstance(featured, dict):
            image_url = featured.get("url", "") or ""
        if not image_url:
            image_url = details.get("restock_image", "")
        looks_like = extract_segment(description, "Looks Like")
        feels_like = extract_segment(description, "Feels Like")
        wash_phrase = derive_wash_phrase(looks_like)
        color_standardized = derive_color_standardized(looks_like)
        color_simplified = derive_color_simplified(looks_like, color_standardized, wash_phrase)
        stretch = derive_stretch(feels_like)
        inseam_label = derive_inseam_label(product_title, tags)
        rise_label = derive_rise_label(description, filter_rise)
        gender = "Women" if any("women" in t.lower() for t in tags) else ""
        product_type_out = "Jeans" if product_type.lower() in {"pants", "jeans"} else ""
        if filter_category:
            product_type_out = filter_category
        variants = product.get("variants", {}).get("edges", [])
        for edge in variants:
            node = edge["node"]
            variant_gid = node.get("id", "")
            variant_id = parse_shopify_id(variant_gid)
            selected_options = node.get("selectedOptions", []) or []
            size = extract_option(selected_options, "Size") or extract_option(selected_options, "Option1")
            color = extract_option(selected_options, "Color") or extract_option(selected_options, "Option2")
            variant_title = f"{product_title} - {size}" if size else product_title
            price = (node.get("price") or {}).get("amount", "")
            compare_at = (node.get("compareAtPrice") or {}).get("amount", "")
            available = node.get("availableForSale", False)
            quantity_available = restock_quantities.get(variant_id, 0)
            rows.append({
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": product_title,
                "Style Name": style_name,
                "Product Type": product_type_out,
                "Tags": ", ".join(tags),
                "Vendor": vendor,
                "Description": description,
                "Variant Title": variant_title,
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": price,
                "Compare at Price": compare_at,
                "Available for Sale": str(available).upper(),
                "Quantity Available": quantity_available,
                "Quantity of style": style_total,
                "SKU - Shopify": variant_id,
                "SKU - Brand": sku_brand,
                "Barcode": node.get("barcode", "") or "",
                "Image URL": image_url,
                "SKU URL": product.get("onlineStoreUrl", ""),
                "Jean Style": "",
                "Inseam Label": inseam_label,
                "Inseam Style": "",
                "Rise Label": rise_label,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
                "Country Produced": country,
                "Gender": gender,
                "_style_name": style_name,
                "_filter_fit": filter_fit,
                "_filter_rise": filter_rise,
                "_leg_opening_num": parse_float(leg_opening),
                "_inseam_num": parse_float(inseam),
                "_description": description,
            })
    return rows


def apply_rise_label_fallback(rows: List[Dict[str, Any]]) -> None:
    by_style: Dict[str, List[str]] = {}
    for row in rows:
        if row["Rise Label"]:
            by_style.setdefault(row["_style_name"], []).append(row["Rise Label"])
    for row in rows:
        if row["Rise Label"]:
            continue
        if row.get("_filter_rise"):
            row["Rise Label"] = row["_filter_rise"]
            continue
        labels = by_style.get(row["_style_name"], [])
        if not labels:
            continue
        counts = {label: labels.count(label) for label in {"Low", "Mid", "High"}}
        top = sorted(counts.items(), key=lambda kv: kv[1], reverse=True)
        if top and top[0][1] > 0 and (len(top) == 1 or top[0][1] > top[1][1]):
            row["Rise Label"] = top[0][0]


def apply_jean_style(rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        style_name = row["_style_name"]
        base = derive_jean_style_base(style_name)
        if base:
            row["Jean Style"] = base

    for row in rows:
        if row["Jean Style"]:
            continue
        style_name = row["_style_name"]
        filter_fit = row.get("_filter_fit", "")
        if "straight" in style_name.lower() or "straight" in filter_fit.lower():
            row["Jean Style"] = infer_straight_style(row.get("_leg_opening_num"))

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        first_word = row["_style_name"].split()[0] if row["_style_name"].split() else ""
        if first_word:
            grouped.setdefault(first_word.lower(), []).append(row)

    for row in rows:
        if row["Jean Style"]:
            continue
        if has_style_keyword(row["_style_name"], row.get("_filter_fit", "")):
            continue
        first_word = row["_style_name"].split()[0].lower() if row["_style_name"].split() else ""
        candidates = grouped.get(first_word, [])
        candidates = [c for c in candidates if c.get("Jean Style")]
        if not candidates:
            continue
        unique_styles = {c["Jean Style"] for c in candidates}
        if len(unique_styles) == 1:
            row["Jean Style"] = unique_styles.pop()
            continue
        leg_opening = row.get("_leg_opening_num")
        if leg_opening is None:
            continue
        closest = min(
            candidates,
            key=lambda c: abs((c.get("_leg_opening_num") or 0) - leg_opening),
        )
        row["Jean Style"] = closest.get("Jean Style", "")

    for row in rows:
        if row["Jean Style"]:
            continue
        if "trouser" in row["_style_name"].lower():
            row["Jean Style"] = infer_straight_style(row.get("_leg_opening_num"))


def apply_inseam_style(rows: List[Dict[str, Any]]) -> None:
    for row in rows:
        row["Inseam Style"] = derive_inseam_style_base(
            row["_style_name"],
            row.get("_description", ""),
            row.get("_inseam_num"),
        )

    prefix_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        prefix = " ".join(row["_style_name"].split()[:2]).lower()
        if prefix:
            prefix_map.setdefault(prefix, []).append(row)

    for row in rows:
        if row["Inseam Style"]:
            continue
        prefix = " ".join(row["_style_name"].split()[:2]).lower()
        candidates = [c for c in prefix_map.get(prefix, []) if c.get("Inseam Style")]
        if not candidates:
            continue
        unique_styles = {c["Inseam Style"] for c in candidates}
        if len(unique_styles) == 1:
            row["Inseam Style"] = unique_styles.pop()
            continue
        inseam = row.get("_inseam_num")
        if inseam is None:
            continue
        closest = min(
            candidates,
            key=lambda c: abs((c.get("_inseam_num") or 0) - inseam),
        )
        row["Inseam Style"] = closest.get("Inseam Style", "")


def finalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    apply_rise_label_fallback(rows)
    apply_jean_style(rows)
    apply_inseam_style(rows)
    cleaned: List[Dict[str, Any]] = []
    for row in rows:
        cleaned.append({key: row.get(key, "") for key in CSV_HEADERS})
    return cleaned


def write_csv(rows: List[Dict[str, Any]]) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"CITIZENSOFHUMANITY_{timestamp}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("CSV written: %s", out_path.resolve())


def main() -> None:
    products = fetch_collection_products()
    rows = build_rows(products)
    final_rows = finalize_rows(rows)
    write_csv(final_rows)


if __name__ == "__main__":
    main()
