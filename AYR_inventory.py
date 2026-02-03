import csv
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from urllib3.exceptions import InsecureRequestWarning
import warnings

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND = "AYR"
LOG_FILE = BASE_DIR / f"{BRAND.lower()}_run.log"

STORE_HOST = "https://ayr-production.myshopify.com"
BASE_HOSTS = [
    STORE_HOST,
    "https://www.ayr.com",
    "https://ayr.com",
]
GRAPHQL_ENDPOINTS = [f"{STORE_HOST}/api/unstable/graphql.json"]
GRAPHQL_TOKEN = "32431e71f8b832fcb465240380b72ecc"
COLLECTION_HANDLES = ["jeans-womenswear", "final-sale-womenswear"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
}

BANNED_DESC_TERMS = ["skirt", "shorts"]
BANNED_TAG_TERMS = ["skirt", "shorts"]
TARGET_PRODUCT_TYPE = "Denim"

FLOAT_REGEX = re.compile(r"([-+]?[0-9]*\.?[0-9]+(?:\s+\d+/\d+)?)")
FRACTION_REGEX = re.compile(r"(?P<int>\d+)?\s*(?P<num>\d+)/(\d+)")
COLOR_SNIPPET_REGEX = re.compile(r"\.\s+")


@dataclass
class VariantRow:
    product_id: str
    handle: str
    product_title: str
    color: str
    size: str
    inseam_code: str
    variant_id: str
    sku: str
    barcode: str
    available: bool
    price: str
    compare_at: str
    image_url: Optional[str]
    online_url: Optional[str]
    description: str
    tags: List[str]
    vendor: str
    product_type: str
    seo_title: Optional[str]


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(BRAND)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    logger.addHandler(stream)

    try:
        fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    except Exception:
        fallback = OUTPUT_DIR / f"{BRAND.lower()}_run.log"
        logger.warning("Primary log path unavailable, falling back to %s", fallback)
        fh = logging.FileHandler(fallback, mode="a", encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def build_session() -> requests.Session:
    warnings.simplefilter("ignore", InsecureRequestWarning)
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    session.verify = False
    return session


def request_json(session: requests.Session, url: str, logger: logging.Logger) -> Dict[str, Any]:
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def convert_fraction(text: str) -> Optional[Decimal]:
    text = text.strip().replace("\u00bd", " 1/2")
    frac_match = FRACTION_REGEX.search(text)
    if frac_match:
        num = Decimal(frac_match.group("num"))
        den = Decimal(frac_match.group(3))
        whole = Decimal(frac_match.group("int")) if frac_match.group("int") else Decimal(0)
        return whole + num / den
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def extract_number_after(label: str, text: str) -> str:
    pattern = re.compile(rf"{label}[^\d]*(\d[\d\s./]*)", re.IGNORECASE)
    m = pattern.search(text)
    if not m:
        return ""
    val = m.group(1)
    dec = convert_fraction(val)
    return str(dec) if dec is not None else ""


def measurement_text_from_html(html: str) -> str:
    """Return the sizing/measurement block text if present, else full text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    sizing = soup.find(id="sizing-details-panel")
    if sizing:
        return sizing.get_text(" ", strip=True)
    return soup.get_text(" ", strip=True)


def parse_inseams(text: str) -> Dict[str, str]:
    """Parse inseam measurements from the sizing text.

    Handles standard "Short/Regular/Long Inseam" patterns, missing spaces
    (e.g., "33 1/2Long Inseam"), and height-based guidance such as
    "5'4 and under" → Short, "5'4-5'7" → Regular, "5'8 and over" → Long.
    """
    mapping: Dict[str, str] = {}
    if not text:
        return mapping

    norm = " ".join(text.split())

    def assign(code: str, match: Optional[re.Match]) -> None:
        if not match:
            return
        dec = convert_fraction(match.group(1))
        if dec is not None:
            mapping.setdefault(code, str(dec))
            mapping.setdefault(str(dec), str(dec))

    for code, label in [("S", "Short"), ("R", "Regular"), ("L", "Long")]:
        assign(code, re.search(rf"(\d[\d\s./]*)\s*\"?\s*{label}\s*Inseam", norm, re.IGNORECASE))
        if code not in mapping:
            assign(code, re.search(rf"(\d[\d\s./]*)\s*\"?\s*{label}\b", norm, re.IGNORECASE))

    height_patterns = [
        ("S", r"(\d[\d\s./]*)\s*\"?\s*(?:in)?seam[^\d]{0,40}5'\s*4\s*(?:and\s*under|under)"),
        ("R", r"(\d[\d\s./]*)\s*\"?\s*(?:in)?seam[^\d]{0,40}5'\s*4\s*-\s*5'\s*7"),
        ("L", r"(\d[\d\s./]*)\s*\"?\s*(?:in)?seam[^\d]{0,40}5'\s*8\s*(?:and\s*over|and\s*above|and\s*over)") ,
    ]
    for code, pattern in height_patterns:
        if code in mapping:
            continue
        assign(code, re.search(pattern, norm, re.IGNORECASE))

    if not mapping:
        assign("default", re.search(r"Inseam:\s*(\d[\d\s./]*)", norm, re.IGNORECASE))

    return mapping


def normalize_color_snippet(description: str, color: str) -> str:
    desc_lower = description.lower()
    idx = desc_lower.find(color.lower())
    if idx == -1:
        return ""
    snippet = desc_lower[idx:]
    end = snippet.find(".")
    if end != -1:
        snippet = snippet[: end]
    return snippet


def derive_color_simplified(snippet: str) -> str:
    if not snippet:
        return ""
    if "medium" in snippet and "wash" in snippet:
        return "Medium"
    if "dark" in snippet and "wash" in snippet:
        return "Dark"
    if "light" in snippet and "wash" in snippet:
        return "Light"
    if "black" in snippet or "rich brown" in snippet:
        return "Dark"
    if "white" in snippet and "cream" in snippet:
        return "Light"
    return ""


def derive_color_standardized(snippet: str) -> str:
    if not snippet:
        return ""
    if ("medium" in snippet and "wash" in snippet) or ("dark" in snippet and "wash" in snippet) or ("light" in snippet and "wash" in snippet):
        return "Blue"
    if "black" in snippet:
        return "Black"
    if "brown" in snippet:
        return "Brown"
    if "white" in snippet or "cream" in snippet:
        return "White"
    return ""


def derive_jean_style(description: str, seo_title: str) -> str:
    desc = description.lower()
    seo = (seo_title or "").lower()
    if "bowed leg" in desc or "barrel leg" in desc:
        return "Barrel"
    if "flare-leg" in desc:
        return "Flare"
    if "stovepipe" in desc:
        return "Skinny"
    if "ultra-fitted 1990s straight leg" in desc or "sraight leg" in desc or "straight leg" in desc and "ultra" in desc:
        return "Straight from Knee"
    if "moderate straight-leg" in desc or "loose straight leg" in desc:
        return "Straight from Thigh"
    if "tapered" in desc:
        return "Tapered"
    if "wide leg" in desc or "wide legs" in desc:
        return "Wide Leg"
    if "barrel" in desc:
        return "Barrel"
    if "baggy" in desc:
        return "Baggy"
    if "straight leg" in desc:
        return "Straight"
    if "flare" in desc:
        return "Flare"
    if "skinny" in desc:
        return "Skinny"
    if "wide" in desc:
        return "Wide Leg"
    if "bootcut" in desc or "boot-cut" in desc:
        return "Bootcut"
    if "barrel" in desc:
        return "Barrel"
    if "baggy" in desc:
        return "Baggy"
    if "barrel" in seo:
        return "Barrel"
    if "flare" in seo:
        return "Flare"
    if "slim leg" in seo or "slim straight" in seo:
        return "Skinny"
    if "straight" in seo:
        return "Straight"
    if "wide leg" in seo:
        return "Wide Leg"
    if "tapered" in seo:
        return "Tapered"
    return ""


def derive_rise_label(description: str, tags: List[str]) -> str:
    desc = description.lower()
    tags_lower = [t.lower() for t in tags]
    if "high-" in desc or "high rise" in desc:
        return "High"
    if "mid-" in desc or "mid rise" in desc:
        return "Mid"
    if "low-" in desc or "low rise" in desc or "loose on" in desc:
        return "Low"
    if any("high rise" in t for t in tags_lower):
        return "High"
    if any("mid rise" in t for t in tags_lower):
        return "Mid"
    if any(sub in tag for tag in tags_lower for sub in ["low rise", "slouchy", "low waist"]):
        return "Low"
    return ""


def derive_inseam_style(description: str, seo_title: str) -> str:
    desc = description.lower()
    seo = (seo_title or "").lower()
    if "ankle" in desc:
        return "Ankle"
    if "crop" in desc:
        return "Cropped"
    if "full length" in desc:
        return "Full Length"
    if "ankle" in seo:
        return "Ankle"
    if "crop" in seo:
        return "Cropped"
    return "Full Length"


def parse_country(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    heading = soup.find(id="made-in-details-heading")
    if not heading:
        return ""
    panel = soup.find(id="made-in-details-panel")
    text = panel.get_text(" ", strip=True).lower() if panel else heading.get_text(" ", strip=True).lower()
    if "california" in text:
        return "USA"
    if "china" in text:
        return "China"
    return ""


def to_mmddyy(dt_str: str) -> str:
    if not dt_str:
        return ""
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.strftime("%m/%d/%y")
    except Exception:
        return dt_str


def fetch_graphql_products(session: requests.Session, logger: logging.Logger) -> List[VariantRow]:
    rows: List[VariantRow] = []
    query = """
    query ($handle: String!, $cursor: String) {
      collection(handle: $handle) {
        products(first: 50, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          edges {
            node {
              id
              handle
              title
              description
              productType
              tags
              vendor
              onlineStoreUrl
              seo { title description }
              featuredImage { url altText }
              variants(first: 250) {
                edges {
                  node {
                    id
                    title
                    sku
                    availableForSale
                    selectedOptions { name value }
                    price { amount }
                    compareAtPrice { amount }
                    image { url altText }
                  }
                }
              }
            }
          }
        }
      }
    }
    """

    headers = {**HEADERS, "X-Shopify-Storefront-Access-Token": GRAPHQL_TOKEN}
    for endpoint in GRAPHQL_ENDPOINTS:
        for handle in COLLECTION_HANDLES:
            cursor = None
            while True:
                payload = {"query": query, "variables": {"handle": handle, "cursor": cursor}}
                resp = session.post(endpoint, json=payload, headers=headers, timeout=40, verify=False)
                if resp.status_code != 200:
                    logger.warning("GraphQL request failed %s: %s", endpoint, resp.text[:200])
                    break
                data = resp.json().get("data", {})
                prod_block = data.get("collection", {}) or {}
                products = prod_block.get("products")
                if not products:
                    break
                for edge in products.get("edges", []):
                    node = edge.get("node", {})
                    product_type = node.get("productType") or ""
                    desc = node.get("description") or ""
                    if product_type != TARGET_PRODUCT_TYPE:
                        continue
                    if any(term in desc.lower() for term in BANNED_DESC_TERMS):
                        continue
                    tags = node.get("tags", []) or []
                    tags_lower = [t.lower() for t in tags]
                    if any(term in tag for tag in tags_lower for term in BANNED_TAG_TERMS):
                        continue
                    product_id = node.get("id", "").split("/")[-1]
                    handle_val = node.get("handle", "")
                    seo = node.get("seo") or {}
                    for v_edge in (node.get("variants", {}) or {}).get("edges", []):
                        v = v_edge.get("node", {})
                        variant_id = v.get("id", "").split("/")[-1]
                        opts = {opt.get("name", ""): opt.get("value", "") for opt in v.get("selectedOptions", [])}
                        color = opts.get("Color") or opts.get("color") or opts.get("Option1") or ""
                        size = opts.get("Size") or opts.get("option2") or opts.get("Option2") or ""
                        inseam_code = opts.get("Option3") or opts.get("Inseam") or ""
                        if (not color or not size or not inseam_code) and v.get("title"):
                            title_parts = [p.strip() for p in v.get("title", "").split("/") if p.strip()]
                            if not color and len(title_parts) >= 1:
                                color = title_parts[0]
                            if not size and len(title_parts) >= 2:
                                size = title_parts[1]
                            if not inseam_code and len(title_parts) >= 3:
                                inseam_code = title_parts[2]
                        rows.append(
                            VariantRow(
                                product_id=product_id,
                                handle=handle_val,
                                product_title=node.get("title", ""),
                                color=color,
                                size=size,
                                inseam_code=inseam_code,
                                variant_id=variant_id,
                                sku=v.get("sku", ""),
                                barcode="",
                                available=bool(v.get("availableForSale")),
                                price=(v.get("price") or {}).get("amount") or "",
                                compare_at=(v.get("compareAtPrice") or {}).get("amount") or "",
                                image_url=(v.get("image") or {}).get("url"),
                                online_url=node.get("onlineStoreUrl"),
                                description=desc,
                                tags=tags,
                                vendor=node.get("vendor", ""),
                                product_type=product_type,
                                seo_title=seo.get("title"),
                            )
                        )
                page_info = products.get("pageInfo", {})
                if page_info.get("hasNextPage"):
                    cursor = page_info.get("endCursor")
                else:
                    break
    logger.info("Collected %s variants from GraphQL", len(rows))
    return rows


def fetch_product_json(session: requests.Session, handle: str, logger: logging.Logger) -> Dict[str, Any]:
    for host in BASE_HOSTS:
        try:
            url = f"{host}/products/{handle}.json"
            resp = session.get(url, timeout=30, verify=False)
            resp.raise_for_status()
            return resp.json().get("product", {})
        except Exception as exc:
            logger.warning("Product JSON fetch failed %s (%s)", host, exc)
    return {}


def fetch_pdp_html(session: requests.Session, handle: str, logger: logging.Logger) -> str:
    for host in BASE_HOSTS:
        try:
            url = f"{host}/products/{handle}"
            resp = session.get(url, timeout=30, verify=False)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            logger.warning("PDP fetch failed %s (%s)", host, exc)
    return ""


def main() -> None:
    logger = setup_logger()
    session = build_session()
    variants = fetch_graphql_products(session, logger)

    output_rows: List[List[Any]] = []
    style_totals_by_product: Dict[str, int] = {}
    pdp_cache: Dict[str, str] = {}
    pdp_text_cache: Dict[str, str] = {}
    measurement_cache: Dict[str, str] = {}
    country_cache: Dict[str, str] = {}
    json_cache: Dict[str, Dict[str, Any]] = {}

    for var in variants:
        if var.handle not in json_cache:
            json_cache[var.handle] = fetch_product_json(session, var.handle, logger)
        if var.handle not in pdp_cache:
            pdp_cache[var.handle] = fetch_pdp_html(session, var.handle, logger)
            measurement_cache[var.handle] = measurement_text_from_html(pdp_cache[var.handle])

    for var in variants:
        product_json = json_cache.get(var.handle, {})
        pj_variants = {str(v.get("id")): v for v in product_json.get("variants", [])}
        pj_variant = pj_variants.get(var.variant_id, {})
        published_at = pj_variant.get("published_at") or product_json.get("published_at") or ""
        created_at = pj_variant.get("created_at") or product_json.get("created_at") or ""
        qty = pj_variant.get("inventory_quantity")
        old_qty = pj_variant.get("old_inventory_quantity")
        qty_available = qty if isinstance(qty, int) and qty >= 0 else 0
        notify_me = abs(qty) if isinstance(qty, int) and qty < 0 else 0
        style_id = var.product_id

        product_field = f"{var.product_title} / {var.color.upper()}" if var.color else var.product_title
        style_total = 0
        if product_field not in style_totals_by_product:
            color_lower = var.color.lower().strip()
            total = 0
            for v in product_json.get("variants", []):
                v_color = str(v.get("option1") or "").lower().strip()
                if color_lower and v_color != color_lower:
                    continue
                total += max(0, v.get("inventory_quantity", 0))
            style_totals_by_product[product_field] = total
            style_total = total
        else:
            style_total = style_totals_by_product[product_field]

        barcode = pj_variant.get("barcode", "")
        if var.handle not in pdp_text_cache:
            html = pdp_cache.get(var.handle, "")
            pdp_text_cache[var.handle] = BeautifulSoup(html, "html.parser").get_text(" ", strip=True) if html else ""
            country_cache[var.handle] = parse_country(html)
        pdp_text = pdp_text_cache.get(var.handle, "")
        measurement_text = measurement_cache.get(var.handle, pdp_text)
        rise = extract_number_after("Front Rise", measurement_text) or extract_number_after("Rise", measurement_text)
        leg_opening = extract_number_after("Leg Opening", measurement_text)
        inseam_map = parse_inseams(measurement_text)
        country = country_cache.get(var.handle, "")
        inseam_val = ""
        if var.inseam_code:
            inseam_val = inseam_map.get(var.inseam_code, "")
        if not inseam_val and inseam_map:
            inseam_val = next(iter(inseam_map.values()))

        color_snippet = normalize_color_snippet(var.description, var.color)
        color_simplified = derive_color_simplified(color_snippet)
        color_standardized = derive_color_standardized(color_snippet)
        fabric_source = "Caitac" if any(t.lower() == "caitac" for t in var.tags) else ""

        inseam_label = ""
        if var.inseam_code == "S":
            inseam_label = "Petite"
        elif var.inseam_code == "R":
            inseam_label = "Regular"
        elif var.inseam_code == "L":
            inseam_label = "Long"
        elif var.inseam_code in {"25", "25\"", "25\u201d"}:
            inseam_label = "Petite"
        elif var.inseam_code in {"27", "27\"", "27\u201d"}:
            inseam_label = "Regular"
        elif var.inseam_code in {"29", "29\"", "29\u201d"}:
            inseam_label = "Long"

        rise_label = derive_rise_label(var.description, var.tags)
        inseam_style = derive_inseam_style(var.description, var.seo_title or "")
        jean_style = derive_jean_style(var.description, var.seo_title or "")

        variant_title_parts = [var.product_title]
        if var.color:
            variant_title_parts.append(var.color.upper())
        if var.size:
            variant_title_parts.append(var.size)
        if var.inseam_code:
            variant_title_parts.append(var.inseam_code)
        variant_title = " / ".join(variant_title_parts)

        row = [
            style_id,
            var.handle,
            to_mmddyy(published_at),
            to_mmddyy(created_at),
            product_field,
            var.product_title,
            var.product_type,
            ", ".join(var.tags),
            var.vendor,
            var.description,
            variant_title,
            var.color,
            var.size,
            rise,
            inseam_val,
            leg_opening,
            var.price,
            var.compare_at,
            "TRUE" if var.available else "FALSE",
            qty_available,
            old_qty if old_qty is not None else "",
            notify_me,
            style_total,
            var.variant_id,
            var.sku,
            barcode,
            var.image_url or (product_json.get("image", {}) or {}).get("src", ""),
            var.online_url or f"https://www.ayr.com/products/{var.handle}",
            jean_style,
            inseam_label,
            rise_label,
            inseam_style,
            color_simplified,
            color_standardized,
            fabric_source,
            country,
        ]
        output_rows.append(row)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"AYR_{timestamp}.csv"
    headers = [
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
        "Old Quantity Available",
        "Notify Me Signups",
        "Quantity of style",
        "SKU - Shopify",
        "SKU - Brand",
        "Barcode",
        "Image URL",
        "SKU URL",
        "Jean Style",
        "Inseam Label",
        "Rise Label",
        "Inseam Style",
        "Color - Simplified",
        "Color - Standardized",
        "Fabric Source",
        "Country Produced",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(output_rows)
    logger.info("CSV written: %s", output_path.resolve())


if __name__ == "__main__":
    main()
