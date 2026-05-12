#!/usr/bin/env python3
"""Inventory exporter for AG Jeans (Storefront GraphQL + Constructor + PDP HTML)."""
from __future__ import annotations
import csv
import logging
import re
import time
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import requests
from bs4 import BeautifulSoup
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
})
SESSION.verify = False
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
BRAND = "AGJEANS"
LOG_PATH = BASE_DIR / f"{BRAND}_run.log"
HOST_ROTATION = [
    "https://www.agjeans.com",
    "https://agjeans.com",
    "https://agjeans-store.myshopify.com",
]
GRAPHQL_HEADERS = {
    "X-Shopify-Storefront-Access-Token": "ffae8e47a84566aa6fa059dfc56c7c56",
    "Content-Type": "application/json",
}
COLLECTION_HANDLES = ["womens-denim", "womens-sale"]
CONSTRUCTOR_API_KEY = "key_Ai9lmSZcQbh1bfYa"
CONSTRUCTOR_CLIENT_ID = "124bb124-e8d8-444b-9186-eaae4100af9f"
CONSTRUCTOR_SESSION = "20"
CONSTRUCTOR_RESULTS_PER_PAGE = 26
CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Product",
    "Style Name",
    "Style Name - Grouping",
    "Product Type",
    "Tags",
    "Vendor",
    "Description",
    "Variant Title",
    "Color",
    "Size",
    "Rise",
    "Knee",
    "Inseam",
    "Leg Opening",
    "Price",
    "Compare at Price",
    "Available for Sale",
    "Quantity Available",
    "Quantity Available (Instore Inventory)",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Hem Style",
    "Inseam Label",
    "Inseam Style",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Fabric Source",
    "Stretch",
]
GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 100, after: $cursor) {
      nodes {
        id
        handle
        title
        productType
        tags
        vendor
        description
        publishedAt
        totalInventory
        onlineStoreUrl
        featuredImage { url }
        variants(first: 100) {
          nodes {
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
        fallback = OUTPUT_DIR / f"{BRAND}_run.log"
        handlers = [logging.FileHandler(fallback, encoding="utf-8"), logging.StreamHandler()]
        logging.warning("Primary log path locked; using fallback %s", fallback)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)
def request_with_rotation(session: requests.Session, path: str, *, method: str = "GET", headers: Optional[dict] = None, json_payload: Optional[dict] = None, params: Optional[dict] = None, timeout: int = 40) -> requests.Response:
    last_error: Optional[Exception] = None
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/{path.lstrip('/')}"
        try:
            if method.upper() == "POST":
                response = session.post(url, headers=headers, json=json_payload, params=params, timeout=timeout)
            else:
                response = session.get(url, headers=headers, params=params, timeout=timeout)
            if response.status_code in {404, 410}:
                continue
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logging.warning("Request failed for %s: %s", url, exc)
            time.sleep(0.5)
    raise RuntimeError(f"Unable to request {path}: {last_error}")
def normalize_text(text: str) -> str:
    value = (text or "").lower().replace("-", " ")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()
def contains_any(text: str, phrases: Sequence[str]) -> bool:
    normalized = normalize_text(text)
    return any(normalize_text(p) in normalized for p in phrases)
def find_word(text: str, word: str) -> bool:
    n = normalize_text(text)
    w = normalize_text(word)
    return bool(re.search(rf"(^|\s){re.escape(w)}(\s|$)", n))
def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return value
def strip_gid(value: str, prefix: str) -> str:
    if not value:
        return ""
    return value.replace(prefix, "") if value.startswith(prefix) else value.split("/")[-1]
def parse_number_with_fraction(raw: str) -> str:
    text = (raw or "").replace('"', '').strip()
    if not text:
        return ""
    tokens = [t for t in re.split(r"\s+", text) if t]
    total = 0.0
    for token in tokens:
        if "/" in token:
            total += float(Fraction(token))
        else:
            total += float(token)
    out = f"{total:.4f}".rstrip("0").rstrip(".")
    return out if "." in out else f"{out}.0"
def extract_measurement(description_text: str, labels: Sequence[str]) -> str:
    text = description_text or ""
    for label in labels:
        pattern = rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?)"
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                return parse_number_with_fraction(m.group(1))
            except Exception:
                return ""
    return ""
def fetch_graphql_products(session: requests.Session, handle: str) -> List[dict]:
    products: List[dict] = []
    cursor: Optional[str] = None
    while True:
        payload = {"query": GRAPHQL_QUERY, "variables": {"handle": handle, "cursor": cursor}}
        resp = request_with_rotation(session, "/api/unstable/graphql.json", method="POST", headers=GRAPHQL_HEADERS, json_payload=payload)
        data = resp.json()
        if data.get("errors"):
            raise RuntimeError(f"GraphQL error for {handle}: {data['errors']}")
        node = (((data.get("data") or {}).get("collection") or {}).get("products") or {})
        batch = node.get("nodes") or []
        products.extend(batch)
        page_info = node.get("pageInfo") or {}
        logging.info("GraphQL %s fetched %s products (total %s)", handle, len(batch), len(products))
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
        if not cursor:
            break
    return products
def fetch_constructor_collection(session: requests.Session, group_id: str) -> List[dict]:
    out: List[dict] = []
    page = 1
    while True:
        params = {
            "key": CONSTRUCTOR_API_KEY,
            "i": CONSTRUCTOR_CLIENT_ID,
            "s": CONSTRUCTOR_SESSION,
            "num_results_per_page": CONSTRUCTOR_RESULTS_PER_PAGE,
            "page": page,
        }
        resp = session.get(f"https://ac.cnstrc.com/browse/group_id/{group_id}", params=params, timeout=40)
        resp.raise_for_status()
        payload = resp.json()
        response = payload.get("response") or {}
        results = response.get("results") or []
        if not results:
            break
        out.extend(results)
        total = int(response.get("total_num_results") or 0)
        if page * CONSTRUCTOR_RESULTS_PER_PAGE >= total:
            break
        page += 1
    logging.info("Constructor %s fetched %s results", group_id, len(out))
    return out
def build_constructor_maps(results: Iterable[dict]) -> Tuple[Dict[str, dict], Dict[str, dict], Dict[str, dict]]:
    by_handle: Dict[str, dict] = {}
    by_variant_id: Dict[str, dict] = {}
    by_sku: Dict[str, dict] = {}
    for result in results:
        data = result.get("data") or {}
        handle = data.get("handle") or ""
        if handle:
            by_handle[handle] = result
        for var in result.get("variations") or []:
            vdata = var.get("data") or {}
            variation_id = str(vdata.get("variation_id") or "")
            sku = str(vdata.get("sku") or "").upper().strip()
            if variation_id:
                by_variant_id[variation_id] = {"result": result, "variation": var}
            if sku:
                by_sku[sku] = {"result": result, "variation": var}
    return by_handle, by_variant_id, by_sku
def clean_html_text(raw_html: str) -> str:
    return BeautifulSoup(raw_html or "", "html.parser").get_text(" ", strip=True)
def parse_pdp(handle: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    details_div = soup.find("div", id=re.compile(r"^ProductAccordion-"))
    details_text = details_div.get_text("\n", strip=True) if details_div else ""
    text = details_text or soup.get_text(" ", strip=True)
    rise = extract_measurement(text, ["Rise", "Front Rise"])
    knee = extract_measurement(text, ["Knee Opening", "Knee"])
    inseam = extract_measurement(text, ["Inseam"])
    leg_opening = extract_measurement(text, ["Bottom Opening", "Bottom", "Leg Opening"])
    stretch = ""
    stretch_el = soup.select_one("#Region-tab_content_7WNRkU .stretch-icons .icon-wrapper.active span")
    if stretch_el:
        raw_stretch = stretch_el.get_text(" ", strip=True)
        if contains_any(raw_stretch, ["non stretch"]):
            stretch = "Rigid"
        elif contains_any(raw_stretch, ["comfort stretch"]):
            stretch = "Low Stretch"
        elif contains_any(raw_stretch, ["power stretch"]):
            stretch = "Medium Stretch"
        elif contains_any(raw_stretch, ["super stretch"]):
            stretch = "Medium to High Stretch"
        elif contains_any(raw_stretch, ["ultimate stretch"]):
            stretch = "High Stretch"
    return {"rise": rise, "knee": knee, "inseam": inseam, "leg_opening": leg_opening, "stretch": stretch}
def get_option(variant: dict, names: Sequence[str]) -> str:
    options = variant.get("selectedOptions") or []
    for option in options:
        name = (option.get("name") or "").strip().lower()
        if name in names:
            return option.get("value") or ""
    return ""
def classify_color_standardized(color: str, description: str, tags: str) -> str:
    def classify(source: str) -> str:
        rules = [
            (["animal print", "leopard", "snake"], "Animal Print"),
            (["black"], "Black"),
            (["blue", "indigo"], "Blue"),
            (["brown"], "Brown"),
            (["tan", "taupe", "beige", "khaki", "canvas"], "Tan"),
            (["white", "ecru", "off white wash"], "White"),
            (["green", "olive", "sage"], "Green"),
            (["grey", "smoke"], "Grey"),
            (["orange"], "Orange"),
            (["pink"], "Pink"),
            (["print"], "Print"),
            (["purple"], "Purple"),
            (["red"], "Red"),
            (["yellow"], "Yellow"),
        ]
        for phrases, label in rules:
            if any(find_word(source, p) for p in phrases):
                return label
        return ""
    out = classify(color)
    if not out:
        out = classify(description)
    if out:
        return out
    m = re.search(r"(?:^|,)\s*Color\s*:\s*([^,]+)", tags, flags=re.IGNORECASE)
    return (m.group(1).strip() if m else "")
def classify_color_simplified(color_standardized: str, description: str, tags: str) -> str:
    if color_standardized.lower() in {"grey", "white", "tan"}:
        return "Light"
    if contains_any(description, ["light to medium", "medium to light", "medium light", "light medium"]):
        return "Light to Medium"
    if contains_any(description, ["medium to dark", "dark to medium", "medium dark", "dark medium"]):
        return "Medium to Dark"
    if contains_any(description, ["dark", "black", "navy"]):
        return "Dark"
    if contains_any(description, ["light wash", "light denim", "light indigo", "light blue"]):
        return "Light"
    if contains_any(description, ["medium", "mid blue", "classic blue"]):
        return "Medium"

    # Tag fallback: support multiple Wash: tags and strip trailing 'wash' text.
    wash_values = [m.strip() for m in re.findall(r"(?:^|,)\s*Wash\s*:\s*([^,]+)", tags, flags=re.IGNORECASE)]
    if not wash_values:
        return ""

    normalized_washes = set()
    for wash in wash_values:
        cleaned = re.sub(r"\bwash\b", "", wash, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if cleaned:
            normalized_washes.add(cleaned.lower())

    if "medium" in normalized_washes and "light" in normalized_washes:
        return "Light to Medium"
    if "medium" in normalized_washes and "dark" in normalized_washes:
        return "Medium to Dark"
    if "black" in normalized_washes:
        return "Dark"
    if "white" in normalized_washes:
        return "Light"
    if "color" in normalized_washes:
        return ""

    # If there are multiple custom wash values, return first cleaned title-cased value.
    first_cleaned = re.sub(r"\bwash\b", "", wash_values[0], flags=re.IGNORECASE)
    first_cleaned = re.sub(r"\s+", " ", first_cleaned).strip()
    return first_cleaned.title() if first_cleaned else ""
def determine_hem_style(description: str, tags: str) -> str:
    rules = [
        (["frayed hem", "frayed hems", "fraying at the hems"], "Frayed Hem"),
        (["cuffed hem", "double cuffed hems", "cuffs at the hem"], "Cuffed Hem"),
        (["raw hem", "raw hems", "raw cut hems", "raw cut hem"], "Raw Hem"),
        (["clean hems"], "Clean Hems"),
        (["released hem"], "Released Hem"),
        (["split hem"], "Split Hem"),
        (["distressed hem", "distressing at the hem", "light distressing at the hem", "busted hems", "well worn hems"], "Distressed Hem"),
    ]
    for phrases, label in rules:
        if contains_any(description, phrases):
            return label
    m = re.search(r"(?:^|,)\s*Hem\s*:\s*([^,]+)", tags, flags=re.IGNORECASE)
    if not m:
        return ""
    hem = m.group(1).strip()
    mapped = {"Busted Hem": "Distressed Hem", "Vintage Raw Hem": "Raw Hem", "Cuff": "Cuffed Hem"}
    return mapped.get(hem, hem)
def infer_straight_from_leg_opening(leg_opening: str) -> str:
    try:
        value = float(leg_opening)
    except (TypeError, ValueError):
        return ""
    if value < 15.5:
        return "Straight from Knee"
    if 15.5 <= value <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"
def determine_jean_style(title: str, title_v2: str, description: str, tags: str, leg_opening: str) -> str:
    def step_text(source: str, *, allow_slim: bool = False, allow_capri: bool = False, allow_tapered: bool = False) -> Tuple[str, bool]:
        if contains_any(source, ["wide leg", "palazzo"]):
            return "Wide Leg", False
        if contains_any(source, ["flare"]):
            return "Flare", False
        if contains_any(source, ["bootcut", "boot"]):
            return "Bootcut", False
        if contains_any(source, ["skinny"] + (["slim"] if allow_slim else []) + (["cigarette leg"] if allow_slim else [])):
            return "Skinny", False
        if contains_any(source, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]):
            return "Barrel", False
        if contains_any(source, ["baggy", "work pant"]):
            return "Baggy", False
        if contains_any(source, ["boyfriend"]) and not contains_any(source, ["ex boyfriend"]):
            return "Boyfriend", False
        if allow_capri and contains_any(source, ["capri"]):
            return "Capri", False
        if contains_any(source, ["slim straight"]):
            return "Straight from Knee", False
        if allow_tapered and contains_any(source, ["taper", "tapering", "tapered"]):
            return "Tapered", False
        straight_found = contains_any(source, ["straight", "straight leg", "straight jeans"])
        return "", straight_found
    out, straight_found = step_text(title, allow_slim=True)
    if out:
        return out
    out, straight_v2 = step_text(title_v2, allow_slim=True, allow_capri=True)
    if out:
        return out
    out, straight_desc = step_text(description, allow_slim=True, allow_capri=True, allow_tapered=True)
    if out:
        return out
    if straight_found or straight_v2 or straight_desc:
        inferred = infer_straight_from_leg_opening(leg_opening)
        if inferred:
            return inferred
    fit_tags = [t.strip() for t in tags.split(",") if t.strip().lower().startswith("fit:")]
    fit_values = [t.split(":", 1)[1].strip().lower() for t in fit_tags if ":" in t]
    fit_set = set(fit_values)
    if "wide" in fit_set:
        return "Wide Leg"
    if "flare" in fit_set:
        return "Flare"
    if "boot" in fit_set:
        return "Bootcut"
    if "skinny" in fit_set or "cigarette" in fit_set:
        return "Skinny"
    if "barrel" in fit_set:
        return "Barrel"
    if "boyfriend" in fit_set and "relaxed" in fit_set:
        return "Boyfriend"
    if "boyfriend" in fit_set and "slim" in fit_set:
        return "Tapered"
    if "slim" in fit_set and "straight" in fit_set:
        return "Straight from Knee"
    if "relaxed" in fit_set and "straight" in fit_set:
        return "Straight from Knee"
    if fit_values == ["straight"] or fit_set == {"straight"}:
        return infer_straight_from_leg_opening(leg_opening)
    return ""
def determine_rise_label(title_v2: str, description: str) -> str:
    for src in [title_v2, description]:
        n = normalize_text(src)
        if "mid rise" in n or re.search(r"(^|\s)mid(\s|$)", n):
            return "Mid"
        if "low rise" in n or re.search(r"(^|\s)low(\s|$)", n):
            return "Low"
        if "high rise" in n or re.search(r"(^|\s)high(\s|$)", n):
            return "High"
    return ""
def determine_inseam_label(style_name: str, description: str) -> str:
    sn = normalize_text(style_name)
    if "extended" in sn:
        return "Long"
    if "petite" in sn:
        return "Petite"
    if "crop" in sn and "ankle" in sn:
        return "Petite"
    if contains_any(description, ["longer inseam", "extra long inseam", "ideal for taller frames", "longest inseam"]):
        return "Long"
    return ""
def determine_inseam_style(style_name: str, description: str) -> str:
    if find_word(style_name, "ankle"):
        return "Ankle"
    if find_word(style_name, "crop"):
        return "Cropped"
    if find_word(description, "ankle"):
        return "Ankle"
    if find_word(description, "crop"):
        return "Cropped"
    if find_word(description, "puddle"):
        return "Full"
    return ""
def build_style_name(product_title: str, title_v2: str) -> str:
    lower_title = normalize_text(product_title)
    jean_match = re.search(r"\bjeans?\b", lower_title)
    if jean_match:
        raw_words = product_title.strip().split()
        idx = next((i for i, w in enumerate(raw_words) if normalize_text(w) in {"jean", "jeans"}), len(raw_words))
        base_text = " ".join(raw_words[:idx]).strip()
    else:
        base_text = (product_title.strip().split()[:1] or [""])[0]

    # Remove duplicate words between base and title_v2 from title_v2 side only.
    base_tokens_norm = {normalize_text(t) for t in re.split(r"\s+", base_text) if t}
    v2_tokens = [t for t in re.split(r"\s+", (title_v2 or "").strip()) if t]
    v2_filtered = [tok for tok in v2_tokens if normalize_text(tok) not in base_tokens_norm]
    combined = re.sub(r"\s+", " ", f"{base_text} {' '.join(v2_filtered)}").strip()

    move_terms = [
        "360°", "Ankle", "Belted", "Cinched", "Coated Denim", "Colorblock", "Crop", "Cuffed", "Embellished",
        "Extended", "Flocked Stripe", "Flocked", "Panel", "Paneled", "Patchwork", "Selvage", "Splatter",
        "Split Hem", "Studded", "Vapor Wash",
    ]

    def pop_term(src: str, term: str) -> tuple[str, bool]:
        pattern = r"\b" + re.escape(term).replace(r"\ ", r"\s+") + r"\b"
        m = re.search(pattern, src, flags=re.IGNORECASE)
        if not m:
            return src, False
        out = (src[:m.start()] + " " + src[m.end():]).strip()
        out = re.sub(r"\s+", " ", out)
        return out, True

    work = combined
    moved: List[str] = []
    seen_norm: set[str] = set()
    for term in move_terms:
        while True:
            work, found = pop_term(work, term)
            if not found:
                break
            n = normalize_text(term)
            if n not in seen_norm:
                moved.append(term)
                seen_norm.add(n)

    # Keep ankle/crop/extended at the very end when present.
    tail_priority = ["Ankle", "Crop", "Extended"]
    tail = [t for t in tail_priority if normalize_text(t) in seen_norm]
    moved_main = [t for t in moved if normalize_text(t) not in {normalize_text(x) for x in tail_priority}]

    final_parts = [work] if work else []
    final_parts.extend(moved_main)
    final_parts.extend(tail)
    return re.sub(r"\s+", " ", " ".join(p for p in final_parts if p)).strip()
def build_style_grouping(product_title: str, jean_style: str) -> str:
    words = [w for w in product_title.split() if w]
    if not words:
        return ""
    first = words[0]
    if first.lower() in {"the", "a", "an"} and len(words) > 1:
        base = f"{words[0]} {words[1]}"
    else:
        base = words[0]
    if not jean_style:
        return base
    if normalize_text(jean_style).startswith(normalize_text(base)):
        return base
    return f"{base} {jean_style}".strip()
def fetch_pdp_details(session: requests.Session, handle: str) -> Dict[str, str]:
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/products/{handle}"
        try:
            response = session.get(url, timeout=40, verify=False)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            return parse_pdp(handle, response.text)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to fetch PDP %s (%s)", url, exc)
    return {"rise": "", "knee": "", "inseam": "", "leg_opening": "", "stretch": ""}
def fetch_pdp_cache(session: requests.Session, handles: Iterable[str]) -> Dict[str, dict]:
    cache: Dict[str, dict] = {}
    unique_handles = sorted(set(h for h in handles if h))
    total = len(unique_handles)
    for i, handle in enumerate(unique_handles, start=1):
        cache[handle] = fetch_pdp_details(session, handle)
        if i % 50 == 0 or i == total:
            logging.info("PDP fetched %s/%s", i, total)
    return cache
def clean_tags(tags: Any) -> str:
    if isinstance(tags, list):
        return ", ".join(str(t) for t in tags)
    return str(tags or "")
def is_jeans_product(tags: str) -> bool:
    return "category:jeans" in tags.lower()


def is_excluded_title(title: str) -> bool:
    n = normalize_text(title)
    blocked = ["bermuda", "a line", "dress", "short", "shirt", "top", "culotte"]
    return any(find_word(n, term) for term in blocked)


def filter_products_for_pdp(products_by_handle: Dict[str, dict]) -> Dict[str, dict]:
    filtered: Dict[str, dict] = {}
    seen = set()
    for handle, product in products_by_handle.items():
        tags = clean_tags(product.get("tags"))
        title = str(product.get("title") or "")
        if not is_jeans_product(tags):
            continue
        if is_excluded_title(title):
            continue
        style_id = strip_gid(product.get("id") or "", "gid://shopify/Product/")
        key = (style_id, handle)
        if key in seen:
            continue
        seen.add(key)
        filtered[handle] = product
    logging.info("Products retained before PDP fetch: %s/%s", len(filtered), len(products_by_handle))
    return filtered

def to_price(value: Optional[str]) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"${float(value):.2f}"
    except Exception:
        return str(value)
def dedupe_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out = []
    for row in rows:
        key = (row.get("Style Id"), row.get("SKU - Shopify"), row.get("SKU - Brand"))
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out
def main() -> None:
    configure_logging()
    session = SESSION
    products_by_handle: Dict[str, dict] = {}
    for collection in COLLECTION_HANDLES:
        for product in fetch_graphql_products(session, collection):
            handle = product.get("handle") or ""
            if handle:
                products_by_handle[handle] = product
    constructor_results = fetch_constructor_collection(session, "womens-denim") + fetch_constructor_collection(session, "womens-sale")
    by_handle, by_variant_id, by_sku = build_constructor_maps(constructor_results)
    products_by_handle = filter_products_for_pdp(products_by_handle)
    pdp_cache = fetch_pdp_cache(session, products_by_handle.keys())
    rows: List[dict] = []
    for handle, product in products_by_handle.items():
        tags = clean_tags(product.get("tags"))
        pdp = pdp_cache.get(handle, {})
        constructor_product = by_handle.get(handle, {})
        cdata = constructor_product.get("data") or {}
        product_title = product.get("title") or ""
        title_v2 = cdata.get("subtitle") or ""
        style_name = build_style_name(product_title, title_v2)
        if is_excluded_title(style_name):
            continue
        description = clean_html_text(product.get("description") or "")
        for variant in ((product.get("variants") or {}).get("nodes") or []):
            variant_id = strip_gid(variant.get("id") or "", "gid://shopify/ProductVariant/")
            sku_brand = (variant.get("sku") or "").strip()
            constructor_hit = by_variant_id.get(variant_id) or by_sku.get(sku_brand.upper()) or {}
            cresult = (constructor_hit.get("result") or {})
            cvariation = (constructor_hit.get("variation") or {})
            cvariation_data = cvariation.get("data") or {}
            color = get_option(variant, ["color", "option1"]) or ""
            size = get_option(variant, ["size", "option2"]) or ""
            inseam_opt = get_option(variant, ["inseam", "option3"]) or ""
            rise = pdp.get("rise") or ""
            knee = pdp.get("knee") or ""
            inseam = pdp.get("inseam") or (parse_number_with_fraction(inseam_opt) if inseam_opt else "")
            leg_opening = pdp.get("leg_opening") or ""
            stretch = pdp.get("stretch") or ""
            inventory_list = cvariation_data.get("inventory") or []
            instore_qty = sum(int(x.get("available") or 0) for x in inventory_list if isinstance(x, dict))
            jean_style = determine_jean_style(product_title, str(title_v2), description, tags, leg_opening)
            rise_label = determine_rise_label(str(title_v2), description)
            hem_style = determine_hem_style(description, tags)
            inseam_label = determine_inseam_label(style_name, description)
            inseam_style = determine_inseam_style(style_name, description)
            color_std = classify_color_standardized(color, description, tags)
            color_simple = classify_color_simplified(color_std, description, tags)
            fabric_source = ""
            mill = (cresult.get("data") or {}).get("mill")
            if isinstance(mill, list) and mill:
                fabric_source = str(mill[0])
            elif isinstance(mill, str):
                fabric_source = mill
            style_grouping = build_style_grouping(product_title, jean_style)
            product_display = f"{style_name} - {color}" if color else style_name
            row = {
                "Style Id": strip_gid(product.get("id") or "", "gid://shopify/Product/"),
                "Handle": handle,
                "Published At": format_date(product.get("publishedAt")),
                "Product": product_display,
                "Style Name": style_name,
                "Style Name - Grouping": style_grouping,
                "Product Type": "Jeans",
                "Tags": tags,
                "Vendor": product.get("vendor") or "",
                "Description": description,
                "Variant Title": f"{product_title} - {variant.get('title') or ''}".strip(" -"),
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Knee": knee,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": to_price((variant.get("price") or {}).get("amount")),
                "Compare at Price": to_price((variant.get("compareAtPrice") or {}).get("amount")),
                "Available for Sale": str(bool(variant.get("availableForSale"))).upper(),
                "Quantity Available": variant.get("quantityAvailable") if variant.get("quantityAvailable") is not None else "",
                "Quantity Available (Instore Inventory)": instore_qty,
                "Quantity of style": product.get("totalInventory") if product.get("totalInventory") is not None else "",
                "SKU - Shopify": variant_id,
                "SKU - Brand": sku_brand,
                "Barcode": variant.get("barcode") or "",
                "Image URL": (product.get("featuredImage") or {}).get("url") or "",
                "SKU URL": f"https://www.agjeans.com/products/{handle}",
                "Jean Style": jean_style,
                "Hem Style": hem_style,
                "Inseam Label": inseam_label,
                "Inseam Style": inseam_style,
                "Rise Label": rise_label,
                "Color - Simplified": color_simple,
                "Color - Standardized": color_std,
                "Fabric Source": fabric_source,
                "Stretch": stretch,
            }
            rows.append(row)
    rows = dedupe_rows(rows)
    # Step-5 style inference fallback and Rise Label fallback by style grouping
    by_first_word: Dict[str, List[dict]] = {}
    by_style_name: Dict[str, List[dict]] = {}
    for row in rows:
        first_word = (row["Style Name"].split()[:1] or [""])[0].lower()
        by_first_word.setdefault(first_word, []).append(row)
        by_style_name.setdefault(row["Style Name"], []).append(row)
    for row in rows:
        if not row["Jean Style"]:
            first_word = (row["Style Name"].split()[:1] or [""])[0].lower()
            peers = [r for r in by_first_word.get(first_word, []) if r.get("Jean Style")]
            if peers:
                styles = sorted({r["Jean Style"] for r in peers})
                if len(styles) == 1:
                    row["Jean Style"] = styles[0]
                else:
                    row["Jean Style"] = peers[0]["Jean Style"]
                row["Style Name - Grouping"] = build_style_grouping(row["Style Name"], row["Jean Style"])
        if not row["Rise Label"]:
            peers = [r for r in by_style_name.get(row["Style Name"], []) if r.get("Rise Label")]
            if peers:
                labels = sorted({r["Rise Label"] for r in peers})
                if len(labels) == 1:
                    row["Rise Label"] = labels[0]
                else:
                    row["Rise Label"] = peers[0]["Rise Label"]

    # Fill blank Fabric Source from other rows with the same Style Id when available.
    style_fabric_map: Dict[str, str] = {}
    for row in rows:
        style_id = str(row.get("Style Id") or "").strip()
        fabric = str(row.get("Fabric Source") or "").strip()
        if style_id and fabric and style_id not in style_fabric_map:
            style_fabric_map[style_id] = fabric
    for row in rows:
        if not str(row.get("Fabric Source") or "").strip():
            style_id = str(row.get("Style Id") or "").strip()
            if style_id in style_fabric_map:
                row["Fabric Source"] = style_fabric_map[style_id]

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out_path = OUTPUT_DIR / f"AGJEANS_{ts}.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info("Rows written: %s", len(rows))
    logging.info("CSV written: %s", out_path.resolve())
if __name__ == "__main__":
    main()
