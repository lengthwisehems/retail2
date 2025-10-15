import argparse
import ast
import csv
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from decimal import Decimal, InvalidOperation
from html import unescape
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse, urlunparse

import requests

BASE_URL = "https://www.motherdenim.com"
HOST_FALLBACKS = {
    "www.motherdenim.com": ["motherdenim.com"],
    "motherdenim.com": ["www.motherdenim.com"],
}
SEARCHSPRING_URL = "https://00svms.a.searchspring.io/api/search/autocomplete.json"

COLLECTION_ENDPOINTS = [
    (f"{BASE_URL}/collections/denim/products.json", True),
    (f"{BASE_URL}/collections/denim-sale/products.json", False),
]

LOCAL_SEARCHSPRING_DOCS = [
    os.path.join("docs", f"SearchSpring{index}.json") for index in range(1, 5)
]

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "mother_inventory_run.log"

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
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
    "Quantity Price Breaks",
    "Available for Sale",
    "Quantity Available",
    "Google Analytics Purchases",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Hem Style",
    "Inseam Label",
    "Rise Label",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Generic HTTP utilities with retry/backoff
# ---------------------------------------------------------------------------
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
DNS_ERROR_KEYWORDS = [
    "failed to resolve",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo failed",
    "nodename nor servname provided",
]


def iter_url_candidates(url: str) -> Iterable[str]:
    parsed = urlparse(url)
    netloc = parsed.netloc
    candidates: List[str] = [netloc]
    candidates.extend(HOST_FALLBACKS.get(netloc.lower(), []))
    seen: Set[str] = set()
    for host in candidates:
        if not host:
            continue
        key = host.lower()
        if key in seen:
            continue
        seen.add(key)
        yield urlunparse(parsed._replace(netloc=host))


def is_name_resolution_error(exc: Exception) -> bool:
    parts: List[str] = []
    current: Optional[BaseException] = exc
    visited: Set[int] = set()
    while current and id(current) not in visited:
        visited.add(id(current))
        parts.append(str(current))
        parts.append(repr(current))
        current = getattr(current, "__cause__", None)
    combined = " ".join(parts).lower()
    return any(keyword in combined for keyword in DNS_ERROR_KEYWORDS)


def request_with_retry(url: str, *, params: Optional[Any] = None, expect_json: bool = True) -> Any:
    last_exc: Optional[Exception] = None
    candidates = list(iter_url_candidates(url))
    for idx, candidate_url in enumerate(candidates):
        for attempt in range(5):
            try:
                response = session.get(candidate_url, params=params, timeout=30)
                if response.status_code in TRANSIENT_STATUSES:
                    raise requests.HTTPError(f"transient status {response.status_code}")
                response.raise_for_status()
                if idx > 0 and candidate_url != url:
                    log(f"[fallback] succeeded via {candidate_url}")
                return response.json() if expect_json else response.text
            except Exception as exc:
                last_exc = exc
                if attempt < 4:
                    sleep_for = 1.0 + attempt * 1.0
                    log(f"[retry] {candidate_url} ({exc}); sleeping {sleep_for:.1f}s")
                    time.sleep(sleep_for)
                    continue
                log(f"[error] {candidate_url} failed after retries: {exc}")
                break
        if idx + 1 < len(candidates) and last_exc and is_name_resolution_error(last_exc):
            next_candidate = candidates[idx + 1]
            log(f"[retry] switching host for {url} -> {next_candidate}")
            continue
        break
    if last_exc:
        raise last_exc
    raise RuntimeError("unreachable retry loop")


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
FRACTION_RE = re.compile(r"^\s*(?P<int>\d+)(?:\s+(?P<num>\d+)\s*/\s*(?P<den>\d+))?(?:\.(?P<dec>\d+))?\s*$")
MEASURE_RE_TEMPLATE = r"(?P<value>-?\d[^,;]*)\s*(?:&quot;|\"|”|″)?\s*{label}"
ALLOWED_JEAN_STYLES = {
    "straight",
    "wide leg",
    "flare",
    "shorts",
    "bootcut",
    "barrel leg",
    "skirt",
    "skinny",
}
INSEAM_OVERRIDE_KEYWORDS = {"petite", "tall", "long", "ankle"}


def parse_fractional_number(text: str) -> str:
    if not text:
        return ""
    cleaned = (
        text.replace("½", " 1/2")
        .replace("¼", " 1/4")
        .replace("¾", " 3/4")
        .replace("’", "")
        .replace("'", "")
        .replace("″", "")
        .replace("“", "")
        .replace("”", "")
        .replace("\"", "")
    )
    cleaned = re.sub(r"[^0-9./\s]", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    match = FRACTION_RE.match(cleaned)
    if not match:
        return ""
    integer = float(match.group("int"))
    numerator = match.group("num")
    denominator = match.group("den")
    decimal_part = match.group("dec")
    value = integer
    if decimal_part is not None:
        try:
            value = float(f"{int(match.group('int'))}.{decimal_part}")
            return f"{value:.2f}".rstrip("0").rstrip(".")
        except Exception:
            pass
    if numerator and denominator:
        try:
            value += float(numerator) / float(denominator)
        except Exception:
            pass
    if abs(value - round(value)) < 1e-6:
        return str(int(round(value)))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def extract_measure(details_text: Optional[str], label: str) -> str:
    if not details_text:
        return ""
    search_text = unescape(details_text)
    pattern = re.compile(MEASURE_RE_TEMPLATE.format(label=re.escape(label)), flags=re.IGNORECASE)
    match = pattern.search(search_text)
    if not match:
        return ""
    return parse_fractional_number(match.group("value"))


def parse_measurements(details_text: Optional[str]) -> Tuple[str, str, str]:
    return (
        extract_measure(details_text, "Rise"),
        extract_measure(details_text, "Inseam"),
        extract_measure(details_text, "Leg Opening"),
    )


def format_price(value: Any) -> str:
    if value in (None, ""):
        return ""
    try:
        if isinstance(value, str) and value.strip().startswith("$"):
            return value.strip()
        number = float(str(value))
        if number > 999:  # treat as cents
            return f"${number / 100:.2f}"
        return f"${number:.2f}"
    except Exception:
        try:
            number = int(value)
            if number > 999:
                return f"${number / 100:.2f}"
            return str(number)
        except Exception:
            return str(value)


def parse_published_at(iso_string: Optional[str]) -> str:
    if not iso_string:
        return ""
    try:
        dt = datetime.fromisoformat(iso_string.replace("Z", ""))
        return dt.strftime("%m/%d/%y")
    except Exception:
        match = re.search(r"(\d{4})-(\d{2})-(\d{2})", iso_string)
        if match:
            year, month, day = match.groups()
            return f"{month}/{day}/{year[-2:]}"
        return ""


def clean_html_text(html_text: Optional[str]) -> str:
    if not html_text:
        return ""
    text = unescape(html_text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def normalize_tags(tags_field: Any) -> str:
    if tags_field in (None, ""):
        return ""
    if isinstance(tags_field, list):
        return ", ".join(str(tag) for tag in tags_field if tag)
    return str(tags_field)


def derive_style_name(default_title: str, style_field: Optional[str]) -> str:
    if style_field:
        return str(style_field)
    if not default_title:
        return ""
    return default_title.split(" - ")[0].strip()


SCIENTIFIC_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?e[+-]?\d+$", re.IGNORECASE)


def stringify_identifier(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        text = format(value, "f").rstrip("0").rstrip(".")
        return text or "0"

    text = str(value).strip()
    if not text:
        return ""

    if SCIENTIFIC_RE.match(text):
        try:
            decimal_value = Decimal(text)
            if decimal_value == decimal_value.to_integral():
                return format(decimal_value.quantize(Decimal(1)), "f")
            normalized = decimal_value.normalize()
            return format(normalized, "f").rstrip("0").rstrip(".")
        except InvalidOperation:
            pass

    if text.lower().endswith(".0") and text.replace(".", "", 1).isdigit():
        return text[:-2]

    return text


def first_string(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if item not in (None, ""):
                return str(item)
        return ""
    return str(value)


def derive_product_type(value: Any) -> str:
    if not value:
        return ""
    if isinstance(value, list):
        cleaned = [str(v).strip() for v in value if v]
    else:
        cleaned = [v.strip() for v in str(value).split(",") if v.strip()]
    if not cleaned:
        return ""
    for candidate in cleaned:
        if candidate.lower() != "jeans":
            return candidate
    return cleaned[0]


def ensure_iterable(value: Any) -> Iterable[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        for item in value:
            if item is not None:
                yield str(item)
        return
    for chunk in str(value).split(","):
        chunk = chunk.strip()
        if chunk:
            yield chunk


def parse_jean_style(value: Any) -> str:
    styles = []
    for item in ensure_iterable(value):
        lowered = item.lower()
        if lowered in ALLOWED_JEAN_STYLES:
            styles.append(item.strip())
    if not styles:
        return ""
    # Remove duplicates while preserving order
    seen = set()
    result: List[str] = []
    for style in styles:
        key = style.lower()
        if key not in seen:
            seen.add(key)
            result.append(style)
    return ", ".join(result)


def parse_inseam_label(ss_fit: Any, ss_inseam: Any) -> str:
    chosen = ""
    fit_values = [item.lower() for item in ensure_iterable(ss_fit)]
    for fit in fit_values:
        for keyword in INSEAM_OVERRIDE_KEYWORDS:
            if keyword in fit:
                return keyword.title()
    if ss_inseam:
        values = list(ensure_iterable(ss_inseam))
        if values:
            chosen = values[0]
    return chosen


def parse_rise_label(value: Any) -> str:
    values = list(ensure_iterable(value))
    return values[0] if values else ""


def normalize_quantity_price_breaks(raw_value: Any) -> str:
    if raw_value in (None, ""):
        return ""
    if isinstance(raw_value, (list, dict)):
        try:
            return json.dumps(raw_value, ensure_ascii=False)
        except Exception:
            return str(raw_value)
    return str(raw_value)


def coerce_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except Exception:
        return None


def derive_handle_from_url(url: Optional[str]) -> str:
    if not url:
        return ""
    match = re.search(r"/products/([^/?#]+)", str(url))
    if match:
        return match.group(1)
    return ""


def parse_embedded_json(raw_value: Any) -> Any:
    if raw_value in (None, ""):
        return None
    if isinstance(raw_value, (list, dict)):
        return raw_value
    text = first_string(raw_value)
    if not text:
        return None

    previous = None
    while text != previous:
        previous = text
        text = unescape(text).strip()
    if not text:
        return None

    candidates = {text, text.replace("\\\"", '"')}

    for candidate in list(candidates):
        try:
            return json.loads(candidate)
        except Exception:
            continue

    pythonish = (
        text.replace("null", "None")
        .replace("true", "True")
        .replace("false", "False")
        .replace("\\/", "/")
    )
    try:
        return ast.literal_eval(pythonish)
    except Exception:
        return None


def extract_inventory_from_variants_list(raw_value: Any) -> Dict[str, int]:
    inventories: Dict[str, int] = {}
    parsed = parse_embedded_json(raw_value)

    def record_inventory(variant_id: Any, quantity: Any) -> None:
        vid = str(variant_id).strip()
        qty = coerce_int(quantity)
        if not vid or qty is None:
            return
        inventories.setdefault(vid, qty)

    entries: List[Any] = []
    if isinstance(parsed, list):
        entries = parsed
    elif isinstance(parsed, dict):
        entries = [parsed]

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        record_inventory(
            entry.get("id") or entry.get("uid"),
            entry.get("inventory_quantity")
            if "inventory_quantity" in entry
            else entry.get("inventoryQuantity"),
        )

    if inventories:
        return inventories

    text = unescape(str(raw_value) if raw_value is not None else "")
    pattern = re.compile(
        r'"id"\s*:\s*(?:"?)(?P<id>\d+)(?:"?)\s*,[^{}]*?"inventory(?:_quantity|Quantity)"\s*:\s*(?:"?)(?P<qty>-?\d+)',
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(text):
        record_inventory(match.group("id"), match.group("qty"))

    return inventories


def extract_variant_id_from_url(url: Optional[str]) -> str:
    if not url:
        return ""
    match = re.search(r"variant=(\d+)", str(url))
    if match:
        return match.group(1)
    return ""


def split_variant_ids(raw_value: Any) -> List[str]:
    if raw_value in (None, ""):
        return []
    if isinstance(raw_value, (list, tuple, set)):
        return [str(part).strip() for part in raw_value if str(part).strip()]
    text = str(raw_value)
    parts: List[str] = []
    for candidate in re.split(r"[|,\s]+", text):
        candidate = candidate.strip()
        if not candidate:
            continue
        if candidate.isdigit():
            parts.append(candidate)
            continue
        extracted = extract_variant_id_from_url(candidate)
        if extracted:
            parts.append(extracted)
    return parts


def extract_inventory_from_bundle_variants(raw_value: Any) -> Dict[str, int]:
    inventories: Dict[str, int] = {}
    parsed = parse_embedded_json(raw_value)
    if isinstance(parsed, dict):
        entries = [parsed]
    elif isinstance(parsed, list):
        entries = parsed
    else:
        entries = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        core = entry.get("mappings", {}).get("core", {})
        uid = core.get("uid") or entry.get("uid") or entry.get("id")
        if not uid:
            uid = extract_variant_id_from_url(core.get("url") or entry.get("url"))
        qty = entry.get("attributes", {}).get("quantity")
        if qty is None:
            qty = entry.get("quantity")
        qty_int = coerce_int(qty)
        if uid and qty_int is not None:
            inventories[str(uid)] = qty_int

    if inventories:
        return inventories

    text = str(raw_value or "")
    previous = None
    while text != previous:
        previous = text
        text = unescape(text)
    text = text.replace("\\/", "/")

    regex_patterns = [
        re.compile(
            r'(?:"uid"\s*:\s*"?(?P<id>\d+)"?|variant=["\']?(?P<id_alt>\d+))[^{}]*?"quantity"\s*:\s*"?(?P<qty>-?\d+)',
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r'"quantity"\s*:\s*"?(?P<qty>-?\d+)"[^{}]*?(?:"uid"|variant=)["\']?(?P<id>\d+)',
            re.IGNORECASE | re.DOTALL,
        ),
    ]

    for pattern in regex_patterns:
        for match in pattern.finditer(text):
            variant_id = match.group("id") or match.groupdict().get("id_alt")
            qty = match.group("qty")
            qty_int = coerce_int(qty)
            if variant_id and qty_int is not None:
                inventories.setdefault(str(variant_id), qty_int)

    return inventories


def update_variant_record(
    variant_map: Dict[str, Dict[str, Any]],
    variant_id: Any,
    quantity: Optional[int] = None,
    ga_purchases: Optional[int] = None,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    vid = str(variant_id).strip()
    if not vid:
        return {}
    entry = variant_map.setdefault(vid, {})
    if quantity is not None and (force or entry.get("quantity_available") in (None, "")):
        entry["quantity_available"] = quantity
    if ga_purchases is not None and "ga_unique_purchases" not in entry:
        entry["ga_unique_purchases"] = ga_purchases
    return entry


# ---------------------------------------------------------------------------
# Data acquisition
# ---------------------------------------------------------------------------
def process_searchspring_item(
    item: Dict[str, Any],
    style_map: Dict[str, Dict[str, Any]],
    variant_map: Dict[str, Dict[str, Any]],
) -> None:
    handle = str(item.get("handle") or derive_handle_from_url(item.get("url")) or "")
    if not handle:
        return

    style_entry = style_map.setdefault(handle, {})
    style_entry.setdefault(
        "style_name",
        derive_style_name("", first_string(item.get("ss_style"))),
    )
    if not style_entry.get("style_name"):
        style_entry["style_name"] = derive_style_name(
            "", first_string(item.get("ss_split_name_last"))
        )

    product_type = derive_product_type(item.get("tags_sub_class"))
    if product_type:
        style_entry["product_type"] = product_type

    details_text = first_string(item.get("mfield_product_details"))
    rise, inseam, leg = parse_measurements(details_text)
    if rise:
        style_entry["rise"] = rise
    if inseam:
        style_entry["inseam"] = inseam
    if leg:
        style_entry["leg_opening"] = leg

    jean_style = parse_jean_style(item.get("ss_fit"))
    if jean_style:
        style_entry["jean_style"] = jean_style

    hem_style_values = list(ensure_iterable(item.get("ss_hem")))
    if hem_style_values:
        style_entry["hem_style"] = hem_style_values[0]

    inseam_label = parse_inseam_label(item.get("ss_fit"), item.get("ss_inseam"))
    if inseam_label:
        style_entry["inseam_label"] = inseam_label

    rise_label = parse_rise_label(item.get("ss_rise"))
    if rise_label:
        style_entry["rise_label"] = rise_label

    quantity_sum = coerce_int(item.get("ss_variant_inventory_sum"))
    if quantity_sum is not None:
        style_entry["quantity_of_style"] = quantity_sum

    ga_purchases = coerce_int(item.get("ga_unique_purchases"))

    bundle_entries = extract_inventory_from_bundle_variants(item.get("ss_bundle_variants"))
    for vid, qty in bundle_entries.items():
        update_variant_record(
            variant_map,
            vid,
            quantity=qty,
            ga_purchases=ga_purchases,
            force=True,
        )

    variant_entries = extract_inventory_from_variants_list(item.get("variants"))
    for vid, qty in variant_entries.items():
        update_variant_record(
            variant_map,
            vid,
            quantity=qty,
            ga_purchases=ga_purchases,
        )

    variant_ids = item.get("variant_id") or item.get("variantId")

    if isinstance(variant_ids, Sequence) and not isinstance(variant_ids, (str, bytes)):
        iterable_variant_ids = variant_ids
    else:
        iterable_variant_ids = split_variant_ids(variant_ids)

    for vid in iterable_variant_ids:
        if not vid:
            continue
        update_variant_record(
            variant_map,
            vid,
            ga_purchases=ga_purchases,
        )

    if ga_purchases is not None and "ga_unique_purchases" not in style_entry:
        style_entry["ga_unique_purchases"] = ga_purchases


def fetch_collection_products() -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    seen_handles: Set[str] = set()

    for url, paginated in COLLECTION_ENDPOINTS:
        if paginated:
            page = 1
            while True:
                params = {"limit": 250, "page": page}
                data = request_with_retry(url, params=params)
                page_products = data.get("products") or []
                log(f"[collection] {url} page {page} -> {len(page_products)} products")
                if not page_products:
                    break
                for product in page_products:
                    handle = product.get("handle")
                    if handle and handle not in seen_handles:
                        products.append(product)
                        seen_handles.add(handle)
                if len(page_products) < 250:
                    break
                page += 1
                time.sleep(0.2)
        else:
            data = request_with_retry(url, expect_json=True)
            page_products = data.get("products") or []
            log(f"[collection] {url} -> {len(page_products)} products")
            for product in page_products:
                handle = product.get("handle")
                if handle and handle not in seen_handles:
                    products.append(product)
                    seen_handles.add(handle)

    log(f"[collection] total products: {len(products)}")
    return products


def fetch_product_detail(handle: str) -> Dict[str, Any]:
    data = request_with_retry(f"{BASE_URL}/products/{handle}.json")
    product = data.get("product") or {}
    return product


def fetch_searchspring_data() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    style_map: Dict[str, Dict[str, Any]] = {}
    variant_map: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        params = [
            ("siteId", "00svms"),
            ("resultsFormat", "json"),
            ("resultsPerPage", 400),
            ("q", "$jeans"),
            ("q", "$womens"),
            ("page", page),
        ]
        data = request_with_retry(SEARCHSPRING_URL, params=params)
        results = data.get("results")
        if isinstance(results, dict):
            results = results.get("results") or results.get("items")
        if not isinstance(results, list):
            results = []
        log(f"[searchspring] page {page} -> {len(results)} results")
        if not results:
            break

        for item in results:
            process_searchspring_item(item, style_map, variant_map)

        pagination = data.get("pagination") or data.get("result", {}).get("pagination") or {}
        total_pages = (
            coerce_int(pagination.get("totalPages"))
            or coerce_int(pagination.get("total_pages"))
            or coerce_int(pagination.get("total_pages_count"))
        )
        if total_pages and page >= total_pages:
            break
        page += 1
        time.sleep(0.3)
    return style_map, variant_map


def audit_local_searchspring(target_skus: Iterable[Any], doc_paths: Optional[Sequence[str]] = None) -> None:
    doc_paths = doc_paths or LOCAL_SEARCHSPRING_DOCS
    sku_order: List[str] = []
    sku_set: set[str] = set()
    for raw in target_skus:
        sku = stringify_identifier(raw)
        if not sku:
            continue
        if sku not in sku_set:
            sku_order.append(sku)
            sku_set.add(sku)

    if not sku_order:
        log("[audit] no valid SKUs provided")
        return

    items: List[Dict[str, Any]] = []
    for path in doc_paths:
        if not path:
            continue
        if not os.path.exists(path):
            log(f"[audit] skipping missing file {path}")
            continue
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception as exc:
            log(f"[audit] failed to load {path}: {exc!r}")
            continue

        results = data.get("results")
        if isinstance(results, dict):
            results = results.get("results") or results.get("items")
        if not isinstance(results, list):
            log(f"[audit] file {path} does not contain a results list")
            continue
        log(f"[audit] loaded {len(results)} items from {path}")
        items.extend(results)

    if not items:
        log("[audit] no Searchspring items loaded; cannot audit quantities")
        return

    style_map: Dict[str, Dict[str, Any]] = {}
    variant_map: Dict[str, Dict[str, Any]] = {}
    for item in items:
        process_searchspring_item(item, style_map, variant_map)

    for sku in sku_order:
        record = variant_map.get(sku)
        qty = record.get("quantity_available") if record else None
        ga = record.get("ga_unique_purchases") if record else None
        log(f"[audit] SKU {sku}: quantity={qty} ga_purchases={ga}")

# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------
def get_variant_image_url(variant: Dict[str, Any], product_images: List[Dict[str, Any]]) -> str:
    featured = variant.get("featured_image")
    if isinstance(featured, dict) and featured.get("src"):
        return featured["src"]
    image_id = variant.get("image_id")
    if image_id and isinstance(product_images, list):
        for image in product_images:
            if str(image.get("id")) == str(image_id) and image.get("src"):
                return image["src"]
    if product_images:
        primary = product_images[0]
        if primary.get("src"):
            return primary["src"]
    return ""


def assemble_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    products = fetch_collection_products()
    style_map, variant_map = fetch_searchspring_data()

    for index, product in enumerate(products, start=1):
        handle = product.get("handle", "")
        log(f"[detail] {index}/{len(products)} handle={handle}")
        try:
            detail = fetch_product_detail(handle)
        except Exception as exc:
            log(f"[error] failed to fetch detail for {handle}: {exc!r}")
            continue

        detail_variant_map = {
            str(variant.get("id")): variant for variant in detail.get("variants", [])
        }
        images = detail.get("images") or []

        style_info = style_map.get(handle, {})
        style_total = style_info.get("quantity_of_style")

        tags = normalize_tags(product.get("tags"))
        description = clean_html_text(product.get("body_html"))
        published_at = parse_published_at(product.get("published_at"))
        product_title = product.get("title", "")

        if style_total in (None, ""):
            aggregate_total = 0
            found_qty = False
            for variant in product.get("variants", []):
                vid = str(variant.get("id"))
                qty = variant_map.get(vid, {}).get("quantity_available")
                if qty is not None:
                    aggregate_total += qty
                    found_qty = True
            if found_qty:
                style_total = aggregate_total
            else:
                style_total = ""

        for variant in product.get("variants", []):
            variant_id_value = variant.get("id")
            vid = str(variant_id_value)
            detail_variant = detail_variant_map.get(vid, {})
            search_variant = variant_map.get(vid, {})

            size = variant.get("option2")
            variant_title = (
                f"{product_title} - {size}" if size else variant.get("title", "")
            )

            quantity_price_breaks = normalize_quantity_price_breaks(
                detail_variant.get("quantity_price_breaks")
            )

            barcode = stringify_identifier(detail_variant.get("barcode"))
            price = format_price(variant.get("price"))
            compare_at = format_price(variant.get("compare_at_price"))

            ga_value = search_variant.get("ga_unique_purchases")
            if ga_value in (None, ""):
                ga_value = style_info.get("ga_unique_purchases", "")

            quantity_available = search_variant.get("quantity_available")
            if quantity_available in (None, ""):
                fallback_qty = coerce_int(detail_variant.get("inventory_quantity"))
                if fallback_qty is not None:
                    quantity_available = fallback_qty

            row = {
                "Style Id": stringify_identifier(product.get("id")),
                "Handle": handle,
                "Published At": published_at,
                "Product": product_title,
                "Style Name": derive_style_name(product_title, style_info.get("style_name")),
                "Product Type": style_info.get("product_type") or product.get("product_type", ""),
                "Tags": tags,
                "Vendor": product.get("vendor", ""),
                "Description": description,
                "Variant Title": variant_title,
                "Color": variant.get("option1", ""),
                "Size": size or "",
                "Rise": style_info.get("rise", ""),
                "Inseam": style_info.get("inseam", ""),
                "Leg Opening": style_info.get("leg_opening", ""),
                "Price": price,
                "Compare at Price": compare_at,
                "Quantity Price Breaks": quantity_price_breaks,
                "Available for Sale": "TRUE" if variant.get("available") else "FALSE",
                "Quantity Available": "" if quantity_available in (None, "") else quantity_available,
                "Google Analytics Purchases": ga_value,
                "Quantity of style": style_total,
                "SKU - Shopify": stringify_identifier(variant_id_value),
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": barcode,
                "Image URL": get_variant_image_url(detail_variant, images),
                "SKU URL": f"{BASE_URL}/products/{handle}",
                "Jean Style": style_info.get("jean_style", ""),
                "Hem Style": style_info.get("hem_style", ""),
                "Inseam Label": style_info.get("inseam_label", ""),
                "Rise Label": style_info.get("rise_label", ""),
            }

            rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------
def write_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("No rows to write")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"MOTHER_{timestamp}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log(f"[csv] wrote {len(rows)} rows -> {path}")
    return path


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_scraper() -> None:
    start = time.time()
    try:
        rows = assemble_rows()
        if not rows:
            log("[warn] no data rows collected; skipping CSV")
            return
        csv_path = write_csv(rows)
        elapsed = time.time() - start
        log(f"[done] completed in {elapsed:.1f}s -> {csv_path}")
    except Exception as exc:
        log(f"[fatal] {exc!r}")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description="Mother Denim inventory exporter")
    parser.add_argument(
        "--audit-skus",
        nargs="+",
        help="One or more SKU/variant IDs to audit using local Searchspring JSON files.",
    )
    parser.add_argument(
        "--audit-file",
        help="Optional path to a text file containing SKU/variant IDs (one per line).",
    )
    parser.add_argument(
        "--audit-docs",
        nargs="*",
        help="Override the default docs/SearchSpring*.json files used for auditing.",
    )
    args = parser.parse_args()

    if args.audit_skus or args.audit_file:
        sku_inputs: List[str] = []
        if args.audit_skus:
            for token in args.audit_skus:
                parts = [part.strip() for part in token.split(",") if part.strip()]
                if parts:
                    sku_inputs.extend(parts)
        if args.audit_file:
            try:
                with open(args.audit_file, "r", encoding="utf-8") as handle:
                    for line in handle:
                        token = line.strip()
                        if not token:
                            continue
                        if "," in token:
                            sku_inputs.extend([part.strip() for part in token.split(",") if part.strip()])
                        else:
                            sku_inputs.append(token)
            except Exception as exc:
                log(f"[audit] failed to read {args.audit_file}: {exc!r}")

        if not sku_inputs:
            log("[audit] no SKU values supplied after parsing inputs")
        else:
            doc_paths = args.audit_docs if args.audit_docs else None
            audit_local_searchspring(sku_inputs, doc_paths)
        return

    run_scraper()


if __name__ == "__main__":
    main()