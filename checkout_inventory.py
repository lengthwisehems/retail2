import argparse
import csv
import logging
import re
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(exist_ok=True)

BRAND = "CHECKOUT"
LOG_PATH = BASE_DIR / f"{BRAND.lower()}_run.log"

HOST_ROTATION = [
    "checkout.goodamerican.com",
    "good-american.myshopify.com",
    "www.goodamerican.com",
]

STOREFRONT_TOKEN = "45b215a31a1aa259d1e128badf92328a"

SEARCHSPRING_URL = "https://5ojqb3.a.searchspring.io/api/search/autocomplete.json"
SEARCHSPRING_SITE_ID = "5ojqb3"

COLLECTION_HANDLES = ["womens-jeans", "sale"]

EXCLUDED_TITLE_TERMS = {"short", "linen", "suit"}
EXCLUDED_PRODUCT_TYPES = {
    "blazers",
    "bodysuits",
    "dresses",
    "goodies accessories",
    "jackets",
    "jumpsuits",
    "shipping protection",
    "shorts",
    "skirts",
    "sweater",
    "sweats",
    "swim",
    "tank",
    "tops",
    "bermudas",
    "blouses",
    "cardigans",
    "clothing tops",
    "crop tops",
    "denim shorts",
    "hoodies",
    "jogger shorts",
    "one-pieces",
    "pant suits",
    "shirts",
    "suits",
    "sweaters",
    "sweatshirts",
    "tank tops",
    "t-shirts",
    "vests",
}

SIZE_VALUES = {
    "00",
    "0",
    "2",
    "4",
    "6",
    "8",
    "10",
    "12",
    "14",
    "15 PLUS",
    "16 PLUS",
    "18 PLUS",
    "20 PLUS",
    "22 PLUS",
    "24 PLUS",
    "26 PLUS",
    "28 PLUS",
    "30 PLUS",
    "32 PLUS",
    "XS",
    "S",
    "M",
    "L",
    "XL",
    "XXL",
    "1XL",
    "2XL",
    "3XL",
    "4XL",
    "5XL",
    "00-4",
    "6-12",
    "14-18 PLUS",
    "20-26 PLUS",
    "28-32 PLUS",
}

LENGTH_LABELS = {
    "REGULAR": "Regular",
    "STANDARD": "Regular",
    "LONG": "Long",
    "TALL": "Long",
    "SHORT": "Petite",
    "PETITE": "Petite",
}

CSV_HEADERS = [
    "Style Id",
    "Handle",
    "Published At",
    "Created At",
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
    "Inseam",
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
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Product Line",
    "Inseam Label",
    "Rise Label",
    "Hem Style",
    "Inseam Style",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)


def configure_logging() -> None:
    handlers: List[logging.Handler] = []
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    try:
        file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
    except OSError as exc:
        fallback_path = OUTPUT_DIR / f"{BRAND.lower()}_run.log"
        logging.basicConfig(level=logging.INFO)
        logging.warning(
            "Primary log file unavailable (%s). Falling back to %s",
            exc,
            fallback_path,
        )
        try:
            file_handler = logging.FileHandler(fallback_path, encoding="utf-8")
            file_handler.setFormatter(formatter)
            handlers.append(file_handler)
        except OSError:
            handlers.append(logging.StreamHandler())

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    logging.basicConfig(level=logging.INFO, handlers=handlers)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Checkout storefront inventory scraper.")
    parser.add_argument("--max-products", type=int, default=None)
    parser.add_argument("--max-variants", type=int, default=None)
    return parser.parse_args()


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("\u00a0", " ")).strip()


def normalize_output_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value)

    decode_attempts = [("latin1", "utf-8"), ("cp1252", "utf-8")]
    for src, dest in decode_attempts:
        if any(token in text for token in ["Ã", "â", "ï»¿", "Â"]):
            try:
                text = text.encode(src, errors="ignore").decode(dest, errors="ignore")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue

    replacements = {
        "â€™": "'",
        "â€˜": "'",
        "â€œ": '"',
        "â€": '"',
        "ï»¿": "",
        "﻿": "",
        "​": "",
        "é": "e",
        "É": "E",
        "–": " ",
        "—": " ",
        "-": " ",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return clean_text(text)


def normalize_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.replace("-", " ").lower()).strip()


def normalize_output_text_keep_hyphen(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value)
    decode_attempts = [("latin1", "utf-8"), ("cp1252", "utf-8")]
    for src, dest in decode_attempts:
        if any(token in text for token in ["Ã", "â", "ï»¿", "Â"]):
            try:
                text = text.encode(src, errors="ignore").decode(dest, errors="ignore")
            except (UnicodeEncodeError, UnicodeDecodeError):
                continue
    replacements = {
        "â€™": "'",
        "â€˜": "'",
        "â€œ": '"',
        "â€": '"',
        "ï»¿": "",
        "﻿": "",
        "​": "",
        "é": "e",
        "É": "E",
        "–": "-",
        "—": "-",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return clean_text(text)


def title_case_preserve_acronyms(value: str) -> str:
    tokens = normalize_key(value).split()
    formatted = []
    for token in tokens:
        if token.isdigit():
            formatted.append(token)
            continue
        if re.fullmatch(r"\d+s", token):
            formatted.append(token.upper())
            continue
        if token.isupper() and len(token) <= 3:
            formatted.append(token)
            continue
        formatted.append(token.capitalize())
    return " ".join(formatted)


def normalize_size(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value.strip()).upper()


def parse_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    return format_month_day_year(parsed)


def format_month_day_year(value: datetime) -> str:
    try:
        return value.strftime("%-m/%-d/%Y")
    except ValueError:
        return value.strftime("%#m/%#d/%Y")


def extract_gid_suffix(gid: Optional[str]) -> str:
    if not gid:
        return ""
    return gid.split("/")[-1]


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, str]] = None,
    payload: Optional[Dict[str, object]] = None,
    max_retries: int = 3,
) -> requests.Response:
    for attempt in range(max_retries):
        response = session.request(
            method,
            url,
            headers=headers,
            params=params,
            json=payload,
            timeout=30,
        )
        if response.status_code in {429, 500, 502, 503, 504}:
            sleep_for = 2 ** attempt
            logging.warning("Retrying %s %s (status %s)", method, url, response.status_code)
            time.sleep(sleep_for)
            continue
        response.raise_for_status()
        return response
    response.raise_for_status()
    return response


def storefront_post(
    session: requests.Session,
    query: str,
    variables: Dict[str, object],
) -> Dict[str, object]:
    headers = {
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    errors: List[str] = []
    for host in HOST_ROTATION:
        url = f"https://{host}/api/unstable/graphql.json"
        try:
            response = request_with_retry(
                session,
                "POST",
                url,
                headers=headers,
                payload={"query": query, "variables": variables},
            )
        except requests.RequestException as exc:
            errors.append(f"{host}: {exc}")
            continue
        payload = response.json()
        if payload.get("errors"):
            errors.append(f"{host}: {payload['errors']}")
            continue
        return payload["data"]
    raise RuntimeError(f"Storefront request failed: {errors}")


def fetch_collection_products(
    session: requests.Session,
    handle: str,
) -> List[Dict[str, object]]:
    query = """
    query ($handle: String!, $cursor: String) {
      collectionByHandle(handle: $handle) {
        products(first: 250, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            id
            handle
            title
            publishedAt
            createdAt
            productType
            tags
            vendor
            description
            totalInventory
            onlineStoreUrl
            images(first: 1) {
              nodes {
                url
              }
            }
            options {
              name
            }
            variants(first: 250) {
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
        }
      }
    }
    """
    products: List[Dict[str, object]] = []
    cursor: Optional[str] = None
    while True:
        data = storefront_post(session, query, {"handle": handle, "cursor": cursor})
        collection = data.get("collectionByHandle")
        if not collection:
            logging.warning("Collection not found: %s", handle)
            break
        product_connection = collection["products"]
        products.extend(product_connection["nodes"])
        page_info = product_connection["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
    return products


def fetch_searchspring_data(session: requests.Session) -> Dict[str, Dict[str, str]]:
    data_map: Dict[str, Dict[str, str]] = {}
    page = 1
    while True:
        params = {
            "siteId": SEARCHSPRING_SITE_ID,
            "resultsFormat": "json",
            "resultsPerPage": "250",
            "page": str(page),
        }
        response = request_with_retry(session, "GET", SEARCHSPRING_URL, params=params)
        payload = response.json()

        results: Iterable[Dict[str, object]] = []
        if isinstance(payload.get("results"), dict) and payload["results"].get("products"):
            results = payload["results"]["products"]
        elif isinstance(payload.get("results"), list):
            results = payload["results"]
        elif isinstance(payload.get("items"), list):
            results = payload["items"]

        results_list = list(results)
        if not results_list:
            break

        for item in results_list:
            handle = item.get("handle") or item.get("ss_handle") or item.get("product_handle")
            if not handle:
                url = item.get("url") or item.get("ss_url")
                if isinstance(url, str):
                    match = re.search(r"/products/([^/?#]+)", url)
                    if match:
                        handle = match.group(1)
            if not handle:
                continue
            ss_pct = item.get("ss_instock_pct")
            if isinstance(ss_pct, (int, float, str)) and str(ss_pct).isdigit():
                instock_pct = f"{int(ss_pct)}%"
            else:
                instock_pct = ""
            image_url = (
                item.get("imageUrl")
                or item.get("image_url")
                or item.get("image")
                or item.get("ss_image")
            )
            data_map[handle] = {
                "instock_pct": instock_pct,
                "image_url": image_url or "",
            }
        page += 1
    logging.info("Searchspring entries loaded: %s", len(data_map))
    return data_map


def has_excluded_title(title: str) -> bool:
    normalized = normalize_key(title)
    return any(term in normalized for term in EXCLUDED_TITLE_TERMS)


def is_excluded_product_type(product_type: str) -> bool:
    normalized = normalize_key(product_type)
    return normalized in EXCLUDED_PRODUCT_TYPES


def determine_variant_options(
    product_options: List[Dict[str, object]],
    selected_options: List[Dict[str, object]],
) -> Tuple[str, str, str]:
    option_names = [opt["name"] for opt in product_options if opt.get("name")]
    selected_map = {opt["name"]: opt["value"] for opt in selected_options if opt.get("name")}
    values = []
    for name in option_names:
        values.append(selected_map.get(name, ""))
    while len(values) < 3:
        values.append("")
    option1, option2, option3 = values[:3]
    return option1, option2, option3


def parse_size_and_length(option2: str, option3: str) -> Tuple[str, str]:
    size = ""
    length = ""
    opt2_norm = normalize_size(option2)
    opt3_norm = normalize_size(option3)

    if opt2_norm in SIZE_VALUES:
        size = opt2_norm
    if opt3_norm in SIZE_VALUES and not size:
        size = opt3_norm

    for candidate in (opt2_norm, opt3_norm):
        if candidate in LENGTH_LABELS:
            if candidate in {"SHORT", "PETITE"}:
                length = "PETITE"
            elif candidate in {"LONG", "TALL"}:
                length = "LONG"
            else:
                length = "REGULAR"
            break

    return size, length


def infer_length_from_title(product_title: str) -> str:
    title_norm = normalize_key(product_title)
    if re.search(r"\bpetite\b", title_norm):
        return "PETITE"
    if re.search(r"\blong\b", title_norm):
        return "LONG"
    return ""


def adjust_base_title_for_variant(base_title: str, base_counter: Counter[str]) -> str:
    base = clean_text(base_title.replace("-", " "))
    norm = normalize_key(base)

    for token in ["petite", "long"]:
        prefix = f"good {token} "
        if norm.startswith(prefix):
            counterpart = norm.replace(f"good {token} ", "good ", 1)
            if base_counter.get(counterpart, 0) > 0:
                base = re.sub(rf"^\s*good\s+{token}\s+", "GOOD ", base, flags=re.IGNORECASE)
            else:
                return clean_text(base)
            break

    base = re.sub(r"\b(PETITE|LONG)\b", "", base, flags=re.IGNORECASE)
    return clean_text(base)


def build_variant_title(
    product_title: str,
    size: str,
    length: str,
    option1: str,
    base_counter: Counter[str],
) -> str:
    full_title = normalize_output_text(product_title)
    title_parts = [clean_text(part) for part in full_title.split("|")]
    base_title = title_parts[0] if title_parts else full_title
    base_title = adjust_base_title_for_variant(base_title, base_counter)

    color_code = title_parts[1] if len(title_parts) > 1 and title_parts[1] else normalize_output_text(option1)

    parts = [base_title, color_code]
    if size:
        parts.append(size)
    if length:
        parts.append(length)
    else:
        inferred = infer_length_from_title(full_title)
        if inferred:
            parts.append(inferred)
    return " | ".join([part for part in parts if part])


def extract_inseam(description: str, option2: str, option3: str) -> str:
    if not description:
        return ""
    text = normalize_output_text(description)
    lower = text.lower()

    label_to_values: Dict[str, List[str]] = {"Regular": [], "Long": [], "Petite": []}
    label_aliases = {"regular": "Regular", "standard": "Regular", "long": "Long", "tall": "Long", "short": "Petite", "petite": "Petite"}

    labeled_patterns = [
        r"inseam[^\d]{0,30}(regular|standard|long|tall|short|petite)[^\d]{0,10}([0-9]+(?:\.[0-9]+)?)",
        r"(regular|standard|long|tall|short|petite)\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?)",
    ]
    for pat in labeled_patterns:
        for match in re.finditer(pat, lower):
            key = label_aliases.get(match.group(1), "")
            if key:
                label_to_values[key].append(match.group(2))

    desired_label = ""
    for opt in (option2, option3):
        norm = normalize_size(opt)
        if norm in LENGTH_LABELS:
            desired_label = LENGTH_LABELS[norm]
            break

    if desired_label and label_to_values.get(desired_label):
        return label_to_values[desired_label][0]

    inseam_generic = re.search(r"inseam\s*[:\-]?\s*(?:regular|standard|long|tall|short|petite)?\s*([0-9]+(?:\.[0-9]+)?)", lower)
    if inseam_generic:
        return inseam_generic.group(1)

    if "inseam" not in lower and desired_label and label_to_values.get(desired_label):
        return label_to_values[desired_label][0]

    for label in ["Regular", "Long", "Petite"]:
        if label_to_values[label]:
            return label_to_values[label][0]
    return ""


def extract_promo(tags: List[str]) -> str:
    promos = []
    for tag in tags:
        lowered = tag.lower()
        if lowered.startswith("promo:") or lowered.startswith("porem:"):
            date_part = tag.split(":", 1)[-1].strip()
            for match in re.finditer(r"(\d{4})/(\d{1,2})/(\d{1,2})", date_part):
                year, month, day = (int(value) for value in match.groups())
                if not (1 <= month <= 12 and 1 <= day <= 31):
                    continue
                promos.append(f"{month:02d}/{day:02d}/{year}")
    return ", ".join(sorted(set(promos)))


def determine_jean_style(title: str, description: str) -> str:
    title_norm = normalize_key(title)
    desc_norm = normalize_key(description)
    style = ""

    if "flare" in title_norm:
        style = "Flare"
    elif "bootcut" in title_norm or "boot" in title_norm:
        style = "Bootcut"
    elif "skinny" in title_norm:
        style = "Skinny"
    elif any(word in title_norm for word in ["barrel", "bowed", "bow leg", "horseshoe"]):
        style = "Barrel"
    elif any(word in title_norm for word in ["wide", "skate", "palazzo", "ease"]):
        style = "Wide Leg"
    elif any(word in title_norm for word in ["baggy", "work pant"]):
        style = "Baggy"
    elif "boyfriend" in title_norm:
        style = "Boyfriend"
    elif "flare" in desc_norm:
        style = "Flare"
    elif "bootcut" in desc_norm:
        style = "Bootcut"
    elif "skinny" in desc_norm:
        style = "Skinny"
    elif any(word in desc_norm for word in ["barrel", "bowed", "bow leg", "horseshoe"]):
        style = "Barrel"
    elif "slim straight" in title_norm or "slim straight" in desc_norm:
        style = "Straight From Knee"
    elif "straight" in title_norm or "straight" in desc_norm:
        style = "Straight"

    knee_phrases = [
        "an iconic straight fit with vintage inspired",
        "an iconic straight fit with vintage-inspired",
        "body hugging and booty shaping",
        "body hugging, booty shaping",
        "body-hugging, booty shaping",
        "booty-shaping, body-sculpting",
        "booty shaping, body sculpting",
        "straight leg from thigh to ankle",
        "close fit through the hip and thigh",
        "curve defining",
        "fitted through your hips and thighs",
        "fitted throughout the hips",
        "sculpting jeans",
        "form-hugging fit from your waist to your thighs",
        "form-hugging fit through the hips and thighs",
        "form hugging fit from your waist to your thighs",
        "form hugging fit through the hips and thighs",
        "hugs your hips and thighs",
        "straight jeans flatter every curve",
    ]
    if style in {"", "Straight"} and any(phrase in desc_norm for phrase in knee_phrases):
        style = "Straight From Knee"

    thigh_phrases = [
        "loose straight legs",
        "loose through hips and thigh",
        "loose through the hip and thigh",
        "loose through the hips and thigh",
        "dropped crotch and baggy, relaxed legs",
        "straight fit through the hips and thighs",
        "we took everything khloe loves about our denim",
    ]
    if style in {"", "Straight"} and any(phrase in desc_norm for phrase in thigh_phrases):
        style = "Straight From Thigh"
    if style in {"", "Straight"} and "relaxed fit" in desc_norm and "straight" in desc_norm:
        style = "Straight From Thigh"

    if style in {"", "Straight"} and "straight" in title_norm and "relax" in title_norm:
        style = "Straight From Thigh"
    if style in {"", "Straight"} and "wide" in desc_norm:
        style = "Wide Leg"

    if style == "Straight":
        return ""
    return style




def normalize_product_line_key(value: str) -> str:
    phrase = normalize_key(value).strip(" -|,.;:")
    if not phrase:
        return ""
    words = phrase.split()
    if words and words[-1].endswith("s") and len(words[-1]) > 1 and words[-1].isalpha():
        words[-1] = words[-1][:-1]
    return " ".join(words)


def extract_before_good_phrase(title: str) -> Tuple[str, str]:
    normalized = normalize_output_text(title)
    base_title = clean_text(normalized.split("|")[0])
    match = re.search(r"\bgood\b", base_title, flags=re.IGNORECASE)
    if not match:
        return "LIMITED EDITION", ""
    prefix = base_title[: match.start()].strip(" -|,.;:")
    if not prefix:
        return "CORE", ""
    return "RAW", clean_text(prefix)


def build_product_line_context(products: List[Dict[str, object]]) -> Dict[str, str]:
    cluster_surface_counts: Dict[str, Counter[str]] = defaultdict(Counter)
    cluster_total_counts: Dict[str, int] = defaultdict(int)
    cluster_pre_good_counts: Dict[str, int] = defaultdict(int)
    cluster_title_counts: Dict[str, int] = defaultdict(int)
    raw_key_by_pid: Dict[str, str] = {}
    status_by_pid: Dict[str, str] = {}

    normalized_titles = []
    for product in products:
        title = normalize_output_text(product.get("title", ""))
        normalized_titles.append(normalize_key(title))

    for product, title_norm in zip(products, normalized_titles):
        pid = str(product.get("id", ""))
        title = normalize_output_text(product.get("title", ""))
        status, raw = extract_before_good_phrase(title)
        status_by_pid[pid] = status
        if status == "RAW" and raw:
            key = normalize_product_line_key(raw)
            if key:
                raw_key_by_pid[pid] = key
                cluster_surface_counts[key][title_case_preserve_acronyms(raw)] += 1
                cluster_total_counts[key] += 1

    for key in cluster_surface_counts:
        if not key:
            continue
        pattern = r"\b" + re.escape(key) + r"s?\b"
        pre_good_pattern = r"\b" + re.escape(key) + r"s?\s+good\b"
        for title_norm in normalized_titles:
            if re.search(pattern, title_norm, flags=re.IGNORECASE):
                cluster_title_counts[key] += 1
            if re.search(pre_good_pattern, title_norm, flags=re.IGNORECASE):
                cluster_pre_good_counts[key] += 1

    canonical_by_key: Dict[str, str] = {}
    for key, counter in cluster_surface_counts.items():
        best_count = max(counter.values())
        winners = [surface for surface, count in counter.items() if count == best_count]
        winners.sort(key=lambda s: (0 if s == s.title() else 1, -len(s), s))
        canonical_by_key[key] = winners[0]

    result: Dict[str, str] = {}
    for product in products:
        pid = str(product.get("id", ""))
        title = normalize_output_text(product.get("title", ""))
        status = status_by_pid.get(pid, "LIMITED EDITION")
        if status == "RAW":
            key = raw_key_by_pid.get(pid, "")
            result[pid] = canonical_by_key.get(key, "Core")
            continue
        if status == "CORE":
            result[pid] = "Core"
        else:
            result[pid] = "Limited Edition"

        title_norm = normalize_key(title)
        if "good" not in title_norm:
            continue

        best_key = ""
        best_len = -1
        best_freq = -1
        for key, canonical in canonical_by_key.items():
            if not key:
                continue
            total = cluster_total_counts.get(key, 0)
            pre_good = cluster_pre_good_counts.get(key, 0)
            if total == 0 or pre_good / total < 0.8:
                continue
            pattern = r"\b" + re.escape(key) + r"s?\b"
            if re.search(pattern, title_norm, flags=re.IGNORECASE):
                phrase_len = len(key)
                freq = cluster_total_counts.get(key, 0)
                if phrase_len > best_len or (phrase_len == best_len and freq > best_freq):
                    best_key = key
                    best_len = phrase_len
                    best_freq = freq
        if best_key:
            result[pid] = canonical_by_key[best_key]
    return result


def build_base_title_counter(products: List[Dict[str, object]]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for product in products:
        title = normalize_output_text(product.get("title", ""))
        base = clean_text(title.split("|")[0])
        counter[normalize_key(base)] += 1
    return counter


def build_grouping_key_set(
    products: List[Dict[str, object]],
    product_line_map: Dict[str, str],
) -> set:
    keys = set()
    for product in products:
        title = normalize_output_text_keep_hyphen(product.get("title", ""))
        style_name = clean_text(title.split("|")[0])
        product_line = product_line_map.get(str(product.get("id", "")), "")
        grouping = normalize_key(style_name)
        pl_norm = normalize_product_line_key(product_line)
        if pl_norm:
            pattern = r"\b" + re.escape(pl_norm) + r"s?\b"
            grouping = re.sub(pattern, "", grouping, flags=re.IGNORECASE)

        rise_patterns = [
            r"ultra high rise",
            r"super high rise",
            r"high rise",
            r"mid rise",
            r"med rise",
            r"medium rise",
            r"low rise",
        ]
        for pat in rise_patterns:
            grouping = re.sub(r"\b" + pat + r"\b", "", grouping)

        grouping = re.sub(r"\bwide leg\b", "wide", grouping)
        if re.search(r"\bskate\b", grouping) and re.search(r"\bwide\b", grouping):
            has_good = bool(re.search(r"\bgood\b", grouping))
            grouping = re.sub(r"\bgood\b", "", grouping)
            grouping = re.sub(r"\bskate\b", "", grouping)
            grouping = re.sub(r"\bwide\b", "", grouping)
            grouping = clean_text(grouping)
            prefix = "good skate wide" if has_good else "skate wide"
            grouping = f"{prefix} {grouping}".strip()

        grouping = re.sub(r"\bcropped\b", "crop", grouping)
        grouping = re.sub(r"\bmini[- ]boot\b", "mini boot", grouping)
        grouping = re.sub(r"\b(petite|long)\b", "", grouping)
        grouping = re.sub(r"\b(jean|jeans|pants)\b", "", grouping)
        grouping = clean_text(grouping)
        keys.add(normalize_key(grouping))
    return keys


def build_style_grouping(
    style_name: str,
    product_line: str,
    base_title_set: set,
    grouping_key_set: set,
) -> str:
    grouping = normalize_key(style_name)
    pl_norm = normalize_product_line_key(product_line)
    if pl_norm:
        pattern = r"\b" + re.escape(pl_norm) + r"s?\b"
        grouping = re.sub(pattern, "", grouping, flags=re.IGNORECASE)

    rise_patterns = [
        r"ultra high rise",
        r"super high rise",
        r"high rise",
        r"mid rise",
        r"med rise",
        r"medium rise",
        r"low rise",
    ]
    for pat in rise_patterns:
        grouping = re.sub(r"\b" + pat + r"\b", "", grouping)

    grouping = re.sub(r"\bcropped\b", "crop", grouping)
    grouping = re.sub(r"\bankle\b", "ankle", grouping)
    grouping = re.sub(r"\bmini[- ]boot\b", "mini boot", grouping)
    grouping = re.sub(r"\bwide leg\b", "wide", grouping)

    tokens = grouping.split()
    if "skate" in tokens and "wide" in tokens:
        has_good = "good" in tokens
        tokens = [t for t in tokens if t not in {"good", "skate", "wide"}]
        prefix = ["good", "skate", "wide"] if has_good else ["skate", "wide"]
        tokens = prefix + tokens
        grouping = " ".join(tokens)

    style_norm = normalize_key(style_name)
    for token in ["petite", "long"]:
        if f"good {token}" in style_norm:
            counterpart = re.sub(rf"\b{token}\b", "", grouping)
            counterpart = re.sub(r"\b(jean|jeans|pants)\b", "", counterpart)
            counterpart = re.sub(r"\bcropped\b", "crop", counterpart)
            counterpart = clean_text(counterpart)
            if normalize_key(counterpart) in grouping_key_set:
                grouping = re.sub(rf"\b{token}\b", "", grouping)
        else:
            grouping = re.sub(rf"\b{token}\b", "", grouping)

    grouping = clean_text(grouping)

    garment = ""
    for suffix in ["jean", "jeans", "pants", "leggings", "trousers", "sweatpants"]:
        if grouping.endswith(f" {suffix}") or grouping == suffix:
            garment = suffix
            grouping = grouping[: -len(suffix)].strip()
            break

    tokens = grouping.split()
    crop_tokens = [t for t in tokens if t in {"crop", "ankle"}]
    tokens = [t for t in tokens if t not in {"crop", "ankle"}]

    pull_on_present = False
    if "pull on" in grouping:
        pull_on_present = True
        tokens = [t for t in tokens if t not in {"pull", "on"}]

    if pull_on_present:
        tokens.extend(crop_tokens)
        tokens.append("pull on")
    else:
        tokens.extend(crop_tokens)

    if garment:
        if garment in {"jean", "jeans", "pants"}:
            garment = ""
        else:
            tokens.append(garment)

    grouping = clean_text(grouping)
    if tokens:
        grouping = clean_text(" ".join(tokens))
    return title_case_preserve_acronyms(grouping)


def determine_product_line(title: str) -> str:
    base_title = clean_text(normalize_output_text(title).split("|")[0])
    upper_title = base_title.upper()
    if not re.search(r"\bgood\b", upper_title, flags=re.IGNORECASE):
        return "Limited Edition"
    prefix = re.split(r"\bgood\b", upper_title, maxsplit=1, flags=re.IGNORECASE)[0].strip(" -|,.;:")
    if not prefix:
        return "Core"
    return clean_text(prefix.title())


def determine_inseam_label(option2: str, option3: str, title: str, inseam: str) -> str:
    for opt in (option2, option3):
        normalized = normalize_size(opt)
        if normalized in LENGTH_LABELS:
            return LENGTH_LABELS[normalized]

    title_norm = normalize_key(title)
    has_petite = "petite" in title_norm
    has_long = "long" in title_norm
    has_regular = "regular" in title_norm or "standard" in title_norm

    conflict = (has_petite and has_long) or (has_long and has_regular)
    if not conflict:
        if has_petite:
            return "Petite"
        if has_long:
            return "Long"

    if conflict and inseam:
        try:
            if float(inseam) > 32:
                return "Long"
        except ValueError:
            pass
    return ""


def determine_rise_label(description: str) -> str:
    desc_norm = normalize_key(description)
    if "ultra high rise" in desc_norm or "ultra high-rise" in desc_norm:
        return "Ultra High"
    if "super high rise" in desc_norm or "super high-rise" in desc_norm:
        return "Ultra High"
    if "high rise" in desc_norm or "high-rise" in desc_norm:
        return "High"
    if "mid rise" in desc_norm or "mid-rise" in desc_norm:
        return "Mid"
    if "low rise" in desc_norm or "low-rise" in desc_norm:
        return "Low"
    return ""


def determine_hem_style(description: str) -> str:
    desc_norm = normalize_key(description)
    if any(phrase in desc_norm for phrase in ["slits at hem", "twisted outseam slit detail"]):
        return "Split Hem"
    if any(
        phrase in desc_norm
        for phrase in [
            "clean hem",
            "clean details and hem",
            "clean hem with grinding",
            "grinded hem",
            "clean, cuffed",
        ]
    ):
        return "Clean Hem"
    if any(
        phrase in desc_norm
        for phrase in [
            "raw hem",
            "raw, distressed step hem",
            "frayed hem",
            "released, raw hem",
            "released hem",
        ]
    ):
        return "Raw Hem"
    if any(phrase in desc_norm for phrase in ["wide hem", "tuxedo hem", "trouser hem"]):
        return "Wide Hem"
    if any(
        phrase in desc_norm
        for phrase in [
            "distressed hem",
            "chewed hem",
            "light distressed hem",
            "distressed knees and hem",
            "distressed detailing and hem",
        ]
    ):
        return "Distressed Hem"
    return ""


def determine_inseam_style(title: str, handle: str, description: str, inseam: str) -> str:
    title_norm = normalize_key(title)
    handle_norm = normalize_key(handle)
    desc_norm = normalize_key(description)

    if "crop" in title_norm or "kick" in title_norm or "crop" in handle_norm or "kick" in handle_norm:
        return "Crop"
    if "ankle" in title_norm or "ankle" in handle_norm:
        return "Ankle"

    if any(
        phrase in desc_norm
        for phrase in [
            "full length",
            "stack at the ankle",
            "floor-length",
            "full-length",
            "floor-sweeping",
            "floor-skimming",
            "floor sweeping",
            "floor-grazing",
            "hit just below the ankle",
        ]
    ):
        return "Full Length"
    if any(
        phrase in desc_norm
        for phrase in [
            "ankle-length",
            "hits just right at the ankle",
            "hit at the ankle",
            "taper at the ankle",
            "tapers at the ankle",
            "tapers slightly at the ankle",
        ]
    ):
        return "Ankle"

    try:
        if inseam and float(inseam) >= 32:
            return "Full Length"
    except ValueError:
        pass
    return ""


def determine_color_standardized(tags: List[str], description: str) -> str:
    tags_norm = [normalize_key(tag) for tag in tags]
    if any("animal" in tag for tag in tags_norm):
        return "Animal Print"
    if any("print" in tag for tag in tags_norm):
        return "Print"
    if any("pink" in tag for tag in tags_norm):
        return "Pink"
    if any("blue" in tag for tag in tags_norm):
        return "Blue"
    if any("black" in tag for tag in tags_norm):
        return "Black"
    if any("brown" in tag for tag in tags_norm):
        return "Brown"
    if any("grey" in tag for tag in tags_norm):
        return "Grey"
    if any("white" in tag for tag in tags_norm):
        return "White"

    desc_norm = normalize_key(description)
    if "blue wash" in desc_norm:
        return "Blue"
    if "black wash" in desc_norm:
        return "Black"
    if "white wash" in desc_norm:
        return "White"
    return ""


def determine_color_simplified(
    tags: List[str],
    description: str,
    color_standardized: str,
) -> str:
    tags_norm = [normalize_key(tag) for tag in tags]
    desc_norm = normalize_key(description)
    color_norm = normalize_key(color_standardized)

    if color_norm in {"white", "tan"} or any(
        term in tags_norm for term in ["washlightblue", "lightwash"]
    ):
        return "Light"
    if color_norm == "black" or any("washdarkblue" in tag for tag in tags_norm):
        return "Dark"
    if any("washmediumlightblue" in tag for tag in tags_norm):
        return "Light to Medium"
    if any("washmediumdarkblue" in tag for tag in tags_norm):
        return "Medium to Dark"
    if any("washmediumblue" in tag for tag in tags_norm):
        return "Medium"
    if "light blue wash" in desc_norm or "white wash" in desc_norm:
        return "Light"
    if "black wash" in desc_norm or "dark blue wash" in desc_norm:
        return "Dark"
    if "medium light blue wash" in desc_norm:
        return "Light to Medium"
    if "medium dark blue wash" in desc_norm:
        return "Medium to Dark"
    if "medium blue wash" in desc_norm:
        return "Medium"
    return ""


def determine_stretch(description: str) -> str:
    desc_norm = normalize_key(description)
    desc_norm = desc_norm.replace("-", " ")
    if any(term in desc_norm for term in ["ridgid", "rigid", "non stretch", "no stretch"]):
        return "Rigid"
    if any(term in desc_norm for term in ["super stretch", "compression", "softtech"]):
        return "Medium Stretch"
    if "comfort stretch" in desc_norm:
        return "Low to Medium Stretch"
    if "comfort denim" in desc_norm:
        return "Low Stretch"
    if "always fit" in desc_norm:
        return "High Stretch"
    if any(term in desc_norm for term in ["power stretch", "jeanius", "pull on", "pull-on"]):
        return "Medium to High Stretch"
    if "stretch" in desc_norm:
        return "Stretch"
    return ""


def build_rows(
    products: List[Dict[str, object]],
    searchspring_map: Dict[str, Dict[str, str]],
    max_products: Optional[int],
    max_variants: Optional[int],
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen_products = 0

    product_line_map = build_product_line_context(products)
    base_counter = build_base_title_counter(products)
    base_title_set = set(base_counter.keys())
    grouping_key_set = build_grouping_key_set(products, product_line_map)

    staged_rows: List[Dict[str, str]] = []

    for product in products:
        if max_products is not None and seen_products >= max_products:
            break
        title = normalize_output_text(product.get("title", ""))
        if not title or has_excluded_title(title):
            continue
        product_type = normalize_output_text(product.get("productType") or "")
        if product_type and is_excluded_product_type(product_type):
            continue

        handle = product.get("handle", "")
        style_id = extract_gid_suffix(product.get("id"))
        published_at = parse_date(product.get("publishedAt"))
        created_at = parse_date(product.get("createdAt"))
        description = normalize_output_text(product.get("description") or "")
        tags = product.get("tags") or []
        tags = [normalize_output_text(tag) for tag in tags] if isinstance(tags, list) else []
        tags_str = ", ".join(tags) if isinstance(tags, list) else normalize_output_text(str(tags))
        vendor = normalize_output_text(product.get("vendor") or "")
        online_store_url = product.get("onlineStoreUrl") or ""
        if not online_store_url:
            online_store_url = f"https://www.goodamerican.com/products/{handle}"

        style_name = clean_text(normalize_output_text_keep_hyphen(product.get("title", "")).split("|")[0])

        product_line = product_line_map.get(str(product.get("id", "")), determine_product_line(title))
        jean_style = determine_jean_style(title, description)
        rise_label = determine_rise_label(description)
        hem_style = determine_hem_style(description)

        style_grouping = build_style_grouping(
            style_name,
            product_line,
            base_title_set,
            grouping_key_set,
        )

        images = product.get("images", {}).get("nodes", []) if isinstance(product.get("images"), dict) else []
        product_image = images[0].get("url") if images else ""

        searchspring = searchspring_map.get(handle, {})
        instock_pct = searchspring.get("instock_pct", "")
        image_url = searchspring.get("image_url", "") or product_image

        variants = product.get("variants", {}).get("nodes", [])
        if not variants:
            continue
        if max_variants is not None:
            variants = variants[:max_variants]

        product_options = product.get("options") or []

        total_inventory = product.get("totalInventory")
        if total_inventory is None:
            total_inventory = sum(
                variant.get("quantityAvailable") or 0 for variant in variants
            )

        for variant in variants:
            option1, option2, option3 = determine_variant_options(
                product_options,
                variant.get("selectedOptions") or [],
            )
            size_value, length_value = parse_size_and_length(option2, option3)
            variant_title = build_variant_title(title, size_value, length_value, option1, base_counter)

            inseam_value = extract_inseam(description, option2, option3)
            inseam_label = determine_inseam_label(option2, option3, title, inseam_value)
            inseam_style = determine_inseam_style(title, handle, description, inseam_value)

            color_standardized = determine_color_standardized(tags, description)
            color_simplified = determine_color_simplified(tags, description, color_standardized)
            stretch = determine_stretch(description)

            row = {
                "Style Id": style_id,
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": title,
                "Style Name": style_name,
                "Style Name - Grouping": style_grouping,
                "Product Type": product_type,
                "Tags": tags_str,
                "Vendor": vendor,
                "Description": description,
                "Variant Title": variant_title,
                "Color": normalize_output_text(option1),
                "Size": size_value,
                "Inseam": inseam_value,
                "Price": (variant.get("price") or {}).get("amount", ""),
                "Compare at Price": (variant.get("compareAtPrice") or {}).get("amount", ""),
                "Promo": extract_promo(tags),
                "Available for Sale": str(variant.get("availableForSale", "")),
                "Quantity Available": str(variant.get("quantityAvailable", "")),
                "Quantity of style": str(total_inventory),
                "Instock Percent": instock_pct,
                "SKU - Shopify": extract_gid_suffix(variant.get("id")),
                "SKU - Brand": variant.get("sku", ""),
                "Barcode": variant.get("barcode", ""),
                "Image URL": image_url,
                "SKU URL": online_store_url,
                "Jean Style": jean_style,
                "Product Line": product_line,
                "Inseam Label": inseam_label,
                "Rise Label": rise_label,
                "Hem Style": hem_style,
                "Inseam Style": inseam_style,
                "Color - Simplified": color_simplified,
                "Color - Standardized": color_standardized,
                "Stretch": stretch,
            }
            staged_rows.append(row)

        seen_products += 1

    rows.extend(staged_rows)

    return rows


def write_csv(rows: List[Dict[str, str]]) -> Path:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = OUTPUT_DIR / f"{BRAND}_{timestamp}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    logging.info("CSV written: %s", output_path.resolve())
    return output_path


def main() -> None:
    configure_logging()
    args = parse_args()
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    logging.info("Fetching Searchspring data")
    searchspring_map = fetch_searchspring_data(session)

    product_map: Dict[str, Dict[str, object]] = {}
    for handle in COLLECTION_HANDLES:
        logging.info("Fetching collection: %s", handle)
        products = fetch_collection_products(session, handle)
        for product in products:
            product_map[product["id"]] = product
        logging.info("Products in %s: %s", handle, len(products))

    combined_products = list(product_map.values())
    logging.info("Unique products: %s", len(combined_products))
    rows = build_rows(
        combined_products,
        searchspring_map,
        args.max_products,
        args.max_variants,
    )
    logging.info("Rows prepared: %s", len(rows))
    write_csv(rows)


if __name__ == "__main__":
    main()
