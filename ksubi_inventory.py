#!/usr/bin/env python3
from __future__ import annotations

import csv
import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from bs4 import BeautifulSoup

BRAND = "KSUBI"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / f"{BRAND}_run.log"

HOST_ROTATION = ["https://ksubi.com", "https://ksubi-us.myshopify.com"]
COLLECTION_HANDLES = ["womens-denim", "womens-denim-sale"]
STOREFRONT_TOKEN = "b3627995803bec74613a3169db43ac6c"

SEARCHSPRING_URL = "https://kcplhc.a.searchspring.io/api/search/autocomplete.json"
SEARCHSPRING_SITE_ID = "kcplhc"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    }
)
SESSION.verify = False
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

STYLING_WORDS_TO_REMOVE = [
    "BELTED", "CARGO", "CARPENTER", "CROP", "CUFFED", "CUTOFF", "FLAG", "FLIP", "KRYSTAL", "PANEL",
    "PATCH", "PLAID", "RENAISSANCE", "RINSE", "RIPPED", "SADDLE", "SEQUIN", "SOTT", "SPARK", "SPLICED",
    "STRAIGHT", "STUDDED", "TRASHED", "WAX", "COATED", "FRONTIER", "LO", "LEATHER", "SELVEDGE",
    "KRUSHED", "REPAIR", "GRAFFITIMETALIK", "1999", "SLICE", "PLUS", "ANKLE", "LOW RISE", "TRAK",
]

CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size", "Rise", "Inseam", "Leg Opening", "Price", "Compare at Price",
    "Available for Sale", "Quantity Available", "Quantity of Style", "Instock Percent", "Google Analytics Purchases",
    "Google Analytics AVG Revenue", "Google Analytics Total Revenue", "SKU - Shopify", "SKU - Brand", "Barcode",
    "Image URL", "SKU URL", "Jean Style", "Inseam Label", "Inseam Style", "Rise Label", "Color - Simplified",
    "Color - Standardized", "Stretch", "Gender",
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
        createdAt
        totalInventory
        onlineStoreUrl
        featuredImage { url }
        category { name }
        seo { description }
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
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

PRODUCT_BY_HANDLE_QUERY = """
query ProductByHandle($handle: String!) {
  product(handle: $handle) {
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
    featuredImage { url }
    category { name }
    seo { description }
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
}
"""



def configure_logging() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    handlers = [logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()]
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", handlers=handlers)


def normalize_text(text: str) -> str:
    value = (text or "").lower().replace("-", " ")
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def contains_any(text: str, phrases: Sequence[str]) -> bool:
    n = normalize_text(text)
    return any(normalize_text(p) in n for p in phrases)


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
        return ""


def strip_gid(value: str, prefix: str) -> str:
    return value.replace(prefix, "") if value.startswith(prefix) else value.split("/")[-1]




def clean_html_text(raw_html: str) -> str:
    return BeautifulSoup(raw_html or "", "html.parser").get_text(" ", strip=True)
def parse_fraction_inches(raw: str) -> str:
    text = (raw or "").replace('"', " ").strip()
    if not text:
        return ""
    total = 0.0
    for tok in re.split(r"\s+", text):
        if not tok:
            continue
        if "/" in tok and re.fullmatch(r"\d+/\d+", tok):
            total += float(Fraction(tok))
        else:
            try:
                total += float(tok)
            except ValueError:
                pass
    return (f"{total:.3f}".rstrip("0").rstrip(".")) if total else ""


def request_with_rotation(path: str, *, method: str = "GET", headers: Optional[dict] = None, payload: Optional[dict] = None, params: Optional[Any] = None, timeout: int = 40) -> requests.Response:
    last_exc: Optional[Exception] = None
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/{path.lstrip('/')}"
        try:
            if method == "POST":
                resp = SESSION.post(url, headers=headers, json=payload, params=params, timeout=timeout)
            else:
                resp = SESSION.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code in {404, 410}:
                continue
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logging.warning("Request failed for %s: %s", url, exc)
            time.sleep(0.6)
    raise RuntimeError(f"Request failed for {path}: {last_exc}")




def fetch_collection_json_handles(collection_handle: str) -> List[str]:
    handles: List[str] = []
    page = 1
    while True:
        resp = request_with_rotation(
            f"/collections/{collection_handle}/products.json",
            params={"limit": 250, "page": page},
        )
        payload = resp.json()
        products = payload.get("products") or []
        if not products:
            break
        for product in products:
            handle = str(product.get("handle") or "").strip()
            if handle:
                handles.append(handle)
        logging.info("JSON %s fetched %s products on page %s", collection_handle, len(products), page)
        page += 1
    return handles


def fetch_product_by_handle(handle: str) -> Optional[dict]:
    headers = {
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        "Content-Type": "application/json",
    }
    data = request_with_rotation(
        "/api/unstable/graphql.json",
        method="POST",
        headers=headers,
        payload={"query": PRODUCT_BY_HANDLE_QUERY, "variables": {"handle": handle}},
    ).json()
    if data.get("errors"):
        logging.warning("GraphQL handle lookup error for %s: %s", handle, data["errors"])
        return None
    return ((data.get("data") or {}).get("product") or None)
def fetch_collection_products(handle: str) -> List[dict]:
    products: List[dict] = []
    cursor: Optional[str] = None
    headers = {
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
        "Content-Type": "application/json",
    }
    while True:
        data = request_with_rotation(
            "/api/unstable/graphql.json",
            method="POST",
            headers=headers,
            payload={"query": GRAPHQL_QUERY, "variables": {"handle": handle, "cursor": cursor}},
        ).json()
        if data.get("errors"):
            raise RuntimeError(data["errors"])
        node = (((data.get("data") or {}).get("collection") or {}).get("products") or {})
        batch = node.get("nodes") or []
        products.extend(batch)
        logging.info("GraphQL %s fetched %s products (total %s)", handle, len(batch), len(products))
        info = node.get("pageInfo") or {}
        if not info.get("hasNextPage"):
            break
        cursor = info.get("endCursor")
        if not cursor:
            break
    return products


def parse_measurement(text: str, labels: Sequence[str]) -> str:
    t = text or ""
    for label in labels:
        # cm / inch format first
        m = re.search(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?)\s*cm\s*/\s*([0-9]+(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)\s*\"", t, flags=re.IGNORECASE)
        if m:
            return parse_fraction_inches(m.group(2))
        # inches direct
        m = re.search(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\s+[0-9]+/[0-9]+|\.[0-9]+|/[0-9]+)?)\s*\"", t, flags=re.IGNORECASE)
        if m:
            return parse_fraction_inches(m.group(1))
        # cm only
        m = re.search(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?)\s*cm", t, flags=re.IGNORECASE)
        if m:
            inches = float(m.group(1)) / 2.54
            return f"{inches:.3f}".rstrip("0").rstrip(".")
    return ""


def fetch_pdp(handle: str) -> Dict[str, str]:
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/products/{handle}"
        try:
            resp = SESSION.get(url, timeout=40)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            ul = soup.select_one("div.product-features-content.product-web-des-2 > ul")
            features_text = ul.get_text("\n", strip=True) if ul else soup.get_text(" ", strip=True)
            return {
                "features": features_text,
                "rise": parse_measurement(features_text, ["Rise"]),
                "inseam": parse_measurement(features_text, ["Inseam", "Inleg length", "Inleg"]),
                "leg_opening": parse_measurement(features_text, ["Hem circumference", "Hem", "Leg opening"]),
            }
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed PDP %s (%s)", url, exc)
    return {"features": "", "rise": "", "inseam": "", "leg_opening": ""}


def clean_styling_words(text: str, preserve: Optional[set[str]] = None) -> str:
    out = text
    preserve = preserve or set()
    for word in sorted(STYLING_WORDS_TO_REMOVE, key=len, reverse=True):
        if word in preserve:
            continue
        pat = r"\b" + re.escape(word).replace(r"\ ", r"\s+") + r"\b"
        out = re.sub(pat, " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def derive_color_method1(title: str) -> str:
    m = re.search(r"\bJEAN\b\s+(.+)$", title, flags=re.IGNORECASE)
    if not m:
        return ""
    cand = clean_styling_words(m.group(1))
    cand = re.sub(r"\bXTRA\b", " ", cand, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cand).strip()


def derive_style_name_families(titles: List[str], colors_known: set[str]) -> Dict[str, str]:
    by_family: Dict[str, List[str]] = defaultdict(list)
    firstword_titles: Dict[str, List[str]] = defaultdict(list)
    for t in titles:
        toks = normalize_text(t).split()
        if toks:
            firstword_titles[toks[0]].append(t)

    def firstword_has_jean_anchor(word: str) -> bool:
        for title in firstword_titles.get(word, []):
            if "jean" in normalize_text(title).split():
                return True
        return False

    for t in titles:
        toks = normalize_text(t).split()
        if not toks:
            continue
        fam = toks[0]
        if (
            len(toks) > 1
            and toks[1] != "jean"
            and toks[1] in firstword_titles
            and not (len(toks) > 2 and toks[1] == "low" and toks[2] == "rise")
            and (firstword_has_jean_anchor(toks[1]) or toks[0].upper() in STYLING_WORDS_TO_REMOVE)
        ):
            fam = toks[1]
        by_family[fam].append(t)

    out: Dict[str, str] = {}
    all_colors_sorted = sorted({c for c in colors_known if c}, key=lambda x: len(x), reverse=True)

    for fam, fam_titles in by_family.items():
        style = ""
        jean_anchors = []
        for t in fam_titles:
            toks = normalize_text(t).split()
            if "jean" in toks:
                jean_anchors.append((toks.index("jean"), t))
        if jean_anchors:
            _, anchor = sorted(jean_anchors, key=lambda x: x[0])[0]
            raw_words = anchor.split()
            nwords = [normalize_text(w) for w in raw_words]
            jidx = nwords.index("jean")
            pre = [w for w in raw_words[:jidx] if normalize_text(w).upper() not in STYLING_WORDS_TO_REMOVE]
            style = (" ".join(pre + [raw_words[jidx]])).strip()
        if not style:
            xtra_anchors = []
            for t in fam_titles:
                toks = normalize_text(t).split()
                if "xtra" in toks:
                    xtra_anchors.append((toks.index("xtra"), t))
            if xtra_anchors:
                _, anchor = sorted(xtra_anchors, key=lambda x: x[0])[0]
                aw = [w for w in anchor.split() if normalize_text(w).upper() not in STYLING_WORDS_TO_REMOVE]
                out_words = []
                for w in aw:
                    if normalize_text(w) == "xtra":
                        break
                    out_words.append(w)
                style = " ".join(out_words).strip()
        if not style:
            # part 3 by colors
            for t in fam_titles:
                work = t
                match_color = ""
                for c in all_colors_sorted:
                    if re.search(r"\b" + re.escape(c) + r"\b", work, flags=re.IGNORECASE):
                        match_color = c
                        break
                if match_color:
                    work = re.sub(r"\b" + re.escape(match_color) + r"\b", " ", work, flags=re.IGNORECASE)
                    work = clean_styling_words(work, preserve={"STRAIGHT"})
                    style = work.strip()
                    break
        if not style:
            style = fam_titles[0].split()[0]

        # targeted family normalizations from business rules/examples
        if normalize_text(style) == "soho":
            style = "SOHO JEAN"

        for t in fam_titles:
            final_style = re.sub(r"\s+", " ", style).strip().upper()
            if normalize_text(t).startswith("straight up "):
                final_style = "STRAIGHT UP"
            out[t] = final_style
    return out


def derive_color_method2(title: str, style_name: str) -> str:
    if not style_name:
        return ""
    cand = ""
    style_core = re.sub(r"\bJEAN\b", " ", style_name, flags=re.IGNORECASE)
    style_core = re.sub(r"\s+", " ", style_core).strip()
    # Prefer the longest style anchor first.
    for anchor in [style_name, style_core]:
        if not anchor:
            continue
        m = re.search(re.escape(anchor) + r"\s+(.+)$", title, flags=re.IGNORECASE)
        if m:
            cand = m.group(1)
            break
    cand = clean_styling_words(cand)
    cand = re.sub(r"\bXTRA\b", " ", cand, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", cand).strip()


def get_option(variant: dict, names: Sequence[str]) -> str:
    for opt in variant.get("selectedOptions") or []:
        if (opt.get("name") or "").strip().lower() in names:
            return str(opt.get("value") or "")
    return ""


def determine_product_type(category: str, seo_desc: str, tags: str) -> str:
    c = normalize_text(category)
    if c and c != "uncategorized":
        return category
    s = normalize_text(seo_desc)
    if "jeans" in s:
        return "Jeans"
    if "denim" in s and ("cargo" in s or "pants" in s):
        return "Jeans"
    t = normalize_text(tags)
    if "denim jeans" in t or "jean" in t or "womens" in t:
        return "Jeans"
    return category or ""


def determine_jean_style(title: str, desc: str, features: str) -> str:
    # Priority rules for title+description straight combinations.
    if contains_any(title, ["straight"]) and contains_any(desc, ["slim straight", "slim cigarette fit"]):
        return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["relaxed straight leg", "relaxed straight"]):
        return "Straight from Thigh"

    for source, rules in [
        (title, [(["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"], "Barrel"),
                 (["playback", "tapered"], "Tapered"), (["baggy"], "Baggy"), (["flare"], "Flare"),
                 (["bootcut"], "Bootcut"), (["skinny"], "Skinny")]),
        (desc, [(["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"], "Barrel"),
                (["skinny"], "Skinny"), (["flare"], "Flare"), (["bootcut"], "Bootcut"),
                (["taper", "tapering", "tapered"], "Tapered"), (["relaxed fit at the thigh"], "Straight from Knee/Thigh"),
                (["baggiest fit", "baggiest jean"], "Baggy"), (["wide leg", "palazzo"], "Wide Leg"),
                (["relaxed straight leg", "relaxed straight"], "Straight from Thigh"), (["slim straight", "slim cigarette fit"], "Straight from Knee")]),
        (features, [(["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"], "Barrel"),
                    (["skinny"], "Skinny"), (["flare"], "Flare"), (["bootcut"], "Bootcut"),
                    (["taper", "tapering", "tapered"], "Tapered"), (["wide leg", "palazzo"], "Wide Leg"),
                    (["relaxed straight leg", "relaxed straight"], "Straight from Thigh"),
                    (["slim straight", "slim cigarette fit"], "Straight from Knee"), (["baggy"], "Baggy")]),
    ]:
        for keys, label in rules:
            if contains_any(source, keys):
                return label
    return ""


def determine_inseam_label(desc: str, features: str) -> str:
    long_keys = ["longer length", "extended length", "long leg length", "extra length", "long inseam", "longer inseam", "extended inseam", "long leg inseam", "extra inseam", "long leg inseam"]
    petite_keys = ["petite length", "short length", "petite inseam", "short inseam"]
    reg_keys = ["regular length", "regular inseam"]
    for text in (desc, features):
        if contains_any(text, long_keys):
            return "Long"
        if contains_any(text, petite_keys):
            return "Petite"
        if contains_any(text, reg_keys):
            return "Regular"
    return ""


def determine_inseam_style(desc: str, features: str, jean_style: str) -> str:
    for text in (desc, features):
        if contains_any(text, ["ankle grazing", "cropped at the ankle", "ankle length", "cropped ankle"]):
            return "Full Length" if jean_style in {"Skinny", "Tapered"} else "Ankle"
        if contains_any(text, ["stack stylishly above the ankle", "stack at the ankle", "full length", "xtra", "stack at the hem", "tapers at the ankle"]):
            return "Full Length"
        if contains_any(text, ["crop length", "cropped ankle"]):
            return "Cropped"
    return ""


def determine_rise_label(desc: str, features: str) -> str:
    ultra_low = ["super low rise", "ultra low rise", "super low waist", "ultra low waist"]
    ultra_high = ["super high rise", "ultra high rise", "super high waist", "ultra high waist"]
    for text in (desc, features):
        if contains_any(text, ultra_low):
            return "Ultra Low"
        if contains_any(text, ultra_high):
            return "Ultra High"
        if contains_any(text, ["mid rise"]):
            return "Mid"
        if contains_any(text, ["low rise"]):
            return "Low"
        if contains_any(text, ["high rise"]):
            return "High"
    return ""


def determine_color_standardized(product: str, desc: str, features: str, option_color: str) -> str:
    rules = [
        (["animal print", "leopard", "snake"], "Animal Print"), (["blue", "indigo", "denim"], "Blue"), (["black"], "Black"),
        (["brown"], "Brown"), (["green", "olive"], "Green"), (["grey", "smoke"], "Grey"), (["orange"], "Orange"),
        (["pink"], "Pink"), (["print"], "Print"), (["purple"], "Purple"), (["red"], "Red"), (["tan", "beige", "khaki"], "Tan"),
        (["white", "ecru"], "White"), (["yellow"], "Yellow"),
    ]
    for src in (product, desc, features, option_color):
        for keys, label in rules:
            if contains_any(src, keys):
                return label
    return ""


def determine_color_simplified(desc: str, features: str, option_color: str) -> str:
    for src in (desc, features):
        if contains_any(src, ["medium light", "light to medium", "medium to light", "light medium"]):
            return "Light to Medium"
        if contains_any(src, ["medium to dark", "dark to medium", "dark medium"]):
            return "Medium to Dark"
        if contains_any(src, ["dark", "deep", "black", "navy"]):
            return "Dark"
        if contains_any(src, ["light blue", "light vintage stonewash", "soft blue", "ecru", "white", "light", "khaki", "tan", "ivory"]):
            return "Light"
        if contains_any(src, ["classic heritage stone washed blue", "mid blue", "medium stone wash", "classic stone washed blue", "vintage washed blue", "classic vintage blue", "medium blue", "classic blue"]):
            return "Medium"
    if contains_any(option_color, ["black", "navy"]):
        return "Dark"
    if contains_any(option_color, ["ecru", "white", "tan", "ivory"]):
        return "Light"
    return ""


def determine_stretch(desc: str, features: str) -> str:
    for src in (desc, features):
        if contains_any(src, ["without stretch", "rigid", "no stretch", "non stretch"]):
            return "Rigid"
        if find_word(src, "stretch"):
            return "Stretch"
    return ""


def fetch_searchspring() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    page = 1
    while True:
        params = [
            ("siteId", SEARCHSPRING_SITE_ID),
            ("resultsFormat", "json"),
            ("resultsPerPage", "250"),
            ("q", "jeans"),
            ("q", "womens"),
            ("page", str(page)),
        ]
        resp = SESSION.get(SEARCHSPRING_URL, params=params, timeout=40)
        resp.raise_for_status()
        payload = resp.json()
        results = []
        if isinstance(payload.get("results"), dict):
            results = payload["results"].get("products") or []
        elif isinstance(payload.get("results"), list):
            results = payload["results"]
        elif isinstance(payload.get("items"), list):
            results = payload["items"]
        if not results:
            break
        for item in results:
            handle = str(item.get("handle") or "")
            if not handle:
                u = str(item.get("url") or "")
                m = re.search(r"/products/([^/?#]+)", u)
                if m:
                    handle = m.group(1)
            if not handle:
                continue
            pct_raw = str(item.get("ss_instock_pct") or "")
            pct = f"{int(pct_raw)}%" if pct_raw.isdigit() else ""
            out[handle] = {
                "Instock Percent": pct,
                "Google Analytics Purchases": str(item.get("ga_item_quantity") or ""),
                "Google Analytics AVG Revenue": str(item.get("ga_product_revenue_per_purchase") or ""),
                "Google Analytics Total Revenue": str(item.get("ga_item_revenue") or ""),
            }
        page += 1
    logging.info("Searchspring entries loaded: %s", len(out))
    return out


def main() -> None:
    configure_logging()

    # quick required validations for style-name logic examples
    examples = {
        "LOW RIDER JEAN LIBERTY BLUE PLAID": "LOW RIDER JEAN",
        "PINUP CROP JEAN NOIR": "PINUP JEAN",
        "KSUPER JEAN LIBERTY BLUE FLAG": "KSUPER JEAN",
    }
    demo_styles = derive_style_name_families(list(examples.keys()) + ["FRONTIER SOHO KULTURE KRYSTAL", "SOHO JEAN ORIGINAL SPLICED", "SPRAY ON XTRA JET BLACK WAX", "SPRAY ON JET BLACK", "STRAIGHT UP STEALTH CARGO"], {"JET BLACK", "STEALTH", "LIBERTY BLUE", "NOIR", "ORIGINAL"})
    logging.info("Style demo: %s", {k: demo_styles.get(k, '') for k in examples})

    products_by_handle: Dict[str, dict] = {}
    json_handles: set[str] = set()
    for c in COLLECTION_HANDLES:
        for p in fetch_collection_products(c):
            h = p.get("handle") or ""
            if h:
                products_by_handle[h] = p
        json_handles.update(fetch_collection_json_handles(c))

    missing_handles = sorted(h for h in json_handles if h and h not in products_by_handle)
    logging.info("Handles from collection JSON not in GraphQL collection fetch: %s", len(missing_handles))
    for i, handle in enumerate(missing_handles, 1):
        product = fetch_product_by_handle(handle)
        if product:
            products_by_handle[handle] = product
        if i % 50 == 0 or i == len(missing_handles):
            logging.info("GraphQL handle backfill fetched %s/%s", i, len(missing_handles))

    # remove duplicate listings by style id/handle and filter out men tags
    filtered: Dict[str, dict] = {}
    seen = set()
    for h, p in products_by_handle.items():
        tags = ", ".join(p.get("tags") or [])
        if "gender_men" in normalize_text(tags):
            continue
        sid = strip_gid(str(p.get("id") or ""), "gid://shopify/Product/")
        key = (sid, h)
        if key in seen:
            continue
        seen.add(key)
        filtered[h] = p
    products_by_handle = filtered
    logging.info("Products retained after dedupe/gender filter: %s", len(products_by_handle))

    # Product-level color method1
    color_m1: Dict[str, str] = {}
    for h, p in products_by_handle.items():
        color_m1[h] = derive_color_method1(str(p.get("title") or "")).upper()

    colors_known = {c for c in color_m1.values() if c}
    colors_known.update({"JET BLACK", "BLANC"})

    style_name_map = derive_style_name_families([str(p.get("title") or "") for p in products_by_handle.values()], colors_known)

    # finalize product-level color and pdp cache
    pdp_cache: Dict[str, Dict[str, str]] = {}
    for i, h in enumerate(sorted(products_by_handle), 1):
        pdp_cache[h] = fetch_pdp(h)
        if i % 50 == 0 or i == len(products_by_handle):
            logging.info("PDP fetched %s/%s", i, len(products_by_handle))

    ss_map = fetch_searchspring()

    rows: List[Dict[str, Any]] = []
    for h, p in products_by_handle.items():
        title = str(p.get("title") or "")
        style_name = style_name_map.get(title, title).upper().strip()
        color = color_m1.get(h) or derive_color_method2(title, style_name).upper()
        if not color:
            color = str(get_option(((p.get("variants") or {}).get("nodes") or [{}])[0], ["color", "option2"]) or "").upper()
        color = re.sub(r"\bXTRA\b", " ", color, flags=re.IGNORECASE)
        color = re.sub(r"\s+", " ", color).strip()

        tags_list = p.get("tags") or []
        tags = ", ".join(tags_list)
        desc = str(p.get("description") or "")
        features = pdp_cache.get(h, {}).get("features", "")

        product_type = determine_product_type(str(((p.get("category") or {}).get("name") or "")), str(((p.get("seo") or {}).get("description") or "")), tags)
        for v in ((p.get("variants") or {}).get("nodes") or []):
            size = get_option(v, ["size", "option1"])
            style_id = strip_gid(str(p.get("id") or ""), "gid://shopify/Product/")
            variant_id = strip_gid(str(v.get("id") or ""), "gid://shopify/ProductVariant/")
            jean_style = determine_jean_style(title, desc, features)
            row = {
                "Style Id": style_id,
                "Handle": h,
                "Published At": format_date(p.get("publishedAt")),
                "Created At": format_date(p.get("createdAt")),
                "Product": title,
                "Style Name": style_name,
                "Product Type": product_type or "Jeans",
                "Tags": tags,
                "Vendor": p.get("vendor") or "",
                "Description": clean_html_text(desc),
                "Variant Title": f"{style_name} - {color} - {size}".strip(" -"),
                "Color": color,
                "Size": size,
                "Rise": pdp_cache[h].get("rise", ""),
                "Inseam": pdp_cache[h].get("inseam", ""),
                "Leg Opening": pdp_cache[h].get("leg_opening", ""),
                "Price": f"${float((v.get('price') or {}).get('amount')):.2f}" if (v.get("price") or {}).get("amount") else "",
                "Compare at Price": f"${float((v.get('compareAtPrice') or {}).get('amount')):.2f}" if (v.get("compareAtPrice") or {}).get("amount") else "",
                "Available for Sale": str(bool(v.get("availableForSale"))).upper(),
                "Quantity Available": v.get("quantityAvailable") if v.get("quantityAvailable") is not None else "",
                "Quantity of Style": p.get("totalInventory") if p.get("totalInventory") is not None else "",
                "Instock Percent": ss_map.get(h, {}).get("Instock Percent", ""),
                "Google Analytics Purchases": ss_map.get(h, {}).get("Google Analytics Purchases", ""),
                "Google Analytics AVG Revenue": ss_map.get(h, {}).get("Google Analytics AVG Revenue", ""),
                "Google Analytics Total Revenue": ss_map.get(h, {}).get("Google Analytics Total Revenue", ""),
                "SKU - Shopify": variant_id,
                "SKU - Brand": v.get("sku") or "",
                "Barcode": v.get("barcode") or "",
                "Image URL": ((p.get("featuredImage") or {}).get("url") or ""),
                "SKU URL": p.get("onlineStoreUrl") or f"https://ksubi.com/products/{h}",
                "Jean Style": jean_style,
                "Inseam Label": determine_inseam_label(desc, features),
                "Inseam Style": determine_inseam_style(desc, features, jean_style),
                "Rise Label": determine_rise_label(desc, features),
                "Color - Simplified": determine_color_simplified(desc, features, color),
                "Color - Standardized": determine_color_standardized(title, desc, features, color),
                "Stretch": determine_stretch(desc, features),
                "Gender": "Women" if "women" in normalize_text(tags) else "",
            }
            rows.append(row)

    # infer jean style/rise label by style name when blank
    by_style = defaultdict(list)
    for r in rows:
        by_style[r["Style Name"]].append(r)
    for r in rows:
        if not r["Jean Style"]:
            peers = [x for x in by_style[r["Style Name"]] if x["Jean Style"]]
            if peers and len({x["Jean Style"] for x in peers}) == 1:
                r["Jean Style"] = peers[0]["Jean Style"]
        if not r["Rise Label"]:
            peers = [x for x in by_style[r["Style Name"]] if x["Rise Label"]]
            if peers and len({x["Rise Label"] for x in peers}) == 1:
                r["Rise Label"] = peers[0]["Rise Label"]

    # Fill Color - Simplified by matching Color when missing.
    color_simple_map: Dict[str, str] = {}
    for r in rows:
        color_key = normalize_text(str(r.get("Color") or ""))
        simple = str(r.get("Color - Simplified") or "").strip()
        if color_key and simple and color_key not in color_simple_map:
            color_simple_map[color_key] = simple
    for r in rows:
        if not str(r.get("Color - Simplified") or "").strip():
            color_key = normalize_text(str(r.get("Color") or ""))
            if color_key in color_simple_map:
                r["Color - Simplified"] = color_simple_map[color_key]

    # remove duplicate variants
    deduped: List[Dict[str, Any]] = []
    seen_keys = set()
    for r in rows:
        k = (r["Style Id"], r["SKU - Shopify"], r["SKU - Brand"])
        if k in seen_keys:
            continue
        seen_keys.add(k)
        deduped.append(r)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = OUTPUT_DIR / f"{BRAND}_{ts}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        w.writeheader()
        w.writerows(deduped)
    logging.info("Rows written: %s", len(deduped))
    logging.info("CSV written: %s", out.resolve())


if __name__ == "__main__":
    main()
