#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime
from fractions import Fraction
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import requests
from bs4 import BeautifulSoup

BRAND = "HUDSON"
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / f"{BRAND}_run.log"

HOST_ROTATION = ["https://www.hudsonjeans.com", "https://hudsonjeans.myshopify.com"]
COLLECTION_HANDLES = ["womens-denim-fits", "womens-sale-denim"]
STOREFRONT_TOKEN = "5d2cd61bc54197b3a7a78452bf3411da"

FILTER_TITLE_WORDS = {"short", "skirt", "capri", "romper"}
FILTER_CATEGORY_WORDS = {"short", "skirt", "capri", "romper"}

STYLE_REMOVE_TERMS = [
    "Accent Hardware", "Corduroy", "Cuff", "Cuffed", "Darted", "Destroyed", "Fit", "Flap Pocket", "Flap",
    "Frayed Seam", "Front Yoke", "Jean w/ Slit Hem", "Jean", "Leather", "Lightweight", "low and loose", "Pant",
    "patch", "petite", "Pintucked", "Raw Hem", "Seam", "Seamed Front Yoke", "Seamed", "Side Seam Snaps", "Split",
    "Snake Print", "Super", "Track Pant", "Trouser Jean", "Trouser", "Vegan Leather", "vent", "slit",
    "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "w/ Slit Hem", "W/ Stud Detailing", "W/ Wide Cuff",
    "W/Flap", "Welt Pocket", "With Cuff", "With Frayed Seam", "Zipper", "BELTED", "CROP", "CUTOFF", "FLAG",
    "FLIP", "KRYSTAL", "CRYSTAL", "PANEL", "PLAID", "RENAISSANCE", "RINSE", "RIPPED", "SADDLE", "SEQUIN",
    "SOTT", "SPARK", "SPLICED", "STUDDED", "TRASHED", "WAX", "COATED", "FRONTIER", "LO", "SELVEDGE",
    "CRUSHED", "REPAIR", "SLICE", "PLUS", "ANKLE", "32 INSEAM", "\"32 INSEAM", "WITH FRAYED SEAMS",
]

CSV_HEADERS = [
    "Style Id", "Handle", "Published At", "Created At", "Product", "Style Name", "Product Type", "Tags", "Vendor",
    "Description", "Variant Title", "Color", "Size", "Rise", "Inseam", "Leg Opening", "Price", "Compare at Price",
    "Available for Sale", "Quantity Available", "Quantity of Style", "SKU - Shopify", "SKU - Brand", "Barcode", "Image URL",
    "SKU URL", "Jean Style", "Product Line", "Inseam Label", "Inseam Style", "Rise Label", "Hem Style",
    "Color - Simplified", "Color - Standardized", "Stretch", "Gender",
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
        onlineStoreUrl
        featuredImage { url }
        category { name }
        variants(first: 100) {
          nodes {
            id
            title
            sku
            barcode
            availableForSale
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

PRODUCT_QUERY = """
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
    onlineStoreUrl
    featuredImage { url }
    category { name }
    variants(first: 100) {
      nodes {
        id
        title
        sku
        barcode
        availableForSale
        price { amount }
        compareAtPrice { amount }
        selectedOptions { name value }
      }
    }
  }
}
"""

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
})
SESSION.verify = False
requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]


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


def contains_phrase_whole(text: str, phrase: str) -> bool:
    n = f" {normalize_text(text)} "
    p = normalize_text(phrase)
    if not p:
        return False
    return bool(re.search(rf"(^|\s){re.escape(p)}(\s|$)", n))


def contains_any_whole(text: str, phrases: Sequence[str]) -> bool:
    return any(contains_phrase_whole(text, p) for p in phrases)


def find_word(text: str, word: str) -> bool:
    n, w = normalize_text(text), normalize_text(word)
    return bool(re.search(rf"(^|\s){re.escape(w)}(\s|$)", n))


def strip_gid(value: str, prefix: str) -> str:
    if not value:
        return ""
    return value.replace(prefix, "") if value.startswith(prefix) else value.split("/")[-1]


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except ValueError:
        return ""


def request_with_rotation(path: str, *, method: str = "GET", headers: Optional[dict] = None, payload: Optional[dict] = None, params: Optional[dict] = None, timeout: int = 40) -> requests.Response:
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
            time.sleep(0.5)
    raise RuntimeError(f"Unable to request {path}: {last_exc}")


def parse_number_with_fraction(text: str) -> str:
    raw = (text or "").replace('"', ' ').strip()
    if not raw:
        return ""
    total = 0.0
    for token in re.split(r"\s+", raw):
        if not token:
            continue
        if re.fullmatch(r"\d+/\d+", token):
            total += float(Fraction(token))
        else:
            try:
                total += float(token)
            except ValueError:
                pass
    return f"{total:.3f}".rstrip("0").rstrip(".") if total else ""


def parse_measurement(desc: str, labels: Sequence[str], before: bool = False) -> str:
    t = desc or ""
    for label in labels:
        if before:
            m = re.search(rf"([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?)\s*(?:\"|cm)?\s*[^\n\r]{{0,3}}{re.escape(label)}", t, flags=re.IGNORECASE)
            if m:
                return parse_number_with_fraction(m.group(1))
        # cm / inches pattern
        m = re.search(rf"{re.escape(label)}\s*:?[\s]*([0-9]+(?:\.[0-9]+)?)\s*cm\s*/\s*([0-9]+(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)\s*\"", t, flags=re.IGNORECASE)
        if m:
            return parse_number_with_fraction(m.group(2))
        # inch direct
        m = re.search(rf"{re.escape(label)}\s*:?[\s]*([0-9]+(?:\.[0-9]+|\s+[0-9]+/[0-9]+|/[0-9]+)?)\s*\"", t, flags=re.IGNORECASE)
        if m:
            return parse_number_with_fraction(m.group(1))
        # cm only
        m = re.search(rf"{re.escape(label)}\s*:?[\s]*([0-9]+(?:\.[0-9]+)?)\s*cm", t, flags=re.IGNORECASE)
        if m:
            inches = float(m.group(1)) / 2.54
            return f"{inches:.3f}".rstrip("0").rstrip(".")
        # loose number near label
        m = re.search(rf"{re.escape(label)}[^0-9]{{0,3}}([0-9]+(?:\.[0-9]+)?)", t, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return ""


def parse_inseam(desc: str) -> str:
    t = desc or ""
    # Step 1: "a ## inseam" or "and ## inseam"
    m = re.search(r"\b(?:a|and)\s+([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)\s*\"?\s*inseam\b", t, flags=re.IGNORECASE)
    if m:
        return parse_number_with_fraction(m.group(1))

    labels = ["inseam", "inleg length", "inleg"]
    for label in labels:
        # Step 2: direct number after label, including cm/in pair, inches, or cm-only
        m = re.search(
            rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?)\s*cm\s*/\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)\s*\"?",
            t,
            flags=re.IGNORECASE,
        )
        if m:
            return parse_number_with_fraction(m.group(2))

        m = re.search(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)\s*\"", t, flags=re.IGNORECASE)
        if m:
            return parse_number_with_fraction(m.group(1))

        m = re.search(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?)\s*cm\b", t, flags=re.IGNORECASE)
        if m:
            inches = float(m.group(1)) / 2.54
            return f"{inches:.3f}".rstrip("0").rstrip(".")

        m = re.search(rf"{re.escape(label)}\s*:?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)\b", t, flags=re.IGNORECASE)
        if m:
            return parse_number_with_fraction(m.group(1))

        # Step 3: number 1-3 chars away from label
        m = re.search(rf"{re.escape(label)}[^0-9]{{1,3}}([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+|/[0-9]+)?)", t, flags=re.IGNORECASE)
        if m:
            return parse_number_with_fraction(m.group(1))
    return ""


def clean_html_text(text: str) -> str:
    return BeautifulSoup(text or "", "html.parser").get_text(" ", strip=True)


def get_option(variant: dict, names: Sequence[str]) -> str:
    for opt in variant.get("selectedOptions") or []:
        name = (opt.get("name") or "").strip().lower()
        if name in names:
            return str(opt.get("value") or "")
    return ""


def fetch_collection_products(handle: str) -> List[dict]:
    products: List[dict] = []
    cursor: Optional[str] = None
    headers = {"X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN, "Content-Type": "application/json"}
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


def fetch_collection_json_handles(collection: str) -> List[str]:
    out: List[str] = []
    page = 1
    while True:
        resp = request_with_rotation(f"/collections/{collection}.json", params={"limit": 250, "page": page})
        payload = resp.json()
        products = payload.get("products") or []
        if not products:
            break
        out.extend([str(p.get("handle") or "") for p in products if p.get("handle")])
        logging.info("JSON %s fetched %s products on page %s", collection, len(products), page)
        page += 1
    return out


def fetch_product_by_handle(handle: str) -> Optional[dict]:
    headers = {"X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN, "Content-Type": "application/json"}
    data = request_with_rotation(
        "/api/unstable/graphql.json",
        method="POST",
        headers=headers,
        payload={"query": PRODUCT_QUERY, "variables": {"handle": handle}},
    ).json()
    return ((data.get("data") or {}).get("product") or None)


def fetch_quick_view(handle: str) -> Dict[str, Any]:
    for host in HOST_ROTATION:
        url = f"{host.rstrip('/')}/products/{handle}?view=quick"
        try:
            resp = SESSION.get(url, timeout=40)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            inv_map: Dict[str, int] = {}
            qty_style = 0
            script = soup.find("script", id="product-json")
            if script and script.string:
                obj = json.loads(script.string)
                for v in obj.get("variants") or []:
                    vid = str(v.get("id") or "")
                    qty = int(v.get("inventory_quantity") or 0)
                    inv_map[vid] = qty
                    qty_style += qty
            image_url = ""
            for sc in soup.find_all("script"):
                text = sc.string or sc.get_text() or ""
                if "window.SwymProductInfo.product" in text and "images" in text:
                    m = re.search(r"images\s*:\s*(\[[^\]]+\])", text, flags=re.DOTALL)
                    if m:
                        arr_txt = m.group(1).replace("\\/", "/")
                        try:
                            imgs = json.loads(arr_txt)
                            if imgs:
                                image_url = str(imgs[0])
                        except Exception:
                            m2 = re.search(r"['\"](//[^'\"]+)['\"]", arr_txt)
                            if m2:
                                image_url = m2.group(1)
                    break
            if image_url.startswith("//"):
                image_url = "https:" + image_url
            return {"inventory_by_variant": inv_map, "quantity_style": qty_style, "image_url": image_url}
        except Exception as exc:  # noqa: BLE001
            logging.warning("Quick view failed %s (%s)", url, exc)
    return {"inventory_by_variant": {}, "quantity_style": 0, "image_url": ""}


def is_filtered_product(title: str, category: str, tags: str) -> bool:
    t = normalize_text(title)
    c = normalize_text(category)
    if any(find_word(t, w) for w in FILTER_TITLE_WORDS):
        return True
    if any(find_word(c, w) for w in FILTER_CATEGORY_WORDS):
        return True
    if "gender men s" in normalize_text(tags) or "gender men" in normalize_text(tags):
        return True
    return False


def build_style_name(title: str) -> str:
    base_title = re.split(r"\s+-\s+", title or "", maxsplit=1)[0]
    starts_with_rise = bool(re.match(r"^\s*(ultra\s+high\s+rise|high[\s-]*rise|mid[\s-]*rise|low[\s-]*rise)\b", base_title, flags=re.IGNORECASE))
    s = base_title.replace("-", " ").replace('"', " ")
    if not starts_with_rise:
        s = re.sub(r"\b(ultra\s+high\s+rise|high\s+rise|mid\s+rise|low\s+rise)\b", " ", s, flags=re.IGNORECASE)
    for term in sorted(STYLE_REMOVE_TERMS, key=len, reverse=True):
        pat = r"\b" + re.escape(term).replace(r"\ ", r"\s+") + r"\b"
        s = re.sub(pat, " ", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    if re.fullmatch(r"(?i).*\bboot\b.*", s) and not contains_any(s, ["bootcut"]):
        s = re.sub(r"\bboot\b", "Bootcut", s, flags=re.IGNORECASE)
    if contains_any(s, ["bootcut"]) and contains_any(s, ["barefoot"]):
        s = re.sub(r"\bbarefoot\b\s+\bbootcut\b", "Bootcut Barefoot", s, flags=re.IGNORECASE)
    if contains_any(s, ["wide"]) and not contains_any(s, ["wide leg"]):
        s = re.sub(r"\bwide\b", "wide leg", s, flags=re.IGNORECASE)
    if contains_any(s, ["maternity"]):
        words = s.split()
        words = [w for w in words if normalize_text(w) != "maternity"]
        if words:
            s = words[0] + " Maternity" + (" " + " ".join(words[1:]) if len(words) > 1 else "")
    # keep Loose only at end
    words = s.split()
    if "Loose" in words[:-1]:
        words = [w for i, w in enumerate(words) if not (normalize_text(w) == "loose" and i != len(words) - 1)]
    s = " ".join(words)
    return s.upper().strip()


def infer_fill_single_word_style(style: str, style_leg_rows: Dict[str, List[dict]], leg_opening: str) -> str:
    if len(style.split()) != 1:
        return style
    prefix = style.split()[0]
    candidates = [k for k in style_leg_rows if k.startswith(prefix + " ")]
    if not candidates:
        return style
    if len(candidates) == 1:
        return candidates[0]
    try:
        lo = float(leg_opening)
        def dist(name: str) -> float:
            vals = [float(r.get("Leg Opening") or 0) for r in style_leg_rows[name] if str(r.get("Leg Opening") or "")] 
            return abs((sum(vals)/len(vals)) - lo) if vals else 9999
        return sorted(candidates, key=dist)[0]
    except Exception:
        return candidates[0]


def infer_straight_by_leg_opening(leg_opening: str) -> str:
    try:
        v = float(leg_opening)
    except Exception:
        return ""
    if v < 15.5:
        return "Straight from Knee"
    if 15.5 <= v <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def determine_jean_style(title: str, desc: str, style_name: str, tags: str, leg_opening: str) -> str:
    if contains_any(title, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(title, ["tapered", "mom"]): return "Tapered"
    if contains_any(title, ["baggy"]): return "Baggy"
    if contains_any(title, ["flare"]): return "Flare"
    if contains_any(title, ["bootcut", "boot"]): return "Bootcut"
    if contains_any(title, ["skinny"]): return "Skinny"
    if contains_any(title, ["wide leg"]): return "Wide Leg"
    if contains_any(title, ["cigarette"]): return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["snug throughout the body", "slim straight", "slim straight", "classic straight fit", "cigarette"]):
        return "Straight from Knee"
    if contains_any(title, ["straight"]) and contains_any(desc, ["relaxed straight leg", "loose", "relaxed straight"]):
        return "Straight from Thigh"
    if contains_any(title, ["straight"]):
        inferred = infer_straight_by_leg_opening(leg_opening)
        if inferred:
            return inferred

    if contains_any(desc, ["barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(desc, ["skinny"]): return "Skinny"
    if contains_any(desc, ["flare"]): return "Flare"
    if contains_any(desc, ["bootcut"]): return "Bootcut"
    if contains_any(desc, ["taper", "tapering", "tapered"]): return "Tapered"
    if contains_any(desc, ["wide leg", "palazzo"]): return "Wide Leg"
    if contains_any(desc, ["straight"]):
        inferred = infer_straight_by_leg_opening(leg_opening)
        if inferred:
            return inferred
    if contains_any(desc, ["baggy", "loose fit"]): return "Baggy"

    if contains_any(tags, ["filter_style_barrel", "barrel", "bowed", "bow leg", "stovepipe", "stove pipe", "horseshoe"]): return "Barrel"
    if contains_any(tags, ["filter_style_skinny", "filter_style_superskinny", "skinny"]): return "Skinny"
    if contains_any(tags, ["filter_style_flare", "flare"]): return "Flare"
    if contains_any(tags, ["filter_style_boot", "bootcut"]): return "Bootcut"
    if contains_any(tags, ["taper", "tapering", "tapered"]): return "Tapered"
    if contains_any(tags, ["filter_style_wide", "wide leg", "palazzo"]): return "Wide Leg"
    if contains_any(tags, ["filter_style_cigarette"]): return "Straight from Knee"
    if contains_any(tags, ["filter_style_straight", "straight"]):
        inferred = infer_straight_by_leg_opening(leg_opening)
        if inferred:
            return inferred
    if contains_any(tags, ["baggy"]): return "Baggy"
    return ""


def infer_inseam_style_from_style_inseam(row: Dict[str, Any], by_style: Dict[str, List[dict]]) -> str:
    peers = [
        x for x in by_style[row["Style Name"]]
        if x.get("Inseam Style") and str(x.get("Inseam") or "") == str(row.get("Inseam") or "")
    ]
    if peers and len({x["Inseam Style"] for x in peers}) == 1:
        return str(peers[0]["Inseam Style"])
    return ""


def determine_inseam_style_title_handle(title: str, handle: str, jean_style: str) -> str:
    if jean_style in {"Skinny", "Tapered"} and contains_any(title, ["ankle"]):
        return "Full Length"
    if contains_any(title, ["ankle"]): return "Ankle"
    if contains_any(title, ["crop"]): return "Cropped"

    if jean_style in {"Skinny", "Tapered"} and contains_any(handle, ["ankle"]):
        return "Full Length"
    if contains_any(handle, ["ankle"]): return "Ankle"
    if contains_any(handle, ["crop"]): return "Cropped"
    if contains_any(handle, ["full length"]): return "Full Length"
    return ""


def determine_inseam_style_desc(desc: str, jean_style: str) -> str:
    if jean_style in {"Skinny", "Tapered"} and contains_any(desc, ["ankle grazing", "cropped at the ankle", "ankle length", "ankle length", "ankle jean", "hit at the ankle bone", "hit below the ankle bone", "hits at the ankle", "slim at the ankle", "drape around the ankle"]):
        return "Full Length"
    if contains_any(desc, ["ankle grazing", "cropped at the ankle", "ankle length", "ankle jean", "hit at the ankle bone", "hit below the ankle bone", "hits at the ankle", "slim at the ankle", "drape around the ankle"]):
        return "Ankle"
    if contains_any(desc, ["stack stylishly above the ankle", "stack at the ankle", "full length", "xtra", "stack at the hem", "tapers at the ankle"]):
        return "Full Length"
    if contains_any(desc, ["crop length", "crop length", "cropped"]):
        return "Cropped"
    return ""


def determine_inseam_style_tags(tags: str, jean_style: str, inseam: str) -> str:

    try:
        inseam_val = float(inseam) if inseam else None
    except Exception:
        inseam_val = None

    n_tags = normalize_text(tags)
    if "full" in n_tags and "ankle" in n_tags and inseam_val and inseam_val >= 30:
        return "Full Length"
    if "ankle" in n_tags and jean_style in {"Skinny", "Tapered", "Straight from Knee"}:
        return "Full Length"
    if "ankle" in n_tags and "full" not in n_tags and "crop" not in n_tags:
        return "Ankle"
    if "full" in n_tags and "ankle" not in n_tags and "crop" not in n_tags:
        return "Full Length"
    if "crop" in n_tags and "petite" in n_tags and inseam_val is not None:
        return "Full Length" if inseam_val >= 29 else "Crop"
    if "crop" in n_tags and "full" not in n_tags and "ankle" not in n_tags:
        return "Cropped"
    return ""


def infer_rise_label_from_style_rise(row: Dict[str, Any], by_style: Dict[str, List[dict]]) -> str:
    peers = [
        x for x in by_style[row["Style Name"]]
        if x.get("Rise Label") and str(x.get("Rise") or "") == str(row.get("Rise") or "")
    ]
    if peers and len({x["Rise Label"] for x in peers}) == 1:
        return str(peers[0]["Rise Label"])
    return ""


def determine_rise_label_title_handle_desc(title: str, handle: str, desc: str) -> str:
    ultra_low = ["super low rise", "ultra low rise", "super low waist", "ultra low waist"]
    ultra_high = ["super high rise", "ultra high rise", "super high waist", "ultra high waist"]
    if contains_any(title, ultra_low): return "Ultra Low"
    if contains_any(title, ultra_high): return "Ultra High"
    if contains_any(title, ["mid rise"]): return "Mid"
    if contains_any(title, ["low rise"]): return "Low"
    if contains_any(title, ["high rise"]): return "High"

    n_handle = normalize_text(handle)
    if contains_any(n_handle, ["super low rise", "ultra low rise", "super low waist", "ultra low waist", "slr", "ulr", "slw", "ulw"]): return "Ultra Low"
    if contains_any(n_handle, ["super high rise", "ultra high rise", "super high waist", "ultra high waist", "shr", "uhr", "shw", "uhw"]): return "Ultra High"
    if contains_any(n_handle, ["mid rise", "mid waist", " mr ", " mw "]): return "Mid"
    if contains_any(n_handle, ["low rise", "low waist", " lr ", " lw "]): return "Low"
    if contains_any(n_handle, ["high rise", "high waist", " hr ", " hw "]): return "High"

    if contains_any(desc, ultra_low): return "Ultra Low"
    if contains_any(desc, ultra_high): return "Ultra High"
    if contains_any(desc, ["mid rise"]): return "Mid"
    if contains_any(desc, ["low rise", "low on the hip", "low on the waist"]): return "Low"
    if contains_any(desc, ["high rise", "high waist", "high on the hip", "high on the waist", "elevated waistline", "elevated cinched waistline"]): return "High"
    return ""


def determine_rise_label_filter_tags(tags: str) -> str:
    if contains_any(tags, ["filter_rise_mid"]): return "Mid"
    if contains_any(tags, ["filter_rise_low"]): return "Low"
    if contains_any(tags, ["filter_rise_high"]): return "High"
    return ""


def determine_rise_label_broad_tags(tags: str) -> str:
    if contains_any(tags, ["mid"]): return "Mid"
    if contains_any(tags, ["high"]): return "High"
    if contains_any(tags, ["low"]): return "Low"
    return ""


def determine_hem_style(desc: str) -> str:
    if contains_any(desc, ["split hem", "side slits", "slit inseams at the hem", "slit at the hem", "slit hem"]): return "Split Hem"
    if contains_any(desc, ["released hem"]): return "Released Hem"
    if contains_any(desc, ["raw hem", "raw edge hem"]): return "Raw Hem"
    if contains_any(desc, ["clean hem", "clean edge hem", "tacking detail at bottom hem", "clean finished hem", "finished hem"]): return "Clean Hem"
    if contains_any(desc, ["wide hem", "trouser hem"]): return "Wide Hem"
    if contains_any(desc, ["distressed hem", "destructed hem"]): return "Distressed Hem"
    if contains_any(desc, ["zippers at the hem"]): return "Zipper Hem"
    return ""


def determine_color_standardized(product: str, color: str, desc: str, tags: str) -> str:
    # 1) Map by Color
    if contains_any_whole(color, ["animal print", "leopard", "snake", "camo"]): return "Animal Print"
    if contains_any_whole(color, ["blue", "indigo"]): return "Blue"
    if contains_any_whole(color, ["brown", "cinnamon", "coffee", "espresso"]): return "Brown"
    if contains_any_whole(color, ["green", "olive", "cypress", "sage"]): return "Green"
    if contains_any_whole(color, ["grey", "gray", "smoke"]): return "Grey"
    if contains_any_whole(color, ["orange"]): return "Orange"
    if contains_any_whole(color, ["pink"]): return "Pink"
    if contains_any_whole(color, ["print"]): return "Print"
    if contains_any_whole(color, ["purple", "violet"]): return "Purple"
    if contains_any_whole(color, ["red", "wine", "burgundy"]): return "Red"
    if contains_any_whole(color, ["tan", "beige", "khaki"]): return "Tan"
    if contains_any_whole(color, ["white", "ecru", "egret", "cream", "bleach"]): return "White"
    if contains_any_whole(product, ["yellow"]): return "Yellow"
    if contains_any_whole(color, ["black", "noir", "raven"]): return "Black"

    # 2) Map by Description
    if contains_any_whole(desc, ["animal print", "leopard", "snake"]): return "Animal Print"
    if contains_any_whole(desc, ["blue", "indigo"]): return "Blue"
    if contains_any_whole(desc, ["brown"]): return "Brown"
    if contains_any_whole(desc, ["green", "olive"]): return "Green"
    if contains_any_whole(desc, ["grey", "gray", "smoke"]): return "Grey"
    if contains_any_whole(desc, ["orange"]): return "Orange"
    if contains_any_whole(desc, ["pink"]): return "Pink"
    if contains_any_whole(desc, ["print"]): return "Print"
    if contains_any_whole(desc, ["purple", "maroon", "violet"]): return "Purple"
    if contains_any_whole(desc, ["red", "wine", "burgundy"]): return "Red"
    if contains_any_whole(desc, ["tan", "beige", "khaki"]): return "Tan"
    if contains_any_whole(desc, ["white", "ecru", "cream"]): return "White"
    if contains_any_whole(desc, ["yellow"]): return "Yellow"
    if contains_any_whole(desc, ["black", "washed black"]): return "Black"
    if contains_any_whole(desc, ["dark base", "acid wash", "dark rinse", "dark stretch denim", "dark wash", "dark washed", "medium wash", "medium dark", "medium light", "rich dark base", "rich dark base"]):
        return "Blue"

    # 3) Map by Tags
    if contains_any_whole(tags, ["animal print", "leopard", "snake"]): return "Animal Print"
    if contains_any_whole(tags, ["filter_color_white", "white", "whitedenim", "ecru"]): return "White"
    if contains_any_whole(tags, ["filter_color_blue", "blue", "indigo"]): return "Blue"
    if contains_any_whole(tags, ["filter_color_black", "black"]): return "Black"
    if contains_any_whole(tags, ["filter_color_brown", "brown"]): return "Brown"
    if contains_any_whole(tags, ["filter_color_green", "green"]): return "Green"
    if contains_any_whole(tags, ["filter_color_grey", "filter_color_gray", "grey", "gray", "smoke"]): return "Grey"
    if contains_any_whole(tags, ["filter_color_orange", "orange"]): return "Orange"
    if contains_any_whole(tags, ["filter_color_pink", "pink"]): return "Pink"
    if contains_any_whole(tags, ["print"]): return "Print"
    if contains_any_whole(tags, ["filter_color_purple", "purple"]): return "Purple"
    if contains_any_whole(tags, ["filter_color_red", "red"]): return "Red"
    if contains_any_whole(tags, ["filter_color_tan", "tan", "filter_color_beige", "beige", "filter_color_khaki", "khaki"]): return "Tan"
    if contains_any_whole(tags, ["filter_color_yellow", "yellow"]): return "Yellow"
    if contains_any_whole(tags, ["filter_wash_med", "med", "medium"]):
        return "Blue"
    return ""


def determine_color_simplified(color_std: str, color: str, desc: str, tags: str) -> str:
    if contains_any(color_std, ["black", "brown"]): return "Dark"
    if contains_any(color_std, ["white", "tan"]): return "Light"
    if contains_any(color, ["wine", "burgundy", "navy", "dark", "deep"]): return "Dark"
    if contains_any(color, ["pastel", "cream", "light"]): return "Light"
    if contains_any(color, ["medium", "mid"]): return "Medium"
    if contains_any(desc, ["light to medium", "medium to light", "medium light", "light medium"]): return "Light to Medium"
    if contains_any(desc, ["medium to dark", "dark to medium", "medium dark", "dark medium"]): return "Medium to Dark"
    if contains_any(desc, ["dark", "deep", "black", "wine", "burgundy", "navy"]): return "Dark"
    if contains_any(desc, ["light blue", "light vintage stonewash", "soft blue", "ecru", "white", "acid wash", "light", "khaki", "tan", "ivory"]): return "Light"
    if contains_any(desc, ["mid blue", "medium stone wash", "classic stone washed blue", "vintage washed blue", "classic vintage blue", "medium blue", "medium wash", "classic blue"]):
        return "Medium"
    if contains_any(tags, ["filter_wash_light"]) and contains_any(tags, ["filter_wash_med", " medium ", " med "]): return "Light to Medium"
    if contains_any(tags, ["filter_wash_dark"]) and contains_any(tags, ["filter_wash_med", " medium ", " med "]): return "Medium to Dark"
    if contains_any(tags, ["filter_wash_med", " medium ", " med "]): return "Medium"
    if contains_any(tags, ["filter_wash_light", " light "]): return "Light"
    if contains_any(tags, ["filter_wash_dark", " dark "]): return "Dark"
    return ""


def determine_stretch(desc: str, tags: str) -> str:
    if contains_any(desc, ["rigid"]): return "Rigid"
    if contains_any(desc, ["super stretch"]): return "High Stretch"
    if contains_any(desc, ["comfort stretch"]): return "Low Stretch"
    if contains_any(desc, ["stretch"]): return "Medium Stretch"
    if contains_any(tags, ["rigid"]): return "Rigid"
    if contains_any(tags, ["super stretch"]): return "High Stretch"
    if contains_any(tags, ["comfort stretch"]): return "Low Stretch"
    if contains_any(tags, ["stretch"]): return "Medium Stretch"
    return ""


def main() -> None:
    configure_logging()

    headers = {"X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN, "Content-Type": "application/json"}

    products_by_handle: Dict[str, dict] = {}
    json_handles: set[str] = set()
    for collection in COLLECTION_HANDLES:
        for product in fetch_collection_products(collection):
            handle = product.get("handle") or ""
            if handle:
                products_by_handle[handle] = product
        json_handles.update(fetch_collection_json_handles(collection))

    missing_handles = sorted(h for h in json_handles if h and h not in products_by_handle)
    logging.info("Handles from collection JSON not in GraphQL collection fetch: %s", len(missing_handles))
    for i, handle in enumerate(missing_handles, 1):
        p = fetch_product_by_handle(handle)
        if p:
            products_by_handle[handle] = p
        if i % 50 == 0 or i == len(missing_handles):
            logging.info("Backfilled products %s/%s", i, len(missing_handles))

    rows: List[Dict[str, Any]] = []
    style_rows: Dict[str, List[dict]] = defaultdict(list)

    for handle, product in products_by_handle.items():
        title = str(product.get("title") or "")
        category = str(((product.get("category") or {}).get("name") or ""))
        tags = ", ".join(product.get("tags") or [])
        if is_filtered_product(title, category, tags):
            continue

        quick = fetch_quick_view(handle)
        desc = str(product.get("description") or "")
        clean_desc = clean_html_text(desc)

        rise = parse_measurement(clean_desc, ["Rise"]) 
        inseam = parse_inseam(clean_desc)
        try:
            inseam_val = float(inseam) if inseam else None
        except Exception:
            inseam_val = None
        if inseam_val is not None and inseam_val < 20:
            fallback = parse_measurement(clean_desc, ["inseam"])
            if fallback:
                inseam = fallback
        leg_opening = parse_measurement(clean_desc, ["hem", "Hem circumference", "Leg opening"]) 

        style_name = build_style_name(title)
        product_line = "Maternity" if contains_any(title, ["maternity"]) else ""
        inseam_label = "Petite" if contains_any(title, ["petite"]) else ("Maternity" if product_line == "Maternity" else "")

        for variant in ((product.get("variants") or {}).get("nodes") or []):
            variant_id = strip_gid(str(variant.get("id") or ""), "gid://shopify/ProductVariant/")
            size = get_option(variant, ["size", "option1"]) or ""
            color = get_option(variant, ["color", "option2"]) or ""
            if not color:
                raw_vt = str(variant.get("title") or "")
                color = raw_vt.split("/")[0].strip() if "/" in raw_vt else raw_vt

            product_display = f"{title} - {color}".strip(" -")
            jean_style = determine_jean_style(title, clean_desc, style_name, tags, leg_opening)
            inseam_style = determine_inseam_style_title_handle(title, handle, jean_style)
            rise_label = determine_rise_label_title_handle_desc(title, handle, clean_desc)
            hem_style = determine_hem_style(clean_desc)
            color_std = determine_color_standardized(title, color, clean_desc, tags)
            color_simple = determine_color_simplified(color_std, color, clean_desc, tags)
            stretch = determine_stretch(clean_desc, tags)
            gender = "Women" if contains_any(tags, ["womens", "gender women s"]) else ""

            qty_available = quick.get("inventory_by_variant", {}).get(variant_id, "")
            qty_style = quick.get("quantity_style", "")

            row = {
                "Style Id": strip_gid(str(product.get("id") or ""), "gid://shopify/Product/"),
                "Handle": handle,
                "Published At": format_date(product.get("publishedAt")),
                "Created At": format_date(product.get("createdAt")),
                "Product": product_display,
                "Style Name": style_name,
                "Product Type": category,
                "Tags": tags,
                "Vendor": product.get("vendor") or "",
                "Description": clean_desc,
                "Variant Title": f"{style_name} - {color} - {size}".strip(" -"),
                "Color": color,
                "Size": size,
                "Rise": rise,
                "Inseam": inseam,
                "Leg Opening": leg_opening,
                "Price": f"${float((variant.get('price') or {}).get('amount')):.2f}" if (variant.get("price") or {}).get("amount") else "",
                "Compare at Price": f"${float((variant.get('compareAtPrice') or {}).get('amount')):.2f}" if (variant.get("compareAtPrice") or {}).get("amount") else "",
                "Available for Sale": str(bool(variant.get("availableForSale"))).upper(),
                "Quantity Available": qty_available,
                "Quantity of Style": qty_style,
                "SKU - Shopify": variant_id,
                "SKU - Brand": variant.get("sku") or "",
                "Barcode": variant.get("barcode") or "",
                "Image URL": quick.get("image_url") or ((product.get("featuredImage") or {}).get("url") or ""),
                "SKU URL": product.get("onlineStoreUrl") or f"https://www.hudsonjeans.com/products/{handle}",
                "Jean Style": jean_style,
                "Product Line": product_line,
                "Inseam Label": inseam_label,
                "Inseam Style": inseam_style,
                "Rise Label": rise_label,
                "Hem Style": hem_style,
                "Color - Simplified": color_simple,
                "Color - Standardized": color_std,
                "Stretch": stretch,
                "Gender": gender,
            }
            rows.append(row)
            style_rows[style_name].append(row)

    # style-name single-word fill from peers
    for row in rows:
        row["Style Name"] = infer_fill_single_word_style(row["Style Name"], style_rows, str(row.get("Leg Opening") or ""))

    # infer shared style-level jean/inseam/rise/color fields
    by_style = defaultdict(list)
    for row in rows:
        by_style[row["Style Name"]].append(row)

    for row in rows:
        if not row["Jean Style"]:
            peers = [x for x in by_style[row["Style Name"]] if x["Jean Style"]]
            if peers and len({x["Jean Style"] for x in peers}) == 1:
                row["Jean Style"] = peers[0]["Jean Style"]
        # Inseam Style exact order:
        # 1) title, 2) handle (already done in row build)
        # 3) infer by style+inseam
        if not row["Inseam Style"]:
            inferred = infer_inseam_style_from_style_inseam(row, by_style)
            if inferred:
                row["Inseam Style"] = inferred
        # 4) description
        if not row["Inseam Style"]:
            row["Inseam Style"] = determine_inseam_style_desc(str(row.get("Description") or ""), str(row.get("Jean Style") or ""))
        # 5) infer by style+inseam again
        if not row["Inseam Style"]:
            inferred = infer_inseam_style_from_style_inseam(row, by_style)
            if inferred:
                row["Inseam Style"] = inferred
        # 6) tags
        if not row["Inseam Style"]:
            row["Inseam Style"] = determine_inseam_style_tags(str(row.get("Tags") or ""), str(row.get("Jean Style") or ""), str(row.get("Inseam") or ""))

        # Rise Label exact order:
        # 1) title, 2) handle, 3) description (already done in row build)
        # 4) infer by style+rise
        if not row["Rise Label"]:
            inferred = infer_rise_label_from_style_rise(row, by_style)
            if inferred:
                row["Rise Label"] = inferred
        # 5) filter_rise tags
        if not row["Rise Label"]:
            row["Rise Label"] = determine_rise_label_filter_tags(str(row.get("Tags") or ""))
        # 6) infer by style+rise again
        if not row["Rise Label"]:
            inferred = infer_rise_label_from_style_rise(row, by_style)
            if inferred:
                row["Rise Label"] = inferred
        # 7) broader tags
        if not row["Rise Label"]:
            row["Rise Label"] = determine_rise_label_broad_tags(str(row.get("Tags") or ""))

    # color backfills
    color_simple_map: Dict[str, str] = {}
    color_std_map: Dict[str, str] = {}
    for row in rows:
        ck = normalize_text(str(row.get("Color") or ""))
        if ck and row.get("Color - Simplified") and ck not in color_simple_map:
            color_simple_map[ck] = str(row["Color - Simplified"])
        if ck and row.get("Color - Standardized") and ck not in color_std_map:
            color_std_map[ck] = str(row["Color - Standardized"])
    for row in rows:
        ck = normalize_text(str(row.get("Color") or ""))
        if ck and not row.get("Color - Simplified") and ck in color_simple_map:
            row["Color - Simplified"] = color_simple_map[ck]
        if ck and not row.get("Color - Standardized") and ck in color_std_map:
            row["Color - Standardized"] = color_std_map[ck]

    # dedupe variants
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        key = (row["Style Id"], row["SKU - Shopify"], row["SKU - Brand"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)

    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    out = OUTPUT_DIR / f"{BRAND}_{ts}.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(deduped)

    logging.info("Rows written: %s", len(deduped))
    logging.info("CSV written: %s", out.resolve())


if __name__ == "__main__":
    main()
