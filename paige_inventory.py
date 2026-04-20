import csv
import html
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_FILE = BASE_DIR / "paige_inventory.log"

ALGOLIA_APP_ID = "DK4YY42827"
ALGOLIA_API_KEY = "333da36aea28227274c0ad598d0fbdb0"
ALGOLIA_INDEX = "production_products"
ALGOLIA_SEARCH_URL = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"
GRAPHQL_URL = "https://paige-7873.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN = "383d494a76122b5e6cadffc2c7667ef2"
HOST_ROTATION = [
    "https://shop.paige.com",
    "https://paige-7873.myshopify.com",
    "https://paige.com",
]
PDP_HOST = "https://shop.paige.com"
STYLE_NAME_REMOVE_PHRASES: List[str] = [
    "Accent Hardware", "Ankle", "Belted", "Coated", "Corduroy", "Crop", "Crushed", "Crystal",
    "Cuff", "Cuffed", "Cutoff", "Darted", "Destroyed", "Fit", "Flag", "Flap Pocket", "Flap",
    "Flip", "Frayed Seam", "Front Yoke", "Frontier", "High Rise", "Inch", "Inset", "Jean",
    "Krystal", "Leather", "Lightweight", "Lo", "low and loose", "Low Rise", "Mid Rise", "Panel",
    "Pant", "Patch", "Petite", "Pintucked", "Plaid", "Plus", "Raw Hem", "Renaissance", "Repair",
    "Rinse", "Ripped", "Saddle", "Seam", "Seamed Front Yoke", "Seamed", "Selvedge", "Sequin",
    "Side Seam Snaps", "Skimmer", "Slice", "Snake Print", "Sott", "Spark", "Spliced", "Split",
    "Studded", "Super", "Track Pant", "Trashed", "Trouser", "Vegan Leather", "vent", "slit",
    "W/ Contrast Front Panel", "w/ Cuff", "w/ Flap Jean", "w/ Slit Hem", "W/ Stud Detailing",
    "W/ Wide Cuff", "W/Flap", "Wax", "Welt Pocket", "With Cuff", "With Frayed Seam", "Zipper",
]

PDP_SELECTOR = "[id^='headlessui-disclosure-panel-'] > div > ul > li"
PDP_PARENT_SELECTOR = "div[data-headlessui-state]"

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
    "Returns",
    "Quantity Available (Instore Inventory)",
    "Quantity Available (Online Inventory)",
    "Google Analytics Purchases",
    "Quantity of style",
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
    "Hem Style",
    "Color - Simplified",
    "Color - Standardized",
    "Country Produced",
    "Stretch",
    "Production Cost",
    "Site Exclusive",
]


def ensure_directories() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        LOG_FILE.touch(exist_ok=True)
    except PermissionError:
        pass


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    try:
        with LOG_FILE.open("a", encoding="utf-8") as fh:
            fh.write(f"[{timestamp}] {message}\n")
    except PermissionError:
        pass


def format_price(value: Optional[float]) -> str:
    if value is None:
        return ""
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"${numeric:.2f}"


def format_date(value: Optional[str]) -> str:
    if not value:
        return ""
    iso_value = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso_value)
    except ValueError:
        try:
            dt = datetime.strptime(value[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return value
    return dt.strftime("%m/%d/%Y")
    
def join_tags(tags: Iterable[str]) -> str:
    cleaned: List[str] = []
    for tag in tags:
        
        if not tag:
            continue
        text = html.unescape(str(tag)).strip()
        if text:
            cleaned.append(text)
    return ", ".join(cleaned)


def safe_string(value: Optional[object]) -> str:
    if value is None:
        return ""
    return str(value)


def extract_tag_value(tags: Iterable[str], prefix: str) -> str:
    prefix_lower = prefix.lower()
    for tag in tags:
        if not tag:
            continue
        tag_text = str(tag)
        if tag_text.lower().startswith(prefix_lower):
            return html.unescape(tag_text[len(prefix):]).strip()
    return ""


def should_keep_product(tags: Iterable[str], title: str, source_collection: str) -> bool:
    tag_set = {str(tag).strip() for tag in tags}
    has_required_tag = any(
        key in tag_set
        for key in (
            "clothingType:Jeans",
            "clothingTypeCode:JEAN",
            "category:Denim Bottoms",
            "clothingType:Pant",
        )
    )
    title_has_keyword = ("jean" in title.lower()) or ("pant" in title.lower())
    if source_collection == "women-denim":
        return True
    return has_required_tag and title_has_keyword


def derive_product_type(tags: Iterable[str], title: str) -> str:
    title_lower = title.lower()
    tag_set = {str(tag).strip() for tag in tags}
    if "jean" in title_lower:
        return "Jeans"
    if any(tag in tag_set for tag in ("clothingType:Jeans", "clothingTypeCode:JEAN", "category:Denim Bottoms")):
        return "Jeans"
    if "pant" in title_lower:
        return "Pants"
    if any(tag in tag_set for tag in ("clothingType:Pant", "clothingTypeCode:PAN")):
        return "Pants"
    return ""


def derive_style_name_base(product_title: str) -> str:
    text = (product_title or "").split("-", 1)[0].strip()
    text = text.replace('"', " ")
    text = re.sub(r"\b\d+\b", " ", text)
    for phrase in sorted(STYLE_NAME_REMOVE_PHRASES, key=len, reverse=True):
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def to_float(value: str) -> Optional[float]:
    try:
        return float(value) if value != "" else None
    except ValueError:
        return None


def contains_phrase(text: str, phrase: str) -> bool:
    if not text or not phrase:
        return False
    pattern = re.escape(phrase.strip()).replace(r"\ ", r"\s+")
    return re.search(rf"(?<![a-z0-9]){pattern}(?![a-z0-9])", text.lower()) is not None


def text_has_any(text: str, phrases: Sequence[str]) -> bool:
    return any(contains_phrase(text, phrase) for phrase in phrases)


def _straight_bucket(leg_opening: str) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return ""
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def _map_jean_keywords(text: str, leg_opening: str, include_mom: bool) -> str:
    text = (text or "").lower()
    if not text:
        return ""
    straight_bucket = _straight_bucket(leg_opening)
    taper_words = ("taper", "tapering", "tapered")
    if include_mom:
        taper_words = (*taper_words, "mom")
    rules = [
        (("barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe"), "Barrel"),
        (taper_words, "Tapered"),
        (("baggy",), "Baggy"),
        (("flare",), "Flare"),
        (("bootcut", "boot"), "Bootcut"),
        (("skinny",), "Skinny"),
        (("wide leg", "wide-leg", "palazzo"), "Wide Leg"),
        (("cigarette", "slim"), "Straight from Knee"),
    ]
    for phrases, output in rules:
        if text_has_any(text, phrases):
            return output
    if contains_phrase(text, "straight"):
        return straight_bucket
    return ""


def derive_jean_style(style_name: str, title: str, description: str, leg_opening: str) -> str:
    # Step 1: Style Name
    mapped = _map_jean_keywords(style_name, leg_opening, include_mom=True)
    if mapped:
        return mapped
    # Step 3: Description
    mapped = _map_jean_keywords(description, leg_opening, include_mom=False)
    if mapped:
        return mapped
    return ""


def derive_jean_style_from_fit(fit_hint: str, tags: str, leg_opening: str) -> str:
    # Step 5 fallback only
    mapped = _map_jean_keywords(fit_hint, leg_opening, include_mom=False)
    if mapped:
        return mapped
    if text_has_any((tags or "").lower(), ("baggy",)):
        return "Baggy"
    return ""


def derive_inseam_label(title: str, description: str, jean_style: str, inseam: str) -> str:
    title_l = title.lower()
    if "petite" in title_l:
        return "Petite"
    if "longer" in description.lower():
        return "Long"
    inseam_value = to_float(inseam)
    if inseam_value is None:
        return "Regular"
    if jean_style in {"Barrel", "Bootcut", "Flare", "Wide Leg", "Straight from Thigh", "Baggy", "Straight from Knee/Thigh"} and inseam_value >= 33:
        return "Long"
    if jean_style in {"Skinny", "Tapered", "Straight from Knee"} and inseam_value >= 30:
        return "Long"
    return "Regular"


def derive_inseam_style(jean_style: str, inseam_label: str, inseam: str, length_hint: str = "") -> str:
    val = to_float(inseam)
    label = inseam_label or "Regular"
    wide_group = {"Barrel", "Bootcut", "Flare", "Wide Leg", "Straight from Thigh", "Baggy", "Straight from Knee/Thigh"}
    skinny_group = {"Skinny", "Tapered", "Straight from Knee"}
    if val is not None:
        if jean_style in wide_group:
            if label == "Petite":
                return "Cropped" if val <= 25 else ("Ankle" if val <= 28 else "Full Length")
            if label == "Regular":
                return "Cropped" if val <= 28 else ("Ankle" if val < 31 else "Full Length")
            if label == "Long" and val >= 33:
                return "Full Length"
        if jean_style in skinny_group:
            if label == "Petite":
                return "Cropped" if val < 25 else "Full Length"
            if label == "Regular":
                return "Cropped" if val < 27 else ("Full Length" if val < 30 else "Full Length")
            if label == "Long" and val >= 30:
                return "Full Length"
    hint = (length_hint or "").strip()
    if hint:
        hint_lower = hint.lower()
        if jean_style in skinny_group and hint_lower == "ankle":
            return "Full Length"
        if jean_style in wide_group and hint_lower == "extra long":
            return "Full Length"
        if jean_style in wide_group:
            return hint
    return ""


def derive_rise_label(title: str, handle: str, description: str, rise_hint: str = "") -> str:
    t = f"{title} {handle} {description} {rise_hint}".lower()
    if any(k in t for k in ("super low rise", "ultra low rise", "super low waist", "ultra low waist")):
        return "Ultra Low"
    if any(k in t for k in ("super high rise", "ultra high rise", "super high waist", "ultra high waist")):
        return "Ultra High"
    if any(k in t for k in ("mid-rise", "mid rise")):
        return "Mid"
    if any(k in t for k in ("low-rise", "low rise", " low waist")):
        return "Low"
    if any(k in t for k in ("high-rise", "high rise", " high waist")):
        return "High"
    # Handle bare Algolia rise_hint values like "Low" or "High"
    hint_stripped = (rise_hint or "").strip().lower()
    if hint_stripped == "low":
        return "Low"
    if hint_stripped == "high":
        return "High"
    if hint_stripped == "mid":
        return "Mid"
    if hint_stripped in ("ultra low", "super low"):
        return "Ultra Low"
    if hint_stripped in ("ultra high", "super high"):
        return "Ultra High"
    return ""


def derive_hem_style(description: str) -> str:
    d = description.lower()
    if any(k in d for k in ("split hem", "side slits", "side-slits", "slit inseams at the hem", "slit at the hem", "slit hem")):
        return "Split Hem"
    if any(k in d for k in ("released hem", "undone finished hem", "undone hem", "released-hem")):
        return "Released Hem"
    if any(k in d for k in ("raw hem", "raw-edge hem", "raw edge hem", "raw-hem")):
        return "Raw Hem"
    if any(k in d for k in ("wide hem", "wide-hem", "trouser hem")):
        return "Wide Hem"
    if any(k in d for k in ("distressed hem", "distressed-hem", "destroyed hem", "destructed hem")):
        return "Distressed Hem"
    if "zippers at the hem" in d:
        return "Zipper Hem"
    if any(k in d for k in ("clean hem", "finished hem", "clean-edge hem", "clean finished hem", "clean-finished hem")):
        return "Clean Hem"
    return ""


def derive_color_standardized(color: str, description: str, color_hint: str = "") -> str:
    c = color.lower()
    d = description.lower()
    hint = color_hint.lower()
    mapping = [
        (("animal print", "leopard", "snake", "camo"), "Animal Print"),
        (("blue", "navy", "indigo"), "Blue"),
        (("green", "moss", "olive", "sage"), "Green"),
        (("grey", "gray", "smoke"), "Grey"),
        (("orange",), "Orange"),
        (("pink",), "Pink"),
        (("purple", "violet", "maroon"), "Purple"),
        (("red", "wine", "burgundy", "oxblood"), "Red"),
        (("tan", "beige", "khaki", "light taupe"), "Tan"),
        (("white", "ecru", "egret", "cream", "bleach"), "White"),
        (("yellow",), "Yellow"),
        (("black", "onyx", "noir", "raven"), "Black"),
        (("brown", "cinnamon", "coffee", "espresso"), "Brown"),
    ]
    for keys, out in mapping:
        if text_has_any(c, keys):
            return out
    for keys, out in mapping:
        if text_has_any(d, keys):
            return out
    if text_has_any(
        d,
        (
            "dark base",
            "acid wash",
            "dark rinse",
            "dark stretch denim",
            "dark washed",
            "medium-dark",
            "medium-light",
            "dark vintage wash",
            "dark wash",
            "deep wash",
            "light vintage wash",
            "light vintage-inspired wash",
            "light wash",
            "light/medium wash",
            "light-to-medium wash",
            "medium wash",
            "medium/dark soft vintage wash",
            "medium/dark wash",
        ),
    ):
        return "Blue"
    for keys, out in mapping:
        if text_has_any(hint, keys):
            return out
    return ""


def derive_color_simplified(color: str, description: str, standardized: str, wash_hint: str = "") -> str:
    s = standardized.lower()
    c = color.lower()
    d = description.lower()
    if "black" in s or "brown" in s:
        return "Dark"
    if "white" in s or "tan" in s:
        return "Light"
    if text_has_any(c, ("wine", "burgundy", "navy", "dark", "deep")):
        return "Dark"
    if text_has_any(c, ("pastel", "cream", "light")):
        return "Light"
    if text_has_any(c, ("medium", "mid")):
        return "Medium"
    if text_has_any(d, ("medium light", "light medium", "light to medium", "medium to light", "light-to-medium", "medium-to-light", "medium-light", "light-medium", "light/medium", "medium/light")):
        return "Light to Medium"
    if text_has_any(d, ("medium dark", "dark medium", "dark to medium", "medium to dark", "dark-to-medium", "medium-to-dark", "medium-dark", "dark-medium", "dark/medium", "medium/dark")):
        return "Medium to Dark"
    if text_has_any(d, ("dark", "deep", "black", "wine", "burgundy", "midnight blue", "forest green", "navy")):
        return "Dark"
    if text_has_any(d, ("light blue", "pale blue", "powdery blue", "powder blue", "light vintage", "soft blue", "soft pink", "ecru", "white", "acid wash", "acid-wash", "light", "khaki", "tan", "ivory")):
        return "Light"
    if text_has_any(d, ("mid blue", "mid-blue", "medium stone wash", "classic stone washed blue", "vintage washed blue", "classic vintage blue", "medium blue", "medium wash", "classic blue")):
        return "Medium"
    return wash_hint


def normalize_text(tag: Tag) -> str:
    text = " ".join(part.strip() for part in tag.stripped_strings if part.strip())
    return re.sub(r"\s+", " ", text).strip()


def strip_html_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    return normalize_text(BeautifulSoup(raw_html, "html.parser"))


def dedupe_description_parts(parts: Sequence[str]) -> List[str]:
    deduped: List[str] = []
    seen: set[str] = set()
    for part in parts:
        cleaned = re.sub(r"\s+", " ", (part or "")).strip()
        if not cleaned:
            continue
        key = cleaned.lower().replace("’", "'").replace("“", '"').replace("”", '"')
        if key in seen:
            continue
        seen.add(key)
        deduped.append(cleaned)
    return deduped


def extract_pdp_description(html_text: str) -> str:
    soup = BeautifulSoup(html_text, "html.parser")
    matches = [node for node in soup.select(PDP_SELECTOR) if isinstance(node, Tag)]
    values = [normalize_text(node) for node in matches if normalize_text(node)]
    if values:
        return ", ".join(values)

    parent = soup.select_one(PDP_PARENT_SELECTOR)
    if parent is not None:
        values = [normalize_text(node) for node in parent.find_all("li") if normalize_text(node)]
        if values:
            return ", ".join(values)

    keyword_hits: List[str] = []
    for node in soup.find_all(["li", "p", "span"]):
        node_text = normalize_text(node)
        node_lower = node_text.lower()
        if any(token in node_lower for token in ("front rise", "rise", "inseam", "leg opening", "stretch")):
            keyword_hits.append(node_text)
    if keyword_hits:
        return ", ".join(dict.fromkeys(keyword_hits))

    return ""


def extract_next_data_description(html_text: str) -> str:
    """Extract product description from Next.js __NEXT_DATA__ JSON embedded in the page.

    Paige's headless Shopify + Next.js site embeds server-rendered product data in a
    <script id="__NEXT_DATA__"> tag.  This data often includes the full product
    description (body_html / descriptionHtml) with measurement details that are not
    available in the Shopify JSON endpoint or in the rendered static HTML elements.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script or not script.string:
        return ""
    try:
        data = json.loads(script.string)
    except (json.JSONDecodeError, ValueError):
        return ""

    measurement_keywords = ("front rise", "inseam", "leg opening", "stretch")
    candidates: List[str] = []

    def _search(obj: object, depth: int = 0) -> None:
        if depth > 20:
            return
        if isinstance(obj, str) and len(obj) < 8000:
            text = strip_html_text(obj) if "<" in obj else obj
            if any(k in text.lower() for k in measurement_keywords):
                candidates.append(text)
        elif isinstance(obj, dict):
            for v in obj.values():
                _search(v, depth + 1)
        elif isinstance(obj, list):
            for item in obj:
                _search(item, depth + 1)

    _search(data)
    if candidates:
        return ", ".join(dict.fromkeys(c for c in candidates if c))
    return ""


def parse_mixed_fraction(raw_value: str) -> Optional[float]:
    value = raw_value.strip().replace("″", "").replace('"', "")
    if not value:
        return None
    mixed = re.fullmatch(r"(\d+)\s+(\d+)/(\d+)", value)
    if mixed:
        whole = float(mixed.group(1))
        numerator = float(mixed.group(2))
        denominator = float(mixed.group(3))
        if denominator == 0:
            return None
        return whole + (numerator / denominator)
    fraction = re.fullmatch(r"(\d+)/(\d+)", value)
    if fraction:
        numerator = float(fraction.group(1))
        denominator = float(fraction.group(2))
        if denominator == 0:
            return None
        return numerator / denominator
    try:
        return float(value)
    except ValueError:
        return None


def extract_measurement(text: str, labels: Sequence[str]) -> str:
    fraction_map = {
        "¼": " 1/4",
        "½": " 1/2",
        "¾": " 3/4",
        "⅛": " 1/8",
        "⅜": " 3/8",
        "⅝": " 5/8",
        "⅞": " 7/8",
    }
    norm_text = text
    for symbol, replacement in fraction_map.items():
        norm_text = norm_text.replace(symbol, replacement)
    number_pattern = r"([0-9]+(?:\s+[0-9]+/[0-9]+|/[0-9]+|\.[0-9]+)?)"
    for label in labels:
        patterns = [
            rf"{label}\s*:\s*{number_pattern}",
            rf"{label}\s*[-]?\s*{number_pattern}\s*(?:in|inch|inches|\"|”)?",
            rf"{number_pattern}\s*(?:in|inch|inches|\"|”)?\s*{label}",
        ]
        for pattern in patterns:
            match = re.search(pattern, norm_text, flags=re.IGNORECASE)
            if not match:
                continue
            value = parse_mixed_fraction(match.group(1))
            if value is None:
                continue
            return format_decimal(value)
    return ""


def format_decimal(value: Optional[float]) -> str:
    if value is None:
        return ""
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return text


def extract_label_text(text: str, labels: Sequence[str]) -> str:
    for label in labels:
        match = re.search(rf"{label}\s*:\s*([^,]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def extract_stretch_from_html(html_text: str) -> str:
    """Parse the stretch-scale widget from static HTML.

    The widget renders as a parent container holding an odd number of child divs
    where every other child (positions 1, 3, 5, 7, 9) is a visible "dot" and the
    active dot carries a class that includes the word "active".  We locate the
    active dot by scanning all divs that contain both "dot" and "active" in their
    combined class string, then count its 1-based position among all dot siblings.
    """
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")

    # Find the active dot: any div whose class string contains both 'dot' and 'active'.
    active_dot = None
    for div in soup.find_all("div"):
        cls = " ".join(div.get("class", [])).lower()
        if "dot" in cls and "active" in cls:
            active_dot = div
            break

    if active_dot is None:
        return ""

    dot_parent = active_dot.parent
    if dot_parent is None:
        return ""

    # Collect all direct-child divs whose class includes "dot".
    dot_siblings = [
        d for d in dot_parent.find_all("div", recursive=False)
        if "dot" in " ".join(d.get("class", [])).lower()
    ]

    # dot_siblings contains only the 5 actual dot elements (connectors/spacers filtered
    # out). The active dot's 1-based position within this filtered list maps directly:
    #   1 → High Stretch, 2 → Medium to High Stretch, 3 → Medium Stretch,
    #   4 → Low Stretch,  5 → Rigid
    mapping = {
        1: "High Stretch",
        2: "Medium to High Stretch",
        3: "Medium Stretch",
        4: "Low Stretch",
        5: "Rigid",
    }
    for idx, child in enumerate(dot_siblings, start=1):
        if child is active_dot:
            return mapping.get(idx, "")
    return ""


def normalize_stretch_value(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    # Keep mapping strict to explicit stretch labels only (no free-text inference).
    normalized = re.sub(r"\s+", " ", raw).strip().lower()
    exact_map = {
        "high stretch": "High Stretch",
        "medium to high stretch": "Medium to High Stretch",
        "medium stretch": "Medium Stretch",
        "low stretch": "Low Stretch",
        "rigid": "Rigid",
    }
    return exact_map.get(normalized, raw)


def derive_stretch_from_algolia_attrs(denim_fabric: str, stretch_attr: str) -> str:
    """Fallback: infer Stretch from Algolia product.meta.attributes fields.

    Rules (in priority order):
      denimFabric=Transcend Vintage + stretch=High Stretch   → High Stretch
      denimFabric=Transcend Vintage + stretch=Medium Stretch → Medium to High Stretch
      denimFabric=Transcend        + stretch=High Stretch    → High Stretch
      denimFabric=Heritage         + stretch=Comfort Stretch → Medium Stretch
      denimFabric=Rigid            + stretch=Rigid           → Rigid
      denimFabric=Heritage         + stretch=Semi Rigid      → Low Stretch

    Comparison is case-insensitive to handle Algolia returning values in any casing.
    """
    fabric = (denim_fabric or "").strip().lower()
    stretch = (stretch_attr or "").strip().lower()
    rules = [
        ("transcend vintage", "high stretch",    "High Stretch"),
        ("transcend vintage", "medium stretch",  "Medium to High Stretch"),
        ("transcend",         "high stretch",    "High Stretch"),
        ("heritage",          "comfort stretch", "Medium Stretch"),
        ("rigid",             "rigid",           "Rigid"),
        ("heritage",          "semi rigid",      "Low Stretch"),
    ]
    for fab, str_val, output in rules:
        if fabric == fab and stretch == str_val:
            return output
    return ""


class PDPBrowserExtractor:
    """Browser-driven PDP extractor for HeadlessUI DETAILS panels.

    Uses a single shared Chromium process with a fresh browser context created
    for every fetch() call.  This prevents a dead/blocked context from cascading
    into silent failures for all subsequent handles — the previous design reused
    one thread-local page, so a TargetClosedError on handle N caused handles
    N+1…end to fail silently because _thread_ensure() saw page is not None and
    returned True without noticing the page was already dead.
    """

    _LAUNCH_ARGS = [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",       # required: Azure App Service cannot nest sandboxes
        "--disable-gpu",      # no GPU on server
        "--disable-dev-shm-usage",
    ]
    _BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}

    def __init__(self) -> None:
        self.enabled = os.getenv("PAIGE_PDP_BROWSER_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
        self.headless = os.getenv("PAIGE_PDP_HEADLESS", "1").strip().lower() not in {"0", "false", "no"}
        self._workers: int = max(1, int(os.getenv("PAIGE_PDP_BROWSER_WORKERS", "2")))
        self._playwright_importable: Optional[bool] = None
        self._import_lock = threading.Lock()
        # Single shared browser (thread-safe: Playwright allows concurrent contexts).
        self._pw = None
        self._browser = None
        self._browser_lock = threading.Lock()
        self._init_failed = False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _playwright_available(self) -> bool:
        if self._playwright_importable is not None:
            return self._playwright_importable
        with self._import_lock:
            if self._playwright_importable is not None:
                return self._playwright_importable
            try:
                import playwright  # noqa: F401
                self._playwright_importable = True
            except Exception as exc:
                log(f"Playwright import failed: {exc}")
                self._playwright_importable = False
        return self._playwright_importable

    def _ensure_browser(self) -> bool:
        """Launch the shared Chromium browser if not already running."""
        if not self.enabled or not self._playwright_available():
            return False
        if self._init_failed:
            return False
        if self._browser is not None:
            return True
        with self._browser_lock:
            if self._browser is not None:
                return True
            if self._init_failed:
                return False
            try:
                from playwright.sync_api import sync_playwright
                self._pw = sync_playwright().start()
                self._browser = self._pw.chromium.launch(
                    headless=self.headless,
                    args=self._LAUNCH_ARGS,
                )
                log(f"Playwright Chromium launched (headless={self.headless})")
                return True
            except Exception as exc:
                log(f"Playwright launch failed: {exc}")
                self._init_failed = True
                return False

    @staticmethod
    def _dismiss_overlays(page: object) -> None:
        try:
            page.evaluate(  # type: ignore[attr-defined]
                """() => {
                  document.querySelectorAll(
                    '#attentive_overlay, iframe#attentive_creative,'
                    ' [id*="attentive_overlay"], [data-testid*="attentive"]'
                  ).forEach(el => { try { el.remove(); } catch(e) {} });
                }"""
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self, handle: str) -> Dict[str, str]:
        _empty: Dict[str, str] = {"details": "", "stretch": "", "description": ""}
        if not self._ensure_browser():
            log(f"PDP browser {handle} | SKIP: browser unavailable")
            return _empty

        url = f"{PDP_HOST}/products/{handle}"
        start = time.time()
        _page_timeout_s = float(os.getenv("PAIGE_PDP_FETCH_TIMEOUT_S", "0") or "0")

        # Fresh context per fetch: clears cookies/state so a blocked prior page
        # cannot cascade into failures here.
        ctx = None
        try:
            ctx = self._browser.new_context(  # type: ignore[union-attr]
                ignore_https_errors=True,
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
                locale="en-US",
                viewport={"width": 1366, "height": 768},
            )
            ctx.route(
                "**/*",
                lambda route: route.abort()
                if route.request.resource_type in self._BLOCKED_TYPES
                else route.continue_(),
            )
            page = ctx.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            )
        except Exception as exc:
            log(f"PDP browser {handle} | context error: {type(exc).__name__}: {str(exc)[:80]}")
            if ctx:
                try:
                    ctx.close()
                except Exception:
                    pass
            return _empty

        try:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=25000)
            except Exception as exc:
                log(f"PDP browser {handle} | goto FAIL ({type(exc).__name__}): {str(exc)[:100]}")
                return _empty

            if _page_timeout_s > 0 and (time.time() - start) > _page_timeout_s:
                log(f"PDP browser {handle} | timeout after goto ({time.time()-start:.1f}s)")
                return _empty

            # Detect bot-check / access-denied pages early.
            try:
                page_title = (page.title() or "").lower()
                if "checkpoint" in page_title or "access denied" in page_title or "blocked" in page_title:
                    log(f"PDP browser {handle} | blocked page detected: {page_title!r}")
                    return _empty
            except Exception:
                pass

            self._dismiss_overlays(page)

            # Wait for React to hydrate before interacting with HeadlessUI.
            # HeadlessUI sets data-headlessui-state on its buttons only after hydration.
            try:
                page.wait_for_selector(
                    "[data-headlessui-state]", timeout=7000, state="attached"
                )
            except Exception:
                pass  # Continue anyway — page may not use HeadlessUI

            if _page_timeout_s > 0 and (time.time() - start) > _page_timeout_s:
                log(f"PDP browser {handle} | timeout after hydration wait")
                return _empty

            details_text = ""
            try:
                btn = page.get_by_role("button", name=re.compile("DETAILS", re.IGNORECASE))
                if btn.count() == 0:
                    btn = page.locator("button:has-text('DETAILS')")
                if btn.count():
                    try:
                        expanded = btn.first.get_attribute("aria-expanded", timeout=500)
                    except Exception:
                        expanded = None
                    if expanded != "true":
                        try:
                            btn.first.click(timeout=1500)
                        except Exception:
                            try:
                                btn.first.click(timeout=1500, force=True)
                            except Exception:
                                pass
                    # Wait for the HeadlessUI panel to become visible after the click.
                    try:
                        page.locator(
                            "[id^='headlessui-disclosure-panel-']"
                        ).first.wait_for(state="visible", timeout=3000)
                    except Exception:
                        page.wait_for_timeout(300)

                # Primary extraction: JavaScript search across the full DOM.
                # Works for HeadlessUI v1 (panel in DOM after click) and v2 (always in DOM).
                try:
                    js_result = page.evaluate(
                        """() => {
                            var keys = ['front rise', 'inseam', 'leg opening', 'stretch'];
                            var found = [];
                            document.querySelectorAll('li, dd, td').forEach(function(el) {
                                var t = (el.textContent || '').trim();
                                var tl = t.toLowerCase();
                                if (t.length > 3 && t.length < 200
                                        && keys.some(function(k) { return tl.indexOf(k) !== -1; })) {
                                    found.push(t.replace(/\\s+/g, ' '));
                                }
                            });
                            var seen = {};
                            return found.filter(function(x) {
                                if (seen[x]) return false;
                                seen[x] = true;
                                return true;
                            }).join(', ');
                        }"""
                    ) or ""
                    if js_result:
                        details_text = js_result
                except Exception:
                    pass

                # Fallback: strict CSS selector on HeadlessUI panel children.
                if not details_text:
                    detail_nodes = page.locator(PDP_SELECTOR)
                    count = detail_nodes.count()
                    if count:
                        items = []
                        for i in range(count):
                            try:
                                text = detail_nodes.nth(i).inner_text(timeout=600).strip()
                            except Exception:
                                continue
                            if text:
                                items.append(re.sub(r"\s+", " ", text))
                        details_text = ", ".join(items)

                # Second fallback: read the entire panel element.
                if not details_text:
                    panel = page.locator("[id^='headlessui-disclosure-panel-']").first
                    if panel.count():
                        try:
                            raw = panel.inner_text(timeout=1500).strip()
                            if raw:
                                details_text = re.sub(r"\s+", " ", raw)
                        except Exception:
                            pass
            except Exception:
                details_text = ""

            stretch = ""
            try:
                idx = page.evaluate(
                    """() => {
                      const allDivs = Array.from(document.querySelectorAll('div'));
                      const activeDot = allDivs.find(el => {
                        const cls = (el.className || '').toString().toLowerCase();
                        return cls.includes('dot') && cls.includes('active');
                      });
                      if (!activeDot) return null;
                      const parent = activeDot.parentElement;
                      if (!parent) return null;
                      const dotSiblings = Array.from(parent.children).filter(node => {
                        const cls = (node.className || '').toString().toLowerCase();
                        return cls.includes('dot');
                      });
                      const pos = dotSiblings.indexOf(activeDot) + 1;
                      return pos > 0 ? pos : null;
                    }"""
                )
                if idx is not None:
                    stretch = {
                        1: "High Stretch",
                        2: "Medium to High Stretch",
                        3: "Medium Stretch",
                        4: "Low Stretch",
                        5: "Rigid",
                    }.get(int(idx), "")
            except Exception:
                stretch = ""

            description_text = ""
            try:
                desc_node = page.locator(
                    "div[class*='productDescription'], div[class*='description'] p"
                ).first
                if desc_node.count():
                    description_text = re.sub(
                        r"\s+", " ", desc_node.inner_text(timeout=600)
                    ).strip()
            except Exception:
                description_text = ""

            elapsed = time.time() - start
            log(f"PDP browser {handle} | {elapsed:.2f}s | details={'yes' if details_text else 'no'}")
            return {"details": details_text, "stretch": stretch, "description": description_text}

        finally:
            try:
                ctx.close()
            except Exception:
                pass

    def close(self) -> None:
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass


class PaigeScraper:
    def __init__(self) -> None:
        ensure_directories()
        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                " AppleWebKit/537.36 (KHTML, like Gecko)"
                " Chrome/124.0.0.0 Safari/537.36",
            }
        )
        self._pdp_cache: Dict[str, Dict[str, str]] = {}
        self.browser_extractor = PDPBrowserExtractor()

    def _prefetch_http_concurrent(self, handles: List[str], seed_by_handle: Dict[str, str]) -> None:
        """Pre-fetch tier 1+2 (HTTP only, no browser) for all handles concurrently."""
        todo = [h for h in handles if h and h not in self._pdp_cache]
        if not todo:
            return
        # 4 workers: aggressive enough to be fast, conservative enough to avoid
        # triggering Cloudflare rate-limiting that would then block the browser phase.
        workers = max(1, int(os.getenv("PAIGE_PDP_HTTP_WORKERS", "4")))
        log(f"Pre-fetching {len(todo)} PDP handles via HTTP ({workers} workers)…")
        start = time.time()

        _was_enabled = self.browser_extractor.enabled
        self.browser_extractor.enabled = False

        def _fetch_one(handle: str) -> None:
            try:
                self.fetch_pdp_fields(handle, seed_by_handle.get(handle, ""))
            except Exception as exc:
                log(f"HTTP pre-fetch error for {handle}: {exc}")

        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(_fetch_one, todo))
        finally:
            self.browser_extractor.enabled = _was_enabled

        elapsed = time.time() - start
        cached = sum(1 for h in todo if h in self._pdp_cache)
        log(f"HTTP pre-fetch complete: {cached}/{len(todo)} handles cached in {elapsed:.1f}s")

    def _prefetch_browser_concurrent(self, handles: List[str]) -> None:
        """Pre-fetch browser data for handles missing measurements, then merge into pdp_cache.

        Results are merged immediately after all fetches complete so the main loop
        reads complete (HTTP + browser) data from _pdp_cache without re-fetching.
        """
        if not self.browser_extractor.enabled:
            log("Browser pre-fetch skipped: PAIGE_PDP_BROWSER_ENABLED is off")
            return
        todo = [h for h in handles if h]
        if not todo:
            log("Browser pre-fetch skipped: all handles already have measurements")
            return
        workers = self.browser_extractor._workers
        playwright_ok = self.browser_extractor._playwright_available()
        log(f"Browser pre-fetch starting: {len(todo)} handles, {workers} workers, playwright_available={playwright_ok}")
        if not playwright_ok:
            log("Browser pre-fetch aborted: Playwright not importable — run: pip install playwright && playwright install chromium")
            return
        log(f"Pre-fetching {len(todo)} PDP handles via browser ({workers} workers)…")
        start = time.time()

        def _fetch_one(handle: str) -> Tuple[str, Dict[str, str]]:
            try:
                return handle, self.browser_extractor.fetch(handle)
            except Exception as exc:
                log(f"Pre-fetch browser error for {handle}: {exc}")
                return handle, {}

        with ThreadPoolExecutor(max_workers=workers) as pool:
            results: List[Tuple[str, Dict[str, str]]] = list(pool.map(_fetch_one, todo))

        elapsed = time.time() - start
        per_page = elapsed / len(todo) if todo else 0.0
        log(f"Browser pre-fetch complete: {len(todo)} handles in {elapsed:.1f}s ({per_page:.2f}s/page)")

        # Merge browser results into pdp_cache (single-threaded, no race conditions).
        merged = 0
        for handle, browser_data in results:
            cached = self._pdp_cache.get(handle)
            if cached is None:
                continue
            browser_details = browser_data.get("details", "")
            browser_desc = browser_data.get("description", "")
            browser_stretch = browser_data.get("stretch", "")
            if not (browser_details or browser_desc or browser_stretch):
                continue
            parts: List[str] = []
            if browser_desc:
                parts.append(browser_desc)
            existing_desc = cached.get("description", "")
            if existing_desc:
                parts.append(existing_desc)
            if browser_details:
                parts.append(browser_details)
            description = ", ".join(dedupe_description_parts(parts))
            rise = extract_measurement(description, ["Front Rise", "Rise"]) or cached.get("rise", "")
            inseam = extract_measurement(description, ["Inseam", "Inleg"]) or cached.get("inseam", "")
            leg_opening = extract_measurement(description, ["Leg Opening", "Opening"]) or cached.get("leg_opening", "")
            stretch = (
                (normalize_stretch_value(browser_stretch) if browser_stretch else "")
                or cached.get("stretch", "")
                or normalize_stretch_value(extract_label_text(description, ["Stretch"]))
            )
            cached.update({
                "description": description,
                "rise": rise,
                "inseam": inseam,
                "leg_opening": leg_opening,
                "stretch": stretch,
            })
            if rise or leg_opening:
                merged += 1
        if merged:
            log(f"Applied browser measurements to {merged} handles in pdp_cache")

    def graphql_request(self, query: str, variables: Dict) -> Dict:
        response = self.session.post(
            GRAPHQL_URL,
            headers={
                "X-Shopify-Storefront-Access-Token": GRAPHQL_TOKEN,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables},
            timeout=45,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(f"GraphQL errors: {payload['errors']}")
        return payload["data"]

    def fetch_graphql_products(self) -> List[Dict]:
        query = """
        query CollectionProducts($handle: String!, $cursor: String) {
          collection(handle: $handle) {
            products(first: 250, after: $cursor) {
              pageInfo { hasNextPage endCursor }
              nodes {
                id
                handle
                title
                description
                createdAt
                publishedAt
                productType
                tags
                vendor
                onlineStoreUrl
                totalInventory
                featuredImage { url }
                variants(first: 250) {
                  nodes {
                    id
                    title
                    availableForSale
                    quantityAvailable
                    price { amount }
                    compareAtPrice { amount }
                    barcode
                    sku
                    selectedOptions { name value }
                  }
                }
              }
            }
          }
        }
        """
        merged: Dict[str, Dict] = {}
        for collection_handle in ("women-denim", "women-sale"):
            cursor = None
            while True:
                data = self.graphql_request(query, {"handle": collection_handle, "cursor": cursor})
                collection = data.get("collection")
                if not collection:
                    break
                block = collection["products"]
                for product in block["nodes"]:
                    product["_source_collection"] = collection_handle
                    existing = merged.get(product["handle"])
                    if existing is None:
                        merged[product["handle"]] = product
                    else:
                        existing["_source_collection"] = "women-denim" if (
                            "women-denim" in (existing.get("_source_collection"), collection_handle)
                        ) else existing.get("_source_collection")
                if not block["pageInfo"]["hasNextPage"]:
                    break
                cursor = block["pageInfo"]["endCursor"]
        return list(merged.values())

    def fetch_collection_handles_json(self) -> set[str]:
        handles: set[str] = set()
        for collection in ("women-denim", "women-sale"):
            page = 1
            while True:
                url = f"https://shop.paige.com/collections/{collection}/products.json?limit=250&page={page}"
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                products = (response.json() or {}).get("products") or []
                if not products:
                    break
                for product in products:
                    handle = (product.get("handle") or "").strip()
                    if handle:
                        handles.add(handle)
                page += 1
        return handles

    def algolia_request(self, params: Dict[str, str]) -> Dict:
        query_string = "&".join(
            f"{key}={requests.utils.quote(str(value))}" for key, value in params.items()
        )
        payload = {"params": query_string}
        response = self.session.post(
            ALGOLIA_SEARCH_URL,
            headers={
                "X-Algolia-Application-Id": ALGOLIA_APP_ID,
                "X-Algolia-API-Key": ALGOLIA_API_KEY,
                "Content-Type": "application/json",
            },
            data=json.dumps(payload),
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def fetch_styles(self) -> List[Dict]:
        styles: List[Dict] = []
        page = 0
        while True:
            response = self.algolia_request(
                {
                    "filters": "(collections:women-denim OR collections:women-sale)",
                    "distinct": "true",
                    "hitsPerPage": 1000,
                    "page": page,
                }
            )
            hits = response.get("hits", [])
            styles.extend(hits)
            nb_pages = response.get("nbPages", 0)
            page += 1
            if page >= nb_pages:
                break
        return styles

    def fetch_variants(self, style_id: str) -> List[Dict]:
        variants: List[Dict] = []
        page = 0
        while True:
            response = self.algolia_request(
                {
                    "filters": f"id={style_id} AND (collections:women-denim OR collections:women-sale)",
                    "distinct": "false",
                    "hitsPerPage": 1000,
                    "page": page,
                }
            )
            hits = response.get("hits", [])
            if not hits:
                break
            variants.extend(hits)
            nb_pages = response.get("nbPages", 0)
            page += 1
            if page >= nb_pages:
                break
        return variants

    def fetch_algolia_variant_map(self) -> Dict[str, Dict]:
        by_id: Dict[str, Dict] = {}
        total_hits = 0
        for style in self.fetch_styles():
            style_id = str(style.get("id") or "")
            if not style_id:
                continue
            for hit in self.fetch_variants(style_id):
                total_hits += 1
                keys = [
                    str(hit.get("objectID") or ""),
                    str(hit.get("id") or ""),
                    str(hit.get("sku") or ""),
                ]
                for key in keys:
                    if key:
                        by_id[key] = hit
        log(f"Loaded {total_hits} Algolia variant hits via per-style pagination")
        return by_id

    def fetch_pdp_fields(self, handle: str, description_seed: str = "") -> Dict[str, str]:
        if handle in self._pdp_cache:
            return self._pdp_cache[handle]
        source = "seed"
        description_parts: List[str] = [description_seed] if description_seed else []
        description = ", ".join(dedupe_description_parts(description_parts))
        stretch = normalize_stretch_value(extract_label_text(description, ["Stretch"]))

        try:
            product_json = self.session.get(
                urljoin(PDP_HOST, f"/products/{handle}.json"),
                timeout=20,
                allow_redirects=True,
            )
            if product_json.status_code == 200:
                body_html = (product_json.json().get("product") or {}).get("body_html") or ""
                body_text = strip_html_text(body_html)
                if body_text:
                    description_parts.append(body_text)
                    source = "product-json"
        except Exception:
            pass

        description = ", ".join(dedupe_description_parts(description_parts))
        rise = extract_measurement(description, ["Front Rise", "Rise"])
        inseam = extract_measurement(description, ["Inseam", "Inleg"])
        leg_opening = extract_measurement(description, ["Leg Opening", "Opening"])

        if not (rise and inseam and leg_opening):
            url = urljoin(PDP_HOST, f"/products/{handle}")
            try:
                response = self.session.get(url, timeout=20, allow_redirects=True)
                response.raise_for_status()
                details_description = extract_pdp_description(response.text)
                next_data_desc = extract_next_data_description(response.text)
                stretch_from_html = extract_stretch_from_html(response.text)
                if stretch_from_html:
                    stretch = normalize_stretch_value(stretch_from_html)
                added = False
                if details_description:
                    description_parts.append(details_description)
                    added = True
                if next_data_desc:
                    description_parts.append(next_data_desc)
                    added = True
                if added:
                    description = ", ".join(dedupe_description_parts(description_parts))
                    rise = extract_measurement(description, ["Front Rise", "Rise"])
                    inseam = extract_measurement(description, ["Inseam", "Inleg"])
                    leg_opening = extract_measurement(description, ["Leg Opening", "Opening"])
                    source = "pdp-html"
            except Exception:
                pass

        # Browser-driven fallback for Headless UI DETAILS disclosure.
        # Guard: skip when browser is intentionally disabled (e.g. HTTP pre-fetch
        # phase) to avoid generating hundreds of "SKIP: browser unavailable" log
        # entries that look like real failures.
        if not (rise and inseam and leg_opening) and self.browser_extractor.enabled:
            browser_data = self.browser_extractor.fetch(handle)
            browser_desc = browser_data.get("description", "")
            browser_details = browser_data.get("details", "")
            if browser_desc:
                description_parts.insert(0, browser_desc)
            if browser_details:
                description_parts.append(browser_details)
            if browser_data.get("stretch"):
                stretch = normalize_stretch_value(browser_data["stretch"])
            description = ", ".join(dedupe_description_parts(description_parts))
            rise = extract_measurement(description, ["Front Rise", "Rise"])
            inseam = extract_measurement(description, ["Inseam", "Inleg"])
            leg_opening = extract_measurement(description, ["Leg Opening", "Opening"])
            if browser_details:
                source = "pdp-browser"

        description = ", ".join(dedupe_description_parts(description_parts))

        result = {
            "description": description,
            "rise": rise,
            "inseam": inseam,
            "leg_opening": leg_opening,
            "stretch": stretch or normalize_stretch_value(extract_label_text(description, ["Stretch"])),
            "elapsed": "",
        }
        self._pdp_cache[handle] = result
        return result

    def build_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []
        fit_hint_by_sku: Dict[str, str] = {}
        tags_by_sku: Dict[str, str] = {}
        processed_handles = 0
        pdp_total_seconds = 0.0
        rise_filled = 0
        inseam_filled = 0
        leg_filled = 0
        algolia_style_map: Dict[str, Dict] = {}
        collection_handles = self.fetch_collection_handles_json()
        try:
            for style_hit in self.fetch_styles():
                handle = style_hit.get("handle")
                if handle and handle not in algolia_style_map:
                    algolia_style_map[handle] = style_hit
        except Exception:
            algolia_style_map = {}
        algolia_variant_map = self.fetch_algolia_variant_map()
        use_graphql = True
        try:
            styles = self.fetch_graphql_products()
            log(f"Fetched {len(styles)} styles from GraphQL collections women-denim + women-sale")
        except Exception as exc:
            log(f"GraphQL style pull failed ({exc}), falling back to Algolia")
            styles = self.fetch_styles()
            use_graphql = False

        # ── Concurrent pre-fetch: HTTP tier 1+2, then browser for gaps ──────
        _seen_pf: set = set()
        _prefetch_handles: List[str] = []
        for _s in styles:
            _h = _s.get("handle", "")
            if _h and _h in collection_handles and _h not in _seen_pf:
                _seen_pf.add(_h)
                _prefetch_handles.append(_h)

        # Build per-handle seed descriptions (mirrors logic in the main loop).
        _seed_by_handle: Dict[str, str] = {}
        for _s in styles:
            _h = _s.get("handle", "")
            if not _h:
                continue
            _sd = safe_string(_s.get("description", "")) if use_graphql else ""
            _as = algolia_style_map.get(_h, {})
            _ab = safe_string(_as.get("body_html_safe", ""))
            if _ab:
                _at = strip_html_text(_ab)
                _sd = ", ".join(dedupe_description_parts([_sd, _at]))
            _seed_by_handle[_h] = _sd

        # Phase 1: HTTP pre-fetch (tiers 1+2) — fills inseam for ~30% of handles.
        self._prefetch_http_concurrent(_prefetch_handles, _seed_by_handle)

        # Phase 2: browser pre-fetch for handles still missing rise or leg_opening.
        _browser_todo = [
            h for h in _prefetch_handles
            if not all(self._pdp_cache.get(h, {}).get(k) for k in ("rise", "inseam", "leg_opening"))
        ]
        self._prefetch_browser_concurrent(_browser_todo)
        # ─────────────────────────────────────────────────────────────────────

        for idx, style in enumerate(styles, start=1):
            if use_graphql:
                style_id = style.get("id", "").replace("gid://shopify/Product/", "")
                handle = style.get("handle", "")
                tags = style.get("tags", [])
                title = style.get("title", "")
                if handle not in collection_handles:
                    continue
                if not should_keep_product(tags, title, style.get("_source_collection", "")):
                    continue
                variants = (style.get("variants") or {}).get("nodes") or []
                meta_attrs: Dict[str, str] = {}
            else:
                style_id = str(style.get("id"))
                handle = style.get("handle", "")
                tags = style.get("tags", [])
                title = style.get("title", "")
                if handle not in collection_handles:
                    continue
                meta_attrs = ((style.get("meta") or {}).get("attributes") or {})
                variants = self.fetch_variants(style_id)
                if not should_keep_product(tags, title, "women-sale"):
                    continue
            if not handle:
                continue
            if not variants:
                log(f"No variants returned for style {style_id} ({handle})")
                continue

            style_algolia = algolia_style_map.get(handle, {})
            seed_description = safe_string(style.get("description", "")) if use_graphql else ""
            algolia_body_html = safe_string(style_algolia.get("body_html_safe", ""))
            if algolia_body_html:
                algolia_text = strip_html_text(algolia_body_html)
                seed_description = ", ".join(dedupe_description_parts([seed_description, algolia_text]))
            pdp_start = time.time()
            pdp_fields = self.fetch_pdp_fields(handle, seed_description)
            pdp_total_seconds += (time.time() - pdp_start)
            processed_handles += 1
            if pdp_fields["rise"]:
                rise_filled += 1
            if pdp_fields["inseam"]:
                inseam_filled += 1
            if pdp_fields["leg_opening"]:
                leg_filled += 1
            style_name = derive_style_name_base(title)
            product_type = derive_product_type(tags, title)
            country = extract_tag_value(tags, "country:")
            production_cost = extract_tag_value(tags, "productionCost:")
            site_exclusive = extract_tag_value(tags, "productType:")
            product_line = extract_tag_value(tags, "sizeType:") or meta_attrs.get("sizeType", "")
            fit_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("fit", "")
            length_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("length", "")
            rise_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("rise", "")
            wash_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("wash", "")
            color_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("colorCategory", "")
            algolia_denim_fabric = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("denimFabric", "")
            algolia_stretch_attr = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("stretch", "")
            inseam_hint = safe_string(((style_algolia.get("meta") or {}).get("attributes") or {}).get("inseam", ""))

            for variant in variants:
                raw_size = safe_string(
                    variant.get("option1")
                    or (variant.get("options") or {}).get("size", "")
                )
                if not raw_size and variant.get("selectedOptions"):
                    for option in variant["selectedOptions"]:
                        if (option.get("name") or "").lower() in {"size", "waist"}:
                            raw_size = safe_string(option.get("value"))
                size_value = raw_size.rstrip("Pp") if raw_size else ""

                sku_brand = safe_string(variant.get("sku"))
                sku_shopify = safe_string(variant.get("objectID") or variant.get("id"))
                variant_title = f"{title} - {size_value}".strip(" -")
                published_at = format_date(
                    variant.get("published_at") or style.get("publishedAt") or style.get("published_at")
                )
                created_at = format_date(style.get("createdAt") or style.get("created_at"))
                title_value = title
                color_value = ""
                if "-" in title_value:
                    color_value = title_value.split("-")[-1].strip()

                available_raw = variant.get("inventory_available")
                if available_raw is None:
                    available_raw = variant.get("availableForSale")
                available_bool = bool(available_raw)
                if isinstance(available_raw, str):
                    available_bool = available_raw.lower() == "true"

                inventory_quantity = int(
                    variant.get("inventory_quantity")
                    or variant.get("quantityAvailable")
                    or 0
                )
                variant_ref = algolia_variant_map.get(sku_shopify) or algolia_variant_map.get(sku_brand) or {}
                locations_inventory = variant_ref.get("locations_inventory") or variant.get("locations_inventory") or {}
                returns_qty = int(locations_inventory.get("100464689435") or 0)
                online_qty = int(locations_inventory.get("82416435483") or 0)
                instore_qty = inventory_quantity - (online_qty + returns_qty)
                ga_purchases = safe_string(variant_ref.get("recently_ordered_count") or variant.get("recently_ordered_count"))

                sku_shopify_value = sku_shopify.replace("gid://shopify/ProductVariant/", "")
                product_line_value = "Maternity" if "maternity" in title.lower() else ""
                jean_style = derive_jean_style(style_name, title, pdp_fields["description"], pdp_fields["leg_opening"])
                inseam_label = derive_inseam_label(title, pdp_fields["description"], jean_style, pdp_fields["inseam"])
                inseam_style = derive_inseam_style(jean_style, inseam_label, pdp_fields["inseam"], length_hint)
                rise_label = derive_rise_label(title, handle, pdp_fields["description"], rise_hint)
                hem_style = derive_hem_style(pdp_fields["description"])
                color_standardized = derive_color_standardized(color_value, pdp_fields["description"], color_hint)
                color_simplified = derive_color_simplified(color_value, pdp_fields["description"], color_standardized, wash_hint)

                row = [
                    style_id,
                    handle,
                    published_at,
                    created_at,
                    title,
                    style_name,
                    product_type,
                    join_tags(tags),
                    style.get("vendor", ""),
                    pdp_fields["description"],
                    variant_title,
                    color_value,
                    size_value,
                    pdp_fields["rise"],
                    pdp_fields["inseam"] or inseam_hint,
                    pdp_fields["leg_opening"],
                    format_price(
                        (variant.get("price") or {}).get("amount")
                        if isinstance(variant.get("price"), dict)
                        else style.get("price")
                    ),
                    format_price(
                        (variant.get("compareAtPrice") or {}).get("amount")
                        if isinstance(variant.get("compareAtPrice"), dict)
                        else style.get("compare_at_price")
                    ),
                    "TRUE" if available_bool else "FALSE",
                    safe_string(inventory_quantity),
                    safe_string(returns_qty),
                    safe_string(instore_qty),
                    safe_string(online_qty),
                    ga_purchases,
                    safe_string(style.get("totalInventory") or style.get("variants_inventory_count")),
                    sku_shopify_value,
                    sku_brand,
                    safe_string(variant.get("barcode")),
                    product_line_value,
                    (style.get("featuredImage") or {}).get("url") or style.get("product_image", ""),
                    style.get("onlineStoreUrl") or f"https://shop.paige.com/products/{handle}",
                    jean_style,
                    inseam_label,
                    inseam_style,
                    rise_label,
                    hem_style,
                    color_simplified,
                    color_standardized,
                    country,
                    normalize_stretch_value(
                        pdp_fields.get("stretch", "")
                        or derive_stretch_from_algolia_attrs(algolia_denim_fabric, algolia_stretch_attr)
                        or meta_attrs.get("stretch", "")
                    ),
                    production_cost,
                    site_exclusive,
                ]
                fit_hint_by_sku[sku_shopify_value] = fit_hint
                tags_by_sku[sku_shopify_value] = join_tags(tags)
                rows.append(row)

            if processed_handles % 50 == 0 or idx == len(styles):
                avg = (pdp_total_seconds / processed_handles) if processed_handles else 0.0
                log(
                    f"{processed_handles}/{len(styles)} handles processed | {avg:.2f}s/page | "
                    f"rise filled = {rise_filled}/{processed_handles} | "
                    f"inseam filled = {inseam_filled}/{processed_handles} | "
                    f"leg opening filled = {leg_filled}/{processed_handles}"
                )
            time.sleep(0.2)

        self.apply_style_name_rules(rows)
        self.apply_jean_style_inference(rows)
        self.apply_jean_style_fit_fallback(rows, fit_hint_by_sku, tags_by_sku)
        self.apply_rise_label_inference(rows)
        self.apply_color_inference(rows)
        self.apply_petite_inseam_rule(rows)
        return rows

    def apply_style_name_rules(self, rows: List[List[str]]) -> None:
        idx_product = CSV_HEADERS.index("Product")
        idx_style_name = CSV_HEADERS.index("Style Name")
        idx_leg = CSV_HEADERS.index("Leg Opening")
        idx_jean_style = CSV_HEADERS.index("Jean Style")

        # Rule 1: unify by first word when leg opening matches and style is most frequent (skip maternity)
        groups: Dict[str, List[List[str]]] = {}
        for row in rows:
            first_word = (row[idx_product].split(" ", 1)[0] if row[idx_product] else "").strip().lower()
            if first_word:
                groups.setdefault(first_word, []).append(row)

        for first_word, group_rows in groups.items():
            if len(group_rows) < 2:
                continue
            by_leg: Dict[str, List[List[str]]] = {}
            for r in group_rows:
                by_leg.setdefault(r[idx_leg], []).append(r)
            for leg_value, leg_rows in by_leg.items():
                non_maternity_rows = [r for r in leg_rows if "maternity" not in r[idx_product].lower()]
                if not non_maternity_rows:
                    continue
                styles = [r[idx_style_name] for r in non_maternity_rows if r[idx_style_name]]
                if len(set(styles)) <= 1:
                    continue
                most_common = max(set(styles), key=styles.count)
                for r in non_maternity_rows:
                    r[idx_style_name] = most_common

        # Rule 2: one-word style names
        for row in rows:
            style_name = row[idx_style_name].strip()
            if not style_name or len(style_name.split()) != 1:
                continue
            product_first_word = (row[idx_product].split(" ", 1)[0] if row[idx_product] else "").strip().lower()
            same_family = [
                r for r in rows
                if (r[idx_product].split(" ", 1)[0] if r[idx_product] else "").strip().lower() == product_first_word
                and r[idx_leg] == row[idx_leg]
                and len(r[idx_style_name].split()) > 1
            ]
            if same_family:
                candidates = [r[idx_style_name] for r in same_family]
                row[idx_style_name] = max(set(candidates), key=candidates.count)
                continue
            jean_first = (row[idx_jean_style].split(" ", 1)[0] if row[idx_jean_style] else "").strip()
            if jean_first:
                row[idx_style_name] = f"{style_name} {jean_first}".strip()

    def apply_jean_style_inference(self, rows: List[List[str]]) -> None:
        idx_style_name = CSV_HEADERS.index("Style Name")
        idx_leg = CSV_HEADERS.index("Leg Opening")
        idx_jean_style = CSV_HEADERS.index("Jean Style")
        idx_product = CSV_HEADERS.index("Product")
        idx_desc = CSV_HEADERS.index("Description")
        idx_inseam = CSV_HEADERS.index("Inseam")
        idx_inseam_label = CSV_HEADERS.index("Inseam Label")
        idx_inseam_style = CSV_HEADERS.index("Inseam Style")

        # Step 1: Re-derive jean_style from updated Style Name for every row.
        # Style Name may have been updated by apply_style_name_rules; the value
        # computed during build_rows() used the pre-rules name, so we redo it here.
        for row in rows:
            updated_style_name = row[idx_style_name]
            # Re-run keyword mapping against the (possibly corrected) style name.
            mapped = _map_jean_keywords(updated_style_name, row[idx_leg], include_mom=True)
            if mapped:
                row[idx_jean_style] = mapped
            # If the style-name mapping didn't resolve it, keep whatever was already there.

        # Step 2: For still-blank Jean Style rows, infer from sibling rows with same
        # style name + leg opening.
        for row in rows:
            if row[idx_jean_style]:
                continue
            style = row[idx_style_name]
            leg = row[idx_leg]
            if not style:
                continue
            matches = [
                r[idx_jean_style]
                for r in rows
                if r[idx_style_name] == style and r[idx_leg] == leg and r[idx_jean_style]
            ]
            if matches:
                row[idx_jean_style] = max(set(matches), key=matches.count)

        # Recompute inseam label/style when jean style got updated.
        for row in rows:
            jean_style = row[idx_jean_style]
            row[idx_inseam_label] = derive_inseam_label(row[idx_product], row[idx_desc], jean_style, row[idx_inseam])
            row[idx_inseam_style] = derive_inseam_style(jean_style, row[idx_inseam_label], row[idx_inseam], "")

    def apply_jean_style_fit_fallback(
        self,
        rows: List[List[str]],
        fit_hint_by_sku: Dict[str, str],
        tags_by_sku: Dict[str, str],
    ) -> None:
        idx_sku_shopify = CSV_HEADERS.index("SKU - Shopify")
        idx_leg = CSV_HEADERS.index("Leg Opening")
        idx_jean_style = CSV_HEADERS.index("Jean Style")
        idx_product = CSV_HEADERS.index("Product")
        idx_desc = CSV_HEADERS.index("Description")
        idx_inseam = CSV_HEADERS.index("Inseam")
        idx_inseam_label = CSV_HEADERS.index("Inseam Label")
        idx_inseam_style = CSV_HEADERS.index("Inseam Style")

        for row in rows:
            if row[idx_jean_style]:
                continue
            sku = row[idx_sku_shopify]
            fit_hint = fit_hint_by_sku.get(sku, "")
            tag_text = tags_by_sku.get(sku, "")
            row[idx_jean_style] = derive_jean_style_from_fit(fit_hint, tag_text, row[idx_leg])
            if row[idx_jean_style]:
                row[idx_inseam_label] = derive_inseam_label(row[idx_product], row[idx_desc], row[idx_jean_style], row[idx_inseam])
                row[idx_inseam_style] = derive_inseam_style(row[idx_jean_style], row[idx_inseam_label], row[idx_inseam], "")

    def apply_rise_label_inference(self, rows: List[List[str]]) -> None:
        idx_style_name = CSV_HEADERS.index("Style Name")
        idx_rise = CSV_HEADERS.index("Rise")
        idx_rise_label = CSV_HEADERS.index("Rise Label")

        for row in rows:
            if row[idx_rise_label]:
                continue
            style = row[idx_style_name]
            rise = row[idx_rise]
            if not style or not rise:
                continue
            matches = [
                r[idx_rise_label]
                for r in rows
                if r[idx_style_name] == style and r[idx_rise] == rise and r[idx_rise_label]
            ]
            if not matches:
                continue
            frequencies = {label: matches.count(label) for label in set(matches)}
            row[idx_rise_label] = max(frequencies, key=frequencies.get)

    def apply_color_inference(self, rows: List[List[str]]) -> None:
        idx_color = CSV_HEADERS.index("Color")
        idx_simplified = CSV_HEADERS.index("Color - Simplified")
        idx_standardized = CSV_HEADERS.index("Color - Standardized")

        by_color: Dict[str, List[List[str]]] = {}
        for row in rows:
            key = (row[idx_color] or "").strip().lower()
            if key:
                by_color.setdefault(key, []).append(row)

        for color_key, color_rows in by_color.items():
            simplified_vals = [r[idx_simplified] for r in color_rows if r[idx_simplified]]
            standardized_vals = [r[idx_standardized] for r in color_rows if r[idx_standardized]]
            most_simplified = max(set(simplified_vals), key=simplified_vals.count) if simplified_vals else ""
            most_standardized = max(set(standardized_vals), key=standardized_vals.count) if standardized_vals else ""
            for r in color_rows:
                if not r[idx_simplified] and most_simplified:
                    r[idx_simplified] = most_simplified
                if not r[idx_standardized] and most_standardized:
                    r[idx_standardized] = most_standardized

    def apply_petite_inseam_rule(self, rows: List[List[str]]) -> None:
        idx_product = CSV_HEADERS.index("Product")
        idx_style_name = CSV_HEADERS.index("Style Name")
        idx_color = CSV_HEADERS.index("Color")
        idx_inseam = CSV_HEADERS.index("Inseam")
        grouped: Dict[Tuple[str, str, str], List[List[str]]] = {}
        for row in rows:
            key = (row[idx_style_name], row[idx_color], row[idx_inseam])
            grouped.setdefault(key, []).append(row)

        for same_group_rows in grouped.values():
            has_petite = any("petite" in row[idx_product].lower() for row in same_group_rows)
            has_non_petite = any("petite" not in row[idx_product].lower() for row in same_group_rows)
            if not (has_petite and has_non_petite):
                continue
            for row in same_group_rows:
                if "petite" in row[idx_product].lower():
                    row[idx_inseam] = ""

    def write_csv(self, rows: List[List[str]]) -> Path:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = OUTPUT_DIR / f"PAIGE_{timestamp}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(CSV_HEADERS)
            writer.writerows(rows)
        log(f"Wrote {len(rows)} rows to {output_path}")
        return output_path

    def run(self) -> Path:
        log("Starting Paige scrape")
        try:
            rows = self.build_rows()
            output_path = self.write_csv(rows)
            log("Scrape complete")
            print("Done.")
            return output_path
        finally:
            self.browser_extractor.close()


def main() -> None:
    scraper = PaigeScraper()
    scraper.run()


if __name__ == "__main__":
    main()
