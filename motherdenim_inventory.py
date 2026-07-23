"""Mother Denim women's denim inventory exporter.

Primary data source is the Shopify Storefront GraphQL API (collections "denim"
and "denim-sale"). SearchSpring supplies measurement details (Rise / Inseam /
Leg Opening), Product Type and the per-variant inventory snapshot used to
derive Google Analytics Purchases and Purchase Reset Date. The product PDP is
used only as a fallback for measurements and the stretch meter.

All of the editable keyword lists live near the top of the file.
"""
import argparse
import csv
import html
import json
import os
import re
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Endpoints / configuration
# ---------------------------------------------------------------------------
# Host rotation - every host below serves both the Storefront GraphQL API and
# the product PDP pages, so any of them can satisfy a request.
HOSTS = [
    "https://motherdenim.myshopify.com",
    "https://www.motherdenim.com",
    "https://motherdenim.com",
]
# The myshopify.com domain rate-limits storefront *page* requests aggressively
# (429/503), so PDP fetches try the customer-facing domain first. GraphQL is
# unaffected and keeps using HOSTS as-is.
PDP_HOSTS = [
    "https://www.motherdenim.com",
    "https://motherdenim.com",
    "https://motherdenim.myshopify.com",
]
GRAPHQL_PATH = "/api/unstable/graphql.json"
STOREFRONT_TOKEN = "a7c1044bc2a3798166579fa2874b03e3"

SEARCHSPRING_SITE_ID = "00svms"
SEARCHSPRING_URL = "https://00svms.a.searchspring.io/api/search/autocomplete.json"

COLLECTIONS = ["denim", "denim-sale"]

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "mother_inventory_run.log"

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
    "Google Analytics Purchases",
    "Purchase Reset Date",
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
    "Stretch",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/127.0.0.0 Safari/537.36"
)

os.makedirs(OUTPUT_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    except Exception:
        pass



# ===========================================================================
# Transformation logic (validated against the style-level check file)
# ===========================================================================

# ---------------------------------------------------------------------------
# Editable keyword lists (kept near the top for easy maintenance)
# ---------------------------------------------------------------------------

# Products whose TITLE contains any of these whole words/phrases are dropped.
TITLE_EXCLUDE_TERMS = [
    "Accessories", "Accessory", "Bermuda", "Bermudas", "Blazer", "Blazers",
    "Blouse", "Blouses", "Bodysuit", "Bodysuits", "Button Up", "Button-Up",
    "Capri", "Cardigan", "Cardigans", "Clothing Top", "Clothing Tops", "Coat",
    "Coats", "Coats & Jackets", "Core Handbags", "Corset", "Corsets",
    "Crop Top", "Crop Tops", "Donation", "Dress", "Dresses",
    "Fashion Core Handbag", "Fashion Core Handbags", "Fashion Handbag",
    "Fashion Handbags", "Gift Wrap", "Goodies Accessories", "Goodies Accessory",
    "Handbag", "Heels", "Hoodie", "Hoodies", "Jacket", "Jackets",
    "Jogger Short", "Jogger Shorts", "Jort", "Jumpsuit", "Jumpsuits", "Maxi",
    "Mini", "Neck", "One Piece", "One Pieces", "One-Piece", "One-Pieces",
    "Outerwear", "Pant Suit", "Pant Suits", "Purse", "Romper", "Rompers",
    "Sandel", "Sandle", "Shacket", "Shipping", "Shipping Protection", "Shirt",
    "Shirts", "Shirts & Tops", "Shoe", "Shoes", "Short", "Shorts", "Skirt",
    "Skirts", "Sleeve", "Sleeves", "Suit", "Suits", "Sweater", "Sweaters",
    "Sweatpant", "Sweatpants", "Sweatshirt", "Sweatshirts", "Swim", "T Shirt",
    "T Shirts", "T-Shirt", "Tank", "Tank Tops", "Tee", "Tees", "Tops", "Tote",
]

# Products whose DESCRIPTION contains any of these whole words/phrases are dropped.
DESCRIPTION_EXCLUDE_TERMS = [
    "Accessory", "Bermuda", "Blazer", "Blouse", "Bodysuit", "button-down",
    "button down", "Button Up", "Button-Up", "Capri", "Cardigan", "Coat",
    "Corset", "Crop Top", "Cropped below the knee", "Dress", "Hoodie",
    "Jacket", "Jort", "Jumpsuit", "One Piece", "One-Piece", "Outerwear",
    "Overalls", "Pant Suit", "Romper", "Sandel", "Sandle", "Shacket",
    "Shipping", "Shipping Protection", "Shirt", "Shoe", "Short", "Shorts",
    "Skirt", "Sleeve", "Suit", "Sweater", "Sweaters", "Sweatpants",
    "Sweatshirt", "Swim", "T Shirt", "T Shirts", "T-Shirt", "Tank Top", "Tee",
    "Tops", "Tote",
]

# Product Type dropped if SearchSpring tags_sub_class[0] is one of these.
PRODUCT_TYPE_EXCLUDE = [
    "Dresses", "Jackets", "Shorts", "Shirts", "Sweaters", "Skirts", "Hair",
    "Belts", "Socks", "Tees", "Sweatpants", "Sweaters/Sweatshirts", "Hats",
    "Bags", "Sweatshirts", "Scarves", "Swimwear",
]

# Step 3 Style Name keywords. Longest phrases first so multi-word names win
# (e.g. "Hustler Roller" before "Hustler"/"Roller").
STYLE_NAME_KEYWORDS = [
    "Bookie", "Buffet", "Checkerboard Duster", "Chisel", "Chomp", "Dazzler",
    "Ditcher Roller", "Ditcher", "Delinquent", "Dodger", "Doozy", "Double Dip",
    "Drama", "Fangirl", "Full Pipe", "Glider", "Half Eaten", "Half Pipe",
    "Headliner", "Hiker", "Hot Rod", "Hustler Roller", "Hustler", "Insider",
    "Jinx", "Kick It", "Lasso", "Lemon Twist", "Looker", "Lunch Line", "Maven",
    "Mixer", "Newbie", "Outsider", "Peeler Jogger", "Pin Up Takeout",
    "Pipe Dream", "Piston", "Pony Keg", "Popsicle", "Pouty", "Rabbit Hole",
    "Rambler Roller", "Rambler", "Rascal", "Reifler", "Relish", "Rerun",
    "Rider", "Rigatoni", "Right Away", "Tomcat Roller", "Roller", "Runaway",
    "Scooter", "Side Dish", "Sidestepper", "Sk8R", "Smokin", "Smoothie",
    "Spinner", "Spitfire", "Stud Finder", "Stunner", "Sugar Cone",
    "Super Cruiser", "Super Ex", "Swooner", "Tagger", "Takeout",
    "Tippy Top Utensil", "Tomcat", "Tripper", "Tune Up", "Tunnel Vision",
    "Twister", "Under Wraps", "Undercover", "Weekender", "Whip",
]

# Step 4 removal list (words/phrases stripped from the title when no keyword hit).
STYLE_NAME_REMOVE_WORDS = [
    "1999", "5-Pocket", "Accent Hardware", "Ankle", "Beaded", "Bee's Knees",
    "Belted", "Braided", "Button", "Cargo", "Carpenter", "Chap", "Checkered",
    "Chew", "Coated", "Constructed", "Contrast", "Corduroy", "Crochet", "Crop",
    "Cropped", "Crushed", "Crystal", "Cuff", "Cuffed", "Cutoff", "Cut-Out",
    "Darted", "Destroyed", "Diamond Cut", "Diamond", "Distressed",
    "Double Flood", "Double Heel", "Double Prep", "Double Sneak", "Drawn",
    "Drawstring", "Embroidery", "Exposed", "Faux", "Fit", "Flag", "Flap Pocket",
    "Flap", "Flip", "Flood", "Floral", "Fray", "Frayed Seam", "Fringe",
    "Front Yoke", "Frontier", "Graffitimetalik", "Heel", "Heyday", "High Rise",
    "High Waisted", "High-Rise", "Hover cuff", "Hover", "Inch", "Inset",
    "Jean W/ Slit Hem", "Jean", "Krushed", "Krystal", "Laced Up", "Lace Up",
    "Leather", "Lightweight", "Lil", "Lo", "Loop", "Long", "Low And Loose",
    "Low Rise", "Low Waised", "Low-Rise", "Mid Rise", "Mid Waisted", "Mid-Rise",
    "Ms.", "Nacho", "Nerdy", "Panel", "Pant", "Pants", "Patch Pocket", "Patch",
    "Petite", "PETITES The Lil", "Pintucked", "Plaid", "Pleated", "Pleaty",
    "Plus", "Pocket Pant", "Pocket", "Poplin", "Prep", "Printed", "Raw Hem",
    "Renaissance", "Repair", "Retro", "Rinse", "Ripped", "Rolled Hem", "Saddle",
    "Sailor", "Seam", "Seamed Front Yoke", "Seamed", "Selvedge", "Sequin",
    "Side Seam Snaps", "Skimp", "Slant", "Slice", "Slit", "SNACKS!",
    "Snake Print", "Sneak", "Sneaker Length", "Sott", "Spark", "Sparkle",
    "Spliced", "Split", "Stacked Waist", "Stacked", "Step Fray", "Stitched",
    "Stoned", "Straight", "Striped", "Studded", "Stunner Zip", "Suede", "Super",
    "Swisher", "Side Zip", "Slung", "The", "Track Pant", "Trashed", "Trim",
    "Trouser Jean", "Trouser", "Tune Up", "Tux", "Two", "Three", "Ultra",
    "Utility", "Vegan Leather", "Velvet", "Vent And Slit", "Vent", "V-High Rise",
    "Vintage", "W/ Contrast Front Panel", "W/ Cuff", "W/ Flap Jean",
    "W/ Slit Hem", "W/ Stud Detailing", "W/ Wide Cuff", "W/Flap", "Wax",
    "Welt Pocket", "Wide Hem", "With Cuff", "With Frayed Seam", "Zip", "Zipper",
    "Zipped", "XYZ",
]


# ---------------------------------------------------------------------------
# Small text helpers
# ---------------------------------------------------------------------------
def whole_word_present(text: str, phrase: str) -> bool:
    """Case-insensitive whole-word/phrase match (never a substring of a word)."""
    if not text or not phrase:
        return False
    pat = r"(?<!\w)" + re.escape(phrase) + r"(?!\w)"
    return re.search(pat, text, re.IGNORECASE) is not None


def normalize_quotes(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("‘", "'").replace("’", "'")
        .replace("“", '"').replace("”", '"')
        .replace("′", "'").replace("″", '"')
    )


def clean_description(raw: Optional[str]) -> str:
    """Output description = raw text, only whitespace-trimmed."""
    if not raw:
        return ""
    return raw.strip()


def normalize_text(raw: Optional[str]) -> str:
    """Tidy user-facing text: straighten curly quotes/apostrophes and collapse
    runs of whitespace to a single space."""
    if not raw:
        return ""
    return re.sub(r"\s+", " ", normalize_quotes(str(raw))).strip()


def fraction_to_decimal(token: str) -> str:
    """'10 3/4' -> '10.75', '9 3/8' -> '9.38' (round half-to-even, 2 places)."""
    token = token.strip()
    m = re.match(r"^(\d+)\s+(\d+)\s*/\s*(\d+)$", token)
    if m:
        value = int(m.group(1)) + int(m.group(2)) / int(m.group(3))
    elif re.match(r"^\d+\s*/\s*\d+$", token):
        a, b = re.split(r"\s*/\s*", token)
        value = int(a) / int(b)
    else:
        try:
            value = float(token)
        except ValueError:
            return ""
    rounded = round(value, 2)
    text = f"{rounded:.2f}".rstrip("0").rstrip(".")
    return text


# ---------------------------------------------------------------------------
# Style Name
# ---------------------------------------------------------------------------
def color_from_title(title: str) -> str:
    """Part of the product title after the first ' - '."""
    if " - " in title:
        return title.split(" - ", 1)[1].strip()
    return ""


def style_base_from_title(title: str) -> str:
    """Part of the product title before the first ' - ' (Step 1 + Step 2)."""
    base = title.split(" - ", 1)[0]
    return base.replace("-", " ")


def derive_style_name(title: str) -> str:
    base = style_base_from_title(title)

    # Step 3: direct keyword match (longest phrase wins).
    for kw in sorted(STYLE_NAME_KEYWORDS, key=len, reverse=True):
        if whole_word_present(base, kw):
            return kw

    # Step 4: strip leading "... x MOTHER ", removal words, numbers, specials.
    work = base
    m = re.search(r".*?\bx\s+MOTHER\b", work, re.IGNORECASE)
    if m:
        work = work[m.end():]

    for phrase in sorted(STYLE_NAME_REMOVE_WORDS, key=len, reverse=True):
        work = re.sub(r"(?<!\w)" + re.escape(phrase) + r"(?!\w)", " ", work, flags=re.IGNORECASE)

    work = re.sub(r"[0-9]", " ", work)
    work = re.sub(r"[^A-Za-z\s]", " ", work)
    work = re.sub(r"\s+", " ", work).strip()
    return work


# ---------------------------------------------------------------------------
# Size / Color
# ---------------------------------------------------------------------------
SIZE_VALUES = {
    "00", "0", "2", "4", "6", "8", "10", "12", "14", "15 Plus", "16 Plus",
    "18 Plus", "20 Plus", "22 Plus", "24 Plus", "26 Plus", "28 Plus", "30 Plus",
    "32 Plus", "XS", "S", "M", "L", "XL", "XXL", "1XL", "2XL", "3XL", "4XL",
    "5XL", "00-4", "6-12", "14-18 Plus", "20-26 Plus", "28-32 Plus", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    "36", "37", "38", "39", "40",
}


def pick_size(options: List[Dict[str, str]]) -> str:
    """Return whichever selected option holds a recognised size value."""
    for opt in options:
        val = str(opt.get("value", "")).strip()
        if val in SIZE_VALUES:
            return val
    return ""


# ---------------------------------------------------------------------------
# Rise / Inseam / Leg Opening
# ---------------------------------------------------------------------------
_NUM = r"[0-9]+(?:\s+[0-9]+\s*/\s*[0-9]+)?(?:\.[0-9]+)?"


def parse_mfield_measure(mfield: Optional[str], label: str) -> str:
    """Parse one measurement out of SearchSpring mfield_product_details, e.g.
    '9 3/8" Rise, 27 1/2" Inseam, 13" Leg Opening'. Tolerant of case, of a
    descriptor word before the label ('27 1/2" Cuffed Inseam') and of the
    lowercase 'Leg opening' variant."""
    if not mfield:
        return ""
    text = normalize_quotes(html.unescape(str(mfield)))
    m = re.search(
        r"(" + _NUM + r")\s*\"?\s*(?:[A-Za-z][A-Za-z-]*\s+)*" + re.escape(label),
        text, re.IGNORECASE,
    )
    if m:
        return fraction_to_decimal(m.group(1))
    # Inseam sometimes has no label (the middle measurement); take the number
    # that is neither the Rise nor the Leg Opening value. Only trust this when
    # the text is clearly the "Rise, Inseam, Leg Opening" triple - otherwise a
    # stray number (e.g. a PDP "Model is 5' 10"" line) would be misread.
    if label.lower() == "inseam" and re.search(r"rise", text, re.IGNORECASE) \
            and re.search(r"leg", text, re.IGNORECASE):
        parts = [p.strip() for p in text.split(",") if p.strip()]
        for part in parts:
            low = part.lower()
            if "rise" in low or "leg" in low or "model" in low:
                continue
            nm = re.search(r"(" + _NUM + r")", part)
            if nm:
                return fraction_to_decimal(nm.group(1))
    return ""


def inseam_from_description(description: str) -> str:
    """'32-inch inseam' -> '32'; '28 3/4 inseam' -> '28.75'."""
    if not description:
        return ""
    m = re.search(
        r"([0-9]+(?:\s+[0-9]+\s*/\s*[0-9]+)?(?:\.[0-9]+)?)\s*-?\s*(?:inch\b|\")?\s*inseam",
        description, re.IGNORECASE,
    )
    if m:
        return fraction_to_decimal(m.group(1))
    return ""


def to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except ValueError:
        return None


def coerce_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Jean Style
# ---------------------------------------------------------------------------
def derive_jean_style_step1(description: str, leg_opening: str) -> str:
    d = description or ""
    lo = to_float(leg_opening)

    def has(*phrases):
        return any(whole_word_present(d, p) for p in phrases)

    if has("Bootcut"):
        return "Bootcut"
    if has("Flare", "Narrow flare jeans", "skinny flare", "flared leg",
            "wide-flared jeans", "flared, wide leg"):
        return "Flare"
    if has("Skinny", "Skinnies"):
        return "Skinny"
    if has("Wide leg", "Wide-leg", "slightly tapered leg",
            "Wide-leg pants with a subtle barrel shape",
            "wide leg and subtle barrel",
            "loose wide leg with a curved outer seam", "palazzo"):
        return "Wide Leg"
    if has("wide barrel leg", "full barrel leg", "wide, curved leg", "barrel-leg",
           "Barrel leg", "Barel leg", "Bowed Leg", "Bow leg", "stovepipe",
           "stove-pipe", "horseshoe"):
        return "Barrel"
    if has("Straight") and lo is not None and lo < 15.5:
        return "Straight from Knee"
    if has("Straight") and lo is not None and 15.5 <= lo <= 18:
        return "Straight from Knee/Thigh"
    if has("Straight") and lo is not None and lo > 18:
        return "Straight from Thigh"
    if has("loose, straight-leg", "loose straight leg", "wide straight leg",
           "Designed to be worn loose and low at the hips, these straight-leg jeans") and lo is None:
        return "Straight from Thigh"
    if has("slim straight") and lo is None:
        return "Straight from Knee"
    if has("Taper", "Tapering", "jogger", "elastic at the ankle", "Tapered"):
        return "Taper"
    if has("Straight"):
        return "Straight Leg"
    return ""


# ---------------------------------------------------------------------------
# Hem Style
# ---------------------------------------------------------------------------
HEM_RULES = [
    ("Zipper Hem", ["zippers at the hem", "hem that can be zipped", "zip hem"]),
    ("Split Hem", ["side slit", "side-slit", "side slits", "side-slits",
                   "slit inseams at the hem", "slit at the hem", "split hem",
                   "slit hem", "slit at hem", "sliced outer seams",
                   "sliced outer seam", "slice at the ankle"]),
    ("Distressed Hem", ["grinding along the hems", "Chewed hem", "busted hems",
                        "Well-worn hems", "Well worn hems", "Light distressed hem",
                        "Distressed knees and hem", "Distressed detailing and hem",
                        "distressed hem", "uneven hem", "chew detail at hem",
                        "chewed up hem", "Hemmed with mild bites"]),
    ("Raw Hem", ["raw-edge hem", "raw edge hem", "raw-hem", "raw, cut hems",
                 "cut hem", "raw-cut hem", "raw hem", "raw ankle hem",
                 "raw crop hem", "raw cropped hem", "Frayed Hem",
                 "Fraying at the hem", "frayed hem", "fraying hem",
                 "frayed step-hem", "frayed triple step-hem",
                 "frayed double step-hem", "frayed single step-hem",
                 "frayed, uneven hem", "frayed ankle", "frayed crop", "fray crop",
                 "fray ankle", "fray hem", "step fray", "cut at the ankle",
                 "Step Fray", "Ankle-frayed"]),
    ("Ribbed Hem", ["ribbed hems", "ribbed hem", "elastic cuff", "elastic at the ankle"]),
    ("Cuffed Hem", ["clean cuffed hem", "clean, cuffed hem", "wide cuffed hem",
                    "clean, untacked cuffed hem", "cuffed hem",
                    "extra-wide cuffed hem", "Cuffs at the hem"]),
    ("Fringe Hem", ["fringe hem", "fringe along the hem"]),
    ("Released Hem", ["Released Hem", "undone finished hem", "undone hem",
                      "released-hem"]),
    ("Wide Hem", ["wide hem", "extra-thick hem", "Trouser hem"]),
]

HEM_CLEAN_PHRASES = [
    "clean ankle-length hem", "clean ankle-length inseam", "clean ankle length hem",
    "clean ankle length inseam", "clean cropped hem", "clean cropped inseam",
    "clean full-length hem", "clean full-length inseam", "clean full length hem",
    "clean full length inseam", "clean long hem", "clean long inseam",
    "Clean details and hem", "finished hem", "clean hem",
]


def derive_hem_style(description: str, ss_hem: Optional[str] = None) -> str:
    """Description-driven hem classification. When more than one phrase matches,
    the longest (most specific) phrase wins; ties break by rule priority."""
    d = description or ""
    best = None  # (phrase_len, -priority, label)
    for priority, (label, phrases) in enumerate(HEM_RULES):
        for p in phrases:
            if whole_word_present(d, p):
                key = (len(p), -priority)
                if best is None or key > best[0]:
                    best = (key, label)
    # Rule 10 - Clean Hem (phrases + the "clean ##-inch inseam" pattern)
    for p in HEM_CLEAN_PHRASES:
        if whole_word_present(d, p):
            key = (len(p), -len(HEM_RULES))
            if best is None or key > best[0]:
                best = (key, "Clean Hem")
    m = re.search(r"clean\s+[0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?-inch\s+inseam", d, re.IGNORECASE)
    if m:
        key = (len(m.group(0)), -len(HEM_RULES))
        if best is None or key > best[0]:
            best = (key, "Clean Hem")
    return best[1] if best else ""


# ---------------------------------------------------------------------------
# Inseam Label
# ---------------------------------------------------------------------------
def derive_inseam_label(title: str, description: str, size: str, jean_style: str,
                        inseam: str) -> str:
    d = description or ""
    petite_size = bool(size) and str(size).strip().upper().endswith("P")
    if (whole_word_present(title, "Petite") or whole_word_present(title, "Petites")
            or whole_word_present(title, "Lil") or petite_size
            or "perfect for women under 5'4" in d.lower()):
        return "Petite"
    ins = to_float(inseam)
    long_group_a = {"Bootcut", "Flare", "Wide Leg", "Straight from Thigh",
                    "Baggy", "Boyfriend", "Straight from Knee/Thigh"}
    long_group_b = {"Barrel", "Skinny", "Tapered", "Straight from Knee"}
    if jean_style in long_group_a and ins is not None and ins >= 33:
        return "Long"
    if jean_style in long_group_b and ins is not None and ins >= 31.5:
        return "Long"
    return "Regular"


# ---------------------------------------------------------------------------
# Inseam Style
# ---------------------------------------------------------------------------
def derive_inseam_style(title: str, description: str, jean_style: str,
                        inseam_label: str, inseam: str) -> str:
    d = description or ""

    # 1. TITLE_INSEAM_STYLE_CONFIRMED
    if whole_word_present(title, "Crop"):
        return "Cropped"
    if whole_word_present(title, "Flood"):
        return "Ankle"
    if any(whole_word_present(title, w) for w in ("Skimp", "Heel", "Sneak")):
        return "Full Length"

    # 2. DESCRIPTION_INSEAM_STYLE
    if (re.search(r"long\s+[0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?\s*-?\s*inch\s+inseam", d, re.IGNORECASE)
            or whole_word_present(d, "Full Length") or whole_word_present(d, "full-length")):
        return "Full Length"

    # 3. MEASUREMENT_INSEAM_STYLE
    ins = to_float(inseam)
    non_taper = {"Straight from Knee/Thigh", "Bootcut", "Wide Leg", "Boyfriend",
                 "Baggy", "Flare", "Straight from Thigh"}
    taper = {"Taper", "Tapered", "Skinny", "Barrel", "Straight from Knee"}
    petite = inseam_label == "Petite"
    if ins is not None:
        if jean_style in non_taper:
            if petite:
                if 25 < ins < 26 and "this style is intended to be oversized and worn low on the hips" in d.lower():
                    return "Ankle"
                if ins < 26:
                    return "Cropped"
                if 26 <= ins <= 28:
                    return "Ankle"
                return "Full Length"
            else:
                if ins < 27.5:
                    return "Cropped"
                if 27.5 <= ins <= 30:
                    return "Ankle"
                return "Full Length"
        if jean_style in taper:
            if petite:
                if ins < 25.5:
                    return "Cropped"
                if 25.5 <= ins <= 27:
                    return "Ankle"
                return "Full Length"
            else:
                if ins < 27:
                    return "Cropped"
                if 27 <= ins <= 28.5:
                    return "Ankle"
                return "Full Length"

    # 4. TITLE_INSEAM_STYLE_LIKELY (only when no inseam measurement)
    if ins is None:
        if whole_word_present(title, "Ankle") or whole_word_present(title, "Nerd"):
            return "Ankle"
        if whole_word_present(title, "Hover"):
            return "Full Length"
    return ""


# ---------------------------------------------------------------------------
# Rise Label
# ---------------------------------------------------------------------------
HIGH_KEYWORDS = ["high rise", "high-waisted", "high-rise", "high waist"]
LOW_HIP_PHRASES = ["low on the hip", "low on the hips", "lower on the hip",
                   "lower on the hips", "low-slung", "low on the waist"]


def _contains_any(text: str, phrases) -> bool:
    return any(whole_word_present(text, p) for p in phrases)


def rise_label_explicit(rise: str, inseam_label: str, description: str,
                        title: str) -> str:
    """Rise Label steps 1, 2, 4, 5 (numeric bounds, combo, description & title
    keywords). Returns '' when none apply so later phases can run."""
    d = description or ""
    rise_val = to_float(rise)

    # 1. Numeric bounds
    if rise_val is not None:
        if rise_val <= 9:
            return "Low"
        if rise_val >= 12:
            return "Ultra High"

    # 2. Rise + inseam label + high/low keyword combos
    if rise_val is not None and _contains_any(d, HIGH_KEYWORDS) and _contains_any(d, LOW_HIP_PHRASES):
        if inseam_label in ("Regular", "Long"):
            if rise_val <= 10:
                return "Low"
            if 10 < rise_val < 11:
                return "Mid"
            if rise_val >= 11:
                return "High"
        if inseam_label == "Petite":
            return "Mid" if rise_val < 10.5 else "High"

    # 4. Description phrases
    if _contains_any(d, ["Rise: Super Low", "Rise: Ultra Low", "Rise - Super Low",
                         "Rise - Ultra Low", "super low rise", "super low-rise",
                         "ultra low rise", "ultra low-rise", "super low waist",
                         "super low-waist", "ultra low waist", "ultra low-waist"]):
        return "Ultra Low"
    if _contains_any(d, ["Rise: Super High", "Rise: Ultra High", "Rise - Super High",
                         "Rise - Ultra High", "highest rise", "super high rise",
                         "super high-rise", "ultra high rise", "ultra high-rise",
                         "super high waist", "super high-waist", "super high-waisted",
                         "super high waisted", "ultra high waist", "ultra high-waist"]):
        return "Ultra High"
    if _contains_any(d, ["Rise: Mid", "Rise - Mid", "Mid-Rise", "mid waisted",
                         "mid-waisted", "sits mid", "Mid Rise"]):
        return "Mid"
    if _contains_any(d, ["Rise: Low", "Rise - Low", "Low-Rise", "Low Rise",
                         "hip-hugging fit", "low waisted", "low-waisted"]):
        return "Low"
    if _contains_any(d, ["Rise: High", "Rise - High", "High-Rise", "High Rise",
                         "High Waist", "High-Waist", "High Waisted", "High-Waisted",
                         "sits high", "high on the hip", "high on the waist"]):
        return "High"

    # 5. Title keywords
    if _contains_any(title, ["super low rise", "super low-rise", "ultra low rise",
                            "ultra low-rise", "super low waist", "super low-waist",
                            "ultra low waist", "ultra low-waist"]):
        return "Ultra Low"
    if _contains_any(title, ["super high rise", "super high-rise", "ultra high rise",
                            "ultra high-rise", "super high waist", "super high-waist",
                            "ultra high waist", "ultra high-waist"]):
        return "Ultra High"
    if _contains_any(title, ["Mid-Rise", "Mid Rise"]):
        return "Mid"
    if _contains_any(title, ["Low-Rise", "Low Rise"]):
        return "Low"
    if _contains_any(title, ["High-Rise", "High Rise"]):
        return "High"
    return ""


def rise_label_rule7(inseam_label: str, description: str,
                     petite: bool = True) -> str:
    """Step 7 - inseam label + broad low-hip phrases. When petite=False the
    Petite branch is deferred so a Style-Name sibling can fill it first."""
    d = description or ""
    if _contains_any(d, LOW_HIP_PHRASES):
        if inseam_label in ("Regular", "Long"):
            return "Low"
        if inseam_label == "Petite" and petite:
            return "Mid"
    return ""


def rise_label_from_ss(ss_rise: Optional[str]) -> str:
    """Step 8 - fall back to SearchSpring ss_rise."""
    if not ss_rise:
        return ""
    mapping = {
        "mid rise": "Mid", "high rise": "High", "low rise": "Low",
        "ultra high rise": "Ultra High", "ultra low rise": "Ultra Low",
    }
    sr = str(ss_rise).strip()
    return mapping.get(sr.lower(), sr)


# ---------------------------------------------------------------------------
# Color - Standardized / Simplified
# ---------------------------------------------------------------------------
def _wash_terms(color):
    """Expand a base color into its wash phrase variants used in the spec."""
    templates = [
        "{c} mineral wash", "{c} stretch denim", "{c} wash", "{c} denim",
        "dark {c}", "faded {c}", "rich {c}", "true {c}", "washed {c}",
        "bright {c}", "classic {c}", "dark-{c}", "faded vintage {c}",
        "light {c}", "light-{c}", "medium {c}", "mid {c}", "mid-{c}",
        "vintage {c}", "faded army {c}", "faded army-{c}", "creamy {c}",
    ]
    return [t.format(c=color) for t in templates]


STD_DESC_GROUPS = [
    ("Blue", _wash_terms("blue") + ["light periwinkle"] + _wash_terms("indigo")
             + _wash_terms("navy")),
    ("Black", _wash_terms("black")),
    ("Gray", _wash_terms("gray") + _wash_terms("grey")),
    ("White", _wash_terms("white") + ["ecru", "off-white", "off white",
                                       "creamy off-white"]),
    ("Tan", _wash_terms("khaki") + _wash_terms("tan") + _wash_terms("beige")),
    ("Green", _wash_terms("green") + _wash_terms("olive") + _wash_terms("sage")),
    ("Orange", _wash_terms("orange") + ["dark salmon"]),
    ("Pink", _wash_terms("pink")),
    ("Red", _wash_terms("red")),
    ("Yellow", _wash_terms("yellow") + ["buttery yellow"]),
    ("Brown", _wash_terms("brown")),
    ("Purple", _wash_terms("purple") + _wash_terms("violet")),
]

# keyword -> standardized color, for the Color field / Option1 mapping.
COLOR_TOKEN_MAP = [
    ("Animal Print", ["Animal Print", "leopard", "snake", "Camo"]),
    ("Print", ["Print"]),
    ("Blue", ["Blue", "Bleu", "Blues", "Navy", "indigo"]),
    ("Tan", ["Tan", "Sand", "buff", "cement", "ginger", "Sable", "Beige", "khaki"]),
    ("Brown", ["Brown", "Cinnamon", "Camel", "Chocolate", "Pecan", "Oak", "Coffee", "Espresso"]),
    ("Green", ["Green", "Olive", "Cypress", "Moss", "Sage"]),
    ("Gray", ["Grey", "Gray", "Stone"]),
    ("Orange", ["Orange"]),
    ("Pink", ["Pink", "Blush", "Coral"]),
    ("Purple", ["Purple", "Violet"]),
    ("Red", ["Red", "wine", "cherry", "burgundy"]),
    ("White", ["White", "ecru", "egret", "cream", "Crème", "Blizzard", "Ivory", "Parchment", "Blanc"]),
    ("Yellow", ["Yellow", "Sunny"]),
    ("Black", ["Black", "Noir", "Onyx", "Raven"]),
]


def _color_from_tokens(value: str) -> str:
    if not value:
        return ""
    for label, tokens in COLOR_TOKEN_MAP:
        if any(whole_word_present(value, t) for t in tokens):
            return label
    return ""


def color_title_clean(title: str) -> str:
    ct = color_from_title(title)
    return re.sub(r"[^A-Za-z0-9\s]", "", ct).strip()


def derive_color_standardized(color: str, description: str, title: str,
                              option1: str) -> str:
    d = description or ""

    # 1. Print check (description). A "stripes" reference makes it a Print even
    #    when a base colour is also named (e.g. "mid-blue with white stripes").
    if whole_word_present(d, "Animal Print") or whole_word_present(d, "leopard") or whole_word_present(d, "snake"):
        return "Animal Print"
    if whole_word_present(d, "Print") or whole_word_present(d, "stripes"):
        return "Print"

    # 2. Description cues, anchored after COLOR_TITLE where possible.
    color_title = color_title_clean(title)
    search_region = d
    if color_title:
        idx = d.lower().find(color_title.lower())
        if idx != -1:
            search_region = d[idx:]

    best_pos = None
    best_label = ""
    for label, phrases in STD_DESC_GROUPS:
        for p in phrases:
            m = re.search(r"(?<!\w)" + re.escape(p) + r"(?!\w)", search_region, re.IGNORECASE)
            if m and (best_pos is None or m.start() < best_pos):
                best_pos = m.start()
                best_label = label
    if best_label:
        return best_label

    # 3. Color token map
    label = _color_from_tokens(color)
    if label:
        return label

    # 4. Option1 token map
    label = _color_from_tokens(option1)
    if label:
        return label
    return ""


def derive_color_simplified(color_standardized: str, description: str) -> str:
    cs = color_standardized
    if cs in ("Animal Print", "Print"):
        return "Print"
    if cs in ("Orange", "Pink", "Purple", "Red", "Yellow", "Green"):
        return "Color"
    if cs in ("Black", "Brown"):
        return "Dark"
    if cs == "White":
        return "Light"

    d = (description or "").lower()

    def any_in(items):
        return any(item in d for item in items)

    if any_in(["dark blue", "dark denim wash", "dark-blue"]):
        return "Dark"
    if any_in(["bright blue", "classic 90s-inspired wash", "classic blue",
               "faded vintage blue", "medium blue", "mid blue", "mid-blue",
               "retro-inspired wash"]):
        return "Medium"
    if any_in(["light blue", "light indigo", "light periwinkle", "light-blue",
               "vintage blue", "baby blue"]):
        return "Light"
    if any_in(["faded gray", "faded grey", "light grey", "light gray",
               "washed gray", "washed grey"]):
        return "Light"
    if "dark grey" in d or "dark gray" in d:
        return "Medium"
    if any_in(["dark khaki", "dark tan", "dark beige"]):
        return "Medium"
    if any_in(["faded khaki", "light khaki", "faded tan", "light tan",
               "faded beige", "light beige"]):
        return "Light"
    return ""


# ---------------------------------------------------------------------------
# Stretch
# ---------------------------------------------------------------------------
def derive_stretch(description: str, tags: List[str], stretch_meter: Optional[str] = None) -> str:
    d = description or ""

    # 1. Rigid / semi-rigid language.
    if whole_word_present(d, "semi-rigid SUPERIOR"):
        return "Low Stretch"
    if whole_word_present(d, "rigid SUPERIOR"):
        return "Rigid"
    if whole_word_present(d, "semi-rigid") or whole_word_present(d, "semi rigid"):
        return "Low Stretch"
    if whole_word_present(d, "rigid"):
        return "Rigid"

    # 2. Stretch meter element from the PDP.
    meter_map = {
        "percent-100": "Rigid", "percent-75": "Low Stretch",
        "percent-50": "Medium Stretch", "percent-25": "Medium to High Stretch",
        "percent-0": "High Stretch",
    }
    if stretch_meter and stretch_meter in meter_map:
        return meter_map[stretch_meter]

    # 3. Tags.
    tagset = set(t.strip() for t in tags)
    tag_map = {
        "stretch_5": "Rigid", "stretch_4": "Low Stretch",
        "stretch_3": "Medium Stretch", "stretch_2": "Medium to High Stretch",
        "stretch_1": "High Stretch",
    }
    for tag, label in tag_map.items():
        if tag in tagset:
            return label
    return ""



# ---------------------------------------------------------------------------
# HTTP helpers with host rotation + retry/backoff
# ---------------------------------------------------------------------------
TRANSIENT_STATUSES = {429, 500, 502, 503, 504}

GRAPHQL_COLLECTION_QUERY = """
query($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 50, after: $cursor) {
      edges { node {
        id handle title description vendor productType tags
        createdAt updatedAt publishedAt onlineStoreUrl totalInventory
        featuredImage { url }
        images(first: 10) { edges { node { url } } }
        variants(first: 200) { edges { node {
          id sku title barcode availableForSale quantityAvailable currentlyNotInStock
          price { amount } compareAtPrice { amount }
          selectedOptions { name value }
        } } }
      } }
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""


def graphql_request(query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
    """POST a GraphQL query, rotating hosts and retrying transient failures."""
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Storefront-Access-Token": STOREFRONT_TOKEN,
    }
    last_exc: Optional[Exception] = None
    for host in HOSTS:
        url = f"{host}{GRAPHQL_PATH}"
        for attempt in range(5):
            try:
                resp = session.post(url, headers=headers,
                                    json={"query": query, "variables": variables},
                                    timeout=40)
                if resp.status_code in TRANSIENT_STATUSES:
                    raise requests.HTTPError(f"transient status {resp.status_code}")
                resp.raise_for_status()
                data = resp.json()
                if data.get("errors"):
                    raise RuntimeError(f"GraphQL errors: {data['errors']}")
                return data["data"]
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt < 4:
                    sleep_for = 2.0 ** attempt
                    log(f"[retry] GraphQL {url} ({exc}); sleeping {sleep_for:.1f}s")
                    time.sleep(sleep_for)
        log(f"[fallback] GraphQL host failed, rotating away from {host}")
    raise last_exc or RuntimeError("GraphQL request failed on all hosts")


class PermanentHTTPError(Exception):
    """A non-retryable HTTP status (e.g. 404) - stop retrying immediately."""


def http_get(url: str, *, params: Any = None, expect_json: bool = True) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(5):
        try:
            resp = session.get(url, params=params, timeout=40)
            if resp.status_code in TRANSIENT_STATUSES:
                raise requests.HTTPError(f"transient status {resp.status_code}")
            if 400 <= resp.status_code < 500:
                raise PermanentHTTPError(f"status {resp.status_code} for {url}")
            resp.raise_for_status()
            return resp.json() if expect_json else resp.text
        except PermanentHTTPError:
            raise
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < 4:
                sleep_for = 2.0 ** attempt
                log(f"[retry] GET {url} ({exc}); sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)
    raise last_exc or RuntimeError(f"GET failed: {url}")


# Small courtesy delay between storefront PDP fetches to avoid rate limiting.
PDP_THROTTLE_SECONDS = 0.4


def fetch_pdp_html(handle: str) -> str:
    """Fetch a product PDP, rotating hosts. Returns '' when the PDP is absent
    (e.g. an unpublished product that still appears in a collection)."""
    time.sleep(PDP_THROTTLE_SECONDS)
    last_exc: Optional[Exception] = None
    for host in PDP_HOSTS:
        url = f"{host}/products/{handle}"
        try:
            return http_get(url, expect_json=False)
        except PermanentHTTPError:
            # 404 on one host means the handle is gone; other hosts agree.
            log(f"[pdp] {handle} not available (404)")
            return ""
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log(f"[fallback] PDP host failed for {handle}, rotating away from {host}")
    if last_exc:
        log(f"[pdp] {handle} failed on all hosts: {last_exc}")
    return ""


# ---------------------------------------------------------------------------
# Data acquisition
# ---------------------------------------------------------------------------
def parse_shopify_id(gid: Optional[str]) -> str:
    """gid://shopify/Product/123 -> 123."""
    if not gid:
        return ""
    return str(gid).rsplit("/", 1)[-1]


def fetch_collection_products(handle: str) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        data = graphql_request(GRAPHQL_COLLECTION_QUERY, {"handle": handle, "cursor": cursor})
        collection = data.get("collection")
        if not collection:
            log(f"[graphql] collection '{handle}' not found")
            break
        conn = collection["products"]
        for edge in conn["edges"]:
            products.append(edge["node"])
        page_info = conn["pageInfo"]
        log(f"[graphql] collection {handle}: {len(products)} products so far")
        if not page_info["hasNextPage"]:
            break
        cursor = page_info["endCursor"]
        time.sleep(0.2)
    return products


def fetch_all_products() -> List[Dict[str, Any]]:
    """Dedup products across the target collections, preserving first-seen order."""
    seen: set = set()
    products: List[Dict[str, Any]] = []
    for handle in COLLECTIONS:
        for product in fetch_collection_products(handle):
            h = product.get("handle")
            if h and h not in seen:
                seen.add(h)
                products.append(product)
    log(f"[graphql] unique products across collections: {len(products)}")
    return products


def _first(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def fetch_searchspring() -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Return (by_handle, variant_map). variant_map[variant_id] = {inventory, updated_at}."""
    by_handle: Dict[str, Dict[str, Any]] = {}
    variant_map: Dict[str, Dict[str, Any]] = {}
    page = 1
    while True:
        params = [
            ("siteId", SEARCHSPRING_SITE_ID),
            ("resultsFormat", "json"),
            ("resultsPerPage", 250),
            ("q", "$jeans"),
            ("q", "$womens"),
            ("page", page),
        ]
        data = http_get(SEARCHSPRING_URL, params=params)
        results = data.get("results") or []
        if not results:
            break
        for item in results:
            handle = item.get("handle")
            if handle and handle not in by_handle:
                by_handle[handle] = item
            for variant in _parse_ss_variants(item.get("variants")):
                vid = str(variant.get("id") or "").strip()
                if not vid:
                    continue
                variant_map.setdefault(vid, {
                    "inventory_quantity": variant.get("inventory_quantity"),
                    "updated_at": variant.get("updated_at"),
                })
        pagination = data.get("pagination") or {}
        total_pages = pagination.get("totalPages")
        log(f"[searchspring] page {page}/{total_pages} -> {len(results)} items")
        if total_pages and page >= total_pages:
            break
        page += 1
        time.sleep(0.2)
    log(f"[searchspring] handles={len(by_handle)} variants={len(variant_map)}")
    return by_handle, variant_map


def _parse_ss_variants(raw: Any) -> List[Dict[str, Any]]:
    if not raw:
        return []
    text = html.unescape(str(raw))
    try:
        parsed = json.loads(text)
    except Exception:
        return []
    if isinstance(parsed, dict):
        return [parsed]
    if isinstance(parsed, list):
        return [v for v in parsed if isinstance(v, dict)]
    return []


# ---------------------------------------------------------------------------
# PDP fallbacks
# ---------------------------------------------------------------------------
def pdp_size_fit_text(pdp_html: str) -> str:
    """Return the plain text of the #accordion-size-fit block."""
    idx = pdp_html.find("accordion-size-fit")
    if idx == -1:
        return ""
    chunk = pdp_html[idx:idx + 4000]
    text = re.sub(r"<[^>]+>", " ", chunk)
    return re.sub(r"\s+", " ", text)


def pdp_stretch_meter(pdp_html: str) -> str:
    """Return the 'percent-NN' token of the stretch meter, if present."""
    m = re.search(r"stretch-unit[^\"']*?(percent-\d+)", pdp_html)
    return m.group(1) if m else ""


# ---------------------------------------------------------------------------
# Filtering helpers
# ---------------------------------------------------------------------------
def title_is_excluded(title: str) -> Optional[str]:
    for term in TITLE_EXCLUDE_TERMS:
        if whole_word_present(title, term):
            return term
    return None


def description_is_excluded(description: str) -> Optional[str]:
    for term in DESCRIPTION_EXCLUDE_TERMS:
        if whole_word_present(description, term):
            return term
    return None


def product_type_from_ss(ss_item: Dict[str, Any]) -> str:
    sub = ss_item.get("tags_sub_class") if ss_item else None
    value = _first(sub)
    return str(value) if value not in (None, "") else ""


# ---------------------------------------------------------------------------
# Size / variant helpers
# ---------------------------------------------------------------------------
def variant_options(variant: Dict[str, Any]) -> List[Dict[str, str]]:
    return variant.get("selectedOptions", []) or []


def variant_size(variant: Dict[str, Any]) -> str:
    return pick_size(variant_options(variant))


def money(node: Any) -> str:
    if isinstance(node, dict):
        amount = node.get("amount")
        if amount not in (None, ""):
            try:
                return f"${float(amount):.2f}"
            except (TypeError, ValueError):
                return f"${amount}"
    return ""


def product_image_url(product: Dict[str, Any]) -> str:
    featured = product.get("featuredImage") or {}
    if featured.get("url"):
        return featured["url"]
    edges = (product.get("images") or {}).get("edges") or []
    for edge in edges:
        url = (edge.get("node") or {}).get("url")
        if url:
            return url
    return ""


JEAN_STYLE_PRIORITY = [
    "Bootcut", "Flare", "Skinny", "Wide Leg", "Barrel",
    "Straight from Knee/Thigh", "Straight from Thigh", "Straight from Knee",
    "Taper",
]


# ---------------------------------------------------------------------------
# Style-level assembly (all cross-item passes live here)
# ---------------------------------------------------------------------------
def build_style_info(products: List[Dict[str, Any]],
                     ss_by_handle: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    info: Dict[str, Dict[str, Any]] = {}

    for product in products:
        handle = product.get("handle")
        if not handle:
            continue
        title = normalize_text(product.get("title", ""))
        description = normalize_text(clean_description(product.get("description")))
        desc_match = normalize_quotes(description)
        ss_item = ss_by_handle.get(handle, {})
        mfield = ss_item.get("mfield_product_details")

        rise = parse_mfield_measure(mfield, "Rise")
        inseam = parse_mfield_measure(mfield, "Inseam")
        leg = parse_mfield_measure(mfield, "Leg Opening")

        # PDP fallback for any missing measurement.
        pdp_html = ""
        if not (rise and inseam and leg):
            try:
                pdp_html = fetch_pdp_html(handle)
            except Exception as exc:  # noqa: BLE001
                log(f"[pdp] failed for {handle}: {exc}")
            if pdp_html:
                fit_text = pdp_size_fit_text(pdp_html)
                rise = rise or parse_mfield_measure(fit_text, "Rise")
                inseam = inseam or parse_mfield_measure(fit_text, "Inseam")
                leg = leg or parse_mfield_measure(fit_text, "Leg Opening")
        if not inseam:
            inseam = inseam_from_description(desc_match)

        # A size sample lets the Inseam Label rule detect the "P" petite suffix.
        variants = [e["node"] for e in (product.get("variants") or {}).get("edges", [])]
        size_sample = ""
        for variant in variants:
            s = variant_size(variant)
            if s:
                size_sample = s
                break
        option1 = ""
        if variants and variant_options(variants[0]):
            option1 = variant_options(variants[0])[0].get("value", "")

        color = color_from_title(title)
        style_name = derive_style_name(title)
        jean_style = derive_jean_style_step1(desc_match, leg)
        hem_style = derive_hem_style(desc_match, _first(ss_item.get("ss_hem")))
        colorstd = derive_color_standardized(color, desc_match, title, option1)
        colorsimp = derive_color_simplified(colorstd, desc_match)

        stretch = derive_stretch(desc_match, product.get("tags", []), None)
        if not stretch:
            if not pdp_html:
                try:
                    pdp_html = fetch_pdp_html(handle)
                except Exception:  # noqa: BLE001
                    pdp_html = ""
            stretch = derive_stretch(desc_match, product.get("tags", []),
                                     pdp_stretch_meter(pdp_html))

        info[handle] = {
            "handle": handle,
            "style_id": parse_shopify_id(product.get("id")),
            "title": title,
            "description": description,
            "desc_match": desc_match,
            "color": color,
            "style_name": style_name,
            "rise": rise,
            "inseam": inseam,
            "leg_opening": leg,
            "jean_style": jean_style,
            "hem_style": hem_style,
            "color_std": colorstd,
            "color_simp": colorsimp,
            "stretch": stretch,
            "product_type": product_type_from_ss(ss_item),
            "ss_fit": _first(ss_item.get("ss_fit")),
            "ss_rise": _first(ss_item.get("ss_rise")),
            "size_sample": size_sample,
            "color_title": color_title_clean(title),
            "rise_val": to_float(rise),
            # placeholders filled by the passes below
            "inseam_label": "",
            "inseam_style": "",
            "rise_label": "",
        }

    _audit_jean_style(info)
    _fill_dependent_fields(info)
    _resolve_rise_label(info)
    _cross_fill(info, "color_std", "color_title")
    _cross_fill(info, "color_simp", "color")
    return info


def _audit_jean_style(info: Dict[str, Dict[str, Any]]) -> None:
    """Harmonise Jean Style within a Style Name (majority, then priority order)."""
    groups: Dict[str, List[str]] = defaultdict(list)
    for handle, entry in info.items():
        groups[entry["style_name"]].append(handle)
    for style_name, handles in groups.items():
        if not style_name:
            continue
        values = [info[h]["jean_style"] for h in handles if info[h]["jean_style"]]
        if not values:
            for h in handles:
                if info[h]["ss_fit"]:
                    info[h]["jean_style"] = info[h]["ss_fit"]
            continue
        counts = Counter(values)
        top = counts.most_common()
        max_count = top[0][1]
        winners = [style for style, count in top if count == max_count]
        if len(winners) == 1:
            majority = winners[0]
        else:
            majority = next((p for p in JEAN_STYLE_PRIORITY if p in winners), winners[0])
        for h in handles:
            info[h]["jean_style"] = majority


def _fill_dependent_fields(info: Dict[str, Dict[str, Any]]) -> None:
    """Inseam Label / Inseam Style / Rise Label steps 1-5 depend on the final
    Jean Style, so they are computed after the audit."""
    for entry in info.values():
        entry["inseam_label"] = derive_inseam_label(
            entry["title"], entry["desc_match"], entry["size_sample"],
            entry["jean_style"], entry["inseam"])
        entry["inseam_style"] = derive_inseam_style(
            entry["title"], entry["desc_match"], entry["jean_style"],
            entry["inseam_label"], entry["inseam"])
        entry["rise_label"] = rise_label_explicit(
            entry["rise"], entry["inseam_label"], entry["desc_match"], entry["title"])


def _rise_step6(info: Dict[str, Dict[str, Any]]) -> None:
    """Fill blank Rise Label from same-Style-Name siblings (closest Rise wins)."""
    groups: Dict[str, List[str]] = defaultdict(list)
    for handle, entry in info.items():
        groups[entry["style_name"]].append(handle)
    for style_name, handles in groups.items():
        if not style_name:
            continue
        anchors = [(info[h]["rise_val"], info[h]["rise_label"])
                   for h in handles if info[h]["rise_label"]]
        if not anchors:
            continue
        for h in handles:
            if info[h]["rise_label"]:
                continue
            rv = info[h]["rise_val"]
            if rv is None:
                cand = Counter(label for _, label in anchors).most_common(1)[0][0]
            else:
                cand = min(anchors, key=lambda a: abs((a[0] if a[0] is not None else 1e9) - rv))[1]
            info[h]["rise_label"] = cand


def _resolve_rise_label(info: Dict[str, Dict[str, Any]]) -> None:
    _rise_step6(info)
    for entry in info.values():
        if not entry["rise_label"]:
            entry["rise_label"] = rise_label_rule7(entry["inseam_label"], entry["desc_match"], petite=False)
    _rise_step6(info)
    for entry in info.values():
        if not entry["rise_label"]:
            entry["rise_label"] = rise_label_rule7(entry["inseam_label"], entry["desc_match"], petite=True)
    for entry in info.values():
        if not entry["rise_label"]:
            entry["rise_label"] = rise_label_from_ss(entry["ss_rise"])


def _cross_fill(info: Dict[str, Dict[str, Any]], field: str, key_field: str) -> None:
    groups: Dict[str, List[str]] = defaultdict(list)
    for handle, entry in info.items():
        groups[entry[key_field]].append(handle)
    for key, handles in groups.items():
        if not key:
            continue
        values = [info[h][field] for h in handles if info[h][field]]
        if not values or len(set(values)) != 1:
            continue
        common = values[0]
        for h in handles:
            if not info[h][field]:
                info[h][field] = common


# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------
def assemble_rows() -> List[Dict[str, Any]]:
    products = fetch_all_products()
    ss_by_handle, ss_variant_map = fetch_searchspring()

    # Filter out non-jean products before doing any per-product work.
    kept: List[Dict[str, Any]] = []
    for product in products:
        title = product.get("title", "")
        reason = title_is_excluded(title)
        if reason:
            log(f"[filter] drop '{title}' (title contains '{reason}')")
            continue
        description = clean_description(product.get("description"))
        reason = description_is_excluded(description)
        if reason:
            log(f"[filter] drop '{title}' (description contains '{reason}')")
            continue
        ptype = product_type_from_ss(ss_by_handle.get(product.get("handle"), {}))
        if ptype and any(ptype.strip().lower() == ex.lower() for ex in PRODUCT_TYPE_EXCLUDE):
            log(f"[filter] drop '{title}' (product type '{ptype}')")
            continue
        kept.append(product)
    log(f"[filter] kept {len(kept)} of {len(products)} products")

    info = build_style_info(kept, ss_by_handle)

    rows: List[Dict[str, Any]] = []
    for product in kept:
        handle = product.get("handle")
        style = info.get(handle)
        if not style:
            continue
        product_title = style["title"]  # normalized (single spaces, straight quotes)
        online_url = product.get("onlineStoreUrl") or f"{HOSTS[1]}/products/{handle}"
        image_url = product_image_url(product)
        published_at = product.get("publishedAt", "") or ""
        created_at = product.get("createdAt", "") or ""
        tags = ", ".join(product.get("tags", []) or [])
        total_inventory = product.get("totalInventory")

        for edge in (product.get("variants") or {}).get("edges", []):
            variant = edge["node"]
            variant_id = parse_shopify_id(variant.get("id"))
            size = variant_size(variant)
            variant_title = f"{product_title} - {size}" if size else variant.get("title", "")

            qty_available = variant.get("quantityAvailable")
            ss_variant = ss_variant_map.get(variant_id, {})
            ga_purchases = ""
            ss_inv = coerce_int(ss_variant.get("inventory_quantity"))
            gql_qty = coerce_int(qty_available)
            if ss_inv is not None and gql_qty is not None:
                ga_purchases = max(0, ss_inv - gql_qty)
            purchase_reset = ss_variant.get("updated_at") or ""

            rows.append({
                "Style Id": style["style_id"],
                "Handle": handle,
                "Published At": published_at,
                "Created At": created_at,
                "Product": product_title,
                "Style Name": style["style_name"],
                "Product Type": style["product_type"],
                "Tags": tags,
                "Vendor": product.get("vendor", ""),
                "Description": style["description"],
                "Variant Title": variant_title,
                "Color": style["color"],
                "Size": size,
                "Rise": style["rise"],
                "Inseam": style["inseam"],
                "Leg Opening": style["leg_opening"],
                "Price": money(variant.get("price")),
                "Compare at Price": money(variant.get("compareAtPrice")),
                "Available for Sale": "TRUE" if variant.get("availableForSale") else "FALSE",
                "Quantity Available": "" if qty_available is None else qty_available,
                "Google Analytics Purchases": ga_purchases,
                "Purchase Reset Date": purchase_reset,
                "Quantity of style": "" if total_inventory is None else total_inventory,
                "SKU - Shopify": variant_id,
                "SKU - Brand": variant.get("sku", "") or "",
                "Barcode": variant.get("barcode", "") or "",
                "Image URL": image_url,
                "SKU URL": online_url,
                "Jean Style": style["jean_style"],
                "Hem Style": style["hem_style"],
                "Inseam Label": style["inseam_label"],
                "Inseam Style": style["inseam_style"],
                "Rise Label": style["rise_label"],
                "Color - Simplified": style["color_simp"],
                "Color - Standardized": style["color_std"],
                "Stretch": style["stretch"],
            })
    return rows


def write_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        raise ValueError("No rows to write")
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    path = os.path.join(OUTPUT_DIR, f"MOTHER_{timestamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    log(f"[csv] wrote {len(rows)} rows -> {path}")
    return path


def run_scraper() -> None:
    start = time.time()
    rows = assemble_rows()
    if not rows:
        log("[warn] no rows collected; skipping CSV")
        return
    path = write_csv(rows)
    log(f"[done] completed in {time.time() - start:.1f}s -> {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Mother Denim inventory exporter")
    parser.parse_args()
    run_scraper()


if __name__ == "__main__":
    main()
