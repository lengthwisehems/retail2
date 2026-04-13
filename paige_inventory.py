import csv
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
BRAND = "PAIGE"
LOG_FILE = BASE_DIR / f"{BRAND}_run.log"

HOST_ROTATION = [
    "https://paige-7873.myshopify.com",
    "https://shop.paige.com",
    "https://paige.com",
    "https://www.paige.com",
]

GRAPHQL_ENDPOINT = "https://paige-7873.myshopify.com/api/unstable/graphql.json"
GRAPHQL_TOKEN_ENV_VARS = ["x-shopify-storefront-access-token", "X_SHOPIFY_STOREFRONT_ACCESS_TOKEN"]
GRAPHQL_TOKEN = "383d494a76122b5e6cadffc2c7667ef2"
COLLECTION_HANDLES = ["women-denim", "women-sale"]

ALGOLIA_APP_ID = "DK4YY42827"
ALGOLIA_API_KEY = "333da36aea28227274c0ad598d0fbdb0"
ALGOLIA_INDEX = "production_products"
ALGOLIA_SEARCH_URL = f"https://{ALGOLIA_APP_ID.lower()}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

REQUIRED_TAG_HINTS = {
    "clothingtype:jeans",
    "clothingtypecode:jean",
    "category:denim bottoms",
    "clothingtype:pant",
}

STYLE_REMOVE_TERMS = [
    "accent hardware", "ankle", "belted", "coated", "corduroy", "crop", "crushed", "crystal", "cuff",
    "cuffed", "cutoff", "darted", "destroyed", "fit", "flag", "flap pocket", "flap", "flip", "frayed seam",
    "front yoke", "frontier", "high rise", "inch", "inset", "jean", "krystal", "leather", "lightweight",
    "lo", "low and loose", "low rise", "mid rise", "panel", "pant", "patch", "petite", "pintucked", "plaid",
    "plus", "raw hem", "renaissance", "repair", "rinse", "ripped", "saddle", "seam", "seamed front yoke",
    "seamed", "selvedge", "sequin", "side seam snaps", "skimmer", "slice", "snake print", "sott", "spark",
    "spliced", "split", "studded", "super", "track pant", "trashed", "trouser", "vegan leather", "vent", "slit",
    "w/ contrast front panel", "w/ cuff", "w/ flap jean", "w/ slit hem", "w/ stud detailing", "w/ wide cuff",
    "w/flap", "wax", "welt pocket", "with cuff", "with frayed seam", "zipper",
]

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
    "Quantity Available (Instore Inventory)",
    "Quantity Available (Online Inventory)",
    "Google Analytics Purchases",
    "Returns",
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

GRAPHQL_QUERY = """
query CollectionProducts($handle: String!, $cursor: String) {
  collection(handle: $handle) {
    products(first: 250, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        handle
        title
        createdAt
        publishedAt
        productType
        vendor
        tags
        description
        onlineStoreUrl
        totalInventory
        featuredImage { url }
        variants(first: 250) {
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
  }
}
"""


def ensure_paths() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOG_FILE.touch(exist_ok=True)


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").replace("\xa0", " ")).strip()


def parse_gid(gid: Optional[str]) -> str:
    if not gid:
        return ""
    return gid.split("/")[-1]


def fmt_date(iso: Optional[str]) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except Exception:
        return ""


def to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").replace("$", ""))
    except Exception:
        return None


def fmt_money(value: Any) -> str:
    num = to_float(value)
    if num is None:
        return ""
    if abs(num - round(num)) < 1e-9:
        return f"${int(round(num)):,}"
    return f"${num:,.2f}"


def contains_required_tag(tags: Iterable[str]) -> bool:
    merged = "|".join(t.lower() for t in tags)
    return any(h in merged for h in REQUIRED_TAG_HINTS)


def title_has_denim_words(title: str) -> bool:
    t = title.lower()
    return "jean" in t or "pant" in t


def normalize_fraction_to_decimal(token: str) -> str:
    token = (token or "").strip().replace("½", " 1/2").replace("¼", " 1/4").replace("¾", " 3/4").replace("⅛", " 1/8")
    if not token:
        return ""
    if " " in token and "/" in token:
        a, b = token.split(" ", 1)
        n, d = b.split("/", 1)
        val = float(a) + float(n) / float(d)
        return f"{val:.3f}"
    if "/" in token:
        n, d = token.split("/", 1)
        val = float(n) / float(d)
        return f"{val:.3f}"
    try:
        return f"{float(token):.3f}"
    except Exception:
        return token


def parse_after_labels(text: str, labels: List[str]) -> str:
    if not text:
        return ""
    src = text
    for lab in labels:
        m = re.search(rf"(?i){re.escape(lab)}\s*[:\-]?\s*([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?)", src)
        if m:
            return normalize_fraction_to_decimal(m.group(1))
    return ""


def parse_leg_opening(text: str) -> str:
    return parse_after_labels(text, ["Leg Opening", "Hem circumference", "Hem"])


def base_title(product_title: str) -> str:
    return re.split(r"\s*-\s*", product_title or "")[0].strip()


def color_from_title(product_title: str) -> str:
    parts = re.split(r"\s*-\s*", product_title or "")
    return parts[-1].strip() if len(parts) > 1 else ""


def build_style_name_initial(product_title: str) -> str:
    title = base_title(product_title)
    title = title.replace('"', " ")
    title = re.sub(r"\d+", " ", title)
    lowered = title.lower()
    for term in sorted(STYLE_REMOVE_TERMS, key=len, reverse=True):
        lowered = re.sub(rf"(?i)\b{re.escape(term)}\b", " ", lowered)
    cleaned = normalize_text(lowered)
    return cleaned.title()


def map_product_type(title: str, tags: List[str]) -> str:
    txt = title.lower()
    merged = ",".join(tags).lower()
    if "jean" in txt:
        return "Jeans"
    if any(k in merged for k in ["clothingtype:jeans", "clothingtypecode:jean", "category:denim bottoms"]):
        return "Jeans"
    if "pant" in txt:
        return "Pants"
    if any(k in merged for k in ["clothingtype:pant", "clothingtypecode:pan"]):
        return "Pants"
    return ""


def option_value(variant: Dict[str, Any], name: str) -> str:
    for opt in variant.get("selectedOptions", []):
        if str(opt.get("name", "")).lower() == name.lower():
            return normalize_text(str(opt.get("value", "")))
    return ""


def classify_straight_by_leg_opening(leg_opening: str) -> str:
    lo = to_float(leg_opening)
    if lo is None:
        return "Straight from Knee/Thigh"
    if lo < 15.5:
        return "Straight from Knee"
    if lo <= 17:
        return "Straight from Knee/Thigh"
    return "Straight from Thigh"


def _contains_any_whole(text: str, phrases: List[str]) -> bool:
    return any(re.search(rf"(?i)\b{re.escape(p)}\b", text or "") for p in phrases)


def infer_jean_style(title: str, description: str, fit: str, leg_opening: str) -> str:
    t = (title or "")
    d = (description or "")
    f = (fit or "")
    if _contains_any_whole(t, ["barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe"]):
        return "Barrel"
    if _contains_any_whole(t, ["tapered", "mom"]):
        return "Tapered"
    if _contains_any_whole(t, ["baggy"]):
        return "Baggy"
    if _contains_any_whole(t, ["flare"]):
        return "Flare"
    if _contains_any_whole(t, ["bootcut", "boot"]):
        return "Bootcut"
    if _contains_any_whole(t, ["skinny"]):
        return "Skinny"
    if _contains_any_whole(t, ["wide leg"]):
        return "Wide Leg"
    if _contains_any_whole(t, ["cigarette", "slim"]):
        return "Straight from Knee"
    if _contains_any_whole(t, ["straight"]):
        return classify_straight_by_leg_opening(leg_opening)

    if _contains_any_whole(d, ["barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe"]):
        return "Barrel"
    if _contains_any_whole(d, ["skinny"]):
        return "Skinny"
    if _contains_any_whole(d, ["flare"]):
        return "Flare"
    if _contains_any_whole(d, ["bootcut"]):
        return "Bootcut"
    if _contains_any_whole(d, ["taper", "tapering", "tapered"]):
        return "Tapered"
    if _contains_any_whole(d, ["wide leg", "wide-leg", "palazzo"]):
        return "Wide Leg"
    if _contains_any_whole(d, ["straight"]):
        return classify_straight_by_leg_opening(leg_opening)
    if _contains_any_whole(d, ["baggy", "loose fit"]):
        return "Baggy"

    if _contains_any_whole(f, ["barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe"]):
        return "Barrel"
    if _contains_any_whole(f, ["skinny"]):
        return "Skinny"
    if _contains_any_whole(f, ["flare"]):
        return "Flare"
    if _contains_any_whole(f, ["bootcut"]):
        return "Bootcut"
    if _contains_any_whole(f, ["taper", "tapering", "tapered"]):
        return "Tapered"
    if _contains_any_whole(f, ["wide leg", "wide-leg", "palazzo"]):
        return "Wide Leg"
    if _contains_any_whole(f, ["cigarette", "slim"]):
        return "Straight from Knee"
    if _contains_any_whole(f, ["straight"]):
        return classify_straight_by_leg_opening(leg_opening)
    if _contains_any_whole(f, ["baggy"]):
        return "Baggy"
    return ""


def infer_inseam_label(title: str, desc: str, jean_style: str, inseam: str) -> str:
    txt = f"{title} {desc}".lower()
    if "petite" in txt:
        return "Petite"
    if "longer" in txt:
        return "Long"
    ins = to_float(inseam)
    if ins is not None:
        if jean_style in {"Barrel", "Bootcut", "Flare", "Straight from Thigh", "Baggy", "Straight from Knee/Thigh"} and ins >= 33:
            return "Long"
        if jean_style in {"Skinny", "Tapered", "Straight from Knee"} and ins >= 30:
            return "Long"
    return "Regular"


def infer_inseam_style(jean_style: str, inseam_label: str, inseam: str, meta_length: str) -> str:
    ins = to_float(inseam)
    label = inseam_label or ""
    group_wide = {"Barrel", "Bootcut", "Flare", "Straight from Thigh", "Baggy", "Straight from Knee/Thigh"}
    group_slim = {"Skinny", "Tapered", "Straight from Knee"}

    if ins is not None:
        if jean_style in group_wide:
            if label == "Petite":
                if ins <= 25:
                    return "Cropped"
                if ins <= 28:
                    return "Ankle"
                return "Full Length"
            if label in {"Regular", ""}:
                if ins <= 28:
                    return "Cropped"
                if ins < 31:
                    return "Ankle"
                if ins < 33:
                    return "Full Length"
            if label in {"Long", ""} and ins >= 33:
                return "Full Length"
        if jean_style in group_slim:
            if label == "Petite":
                return "Cropped" if ins < 25 else "Full Length"
            if label in {"Regular", ""}:
                return "Cropped" if ins < 27 else ("Full Length" if ins < 30 else "")
            if label in {"Long", ""} and ins >= 30:
                return "Full Length"

    ml = (meta_length or "").strip()
    if jean_style in group_slim and ml.lower() == "ankle":
        return "Full Length"
    if jean_style in group_wide and ml:
        return ml
    return ""


def infer_rise_label(title: str, handle: str, desc: str, meta_rise: str) -> str:
    t = (title or "").lower()
    h = (handle or "").lower()
    d = (desc or "").lower()
    m = (meta_rise or "").lower()
    if any(k in t for k in ["super low rise", "super low-rise", "ultra low rise", "ultra low-rise", "super low waist", "super low-waist", "ultra low waist", "ultra low-waist"]):
        return "Ultra Low"
    if any(k in t for k in ["super high rise", "super high-rise", "ultra high rise", "ultra high-rise", "super high waist", "super high-waist", "ultra high waist", "ultra high-waist"]):
        return "Ultra High"
    if any(k in t for k in ["mid-rise", "mid rise"]):
        return "Mid"
    if any(k in t for k in ["low-rise", "low rise"]):
        return "Low"
    if any(k in t for k in ["high-rise", "high rise"]):
        return "High"

    if any(k in h for k in ["superlowrise", "super-low-rise", "superlow-rise", "super-lowrise", "super-lr", "superlr", "-slr-", "ultralowrise", "ultra-low-rise", "ultra-lowrise", "ultralow-rise", "ultra-lr", "ultralr", "-ulr-", "super-low-waist", "superlow-waist", "super-lowwaist", "superlowwaist", "superlw", "super-lw", "-slw-", "ultralowwaist", "ultra-low-waist", "ultralow-waist", "ultra-lowwaist", "ultra-lw", "ultralw", "-ulw-"]):
        return "Ultra Low"
    if any(k in h for k in ["superhighrise", "super-high-rise", "superhigh-rise", "super-highrise", "super-hr", "superhr", "-shr-", "ultrahighrise", "ultra-high-rise", "ultra-highrise", "ultrahigh-rise", "ultra-hr", "ultrahr", "-uhr-", "super-high-waist", "superhigh-waist", "super-highwaist", "superhighwaist", "superhw", "super-hw", "-shw-", "ultrahighwaist", "ultra-high-waist", "ultrahigh-waist", "ultra-highwaist", "ultra-hw", "ultrahw", "-uhw-"]):
        return "Ultra High"
    if any(k in h for k in ["mid-rise", "mid-waist", "-mr-", "-mw-"]):
        return "Mid"
    if any(k in h for k in ["low-rise", "low-waist", "-lr-", "-lw-"]):
        return "Low"
    if any(k in h for k in ["high-rise", "high-waist", "-hr-", "-hw-"]):
        return "High"

    if any(k in d for k in ["rise: super low", "rise: ultra low", "rise - super low", "rise - ultra low", "super low rise", "super low-rise", "ultra low rise", "ultra low-rise", "super low waisted", "super low-waisted", "ultra low waisted", "ultra low-waisted", "super low waist", "super low-waist", "ultra low waist", "ultra low-waist"]):
        return "Ultra Low"
    if any(k in d for k in ["rise: super high", "rise: ultra high", "rise - super high", "rise - ultra high", "super high rise", "super high-rise", "ultra high rise", "ultra high-rise", "super high waist", "super high-waist", "ultra high waist", "ultra high-waist", "super high waisted", "super high-waisted", "ultra high waisted", "ultra high-waisted"]):
        return "Ultra High"
    if any(k in d for k in ["rise: mid", "rise - mid", "mid-rise", "mid rise"]):
        return "Mid"
    if any(k in d for k in ["rise: low", "rise - low", "low-rise", "low rise", "low on the hip", "low on the waist", "low waist", "low waisted", "low-waisted"]):
        return "Low"
    if any(k in d for k in ["rise: high", "rise - high", "high-rise", "high rise", "high waist", "high waisted", "high-waisted", "high-waist", "high on the hip", "high on the waist"]):
        return "High"

    if "mid" in m:
        return "Mid"
    if "low" in m:
        return "Low"
    if "high" in m:
        return "High"
    return ""


def infer_hem_style(desc: str) -> str:
    d = (desc or "").lower()
    if any(k in d for k in ["split hem", "side slits", "slit at the hem"]):
        return "Split Hem"
    if "released hem" in d:
        return "Released Hem"
    if any(k in d for k in ["raw hem", "raw-edge hem", "raw edge hem"]):
        return "Raw Hem"
    if any(k in d for k in ["clean hem", "clean-edge hem", "finished hem"]):
        return "Clean Hem"
    if "wide hem" in d or "trouser hem" in d:
        return "Wide Hem"
    if "distressed hem" in d:
        return "Distressed Hem"
    return "Clean Hem" if d else ""


def infer_color_standardized(color: str, desc: str, tags: str) -> str:
    def has_word(hay: str, w: str) -> bool:
        return re.search(rf"(?i)\b{re.escape(w)}\b", hay or "") is not None

    c = color or ""
    d = desc or ""
    t = tags or ""
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["leopard", "snake", "animal"]):
            return "Animal Print"
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["blue", "indigo", "denim"]):
            return "Blue"
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["black", "noir", "raven"]):
            return "Black"
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["brown", "coffee", "espresso"]):
            return "Brown"
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["green", "olive", "sage"]):
            return "Green"
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["grey", "gray", "smoke"]):
            return "Grey"
    for hay in [c, d, t]:
        if any(has_word(hay, w) for w in ["white", "ecru", "cream"]):
            return "White"
    if any(has_word(c, w) for w in ["tan", "beige", "khaki"]):
        return "Tan"
    return ""


def infer_color_simplified(color_std: str, color: str, desc: str, tags: str) -> str:
    cs = (color_std or "").lower()
    c = (color or "").lower()
    d = (desc or "").lower()
    t = (tags or "").lower()
    if cs in {"black", "brown"}:
        return "Dark"
    if cs in {"white", "tan"}:
        return "Light"
    if any(k in c for k in ["dark", "deep", "navy", "burgundy", "wine"]):
        return "Dark"
    if any(k in c for k in ["light", "cream", "pastel"]):
        return "Light"
    if any(k in c for k in ["mid", "medium"]):
        return "Medium"
    if any(k in d for k in ["light to medium", "light-to-medium", "medium-light", "light medium"]):
        return "Light to Medium"
    if any(k in d for k in ["medium to dark", "medium-to-dark", "dark-medium", "medium dark"]):
        return "Medium to Dark"
    if any(k in d for k in ["dark", "black", "navy"]):
        return "Dark"
    if any(k in d for k in ["light", "khaki", "tan", "ivory", "white"]):
        return "Light"
    if any(k in d for k in ["medium", "mid blue", "classic blue"]):
        return "Medium"
    if "filter_wash_med" in t and "filter_wash_dark" in t:
        return "Medium to Dark"
    if "filter_wash_med" in t and "filter_wash_light" in t:
        return "Light to Medium"
    if any(k in t for k in ["filter_wash_dark", " dark"]):
        return "Dark"
    if any(k in t for k in ["filter_wash_light", " light"]):
        return "Light"
    if any(k in t for k in ["filter_wash_med", " medium", " med"]):
        return "Medium"
    return ""


def infer_stretch(desc: str, tags: str) -> str:
    txt = f"{desc} {tags}".lower()
    if "rigid" in txt:
        return "Rigid"
    if "super stretch" in txt:
        return "High Stretch"
    if "comfort stretch" in txt:
        return "Low Stretch"
    if "stretch" in txt:
        return "Medium Stretch"
    return ""


def parse_measurement(text: str, labels: List[str]) -> str:
    if not text:
        return ""
    src = text.replace("½", " 1/2").replace("¼", " 1/4").replace("¾", " 3/4").replace("⅛", " 1/8")
    for lab in labels:
        patt = rf"(?i){re.escape(lab)}\s*[:\-]?\s*(?:[A-Za-z]+\s*)?(?:\(?\s*)?([0-9]+(?:\.[0-9]+)?(?:\s+[0-9]+/[0-9]+)?)\s*(?:\"|in|inch|inches)?"
        m = re.search(patt, src)
        if not m:
            continue
        token = m.group(1).strip()
        return normalize_fraction_to_decimal(token)
    return ""


def parse_pdp(handle: str, session: requests.Session) -> Dict[str, str]:
    detail_selectors = [
        "[id^='headlessui-disclosure-panel-']",
        "[data-testid*='details']",
        "[data-test*='details']",
        "[data-qa*='details']",
        "[aria-labelledby*='details']",
        "[id*='details']",
        ".product-details",
        ".product-detail",
    ]
    for host in HOST_ROTATION:
        pdp_urls = [
            f"{host.rstrip('/')}/products/{handle}",
            f"{host.rstrip('/')}/products/{handle}?view=details",
            f"{host.rstrip('/')}/products/{handle}?modals=details_modal",
        ]
        for url in pdp_urls:
            try:
                r = session.get(url, timeout=30)
                if r.status_code == 404:
                    continue
                r.raise_for_status()
                html = r.text
                soup = BeautifulSoup(html, "html.parser")

                details: List[str] = []
                for sel in detail_selectors:
                    for node in soup.select(sel):
                        panel_text = normalize_text(node.get_text(" ", strip=True))
                        if panel_text and panel_text not in details:
                            details.append(panel_text)

                details_text = ". ".join(details)
                full_text = normalize_text(soup.get_text(" ", strip=True))
                source = f"{details_text}. {full_text}" if details_text else full_text
                stretch = ""
                scale = soup.find(class_=re.compile(r"womenStretchScale"))
                if scale:
                    dots = scale.find_all(class_=re.compile(r"__dot"))
                    for idx, dot in enumerate(dots, start=1):
                        classes = " ".join(dot.get("class", []))
                        if "__active" in classes:
                            stretch = {
                                1: "High Stretch",
                                3: "Medium to High Stretch",
                                5: "Medium Stretch",
                                7: "Low Stretch",
                                9: "Low Stretch",
                            }.get(idx, "")
                            break
                return {
                    "description_extra": details_text,
                    "rise": parse_after_labels(source, ["Front Rise", "Rise"]),
                    "inseam": parse_after_labels(source, ["Inseam", "Inleg"]),
                    "leg_opening": parse_leg_opening(source),
                    "stretch": stretch or infer_stretch(source, ""),
                }
            except Exception:
                continue
    return {"description_extra": "", "rise": "", "inseam": "", "leg_opening": "", "stretch": ""}


class PaigeInventory:
    def __init__(self) -> None:
        ensure_paths()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept": "application/json,text/html,*/*",
        })

    def algolia_query(self, params: Dict[str, str]) -> Dict[str, Any]:
        query = "&".join(f"{k}={requests.utils.quote(str(v))}" for k, v in params.items())
        r = self.session.post(
            ALGOLIA_SEARCH_URL,
            headers={
                "X-Algolia-Application-Id": ALGOLIA_APP_ID,
                "X-Algolia-API-Key": ALGOLIA_API_KEY,
                "Content-Type": "application/json",
            },
            json={"params": query},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def fetch_collection_handles(self) -> set[str]:
        handles: set[str] = set()
        for host in ["https://shop.paige.com", "https://paige.com"]:
            for coll in COLLECTION_HANDLES:
                page = 1
                while True:
                    url = f"{host}/collections/{coll}/products.json"
                    try:
                        r = self.session.get(url, params={"limit": 250, "page": page}, timeout=30)
                        if r.status_code != 200:
                            break
                        obj = r.json()
                        products = obj.get("products", [])
                        if not products:
                            break
                        for p in products:
                            h = p.get("handle")
                            if h:
                                handles.add(str(h))
                        page += 1
                    except Exception:
                        break
        log(f"Collection JSON handles loaded: {len(handles)}")
        return handles

    def fetch_graphql_collection(self, handle: str) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        cursor = None
        headers = {
            "x-shopify-storefront-access-token": GRAPHQL_TOKEN,
            "Content-Type": "application/json",
        }
        while True:
            payload = {"query": GRAPHQL_QUERY, "variables": {"handle": handle, "cursor": cursor}}
            r = self.session.post(GRAPHQL_ENDPOINT, headers=headers, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            if data.get("errors"):
                raise RuntimeError(str(data["errors"]))
            coll = (((data.get("data") or {}).get("collection") or {}).get("products") or {})
            nodes = coll.get("nodes") or []
            products.extend(nodes)
            page_info = coll.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return products

    def fetch_graphql(self) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, set[str]], bool]:
        by_handle: Dict[str, Dict[str, Any]] = {}
        collections_by_handle: Dict[str, set[str]] = {}
        try:
            for coll in COLLECTION_HANDLES:
                items = self.fetch_graphql_collection(coll)
                log(f"GraphQL collection {coll}: {len(items)} products")
                for p in items:
                    h = p.get("handle")
                    if h:
                        hs = str(h)
                        by_handle[hs] = p
                        collections_by_handle.setdefault(hs, set()).add(coll)
            return by_handle, collections_by_handle, True
        except Exception as exc:
            log(f"GraphQL failed; falling back to Algolia. Error: {exc}")
            return by_handle, collections_by_handle, False

    def fetch_algolia_styles(self) -> Dict[str, Dict[str, Any]]:
        page = 0
        styles: Dict[str, Dict[str, Any]] = {}
        filt = "collections:women-denim OR collections:women-sale"
        while True:
            obj = self.algolia_query({"filters": filt, "distinct": "true", "hitsPerPage": "1000", "page": str(page)})
            hits = obj.get("hits", [])
            for h in hits:
                handle = h.get("handle")
                if handle:
                    styles[str(handle)] = h
            page += 1
            if page >= int(obj.get("nbPages", 0)):
                break
        log(f"Algolia styles loaded: {len(styles)}")
        return styles

    def fetch_algolia_variants(self) -> Dict[str, Dict[str, Any]]:
        variants: Dict[str, Dict[str, Any]] = {}
        shard_filters = [
            "(collections:women-sale)",
            "(collections:women-denim AND inventory_quantity<=0)",
            "(collections:women-denim AND inventory_quantity=1)",
            "(collections:women-denim AND inventory_quantity>=2 AND inventory_quantity<=3)",
            "(collections:women-denim AND inventory_quantity>=4 AND inventory_quantity<=6)",
            "(collections:women-denim AND inventory_quantity>=7 AND inventory_quantity<=10)",
            "(collections:women-denim AND inventory_quantity>=11 AND inventory_quantity<=15)",
            "(collections:women-denim AND inventory_quantity>=16 AND inventory_quantity<=25)",
            "(collections:women-denim AND inventory_quantity>=26)",
        ]
        for shard in shard_filters:
            page = 0
            while True:
                obj = self.algolia_query({"filters": shard, "distinct": "false", "hitsPerPage": "1000", "page": str(page)})
                hits = obj.get("hits", [])
                if not hits:
                    break
                for h in hits:
                    oid = str(h.get("objectID", ""))
                    if oid:
                        variants[oid] = h
                page += 1
                if page >= int(obj.get("nbPages", 0)):
                    break
        log(f"Algolia variants loaded: {len(variants)}")
        return variants

    def passes_filter(self, title: str, tags: List[str], collections: set[str]) -> bool:
        if "women-denim" in collections:
            return True
        if "women-sale" in collections:
            return contains_required_tag(tags) and title_has_denim_words(title)
        return contains_required_tag(tags) and title_has_denim_words(title)

    def run(self) -> Path:
        collection_handles = self.fetch_collection_handles()
        gql_by_handle, gql_collections, gql_ok = self.fetch_graphql()
        algolia_styles = self.fetch_algolia_styles()
        algolia_variants = self.fetch_algolia_variants()

        # ensure coverage from collections
        for h in collection_handles:
            if h not in gql_by_handle and h in algolia_styles:
                gql_by_handle[h] = {"_algolia": algolia_styles[h]}
                colls = set(str(c) for c in (algolia_styles[h].get("collections") or []))
                gql_collections[h] = colls

        rows: List[Dict[str, str]] = []
        pdp_cache: Dict[str, Dict[str, str]] = {}

        for handle, product in sorted(gql_by_handle.items()):
            if product.get("_algolia"):
                style_hit = product["_algolia"]
                title = str(style_hit.get("title", ""))
                tags = [str(t) for t in style_hit.get("tags", [])]
                collections = gql_collections.get(handle, set(str(c) for c in (style_hit.get("collections") or [])))
                if not self.passes_filter(title, tags, collections):
                    continue
                variant_nodes = [v for v in algolia_variants.values() if str(v.get("handle")) == handle]
                if not variant_nodes:
                    continue
                p = {
                    "id": f"gid://shopify/Product/{style_hit.get('id')}",
                    "handle": handle,
                    "title": title,
                    "createdAt": style_hit.get("created_at"),
                    "publishedAt": style_hit.get("published_at"),
                    "productType": style_hit.get("product_type", ""),
                    "vendor": style_hit.get("vendor", ""),
                    "tags": tags,
                    "description": style_hit.get("body_html_safe", "") or "",
                    "onlineStoreUrl": f"https://shop.paige.com/products/{handle}",
                    "totalInventory": style_hit.get("variants_inventory_count", ""),
                    "featuredImage": {"url": style_hit.get("product_image", "")},
                    "variants": {"nodes": []},
                }
                for v in variant_nodes:
                    p["variants"]["nodes"].append(
                        {
                            "id": f"gid://shopify/ProductVariant/{v.get('objectID')}",
                            "title": v.get("title", ""),
                            "sku": v.get("sku", ""),
                            "barcode": v.get("barcode", ""),
                            "availableForSale": bool(v.get("inventory_available")),
                            "quantityAvailable": v.get("inventory_quantity", ""),
                            "price": {"amount": v.get("price")},
                            "compareAtPrice": {"amount": v.get("compare_at_price")},
                            "selectedOptions": [
                                {"name": "Size", "value": v.get("option1", "")},
                                {"name": "Color", "value": (v.get("meta", {}).get("attributes", {}).get("colorCategory") or "")},
                            ],
                        }
                    )
            else:
                p = product
                title = str(p.get("title", ""))
                tags = [str(t) for t in p.get("tags", [])]
                collections = gql_collections.get(handle, set())
                if not self.passes_filter(title, tags, collections):
                    continue

            if handle not in pdp_cache:
                pdp_cache[handle] = parse_pdp(handle, self.session)
            pdp = pdp_cache[handle]
            base_desc = normalize_text(str(p.get("description", "")))
            full_desc = normalize_text(f"{base_desc}. {pdp.get('description_extra','')}")
            tag_list = [str(t) for t in p.get("tags", [])]
            tags_joined = ", ".join(tag_list)
            style_name = re.split(r"\s*-\s*", title)[0].strip()
            style_name = build_style_name_initial(title)
            product_type = str(p.get("productType", ""))
            mapped_pt = map_product_type(title, tag_list)
            if mapped_pt:
                product_type = mapped_pt
            quantity_of_style = p.get("totalInventory", "")
            product_line = "Maternity" if "maternity" in title.lower() else ""
            site_exclusive = "Yes" if any("site" in t.lower() and "exclusive" in t.lower() for t in tag_list) else ""
            image_url = ((p.get("featuredImage") or {}).get("url") or "")
            sku_url = p.get("onlineStoreUrl") or f"https://shop.paige.com/products/{handle}"

            for v in ((p.get("variants") or {}).get("nodes") or []):
                sku_shop = parse_gid(v.get("id"))
                a = algolia_variants.get(sku_shop, {})
                color = option_value(v, "Color") or option_value(v, "colour")
                if not color:
                    parts = re.split(r"\s*-\s*", title)
                    if len(parts) > 1:
                        color = parts[-1].strip()
                if not color:
                    for tg in tag_list:
                        if tg.lower().startswith("washname:"):
                            color = tg.split(":", 1)[1].strip()
                            break
                size = option_value(v, "Size") or option_value(v, "Waist") or str(a.get("option1", ""))
                variant_title = f"{title} - {size}".strip(" -")
                meta = a.get("meta", {}).get("attributes", {})
                rise = (
                    pdp.get("rise")
                    or parse_after_labels(full_desc, ["Front Rise", "Rise"])
                    or parse_measurement(str(meta.get("rise", "")), ["rise"])
                    or parse_measurement(str(meta.get("rise", "")), [""])
                )
                inseam = (
                    option_value(v, "Inseam")
                    or pdp.get("inseam")
                    or parse_after_labels(full_desc, ["Inseam Options", "Inseam", "Inleg"])
                    or parse_measurement(str(meta.get("inseam", "")), ["inseam"])
                    or parse_measurement(str(meta.get("length", "")), ["length"])
                    or parse_measurement(str(meta.get("inseam", "")), [""])
                )
                leg_opening = pdp.get("leg_opening") or parse_leg_opening(full_desc)
                jean_style = infer_jean_style(title, full_desc, str(meta.get("fit", "")), leg_opening)
                inseam_label = infer_inseam_label(title, full_desc, jean_style, inseam)
                inseam_style = infer_inseam_style(jean_style, inseam_label, inseam, str(meta.get("length", "")))
                rise_label = infer_rise_label(title, handle, full_desc, str(meta.get("rise", "")))
                hem_style = infer_hem_style(full_desc)
                color_std = infer_color_standardized(color, full_desc, tags_joined)
                color_simple = infer_color_simplified(color_std, color, full_desc, tags_joined)
                stretch = pdp.get("stretch") or infer_stretch(full_desc, tags_joined)
                country = str(meta.get("country", "") or "")
                if not country:
                    for tg in tag_list:
                        if tg.lower().startswith("country:"):
                            country = tg.split(":", 1)[1].strip()
                            break
                if not country:
                    country = "Unknown"
                production_cost = ""
                for tg in tag_list:
                    if tg.lower().startswith("productioncost:"):
                        production_cost = tg.split(":", 1)[1].strip()
                if not production_cost:
                    production_cost = str(meta.get("productionCost", "") or "")
                if not production_cost:
                    production_cost = "N/A"
                site_exclusive = "Exclusive" if "exclusive" in product_type.lower() else ""
                if not jean_style:
                    jean_style = "Straight from Knee/Thigh" if "straight" in title.lower() else ""
                if not inseam_style:
                    ins_val = to_float(inseam) or 0
                    if ins_val >= 32:
                        inseam_style = "Full Length"
                    elif ins_val and ins_val <= 29:
                        inseam_style = "Cropped"
                if not rise_label:
                    rise_label = infer_rise_label(style_name, handle, full_desc, str(meta.get("rise", "")))
                inv_qty = to_float(str(a.get("inventory_quantity", "")))
                if inv_qty is None:
                    inv_qty = to_float(str(v.get("quantityAvailable", ""))) or 0.0
                locations = a.get("locations_inventory", {}) or {}
                online_qty = to_float(str(locations.get("82416435483", "")))
                returns_qty = to_float(str(locations.get("100464689435", "")))
                online_qty = online_qty if online_qty is not None else to_float(str(a.get("inventory_quantity", ""))) or 0.0
                returns_qty = returns_qty if returns_qty is not None else 0.0
                instore_qty = int(round(inv_qty - (online_qty + returns_qty)))
                if instore_qty < 0:
                    instore_qty = 0

                rows.append(
                    {
                        "Style Id": parse_gid(p.get("id")),
                        "Handle": handle,
                        "Published At": fmt_date(p.get("publishedAt") or p.get("published_at")),
                        "Created At": fmt_date(p.get("createdAt") or p.get("created_at")),
                        "Product": title,
                        "Style Name": style_name,
                        "Product Type": product_type,
                        "Tags": tags_joined,
                        "Vendor": str(p.get("vendor", "")),
                        "Description": full_desc,
                        "Variant Title": variant_title,
                        "Color": color,
                        "Size": size,
                        "Rise": rise,
                        "Inseam": inseam,
                        "Leg Opening": leg_opening,
                        "Price": fmt_money((v.get("price") or {}).get("amount")),
                        "Compare at Price": fmt_money((v.get("compareAtPrice") or {}).get("amount")),
                        "Available for Sale": "TRUE" if v.get("availableForSale") else "FALSE",
                        "Quantity Available": str(v.get("quantityAvailable", "")),
                        "Quantity Available (Instore Inventory)": str(instore_qty),
                        "Quantity Available (Online Inventory)": str(int(round(online_qty))),
                        "Google Analytics Purchases": str(a.get("recently_ordered_count", 0)),
                        "Returns": str(int(round(returns_qty))),
                        "Quantity of style": str(quantity_of_style),
                        "SKU - Shopify": sku_shop,
                        "SKU - Brand": str(v.get("sku", "")),
                        "Barcode": str(v.get("barcode", "")),
                        "Product Line": product_line,
                        "Image URL": image_url,
                        "SKU URL": sku_url,
                        "Jean Style": jean_style,
                        "Inseam Label": inseam_label,
                        "Inseam Style": inseam_style,
                        "Rise Label": rise_label,
                        "Hem Style": hem_style,
                        "Color - Simplified": color_simple,
                        "Color - Standardized": color_std,
                        "Country Produced": country,
                        "Stretch": stretch,
                        "Production Cost": production_cost,
                        "Site Exclusive": site_exclusive,
                    }
                )

        # dedupe rows by sku-shopify
        dedup: Dict[str, Dict[str, str]] = {}
        for r in rows:
            key = r["SKU - Shopify"] or f"{r['Handle']}::{r['SKU - Brand']}::{r['Size']}"
            dedup[key] = r
        out_rows = list(dedup.values())

        # style-name normalization by first word + leg opening (except maternity)
        by_first: Dict[str, List[Dict[str, str]]] = {}
        for r in out_rows:
            first = (r["Style Name"].split() or [""])[0].lower()
            if first:
                by_first.setdefault(first, []).append(r)
        for first, grp in by_first.items():
            non_maternity = [r for r in grp if "maternity" not in r["Product"].lower()]
            if len(non_maternity) < 2:
                continue
            freq: Dict[str, int] = {}
            for r in non_maternity:
                freq[r["Style Name"]] = freq.get(r["Style Name"], 0) + 1
            top_name = max(freq, key=freq.get)
            top_leg_vals = {r["Leg Opening"] for r in non_maternity if r["Style Name"] == top_name and r["Leg Opening"]}
            if not top_leg_vals:
                continue
            for r in non_maternity:
                if r["Style Name"] != top_name and r["Leg Opening"] in top_leg_vals:
                    r["Style Name"] = top_name

        # one-word style name rule
        for r in out_rows:
            words = r["Style Name"].split()
            if len(words) != 1:
                continue
            first = words[0].lower()
            candidates = [x for x in out_rows if x is not r and x["Style Name"].lower().startswith(first + " ")]
            if candidates:
                same_leg = [x for x in candidates if x["Leg Opening"] and x["Leg Opening"] == r["Leg Opening"]]
                pick_pool = same_leg or candidates
                counts: Dict[str, int] = {}
                for c in pick_pool:
                    counts[c["Style Name"]] = counts.get(c["Style Name"], 0) + 1
                r["Style Name"] = max(counts, key=counts.get)
            else:
                js = r["Jean Style"] or "Straight from Knee/Thigh"
                r["Style Name"] = f"{r['Style Name']} {js.split()[0]}"

        # variant title depends on final style name
        for r in out_rows:
            r["Variant Title"] = f"{r['Product']} - {r['Size']}".strip(" -")

        # jean-style inference from matching style name + leg opening
        by_style_leg: Dict[Tuple[str, str], List[str]] = {}
        for r in out_rows:
            key = (r["Style Name"].strip().lower(), r["Leg Opening"].strip())
            if r["Jean Style"]:
                by_style_leg.setdefault(key, []).append(r["Jean Style"])
        for r in out_rows:
            if r["Jean Style"]:
                continue
            key = (r["Style Name"].strip().lower(), r["Leg Opening"].strip())
            vals = by_style_leg.get(key, [])
            if vals and len(set(vals)) == 1:
                r["Jean Style"] = vals[0]

        # backfill same-color standardized/simplified
        std_map: Dict[str, str] = {}
        sim_map: Dict[str, str] = {}
        for r in out_rows:
            c = r["Color"].strip().lower()
            if c and r["Color - Standardized"]:
                std_map[c] = r["Color - Standardized"]
            if c and r["Color - Simplified"]:
                sim_map[c] = r["Color - Simplified"]
        for r in out_rows:
            c = r["Color"].strip().lower()
            if c and not r["Color - Standardized"]:
                r["Color - Standardized"] = std_map.get(c, "")
            if c and not r["Color - Simplified"]:
                r["Color - Simplified"] = sim_map.get(c, "")

        for r in out_rows:
            if not r["Quantity Available (Instore Inventory)"]:
                r["Quantity Available (Instore Inventory)"] = "0"
            # petite duplicate rule: if same style+color+inseam exists for non-petite, blank inseam on petite
        key_non_petite = {(r["Style Name"], r["Color"], r["Inseam"]) for r in out_rows if "petite" not in r["Product"].lower() and r["Inseam"]}
        for r in out_rows:
            if "petite" in r["Product"].lower() and (r["Style Name"], r["Color"], r["Inseam"]) in key_non_petite:
                r["Inseam"] = ""

        for r in out_rows:
            if not r["Jean Style"]:
                r["Jean Style"] = "Straight from Knee/Thigh"
            if not r["Inseam Label"]:
                try:
                    r["Inseam Label"] = "Long" if float(r["Inseam"]) >= 32 else "Regular"
                except Exception:
                    r["Inseam Label"] = "Regular"
            if not r["Inseam Style"]:
                try:
                    v = float(r["Inseam"])
                except Exception:
                    v = 30
                r["Inseam Style"] = "Full Length" if v >= 32 else ("Cropped" if v <= 29 else "Ankle")
            if not r["Rise Label"]:
                rv = to_float(r["Rise"]) or 10
                r["Rise Label"] = "High" if rv >= 11.5 else ("Low" if rv <= 9.5 else "Mid")
            if not r["Color - Simplified"]:
                cs = (r["Color - Standardized"] or "").lower()
                if cs in {"black", "brown"}:
                    r["Color - Simplified"] = "Dark"
                elif cs in {"white", "tan"}:
                    r["Color - Simplified"] = "Light"
                else:
                    r["Color - Simplified"] = "Medium"

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_path = OUTPUT_DIR / f"{BRAND}_{ts}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_HEADERS)
            w.writeheader()
            w.writerows(out_rows)

        log(f"GraphQL used: {gql_ok}")
        log(f"Rows written: {len(out_rows)}")
        log(f"Output: {out_path}")
        return out_path


def main() -> None:
    PaigeInventory().run()


if __name__ == "__main__":
    main()
