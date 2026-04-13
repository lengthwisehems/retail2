import csv
import html
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, FeatureNotFound, Tag
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
CLICK_TARGET_TEXTS: List[str] = ["DETAILS"]
BROWSER_RENDER_ENABLED = True
BROWSER_RENDER_TIMEOUT_MS = 45000

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
    "Rise",
    "Inseam",
    "Leg Opening",
    "Variant Title",
    "Color",
    "Size",
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


def derive_jean_style(title: str, description: str, leg_opening: str, fit_hint: str = "") -> str:
    text_sources = [title.lower(), description.lower(), fit_hint.lower()]
    lo = to_float(leg_opening)

    def straight_bucket() -> str:
        if lo is None:
            return ""
        if lo < 15.5:
            return "Straight from Knee"
        if lo <= 17:
            return "Straight from Knee/Thigh"
        return "Straight from Thigh"

    if any(k in text_sources[0] for k in ("barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
        return "Barrel"
    if any(k in text_sources[0] for k in ("tapered", " mom ")):
        return "Tapered"
    if "baggy" in text_sources[0]:
        return "Baggy"
    if "flare" in text_sources[0]:
        return "Flare"
    if "bootcut" in text_sources[0] or " boot" in text_sources[0]:
        return "Bootcut"
    if "skinny" in text_sources[0]:
        return "Skinny"
    if "wide leg" in text_sources[0]:
        return "Wide Leg"
    if "cigarette" in text_sources[0] or "slim" in text_sources[0]:
        return "Straight from Knee"
    if "straight" in text_sources[0]:
        return straight_bucket()

    for text in text_sources[1:]:
        if any(k in text for k in ("barrel", "bowed", "bow leg", "stovepipe", "stove-pipe", "horseshoe")):
            return "Barrel"
        if "skinny" in text:
            return "Skinny"
        if "flare" in text:
            return "Flare"
        if "bootcut" in text:
            return "Bootcut"
        if any(k in text for k in ("taper", "tapering", "tapered")):
            return "Tapered"
        if any(k in text for k in ("wide leg", "wide-leg", "palazzo")):
            return "Wide Leg"
        if "straight" in text:
            bucket = straight_bucket()
            if bucket:
                return bucket
        if "baggy" in text or "loose fit" in text:
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
    if jean_style in {"Barrel", "Bootcut", "Flare", "Straight from Thigh", "Baggy", "Straight from Knee/Thigh"} and inseam_value >= 33:
        return "Long"
    if jean_style in {"Skinny", "Tapered", "Straight from Knee"} and inseam_value >= 30:
        return "Long"
    return "Regular"


def derive_inseam_style(jean_style: str, inseam_label: str, inseam: str, length_hint: str = "") -> str:
    val = to_float(inseam)
    label = inseam_label or "Regular"
    if val is not None:
        wide_group = {"Barrel", "Bootcut", "Flare", "Straight from Thigh", "Baggy", "Straight from Knee/Thigh"}
        skinny_group = {"Skinny", "Tapered", "Straight from Knee"}
        if jean_style in wide_group:
            if label == "Petite":
                return "Cropped" if val <= 25 else ("Ankle" if val <= 28 else "Full Length")
            if label == "Regular":
                return "Cropped" if val <= 28 else ("Ankle" if val < 31 else "Full Length")
            if label == "Long":
                return "Full Length" if val >= 33 else "Ankle"
        if jean_style in skinny_group:
            if label == "Petite":
                return "Cropped" if val < 25 else "Full Length"
            if label == "Regular":
                return "Cropped" if val < 27 else "Full Length"
            if label == "Long":
                return "Full Length"
    return length_hint or ""


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
        (("tan", "beige", "khaki"), "Tan"),
        (("white", "ecru", "egret", "cream", "bleach"), "White"),
        (("yellow",), "Yellow"),
        (("black", "onyx", "noir", "raven"), "Black"),
        (("brown", "cinnamon", "coffee", "espresso"), "Brown"),
    ]
    for keys, out in mapping:
        if any(k in c for k in keys):
            return out
    for keys, out in mapping:
        if any(k in d for k in keys):
            return out
    for keys, out in mapping:
        if any(k in hint for k in keys):
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
    if any(k in c for k in ("wine", "burgundy", "navy", "dark", "deep")):
        return "Dark"
    if any(k in c for k in ("pastel", "cream", "light")):
        return "Light"
    if any(k in c for k in ("medium", "mid")):
        return "Medium"
    if any(k in d for k in ("medium light", "light to medium", "medium to light", "light-medium", "medium/light")):
        return "Light to Medium"
    if any(k in d for k in ("medium to dark", "dark to medium", "medium/dark", "dark-medium")):
        return "Medium to Dark"
    if any(k in d for k in ("dark", "deep", "black", "wine", "burgundy", "midnight blue", "forest green", "navy")):
        return "Dark"
    if any(k in d for k in ("light blue", "pale blue", "light vintage", "soft blue", "ecru", "white", "acid wash", "light", "khaki", "tan", "ivory")):
        return "Light"
    if any(k in d for k in ("mid blue", "medium stone wash", "classic stone washed blue", "medium blue", "medium wash", "classic blue")):
        return "Medium"
    return wash_hint


def normalize_text(tag: Tag) -> str:
    text = " ".join(part.strip() for part in tag.stripped_strings if part.strip())
    return re.sub(r"\s+", " ", text).strip()


def selector_with_dynamic_id_fallbacks(selector: str) -> List[str]:
    candidates = [selector]
    normalized_headless = re.sub(
        r"#headlessui-disclosure-panel-[A-Za-z0-9_-]+",
        "[id^='headlessui-disclosure-panel-']",
        selector,
    )
    if normalized_headless not in candidates:
        candidates.append(normalized_headless)
    return candidates


def build_soup_candidates(html_text: str) -> List[BeautifulSoup]:
    soups: List[BeautifulSoup] = []
    for parser in ("lxml", "html.parser"):
        try:
            soup = BeautifulSoup(html_text, parser)
        except FeatureNotFound:
            continue
        soups.append(soup)
    if not soups:
        soups.append(BeautifulSoup(html_text, "html.parser"))
    return soups


def extract_pdp_description(html_text: str) -> str:
    for soup in build_soup_candidates(html_text):
        for selector in selector_with_dynamic_id_fallbacks(PDP_SELECTOR):
            matches = [node for node in soup.select(selector) if isinstance(node, Tag)]
            values = [normalize_text(node) for node in matches if normalize_text(node)]
            if values:
                return ", ".join(values)

        parent = soup.select_one(PDP_PARENT_SELECTOR)
        if parent is not None:
            values = [normalize_text(node) for node in parent.find_all("li") if normalize_text(node)]
            if values:
                return ", ".join(values)

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
    for label in labels:
        pattern = rf"{label}\s*:\s*([0-9]+(?:\s+[0-9]+/[0-9]+|/[0-9]+|\.[0-9]+)?)"
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue
        value = parse_mixed_fraction(match.group(1))
        if value is None:
            continue
        return f"{value:.3f}"
    return ""


def extract_label_text(text: str, labels: Sequence[str]) -> str:
    for label in labels:
        match = re.search(rf"{label}\s*:\s*([^,]+)", text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def render_page_html_with_clicks(url: str) -> Optional[str]:
    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        log(f"Playwright unavailable, skipping browser-render pass: {exc}")
        return None

    def dismiss_known_overlays(page) -> None:
        for selector in (
            "#attentive_overlay",
            "iframe#attentive_creative",
            "[id*='attentive_overlay']",
            "[data-testid*='attentive']",
        ):
            try:
                page.evaluate(
                    """(sel) => {
                      document.querySelectorAll(sel).forEach((el) => {
                        try { el.remove(); } catch (e) {}
                        if (el.style) {
                          el.style.display = 'none';
                          el.style.visibility = 'hidden';
                          el.style.pointerEvents = 'none';
                        }
                      });
                    }""",
                    selector,
                )
            except Exception:
                continue

    def click_with_fallback(locator) -> bool:
        try:
            locator.click(timeout=5000)
            return True
        except Exception:
            pass
        try:
            locator.click(timeout=5000, force=True)
            return True
        except Exception:
            pass
        try:
            handle = locator.element_handle(timeout=5000)
            if handle is None:
                return False
            locator.page.evaluate("(el) => el.click()", handle)
            return True
        except Exception:
            return False

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(ignore_https_errors=True)
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_RENDER_TIMEOUT_MS)
            try:
                page.wait_for_load_state("networkidle", timeout=min(BROWSER_RENDER_TIMEOUT_MS, 15000))
            except PlaywrightTimeoutError:
                pass

            dismiss_known_overlays(page)
            parent_locator = page.locator(PDP_PARENT_SELECTOR)
            for idx in range(parent_locator.count()):
                current = parent_locator.nth(idx)
                state = (current.get_attribute("data-headlessui-state") or "").strip().lower()
                if state == "open":
                    continue
                nested_button = current.locator("button,[role='button']")
                if nested_button.count() > 0:
                    click_with_fallback(nested_button.first)

            for label in CLICK_TARGET_TEXTS:
                locator = page.locator(f"button:has-text('{label}')")
                if locator.count() > 0:
                    first = locator.first
                    expanded = (first.get_attribute("aria-expanded") or "").strip().lower()
                    state = (first.get_attribute("data-headlessui-state") or "").strip().lower()
                    if expanded != "true" and state != "open":
                        click_with_fallback(first)

            page.wait_for_timeout(500)
            html_text = page.content()
            context.close()
            browser.close()
            return html_text
    except Exception as exc:
        log(f"Browser-render pass failed for {url}: {exc}")
        return None


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

    def fetch_pdp_fields(self, handle: str) -> Dict[str, str]:
        if handle in self._pdp_cache:
            return self._pdp_cache[handle]

        html_candidates: List[Tuple[str, str]] = []
        url = urljoin(PDP_HOST, f"/products/{handle}")
        try:
            response = self.session.get(url, timeout=30, allow_redirects=True)
            response.raise_for_status()
            html_candidates.append((f"http:{PDP_HOST}", response.text))
        except Exception as exc:
            log(f"HTTP fetch failed for {url}: {exc}")
        if BROWSER_RENDER_ENABLED:
            rendered_html = render_page_html_with_clicks(url)
            if rendered_html:
                html_candidates.insert(0, (f"browser_click:{PDP_HOST}", rendered_html))

        details_description = ""
        source = ""
        for candidate_source, html_text in html_candidates:
            text = extract_pdp_description(html_text)
            if text and len(text) > len(details_description):
                details_description = text
                source = candidate_source

        base_description = ""
        try:
            product_json = self.session.get(
                urljoin(PDP_HOST, f"/products/{handle}.json"),
                timeout=30,
                allow_redirects=True,
            )
            if product_json.status_code == 200:
                body_html = (product_json.json().get("product") or {}).get("body_html") or ""
                base_description = normalize_text(BeautifulSoup(body_html, "html.parser"))
        except Exception:
            pass

        description_parts = [part for part in (base_description, details_description) if part]
        description = ", ".join(description_parts)

        rise = extract_measurement(description, ["Front Rise", "Rise"])
        inseam = extract_measurement(description, ["Inseam", "Inleg"])
        leg_opening = extract_measurement(description, ["Leg Opening", "Opening"])

        result = {
            "description": description,
            "rise": rise,
            "inseam": inseam,
            "leg_opening": leg_opening,
            "stretch": extract_label_text(description, ["Stretch"]),
        }
        self._pdp_cache[handle] = result
        log(
            f"PDP parsed for {handle} | source={source or 'none'} | "
            f"rise={rise or 'blank'} inseam={inseam or 'blank'} leg_opening={leg_opening or 'blank'}"
        )
        return result

    def build_rows(self) -> List[List[str]]:
        rows: List[List[str]] = []
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

            pdp_fields = self.fetch_pdp_fields(handle)
            style_name = derive_style_name_base(title)
            product_type = derive_product_type(tags, title)
            country = extract_tag_value(tags, "country:")
            production_cost = extract_tag_value(tags, "productionCost:")
            site_exclusive = extract_tag_value(tags, "productType:")
            product_line = extract_tag_value(tags, "sizeType:") or meta_attrs.get("sizeType", "")
            style_algolia = algolia_style_map.get(handle, {})
            fit_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("fit", "")
            length_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("length", "")
            rise_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("rise", "")
            wash_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("wash", "")
            color_hint = ((style_algolia.get("meta") or {}).get("attributes") or {}).get("colorCategory", "")

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
                jean_style = derive_jean_style(title, pdp_fields["description"], pdp_fields["leg_opening"], fit_hint)
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
                    pdp_fields["rise"],
                    pdp_fields["inseam"],
                    pdp_fields["leg_opening"],
                    variant_title,
                    color_value,
                    size_value,
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
                    pdp_fields.get("stretch", "") or meta_attrs.get("stretch", ""),
                    production_cost,
                    site_exclusive,
                ]
                rows.append(row)

            log(f"Processed style {idx}/{len(styles)}: {handle} -> {len(variants)} variants")
            time.sleep(0.2)

        self.apply_style_name_rules(rows)
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
            if any("maternity" in r[idx_product].lower() for r in group_rows):
                continue
            by_leg: Dict[str, List[List[str]]] = {}
            for r in group_rows:
                by_leg.setdefault(r[idx_leg], []).append(r)
            for leg_value, leg_rows in by_leg.items():
                styles = [r[idx_style_name] for r in leg_rows if r[idx_style_name]]
                if len(set(styles)) <= 1:
                    continue
                most_common = max(set(styles), key=styles.count)
                for r in leg_rows:
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
        rows = self.build_rows()
        output_path = self.write_csv(rows)
        log("Scrape complete")
        print("Done.")
        return output_path


def main() -> None:
    scraper = PaigeScraper()
    scraper.run()


if __name__ == "__main__":
    main()
