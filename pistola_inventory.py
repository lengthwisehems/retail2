import csv
import logging
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import requests

COLLECTION_URL_TEMPLATE = "https://www.pistoladenim.com/collections/all-denim/products.json?limit=250&page={page}"
CATEGORY_PAGE_URL = "https://www.pistoladenim.com/collections/all-denim"
NOSTO_ENDPOINT = "https://search.nosto.com/v1/graphql"
NOSTO_PAGE_SIZE = 200

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "pistola_inventory.log"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
REQUEST_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

HEADERS = [
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
    "Price Range",
    "Compare at Price",
    "Available for Sale",
    "Quantity Available",
    "Quantity of style",
    "SKU - Shopify",
    "SKU - Brand",
    "Barcode",
    "Image URL",
    "SKU URL",
    "Jean Style",
    "Inseam Label",
    "Rise Label",
    "Color - Simplified",
    "Color - Standardized",
    "Stretch",
]

FRACTION_MAP = {
    "½": "1/2",
    "¼": "1/4",
    "¾": "3/4",
    "⅛": "1/8",
    "⅜": "3/8",
    "⅝": "5/8",
    "⅞": "7/8",
}


def setup_logging(preferred_path: Path) -> Path:
    preferred_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    actual_path = preferred_path
    try:
        file_handler = logging.FileHandler(preferred_path, mode="a", encoding="utf-8")
    except PermissionError:
        fallback_path = preferred_path.with_name(f"{preferred_path.stem}_fallback.log")
        fallback_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(fallback_path, mode="a", encoding="utf-8")
        actual_path = fallback_path
        logger.warning(
            "Permission denied when opening %s; logging to fallback file %s",
            preferred_path,
            fallback_path,
        )

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return actual_path


def normalize_tags(tags_value: Iterable[str]) -> str:
    tags = [t.strip() for t in tags_value if t and t.strip()]
    return ", ".join(tags)


def parse_fractional_number(raw: str) -> Optional[str]:
    if not raw:
        return None
    value = raw.strip()
    for symbol, replacement in FRACTION_MAP.items():
        value = value.replace(symbol, replacement)
    value = value.replace("\u201d", "").replace("\u201c", "").replace('"', "").replace("'", "")
    value = value.replace("in", "").replace("In", "").replace("\u2033", "")
    value = value.strip()
    if not value:
        return None
    parts = value.split()
    total = 0.0
    try:
        if len(parts) == 1:
            total = float(eval_fraction(parts[0]))
        elif len(parts) >= 2:
            total = float(eval_fraction(parts[0]))
            for part in parts[1:]:
                total += float(eval_fraction(part))
    except Exception:
        return None
    return f"{total:.2f}".rstrip("0").rstrip(".")


def eval_fraction(token: str) -> float:
    if "/" in token:
        num, denom = token.split("/", 1)
        return float(num) / float(denom)
    return float(token)


def extract_measure(info_text: str, keyword: str) -> Optional[str]:
    if not info_text:
        return None
    cleaned = info_text
    for symbol, replacement in FRACTION_MAP.items():
        cleaned = cleaned.replace(symbol, replacement)
    cleaned = cleaned.replace("•", " ")
    pattern = re.compile(rf"{re.escape(keyword)}[^0-9]*([0-9]+(?:\s+[0-9]+/[0-9]+)?(?:\.[0-9]+)?)", re.IGNORECASE)
    match = pattern.search(cleaned)
    if not match:
        return None
    return parse_fractional_number(match.group(1))


def format_price(value: Optional[float]) -> str:
    if value in (None, "", "null"):
        return ""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    return f"${number:.2f}"


def compute_price_range(price: Optional[float]) -> str:
    if price in (None, "", "null"):
        return ""
    try:
        number = float(price)
    except (TypeError, ValueError):
        return ""
    lower = int(math.floor(number / 50.0) * 50)
    upper = lower + 50
    return f"{lower}:{upper}"


def format_published_at(raw: str) -> str:
    if not raw:
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    iso_candidate = text.replace("Z", "+00:00")
    dt: Optional[datetime] = None
    try:
        dt = datetime.fromisoformat(iso_candidate)
    except ValueError:
        dt = None
    if dt is None:
        try:
            dt = datetime.strptime(text[:10], "%Y-%m-%d")
        except ValueError:
            return ""
    return dt.strftime("%m/%d/%Y")


@dataclass
class ShopifyVariant:
    handle: str
    published_at: str
    product_type: str
    tags: List[str]
    vendor: str
    compare_at_price: Optional[str]
    available: Optional[bool]


class PistolaScraper:
    def __init__(self) -> None:
        self.base_dir = BASE_DIR
        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = setup_logging(LOG_PATH)
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)

    def run(self) -> Path:
        logging.info("Starting Pistola inventory scrape")
        html = self.fetch_category_html()
        account_id = self.extract_account_id(html)
        category_id = self.extract_category_id(html)
        logging.info("Parsed account_id=%s category_id=%s", account_id, category_id)
        shopify_variants = self.fetch_shopify_variants()
        logging.info("Loaded %s Shopify variants", len(shopify_variants))
        nosto_hits = self.fetch_nosto_hits(account_id, category_id)
        logging.info("Fetched %s Nosto products", len(nosto_hits))
        rows = self.assemble_rows(nosto_hits, shopify_variants)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        output_path = self.output_dir / f"PISTOLA_{timestamp}.csv"
        with output_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADERS)
            writer.writeheader()
            writer.writerows(rows)
        logging.info("Wrote %s rows to %s", len(rows), output_path)
        return output_path

    def fetch_category_html(self) -> str:
        resp = self.session.get(CATEGORY_PAGE_URL, timeout=30)
        resp.raise_for_status()
        return resp.text

    def extract_account_id(self, html: str) -> str:
        match = re.search(r"(shopify-\d+)", html)
        if not match:
            raise ValueError("Unable to locate Nosto account id")
        return match.group(1)

    def extract_category_id(self, html: str) -> str:
        match = re.search(r"\"rid\":(\d+)", html)
        if not match:
            raise ValueError("Unable to locate Nosto category id")
        return match.group(1)

    def fetch_shopify_variants(self) -> Dict[str, ShopifyVariant]:
        variants: Dict[str, ShopifyVariant] = {}
        page = 1
        while True:
            url = COLLECTION_URL_TEMPLATE.format(page=page)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            products = payload.get("products", [])
            if not products:
                break
            for product in products:
                handle = product.get("handle", "")
                published_at = product.get("published_at", "")
                product_type = product.get("product_type", "")
                vendor = product.get("vendor", "")
                tags_raw = product.get("tags", [])
                if isinstance(tags_raw, str):
                    tags_list = [t.strip() for t in tags_raw.split(",") if t.strip()]
                else:
                    tags_list = [t for t in tags_raw if t]
                for variant in product.get("variants", []):
                    variant_id = str(variant.get("id"))
                    compare_at = variant.get("compare_at_price")
                    variants[variant_id] = ShopifyVariant(
                        handle=handle,
                        published_at=published_at,
                        product_type=str(product_type or ""),
                        tags=tags_list,
                        vendor=vendor,
                        compare_at_price=str(compare_at) if compare_at else "",
                        available=variant.get("available"),
                    )
            page += 1
        return variants

    def fetch_nosto_hits(self, account_id: str, category_id: str) -> List[dict]:
        hits: List[dict] = []
        cursor = 0
        while True:
            variables = {
                "accountId": account_id,
                "categoryId": category_id,
                "size": NOSTO_PAGE_SIZE,
                "from": cursor,
            }
            query = """
            query ($accountId: String!, $categoryId: String!, $size: Int!, $from: Int!) {
              search(accountId: $accountId, products: {categoryId: $categoryId, size: $size, from: $from}) {
                products {
                  total
                  from
                  size
                  hits {
                    productId
                    name
                    url
                    description
                    price
                    listPrice
                    availability
                    inventoryLevel
                    imageUrl
                    categories
                    tags1
                    customFields { key value }
                    skus {
                      id
                      name
                      price
                      listPrice
                      availability
                      inventoryLevel
                      customFields { key value }
                    }
                  }
                }
              }
            }
            """
            resp = self.session.post(
                NOSTO_ENDPOINT,
                headers={"Content-Type": "application/json", "X-Nosto-Integration": "Client Script"},
                json={"query": query, "variables": variables},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            products = data.get("data", {}).get("search", {}).get("products", {})
            page_hits = products.get("hits", [])
            if not page_hits:
                break
            hits.extend(page_hits)
            cursor = products.get("from", 0) + products.get("size", 0)
            total = products.get("total", cursor)
            if cursor >= total:
                break
        return hits

    def assemble_rows(self, hits: List[dict], variant_meta: Dict[str, ShopifyVariant]) -> List[dict]:
        rows: List[dict] = []
        for hit in hits:
            product_level = self.prepare_product_level(hit)
            sku_records = hit.get("skus", [])
            style_total = hit.get("inventoryLevel")
            if style_total in (None, "", "null"):
                style_total = sum(
                    sku.get("inventoryLevel") or 0 for sku in sku_records
                    if isinstance(sku.get("inventoryLevel"), (int, float))
                )
            style_tags = hit.get("tags1", []) or []
            if not self.is_jean(hit.get("categories", []), style_tags):
                continue
            for sku in sku_records:
                variant_id = str(sku.get("id"))
                meta = variant_meta.get(variant_id)
                if not meta:
                    continue
                custom_fields = {cf.get("key", ""): cf.get("value", "") for cf in sku.get("customFields", [])}
                size_raw = custom_fields.get("size", "")
                size_clean, inseam_label_override = self.process_size(size_raw)
                color_value = custom_fields.get("color", "")
                sku_brand = custom_fields.get("skucode", "")
                barcode = custom_fields.get("gtin", "")
                price = sku.get("price") if sku.get("price") is not None else hit.get("price")
                compare_at = meta.compare_at_price
                available_str = "" if meta.available is None else ("TRUE" if meta.available else "FALSE")
                combined_tags = list(meta.tags)
                for tag in style_tags:
                    if tag not in combined_tags:
                        combined_tags.append(tag)
                style_name = product_level.style_name or self.extract_tag_value(combined_tags, "Body")
                product_type_value = self.resolve_product_type(meta, product_level)
                jean_style = product_level.jean_style or self.extract_tag_value(combined_tags, "Fit")
                rise_label = product_level.rise_label or self.extract_tag_value(combined_tags, "Rise")
                color_simplified = product_level.color_simplified or self.extract_tag_value(combined_tags, "Wash")
                color_standardized = product_level.color_standardized or self.extract_tag_value(combined_tags, "Color")
                stretch = product_level.stretch or self.extract_tag_value(combined_tags, "Stretch")
                if not style_name:
                    name_segment = (hit.get("name", "") or "").split(" - ")[0].strip()
                    if name_segment:
                        words = name_segment.split()
                        if words and words[0].upper() == "PETITE" and len(words) > 1:
                            style_name = words[1]
                        elif words:
                            style_name = words[0]
                if not stretch:
                    stretch = self.map_stretch_scale(product_level.stretch_scale)
                row = {
                    "Style Id": str(hit.get("productId", "")),
                    "Handle": meta.handle,
                    "Published At": format_published_at(meta.published_at),
                    "Product": hit.get("name", ""),
                    "Style Name": style_name,
                    "Product Type": product_type_value,
                    "Tags": normalize_tags(meta.tags),
                    "Vendor": meta.vendor,
                    "Description": hit.get("description", ""),
                    "Variant Title": f"{hit.get('name', '')} - {size_clean}".strip(),
                    "Color": color_value,
                    "Size": size_clean,
                    "Rise": product_level.rise,
                    "Inseam": product_level.inseam,
                    "Leg Opening": product_level.leg_opening,
                    "Price": format_price(price),
                    "Price Range": compute_price_range(price),
                    "Compare at Price": format_price(compare_at) if compare_at else "",
                    "Available for Sale": available_str,
                    "Quantity Available": self.format_int(sku.get("inventoryLevel")),
                    "Quantity of style": self.format_int(style_total),
                    "SKU - Shopify": variant_id,
                    "SKU - Brand": sku_brand,
                    "Barcode": barcode,
                    "Image URL": hit.get("imageUrl", ""),
                    "SKU URL": hit.get("url", ""),
                    "Jean Style": jean_style,
                    "Inseam Label": self.resolve_inseam_label(
                        base_label=product_level.inseam_label,
                        override=inseam_label_override,
                        petite_flag=product_level.petite_flag or size_raw.upper().endswith("P"),
                        tags=combined_tags,
                    ),
                    "Rise Label": rise_label,
                    "Color - Simplified": color_simplified,
                    "Color - Standardized": color_standardized,
                    "Stretch": stretch,
                }
                rows.append(row)
        return rows

    def process_size(self, size: str) -> Tuple[str, Optional[str]]:
        if not size:
            return "", None
        cleaned = size.strip()
        override = None
        if cleaned.endswith("P"):
            cleaned = cleaned[:-1]
            override = "Petite"
        return cleaned, override

    def resolve_inseam_label(
        self,
        base_label: str,
        override: Optional[str],
        petite_flag: bool,
        tags: Iterable[str],
    ) -> str:
        label = base_label
        tags_list = [t for t in tags if t]
        if not label:
            for tag in tags_list:
                if ":" in tag:
                    key, value = tag.split(":", 1)
                    if key.strip().lower() == "length":
                        label = value.strip()
                        break
        petite_from_tags = any("petite" in (tag or "").lower() for tag in tags_list)
        petite_present = petite_flag or petite_from_tags or (base_label and "petite" in base_label.lower()) or (override and override.lower() == "petite")
        if override:
            label = override
        if petite_present and base_label and base_label.lower() != "petite":
            label = "Petite" if not base_label else f"Petite + {base_label}"
        if not label and petite_present:
            label = "Petite"
        return label or ""

    def extract_tag_value(self, tags: Iterable[str], prefix: str) -> str:
        prefix_lower = prefix.lower() + ":"
        for tag in tags:
            if not tag:
                continue
            if tag.lower().startswith(prefix_lower):
                return tag.split(":", 1)[1].strip()
        return ""

    def resolve_product_type(self, meta: ShopifyVariant, product_level: "PistolaScraper.ProductLevel") -> str:
        product_type = (meta.product_type or "").strip()
        if product_type == "01":
            return "DENIM PANTS"
        if product_type:
            return product_type
        fallback = (product_level.product_type or "").strip()
        if fallback == "01":
            return "DENIM PANTS"
        return fallback

    def extract_product_type_from_tags(self, tags: Iterable[str]) -> str:
        normalized = [tag for tag in tags if tag]
        for tag in normalized:
            lower = tag.lower()
            if lower.startswith("group:") and "jean" in lower:
                return tag.split(":", 1)[1].strip().split(",")[0]
        for tag in normalized:
            lower = tag.lower()
            if lower.startswith("group:"):
                return tag.split(":", 1)[1].strip().split(",")[0]
        return ""

    def is_jean(self, categories: Iterable[str], tags: Iterable[str]) -> bool:
        for entry in categories or []:
            if entry and entry.upper().startswith("CATEGORY:") and "JEAN" in entry.upper():
                return True
        for tag in tags or []:
            if tag and tag.lower().startswith("group:") and "jean" in tag.lower():
                return True
        return False

    def map_stretch_scale(self, scale: str) -> str:
        mapping = {
            "1": "Rigid",
            "2": "Comfort Stretch",
            "3": "Stretch",
        }
        return mapping.get(str(scale).strip(), "")

    def format_int(self, value: Optional[object]) -> str:
        if value in (None, "", "null"):
            return ""
        try:
            return str(int(float(value)))
        except (TypeError, ValueError):
            return ""

    @dataclass
    class ProductLevel:
        style_name: str
        product_type: str
        rise: str
        inseam: str
        leg_opening: str
        jean_style: str
        inseam_label: str
        rise_label: str
        color_simplified: str
        color_standardized: str
        stretch: str
        petite_flag: bool
        stretch_scale: str

    def prepare_product_level(self, hit: dict) -> "PistolaScraper.ProductLevel":
        categories = hit.get("categories", []) or []
        category_map: Dict[str, List[str]] = {}
        petite_flag = False
        for entry in categories:
            entry = entry.strip()
            if not entry:
                continue
            if entry.lower() == "petite":
                petite_flag = True
                continue
            if ":" in entry:
                key, value = entry.split(":", 1)
                key = key.strip().upper()
                value = value.strip()
                category_map.setdefault(key, []).append(value)
        custom_fields = {cf.get("key", ""): cf.get("value", "") for cf in hit.get("customFields", [])}
        info_text = custom_fields.get("info-tab1") or custom_fields.get("info_tab1") or custom_fields.get("infoTab1") or ""
        rise = extract_measure(info_text, "Rise") or ""
        inseam = extract_measure(info_text, "Inseam") or ""
        leg_opening = extract_measure(info_text, "Leg Opening") or ""
        return PistolaScraper.ProductLevel(
            style_name=self.first_or_blank(category_map.get("BODY")),
            product_type=self.first_or_blank(category_map.get("CATEGORY")),
            rise=rise,
            inseam=inseam,
            leg_opening=leg_opening,
            jean_style=self.first_or_blank(category_map.get("FIT")),
            inseam_label=self.derive_inseam_label(category_map, petite_flag),
            rise_label=self.first_or_blank(category_map.get("RISE")),
            color_simplified=self.first_or_blank(category_map.get("WASH")),
            color_standardized=self.first_or_blank(category_map.get("COLOR")),
            stretch=self.first_or_blank(category_map.get("STRETCH")),
            petite_flag=petite_flag,
            stretch_scale=custom_fields.get("seed-stretchability_scale", ""),
        )

    def derive_inseam_label(self, category_map: Dict[str, List[str]], petite_flag: bool) -> str:
        length_value = self.first_or_blank(category_map.get("LENGTH"))
        if petite_flag and length_value:
            return f"Petite + {length_value}"
        if petite_flag:
            return "Petite"
        return length_value

    def first_or_blank(self, values: Optional[List[str]]) -> str:
        if not values:
            return ""
        return values[0].strip()


def main() -> None:
    scraper = PistolaScraper()
    scraper.run()


if __name__ == "__main__":
    main()