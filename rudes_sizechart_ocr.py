"""
rudes_sizechart_ocr.py
======================
Finds the size-chart image (URL contains '___') for every product on
rudesdenim.com/collections/shop-all, OCRs it, and extracts Rise, Inseam,
and Leg Opening for a target size (default: 26).

When no size-chart image is found it falls back to parsing the product
description HTML for inline measurement text (e.g. "Inseam 32\" | Rise 11\"").

Output: Output/rudes_sizechart_ocr_<timestamp>.xlsx
        One row per product.

Requirements
------------
  pip install easyocr pillow openpyxl requests beautifulsoup4
  (No external binary needed — EasyOCR bundles its own models.)
  First run downloads ~100 MB of model weights to ~/.EasyOCR/model/
"""

from __future__ import annotations

import html as _html
import io
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
import urllib3
from requests.adapters import HTTPAdapter, Retry

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
COLLECTION_URL  = "https://rudesdenim.com/collections/shop-all"
TARGET_SIZE     = "26"          # Rudes denim size to extract measurements for
REQUEST_TIMEOUT = 30
BASE_DIR   = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def _configure_logging() -> logging.Logger:
    logger = logging.getLogger("rudes_ocr")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s")
    for path in (BASE_DIR / "rudes_sizechart_ocr.log", OUTPUT_DIR / "rudes_sizechart_ocr.log"):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(path, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
            break
        except OSError:
            pass
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist={429, 500, 502, 503, 504},
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )
    session.verify = False
    return session


# ---------------------------------------------------------------------------
# Collection crawl
# ---------------------------------------------------------------------------
def get_all_products(session: requests.Session, logger: logging.Logger) -> List[Dict]:
    """Return list of product dicts from the shop-all collection products.json."""
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(COLLECTION_URL)
    base = urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/") + "/products.json", "", ""))

    all_products: List[Dict] = []
    page = 1
    while True:
        try:
            resp = session.get(base, params={"limit": 250, "page": page}, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("products.json page %s failed: %s", page, exc)
            break
        prods = data.get("products") or []
        logger.info("products.json page %s → %s products", page, len(prods))
        if not prods:
            break
        all_products.extend(prods)
        if len(prods) < 250:
            break
        page += 1
        time.sleep(0.3)

    logger.info("Total products collected: %s", len(all_products))
    return all_products


# ---------------------------------------------------------------------------
# Size-chart image URL detection
# ---------------------------------------------------------------------------
_TRIPLE_UNDERSCORE = re.compile(r"https?://[^\"'<> )]*___[^\"'<> )]+", re.IGNORECASE)

# Fallback: any .webp uploaded to the /files/ CDN path (distinct from /products/ product photos)
_CDN_FILES_WEBP = re.compile(
    r"https://cdn\.shopify\.com/s/files/1/0792/0563/0243/files/"
    r"[^\"'<> )]+\.webp(?:\?[^\"'<> )]*)?",
    re.IGNORECASE,
)


def find_size_chart_url(html_text: str) -> Optional[str]:
    """Return a size-chart image URL from the page HTML.

    Prefers URLs with '___' (naming convention for Rudes size-chart images).
    Falls back to any .webp in the /files/ CDN path when '___' is absent.
    """
    matches = _TRIPLE_UNDERSCORE.findall(html_text)
    if matches:
        return matches[0]
    matches = _CDN_FILES_WEBP.findall(html_text)
    return matches[0] if matches else None


def _has_size_chart_link(pdp_html: str) -> bool:
    """Return True if the PDP contains the 'size chart' link.

    On rudesdenim.com the presence of a size chart is indicated by an
    <a class="product__info__link"> element with text "size chart".  This link
    is present for every product that has a size-chart image and absent for
    products that don't — making it a reliable gate before attempting OCR.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        # BeautifulSoup not installed: fall back to a fast regex check
        return bool(re.search(r'product__info__link', pdp_html, re.I)
                    and re.search(r'size\s+chart', pdp_html, re.I))
    soup = BeautifulSoup(pdp_html, "html.parser")
    link = soup.find("a", class_="product__info__link")
    return link is not None and "size chart" in link.get_text(strip=True).lower()


# ---------------------------------------------------------------------------
# Measurement value normalisation helpers
# ---------------------------------------------------------------------------
_FRACTION_MAP = {
    "½": " 1/2",
    "¼": " 1/4",
    "¾": " 3/4",
    "⅛": " 1/8",
    "⅜": " 3/8",
    "⅝": " 5/8",
    "⅞": " 7/8",
    "″": '"',  # ″ DOUBLE PRIME
    "“": '"',
    "”": '"',
}

# Single OCR letters that are commonly misread from digit pairs
_LETTER_TO_NUMBER = {
    "B": "13",  # "B" often comes from bold "13" in narrow cells
    "D": "0",
    "O": "0",
    "I": "1",
    "l": "1",
}


def _normalise_measurement(raw: str) -> str:
    """
    Converts a raw OCR token like '12/4"', '23"', 'B"', '217/8"' to a
    human-readable measurement string.

    Strategy
    --------
    1. Replace unicode fractions and fancy quotes.
    2. Replace known single-letter OCR artefacts.
    3. Interpret patterns like "217/8"" as "21 7/8"" (two-digit whole + fraction).
    4. Convert fractions to decimal and format as whole or .25/.5/.75/.xx.
    """
    s = raw.strip()
    for uc, rep in _FRACTION_MAP.items():
        s = s.replace(uc, rep)

    # Strip leading/trailing punctuation except digits, letters, /  "  space  .
    s = re.sub(r"^[^0-9A-Za-z]+", "", s)
    s = re.sub(r"[^0-9\"./½¼¾\s]+$", "", s).strip()

    # Replace single-letter artefacts that stand alone before "
    s = re.sub(
        r"\b([A-Z])\b(?=\"|$| )",
        lambda m: _LETTER_TO_NUMBER.get(m.group(1), m.group(1)),
        s,
    )

    # Handle OCR artefact: letter between digit and fraction slash (e.g. "11V/2" → "11 1/2")
    # EasyOCR sometimes reads the "1" numerator as "V", "l", "I", etc.
    s = re.sub(
        r'(\d)([A-Za-z])(/\d)',
        lambda m: '{} {}{}'.format(
            m.group(1),
            _LETTER_TO_NUMBER.get(m.group(2).upper(), '1'),
            m.group(3),
        ),
        s,
    )

    # Handle patterns like "217/8" → "21 7/8",  "121/2" → "12 1/2"
    s = re.sub(r"(\d{2,3})(\d)(\/\d)", r"\1 \2\3", s)

    # Remove quotes for numeric parsing, add back at end
    s_clean = s.replace('"', "").strip()

    # Try to parse as  [whole] [num/denom]
    m = re.match(
        r"^(-?\d+(?:\.\d+)?)"
        r"(?:\s+(\d+)\s*/\s*(\d+))?$",
        s_clean,
    )
    if not m:
        # Return as-is (with " if it had one) if we can't parse
        return s if s else raw

    base = float(m.group(1))
    if m.group(2) and m.group(3):
        try:
            base += float(m.group(2)) / float(m.group(3))
        except ZeroDivisionError:
            pass

    if base == int(base):
        return str(int(base))
    return f'{base:.3f}'.rstrip('0').rstrip('.')


# ---------------------------------------------------------------------------
# OCR engine  (EasyOCR — no external binary required)
# ---------------------------------------------------------------------------

_easyocr_reader = None  # cached after first load


def _get_easyocr_reader():
    """Lazy-load EasyOCR reader (downloads models on first call, ~100 MB)."""
    global _easyocr_reader
    if _easyocr_reader is None:
        try:
            import easyocr
        except ImportError as exc:
            raise RuntimeError(
                "EasyOCR not available. Install with:  pip install easyocr"
            ) from exc
        _easyocr_reader = easyocr.Reader(["en"], gpu=False, verbose=False)
    return _easyocr_reader


def _cluster_rows(words: list, y_tol: int = 35) -> List[List]:
    """Group word tuples (text, x_center, y_center, conf) into horizontal rows."""
    rows: List[List] = []
    for w in sorted(words, key=lambda x: x[2]):
        placed = False
        for row in rows:
            if abs(row[0][2] - w[2]) <= y_tol:
                row.append(w)
                placed = True
                break
        if not placed:
            rows.append([w])
    for row in rows:
        row.sort(key=lambda x: x[1])  # sort by x within each row
    return rows


def _merge_adjacent_fragments(row: list, max_gap: int = 120) -> list:
    """
    Merge word fragments that are very close together (split fractions like
    '22' + '5/8"' → '22 5/8"') to get compound measurement strings.
    Returns a new list of merged word tuples.
    """
    if not row:
        return row
    merged: List = []
    buf = list(row[0])  # [text, x_center, y_center, conf]
    for w in row[1:]:
        prev_right = buf[1] + 50  # rough right edge (x_center + half-width estimate)
        gap = w[1] - buf[1]
        if gap <= max_gap and re.search(r"\d", w[0]) and (
            re.search(r"\d", buf[0]) or re.search(r"[\"′]", buf[0])
        ):
            # merge: combine text, keep x_center of combined span midpoint
            new_text = buf[0].rstrip('"') + " " + w[0] if '"' not in w[0] else buf[0] + " " + w[0]
            new_x = (buf[1] + w[1]) // 2
            new_conf = min(buf[3], w[3])
            buf = [new_text, new_x, buf[2], new_conf]
        else:
            merged.append(tuple(buf))
            buf = list(w)
    merged.append(tuple(buf))
    return merged


def _pick_value_at_column(row: list, col_x: int, x_tol: int = 120) -> Optional[str]:
    """
    From a list of word tuples in a row, return the text of the word whose
    x_center is closest to col_x, within x_tol pixels.  Returns None if
    no word is close enough.
    """
    best_dist = x_tol + 1
    best_text = None
    for w in row:
        dist = abs(w[1] - col_x)
        if dist < best_dist:
            best_dist = dist
            best_text = w[0]
    return best_text


def _row_majority_value(row: list) -> Optional[str]:
    """
    Return the most common well-formed measurement string (contains a digit and
    optionally a quote) from across an entire row, excluding the label words at
    the far left.  Used as a fallback when the column-aligned value looks incomplete.
    """
    from collections import Counter

    # Skip label words (only keep words that look like measurements: 2+ digits)
    candidates = [
        w[0] for w in row
        if re.search(r"\d{2}", w[0])  # at least two consecutive digits → not a single size number
    ]
    if not candidates:
        return None
    most_common = Counter(candidates).most_common(1)
    return most_common[0][0] if most_common else None


def _looks_incomplete(value: Optional[str]) -> bool:
    """Return True if a normalised measurement value looks truncated or invalid."""
    if not value:
        return True
    # Strip the inch mark and whitespace for digit-count check
    clean = value.replace('"', "").strip()
    # Single digit (e.g. "2"") almost certainly a truncated two-digit value
    if re.fullmatch(r"[0-9]", clean):
        return True
    # Single letter — OCR artefact that normalisation didn't resolve
    if re.fullmatch(r"[A-Za-z]", clean):
        return True
    return False


def ocr_size_chart(
    session: requests.Session,
    img_url: str,
    target_size: str,
    logger: logging.Logger,
) -> Tuple[str, str, str]:
    """
    Download img_url, OCR it, and return (rise, inseam, leg_opening) for target_size.
    Returns ("", "", "") on any failure.
    """
    try:
        reader = _get_easyocr_reader()
    except RuntimeError as exc:
        logger.warning("OCR unavailable: %s", exc)
        return ("", "", "")

    try:
        resp = session.get(img_url, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("Failed to download size chart %s: %s", img_url, exc)
        return ("", "", "")

    try:
        from PIL import Image
        import numpy as np
        im = Image.open(io.BytesIO(resp.content)).convert("RGB")
        img_array = np.array(im)
    except Exception as exc:
        logger.warning("Failed to open image %s: %s", img_url, exc)
        return ("", "", "")

    # Run EasyOCR — returns [(bbox, text, confidence), ...]
    # bbox: [[x1,y1],[x2,y1],[x2,y2],[x1,y2]]  (top-left, top-right, bottom-right, bottom-left)
    try:
        ocr_results = reader.readtext(img_array)
    except Exception as exc:
        logger.warning("EasyOCR failed on %s: %s", img_url, exc)
        return ("", "", "")

    # Build word list with spatial info: (text, x_center, y_center, conf_0_to_100)
    words = []
    for (bbox, text, conf) in ocr_results:
        txt = text.strip()
        if conf > 0.2 and txt:
            x_center = int((bbox[0][0] + bbox[1][0]) / 2)
            y_center = int((bbox[0][1] + bbox[2][1]) / 2)
            words.append((txt, x_center, y_center, int(conf * 100)))

    if not words:
        logger.warning("OCR returned no words for %s", img_url)
        return ("", "", "")

    rows = _cluster_rows(words, y_tol=35)

    # ── Step 1: find the size header row and the x_center of target_size ──────
    col_x: Optional[int] = None
    header_row_idx: Optional[int] = None
    for idx, row in enumerate(rows):
        for w in row:
            if w[0] == target_size and w[3] > 50:
                # Make sure it's the true header (earlier rows are preferred)
                if header_row_idx is None or idx < header_row_idx:
                    col_x = w[1]
                    header_row_idx = idx
        if header_row_idx is not None:
            break

    if col_x is None:
        # Fallback: size "26" might be read with lower confidence
        for idx, row in enumerate(rows):
            for w in row:
                if w[0] == target_size:
                    col_x = w[1]
                    break
            if col_x is not None:
                break

    if col_x is None:
        logger.warning("Could not locate size '%s' in OCR output for %s", target_size, img_url)
        return ("", "", "")

    logger.debug("Size '%s' found at x_center=%s", target_size, col_x)

    # ── Step 2: find and extract measurement rows ────────────────────────────
    LABELS = {
        "rise":        ("RISE",),
        "inseam":      ("INSEAM",),
        "leg_opening": ("LEG", "OPENNING", "OPENING"),
    }

    def find_measurement_row(label_words: Tuple[str, ...]) -> Optional[List]:
        """Return the row that contains the first matching label word (substring match)."""
        for row in rows:
            for w in row:
                w_upper = w[0].upper()
                if any(label in w_upper for label in label_words):
                    return row
        return None

    results: Dict[str, str] = {}
    for key, labels in LABELS.items():
        mrow = find_measurement_row(labels)
        if mrow is None:
            logger.debug("Label '%s' not found in OCR output for %s", labels[0], img_url)
            results[key] = ""
            continue

        # Merge adjacent fragments (split fractions) before picking column
        merged = _merge_adjacent_fragments(mrow, max_gap=120)
        raw = _pick_value_at_column(merged, col_x, x_tol=130)

        if raw is None:
            results[key] = ""
            logger.debug("No value near x=%s in %s row for %s", col_x, labels[0], img_url)
            continue

        # Normalise first — this handles known OCR artefacts like "B" → "13"
        value = _normalise_measurement(raw)

        # If the normalised result still looks incomplete (no digit survived),
        # fall back to the most common well-formed value in the row.
        # This handles constant rows like INSEAM where a cell reads "2" instead of "32"".
        if _looks_incomplete(value):
            majority = _row_majority_value(merged)
            if majority:
                logger.debug(
                    "%s: normalised %r still looks incomplete; using row majority %r",
                    labels[0], value, majority,
                )
                value = _normalise_measurement(majority)

        logger.debug("%s for size %s → raw=%r  normalised=%r", key, target_size, raw, value)
        results[key] = value

    return results.get("rise", ""), results.get("inseam", ""), results.get("leg_opening", "")


# ---------------------------------------------------------------------------
# Body-HTML fallback (from rudes_inventory.py)
# ---------------------------------------------------------------------------
def _clean_html(h: str) -> str:
    if not h:
        return ""
    txt = _html.unescape(re.sub(r"<br\s*/?>", " ", h, flags=re.I))
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _parse_number_like(s: str) -> str:
    if not s:
        return ""
    s = s.replace("“", '"').replace("”", '"').replace("″", '"').replace("'", "'")
    s = s.replace("½", " 1/2").replace("¼", " 1/4").replace("¾", " 3/4")
    s = s.replace('"', "").strip()
    # Range values like "11/12" (two multi-digit numbers, no space) are size ranges;
    # use the higher value (e.g. 12 from "11/12") since that is the listed measurement.
    if re.fullmatch(r'\d{2,}/\d{2,}', s):
        parts = s.split('/')
        return str(max(int(parts[0]), int(parts[1])))
    m = re.search(r"(-?\d+(?:\.\d+)?)(?:\s+(\d+)/(\d+))?", s)
    if not m:
        return s.strip()
    base = float(m.group(1))
    if m.group(2) and m.group(3):
        base += float(m.group(2)) / float(m.group(3))
    if base == int(base):
        return str(int(base))
    return f'{base:.3f}'.rstrip('0').rstrip('.')


def extract_measures_from_body(body_html: str) -> Tuple[str, str, str]:
    """Extract Rise / Inseam / Leg Opening from the product description HTML."""
    txt = _clean_html(body_html or "")
    if not txt:
        return ("", "", "")

    def grab(labels: List[str]) -> str:
        for lab in labels:
            # Accept colon, dash, or plain whitespace as separator after label
            m = re.search(
                rf"{re.escape(lab)}\s*[:\-]?\s*([0-9][^,;|<\n]*)",
                txt,
                re.IGNORECASE,
            )
            if m:
                return _parse_number_like(m.group(1).split("|")[0].strip())
        return ""

    rise_patterns = [
        # Try explicit fraction first (e.g. "Rise: 11 1/2"") — the space before the
        # fraction distinguishes it from range values like "Rise: 11/12"" which appear
        # earlier in the HTML in a generic size-26 block.
        r"(?:Front )?Rise:\s*(\d+\s+\d+/\d+[^,;|<\n]*)",   # fractional: "Rise: 11 1/2""
        r"\|\s*Rise\s+(\d+\s+\d+/\d+[^,;|<\n]*)",           # pipe+fraction
        r"(?:Front )?Rise:\s*([0-9][^,;|<\n]*)",             # general (catches range "11/12")
        r"\|\s*Rise\s+([0-9][^,;|<\n]*)",                    # pipe general
    ]
    inseam_patterns = [
        r"Inseam:\s*([0-9][^,;|<\n]*)",             # "Inseam: 32""
        r":\s*Inseam\s+([0-9][^,;|<\n]*)",          # ": Inseam 32""
    ]

    def grab_first(patterns: List[str]) -> str:
        for pat in patterns:
            m = re.search(pat, txt, re.IGNORECASE)
            if m:
                return _parse_number_like(m.group(1).split("|")[0].strip())
        return ""

    rise   = grab_first(rise_patterns)
    inseam = grab_first(inseam_patterns)
    leg    = grab(["Leg Opening", "Leg Openning", "Opening"])
    return rise, inseam, leg


# ---------------------------------------------------------------------------
# Excel writer
# ---------------------------------------------------------------------------
COLUMNS = [
    "product.id",
    "product.handle",
    "product.title",
    "size_chart_url",
    "rise_26",
    "inseam_26",
    "leg_opening_26",
    "measurement_source",
    "notes",
]


def _write_excel(rows: List[Dict], logger: logging.Logger) -> Path:
    try:
        from openpyxl import Workbook
    except ImportError:
        logger.warning("openpyxl not installed; skipping Excel output")
        return Path()

    wb = Workbook()
    ws = wb.active
    ws.title = "Measurements"
    ws.append(COLUMNS)
    for row in rows:
        ws.append([row.get(c, "") for c in COLUMNS])

    # Auto-width
    for col in ws.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    ts = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d_%H-%M-%S")
    path = OUTPUT_DIR / f"rudes_sizechart_ocr_{ts}.xlsx"
    wb.save(path)
    return path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    logger = _configure_logging()
    logger.info("=== Rudes size-chart OCR  (target size: %s) ===", TARGET_SIZE)

    session = build_session()

    products = get_all_products(session, logger)
    if not products:
        logger.error("No products retrieved — aborting")
        return

    # Build a handle→product_json lookup (we need body_html for fallback)
    # products.json includes body_html
    product_map: Dict[str, Dict] = {p["handle"]: p for p in products}
    handles = list(product_map.keys())

    output_rows: List[Dict] = []

    for n, handle in enumerate(handles, start=1):
        prod = product_map[handle]
        prod_id = prod.get("id", "")
        title   = prod.get("title", "")
        body_html = prod.get("body_html", "") or ""

        logger.info("[%d/%d] %s — %s", n, len(handles), handle, title)

        # ── Fetch PDP HTML ────────────────────────────────────────────────────
        from urllib.parse import urlsplit
        parts = urlsplit(COLLECTION_URL)
        pdp_url = f"{parts.scheme}://{parts.netloc}/products/{handle}"
        pdp_html = ""
        try:
            resp = session.get(pdp_url, timeout=REQUEST_TIMEOUT)
            if resp.ok:
                pdp_html = resp.text
        except Exception as exc:
            logger.warning("PDP fetch failed for %s: %s", handle, exc)

        # ── Measurements — new order of operations ────────────────────────────
        # 1. Check for a "size chart" link.  If present → find the image URL and OCR it.
        # 2. If no size chart link → go straight to HTML measurement parsing (skips OCR).
        rise = inseam = leg_opening = ""
        size_chart_url = ""
        source = "not_found"
        notes = ""

        if pdp_html and _has_size_chart_link(pdp_html):
            # Step 1: locate the image URL (try ___ first, then CDN /files/*.webp)
            size_chart_url = find_size_chart_url(pdp_html) or ""
            if size_chart_url:
                logger.info("  Size chart found: %s", size_chart_url)
                rise, inseam, leg_opening = ocr_size_chart(session, size_chart_url, TARGET_SIZE, logger)
                if any([rise, inseam, leg_opening]):
                    source = "ocr"
                else:
                    notes = "ocr_returned_empty"
            else:
                logger.info("  Size chart link present but no image URL found")
                notes = "size_chart_link_but_no_url"
        else:
            logger.debug("  No size chart link — using HTML measurement parsing")

        if not any([rise, inseam, leg_opening]):
            # Prefer pdp_html over products.json body_html: measurement text is in
            # Liquid-rendered metafields visible on the PDP but absent from body_html.
            html_for_fallback = pdp_html or body_html
            rise, inseam, leg_opening = extract_measures_from_body(html_for_fallback)
            if any([rise, inseam, leg_opening]):
                source = "body_html"
                if notes:
                    notes += ";body_html_fallback"
                else:
                    notes = "body_html_fallback" if not size_chart_url else "body_html_fallback_after_ocr"
            else:
                source = "not_found"
                if not notes:
                    notes = "no_chart_and_no_body_measurements"

        output_rows.append(
            {
                "product.id":          prod_id,
                "product.handle":      handle,
                "product.title":       title,
                "size_chart_url":      size_chart_url,
                "rise_26":             rise,
                "inseam_26":           inseam,
                "leg_opening_26":      leg_opening,
                "measurement_source":  source,
                "notes":               notes,
            }
        )

        time.sleep(0.25)

    # ── Write output ─────────────────────────────────────────────────────────
    path = _write_excel(output_rows, logger)
    if path.exists():
        logger.info("Excel written to %s", path)

    # Summary
    ocr_count  = sum(1 for r in output_rows if r["measurement_source"] == "ocr")
    body_count = sum(1 for r in output_rows if r["measurement_source"] == "body_html")
    miss_count = sum(1 for r in output_rows if r["measurement_source"] == "not_found")
    logger.info(
        "Done. %d products: %d via OCR, %d via body_html, %d not found",
        len(output_rows), ocr_count, body_count, miss_count,
    )


if __name__ == "__main__":
    main()
