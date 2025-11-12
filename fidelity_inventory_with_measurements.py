"""Fidelity Denim monthly scraper with PDP measurement fallback."""
from __future__ import annotations

import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup

import fidelity_inventory as base

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "Output"
LOG_PATH = BASE_DIR / "fidelity_measurements_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "fidelity_measurements_run.log"

OUTPUT_DIR.mkdir(exist_ok=True)

LOGGER = logging.getLogger("fidelity_inventory.measurements")
LOGGER.setLevel(logging.INFO)


def configure_logging() -> logging.Logger:
    handlers: List[logging.Handler] = []
    selected_path: Path | None = None
    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path)
            handlers.append(handler)
            selected_path = path
            if path != LOG_PATH:
                print(
                    f"WARNING: Primary log path {LOG_PATH} unavailable. Using fallback log at {path}.",
                    flush=True,
                )
            break
        except OSError as exc:
            print(f"WARNING: Unable to open log file {path}: {exc}", flush=True)
            continue

    if not handlers:
        handlers.append(logging.StreamHandler())

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
        LOGGER.addHandler(handler)

    LOGGER.info("Logging initialized (path=%s)", selected_path or "stream-only")
    base.LOGGER = LOGGER  # Route shared helper logs to the measurement logger.
    return LOGGER


def fetch_product_html(handle: str) -> str:
    response = base.perform_shopify_request("GET", f"/products/{handle}", logger=LOGGER)
    return response.text


def parse_product_sections(html: str) -> Tuple[Dict[str, str], str]:
    soup = BeautifulSoup(html, "html.parser")
    measurement_text = ""
    description_sections: List[str] = []
    for details in soup.select("div.product-detail-accordion details.cc-accordion-item"):
        summary = details.find("summary", class_="cc-accordion-item__title")
        if not summary:
            continue
        heading_raw = summary.get_text(strip=True)
        if not heading_raw:
            continue
        content = details.find("div", class_="cc-accordion-item__content")
        if not content:
            continue
        text = content.get_text(" ", strip=True)
        cleaned_text = base.clean_description_text(text)
        heading_upper = heading_raw.upper()
        if heading_upper == "SIZE + FIT":
            measurement_text = cleaned_text
            continue
        if cleaned_text:
            description_sections.append(f"{heading_raw.upper()} {cleaned_text}")
    measurements = base.parse_measurements_from_text(measurement_text)
    description = " ".join(description_sections).strip()
    return measurements, description


def gather_html_overrides(products: List[Dict[str, object]]) -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    measurement_overrides: Dict[str, Dict[str, str]] = {}
    description_overrides: Dict[str, str] = {}
    for product in products:
        style_id = base.normalize_shopify_id_length(
            base.stringify_identifier(product.get("id"))
        )
        handle = product.get("handle", "")
        if not handle:
            continue

        description = base.clean_description_text(product.get("description", ""))
        initial_measurements = base.parse_measurements_from_text(description)
        needs_measurements = any(
            not initial_measurements.get(key)
            for key in ("rise", "back_rise", "inseam", "leg_opening")
        )
        needs_description = len(description) < 100
        if not needs_measurements and not needs_description:
            continue

        try:
            html = fetch_product_html(handle)
        except Exception as exc:  # pragma: no cover - network path
            LOGGER.warning("Failed to fetch HTML for %s: %s", handle, exc)
            continue

        measurements, full_description = parse_product_sections(html)
        if needs_measurements and measurements:
            measurement_overrides[style_id] = measurements
        if needs_description and full_description:
            description_overrides[style_id] = full_description
        time.sleep(0.2)

    LOGGER.info(
        "Applied HTML overrides to %s styles (measurements) and %s descriptions",
        len(measurement_overrides),
        len(description_overrides),
    )
    return measurement_overrides, description_overrides


def write_csv(rows: List[Dict[str, str]]) -> str:
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"FIDELITY_Measurements_{timestamp}.csv"
    output_path = OUTPUT_DIR / filename
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=base.CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)
    LOGGER.info("Wrote %s rows to %s", len(rows), output_path)
    return str(output_path)


def main() -> str:
    configure_logging()
    products = base.fetch_products(LOGGER)
    if not products:
        raise RuntimeError("No products returned from GraphQL")
    measurement_overrides, description_overrides = gather_html_overrides(products)
    variant_old_qty, product_line_map = base.fetch_globo_mappings(LOGGER)
    rows = base.assemble_rows(
        products,
        variant_old_qty,
        product_line_map,
        measurement_overrides=measurement_overrides,
        description_overrides=description_overrides,
    )
    if not rows:
        raise RuntimeError("No rows assembled for CSV output")
    return write_csv(rows)


if __name__ == "__main__":
    try:
        result = main()
        print(result)
    except Exception as exc:  # pragma: no cover - entry point logging
        LOGGER.exception("Run failed: %s", exc)
        raise

