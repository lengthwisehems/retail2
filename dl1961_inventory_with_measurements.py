"""DL1961 monthly inventory scraper with measurement fallback."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from dl1961_inventory import (
    OUTPUT_DIR,
    CSV_HEADERS,
    SESSION,
    LOGGER as BASE_LOGGER,
    assemble_rows,
    fetch_searchspring,
    fetch_storefront_products,
)

LOG_PATH = Path(__file__).resolve().parent / "dl1961_measurements_run.log"
FALLBACK_LOG_PATH = OUTPUT_DIR / "dl1961_measurements_run.log"


def configure_logging() -> logging.Logger:
    handlers: List[logging.Handler] = []
    selected_path: Optional[Path] = None
    for path in (LOG_PATH, FALLBACK_LOG_PATH):
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            handler = logging.FileHandler(path)
            handlers.append(handler)
            selected_path = path
            if path != LOG_PATH:
                BASE_LOGGER.warning(
                    "Primary measurement log unavailable; using fallback at %s", path
                )
            break
        except (OSError, PermissionError) as exc:
            BASE_LOGGER.warning("Unable to open measurement log %s: %s", path, exc)
    handlers.append(logging.StreamHandler())
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    for handler in handlers:
        logger.addHandler(handler)
    if selected_path is None:
        logger.warning("Measurement run writing to console only (no file log).")
    return logger


LOGGER = configure_logging()

MEASUREMENT_HOSTS = [
    "https://www.dl1961.com",
    "https://dl1961.com",
]


class MeasurementFetcher:
    def __init__(self) -> None:
        self.cache: Dict[str, Dict[str, Optional[float]]] = {}

    def __call__(self, handle: str) -> Dict[str, Optional[float]]:
        if handle in self.cache:
            return self.cache[handle]
        result = self.fetch_from_html(handle)
        self.cache[handle] = result
        return result

    def fetch_from_html(self, handle: str) -> Dict[str, Optional[float]]:
        for base in MEASUREMENT_HOSTS:
            url = f"{base.rstrip('/')}/products/{handle}".replace("//products", "/products")
            try:
                response = SESSION.get(url, timeout=30, headers={"Accept": "text/html"})
            except requests.RequestException as exc:
                LOGGER.warning("Measurement fetch %s failed: %s", url, exc)
                continue
            if response.status_code == 404:
                LOGGER.warning("Measurement fetch for %s returned 404", url)
                continue
            if response.status_code != 200:
                LOGGER.warning(
                    "Measurement fetch %s returned %s", url, response.status_code
                )
                continue
            return self.parse_html(response.text)
        return {"rise": None, "inseam": None, "leg_opening": None}

    @staticmethod
    def parse_html(html: str) -> Dict[str, Optional[float]]:
        soup = BeautifulSoup(html, "html.parser")
        container = soup.select_one("div.pro-benefits")
        if not container:
            return {"rise": None, "inseam": None, "leg_opening": None}
        text = container.get_text(" ", strip=True).lower()
        pattern_map = {
            "rise": r"rise[:\s]*([0-9]+(?:\.[0-9]+)?)",
            "inseam": r"inseam[:\s]*([0-9]+(?:\.[0-9]+)?)",
            "leg_opening": r"leg opening[:\s]*([0-9]+(?:\.[0-9]+)?)",
        }
        measurements: Dict[str, Optional[float]] = {"rise": None, "inseam": None, "leg_opening": None}
        for key, pattern in pattern_map.items():
            match = re.search(pattern, text)
            if match:
                try:
                    measurements[key] = float(match.group(1))
                except ValueError:
                    measurements[key] = None
        return measurements


def write_csv(rows: List[List[str]], filename: str) -> None:
    path = OUTPUT_DIR / filename
    with path.open("w", newline="", encoding="utf-8") as f:
        import csv

        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
        writer.writerows(rows)


def main() -> None:
    search_hits = fetch_searchspring()
    products = fetch_storefront_products()
    measurement_fetcher = MeasurementFetcher()
    rows = assemble_rows(products, search_hits, measurement_fetcher)
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"DL1961_Measurements_{timestamp}.csv"
    write_csv(rows, filename)
    LOGGER.info("Wrote %s rows to %s", len(rows), filename)


if __name__ == "__main__":
    main()

