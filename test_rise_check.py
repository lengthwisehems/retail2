#!/usr/bin/env python3
"""Quick spot-check: verify Rise extraction for 6 known handles."""
import sys
sys.path.insert(0, "/home/user/retail2")

from paige_inventory_forAzure import PaigeScraper

CHECKS = [
    ("women-high-rise-laurel-canyon-32in-berlin",                    "10.5"),
    ("women-paxton-glastonbury",                                      "9.5"),
    ("women-mason-ankle-overdrive",                                  "10.25"),
    ("women-hoxton-ultra-skinny-aerial",                             "10"),
    ("women-hr-laurel-canyon-32in-reverse-seamed-beltloops-bespoke", "10.5"),
    ("women-cindy-venetian-breeze",                                  "10.5"),
]

scraper = PaigeScraper()
try:
    print()
    print(f"{'Handle':<58} {'Got':>7} {'Expected':>9} {'':>6}")
    print("-" * 85)
    results = []
    for handle, expected in CHECKS:
        fields = scraper.fetch_pdp_fields(handle)
        got = fields.get("rise", "")
        ok = got == expected
        results.append(ok)
        status = "PASS" if ok else "FAIL"
        print(f"{handle:<58} {got:>7} {expected:>9} {status:>6}")
    print()
    if all(results):
        print("ALL PASS")
    else:
        failed = sum(1 for r in results if not r)
        print(f"{failed}/{len(results)} FAILED")
finally:
    scraper.browser_extractor.close()
