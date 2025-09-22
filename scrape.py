# path: scripts/extract_product_hrefs.py
"""
Extract product hrefs from saved category HTML pages.

Looks for: <a data-testid="product-card-menu-link-body" href="..."> … </a>

Usage:
  python scripts/extract_product_hrefs.py --dir html_formats
"""

from __future__ import annotations
import argparse
from pathlib import Path
from typing import Dict, List
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Group → filename mapping
FILE_MAP: Dict[str, str] = {
    "concentrates": "html_format_concentrates.txt",
    "edibles":      "html_format_edibles.txt",
    "flower":       "html_format_flower.txt",
    "hash":         "html_format_hash.txt",
    "pre-rolls":    "html_format_pre-rolls.txt",
    "vaporizers":   "html_format_vaporizers.txt",
}

DOMAIN = "https://cannabisrealmny.com/"

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Extract product hrefs from saved HTML files.")
    ap.add_argument("--dir", default="html_formats", help="Directory with html_format_*.txt files")
    ap.add_argument("--counts-only", action="store_true", help="Only print counts per group")
    return ap.parse_args()

def extract_hrefs_from_file(path: Path) -> List[str]:
    html = path.read_text(encoding="utf-8", errors="ignore")
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.select('a[data-testid="product-card-menu-link-body"][href]')
    seen = set()
    hrefs: List[str] = []
    for a in anchors:
        href = a.get("href", "").strip()
        if href and href not in seen:
            seen.add(href)
            hrefs.append(urljoin(DOMAIN, href))  # prefix with domain
    return hrefs

def main() -> int:
    args = parse_args()
    root = Path(args.dir)

    if not root.exists():
        print(f"[ERR] Directory not found: {root.resolve()}")
        return 2

    grand_total = 0
    combined_seen = set()

    for group, fname in FILE_MAP.items():
        fpath = root / fname
        if not fpath.exists():
            print(f"[WARN] Missing file for {group}: {fpath}")
            continue

        hrefs = extract_hrefs_from_file(fpath)
        count = len(hrefs)
        grand_total += count
        print(f"\n=== {group} ===")
        print(f"File: {fpath.name}")
        print(f"Hrefs found: {count}")

        if not args.counts_only:
            for h in hrefs:
                print(h)

        combined_seen.update(hrefs)

    print("\n=== SUMMARY ===")
    print(f"Groups processed: {len(FILE_MAP)}")
    print(f"Combined unique hrefs: {len(combined_seen)}")
    print(f"Grand total (raw, may include duplicates across groups): {grand_total}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
