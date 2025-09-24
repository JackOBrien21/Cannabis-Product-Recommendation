# path: scripts/scrape_coas_from_excel.py
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) CodeCopilotScraper/1.1"
OUT_ROOT = Path("out")

# --- Robust matchers based on your page source ---
ALLEAVES_PDF_PAT = re.compile(r"alleaves\.com/.+\.pdf(?:$|\?)", re.I)
COAISH_PDF_PAT = re.compile(r"(?:\bcoa\b|certificate|analysis).+\.pdf(?:$|\?)", re.I)

EFFECT_NAME_RE = re.compile(
    r"(Creative|Energized|Happy|Calm|Sleepy|Hungry|Focused|Relaxed|Talkative|Uplifted|Giggly|Euphoric|Sociable|Aroused|Tingly)",
    re.I,
)

SAFE_FN = re.compile(r"[^A-Za-z0-9._-]+")

# ---------- CLI ----------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Scrape COAs from Excel list of product URLs.")
    ap.add_argument("--input", required=True, help="Excel (.xlsx/.xls) or CSV with URLs (URL default in col 2, Type in col 1).")
    ap.add_argument("--sheet", default=None, help="Excel sheet name or index.")
    ap.add_argument("--url-col", type=int, default=2, help="1-based URL column (fallback if no header). Default: 2")
    ap.add_argument("--type-col", type=int, default=1, help="1-based Type column (0=disable). Default: 1")
    ap.add_argument("--out-subdir", default="coas_rockland", help="PDF subfolder under ./out. Default: coas_rockland")
    ap.add_argument("--index-name", default="product_index_rockland.csv", help="Index CSV name under ./out. Default: product_index_rockland.csv")
    ap.add_argument("--sleep", type=float, default=0.5, help="Polite pause between downloads. Default: 0.5s")
    ap.add_argument("--max", type=int, default=0, help="Process at most N rows (0=all).")
    ap.add_argument("--force", action="store_true", help="Re-download even if target PDF exists.")
    return ap.parse_args()

# ---------- IO ----------
def ensure_dirs(out_subdir: str, index_name: str) -> Tuple[Path, Path]:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    pdf_dir = OUT_ROOT / out_subdir
    pdf_dir.mkdir(parents=True, exist_ok=True)
    index_csv = OUT_ROOT / index_name
    if not index_csv.exists():
        with index_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "type","product_href","coa_url","company","product_name","weight","effects",
                    "local_path","sha256","size_kb","status","error",
                ],
            )
            writer.writeheader()
    return pdf_dir, index_csv

def append_index_row(index_csv: Path, row: Dict[str, str]) -> None:
    with index_csv.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "type","product_href","coa_url","company","product_name","weight","effects",
                "local_path","sha256","size_kb","status","error",
            ],
        )
        writer.writerow(row)

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def http_get(url: str, expect_pdf: bool = False) -> requests.Response:
    r = requests.get(
        url,
        headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/pdf,*/*"},
        timeout=50,
    )
    r.raise_for_status()
    if expect_pdf:
        ctype = (r.headers.get("Content-Type") or "").lower()
        if ("pdf" not in ctype and "octet-stream" not in ctype) and not url.lower().endswith(".pdf"):
            # Some servers mislabel; if URL ends with .pdf we tolerate
            raise ValueError(f"Expected PDF-like content, got {ctype or 'unknown'}")
    return r

# ---------- Parsing ----------
def parse_company_product_weight_from_url(product_url: str) -> Tuple[str, str, str]:
    """
    From: .../menu/{company}/{company}-{product_name}-{weight}
    """
    slug_part = product_url.split("/menu/", 1)[-1]  # "{company}/{slug}"
    parts = slug_part.split("/")
    company = (parts[0] if parts else "").strip()
    slug = (parts[1] if len(parts) > 1 else "").strip()

    m = re.search(r"(\d+(?:\.\d+)?(?:g|mg|oz))$", slug)
    weight = m.group(1) if m else ""
    product_name = slug
    if company and product_name.startswith(company):
        product_name = product_name[len(company):].lstrip("-")
    if weight:
        product_name = product_name[: -len(weight)].rstrip("-")
    return company, product_name, weight

def clean_effect_token(txt: str) -> Optional[str]:
    # Why: SVG <title>EFFECT_*</title> leaks into text; keep only human labels.
    m = EFFECT_NAME_RE.search(txt or "")
    return m.group(1).capitalize() if m else None

def parse_effects(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    # Find the Effects section header, then collect its anchor labels
    effects: List[str] = []
    for h2 in soup.find_all("h2"):
        if "effects" in h2.get_text(strip=True).lower():
            section = h2.find_parent("section") or h2.parent
            if not section:
                break
            # Prefer anchors in the effects section; labels are inside the anchor's div
            for a in section.find_all("a"):
                label = clean_effect_token(a.get_text(" ", strip=True))
                if label:
                    effects.append(label)
            break
    # Dedupe while preserving order
    seen, out = set(), []
    for e in effects:
        if e not in seen:
            seen.add(e)
            out.append(e)
    return out

def find_coa_href(html: str, base_url: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")

    # 1) Exact: the visible "Download COA" link on the product page (as in your snippet)
    # <div class="sc-50575688-0 eZMgns"><a href="...pdf">Download COA</a></div>  ← sample
    a = soup.find("a", string=lambda s: s and "coa" in s.lower())
    if a and a.get("href"):
        href = a["href"].strip()
        return href if href.startswith("http") else requests.compat.urljoin(base_url, href)

    # 2) Any Alleaves PDF
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if ALLEAVES_PDF_PAT.search(href):
            return href if href.startswith("http") else requests.compat.urljoin(base_url, href)

    # 3) Any COA-ish .pdf
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.lower().endswith(".pdf") and COAISH_PDF_PAT.search(href.lower()):
            return href if href.startswith("http") else requests.compat.urljoin(base_url, href)

    return None

def safe_filename(*parts: str, suffix: str = ".pdf") -> str:
    base = "_".join(p for p in parts if p).strip("_")
    base = SAFE_FN.sub("_", base).strip("_") or "coa"
    return (base[:140] + suffix)

# ---------- Excel/CSV loader ----------
def _pick_col_by_header(df: pd.DataFrame, preferred: List[str]) -> Optional[str]:
    names = {str(c).strip().lower(): c for c in df.columns}
    for k in preferred:
        if k in names:
            return names[k]
    return None

def _series_by_pos(df: pd.DataFrame, one_based: int) -> Optional[pd.Series]:
    if one_based <= 0:
        return None
    idx = one_based - 1
    if idx < 0 or idx >= len(df.columns):
        return None
    return df[df.columns[idx]]

def load_rows(input_path: Path, sheet: Optional[str|int], url_col_1b: int, type_col_1b: int) -> List[Tuple[str, str]]:
    if input_path.suffix.lower() in (".xlsx", ".xls"):
        df = pd.read_excel(input_path, sheet_name=sheet) if sheet is not None else pd.read_excel(input_path)
    else:
        df = pd.read_csv(input_path)

    # header-based first
    url_hdr = _pick_col_by_header(df, ["url","href","link","product_url","product url"])
    type_hdr = _pick_col_by_header(df, ["type","category","group"])

    url_s = df[url_hdr] if url_hdr else _series_by_pos(df, url_col_1b)
    if url_s is None:
        raise ValueError("Cannot locate URL column. Provide --url-col or add a URL header.")
    type_s = df[type_hdr] if type_hdr else (_series_by_pos(df, type_col_1b) if type_col_1b > 0 else None)

    out: List[Tuple[str, str]] = []
    for i in range(len(df)):
        url = str(url_s.iloc[i]).strip()
        if not url.startswith("http"):
            continue
        ptype = str(type_s.iloc[i]).strip().lower() if type_s is not None else ""
        out.append((ptype, url))

    # Dedupe by URL (keep first)
    seen, deduped = set(), []
    for ptype, url in out:
        key = url.rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append((ptype, url))
    return deduped

# ---------- Worker ----------
def process_one(url: str, ptype: str, pdf_dir: Path, index_csv: Path, sleep_s: float, force: bool) -> None:
    rec = {
        "type": ptype or "",
        "product_href": url,
        "coa_url": "",
        "company": "",
        "product_name": "",
        "weight": "",
        "effects": "[]",
        "local_path": "",
        "sha256": "",
        "size_kb": "",
        "status": "",
        "error": "",
    }
    try:
        page = http_get(url)
        html = page.text

        company, pname, weight = parse_company_product_weight_from_url(url)
        rec["company"], rec["product_name"], rec["weight"] = company, pname, weight

        effects = parse_effects(html)
        rec["effects"] = json.dumps(effects, ensure_ascii=False)

        coa_url = find_coa_href(html, url)
        if not coa_url:
            rec["status"] = "no_coa"
            print(f"   [warn] COA not found")
            append_index_row(index_csv, rec)
            return
        rec["coa_url"] = coa_url
        print(f"   [ok] COA: {coa_url}")

        # Filename: {Brand}_{Product}_{Weight}.pdf
        fname = safe_filename(company, pname, weight, suffix=".pdf")
        pdf_path = pdf_dir / fname

        if pdf_path.exists() and not force:
            print(f"   [skip] exists -> {pdf_path.name}")
        else:
            pdf = http_get(coa_url, expect_pdf=True)
            with pdf_path.open("wb") as f:
                f.write(pdf.content)
            print(f"   [save] {pdf_path}  ({len(pdf.content)} bytes)")
            time.sleep(sleep_s)

        rec["local_path"] = str(pdf_path)
        rec["sha256"] = sha256_file(pdf_path) if pdf_path.exists() else ""
        rec["size_kb"] = str(pdf_path.stat().st_size // 1024) if pdf_path.exists() else ""
        rec["status"] = "ok"
        append_index_row(index_csv, rec)

    except Exception as e:
        rec["status"] = "error"
        rec["error"] = str(e)
        print(f"   [err] {e}")
        append_index_row(index_csv, rec)

# ---------- Main ----------
def main():
    args = parse_args()
    pdf_dir, index_csv = ensure_dirs(args.out_subdir, args.index_name)

    rows = load_rows(Path(args.input), args.sheet, args.url_col, args.type_col)
    total = len(rows)
    if args.max and total > args.max:
        rows = rows[: args.max]
    print(f"[info] loaded {len(rows)} rows (of {total}). PDFs -> {pdf_dir}  Index -> {index_csv}")

    for i, (ptype, url) in enumerate(rows, 1):
        print(f"[{i}/{len(rows)}] {url}  (type='{ptype or 'unknown'}')")
        process_one(url, ptype, pdf_dir, index_csv, args.sleep, args.force)

if __name__ == "__main__":
    sys.exit(main())
