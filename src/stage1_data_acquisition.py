"""
Stage 1: Data Acquisition
Downloads FIGARO IC-IOT and employment data from Eurostat.
"""
import argparse
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ICIOT_DATASET = "naio_10_fcp_ip1"   # product-by-product IC-IOT
EMP_DATASET = "nama_10_a64_e"

STAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# The 64 CPA product codes in the IC-IOT (in order matching paper's Annex B)
CPA_PRODUCT_CODES = [
    "CPA_A01", "CPA_A02", "CPA_A03",                               # 1-3  A
    "CPA_B",                                                         # 4    B
    "CPA_C10-12", "CPA_C13-15", "CPA_C16", "CPA_C17", "CPA_C18",  # 5-9  C
    "CPA_C19", "CPA_C20", "CPA_C21", "CPA_C22", "CPA_C23",        # 10-14
    "CPA_C24", "CPA_C25", "CPA_C26", "CPA_C27", "CPA_C28",        # 15-19
    "CPA_C29", "CPA_C30", "CPA_C31_32", "CPA_C33",                # 20-23
    "CPA_D35",                                                       # 24   D
    "CPA_E36", "CPA_E37-39",                                        # 25-26 E
    "CPA_F",                                                         # 27   F
    "CPA_G45", "CPA_G46", "CPA_G47",                               # 28-30 G
    "CPA_H49", "CPA_H50", "CPA_H51", "CPA_H52", "CPA_H53",        # 31-35 H
    "CPA_I",                                                         # 36   I
    "CPA_J58", "CPA_J59_60", "CPA_J61", "CPA_J62_63",             # 37-40 J
    "CPA_K64", "CPA_K65", "CPA_K66",                               # 41-43 K
    "CPA_L",                                                         # 44   L
    "CPA_M69_70", "CPA_M71", "CPA_M72", "CPA_M73", "CPA_M74_75",  # 45-49 M
    "CPA_N77", "CPA_N78", "CPA_N79", "CPA_N80-82",                 # 50-53 N
    "CPA_O84",                                                       # 54   O
    "CPA_P85",                                                       # 55   P
    "CPA_Q86", "CPA_Q87_88",                                        # 56-57 Q
    "CPA_R90-92", "CPA_R93",                                        # 58-59 R
    "CPA_S94", "CPA_S95", "CPA_S96",                               # 60-62 S
    "CPA_T",                                                         # 63   T
    "CPA_U",                                                         # 64   U
]

# Value-added row codes in prd_ava
VALUE_ADDED_CODES = ["B2A3G", "D1", "D21X31", "D29X39", "OP_RES", "OP_NRES"]

# Final demand column codes in prd_use
FINAL_DEMAND_CODES = ["P3_S13", "P3_S14", "P3_S15", "P5M", "P51G"]

# NACE codes for employment (matching the 64 CPA products above, in same order)
NACE_EMP_CODES = [
    "A01", "A02", "A03",
    "B",
    "C10-C12", "C13-C15", "C16", "C17", "C18",
    "C19", "C20", "C21", "C22", "C23",
    "C24", "C25", "C26", "C27", "C28",
    "C29", "C30", "C31_C32", "C33",
    "D35",
    "E36", "E37-E39",
    "F",
    "G45", "G46", "G47",
    "H49", "H50", "H51", "H52", "H53",
    "I",
    "J58", "J59_J60", "J61", "J62_J63",
    "K64", "K65", "K66",
    "L68",
    "M69_M70", "M71", "M72", "M73", "M74_M75",
    "N77", "N78", "N79", "N80-N82",
    "O84",
    "P85",
    "Q86", "Q87_Q88",
    "R90-R92", "R93",
    "S94", "S95", "S96",
    "T",
    "U99",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path, year: int) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"stage1_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("stage1")


def fetch_json(url: str, retries: int = 3, delay: float = 2.0) -> dict:
    """Fetch a URL and return parsed JSON, with retry logic."""
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code == 413:
                raise RuntimeError(f"413 Request too large: {url}") from e
            logging.warning(f"HTTP {e.code} on attempt {attempt}: {url}")
        except Exception as e:
            logging.warning(f"Attempt {attempt} failed: {e}")
        if attempt < retries:
            time.sleep(delay * attempt)
    raise RuntimeError(f"Failed to fetch after {retries} attempts: {url}")


def decode_statjson(data: dict) -> list[dict]:
    """
    Decode Eurostat statistics JSON (version 2.0) into a flat list of dicts.
    Returns rows with one column per dimension + 'value'.
    """
    dims = data.get("id", [])
    size = data.get("size", [])
    values = data.get("value", {})

    # Build lookup: for each dimension, index → code
    dim_idx2code = {}
    for dim in dims:
        cats = data.get("dimension", {}).get(dim, {}).get("category", {})
        idx2code = {v: k for k, v in cats.get("index", {}).items()}
        dim_idx2code[dim] = idx2code

    rows = []
    # Compute strides for multi-dimensional indexing
    strides = []
    stride = 1
    for s in reversed(size):
        strides.insert(0, stride)
        stride *= s

    for flat_idx_str, val in values.items():
        flat_idx = int(flat_idx_str)
        row = {}
        remaining = flat_idx
        for dim, s, st in zip(dims, size, strides):
            dim_i = remaining // st
            remaining = remaining % st
            row[dim] = dim_idx2code[dim].get(dim_i, f"?{dim_i}")
        row["value"] = val
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# IC-IOT download
# ---------------------------------------------------------------------------

def download_iciot_for_country(c_orig: str, year: int) -> list[dict]:
    """Download all IC-IOT rows for a given country of origin."""
    url = (
        f"{STAT_API}/{ICIOT_DATASET}"
        f"?c_orig={c_orig}&time={year}&unit=MIO_EUR&format=JSON&lang=EN"
    )
    logging.info(f"  Fetching IC-IOT for c_orig={c_orig} year={year} ...")
    data = fetch_json(url)
    rows = decode_statjson(data)
    return rows


def download_iciot(eu_countries: list[str], all_countries_in_table: list[str],
                   year: int, raw_dir: Path) -> Path:
    """
    Download the IC-IOT for all countries of origin present in the table.
    Saves to data/raw/figaro_iciot_{year}.jsonl (one JSON object per line).
    Returns the output path.
    """
    out_path = raw_dir / f"figaro_iciot_{year}.jsonl"
    if out_path.exists():
        logging.info(f"IC-IOT already downloaded: {out_path}")
        return out_path

    # We need all c_orig values: EU-28 + non-EU that appear in the table.
    # The table has 50 countries; we download all of them so we have the full picture.
    # Start with EU countries (most important) then non-EU.
    all_orig_countries = list(all_countries_in_table)
    logging.info(f"Downloading IC-IOT for {len(all_orig_countries)} origin countries...")

    with open(out_path, "w") as fout:
        for i, c_orig in enumerate(all_orig_countries, 1):
            logging.info(f"  [{i}/{len(all_orig_countries)}] c_orig={c_orig}")
            rows = download_iciot_for_country(c_orig, year)
            for row in rows:
                fout.write(json.dumps(row) + "\n")
            logging.info(f"    -> {len(rows)} rows")
            # Small delay to be polite to the API
            time.sleep(0.5)

    logging.info(f"IC-IOT saved to {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Employment download
# ---------------------------------------------------------------------------

def download_employment(eu_countries: list[str], year: int, raw_dir: Path) -> Path:
    """
    Download employment data (EMP_DC, THS_PER) for all EU countries and
    all 64 NACE industries, one country at a time (URL length limit).
    Saves to data/raw/employment_{year}.jsonl.
    """
    out_path = raw_dir / f"employment_{year}.jsonl"
    if out_path.exists():
        logging.info(f"Employment data already downloaded: {out_path}")
        return out_path

    # The nace_r2 filter is unreliable in this API — download all NACE codes
    # per country and post-filter to the 64 we need.
    nace_target = set(NACE_EMP_CODES)
    logging.info(f"Fetching employment data for {len(eu_countries)} countries (all NACE, post-filtered)...")

    total_rows = 0
    with open(out_path, "w") as fout:
        for i, geo in enumerate(eu_countries, 1):
            url = (
                f"{STAT_API}/{EMP_DATASET}"
                f"?na_item=EMP_DC&unit=THS_PER&geo={geo}"
                f"&time={year}&format=JSON&lang=EN"
            )
            logging.info(f"  [{i}/{len(eu_countries)}] Employment for geo={geo} ...")
            data = fetch_json(url)
            rows = decode_statjson(data)
            # Keep only the 64 target NACE codes
            filtered = [r for r in rows if r.get("nace_r2", "") in nace_target]
            logging.info(f"    -> {len(rows)} rows fetched, {len(filtered)} match target NACE codes")
            for row in filtered:
                fout.write(json.dumps(row) + "\n")
            total_rows += len(filtered)
            time.sleep(0.3)

    logging.info(f"Employment saved to {out_path} ({total_rows} total rows)")
    return out_path


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify_and_summarise(iciot_path: Path, emp_path: Path,
                          eu_countries: list[str], year: int,
                          raw_dir: Path) -> None:
    """
    Produce a summary/verification report. Saves to data/raw/data_summary_{year}.txt.
    """
    summary_path = raw_dir / f"data_summary_{year}.txt"
    lines = []

    # --- IC-IOT summary ---
    lines.append("=== FIGARO IC-IOT Summary ===")
    c_orig_set, c_dest_set, prd_ava_set, prd_use_set = set(), set(), set(), set()
    iciot_count = 0
    with open(iciot_path) as f:
        for line in f:
            row = json.loads(line)
            c_orig_set.add(row.get("c_orig", ""))
            c_dest_set.add(row.get("c_dest", ""))
            prd_ava_set.add(row.get("prd_ava", ""))
            prd_use_set.add(row.get("prd_use", ""))
            iciot_count += 1

    lines.append(f"  Total rows (non-zero cells): {iciot_count}")
    lines.append(f"  Countries of origin (c_orig): {len(c_orig_set)} - {sorted(c_orig_set)}")
    lines.append(f"  Countries of destination (c_dest): {len(c_dest_set)} - {sorted(c_dest_set)}")
    lines.append(f"  Products available (prd_ava): {len(prd_ava_set)} - {sorted(prd_ava_set)}")
    lines.append(f"  Products used (prd_use): {len(prd_use_set)} - {sorted(prd_use_set)}")

    eu_in_orig = set(eu_countries) & c_orig_set
    missing_eu_orig = set(eu_countries) - c_orig_set
    lines.append(f"  EU countries in c_orig: {len(eu_in_orig)}/28")
    if missing_eu_orig:
        lines.append(f"  MISSING from c_orig: {sorted(missing_eu_orig)}")

    cpa_in_ava = set(CPA_PRODUCT_CODES) & prd_ava_set
    missing_cpa = set(CPA_PRODUCT_CODES) - prd_ava_set
    lines.append(f"  CPA product codes in prd_ava: {len(cpa_in_ava)}/64")
    if missing_cpa:
        lines.append(f"  MISSING CPA codes: {sorted(missing_cpa)}")

    # --- Employment summary ---
    lines.append("")
    lines.append("=== Employment Summary ===")
    emp_by_geo = {}
    nace_found = set()
    with open(emp_path) as f:
        for line in f:
            row = json.loads(line)
            geo = row.get("geo", "")
            nace = row.get("nace_r2", "")
            val = row.get("value", 0) or 0
            nace_found.add(nace)
            emp_by_geo[geo] = emp_by_geo.get(geo, 0) + val

    lines.append(f"  NACE codes found: {len(nace_found)} - {sorted(nace_found)}")

    eu_total = sum(v for g, v in emp_by_geo.items() if g in eu_countries)
    lines.append(f"  EU-28 total employment (sum over countries+industries): {eu_total:.0f} thousand persons")
    lines.append(f"  Paper reference (2010): ~225,677 thousand persons")
    pct_diff = abs(eu_total - 225677) / 225677 * 100
    lines.append(f"  Deviation from paper: {pct_diff:.1f}%")
    if pct_diff > 5:
        lines.append(f"  WARNING: Deviation > 5% - review before proceeding")

    lines.append("")
    lines.append("  Employment by country (thousand persons):")
    for geo in sorted(emp_by_geo):
        if geo in eu_countries:
            lines.append(f"    {geo}: {emp_by_geo[geo]:.1f}")

    missing_nace = set(NACE_EMP_CODES) - nace_found
    if missing_nace:
        lines.append(f"  WARNING - NACE codes not found: {sorted(missing_nace)}")

    summary_text = "\n".join(lines)
    with open(summary_path, "w") as f:
        f.write(summary_text)

    logging.info(f"Summary saved to {summary_path}")
    logging.info(f"EU-28 employment total: {eu_total:.0f} thousand (paper: ~225,677)")
    if pct_diff > 5:
        logging.warning(f"Employment deviation from paper is {pct_diff:.1f}% (> 5% threshold)")


# ---------------------------------------------------------------------------
# Probe: get all countries present in the table
# ---------------------------------------------------------------------------

def get_all_iciot_countries(year: int) -> list[str]:
    """
    Get the full list of c_orig (and c_dest) countries in the IC-IOT
    by querying a single well-known country.
    """
    url = (
        f"{STAT_API}/{ICIOT_DATASET}"
        f"?c_orig=DE&time={year}&unit=MIO_EUR&format=JSON&lang=EN"
    )
    data = fetch_json(url)
    c_dest_cats = data.get("dimension", {}).get("c_dest", {}).get("category", {})
    return list(c_dest_cats.get("index", {}).keys())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Stage 1: Data Acquisition")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    year = cfg["reference_year"]
    eu_countries = cfg["eu_member_states"]

    base_dir = Path(args.config).parent
    raw_dir = base_dir / "data" / "raw"
    log_dir = base_dir / "logs"
    raw_dir.mkdir(parents=True, exist_ok=True)

    log = setup_logging(log_dir, year)
    log.info(f"=== Stage 1: Data Acquisition (year={year}) ===")

    # 1. Get all countries present in the IC-IOT table
    log.info("Probing IC-IOT table for available countries...")
    all_iciot_countries = get_all_iciot_countries(year)
    log.info(f"  Found {len(all_iciot_countries)} countries in IC-IOT: {all_iciot_countries}")

    # 2. Download IC-IOT
    iciot_path = download_iciot(eu_countries, all_iciot_countries, year, raw_dir)

    # 3. Download employment
    emp_path = download_employment(eu_countries, year, raw_dir)

    # 4. Verify and summarise
    verify_and_summarise(iciot_path, emp_path, eu_countries, year, raw_dir)

    log.info("=== Stage 1 complete ===")


if __name__ == "__main__":
    main()
