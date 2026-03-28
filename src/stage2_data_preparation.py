"""
Stage 2: Data Preparation
Parse raw IC-IOT and employment data into analysis-ready matrices.
Implements the Arto et al. (2015) export definition.
Uses vectorized pandas operations for performance.
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from src.stage1_data_acquisition import (
    CPA_PRODUCT_CODES,
    FINAL_DEMAND_CODES,
    NACE_EMP_CODES,
    VALUE_ADDED_CODES,
)


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"stage2_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("stage2")


# ---------------------------------------------------------------------------
# IC-IOT loading
# ---------------------------------------------------------------------------

def load_iciot(iciot_path: Path) -> pd.DataFrame:
    """
    Load raw IC-IOT JSONL into a DataFrame using chunked reading.
    Only keeps rows with non-zero values and known product codes.
    """
    log = logging.getLogger("stage2")
    log.info(f"Loading IC-IOT JSONL from {iciot_path} (chunked)...")

    cpa_set = set(CPA_PRODUCT_CODES)
    fd_set = set(FINAL_DEMAND_CODES)
    va_set = set(VALUE_ADDED_CODES)
    keep_prd_use = cpa_set | fd_set

    chunks = []
    chunk_size = 200_000
    with pd.read_json(iciot_path, lines=True, chunksize=chunk_size) as reader:
        for i, chunk in enumerate(reader):
            if i % 10 == 0:
                log.info(f"  Processing chunk {i} ({i * chunk_size:,} rows read)...")
            # Keep only the columns we need
            chunk = chunk[["c_orig", "prd_ava", "c_dest", "prd_use", "value"]].copy()
            chunk["value"] = pd.to_numeric(chunk["value"], errors="coerce").fillna(0.0)
            # Drop zeros and value-added rows early (saves memory)
            chunk = chunk[(chunk["value"] != 0) & (~chunk["prd_ava"].isin(va_set))]
            chunk = chunk[chunk["prd_use"].isin(keep_prd_use)]
            chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"  Loaded {len(df)} non-zero product rows")
    return df


# ---------------------------------------------------------------------------
# Matrix construction
# ---------------------------------------------------------------------------

def build_matrices(df: pd.DataFrame, eu_countries: list[str]) -> dict:
    """
    Build Z_EU, e_nonEU, f_intraEU, x_EU using vectorized pandas operations.
    """
    log = logging.getLogger("stage2")

    eu_set = set(eu_countries)
    cpa_set = set(CPA_PRODUCT_CODES)
    fd_set = set(FINAL_DEMAND_CODES)
    va_set = set(VALUE_ADDED_CODES)

    N = len(eu_countries)     # 28
    P = len(CPA_PRODUCT_CODES)  # 64
    N_EU = N * P              # 1792

    # Ordered index
    index = [(c, p) for c in eu_countries for p in CPA_PRODUCT_CODES]
    idx_map = {t: i for i, t in enumerate(index)}
    row_labels = [f"{c}_{p}" for c, p in index]

    # --- Classify rows ---
    # Separate product rows from VA rows
    df_prod = df[df["prd_ava"].isin(cpa_set)].copy()
    df_z = df_prod[df_prod["prd_use"].isin(cpa_set)].copy()    # intermediate
    df_fd = df_prod[df_prod["prd_use"].isin(fd_set)].copy()    # final demand

    log.info(f"  Intermediate rows: {len(df_z)}, Final demand rows: {len(df_fd)}")

    # Add numeric indices for matrix construction
    # (c_orig, prd_ava) -> row index
    c_orig_idx = {c: i for i, c in enumerate(eu_countries)}
    prd_idx = {p: i for i, p in enumerate(CPA_PRODUCT_CODES)}

    def row_idx(c, p):
        return c_orig_idx[c] * P + prd_idx[p]

    # -----------------------------------------------------------------------
    # Z_EU: EU→EU intermediate flows
    # -----------------------------------------------------------------------
    log.info("Building Z_EU (EU intra intermediate matrix)...")
    z_eu = df_z[df_z["c_orig"].isin(eu_set) & df_z["c_dest"].isin(eu_set)].copy()
    z_eu = z_eu[z_eu["value"] != 0]

    # Map to integer indices
    z_eu["row_i"] = z_eu["c_orig"].map(c_orig_idx) * P + z_eu["prd_ava"].map(prd_idx)
    z_eu["col_i"] = z_eu["c_dest"].map(c_orig_idx) * P + z_eu["prd_use"].map(prd_idx)
    z_eu = z_eu.dropna(subset=["row_i", "col_i"])
    z_eu["row_i"] = z_eu["row_i"].astype(int)
    z_eu["col_i"] = z_eu["col_i"].astype(int)

    Z_EU = np.zeros((N_EU, N_EU), dtype=np.float64)
    np.add.at(Z_EU, (z_eu["row_i"].values, z_eu["col_i"].values), z_eu["value"].values)
    log.info(f"  Z_EU: shape={Z_EU.shape}, non-zeros={np.count_nonzero(Z_EU)}")

    # -----------------------------------------------------------------------
    # e_nonEU: EU exports to non-EU (intermediate + final demand)
    # -----------------------------------------------------------------------
    log.info("Building e_nonEU (EU exports to non-EU)...")
    e_nonEU = np.zeros(N_EU, dtype=np.float64)

    # Intermediate exports to non-EU
    z_noneu = df_z[df_z["c_orig"].isin(eu_set) & ~df_z["c_dest"].isin(eu_set)].copy()
    z_noneu = z_noneu[z_noneu["value"] != 0]
    z_noneu["row_i"] = z_noneu["c_orig"].map(c_orig_idx) * P + z_noneu["prd_ava"].map(prd_idx)
    z_noneu = z_noneu.dropna(subset=["row_i"])
    z_noneu["row_i"] = z_noneu["row_i"].astype(int)
    np.add.at(e_nonEU, z_noneu["row_i"].values, z_noneu["value"].values)

    # Final demand exports to non-EU
    fd_noneu = df_fd[df_fd["c_orig"].isin(eu_set) & ~df_fd["c_dest"].isin(eu_set)].copy()
    fd_noneu = fd_noneu[fd_noneu["value"] != 0]
    fd_noneu["row_i"] = fd_noneu["c_orig"].map(c_orig_idx) * P + fd_noneu["prd_ava"].map(prd_idx)
    fd_noneu = fd_noneu.dropna(subset=["row_i"])
    fd_noneu["row_i"] = fd_noneu["row_i"].astype(int)
    np.add.at(e_nonEU, fd_noneu["row_i"].values, fd_noneu["value"].values)

    log.info(f"  e_nonEU: total = {e_nonEU.sum():.1f} MIO_EUR")

    # -----------------------------------------------------------------------
    # f_intraEU: intra-EU final demand (for reference / output)
    # Store as (N_EU, N) summed across FD categories
    # -----------------------------------------------------------------------
    log.info("Building f_intraEU...")
    fd_eu = df_fd[df_fd["c_orig"].isin(eu_set) & df_fd["c_dest"].isin(eu_set)].copy()
    fd_eu = fd_eu[fd_eu["value"] != 0]
    fd_eu["row_i"] = fd_eu["c_orig"].map(c_orig_idx) * P + fd_eu["prd_ava"].map(prd_idx)
    fd_eu["dest_i"] = fd_eu["c_dest"].map(c_orig_idx)
    fd_eu = fd_eu.dropna(subset=["row_i", "dest_i"])
    fd_eu["row_i"] = fd_eu["row_i"].astype(int)
    fd_eu["dest_i"] = fd_eu["dest_i"].astype(int)

    f_intraEU = np.zeros((N_EU, N), dtype=np.float64)
    np.add.at(f_intraEU, (fd_eu["row_i"].values, fd_eu["dest_i"].values), fd_eu["value"].values)
    log.info(f"  f_intraEU: total = {f_intraEU.sum():.1f} MIO_EUR")

    # -----------------------------------------------------------------------
    # x_EU: total output = row sums over ALL destinations and uses
    # -----------------------------------------------------------------------
    log.info("Computing x_EU (total output)...")
    x_EU = np.zeros(N_EU, dtype=np.float64)

    # All intermediate uses from EU suppliers
    z_eu_all = df_z[df_z["c_orig"].isin(eu_set)].copy()
    z_eu_all = z_eu_all[z_eu_all["value"] != 0]
    z_eu_all["row_i"] = z_eu_all["c_orig"].map(c_orig_idx) * P + z_eu_all["prd_ava"].map(prd_idx)
    z_eu_all = z_eu_all.dropna(subset=["row_i"])
    z_eu_all["row_i"] = z_eu_all["row_i"].astype(int)
    np.add.at(x_EU, z_eu_all["row_i"].values, z_eu_all["value"].values)

    # All final demand from EU suppliers
    fd_eu_all = df_fd[df_fd["c_orig"].isin(eu_set)].copy()
    fd_eu_all = fd_eu_all[fd_eu_all["value"] != 0]
    fd_eu_all["row_i"] = fd_eu_all["c_orig"].map(c_orig_idx) * P + fd_eu_all["prd_ava"].map(prd_idx)
    fd_eu_all = fd_eu_all.dropna(subset=["row_i"])
    fd_eu_all["row_i"] = fd_eu_all["row_i"].astype(int)
    np.add.at(x_EU, fd_eu_all["row_i"].values, fd_eu_all["value"].values)

    log.info(f"  x_EU: total = {x_EU.sum():.1f} MIO_EUR, zeros = {np.sum(x_EU == 0)}")

    return {
        "Z_EU": Z_EU,
        "e_nonEU": e_nonEU,
        "f_intraEU": f_intraEU,
        "x_EU": x_EU,
        "index": index,
        "row_labels": row_labels,
        "eu_countries": eu_countries,
        "cpa_codes": CPA_PRODUCT_CODES,
    }


# ---------------------------------------------------------------------------
# Employment vector
# ---------------------------------------------------------------------------

def build_employment_vector(emp_path: Path, eu_countries: list[str],
                             index: list[tuple]) -> np.ndarray:
    """
    Build employment vector Em_EU (N_EU,) ordered consistently with index.
    """
    log = logging.getLogger("stage2")
    log.info("Building employment vector Em_EU...")

    cpa_to_nace = dict(zip(CPA_PRODUCT_CODES, NACE_EMP_CODES))

    emp_data = {}
    with open(emp_path) as f:
        for line in f:
            row = json.loads(line)
            geo = row.get("geo", "")
            nace = row.get("nace_r2", "")
            val = row.get("value")
            if val is not None:
                emp_data[(geo, nace)] = float(val)

    em_EU = np.zeros(len(index), dtype=np.float64)
    missing = []
    for i, (country, cpa_code) in enumerate(index):
        nace_code = cpa_to_nace.get(cpa_code, "")
        val = emp_data.get((country, nace_code))
        if val is not None:
            em_EU[i] = val
        else:
            missing.append((country, cpa_code, nace_code))

    if missing:
        log.warning(f"  Missing employment for {len(missing)} country-industry cells")
        # Log by country
        by_country = {}
        for c, cpa, nace in missing:
            by_country.setdefault(c, []).append(nace)
        for c, naces in sorted(by_country.items()):
            log.warning(f"    {c}: missing {naces}")

    log.info(f"  Em_EU total: {em_EU.sum():.0f} thousand persons")
    return em_EU


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------

def save_outputs(matrices: dict, em_EU: np.ndarray, prepared_dir: Path) -> None:
    log = logging.getLogger("stage2")
    prepared_dir.mkdir(parents=True, exist_ok=True)

    row_labels = matrices["row_labels"]
    eu_countries = matrices["eu_countries"]
    Z_EU = matrices["Z_EU"]
    e_nonEU = matrices["e_nonEU"]
    x_EU = matrices["x_EU"]
    f_intraEU = matrices["f_intraEU"]

    log.info("Saving Z_EU.csv...")
    pd.DataFrame(Z_EU, index=row_labels, columns=row_labels).to_csv(
        prepared_dir / "Z_EU.csv"
    )

    log.info("Saving e_nonEU.csv...")
    pd.DataFrame({"label": row_labels, "e_nonEU_MIO_EUR": e_nonEU}).to_csv(
        prepared_dir / "e_nonEU.csv", index=False
    )

    log.info("Saving x_EU.csv...")
    pd.DataFrame({"label": row_labels, "x_EU_MIO_EUR": x_EU}).to_csv(
        prepared_dir / "x_EU.csv", index=False
    )

    log.info("Saving Em_EU.csv...")
    pd.DataFrame({"label": row_labels, "em_EU_THS_PER": em_EU}).to_csv(
        prepared_dir / "Em_EU.csv", index=False
    )

    log.info("Saving f_intraEU_final.csv...")
    pd.DataFrame(f_intraEU, index=row_labels, columns=eu_countries).to_csv(
        prepared_dir / "f_intraEU_final.csv"
    )

    meta = {
        "eu_countries": eu_countries,
        "cpa_codes": CPA_PRODUCT_CODES,
        "nace_codes": NACE_EMP_CODES,
        "n_countries": len(eu_countries),
        "n_industries": len(CPA_PRODUCT_CODES),
        "n_total": len(row_labels),
        "row_col_order": "country-major",
        "units": {
            "Z_EU": "million EUR",
            "e_nonEU": "million EUR",
            "x_EU": "million EUR",
            "Em_EU": "thousand persons",
        },
    }
    with open(prepared_dir / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    log.info(f"All outputs saved to {prepared_dir}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Stage 2: Data Preparation")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    year = cfg["reference_year"]
    eu_countries = cfg["eu_member_states"]

    base_dir = Path(args.config).parent
    raw_dir = base_dir / "data" / "raw"
    prepared_dir = base_dir / "data" / "prepared"
    log_dir = base_dir / "logs"

    log = setup_logging(log_dir)
    log.info(f"=== Stage 2: Data Preparation (year={year}) ===")

    iciot_path = raw_dir / f"figaro_iciot_{year}.jsonl"
    emp_path = raw_dir / f"employment_{year}.jsonl"

    for p in [iciot_path, emp_path]:
        if not p.exists():
            log.error(f"Required file missing: {p}. Run Stage 1 first.")
            sys.exit(1)

    # Load IC-IOT
    df = load_iciot(iciot_path)

    # Build matrices
    matrices = build_matrices(df, eu_countries)

    # Build employment vector
    em_EU = build_employment_vector(emp_path, eu_countries, matrices["index"])

    # Save
    save_outputs(matrices, em_EU, prepared_dir)

    log.info("=== Stage 2 complete ===")


if __name__ == "__main__":
    main()
