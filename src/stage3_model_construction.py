"""
Stage 3: Model Construction
Build the Leontief model and compute employment content of exports.
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


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"stage3_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("stage3")


# ---------------------------------------------------------------------------
# Load prepared data
# ---------------------------------------------------------------------------

def load_prepared(prepared_dir: Path) -> dict:
    log = logging.getLogger("stage3")
    log.info("Loading prepared data...")

    with open(prepared_dir / "metadata.json") as f:
        meta = json.load(f)

    eu_countries = meta["eu_countries"]
    cpa_codes = meta["cpa_codes"]
    N_EU = meta["n_total"]   # 1792

    log.info(f"  EU countries: {len(eu_countries)}, CPA codes: {len(cpa_codes)}, N_EU: {N_EU}")

    # Z_EU
    log.info("  Loading Z_EU.csv...")
    Z_EU = pd.read_csv(prepared_dir / "Z_EU.csv", index_col=0).values.astype(np.float64)

    # e_nonEU
    e_nonEU = pd.read_csv(prepared_dir / "e_nonEU.csv")["e_nonEU_MIO_EUR"].values.astype(np.float64)

    # x_EU
    x_EU = pd.read_csv(prepared_dir / "x_EU.csv")["x_EU_MIO_EUR"].values.astype(np.float64)

    # Em_EU
    em_EU = pd.read_csv(prepared_dir / "Em_EU.csv")["em_EU_THS_PER"].values.astype(np.float64)

    log.info(f"  Z_EU: {Z_EU.shape}, sum={Z_EU.sum():.1f}")
    log.info(f"  e_nonEU: shape={e_nonEU.shape}, sum={e_nonEU.sum():.1f}")
    log.info(f"  x_EU: shape={x_EU.shape}, sum={x_EU.sum():.1f}, zeros={np.sum(x_EU == 0)}")
    log.info(f"  em_EU: shape={em_EU.shape}, sum={em_EU.sum():.0f}")

    return {
        "Z_EU": Z_EU,
        "e_nonEU": e_nonEU,
        "x_EU": x_EU,
        "em_EU": em_EU,
        "eu_countries": eu_countries,
        "cpa_codes": cpa_codes,
        "N_EU": N_EU,
    }


# ---------------------------------------------------------------------------
# Model construction
# ---------------------------------------------------------------------------

def build_technical_coefficients(Z_EU: np.ndarray, x_EU: np.ndarray) -> np.ndarray:
    """
    A = Z_EU * diag(x_EU)^{-1}
    Handle zero outputs by setting coefficient to 0.
    """
    log = logging.getLogger("stage3")
    log.info("Building technical coefficients matrix A...")

    x_inv = np.where(x_EU > 0, 1.0 / x_EU, 0.0)
    A = Z_EU * x_inv[np.newaxis, :]  # broadcast: A[i,j] = Z[i,j] / x[j]

    # Validation
    col_sums = A.sum(axis=0)
    max_col_sum = col_sums.max()
    n_exceeding = np.sum(col_sums >= 1.0)
    log.info(f"  A: max column sum={max_col_sum:.6f}, columns >= 1: {n_exceeding}")
    if n_exceeding > 0:
        log.warning(f"  WARNING: {n_exceeding} columns with sum >= 1 (model may not converge)")

    return A


def build_leontief_inverse(A: np.ndarray) -> np.ndarray:
    """
    L = (I - A)^{-1}
    Uses numpy's direct solver for stability.
    """
    log = logging.getLogger("stage3")
    log.info(f"Building Leontief inverse L = (I - A)^{{-1}} ({A.shape[0]}×{A.shape[0]})...")
    t0 = time.time()

    N = A.shape[0]
    I_minus_A = np.eye(N) - A

    # Solve L * (I - A) = I  =>  L = inv(I - A)
    # Use np.linalg.solve for numerical stability: (I-A) @ L = I
    # Actually, we want L = (I-A)^{-1}, which means (I-A) @ L = I
    # So each column of L solves (I-A) @ l = e_j
    log.info("  Inverting (I - A)...")
    try:
        L = np.linalg.inv(I_minus_A)
    except np.linalg.LinAlgError as e:
        log.error(f"  Matrix inversion failed: {e}")
        raise

    elapsed = time.time() - t0
    log.info(f"  Leontief inverse computed in {elapsed:.1f}s")

    # Validation
    n_negative = np.sum(L < -1e-10)
    n_diag_lt1 = np.sum(np.diag(L) < 1.0 - 1e-10)
    log.info(f"  L: min={L.min():.6f}, max={L.max():.6f}")
    log.info(f"  L: negative elements (< -1e-10): {n_negative}")
    log.info(f"  L: diagonal elements < 1: {n_diag_lt1}")

    # Identity check
    residual = np.max(np.abs(L @ I_minus_A - np.eye(N)))
    log.info(f"  Identity check max(|L*(I-A) - I|) = {residual:.2e}")
    if residual > 1e-4:
        log.warning(f"  WARNING: Identity check residual {residual:.2e} > 1e-4")

    return L


def build_employment_coefficients(x_EU: np.ndarray, em_EU: np.ndarray) -> np.ndarray:
    """
    d = diag(x_EU)^{-1} * em_EU
    Employment per unit of output (thousand persons per million EUR).
    """
    log = logging.getLogger("stage3")
    log.info("Building employment coefficients d...")

    d = np.where(x_EU > 0, em_EU / x_EU, 0.0)

    log.info(f"  d: min={d.min():.6f}, max={d.max():.6f}, mean={d.mean():.6f}")
    log.info(f"  d: zeros={np.sum(d == 0)}, negatives={np.sum(d < 0)}")

    return d


def compute_employment_content(d: np.ndarray, L: np.ndarray,
                                e_nonEU: np.ndarray,
                                eu_countries: list[str],
                                cpa_codes: list[str]) -> dict:
    """
    Compute employment content of exports.

    em_exports = diag(d) * L * e_nonEU
      -> total employment in each (country, industry) cell supported by EU exports

    Also compute the 28×28 country matrix:
      em_exports[r,s] = d^r' * L^{rs} * e^s
      -> employment in country r supported by exports of country s
    """
    log = logging.getLogger("stage3")
    N = len(eu_countries)
    P = len(cpa_codes)

    log.info("Computing employment content of exports...")

    # Full employment content vector (1792,)
    em_exports_total = d * (L @ e_nonEU)
    log.info(f"  Total EU export-supported employment: {em_exports_total.sum():.0f} thousand persons")
    log.info(f"  Paper reference: ~25,597 thousand persons")

    # 28×28 country matrix: em_country[r,s] = employment in r supported by s's exports
    log.info("Computing 28×28 country employment matrix...")
    em_country_matrix = np.zeros((N, N), dtype=np.float64)

    for s_idx, s in enumerate(eu_countries):
        # e^s: export vector for country s only
        e_s = np.zeros(N * P, dtype=np.float64)
        s_start = s_idx * P
        s_end = (s_idx + 1) * P
        e_s[s_start:s_end] = e_nonEU[s_start:s_end]

        if e_s.sum() == 0:
            continue

        # L @ e^s: propagation through the full Leontief system
        Le_s = L @ e_s

        # Employment in each country r: d^r * L^{rs} * e^s
        for r_idx in range(N):
            r_start = r_idx * P
            r_end = (r_idx + 1) * P
            em_country_matrix[r_idx, s_idx] = np.dot(d[r_start:r_end], Le_s[r_start:r_end])

    log.info(f"  Country matrix: total={em_country_matrix.sum():.0f}, "
             f"shape={em_country_matrix.shape}")

    return {
        "em_exports_total": em_exports_total,
        "em_country_matrix": em_country_matrix,
    }


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------

def save_outputs(A: np.ndarray, L: np.ndarray, d: np.ndarray,
                 results: dict, model_dir: Path,
                 eu_countries: list[str], cpa_codes: list[str]) -> None:
    log = logging.getLogger("stage3")
    model_dir.mkdir(parents=True, exist_ok=True)

    N = len(eu_countries)
    P = len(cpa_codes)
    row_labels = [f"{c}_{p}" for c in eu_countries for p in cpa_codes]

    log.info("Saving A_EU.csv...")
    pd.DataFrame(A, index=row_labels, columns=row_labels).to_csv(model_dir / "A_EU.csv")

    log.info("Saving L_EU.csv...")
    pd.DataFrame(L, index=row_labels, columns=row_labels).to_csv(model_dir / "L_EU.csv")

    log.info("Saving d_EU.csv...")
    pd.DataFrame({"label": row_labels, "d_THS_PER_per_MIO_EUR": d}).to_csv(
        model_dir / "d_EU.csv", index=False
    )

    log.info("Saving em_exports_total.csv...")
    em_total = results["em_exports_total"]
    pd.DataFrame({"label": row_labels, "em_exports_THS_PER": em_total}).to_csv(
        model_dir / "em_exports_total.csv", index=False
    )

    log.info("Saving em_exports_country_matrix.csv...")
    em_mat = results["em_country_matrix"]
    pd.DataFrame(em_mat, index=eu_countries, columns=eu_countries).to_csv(
        model_dir / "em_exports_country_matrix.csv"
    )

    log.info(f"All model outputs saved to {model_dir}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Stage 3: Model Construction")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    base_dir = Path(args.config).parent
    prepared_dir = base_dir / "data" / "prepared"
    model_dir = base_dir / "data" / "model"
    log_dir = base_dir / "logs"

    log = setup_logging(log_dir)
    log.info("=== Stage 3: Model Construction ===")

    if not prepared_dir.exists():
        log.error("Prepared data directory not found. Run Stage 2 first.")
        sys.exit(1)

    # Load data
    data = load_prepared(prepared_dir)
    Z_EU = data["Z_EU"]
    e_nonEU = data["e_nonEU"]
    x_EU = data["x_EU"]
    em_EU = data["em_EU"]
    eu_countries = data["eu_countries"]
    cpa_codes = data["cpa_codes"]

    # Build model
    A = build_technical_coefficients(Z_EU, x_EU)
    L = build_leontief_inverse(A)
    d = build_employment_coefficients(x_EU, em_EU)

    # Compute employment content
    results = compute_employment_content(d, L, e_nonEU, eu_countries, cpa_codes)

    # Save
    save_outputs(A, L, d, results, model_dir, eu_countries, cpa_codes)

    log.info("=== Stage 3 complete ===")


if __name__ == "__main__":
    main()
