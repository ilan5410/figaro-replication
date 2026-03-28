"""
Deterministic validators — gate checks between pipeline stages.

These run WITHOUT any LLM call. They enforce hard mathematical invariants
and dimensional constraints that agent self-evaluation cannot reliably verify.

See FIGARO_AGENT_BEST_PRACTICES.md §7.2 and §8.1.
"""
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("figaro.validators")

REPO_ROOT = Path(__file__).parent.parent.parent


def validate_stage1(state: dict) -> tuple[bool, list[str]]:
    """
    Validate Stage 1 (Data Acquisition) outputs.

    Returns:
        (valid: bool, errors: list[str])
    """
    errors = []
    cfg = state.get("config", {})
    year = cfg.get("reference_year", 2010)
    eu_countries = set(cfg.get("eu_member_states", []))

    raw_dir = REPO_ROOT / "data" / "raw"

    iciot_path = raw_dir / f"figaro_iciot_{year}.csv"
    emp_path = raw_dir / f"employment_{year}.csv"
    summary_path = raw_dir / f"data_summary_{year}.txt"

    # File existence
    for p in [iciot_path, emp_path]:
        if not p.exists():
            errors.append(f"Missing required file: {p.relative_to(REPO_ROOT)}")

    if errors:
        return False, errors

    # IC-IOT: minimum row count and country presence
    try:
        iciot = pd.read_csv(iciot_path, nrows=5)
        total_rows = sum(1 for _ in open(iciot_path)) - 1  # subtract header
        if total_rows < 100_000:
            errors.append(f"IC-IOT has only {total_rows} rows (expected > 100,000)")

        # Check all EU countries present as c_orig
        iciot_full = pd.read_csv(iciot_path, usecols=["c_orig"])
        present = set(iciot_full["c_orig"].unique())
        missing = eu_countries - present
        if missing:
            errors.append(f"IC-IOT missing EU countries as c_orig: {sorted(missing)}")
        log.info(f"IC-IOT: {total_rows} rows, {len(present)} unique c_orig countries")
    except Exception as e:
        errors.append(f"IC-IOT validation error: {e}")

    # Employment: row count and EU coverage
    try:
        emp = pd.read_csv(emp_path)
        # Filter to leaf-level employment
        emp_filtered = emp[
            (emp.get("na_item", emp.get("NA_ITEM", pd.Series(dtype=str))) == "EMP_DC") &
            (emp.get("unit", emp.get("UNIT", pd.Series(dtype=str))) == "THS_PER")
        ] if "na_item" in emp.columns or "NA_ITEM" in emp.columns else emp

        geo_col = "geo" if "geo" in emp.columns else "GEO"
        present_geo = set(emp[geo_col].unique()) if geo_col in emp.columns else set()
        missing_geo = eu_countries - present_geo
        if missing_geo:
            errors.append(f"Employment missing EU countries: {sorted(missing_geo)}")

        log.info(f"Employment: {len(emp)} rows, {len(present_geo)} geo codes")
    except Exception as e:
        errors.append(f"Employment validation error: {e}")

    if not summary_path.exists():
        log.warning("data_summary file missing — not a hard failure")

    valid = len(errors) == 0
    return valid, errors


def validate_stage2(state: dict) -> tuple[bool, list[str]]:
    """
    Validate Stage 2 (Data Preparation) outputs.

    Enforces the mathematical invariants that MUST hold for the Leontief
    model to be meaningful. These are non-negotiable.
    """
    errors = []
    cfg = state.get("config", {})
    year = cfg.get("reference_year", 2010)
    n_countries = len(cfg.get("eu_member_states", []))
    n_products = cfg.get("n_industries", 64)
    n_total = n_countries * n_products  # should be 1792

    prepared_dir = REPO_ROOT / "data" / "prepared"

    # File existence
    required_files = ["Z_EU.csv", "e_nonEU.csv", "x_EU.csv", "Em_EU.csv", "metadata.json"]
    for fname in required_files:
        if not (prepared_dir / fname).exists():
            errors.append(f"Missing: data/prepared/{fname}")

    if errors:
        return False, errors

    # Load and check dimensions
    try:
        with open(prepared_dir / "metadata.json") as f:
            meta = json.load(f)
        actual_n = meta.get("n_total", 0)
        if actual_n != n_total:
            errors.append(f"metadata.n_total={actual_n}, expected {n_total}")
        log.info(f"Metadata: {meta.get('n_countries')} countries × {meta.get('n_products')} products = {actual_n}")
    except Exception as e:
        errors.append(f"metadata.json load error: {e}")
        return False, errors

    # Z_EU dimensions
    try:
        Z = pd.read_csv(prepared_dir / "Z_EU.csv", index_col=0)
        if Z.shape != (n_total, n_total):
            errors.append(f"Z_EU shape {Z.shape}, expected ({n_total}, {n_total})")
        Z_vals = Z.values.astype(float)
        n_neg = np.sum(Z_vals < -1e-10)
        if n_neg > 0:
            errors.append(f"Z_EU has {n_neg} negative values")
        log.info(f"Z_EU: {Z.shape}, negatives: {n_neg}, sum: {Z_vals.sum():.1f}")
    except Exception as e:
        errors.append(f"Z_EU validation error: {e}")

    # e_nonEU
    try:
        e = pd.read_csv(prepared_dir / "e_nonEU.csv")
        if len(e) != n_total:
            errors.append(f"e_nonEU length {len(e)}, expected {n_total}")
        e_vals = e.iloc[:, -1].values.astype(float)
        if np.any(e_vals < -1e-10):
            errors.append(f"e_nonEU has {np.sum(e_vals < 0)} negative values")
        log.info(f"e_nonEU: {len(e)} rows, sum: {e_vals.sum():.1f}")
    except Exception as e_err:
        errors.append(f"e_nonEU validation error: {e_err}")

    # Em_EU: employment total ~225,677 (±5%)
    try:
        em = pd.read_csv(prepared_dir / "Em_EU.csv")
        if len(em) != n_total:
            errors.append(f"Em_EU length {len(em)}, expected {n_total}")
        em_vals = em.iloc[:, -1].values.astype(float)
        total_emp = em_vals.sum()
        pct_dev = abs(total_emp - 225677) / 225677 * 100
        log.info(f"Em_EU: total={total_emp:.0f}, deviation from 225677: {pct_dev:.1f}%")
        if pct_dev > 10:
            errors.append(
                f"Employment total {total_emp:.0f} deviates {pct_dev:.1f}% from expected 225,677"
            )
        elif pct_dev > 5:
            log.warning(f"Employment total deviates {pct_dev:.1f}% from 225,677 (>5% warning)")
        if np.any(em_vals < -1e-10):
            errors.append(f"Em_EU has {np.sum(em_vals < 0)} negative values")
    except Exception as e_err:
        errors.append(f"Em_EU validation error: {e_err}")

    valid = len(errors) == 0
    return valid, errors


def validate_stage3(state: dict) -> tuple[bool, list[str]]:
    """
    Validate Stage 3+4 (Model Construction + Decomposition) outputs.

    These stages are deterministic — validation is a sanity check on the
    math, not on agent behavior.
    """
    errors = []
    model_dir = REPO_ROOT / "data" / "model"
    decomp_dir = REPO_ROOT / "data" / "decomposition"

    cfg = state.get("config", {})
    n_total = len(cfg.get("eu_member_states", [])) * cfg.get("n_industries", 64)

    # Model files
    for fname in ["A_EU.csv", "L_EU.csv", "d_EU.csv",
                  "em_exports_total.csv", "em_exports_country_matrix.csv"]:
        if not (model_dir / fname).exists():
            errors.append(f"Missing: data/model/{fname}")

    # Decomposition files
    for fname in ["country_decomposition.csv", "annex_c_matrix.csv",
                  "industry_table4.csv"]:
        if not (decomp_dir / fname).exists():
            errors.append(f"Missing: data/decomposition/{fname}")

    if errors:
        return False, errors

    # A column sums < 1
    try:
        A = pd.read_csv(model_dir / "A_EU.csv", index_col=0).values.astype(float)
        col_sums = A.sum(axis=0)
        n_ge1 = np.sum(col_sums >= 1.0)
        if n_ge1 > 0:
            errors.append(f"A matrix: {n_ge1} column sums >= 1.0 (Leontief instability)")
        log.info(f"A: max col sum={col_sums.max():.6f}, cols >= 1: {n_ge1}")
    except Exception as e:
        errors.append(f"A_EU validation error: {e}")

    # L diagonal >= 1
    try:
        L = pd.read_csv(model_dir / "L_EU.csv", index_col=0).values.astype(float)
        diag = np.diag(L)
        n_lt1 = np.sum(diag < 1.0 - 1e-10)
        n_neg = np.sum(L < -1e-10)
        if n_neg > 10:
            errors.append(f"L matrix: {n_neg} negative elements")
        if n_lt1 > 10:
            errors.append(f"L matrix: {n_lt1} diagonal elements < 1")
        log.info(f"L: min diagonal={diag.min():.4f}, neg elements: {n_neg}")
    except Exception as e:
        errors.append(f"L_EU validation error: {e}")

    # Total export employment reasonable
    try:
        em_exp = pd.read_csv(model_dir / "em_exports_total.csv")
        total = em_exp.iloc[:, -1].values.astype(float).sum()
        pct = abs(total - 25597) / 25597 * 100
        log.info(f"Total export employment: {total:.0f} (paper: 25,597, dev: {pct:.1f}%)")
        if pct > 30:
            errors.append(f"Export employment {total:.0f} deviates {pct:.1f}% from paper's 25,597")
    except Exception as e:
        errors.append(f"em_exports validation error: {e}")

    valid = len(errors) == 0
    return valid, errors
