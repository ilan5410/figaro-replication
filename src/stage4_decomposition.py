"""
Stage 4: Decomposition
Decompose employment content into domestic/spillover, direct/indirect,
and industry-level components.
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
    log_file = log_dir / f"stage4_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("stage4")


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_data(prepared_dir: Path, model_dir: Path) -> dict:
    log = logging.getLogger("stage4")
    log.info("Loading prepared and model data...")

    with open(prepared_dir / "metadata.json") as f:
        meta = json.load(f)
    eu_countries = meta["eu_countries"]
    cpa_codes = meta["cpa_codes"]
    N = len(eu_countries)
    P = len(cpa_codes)
    N_EU = N * P

    e_nonEU = pd.read_csv(prepared_dir / "e_nonEU.csv")["e_nonEU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(prepared_dir / "Em_EU.csv")["em_EU_THS_PER"].values.astype(np.float64)

    log.info("  Loading L_EU.csv...")
    L = pd.read_csv(model_dir / "L_EU.csv", index_col=0).values.astype(np.float64)
    d = pd.read_csv(model_dir / "d_EU.csv")["d_THS_PER_per_MIO_EUR"].values.astype(np.float64)

    # Load country employment matrix (already computed)
    em_mat = pd.read_csv(model_dir / "em_exports_country_matrix.csv", index_col=0).values.astype(np.float64)

    log.info(f"  Loaded: L={L.shape}, d={d.shape}, e={e_nonEU.shape}, em_mat={em_mat.shape}")

    return {
        "L": L,
        "d": d,
        "e_nonEU": e_nonEU,
        "em_EU": em_EU,
        "em_country_matrix": em_mat,
        "eu_countries": eu_countries,
        "cpa_codes": cpa_codes,
        "N": N,
        "P": P,
        "N_EU": N_EU,
    }


# ---------------------------------------------------------------------------
# Domestic / Spillover decomposition
# ---------------------------------------------------------------------------

def compute_domestic_spillover(data: dict) -> pd.DataFrame:
    """
    For each EU country r:
      - domestic_effect:     d^r' * L^{rr} * e^r
      - spillover_received:  sum_{s!=r} d^r' * L^{rs} * e^s
      - spillover_generated: sum_{s!=r} d^s' * L^{sr} * e^r
    """
    log = logging.getLogger("stage4")
    log.info("Computing domestic/spillover decomposition...")

    L = data["L"]
    d = data["d"]
    e = data["e_nonEU"]
    em = data["em_EU"]
    em_mat = data["em_country_matrix"]   # [r,s] = employment in r from exports of s
    eu_countries = data["eu_countries"]
    N = data["N"]
    P = data["P"]

    rows = []
    for r_idx, r in enumerate(eu_countries):
        r_start = r_idx * P
        r_end = (r_idx + 1) * P

        d_r = d[r_start:r_end]           # employment coeff for country r
        e_r = e[r_start:r_end]           # exports of country r to non-EU

        # Total employment in country r (from employment data, sum across industries)
        total_emp_r = em[r_start:r_end].sum()

        # Domestic effect: employment in r due to r's own exports
        # = em_mat[r, r] (already computed in stage 3)
        domestic = em_mat[r_idx, r_idx]

        # Spillover received: employment in r due to OTHER countries' exports
        # = sum_{s != r} em_mat[r, s]
        spillover_received = em_mat[r_idx, :].sum() - domestic

        # Spillover generated: employment in OTHER countries due to r's exports
        # = sum_{s != r} em_mat[s, r]
        spillover_generated = em_mat[:, r_idx].sum() - domestic

        # Total employment supported in r by all EU exports
        total_in_r = em_mat[r_idx, :].sum()   # domestic + spillover_received

        # Total employment supported by r's exports (domestic + generated in others)
        total_by_r = em_mat[:, r_idx].sum()   # domestic + spillover_generated

        # Direct effect: d^r * e^r (without multiplier)
        direct = float(np.dot(d_r, e_r))

        # Indirect effect: domestic - direct
        indirect = domestic - direct

        # Shares
        share_in_total = total_in_r / total_emp_r * 100 if total_emp_r > 0 else 0
        dom_share_by_r = domestic / total_by_r * 100 if total_by_r > 0 else 0
        spill_share_by_r = spillover_generated / total_by_r * 100 if total_by_r > 0 else 0

        rows.append({
            "country": r,
            "total_employment_THS": total_emp_r,
            "domestic_effect_THS": domestic,
            "spillover_received_THS": spillover_received,
            "spillover_generated_THS": spillover_generated,
            "direct_effect_THS": direct,
            "indirect_effect_THS": indirect,
            "total_in_country_THS": total_in_r,
            "total_by_country_THS": total_by_r,
            "export_emp_share_pct": share_in_total,
            "domestic_share_pct": dom_share_by_r,
            "spillover_share_pct": spill_share_by_r,
        })

        log.info(
            f"  {r}: domestic={domestic:.0f}, spill_recv={spillover_received:.0f}, "
            f"spill_gen={spillover_generated:.0f}, share={share_in_total:.1f}%, "
            f"spill%={spill_share_by_r:.1f}%"
        )

    df = pd.DataFrame(rows)
    log.info(f"\n  Total export-supported employment (domestic+spillover sum): "
             f"{df['domestic_effect_THS'].sum() + df['spillover_received_THS'].sum():.0f} THS")
    return df


# ---------------------------------------------------------------------------
# Industry-level decomposition (Table 4: 10-sector)
# ---------------------------------------------------------------------------

def compute_industry_decomposition(data: dict, agg10: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aggregate the (1792,) employment content vector to 10 sectors.
    Returns:
      - table4: 10×10 matrix (rows=employment sector, cols=export product sector)
      - figure3: by-product employment shares with domestic/spillover split
    """
    log = logging.getLogger("stage4")
    log.info("Computing industry-level decomposition (10 sectors)...")

    L = data["L"]
    d = data["d"]
    e = data["e_nonEU"]
    em_mat = data["em_country_matrix"]
    eu_countries = data["eu_countries"]
    N = data["N"]
    P = data["P"]
    N_EU = N * P

    # Build 64→10 sector mapping (0-based product index → sector name)
    prod_to_sector = {}
    for sector, indices in agg10.items():
        for idx in indices:
            prod_to_sector[idx - 1] = sector   # convert to 0-based

    sector_names = list(agg10.keys())

    # --- Table 4 ---
    # Cell (i, j) = employment in sector i (rows) supported by exports of sector j (cols)
    # We need to compute this for the full 10×10 aggregation

    # For each combination of (product group j, country s), compute L @ e_sj
    # where e_sj is the export vector restricted to product group j of country s
    # Then sum employment in sector i across all countries and industries

    table4 = np.zeros((10, 10), dtype=np.float64)
    sector_idx = {s: i for i, s in enumerate(sector_names)}

    # Build index: which entries correspond to which (country, product_idx)?
    # Row flat_idx = country_idx * P + prod_idx

    for j_sec_idx, j_sec in enumerate(sector_names):
        j_prods = [idx - 1 for idx in agg10[j_sec]]  # 0-based product indices

        # Build export vector restricted to this product sector (all countries)
        e_j = np.zeros(N_EU, dtype=np.float64)
        for c_idx in range(N):
            for p_idx in j_prods:
                flat = c_idx * P + p_idx
                e_j[flat] = e[flat]

        if e_j.sum() == 0:
            continue

        # Employment vector from this sector's exports
        em_j = d * (L @ e_j)    # (N_EU,) - employment at each country-industry

        # Aggregate to 10 sectors (rows)
        for i_sec_idx, i_sec in enumerate(sector_names):
            i_prods = [idx - 1 for idx in agg10[i_sec]]  # 0-based product indices
            total = 0.0
            for c_idx in range(N):
                for p_idx in i_prods:
                    flat = c_idx * P + p_idx
                    total += em_j[flat]
            table4[i_sec_idx, j_sec_idx] = total

    log.info(f"  Table 4 built: shape={table4.shape}, total={table4.sum():.0f}")

    table4_df = pd.DataFrame(table4, index=sector_names, columns=sector_names)

    # --- Figure 3 data ---
    # For each product sector j: total employment + domestic/spillover split
    fig3_rows = []
    for j_sec_idx, j_sec in enumerate(sector_names):
        col_total = table4[:, j_sec_idx].sum()

        # Domestic = employment in same country as the exporter
        # We need to compute this per country
        j_prods = [idx - 1 for idx in agg10[j_sec]]
        domestic_j = 0.0
        for c_idx, c in enumerate(eu_countries):
            for p_idx in j_prods:
                flat = c_idx * P + p_idx
                if e[flat] > 0:
                    # Domestic = d^c' * L^{cc} * e^c_p
                    c_start = c_idx * P
                    c_end = (c_idx + 1) * P
                    e_cp = np.zeros(N_EU)
                    e_cp[flat] = e[flat]
                    Le_cp = L @ e_cp
                    domestic_j += np.dot(d[c_start:c_end], Le_cp[c_start:c_end])

        spillover_j = col_total - domestic_j
        fig3_rows.append({
            "sector": j_sec,
            "total_employment_THS": col_total,
            "domestic_THS": domestic_j,
            "spillover_THS": spillover_j,
        })

    fig3_df = pd.DataFrame(fig3_rows)
    log.info(f"  Figure 3 data built: {len(fig3_df)} sectors")

    return table4_df, fig3_df


# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------

def save_outputs(decomp_df: pd.DataFrame, em_mat: np.ndarray,
                 table4_df: pd.DataFrame, fig3_df: pd.DataFrame,
                 decomp_dir: Path, eu_countries: list[str]) -> None:
    log = logging.getLogger("stage4")
    decomp_dir.mkdir(parents=True, exist_ok=True)

    log.info("Saving country_decomposition.csv...")
    decomp_df.to_csv(decomp_dir / "country_decomposition.csv", index=False)

    log.info("Saving annex_c_matrix.csv...")
    pd.DataFrame(em_mat, index=eu_countries, columns=eu_countries).to_csv(
        decomp_dir / "annex_c_matrix.csv"
    )

    log.info("Saving industry_table4.csv...")
    table4_df.to_csv(decomp_dir / "industry_table4.csv")

    log.info("Saving industry_figure3.csv...")
    fig3_df.to_csv(decomp_dir / "industry_figure3.csv", index=False)

    log.info(f"All decomposition outputs saved to {decomp_dir}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Stage 4: Decomposition")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    base_dir = Path(args.config).parent
    prepared_dir = base_dir / "data" / "prepared"
    model_dir = base_dir / "data" / "model"
    decomp_dir = base_dir / "data" / "decomposition"
    log_dir = base_dir / "logs"

    log = setup_logging(log_dir)
    log.info("=== Stage 4: Decomposition ===")

    data = load_data(prepared_dir, model_dir)

    # Country decomposition
    decomp_df = compute_domestic_spillover(data)

    # Industry decomposition
    agg10 = cfg["industry_aggregation_10"]
    table4_df, fig3_df = compute_industry_decomposition(data, agg10)

    # Save
    save_outputs(
        decomp_df,
        data["em_country_matrix"],
        table4_df,
        fig3_df,
        decomp_dir,
        data["eu_countries"],
    )

    log.info("=== Stage 4 complete ===")


if __name__ == "__main__":
    main()
