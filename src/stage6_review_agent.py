"""
Stage 6: Review Agent
Independently verifies all results for correctness.
Produces outputs/review_report.md with PASS/FAIL status for each check.
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
    log_file = log_dir / f"stage6_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("stage6")


# ---------------------------------------------------------------------------
# Check framework
# ---------------------------------------------------------------------------

class Check:
    def __init__(self, name: str):
        self.name = name
        self.status = "PASS"
        self.details = []
        self.actual = None
        self.expected = None

    def fail(self, msg: str):
        self.status = "FAIL"
        self.details.append(f"FAIL: {msg}")

    def warn(self, msg: str):
        if self.status == "PASS":
            self.status = "WARN"
        self.details.append(f"WARN: {msg}")

    def info(self, msg: str):
        self.details.append(f"  {msg}")

    def __str__(self):
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}[self.status]
        lines = [f"{icon} **{self.name}** — {self.status}"]
        for d in self.details:
            lines.append(f"   {d}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Load all data
# ---------------------------------------------------------------------------

def load_all(prepared_dir, model_dir, decomp_dir):
    log = logging.getLogger("stage6")
    log.info("Loading all intermediate and final outputs...")

    with open(prepared_dir / "metadata.json") as f:
        meta = json.load(f)

    eu_countries = meta["eu_countries"]
    cpa_codes = meta["cpa_codes"]
    N = len(eu_countries)
    P = len(cpa_codes)

    Z_EU = pd.read_csv(prepared_dir / "Z_EU.csv", index_col=0).values.astype(np.float64)
    e_nonEU = pd.read_csv(prepared_dir / "e_nonEU.csv")["e_nonEU_MIO_EUR"].values.astype(np.float64)
    x_EU = pd.read_csv(prepared_dir / "x_EU.csv")["x_EU_MIO_EUR"].values.astype(np.float64)
    em_EU = pd.read_csv(prepared_dir / "Em_EU.csv")["em_EU_THS_PER"].values.astype(np.float64)

    A = pd.read_csv(model_dir / "A_EU.csv", index_col=0).values.astype(np.float64)
    L = pd.read_csv(model_dir / "L_EU.csv", index_col=0).values.astype(np.float64)
    d = pd.read_csv(model_dir / "d_EU.csv")["d_THS_PER_per_MIO_EUR"].values.astype(np.float64)
    em_exports = pd.read_csv(model_dir / "em_exports_total.csv")["em_exports_THS_PER"].values.astype(np.float64)
    em_mat = pd.read_csv(model_dir / "em_exports_country_matrix.csv", index_col=0).values.astype(np.float64)

    decomp = pd.read_csv(decomp_dir / "country_decomposition.csv")
    table4 = pd.read_csv(decomp_dir / "industry_table4.csv", index_col=0).values.astype(np.float64)
    annex_c = pd.read_csv(decomp_dir / "annex_c_matrix.csv", index_col=0).values.astype(np.float64)

    log.info(f"  Z_EU: {Z_EU.shape}, A: {A.shape}, L: {L.shape}")
    log.info(f"  e: {e_nonEU.shape}, x: {x_EU.shape}, em: {em_EU.shape}")
    log.info(f"  em_mat: {em_mat.shape}, decomp: {decomp.shape}")

    return {
        "Z_EU": Z_EU, "e_nonEU": e_nonEU, "x_EU": x_EU, "em_EU": em_EU,
        "A": A, "L": L, "d": d, "em_exports": em_exports, "em_mat": em_mat,
        "decomp": decomp, "table4": table4, "annex_c": annex_c,
        "eu_countries": eu_countries, "cpa_codes": cpa_codes, "N": N, "P": P,
    }


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def check_data_integrity(data: dict) -> list[Check]:
    checks = []

    # 7.1.1 Employment total
    c = Check("7.1.1 EU-28 total employment ~225,677 thousand")
    actual = data["em_EU"].sum()
    expected = 225677
    pct = abs(actual - expected) / expected * 100
    c.info(f"Actual: {actual:.0f}, Expected: ~{expected}, Deviation: {pct:.1f}%")
    if pct > 5:
        c.warn(f"Deviation {pct:.1f}% > 5%")
    checks.append(c)

    # 7.1.2 No negative Z, x, em
    c = Check("7.1.2 No negative values in Z, x, em")
    for name, arr in [("Z_EU", data["Z_EU"]), ("x_EU", data["x_EU"]), ("em_EU", data["em_EU"])]:
        n_neg = np.sum(arr < -1e-10)
        if n_neg > 0:
            c.fail(f"{name}: {n_neg} negative values")
        else:
            c.info(f"{name}: no negatives ✓")
    checks.append(c)

    # 7.1.3 Dimensions
    c = Check("7.1.3 Dimensions: Z (1792×1792), e (1792,), d (1792,)")
    N_EU = data["N"] * data["P"]
    for name, shape, expected_shape in [
        ("Z_EU", data["Z_EU"].shape, (N_EU, N_EU)),
        ("A", data["A"].shape, (N_EU, N_EU)),
        ("L", data["L"].shape, (N_EU, N_EU)),
        ("e_nonEU", data["e_nonEU"].shape, (N_EU,)),
        ("d", data["d"].shape, (N_EU,)),
        ("em_EU", data["em_EU"].shape, (N_EU,)),
    ]:
        if shape != expected_shape:
            c.fail(f"{name}: got {shape}, expected {expected_shape}")
        else:
            c.info(f"{name}: {shape} ✓")
    checks.append(c)

    # 7.1.4 Country matrix dimensions
    c = Check("7.1.4 Country matrix dimensions (28×28)")
    shape = data["em_mat"].shape
    if shape != (data["N"], data["N"]):
        c.fail(f"em_mat shape {shape}, expected ({data['N']}, {data['N']})")
    else:
        c.info(f"em_mat: {shape} ✓")
    checks.append(c)

    return checks


def check_leontief_model(data: dict) -> list[Check]:
    checks = []

    A = data["A"]
    L = data["L"]
    N_EU = data["N"] * data["P"]

    # 7.2.1 A column sums < 1
    c = Check("7.2.1 A column sums in [0, 1)")
    col_sums = A.sum(axis=0)
    n_ge1 = np.sum(col_sums >= 1.0)
    n_neg = np.sum(col_sums < -1e-10)
    max_sum = col_sums.max()
    c.info(f"Max column sum: {max_sum:.6f}, columns >= 1: {n_ge1}, negative cols: {n_neg}")
    if n_ge1 > 0:
        c.fail(f"{n_ge1} columns have sum >= 1")
    if n_neg > 0:
        c.fail(f"{n_neg} columns have negative sum")
    checks.append(c)

    # 7.2.2 L non-negative and diagonal >= 1
    c = Check("7.2.2 L non-negative and diagonal >= 1")
    n_neg = np.sum(L < -1e-10)
    diag = np.diag(L)
    n_diag_lt1 = np.sum(diag < 1.0 - 1e-10)
    c.info(f"L min={L.min():.6f}, negative elements: {n_neg}")
    c.info(f"Diagonal: min={diag.min():.6f}, max={diag.max():.6f}, count < 1: {n_diag_lt1}")
    if n_neg > 0:
        c.fail(f"{n_neg} negative elements in L")
    if n_diag_lt1 > 0:
        c.warn(f"{n_diag_lt1} diagonal elements < 1")
    checks.append(c)

    # 7.2.3 Identity check
    c = Check("7.2.3 Identity check: L*(I-A) ≈ I (tolerance 1e-6)")
    I_minus_A = np.eye(N_EU) - A
    residual = np.max(np.abs(L @ I_minus_A - np.eye(N_EU)))
    c.info(f"Max |L*(I-A) - I| = {residual:.2e}")
    if residual > 1e-4:
        c.fail(f"Residual {residual:.2e} > 1e-4")
    elif residual > 1e-6:
        c.warn(f"Residual {residual:.2e} > 1e-6 but < 1e-4")
    checks.append(c)

    # 7.2.4 Employment coefficients reasonable
    c = Check("7.2.4 Employment coefficients d >= 0")
    n_neg = np.sum(data["d"] < -1e-10)
    c.info(f"d: min={data['d'].min():.6f}, max={data['d'].max():.6f}, negatives: {n_neg}")
    if n_neg > 0:
        c.fail(f"{n_neg} negative d values")
    checks.append(c)

    return checks


def check_accounting_identities(data: dict) -> list[Check]:
    checks = []

    decomp = data["decomp"]
    em_mat = data["em_mat"]
    em_exports = data["em_exports"]
    N = data["N"]

    # 7.3.1 Total export-supported employment
    c = Check("7.3.1 Total export-supported employment ≈ 25,597 thousand")
    actual = em_exports.sum()
    expected = 25597
    pct = abs(actual - expected) / expected * 100
    c.info(f"Actual: {actual:.0f}, Expected: ~{expected}, Deviation: {pct:.1f}%")
    if pct > 25:
        c.fail(f"Deviation {pct:.1f}% > 25%")
    elif pct > 10:
        c.warn(f"Deviation {pct:.1f}% > 10%")
    checks.append(c)

    # 7.3.2 domestic + spillover_received = total_in_country
    c = Check("7.3.2 Domestic + spillover_received = total employment in country")
    max_err = 0.0
    for _, row in decomp.iterrows():
        lhs = row["domestic_effect_THS"] + row["spillover_received_THS"]
        rhs = row["total_in_country_THS"]
        err = abs(lhs - rhs)
        if err > max_err:
            max_err = err
        if err > 1.0:
            c.fail(f"{row['country']}: domestic+spill_recv={lhs:.1f} != total_in={rhs:.1f} (err={err:.1f})")
    c.info(f"Max error: {max_err:.4f}")
    if max_err <= 1.0:
        c.info("All countries pass ✓")
    checks.append(c)

    # 7.3.3 domestic + spillover_generated = total_by_country
    c = Check("7.3.3 Domestic + spillover_generated = total employment by country")
    max_err = 0.0
    for _, row in decomp.iterrows():
        lhs = row["domestic_effect_THS"] + row["spillover_generated_THS"]
        rhs = row["total_by_country_THS"]
        err = abs(lhs - rhs)
        if err > max_err:
            max_err = err
        if err > 1.0:
            c.fail(f"{row['country']}: dom+spill_gen={lhs:.1f} != total_by={rhs:.1f}")
    c.info(f"Max error: {max_err:.4f}")
    if max_err <= 1.0:
        c.info("All countries pass ✓")
    checks.append(c)

    # 7.3.4 direct + indirect = domestic
    c = Check("7.3.4 Direct + indirect = domestic effect")
    max_err = 0.0
    for _, row in decomp.iterrows():
        lhs = row["direct_effect_THS"] + row["indirect_effect_THS"]
        rhs = row["domestic_effect_THS"]
        err = abs(lhs - rhs)
        if err > max_err:
            max_err = err
        if err > 1.0:
            c.fail(f"{row['country']}: direct+indirect={lhs:.1f} != domestic={rhs:.1f}")
    c.info(f"Max error: {max_err:.4f}")
    if max_err <= 1.0:
        c.info("All countries pass ✓")
    checks.append(c)

    # 7.3.5 Annex C row/col sums consistency
    c = Check("7.3.5 Annex C: row sums = employment in country, col sums = employment by country")
    max_row_err = 0.0
    max_col_err = 0.0
    for i, row in decomp.iterrows():
        c_code = row["country"]
        c_idx = list(data["eu_countries"]).index(c_code)
        row_sum = em_mat[c_idx, :].sum()
        col_sum = em_mat[:, c_idx].sum()
        row_expected = row["total_in_country_THS"]
        col_expected = row["total_by_country_THS"]
        row_err = abs(row_sum - row_expected)
        col_err = abs(col_sum - col_expected)
        max_row_err = max(max_row_err, row_err)
        max_col_err = max(max_col_err, col_err)
    c.info(f"Max row sum error: {max_row_err:.4f}")
    c.info(f"Max col sum error: {max_col_err:.4f}")
    if max_row_err > 1.0:
        c.fail(f"Row sum error {max_row_err:.1f} > 1")
    if max_col_err > 1.0:
        c.fail(f"Col sum error {max_col_err:.1f} > 1")
    checks.append(c)

    return checks


def check_paper_values(data: dict) -> list[Check]:
    """Cross-check against specific paper values."""
    checks = []
    decomp = data["decomp"]
    em_mat = data["em_mat"]
    eu_countries = data["eu_countries"]

    def get_country(code):
        return decomp[decomp["country"] == code].iloc[0]

    def pct_dev(actual, expected):
        return abs(actual - expected) / expected * 100

    def make_check(check_id, name, actual, expected, warn_pct=10, fail_pct=25):
        c = Check(f"{check_id} {name}")
        pct = pct_dev(actual, expected)
        c.actual = actual
        c.expected = expected
        c.info(f"Actual: {actual:.1f}, Expected: ~{expected}, Deviation: {pct:.1f}%")
        if pct > fail_pct:
            c.fail(f"Deviation {pct:.1f}% > {fail_pct}%")
        elif pct > warn_pct:
            c.warn(f"Deviation {pct:.1f}% > {warn_pct}%")
        return c

    # 7.4.1 EU-28 total export employment ≈ 25,597
    total = data["em_exports"].sum()
    checks.append(make_check("7.4.1", "EU-28 total export employment ≈ 25,597 THS", total, 25597))

    # 7.4.2 EU-28 share ≈ 11.3%
    eu_total_emp = data["em_EU"].sum()
    actual_share = total / eu_total_emp * 100
    checks.append(make_check("7.4.2", "EU-28 export employment share ≈ 11.3%", actual_share, 11.3))

    # 7.4.3 Germany employment in DE supported by all EU: ~5,700
    de_idx = eu_countries.index("DE")
    de_in = em_mat[de_idx, :].sum()
    checks.append(make_check("7.4.3", "Germany: employment IN DE ≈ 5,700 THS", de_in, 5700))

    # 7.4.4 Germany: employment BY German exports: ~6,056
    de_by = em_mat[:, de_idx].sum()
    checks.append(make_check("7.4.4", "Germany: employment BY DE exports ≈ 6,056 THS", de_by, 6056))

    # 7.4.5 Luxembourg share ~25%
    lu = get_country("LU")
    lu_share = lu["export_emp_share_pct"]
    checks.append(make_check("7.4.5", "Luxembourg export employment share ≈ 25%", lu_share, 25))

    # 7.4.6 Luxembourg spillover share ~46.7%
    # NOTE: LU has 29 missing employment cells (confidential); this biases
    # the spillover share upward. Wider tolerance of 35% applied.
    lu_spill = lu["spillover_share_pct"]
    checks.append(make_check("7.4.6", "Luxembourg spillover share ≈ 46.7% [known bias: 29 missing emp cells]",
                              lu_spill, 46.7, warn_pct=20, fail_pct=35))

    # 7.4.7 Romania spillover share ~4.5%
    ro = get_country("RO")
    ro_spill = ro["spillover_share_pct"]
    checks.append(make_check("7.4.7", "Romania spillover share ≈ 4.5%", ro_spill, 4.5))

    # 7.4.8 Industry B-E total ~9,889
    table4 = data["table4"]
    # B-E is row index 1 (0-based) in the 10-sector aggregation
    sector_names = list(pd.read_csv(
        Path("data/decomposition/industry_table4.csv"), index_col=0
    ).index)
    if "B-E" in sector_names:
        be_idx = sector_names.index("B-E")
        be_total = table4[be_idx, :].sum()
        checks.append(make_check("7.4.8", "Industry B-E total ≈ 9,889 THS", be_total, 9889))
    else:
        c = Check("7.4.8 Industry B-E total ≈ 9,889 THS")
        c.warn(f"B-E not found in sector names: {sector_names}")
        checks.append(c)

    return checks


def check_reasonableness(data: dict) -> list[Check]:
    checks = []
    decomp = data["decomp"]

    # 7.5.1 No country > 50% export employment share
    c = Check("7.5.1 No country > 50% export employment share")
    max_share = decomp["export_emp_share_pct"].max()
    max_country = decomp.loc[decomp["export_emp_share_pct"].idxmax(), "country"]
    c.info(f"Max share: {max_share:.1f}% ({max_country})")
    if max_share > 50:
        c.fail(f"{max_country} has {max_share:.1f}% > 50%")
    checks.append(c)

    # 7.5.2 Direct < domestic
    c = Check("7.5.2 Direct effects < domestic effects for all countries")
    violations = decomp[decomp["direct_effect_THS"] >= decomp["domestic_effect_THS"]]
    if len(violations) > 0:
        c.fail(f"{len(violations)} countries have direct >= domestic: {violations['country'].tolist()}")
    else:
        c.info("All countries: direct < domestic ✓")
    checks.append(c)

    # 7.5.3 Large countries top absolute employment
    c = Check("7.5.3 Large countries (DE, UK, FR, IT) in top 5 by absolute employment")
    top5 = set(decomp.nlargest(5, "domestic_effect_THS")["country"].tolist())
    large = {"DE", "UK", "FR", "IT"}
    intersection = large & top5
    c.info(f"Top 5 countries: {top5}")
    c.info(f"Large countries in top 5: {intersection}")
    if len(intersection) < 3:
        c.warn(f"Only {len(intersection)} of 4 large countries in top 5")
    checks.append(c)

    # 7.5.4 Small open economies have high shares
    c = Check("7.5.4 Small open economies (LU, IE) in top 5 by export employment share")
    top5_share = set(decomp.nlargest(5, "export_emp_share_pct")["country"].tolist())
    small_open = {"LU", "IE"}
    intersection = small_open & top5_share
    c.info(f"Top 5 by share: {top5_share}")
    c.info(f"Small open economies in top 5: {intersection}")
    if len(intersection) == 0:
        c.warn("No small open economies (LU, IE) in top 5 by share")
    checks.append(c)

    return checks


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(all_checks: dict[str, list[Check]], decomp: pd.DataFrame,
                     outputs_dir: Path) -> None:
    log = logging.getLogger("stage6")

    total_pass = sum(1 for checks in all_checks.values() for c in checks if c.status == "PASS")
    total_warn = sum(1 for checks in all_checks.values() for c in checks if c.status == "WARN")
    total_fail = sum(1 for checks in all_checks.values() for c in checks if c.status == "FAIL")
    total = total_pass + total_warn + total_fail

    overall = "SUCCESSFUL REPLICATION" if total_fail == 0 else "REPLICATION WITH ISSUES"

    lines = [
        "# FIGARO Employment Content Replication — Review Report",
        "",
        f"**Overall Assessment: {overall}**",
        "",
        f"- PASS: {total_pass}/{total}",
        f"- WARN: {total_warn}/{total}",
        f"- FAIL: {total_fail}/{total}",
        "",
        "---",
        "",
    ]

    for section_name, checks in all_checks.items():
        lines.append(f"## {section_name}")
        lines.append("")
        for c in checks:
            lines.append(str(c))
            lines.append("")

    # Country summary table
    lines += [
        "---",
        "",
        "## Country Summary",
        "",
        "| Country | Total Emp (THS) | Domestic (THS) | Spill Gen (THS) | Share % | Spill% |",
        "|---------|----------------|----------------|-----------------|---------|--------|",
    ]
    for _, row in decomp.sort_values("spillover_share_pct").iterrows():
        lines.append(
            f"| {row['country']} | {row['total_employment_THS']:.0f} | "
            f"{row['domestic_effect_THS']:.0f} | {row['spillover_generated_THS']:.0f} | "
            f"{row['export_emp_share_pct']:.1f}% | {row['spillover_share_pct']:.1f}% |"
        )

    # Known limitations
    lines += [
        "",
        "---",
        "",
        "## Known Limitations",
        "",
        "1. **Product-by-product vs. industry-by-industry**: The paper uses industry-by-industry IC-IOT "
        "(not publicly available). This replication uses product-by-product tables. Results differ.",
        "",
        "2. **Employment data vintage**: The paper uses a 2019 vintage of `nama_10_a64_e`. "
        "Current download may reflect revised figures.",
        "",
        "3. **Missing employment data**: Some country-industry cells are suppressed (confidential), "
        "notably for Luxembourg (29 missing) and Malta (31 missing). These are set to 0, "
        "causing underestimation of employment effects for those countries.",
        "",
        "4. **Upward bias**: Employment coefficients don't distinguish exporting vs. non-exporting firms. "
        "Exporters tend to be more productive, leading to upward bias (paper footnote 5).",
        "",
        "5. **FIGARO data vintage**: The table may have been revised since 2019.",
        "",
        "6. **Non-EU countries in Leontief system**: The full FIGARO table has 50 countries. "
        "This replication uses only EU-28 for the Leontief inverse (RoW not included as a full matrix block). "
        "The export vector e captures flows to all 22 non-EU countries.",
        "",
        f"*Report generated: {time.strftime('%Y-%m-%d %H:%M:%S')}*",
    ]

    report_text = "\n".join(lines)
    out_path = outputs_dir / "review_report.md"
    with open(out_path, "w") as f:
        f.write(report_text)

    log.info(f"Review report saved to {out_path}")
    log.info(f"Overall: {overall} — PASS={total_pass}, WARN={total_warn}, FAIL={total_fail}")

    return total_fail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Stage 6: Review Agent")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    base_dir = Path(args.config).parent
    prepared_dir = base_dir / "data" / "prepared"
    model_dir = base_dir / "data" / "model"
    decomp_dir = base_dir / "data" / "decomposition"
    outputs_dir = base_dir / "outputs"
    log_dir = base_dir / "logs"

    log = setup_logging(log_dir)
    log.info("=== Stage 6: Review Agent ===")

    data = load_all(prepared_dir, model_dir, decomp_dir)

    all_checks = {
        "7.1 Data Integrity": check_data_integrity(data),
        "7.2 Leontief Model": check_leontief_model(data),
        "7.3 Accounting Identities": check_accounting_identities(data),
        "7.4 Cross-checks vs. Paper": check_paper_values(data),
        "7.5 Reasonableness": check_reasonableness(data),
    }

    n_fail = generate_report(all_checks, data["decomp"], outputs_dir)

    if n_fail > 0:
        log.warning(f"Review found {n_fail} FAIL checks. See review_report.md.")
        sys.exit(1)
    else:
        log.info("All critical checks passed.")

    log.info("=== Stage 6 complete ===")


if __name__ == "__main__":
    main()
