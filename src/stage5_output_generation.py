"""
Stage 5: Output Generation (deterministic)
Produce all tables and figures matching Rémond-Tiedrez et al. (2019).

Outputs:
  outputs/tables/table1_employment_exports.csv + .xlsx
  outputs/figures/figure1.png + .pdf   (employment supported, two bar series)
  outputs/figures/figure2.png + .pdf   (export employment share, stacked)
  outputs/tables/table3_spillover.csv + .xlsx
  outputs/tables/table4_industry.csv + .xlsx
  outputs/figures/figure3.png + .pdf   (employment by product/sector)
  outputs/tables/annex_c.csv + .xlsx
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import yaml

PINK = "#E91E8C"
LIGHT_PINK = "#F9B4D5"
LIME = "#7CB342"
SOURCE_NOTE = "Source: Eurostat FIGARO, authors' calculations"


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"stage5_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return logging.getLogger("stage5")


def load_data(prepared_dir: Path, model_dir: Path, decomp_dir: Path) -> dict:
    log = logging.getLogger("stage5")
    log.info("Loading inputs...")

    with open(prepared_dir / "metadata.json") as f:
        meta = json.load(f)
    eu_countries = meta["eu_countries"]
    P = len(meta["cpa_codes"])

    em_eu = pd.read_csv(prepared_dir / "Em_EU.csv")["em_EU_THS_PER"].values
    e_noneu = pd.read_csv(prepared_dir / "e_nonEU.csv")["e_nonEU_MIO_EUR"].values

    country_decomp = pd.read_csv(decomp_dir / "country_decomposition.csv")
    annex_c = pd.read_csv(decomp_dir / "annex_c_matrix.csv", index_col=0)
    table4_src = pd.read_csv(decomp_dir / "industry_table4.csv", index_col=0)
    fig3_src = pd.read_csv(decomp_dir / "industry_figure3.csv")

    log.info(f"  country_decomp: {country_decomp.shape}, annex_c: {annex_c.shape}")
    log.info(f"  table4_src: {table4_src.shape}, fig3_src: {fig3_src.shape}")

    # Aggregate Em_EU and e_nonEU to country level
    emp_by_country = []
    exp_by_country = []
    for c_idx in range(len(eu_countries)):
        start, end = c_idx * P, (c_idx + 1) * P
        emp_by_country.append(em_eu[start:end].sum())
        exp_by_country.append(np.clip(e_noneu[start:end], 0, None).sum())

    country_totals = pd.DataFrame({
        "country": eu_countries,
        "total_employment_THS": emp_by_country,
        "exports_to_nonEU_MIO_EUR": exp_by_country,
    })

    return {
        "eu_countries": eu_countries,
        "country_totals": country_totals,
        "country_decomp": country_decomp,
        "annex_c": annex_c,
        "table4_src": table4_src,
        "fig3_src": fig3_src,
    }


def save_table(df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    log = logging.getLogger("stage5")
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_dir / f"{stem}.csv", index=False)
    with pd.ExcelWriter(out_dir / f"{stem}.xlsx", engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
    log.info(f"  Saved {stem}.csv + .xlsx ({len(df)} rows)")


def save_figure(fig: plt.Figure, out_dir: Path, stem: str) -> None:
    log = logging.getLogger("stage5")
    out_dir.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_dir / f"{stem}.png", dpi=300, bbox_inches="tight")
    fig.savefig(out_dir / f"{stem}.pdf", bbox_inches="tight")
    plt.close(fig)
    log.info(f"  Saved {stem}.png + .pdf")


def produce_table1(data: dict, tables_dir: Path) -> None:
    df = data["country_totals"][["country", "total_employment_THS", "exports_to_nonEU_MIO_EUR"]].copy()
    df = df.sort_values("country").reset_index(drop=True)
    save_table(df, tables_dir, "table1_employment_exports")


def produce_figure1(data: dict, figures_dir: Path) -> None:
    """Two grouped bar series per country: employment IN country vs BY country exports."""
    cd = data["country_decomp"].copy()
    cd = cd.sort_values("total_by_country_THS", ascending=False)
    countries = cd["country"].tolist()
    x = np.arange(len(countries))
    width = 0.38

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x - width / 2, cd["total_in_country_THS"], width,
           color=PINK, label="Employment IN country (supported by all EU exports)")
    ax.bar(x + width / 2, cd["total_by_country_THS"], width,
           color=LIGHT_PINK, label="Employment BY country exports (across all EU)")

    ax.set_xticks(x)
    ax.set_xticklabels(countries, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("Thousands of persons")
    ax.set_title("Employment supported by EU exports to non-member countries (2010)")
    ax.legend(loc="upper right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.annotate(SOURCE_NOTE, xy=(0, -0.18), xycoords="axes fraction", fontsize=7, color="gray")
    fig.tight_layout()
    save_figure(fig, figures_dir, "figure1")


def produce_figure2(data: dict, figures_dir: Path) -> None:
    """Stacked share bars: domestic + spillover received, with direct effect marker."""
    cd = data["country_decomp"].copy()
    cd["_sort_key"] = (
        (cd["domestic_effect_THS"] + cd["spillover_received_THS"]) / cd["total_employment_THS"]
    )
    cd = cd.sort_values("_sort_key", ascending=False)
    countries = cd["country"].tolist()
    x = np.arange(len(countries))

    dom_pct = cd["domestic_effect_THS"] / cd["total_employment_THS"] * 100
    spill_pct = cd["spillover_received_THS"] / cd["total_employment_THS"] * 100
    direct_pct = cd["direct_effect_THS"] / cd["total_employment_THS"] * 100

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(x, dom_pct, color=PINK, label="Domestic effect")
    ax.bar(x, spill_pct, bottom=dom_pct, color=LIGHT_PINK, label="Spillover received")
    ax.plot(x, direct_pct, "o", color=LIME, markersize=5, label="Direct effect", zorder=5)

    ax.set_xticks(x)
    ax.set_xticklabels(countries, rotation=45, ha="right", fontsize=8)
    ax.set_ylabel("% of total employment")
    ax.set_title("Employment supported by EU exports as share of total employment (2010)")
    ax.legend(loc="upper right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.1f}%"))
    ax.annotate(SOURCE_NOTE, xy=(0, -0.18), xycoords="axes fraction", fontsize=7, color="gray")
    fig.tight_layout()
    save_figure(fig, figures_dir, "figure2")


def produce_table3(data: dict, tables_dir: Path) -> None:
    """Spillover table, sorted ascending by spillover_share_pct (RO first, LU last)."""
    cd = data["country_decomp"].copy()
    df = cd[["country", "total_by_country_THS", "domestic_effect_THS",
             "spillover_generated_THS", "domestic_share_pct", "spillover_share_pct"]].copy()
    df = df.sort_values("spillover_share_pct", ascending=True).reset_index(drop=True)
    save_table(df, tables_dir, "table3_spillover")


def produce_table4(data: dict, tables_dir: Path) -> None:
    """10×10 industry table with row and column totals."""
    t4 = data["table4_src"].copy()
    t4["Total"] = t4.sum(axis=1)
    totals_row = t4.sum(axis=0)
    totals_row.name = "Total"
    t4 = pd.concat([t4, totals_row.to_frame().T])
    df = t4.reset_index().rename(columns={"index": "sector"})
    save_table(df, tables_dir, "table4_industry")


def produce_figure3(data: dict, figures_dir: Path) -> None:
    """10-sector stacked bars: domestic + spillover employment by export product."""
    f3 = data["fig3_src"].copy()
    sectors = f3["sector"].tolist()
    x = np.arange(len(sectors))

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x, f3["domestic_THS"], color=PINK, label="Domestic employment")
    ax.bar(x, f3["spillover_THS"], bottom=f3["domestic_THS"],
           color=LIGHT_PINK, label="Spillover employment")

    ax.set_xticks(x)
    ax.set_xticklabels(sectors, rotation=30, ha="right", fontsize=8)
    ax.set_ylabel("Thousands of persons")
    ax.set_title("Employment content of EU exports by product sector (2010)")
    ax.legend(fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.annotate(SOURCE_NOTE, xy=(0, -0.18), xycoords="axes fraction", fontsize=7, color="gray")
    fig.tight_layout()
    save_figure(fig, figures_dir, "figure3")


def produce_annex_c(data: dict, tables_dir: Path) -> None:
    """Full 28×28 employment matrix (rows=employment location, cols=exporting country)."""
    ac = data["annex_c"].copy()
    df = ac.reset_index().rename(columns={"index": "country"})
    save_table(df, tables_dir, "annex_c")


def main():
    parser = argparse.ArgumentParser(description="Stage 5: Output Generation")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    base_dir = Path(args.config).parent
    prepared_dir = base_dir / "data" / "prepared"
    model_dir = base_dir / "data" / "model"
    decomp_dir = base_dir / "data" / "decomposition"
    figures_dir = base_dir / "outputs" / "figures"
    tables_dir = base_dir / "outputs" / "tables"
    log_dir = base_dir / "logs"

    log = setup_logging(log_dir)
    log.info("=== Stage 5: Output Generation ===")

    data = load_data(prepared_dir, model_dir, decomp_dir)

    log.info("Producing Table 1...")
    produce_table1(data, tables_dir)

    log.info("Producing Figure 1...")
    produce_figure1(data, figures_dir)

    log.info("Producing Figure 2...")
    produce_figure2(data, figures_dir)

    log.info("Producing Table 3...")
    produce_table3(data, tables_dir)

    log.info("Producing Table 4...")
    produce_table4(data, tables_dir)

    log.info("Producing Figure 3...")
    produce_figure3(data, figures_dir)

    log.info("Producing Annex C...")
    produce_annex_c(data, tables_dir)

    warnings_path = base_dir / "outputs" / "output_warnings.txt"
    warnings_path.parent.mkdir(parents=True, exist_ok=True)
    warnings_path.write_text(
        "Stage 5 complete: 3 figures + 4 tables produced deterministically.\n"
        "Table type: product-by-product (public IC-IOT). "
        "Paper uses industry-by-industry (proprietary). Results may differ slightly.\n"
    )

    log.info("=== Stage 5 complete ===")


if __name__ == "__main__":
    main()
