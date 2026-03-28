"""
Stage 5: Output Generation
Reproduce all tables and figures from the paper.
"""
import argparse
import logging
import sys
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")   # non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

EU_COUNTRY_NAMES = {
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CY": "Cyprus",
    "CZ": "Czechia", "DE": "Germany", "DK": "Denmark", "EE": "Estonia",
    "EL": "Greece", "ES": "Spain", "FI": "Finland", "FR": "France",
    "HR": "Croatia", "HU": "Hungary", "IE": "Ireland", "IT": "Italy",
    "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "MT": "Malta",
    "NL": "Netherlands", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "SE": "Sweden", "SI": "Slovenia", "SK": "Slovakia", "UK": "United Kingdom",
}

# Eurostat pink/magenta palette
PINK = "#E84B8A"
LIGHT_PINK = "#F2A0C4"
LIME = "#5CB85C"
DARK_PINK = "#C1185A"


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


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_data(prepared_dir: Path, model_dir: Path, decomp_dir: Path) -> dict:
    log = logging.getLogger("stage5")
    log.info("Loading data for output generation...")

    decomp = pd.read_csv(decomp_dir / "country_decomposition.csv")
    annex_c = pd.read_csv(decomp_dir / "annex_c_matrix.csv", index_col=0)
    table4 = pd.read_csv(decomp_dir / "industry_table4.csv", index_col=0)
    fig3_data = pd.read_csv(decomp_dir / "industry_figure3.csv")

    # Load export totals (from e_nonEU)
    e_df = pd.read_csv(prepared_dir / "e_nonEU.csv")
    e_df["country"] = e_df["label"].str.split("_").str[0]
    exports_by_country = e_df.groupby("country")["e_nonEU_MIO_EUR"].sum().reset_index()
    exports_by_country.columns = ["country", "exports_MIO_EUR"]

    log.info(f"  decomp: {len(decomp)} countries")
    log.info(f"  annex_c: {annex_c.shape}")
    log.info(f"  table4: {table4.shape}")

    return {
        "decomp": decomp,
        "annex_c": annex_c,
        "table4": table4,
        "fig3_data": fig3_data,
        "exports_by_country": exports_by_country,
    }


# ---------------------------------------------------------------------------
# Table 1: Employment and exports
# ---------------------------------------------------------------------------

def make_table1(data: dict, tables_dir: Path) -> pd.DataFrame:
    log = logging.getLogger("stage5")
    log.info("Generating Table 1: Employment and exports...")

    decomp = data["decomp"]
    exports = data["exports_by_country"]

    t1 = decomp[["country", "total_employment_THS"]].merge(exports, on="country", how="left")
    t1["country_name"] = t1["country"].map(EU_COUNTRY_NAMES)
    t1 = t1.rename(columns={
        "country_name": "Country",
        "total_employment_THS": "Employment (thousand persons)",
        "exports_MIO_EUR": "Exports to non-member countries (million EUR)",
    })
    t1 = t1[["Country", "Employment (thousand persons)",
              "Exports to non-member countries (million EUR)"]].sort_values("Country")

    # Add EU-28 total row
    eu_total = pd.DataFrame([{
        "Country": "EU-28",
        "Employment (thousand persons)": t1["Employment (thousand persons)"].sum(),
        "Exports to non-member countries (million EUR)": t1["Exports to non-member countries (million EUR)"].sum(),
    }])
    t1 = pd.concat([t1, eu_total], ignore_index=True)

    out_csv = tables_dir / "table1_employment_exports.csv"
    t1.to_csv(out_csv, index=False)
    log.info(f"  Saved: {out_csv}")

    # Excel version
    out_xlsx = tables_dir / "table1_employment_exports.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        t1.to_excel(writer, sheet_name="Table1", index=False)
    log.info(f"  Saved: {out_xlsx}")

    return t1


# ---------------------------------------------------------------------------
# Table 3: Employment by spillover share
# ---------------------------------------------------------------------------

def make_table3(data: dict, tables_dir: Path) -> pd.DataFrame:
    log = logging.getLogger("stage5")
    log.info("Generating Table 3: Employment by spillover share...")

    decomp = data["decomp"]
    t3 = decomp[["country", "total_by_country_THS", "domestic_effect_THS",
                  "spillover_generated_THS", "domestic_share_pct",
                  "spillover_share_pct"]].copy()
    t3["country_name"] = t3["country"].map(EU_COUNTRY_NAMES)
    t3 = t3.rename(columns={
        "country_name": "Country",
        "total_by_country_THS": "Total (thousand)",
        "domestic_effect_THS": "Domestic (thousand)",
        "spillover_generated_THS": "Spillover (thousand)",
        "domestic_share_pct": "Domestic (%)",
        "spillover_share_pct": "Spillover (%)",
    })
    t3 = t3.sort_values("Spillover (%)")[
        ["Country", "Total (thousand)", "Domestic (thousand)",
         "Spillover (thousand)", "Domestic (%)", "Spillover (%)"]
    ]

    out_csv = tables_dir / "table3_employment_spillover.csv"
    t3.to_csv(out_csv, index=False)
    log.info(f"  Saved: {out_csv}")

    out_xlsx = tables_dir / "table3_employment_spillover.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        t3.to_excel(writer, sheet_name="Table3", index=False)
    log.info(f"  Saved: {out_xlsx}")

    return t3


# ---------------------------------------------------------------------------
# Table 4: Industry 10×10
# ---------------------------------------------------------------------------

def make_table4(data: dict, tables_dir: Path) -> None:
    log = logging.getLogger("stage5")
    log.info("Generating Table 4: 10×10 industry matrix...")

    t4 = data["table4"].copy()
    t4.index.name = "Employment in sector \\ Exports from sector"

    out_csv = tables_dir / "table4_industry_10x10.csv"
    t4.to_csv(out_csv)
    log.info(f"  Saved: {out_csv}")

    out_xlsx = tables_dir / "table4_industry_10x10.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        t4.to_excel(writer, sheet_name="Table4")
    log.info(f"  Saved: {out_xlsx}")


# ---------------------------------------------------------------------------
# Figure 1: Employment supported by EU exports (bar chart)
# ---------------------------------------------------------------------------

def make_figure1(data: dict, figures_dir: Path) -> None:
    log = logging.getLogger("stage5")
    log.info("Generating Figure 1: Employment supported by EU exports...")

    decomp = data["decomp"]
    annex_c = data["annex_c"]

    # Pink bars: employment IN country supported by ALL EU exports = row sums of annex_c
    emp_in_country = annex_c.sum(axis=1).rename("emp_in")

    # Light pink bars: employment SUPPORTED BY country's exports = col sums of annex_c
    emp_by_country = annex_c.sum(axis=0).rename("emp_by")

    df = pd.DataFrame({
        "emp_in": emp_in_country,
        "emp_by": emp_by_country,
    })
    df["country_name"] = df.index.map(EU_COUNTRY_NAMES)
    df = df.sort_values("emp_by", ascending=False)

    fig, ax = plt.subplots(figsize=(18, 7))
    x = np.arange(len(df))
    width = 0.4

    bars1 = ax.bar(x - width / 2, df["emp_in"] / 1000, width, color=PINK,
                   label="Employment in member state\n(supported by all EU exports)")
    bars2 = ax.bar(x + width / 2, df["emp_by"] / 1000, width, color=LIGHT_PINK,
                   label="Employment across EU\n(supported by member state's exports)")

    ax.set_xlabel("")
    ax.set_ylabel("Million persons")
    ax.set_title(
        "Figure 1: Employment supported by EU exports to non-member countries\n"
        "(thousand persons, 2010)",
        fontsize=11,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(df["country_name"], rotation=45, ha="right", fontsize=8)
    ax.legend(fontsize=9)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}"))
    ax.set_ylabel("Thousand persons")

    # Source note
    fig.text(0.01, 0.01,
             "Source: Eurostat FIGARO IC-IOT (product-by-product, 2010), "
             "nama_10_a64_e. Authors' calculations.",
             fontsize=7, color="gray")

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    for fmt in ["png", "pdf"]:
        out = figures_dir / f"figure1_employment_supported.{fmt}"
        plt.savefig(out, dpi=300 if fmt == "png" else 150, bbox_inches="tight")
        log.info(f"  Saved: {out}")
    plt.close()


# ---------------------------------------------------------------------------
# Figure 2: Employment share (% of total, stacked bars)
# ---------------------------------------------------------------------------

def make_figure2(data: dict, figures_dir: Path) -> None:
    log = logging.getLogger("stage5")
    log.info("Generating Figure 2: Employment share as % of total...")

    decomp = data["decomp"].copy()
    decomp["country_name"] = decomp["country"].map(EU_COUNTRY_NAMES)

    # Sort by total share descending
    decomp = decomp.sort_values("export_emp_share_pct", ascending=False)

    dom_pct = decomp["domestic_effect_THS"] / decomp["total_employment_THS"] * 100
    spill_pct = decomp["spillover_received_THS"] / decomp["total_employment_THS"] * 100
    direct_pct = decomp["direct_effect_THS"] / decomp["total_employment_THS"] * 100

    fig, ax = plt.subplots(figsize=(18, 7))
    x = np.arange(len(decomp))
    width = 0.6

    ax.bar(x, dom_pct.values, width, color=PINK, label="Domestic effect")
    ax.bar(x, spill_pct.values, width, bottom=dom_pct.values, color=LIGHT_PINK,
           label="Spillover received")
    ax.scatter(x, direct_pct.values, color=LIME, zorder=5, s=30,
               label="Direct effect (marker)")

    ax.set_ylabel("% of total employment")
    ax.set_title(
        "Figure 2: Employment supported by exports as % of total employment\n"
        "(2010, ordered by total share descending)",
        fontsize=11,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(decomp["country_name"].values, rotation=45, ha="right", fontsize=8)
    ax.legend(fontsize=9)

    fig.text(0.01, 0.01,
             "Source: Eurostat FIGARO IC-IOT (product-by-product, 2010), "
             "nama_10_a64_e. Authors' calculations.",
             fontsize=7, color="gray")

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    for fmt in ["png", "pdf"]:
        out = figures_dir / f"figure2_employment_share.{fmt}"
        plt.savefig(out, dpi=300 if fmt == "png" else 150, bbox_inches="tight")
        log.info(f"  Saved: {out}")
    plt.close()


# ---------------------------------------------------------------------------
# Figure 3: Employment by product (% of total)
# ---------------------------------------------------------------------------

def make_figure3(data: dict, figures_dir: Path) -> None:
    log = logging.getLogger("stage5")
    log.info("Generating Figure 3: Employment by product sector...")

    fig3 = data["fig3_data"].copy()
    total_emp = fig3["total_employment_THS"].sum()
    fig3["domestic_pct"] = fig3["domestic_THS"] / total_emp * 100
    fig3["spillover_pct"] = fig3["spillover_THS"] / total_emp * 100

    fig, ax = plt.subplots(figsize=(12, 6))
    x = np.arange(len(fig3))
    width = 0.6

    ax.bar(x, fig3["domestic_pct"].values, width, color=PINK, label="Domestic")
    ax.bar(x, fig3["spillover_pct"].values, width,
           bottom=fig3["domestic_pct"].values, color=LIGHT_PINK, label="Spillover")

    ax.set_ylabel("% of total export-supported employment")
    ax.set_title(
        "Figure 3: Employment supported by exports, by product group\n"
        "(% of total export-supported employment, 2010)",
        fontsize=11,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(fig3["sector"].values, rotation=30, ha="right")
    ax.legend()

    fig.text(0.01, 0.01,
             "Source: Eurostat FIGARO IC-IOT (product-by-product, 2010), "
             "nama_10_a64_e. Authors' calculations.",
             fontsize=7, color="gray")

    plt.tight_layout(rect=[0, 0.03, 1, 1])
    for fmt in ["png", "pdf"]:
        out = figures_dir / f"figure3_by_product.{fmt}"
        plt.savefig(out, dpi=300 if fmt == "png" else 150, bbox_inches="tight")
        log.info(f"  Saved: {out}")
    plt.close()


# ---------------------------------------------------------------------------
# Annex C
# ---------------------------------------------------------------------------

def make_annex_c(data: dict, tables_dir: Path) -> None:
    log = logging.getLogger("stage5")
    log.info("Generating Annex C: 28×28 country employment matrix...")

    annex_c = data["annex_c"].copy()
    annex_c.index = [EU_COUNTRY_NAMES.get(c, c) for c in annex_c.index]
    annex_c.columns = [EU_COUNTRY_NAMES.get(c, c) for c in annex_c.columns]
    annex_c.index.name = "Employment in \\ Exports from"

    out_csv = tables_dir / "annex_c_country_matrix.csv"
    annex_c.to_csv(out_csv)
    log.info(f"  Saved: {out_csv}")

    out_xlsx = tables_dir / "annex_c_country_matrix.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        annex_c.to_excel(writer, sheet_name="AnnexC")
    log.info(f"  Saved: {out_xlsx}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
    tables_dir = base_dir / "outputs" / "tables"
    figures_dir = base_dir / "outputs" / "figures"
    log_dir = base_dir / "logs"

    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)

    log = setup_logging(log_dir)
    log.info("=== Stage 5: Output Generation ===")

    data = load_data(prepared_dir, model_dir, decomp_dir)

    make_table1(data, tables_dir)
    make_table3(data, tables_dir)
    make_table4(data, tables_dir)
    make_figure1(data, figures_dir)
    make_figure2(data, figures_dir)
    make_figure3(data, figures_dir)
    make_annex_c(data, tables_dir)

    log.info("=== Stage 5 complete ===")


if __name__ == "__main__":
    main()
