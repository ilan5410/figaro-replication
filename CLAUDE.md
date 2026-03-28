# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

This project replicates the analysis in:
> Rémond-Tiedrez, Valderas-Jaramillo, Amores & Rueda-Cantuche (2019), "The employment content of EU exports: an application of FIGARO tables", *EURONA*, Issue 1, pp. 59–78.

The full specification is in `figaro_replication_instructions.md`. Read it before making any architectural decisions.

## Commands

```bash
# Run full pipeline (~35min first time: ~30min Stage 1 download + ~3min Stage 2 + ~1min 3-6)
python3 run_pipeline.py --config config.yaml

# Resume from a specific stage (data already downloaded)
python3 run_pipeline.py --config config.yaml --start-stage 3

# Run a single stage
python3 -m src.stage1_data_acquisition --config config.yaml   # ~30min, downloads to data/raw/
python3 -m src.stage2_data_preparation --config config.yaml   # ~3min, parses 11M rows
python3 -m src.stage3_model_construction --config config.yaml # ~10s
python3 -m src.stage4_decomposition --config config.yaml      # ~2s
python3 -m src.stage5_output_generation --config config.yaml  # ~3s
python3 -m src.stage6_review_agent --config config.yaml       # ~1s
```

Dependencies: `pip3 install pyyaml numpy pandas matplotlib openpyxl`

## Architecture

A 6-stage sequential multi-agent pipeline. Each stage is a self-contained module that logs its actions, saves intermediate outputs to disk (CSV/parquet), and can be re-run independently.

| Stage | Module | Input → Output |
|-------|--------|----------------|
| 1 | `data_acquisition` | Config → `data/raw/` |
| 2 | `data_preparation` | Raw files → cleaned matrices in `data/prepared/` |
| 3 | `model_construction` | Prepared data → Leontief model in `data/model/` |
| 4 | `decomposition` | Model outputs → decomposed results in `data/decomposition/` |
| 5 | `output_generation` | Decomposition → tables and figures in `outputs/` |
| 6 | `review_agent` | All intermediates → `outputs/review_report.md` |

A master orchestrator script (`run_pipeline.py`) runs all stages in sequence, reading `config.yaml` at startup.

## Core Mathematics

```
Employment content of exports = diag(d)' · L · e

Where:
  A = Z · diag(x)^-1         [Technical coefficients, 1792×1792]
  L = (I - A)^-1             [Leontief inverse]
  d = diag(x)^-1 · Em        [Employment coefficients]
  e                           [Exports to non-EU countries, 1792×1 vector]
```

Matrix dimensions: 28 EU countries × 64 NACE industries = 1,792. The US and RoW are in the IC-IOT system but excluded from the EU Leontief sub-matrix.

## Critical Methodological Points

**Export definition (Arto 2015 spec — `export_definition: "arto_2015"`)**: The EU-28 Leontief inverse is built from intra-EU intermediate flows only. The export vector `e` contains:
- All EU→non-EU flows (intermediate + final demand)
- Intra-EU final demand flows (treated as exogenous)

This avoids double-counting intra-EU intermediate trade.

**Table type**: The paper uses industry-by-industry IC-IOT (not publicly available). This pipeline defaults to product-by-product. Results will differ — document this limitation.

**Employment measure**: Persons employed (`EMP_DC`) in thousands, from Eurostat table `nama_10_a64_e`.

## Validation Benchmarks (Stage 6)

The review agent must verify against these paper values (2010, ±10% warning / >25% error):
- EU-28 total employment: ~225,677 thousand
- Total export-supported employment: ~25,597 thousand
- Germany jobs in Germany: ~5,700 thousand; by German exports: ~6,056 thousand
- Luxembourg spillover share: ~46.7%
- Industry B-E total: ~9,889 thousand jobs

Leontief model checks: A column sums < 1, all L elements ≥ 0, diagonal ≥ 1, `L·(I-A) ≈ I` (max error < 1e-6).

## Development Conventions

**Write multi-line logic as script files, then execute them.** Do not write multi-line Python/shell logic inline in bash commands. Instead, write it to a `.py` or `.sh` file, then run it with `python script.py` or `bash script.py`. Single-line commands are fine inline.

## Data Sources & API Quirks

**IC-IOT** (`naio_10_fcp_ip1`): `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1`
- Dimensions: `c_orig`, `c_dest`, `prd_ava` (row product), `prd_use` (col product), `unit=MIO_EUR`, `time`
- Must query one `c_orig` at a time (full table is too large)
- 64 CPA product codes + 6 value-added rows in `prd_ava`; 64 CPA + 5 final-demand codes in `prd_use`
- 50 countries (EU-28 + NO/CH + 20 non-EU + WRL_REST), years 2010–2013

**Employment** (`nama_10_a64_e`): `https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_a64_e`
- Filters: `na_item=EMP_DC`, `unit=THS_PER`, `geo=<one country>`, `time=<year>`
- **The `nace_r2` filter silently returns 0 rows** — always download all NACE codes and post-filter in Python
- Returns ~94 NACE codes per country (aggregates + leaves); target leaf codes are in `NACE_EMP_CODES` in `stage1_data_acquisition.py`

**Current results** (2010, product-by-product): EU-28 export employment = 24,946 thousand (paper: 25,597, −2.5%). Review: 22 PASS, 3 WARN, 0 FAIL. Warnings are all explained by LU/MT missing confidential employment data and product-vs-industry table type difference.
