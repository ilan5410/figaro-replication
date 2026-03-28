"""
LangGraph pipeline state schema for the FIGARO agentic pipeline.

Agents communicate via file paths and metadata — NOT raw matrix data.
Each stage reads what it needs and writes outputs to disk, then updates
the state with the resulting paths.
"""
from typing import Optional, TypedDict


class PipelineState(TypedDict):
    # Configuration (loaded from config.yaml at startup)
    config: dict

    # Current stage number (for logging / routing)
    stage: int

    # ── Stage 1: Data Acquisition ─────────────────────────────────────────
    raw_data_paths: Optional[dict]
    # Expected keys:
    #   "iciot":      "data/raw/figaro_iciot_2010.csv"
    #   "employment": "data/raw/employment_2010.csv"
    #   "summary":    "data/raw/data_summary_2010.txt"
    data_summary: Optional[str]       # Content of data_summary_2010.txt
    data_valid: Optional[bool]        # Gate: passed S1 validator?

    # ── Stage 2: Data Preparation ─────────────────────────────────────────
    prepared_paths: Optional[dict]
    # Expected keys:
    #   "Z_EU":              "data/prepared/Z_EU.csv"
    #   "e_nonEU":           "data/prepared/e_nonEU.csv"
    #   "x_EU":              "data/prepared/x_EU.csv"
    #   "Em_EU":             "data/prepared/Em_EU.csv"
    #   "f_intraEU_final":   "data/prepared/f_intraEU_final.csv"
    #   "metadata":          "data/prepared/metadata.json"
    #   "summary":           "data/prepared/preparation_summary.txt"
    preparation_valid: Optional[bool]  # Gate: passed S2 validator?

    # ── Stage 3+4: Model & Decomposition (deterministic) ─────────────────
    model_paths: Optional[dict]
    # Expected keys:
    #   "A_EU":                    "data/model/A_EU.csv"
    #   "L_EU":                    "data/model/L_EU.csv"
    #   "d_EU":                    "data/model/d_EU.csv"
    #   "em_exports_total":        "data/model/em_exports_total.csv"
    #   "em_exports_country_matrix": "data/model/em_exports_country_matrix.csv"
    #   "summary":                 "data/model/model_summary.txt"

    decomposition_paths: Optional[dict]
    # Expected keys:
    #   "country_decomposition":   "data/decomposition/country_decomposition.csv"
    #   "annex_c_matrix":          "data/decomposition/annex_c_matrix.csv"
    #   "industry_table4":         "data/decomposition/industry_table4.csv"
    #   "industry_figure3":        "data/decomposition/industry_figure3.csv"

    # ── Stage 5: Output Generation ────────────────────────────────────────
    output_paths: Optional[dict]
    # Expected keys:
    #   "figures": ["outputs/figures/figure1.png", ...]
    #   "tables":  ["outputs/tables/table1.csv", ...]

    # ── Stage 6: Review ───────────────────────────────────────────────────
    review_report_path: Optional[str]   # "outputs/review_report.md"
    review_passed: Optional[bool]       # True if no FAIL checks

    # ── Error handling ────────────────────────────────────────────────────
    errors: list[str]
    human_intervention_needed: bool

    # ── Metrics tracking ─────────────────────────────────────────────────
    stage_metrics: Optional[dict]
    # Per-stage: {"s1": {"tokens": 0, "cost_usd": 0.0, "retries": 0}, ...}
