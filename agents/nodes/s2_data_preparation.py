"""
Stage 2: Data Preparation Agent node (LangGraph).

Invokes Claude to parse raw FIGARO and employment data, extract the EU-28
sub-matrices, and save analysis-ready files to data/prepared/.

This is the riskiest agentic stage — misalignment of country/industry ordering
produces silently wrong results. The deterministic validator in validators.py
is the safety net.
"""
import logging
import time
from pathlib import Path

from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent

from agents.state import PipelineState
from agents.tools import get_tools_for_stage

log = logging.getLogger("figaro.s2_preparation")

REPO_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

MAX_ITERATIONS = 5
TIMEOUT_S = 600    # 10 minutes — parsing 11M rows is slow
MAX_COST_USD = 0.50


def run_s2_data_preparation(state: PipelineState) -> PipelineState:
    """LangGraph node: Run the data preparation agent."""
    log.info("=== Stage 2: Data Preparation Agent (Agentic) ===")
    cfg = state["config"]
    year = cfg.get("reference_year", 2010)
    eu_countries = cfg.get("eu_member_states", [])
    n_industries = cfg.get("n_industries", 64)
    n_total = len(eu_countries) * n_industries
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    # Check if prepared data already exists
    prepared_dir = REPO_ROOT / "data" / "prepared"
    if all((prepared_dir / f).exists() for f in
           ["Z_EU.csv", "e_nonEU.csv", "x_EU.csv", "Em_EU.csv", "metadata.json"]):
        log.info("Prepared data already exists — skipping preparation")
        return {
            **state,
            "stage": 2,
            "prepared_paths": {
                "Z_EU": str(prepared_dir / "Z_EU.csv"),
                "e_nonEU": str(prepared_dir / "e_nonEU.csv"),
                "x_EU": str(prepared_dir / "x_EU.csv"),
                "Em_EU": str(prepared_dir / "Em_EU.csv"),
                "metadata": str(prepared_dir / "metadata.json"),
            },
            "preparation_valid": True,
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    system_prompt = (PROMPTS_DIR / "data_preparation.md").read_text(encoding="utf-8")

    raw_paths = state.get("raw_data_paths", {})
    iciot_file = raw_paths.get("iciot", f"data/raw/figaro_iciot_{year}.csv")
    emp_file = raw_paths.get("employment", f"data/raw/employment_{year}.csv")

    task_message = f"""
Prepare the FIGARO data for the Leontief analysis (year {year}).

Input files:
  - IC-IOT: {iciot_file}
  - Employment: {emp_file}

Configuration:
  - EU member states ({len(eu_countries)}): {eu_countries}
  - Number of industries: {n_industries}
  - Expected total dimension: {n_total} = {len(eu_countries)} × {n_industries}
  - Export definition: arto_2015 (EU→non-EU intermediate + final demand)

Required outputs (save to data/prepared/):
  1. Z_EU.csv          — {n_total}×{n_total} intermediate use matrix (EU×EU only)
  2. e_nonEU.csv       — {n_total}×1 export vector (column: e_nonEU_MIO_EUR)
  3. x_EU.csv          — {n_total}×1 total output (column: x_EU_MIO_EUR)
  4. Em_EU.csv         — {n_total}×1 employment (column: em_EU_THS_PER)
  5. f_intraEU_final.csv — intra-EU final demand (reference only)
  6. metadata.json     — country order, industry order, dimensions
  7. preparation_summary.txt — human-readable summary of what was prepared

CRITICAL ordering requirements:
  - The ordering of (country, product) pairs in Z_EU, e_nonEU, x_EU, Em_EU
    MUST be identical: outer loop = countries in this order: {eu_countries}
    inner loop = CPA products in their natural alphabetical order from the data
  - Save the explicit ordering to metadata.json so downstream stages can verify

Start by inspecting the IC-IOT file structure (first few rows and column names)
before writing any processing code. The IC-IOT has columns:
  c_orig, c_dest, prd_ava, prd_use, unit, time, OBS_VALUE

Then:
1. Extract the 64 CPA product codes (prd_ava codes that start with "CPA_")
2. Build Z^EU: filter to c_orig ∈ EU, c_dest ∈ EU, prd_ava is CPA code, prd_use is CPA code
3. Build e_nonEU: filter to c_orig ∈ EU, c_dest ∉ EU (all deliveries: intermediate + final)
4. Compute x_EU: row sums of full delivery matrix (both EU and non-EU destinations)
5. Parse employment: filter to EMP_DC/THS_PER, map NACE codes to CPA codes, align ordering

After preparing, run verification checks and print:
- Z_EU shape (should be {n_total}×{n_total})
- Employment total (should be ~225,677 for 2010)
- First 3 row/column labels of Z_EU to confirm ordering
"""

    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=4096)
    tools = get_tools_for_stage("s2_preparation", timeout=TIMEOUT_S)

    agent = create_react_agent(model=model, tools=tools, prompt=system_prompt)

    t0 = time.time()
    retries = 0

    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": task_message}]},
            config={"recursion_limit": 25},  # max ~12 tool calls
        )
        elapsed = time.time() - t0
        log.info(f"Data preparation agent completed in {elapsed:.1f}s")

        # Check output files
        prepared_paths = {}
        for fname, key in [
            ("Z_EU.csv", "Z_EU"),
            ("e_nonEU.csv", "e_nonEU"),
            ("x_EU.csv", "x_EU"),
            ("Em_EU.csv", "Em_EU"),
            ("metadata.json", "metadata"),
        ]:
            full_path = prepared_dir / fname
            if full_path.exists():
                prepared_paths[key] = str(full_path)
            else:
                errors.append(f"Stage 2: {fname} not produced")

        stage_metrics["s2"] = {"elapsed_s": elapsed, "retries": retries}

        preparation_valid = len(errors) == len(state.get("errors", []))  # no new errors

        return {
            **state,
            "stage": 2,
            "prepared_paths": prepared_paths,
            "preparation_valid": preparation_valid,
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Data preparation agent failed: {exc}", exc_info=True)
        errors.append(f"Stage 2 exception: {exc}")
        stage_metrics["s2"] = {"elapsed_s": elapsed, "retries": retries, "error": str(exc)}

        return {
            **state,
            "stage": 2,
            "preparation_valid": False,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
