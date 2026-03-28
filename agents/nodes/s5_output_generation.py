"""
Stage 5: Output Generation Agent node (LangGraph).

Invokes Claude to produce all tables and figures matching the paper, using
matplotlib and pandas. Outputs go to outputs/figures/ and outputs/tables/.
"""
import logging
import time
from pathlib import Path

from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent

from agents.state import PipelineState
from agents.tools import get_tools_for_stage

log = logging.getLogger("figaro.s5_output")

REPO_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

MAX_ITERATIONS = 4
TIMEOUT_S = 300
MAX_COST_USD = 0.50


def run_s5_output_generation(state: PipelineState) -> PipelineState:
    """LangGraph node: Run the output generation agent."""
    log.info("=== Stage 5: Output Generation Agent (Agentic) ===")
    cfg = state["config"]
    year = cfg.get("reference_year", 2010)
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    # Clean up scripts from previous runs of this stage
    scripts_dir = REPO_ROOT / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    cleaned = [p.unlink() or p.name for p in scripts_dir.glob("tmp_s5_*")]
    if cleaned:
        log.info(f"Cleaned {len(cleaned)} old s5 scripts")

    system_prompt = (PROMPTS_DIR / "output_generation.md").read_text(encoding="utf-8")

    task_message = f"""
Produce all tables and figures for the FIGARO employment content replication, year {year}.

**Do NOT list files or read files first. All file paths and column names are known — write the script immediately.**

Input files (all exist, use these exact paths):
  - data/prepared/Em_EU.csv              — 1792×1, col: em_EU_THS_PER
  - data/prepared/e_nonEU.csv            — 1792×1, col: e_nonEU_MIO_EUR
  - data/prepared/metadata.json          — has key "eu_countries" (list of 28 ISO-2 codes)
  - data/model/em_exports_total.csv      — 1792×1
  - data/model/em_exports_country_matrix.csv — 28×28
  - data/decomposition/country_decomposition.csv — 28 rows, cols include:
      country, total_employment_THS, total_in_country_THS, total_by_country_THS,
      domestic_effect_THS, spillover_received_THS, spillover_generated_THS,
      direct_effect_THS, indirect_effect_THS,
      export_emp_share_pct, domestic_share_pct, spillover_share_pct
  - data/decomposition/annex_c_matrix.csv    — 28×28
  - data/decomposition/industry_table4.csv   — 10×10 sector matrix
  - data/decomposition/industry_figure3.csv  — by-product breakdown

Outputs to produce:
  1. outputs/tables/table1_employment_exports.csv + .xlsx
  2. outputs/figures/figure1.png + .pdf   (employment supported, two bar series)
  3. outputs/figures/figure2.png + .pdf   (export employment share, stacked)
  4. outputs/tables/table3_spillover.csv + .xlsx
  5. outputs/tables/table4_industry.csv + .xlsx
  6. outputs/figures/figure3.png + .pdf   (employment by product)
  7. outputs/tables/annex_c.csv + .xlsx

Write ONE Python script that generates all 7 outputs, then execute it.
After the script succeeds, write a one-line summary to outputs/output_warnings.txt and stop.
"""

    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=4096)
    tools = get_tools_for_stage("s5_output", timeout=TIMEOUT_S)

    agent = create_react_agent(model=model, tools=tools, prompt=system_prompt)

    t0 = time.time()
    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": task_message}]},
            config={"recursion_limit": 10},  # write(1) + execute(1) + fix(1) + execute(1) + done(1)
        )
        elapsed = time.time() - t0
        log.info(f"Output generation agent completed in {elapsed:.1f}s")

        # Collect output paths
        figures_dir = REPO_ROOT / "outputs" / "figures"
        tables_dir = REPO_ROOT / "outputs" / "tables"

        figures = [str(p) for p in sorted(figures_dir.glob("*.png"))] if figures_dir.exists() else []
        tables = [str(p) for p in sorted(tables_dir.glob("*.csv"))] if tables_dir.exists() else []

        log.info(f"Produced {len(figures)} figures, {len(tables)} tables")

        stage_metrics["s5"] = {
            "elapsed_s": elapsed,
            "figures_produced": len(figures),
            "tables_produced": len(tables),
        }

        return {
            **state,
            "stage": 5,
            "output_paths": {"figures": figures, "tables": tables},
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Output generation agent failed: {exc}", exc_info=True)
        errors.append(f"Stage 5 exception: {exc}")
        stage_metrics["s5"] = {"elapsed_s": elapsed, "error": str(exc)}

        return {
            **state,
            "stage": 5,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
