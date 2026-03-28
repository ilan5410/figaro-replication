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

    system_prompt = (PROMPTS_DIR / "output_generation.md").read_text(encoding="utf-8")

    task_message = f"""
Produce all tables and figures for the FIGARO employment content replication, year {year}.

Input data locations:
  - data/prepared/    — Em_EU.csv, e_nonEU.csv, metadata.json
  - data/model/       — em_exports_total.csv, em_exports_country_matrix.csv
  - data/decomposition/ — country_decomposition.csv, annex_c_matrix.csv,
                          industry_table4.csv, industry_figure3.csv

Outputs to produce:
  1. outputs/tables/table1_employment_exports.csv + .xlsx
  2. outputs/figures/figure1.png + .pdf   (employment supported, two bar series)
  3. outputs/figures/figure2.png + .pdf   (export employment share, stacked)
  4. outputs/tables/table3_spillover.csv + .xlsx
  5. outputs/tables/table4_industry.csv + .xlsx
  6. outputs/figures/figure3.png + .pdf   (employment by product)
  7. outputs/tables/annex_c.csv + .xlsx

After producing all outputs, verify each file exists and write a summary to
outputs/output_warnings.txt listing what was produced and any issues.
"""

    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=8192)
    tools = get_tools_for_stage("s5_output", timeout=TIMEOUT_S)

    agent = create_react_agent(model=model, tools=tools, prompt=system_prompt)

    t0 = time.time()
    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": task_message}]},
            config={"recursion_limit": MAX_ITERATIONS * 15},
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
