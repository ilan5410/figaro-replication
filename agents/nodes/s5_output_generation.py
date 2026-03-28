"""
Stage 5: Output Generation Agent node (LangGraph).

Invokes Claude to produce all tables and figures matching the paper, using
matplotlib and pandas. The agent writes and executes a single Python script
that generates all 7 outputs in one pass.

Prompt engineering applied (same pattern as S6):
  - All file paths and column names provided upfront — no exploration needed
  - Explicit tool usage rules: execute_python only, never read_file on CSVs
  - Explicit 3-step workflow: write script → execute → write summary → stop
  - recursion_limit=15 is sufficient (8 steps nominal, 10 with one fix pass)
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

TIMEOUT_S = 300


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

**TOOL USAGE RULES — follow exactly:**
1. Use `execute_python` for ALL figure and table generation (matplotlib, pandas, openpyxl)
2. Use `write_file` ONLY to save outputs/output_warnings.txt at the end
3. NEVER use `read_file` on any CSV or matrix file — load them in Python scripts instead
4. NEVER use `list_directory` — all paths are given below

**Workflow (3 steps only):**
Step 1 — execute_python: Write and run ONE script that generates ALL 7 outputs.
Step 2 — write_file: Save a one-line summary to outputs/output_warnings.txt.
Step 3 — Stop. Do not run more scripts or re-read anything.

Input files (all exist — use these exact paths, load with pandas in Python):
  - data/prepared/Em_EU.csv              — col: em_EU_THS_PER
  - data/prepared/e_nonEU.csv            — col: e_nonEU_MIO_EUR
  - data/prepared/metadata.json          — key "eu_countries": list of 28 ISO-2 codes,
                                           key "cpa_codes": list of 64 CPA codes
  - data/model/em_exports_country_matrix.csv — 28×28 (index_col=0)
  - data/decomposition/country_decomposition.csv — 28 rows, cols:
      country, total_employment_THS, total_in_country_THS, total_by_country_THS,
      domestic_effect_THS, spillover_received_THS, spillover_generated_THS,
      direct_effect_THS, indirect_effect_THS, export_emp_share_pct,
      domestic_share_pct, spillover_share_pct
  - data/decomposition/annex_c_matrix.csv    — 28×28 (index_col=0)
  - data/decomposition/industry_table4.csv   — 10×10 (index_col=0)
  - data/decomposition/industry_figure3.csv  — cols: sector, total_employment_THS,
                                               domestic_THS, spillover_THS

Outputs to produce (save to outputs/figures/ and outputs/tables/):
  1. outputs/tables/table1_employment_exports.csv + .xlsx
  2. outputs/figures/figure1.png + .pdf
  3. outputs/figures/figure2.png + .pdf
  4. outputs/tables/table3_spillover.csv + .xlsx
  5. outputs/tables/table4_industry.csv + .xlsx
  6. outputs/figures/figure3.png + .pdf
  7. outputs/tables/annex_c.csv + .xlsx
"""

    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=4096)
    tools = get_tools_for_stage("s5_output", timeout=TIMEOUT_S)

    agent = create_react_agent(model=model, tools=tools, prompt=system_prompt)

    t0 = time.time()
    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": task_message}]},
            config={"recursion_limit": 15},
        )
        elapsed = time.time() - t0
        n_steps = len(result.get("messages", [])) - 1
        log.info(f"Output generation agent completed in {elapsed:.1f}s ({n_steps} steps used of limit 15)")

        figures_dir = REPO_ROOT / "outputs" / "figures"
        tables_dir = REPO_ROOT / "outputs" / "tables"
        figures = [str(p) for p in sorted(figures_dir.glob("*.png"))] if figures_dir.exists() else []
        tables = [str(p) for p in sorted(tables_dir.glob("*.csv"))] if tables_dir.exists() else []
        log.info(f"Produced {len(figures)} figures, {len(tables)} tables")

        # Check for missing expected outputs
        expected = [
            "outputs/tables/table1_employment_exports.csv",
            "outputs/figures/figure1.png",
            "outputs/figures/figure2.png",
            "outputs/tables/table3_spillover.csv",
            "outputs/tables/table4_industry.csv",
            "outputs/figures/figure3.png",
            "outputs/tables/annex_c.csv",
        ]
        for rel_path in expected:
            if not (REPO_ROOT / rel_path).exists():
                errors.append(f"Stage 5: {rel_path} not produced")

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
