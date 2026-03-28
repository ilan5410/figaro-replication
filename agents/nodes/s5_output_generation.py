"""
Stage 5: Output Generation Agent node (LangGraph).

Architecture: Direct LLM code generation (not a ReAct agent loop).

The create_react_agent approach failed repeatedly — the agent wasted steps on
planning and data exploration instead of writing code. This module uses a
proven pattern (Microsoft LIDA / ChartGPT style):

  1. Pre-compute data summaries → inject into prompt (no exploration needed)
  2. Single LLM call → generate complete Python script
  3. Execute the script
  4. If error → one retry with error injected
  5. Done (2-4 LLM calls max, deterministic flow)
"""
import json
import logging
import re
import subprocess
import time
from pathlib import Path

import numpy as np
import pandas as pd
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from agents.state import PipelineState

log = logging.getLogger("figaro.s5_output")

REPO_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

TIMEOUT_S = 300
MAX_RETRIES = 1


def build_data_context() -> str:
    """Pre-read data files and build a text summary for the LLM prompt.

    This eliminates any need for the LLM to explore or probe data.
    It sees the actual values and can write correct code on the first try.
    """
    parts = []

    # metadata.json
    with open(REPO_ROOT / "data" / "prepared" / "metadata.json") as f:
        meta = json.load(f)
    eu_countries = meta["eu_countries"]
    cpa_codes = meta["cpa_codes"]
    parts.append(f"eu_countries ({len(eu_countries)}): {eu_countries}")
    parts.append(f"cpa_codes ({len(cpa_codes)}): first 5 = {cpa_codes[:5]}, last 5 = {cpa_codes[-5:]}")

    # Em_EU.csv
    em_eu = pd.read_csv(REPO_ROOT / "data" / "prepared" / "Em_EU.csv")
    parts.append(f"\nEm_EU.csv: shape={em_eu.shape}, columns={list(em_eu.columns)}")
    parts.append(f"  Total employment: {em_eu['em_EU_THS_PER'].sum():.1f} THS")

    # e_nonEU.csv
    e_noneu = pd.read_csv(REPO_ROOT / "data" / "prepared" / "e_nonEU.csv")
    parts.append(f"\ne_nonEU.csv: shape={e_noneu.shape}, columns={list(e_noneu.columns)}")
    parts.append(f"  Total exports: {e_noneu['e_nonEU_MIO_EUR'].sum():.1f} MIO EUR")

    # country_decomposition.csv — full content (only 28 rows)
    cd = pd.read_csv(REPO_ROOT / "data" / "decomposition" / "country_decomposition.csv")
    parts.append(f"\ncountry_decomposition.csv: shape={cd.shape}")
    parts.append(f"  columns: {list(cd.columns)}")
    parts.append(f"  Full data:\n{cd.to_string(index=False)}")

    # industry_figure3.csv — full content (only ~10 rows)
    f3 = pd.read_csv(REPO_ROOT / "data" / "decomposition" / "industry_figure3.csv")
    parts.append(f"\nindustry_figure3.csv: shape={f3.shape}")
    parts.append(f"  columns: {list(f3.columns)}")
    parts.append(f"  Full data:\n{f3.to_string(index=False)}")

    # industry_table4.csv — full content (10x10)
    t4 = pd.read_csv(REPO_ROOT / "data" / "decomposition" / "industry_table4.csv", index_col=0)
    parts.append(f"\nindustry_table4.csv: shape={t4.shape}")
    parts.append(f"  index: {list(t4.index)}")
    parts.append(f"  columns: {list(t4.columns)}")
    parts.append(f"  Full data:\n{t4.to_string()}")

    # annex_c_matrix.csv — shape + first 3 rows (28x28)
    ac = pd.read_csv(REPO_ROOT / "data" / "decomposition" / "annex_c_matrix.csv", index_col=0)
    parts.append(f"\nannex_c_matrix.csv: shape={ac.shape}")
    parts.append(f"  index (rows = employment location): {list(ac.index)}")
    parts.append(f"  columns (cols = exporting country): {list(ac.columns)}")
    parts.append(f"  First 3 rows:\n{ac.head(3).to_string()}")

    # em_exports_country_matrix.csv — shape only (28x28)
    em_mat = pd.read_csv(REPO_ROOT / "data" / "model" / "em_exports_country_matrix.csv", index_col=0)
    parts.append(f"\nem_exports_country_matrix.csv: shape={em_mat.shape}")
    parts.append(f"  index: {list(em_mat.index)}")
    parts.append(f"  columns: {list(em_mat.columns)}")

    return "\n".join(parts)


def extract_code_block(content: str) -> str:
    """Extract Python code from markdown code fence in LLM response."""
    # Try ```python ... ``` first
    match = re.search(r"```python\s*\n(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try ``` ... ``` (no language tag)
    match = re.search(r"```\s*\n(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()

    # If no code fence, return content as-is (best effort)
    return content.strip()


def execute_script(code: str, stage_name: str, timeout: int = TIMEOUT_S) -> tuple:
    """Write code to scripts/ and execute. Returns (script_path, result_dict)."""
    scripts_dir = REPO_ROOT / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)

    ts = time.strftime("%Y%m%d_%H%M%S")
    script_path = scripts_dir / f"tmp_{stage_name}_{ts}.py"
    script_path.write_text(code)

    log.info(f"[{stage_name}] Executing {script_path} ({len(code.splitlines())} lines)")

    try:
        result = subprocess.run(
            ["python3", str(script_path)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(REPO_ROOT),
        )

        stdout = result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout
        stderr = result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr

        if result.returncode != 0:
            log.warning(f"[{stage_name}] Script failed (rc={result.returncode})")
            log.warning(f"  stderr: {stderr[:500]}")
        else:
            log.info(f"[{stage_name}] Script succeeded")
            if stdout:
                log.info(f"  stdout (last 500): {stdout[-500:]}")

        return script_path, {
            "stdout": stdout,
            "stderr": stderr,
            "returncode": result.returncode,
        }

    except subprocess.TimeoutExpired:
        log.error(f"[{stage_name}] Script timed out after {timeout}s")
        return script_path, {
            "stdout": "",
            "stderr": f"TIMEOUT: Script exceeded {timeout}s limit.",
            "returncode": -1,
        }


def run_s5_output_generation(state: PipelineState) -> PipelineState:
    """LangGraph node: generate outputs via direct LLM code generation."""
    log.info("=== Stage 5: Output Generation (Direct LLM Call) ===")
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

    # Step 1: Pre-compute data summaries (deterministic Python, no LLM)
    log.info("Building data context...")
    try:
        data_context = build_data_context()
    except Exception as exc:
        log.error(f"Failed to build data context: {exc}", exc_info=True)
        errors.append(f"Stage 5: failed to build data context: {exc}")
        stage_metrics["s5"] = {"elapsed_s": 0, "error": str(exc)}
        return {
            **state,
            "stage": 5,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }

    log.info(f"Data context: {len(data_context)} chars")

    # Load system prompt
    system_prompt = (PROMPTS_DIR / "output_generation.md").read_text(encoding="utf-8")

    # Build the task message with all data pre-loaded
    task_message = f"""Produce all tables and figures for the FIGARO employment content replication, year {year}.

Write a COMPLETE, SELF-CONTAINED Python script that generates ALL 7 outputs when executed.
The script runs from the repo root with `python3 script.py`.

Respond with ONLY a Python code block (```python ... ```). No explanation, no planning, no preamble.

=== DATA SUMMARY (pre-loaded from actual files) ===

{data_context}

=== OUTPUT FILES TO PRODUCE ===

1. outputs/tables/table1_employment_exports.csv + .xlsx
   - Columns: country, total_employment_THS, exports_to_nonEU_MIO_EUR
   - Employment: sum Em_EU per country (64 industries each), Exports: sum e_nonEU per country
   - Sort by country code alphabetically

2. outputs/figures/figure1.png + .pdf
   - Two grouped bars per country (28 countries), sorted descending by total_by_country_THS
   - Pink (#E91E8C): total_in_country_THS, Light pink (#F9B4D5): total_by_country_THS
   - Title: "Employment supported by EU exports to non-member countries ({year})"

3. outputs/figures/figure2.png + .pdf
   - Stacked bars as % of total_employment_THS, sorted descending by (domestic+spillover_received)/total
   - Pink (#E91E8C): domestic_effect_THS/total*100, Light pink (#F9B4D5): spillover_received_THS/total*100
   - Lime dot (#7CB342): direct_effect_THS/total*100
   - Title: "Employment supported by EU exports as share of total employment ({year})"

4. outputs/tables/table3_spillover.csv + .xlsx
   - Columns: country, total_by_country_THS, domestic_effect_THS, spillover_generated_THS, domestic_share_pct, spillover_share_pct
   - Sort ascending by spillover_share_pct (Romania first, Luxembourg last)

5. outputs/tables/table4_industry.csv + .xlsx
   - 10x10 sector matrix from industry_table4.csv with row and column totals added

6. outputs/figures/figure3.png + .pdf
   - 10 sectors stacked bars: Pink domestic_THS, Light pink spillover_THS
   - From industry_figure3.csv

7. outputs/tables/annex_c.csv + .xlsx
   - Full 28x28 matrix from annex_c_matrix.csv

=== REQUIREMENTS ===
- matplotlib.use('Agg') at the very top
- Figure size (14, 6), DPI 300, tick labels rotated 45 degrees
- Source note: "Source: Eurostat FIGARO, authors' calculations"
- Excel files: use openpyxl engine
- Create output directories with os.makedirs(..., exist_ok=True)
- At the end, verify each file exists and print file sizes
"""

    # Initialize model with large max_tokens for complete script generation
    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=16384)

    t0 = time.time()
    llm_calls = 0

    try:
        # Step 2: Single LLM call to generate complete script
        log.info("Calling LLM to generate output script...")
        response = model.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=task_message),
        ])
        llm_calls += 1
        code = extract_code_block(response.content)
        log.info(f"Generated script: {len(code)} chars, {len(code.splitlines())} lines")

        # Step 3: Execute the script
        script_path, result = execute_script(code, "s5_output")

        # Step 4: If error, one retry with error injected
        if result["returncode"] != 0 and MAX_RETRIES > 0:
            log.warning("Script failed, attempting one retry with error context...")
            fix_response = model.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=task_message),
                AIMessage(content=f"```python\n{code}\n```"),
                HumanMessage(content=f"""The script failed with this error:

{result['stderr']}

stdout (last 2000 chars):
{result['stdout']}

Fix the script. Respond with ONLY the corrected Python code block. Do not explain — just provide the fixed code."""),
            ])
            llm_calls += 1
            code = extract_code_block(fix_response.content)
            log.info(f"Retry script: {len(code)} chars, {len(code.splitlines())} lines")
            script_path, result = execute_script(code, "s5_output_retry")

        elapsed = time.time() - t0
        log.info(f"Output generation completed in {elapsed:.1f}s ({llm_calls} LLM calls)")

        # Step 5: Collect outputs, return state
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

        if result["returncode"] != 0:
            errors.append(f"Stage 5: script failed after {llm_calls} attempts")

        stage_metrics["s5"] = {
            "elapsed_s": elapsed,
            "llm_calls": llm_calls,
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
        log.error(f"Output generation failed: {exc}", exc_info=True)
        errors.append(f"Stage 5 exception: {exc}")
        stage_metrics["s5"] = {"elapsed_s": elapsed, "llm_calls": llm_calls, "error": str(exc)}

        return {
            **state,
            "stage": 5,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
