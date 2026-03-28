"""
Stage 6: Review Agent node (LangGraph).

Architecture: Direct LLM code generation (same pattern as S5).

Invokes Claude to independently review all pipeline outputs, compare against
published benchmarks, and produce outputs/review_report.md.

Pattern: pre-compute data summaries → single LLM call → execute → one retry.
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

log = logging.getLogger("figaro.s6_review")

REPO_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

TIMEOUT_S = 300
MAX_RETRIES = 1


def build_review_context(cfg: dict) -> str:
    """Pre-read data files and build a text summary for the review LLM prompt."""
    parts = []

    # metadata.json
    with open(REPO_ROOT / "data" / "prepared" / "metadata.json") as f:
        meta = json.load(f)
    eu_countries = meta["eu_countries"]
    n_total = meta.get("n_total", len(eu_countries) * len(meta["cpa_codes"]))
    parts.append(f"n_total: {n_total}")
    parts.append(f"eu_countries ({len(eu_countries)}): {eu_countries}")

    # Em_EU summary
    em_eu = pd.read_csv(REPO_ROOT / "data" / "prepared" / "Em_EU.csv")
    em_total = em_eu["em_EU_THS_PER"].sum()
    parts.append(f"\nEm_EU.csv: shape={em_eu.shape}, total={em_total:.1f} THS")
    parts.append(f"  Min={em_eu['em_EU_THS_PER'].min():.4f}, Max={em_eu['em_EU_THS_PER'].max():.1f}")
    n_neg_em = (em_eu["em_EU_THS_PER"] < 0).sum()
    parts.append(f"  Negative values: {n_neg_em}")

    # e_nonEU summary
    e_noneu = pd.read_csv(REPO_ROOT / "data" / "prepared" / "e_nonEU.csv")
    parts.append(f"\ne_nonEU.csv: shape={e_noneu.shape}, total={e_noneu['e_nonEU_MIO_EUR'].sum():.1f} MIO EUR")
    n_neg_e = (e_noneu["e_nonEU_MIO_EUR"] < 0).sum()
    parts.append(f"  Negative values: {n_neg_e}")

    # x_EU summary
    x_eu = pd.read_csv(REPO_ROOT / "data" / "prepared" / "x_EU.csv")
    parts.append(f"\nx_EU.csv: shape={x_eu.shape}, total={x_eu['x_EU_MIO_EUR'].sum():.1f} MIO EUR")

    # Z_EU shape only (too large to include data)
    z_path = REPO_ROOT / "data" / "prepared" / "Z_EU.csv"
    if z_path.exists():
        # Just read first line to get column count
        with open(z_path) as f:
            header = f.readline()
        n_cols = len(header.split(",")) - 1  # minus index column
        parts.append(f"\nZ_EU.csv: exists, n_cols={n_cols} (expected {n_total})")

    # A_EU — column sums summary
    a_path = REPO_ROOT / "data" / "model" / "A_EU.csv"
    if a_path.exists():
        # Read just a small sample for validation info
        a_sample = pd.read_csv(a_path, index_col=0, nrows=5)
        parts.append(f"\nA_EU.csv: exists, columns={len(a_sample.columns)}")

    # L_EU shape
    l_path = REPO_ROOT / "data" / "model" / "L_EU.csv"
    if l_path.exists():
        l_sample = pd.read_csv(l_path, index_col=0, nrows=5)
        parts.append(f"\nL_EU.csv: exists, columns={len(l_sample.columns)}")

    # d_EU summary
    d_eu = pd.read_csv(REPO_ROOT / "data" / "model" / "d_EU.csv")
    parts.append(f"\nd_EU.csv: shape={d_eu.shape}, columns={list(d_eu.columns)}")
    parts.append(f"  Min={d_eu.iloc[:, -1].min():.6f}, Max={d_eu.iloc[:, -1].max():.4f}")
    n_neg_d = (d_eu.iloc[:, -1] < 0).sum()
    parts.append(f"  Negative values: {n_neg_d}")

    # em_exports_country_matrix — full content (28x28)
    em_mat = pd.read_csv(REPO_ROOT / "data" / "model" / "em_exports_country_matrix.csv", index_col=0)
    parts.append(f"\nem_exports_country_matrix.csv: shape={em_mat.shape}")
    total_export_emp = em_mat.values.sum()
    parts.append(f"  Grand total (sum of all cells): {total_export_emp:.1f} THS")

    # country_decomposition — full content (28 rows)
    cd = pd.read_csv(REPO_ROOT / "data" / "decomposition" / "country_decomposition.csv")
    parts.append(f"\ncountry_decomposition.csv: shape={cd.shape}")
    parts.append(f"  columns: {list(cd.columns)}")
    parts.append(f"  Full data:\n{cd.to_string(index=False)}")

    # annex_c_matrix shape
    ac = pd.read_csv(REPO_ROOT / "data" / "decomposition" / "annex_c_matrix.csv", index_col=0)
    parts.append(f"\nannex_c_matrix.csv: shape={ac.shape}")
    parts.append(f"  Row sums (first 5): {list(ac.sum(axis=1).head(5).round(1))}")
    parts.append(f"  Col sums (first 5): {list(ac.sum(axis=0).head(5).round(1))}")

    return "\n".join(parts)


def extract_code_block(content: str) -> str:
    """Extract Python code from markdown code fence in LLM response."""
    match = re.search(r"```python\s*\n(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*\n(.*?)```", content, re.DOTALL)
    if match:
        return match.group(1).strip()
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


def run_s6_review(state: PipelineState) -> PipelineState:
    """LangGraph node: run review via direct LLM code generation."""
    log.info("=== Stage 6: Review (Direct LLM Call) ===")
    cfg = state["config"]
    year = cfg.get("reference_year", 2010)
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    # Clean up scripts from previous runs
    scripts_dir = REPO_ROOT / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    cleaned = [p.unlink() or p.name for p in scripts_dir.glob("tmp_s6_*")]
    if cleaned:
        log.info(f"Cleaned {len(cleaned)} old s6 scripts")

    # Step 1: Pre-compute data summaries
    log.info("Building review data context...")
    try:
        data_context = build_review_context(cfg)
    except Exception as exc:
        log.error(f"Failed to build review context: {exc}", exc_info=True)
        errors.append(f"Stage 6: failed to build review context: {exc}")
        stage_metrics["s6"] = {"elapsed_s": 0, "error": str(exc)}
        return {
            **state,
            "stage": 6,
            "review_report_path": None,
            "review_passed": False,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }

    log.info(f"Review context: {len(data_context)} chars")

    # Load system prompt
    system_prompt = (PROMPTS_DIR / "review.md").read_text(encoding="utf-8")

    task_message = f"""Review the FIGARO employment content replication outputs for year {year}.

Write a COMPLETE, SELF-CONTAINED Python script that:
1. Loads all data files and runs every check from sections 7.1-7.5
2. Prints structured PASS/WARN/FAIL output to stdout
3. Writes the full review report to outputs/review_report.md

Respond with ONLY a Python code block (```python ... ```). No explanation, no planning.

=== DATA SUMMARY (pre-loaded from actual files) ===

{data_context}

=== CHECKS TO RUN ===

7.1 Data Integrity:
  - EU-28 total employment (Em_EU sum) ~225,677 THS (warn >5%)
  - No negatives in Z_EU, x_EU, Em_EU
  - Dimensions: Z_EU {cfg.get('n_industries', 64)*len(cfg.get('eu_member_states', []))}x{cfg.get('n_industries', 64)*len(cfg.get('eu_member_states', []))}, e_nonEU/d/em_EU vectors correct length
  - em_exports_country_matrix is 28x28

7.2 Leontief Model:
  - A column sums all < 1.0
  - L all elements >= 0, diagonal >= 1
  - max |L*(I-A) - I| < 1e-6
  - Employment coefficients d >= 0

7.3 Accounting Identities:
  - Total export-supported employment ~25,597 THS (warn >10%, fail >25%)
  - domestic_effect + spillover_received = total_in_country (per country)
  - domestic_effect + spillover_generated = total_by_country (per country)
  - direct_effect + indirect_effect = domestic_effect (per country)
  - Annex C row sums = total_in_country, col sums = total_by_country

7.4 Cross-checks vs Paper (2010):
  - Germany IN DE ~5,700 THS, BY DE ~6,056 THS
  - Luxembourg spillover share ~46.7%
  - Romania spillover share ~4.5%
  - Industry B-E total ~9,889 THS

7.5 Reasonableness:
  - No country >50% export employment share
  - Direct < domestic for all countries
  - Large countries (DE, UK, FR, IT) in top 5 by absolute
  - Small open economies (LU, IE) in top 5 by share

=== REQUIREMENTS ===
- Load Z_EU and L_EU with pd.read_csv(..., index_col=0), convert to .values for matrix ops
- Do NOT print large matrices — only check results and summary statistics
- The script must write outputs/review_report.md using Python open()/write()
- Use the markdown format from the review prompt (PASS/WARN/FAIL with emojis)
- Known limitation: product-by-product vs industry-by-industry table type
"""

    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=16384)

    t0 = time.time()
    llm_calls = 0

    try:
        # Step 2: Single LLM call
        log.info("Calling LLM to generate review script...")
        response = model.invoke([
            SystemMessage(content=system_prompt),
            HumanMessage(content=task_message),
        ])
        llm_calls += 1
        code = extract_code_block(response.content)
        log.info(f"Generated script: {len(code)} chars, {len(code.splitlines())} lines")

        # Step 3: Execute
        script_path, result = execute_script(code, "s6_review")

        # Step 4: One retry if failed
        if result["returncode"] != 0 and MAX_RETRIES > 0:
            log.warning("Review script failed, attempting one retry...")
            fix_response = model.invoke([
                SystemMessage(content=system_prompt),
                HumanMessage(content=task_message),
                AIMessage(content=f"```python\n{code}\n```"),
                HumanMessage(content=f"""The script failed with this error:

{result['stderr']}

stdout (last 2000 chars):
{result['stdout']}

Fix the script. Respond with ONLY the corrected Python code block."""),
            ])
            llm_calls += 1
            code = extract_code_block(fix_response.content)
            log.info(f"Retry script: {len(code)} chars, {len(code.splitlines())} lines")
            script_path, result = execute_script(code, "s6_review_retry")

        elapsed = time.time() - t0
        log.info(f"Review completed in {elapsed:.1f}s ({llm_calls} LLM calls)")

        # Step 5: Check results
        review_report_path = REPO_ROOT / "outputs" / "review_report.md"
        review_passed = False

        if review_report_path.exists():
            report_text = review_report_path.read_text()
            if "FAIL: 0" in report_text or "SUCCESSFUL REPLICATION" in report_text:
                review_passed = True
            elif "FAIL:" in report_text:
                for line in report_text.splitlines():
                    if line.startswith("- FAIL:"):
                        count_str = line.split(":")[1].strip().split("/")[0].strip()
                        try:
                            review_passed = (int(count_str) == 0)
                        except ValueError:
                            pass
            log.info(f"Review passed: {review_passed}")
        else:
            log.warning("review_report.md not found after script execution")
            # Fallback: save stdout as report if script succeeded
            if result["returncode"] == 0 and result["stdout"]:
                log.info("Saving stdout as fallback review report")
                review_report_path.parent.mkdir(parents=True, exist_ok=True)
                review_report_path.write_text(
                    f"# FIGARO Review Report (from script stdout)\n\n```\n{result['stdout']}\n```\n"
                )
            else:
                errors.append("Stage 6: review_report.md not produced")

        stage_metrics["s6"] = {
            "elapsed_s": elapsed,
            "llm_calls": llm_calls,
            "report_exists": review_report_path.exists(),
        }

        return {
            **state,
            "stage": 6,
            "review_report_path": str(review_report_path) if review_report_path.exists() else None,
            "review_passed": review_passed,
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Review failed: {exc}", exc_info=True)
        errors.append(f"Stage 6 exception: {exc}")
        stage_metrics["s6"] = {"elapsed_s": elapsed, "llm_calls": llm_calls, "error": str(exc)}

        return {
            **state,
            "stage": 6,
            "review_report_path": None,
            "review_passed": False,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
