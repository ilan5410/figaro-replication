"""
Stage 1: Data Acquisition Agent node (LangGraph).

Invokes Claude to download FIGARO IC-IOT and employment data from Eurostat
APIs. The agent writes Python scripts to handle the download, retry logic,
and saves raw data to data/raw/.
"""
import logging
import time
from pathlib import Path

from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent

from agents.state import PipelineState
from agents.tools import get_tools_for_stage

log = logging.getLogger("figaro.s1_acquisition")

REPO_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

# Stage 1 needs a long timeout because IC-IOT downloads take 30+ minutes
MAX_ITERATIONS = 5
TIMEOUT_S = 2400   # 40 minutes — download-bound
MAX_COST_USD = 1.00


def run_s1_data_acquisition(state: PipelineState) -> PipelineState:
    """LangGraph node: Run the data acquisition agent."""
    log.info("=== Stage 1: Data Acquisition Agent (Agentic) ===")
    cfg = state["config"]
    year = cfg.get("reference_year", 2010)
    eu_countries = cfg.get("eu_member_states", [])
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    # Check if data already exists (allows skipping downloads)
    raw_dir = REPO_ROOT / "data" / "raw"
    iciot_path = raw_dir / f"figaro_iciot_{year}.csv"
    emp_path = raw_dir / f"employment_{year}.csv"

    if iciot_path.exists() and emp_path.exists():
        log.info("Raw data already exists — skipping download")
        summary_path = raw_dir / f"data_summary_{year}.txt"
        summary = summary_path.read_text() if summary_path.exists() else "Data files exist (pre-downloaded)"

        return {
            **state,
            "stage": 1,
            "raw_data_paths": {
                "iciot": str(iciot_path),
                "employment": str(emp_path),
                "summary": str(summary_path) if summary_path.exists() else None,
            },
            "data_summary": summary,
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    # Clean up scripts from previous runs of this stage
    scripts_dir = REPO_ROOT / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    cleaned = [p.unlink() or p.name for p in scripts_dir.glob("tmp_s1_*")]
    if cleaned:
        log.info(f"Cleaned {len(cleaned)} old s1 scripts")

    system_prompt = (PROMPTS_DIR / "data_acquisition.md").read_text(encoding="utf-8")

    task_message = f"""
Download FIGARO data for year {year}.

Configuration:
  - Reference year: {year}
  - EU member states ({len(eu_countries)} countries): {eu_countries}
  - Non-EU countries: {cfg.get("non_eu_countries", ["US"])}

Output directory: data/raw/

Required outputs:
  1. data/raw/figaro_iciot_{year}.csv    — Full IC-IOT from naio_10_fcp_ip1 API
  2. data/raw/employment_{year}.csv     — Employment from nama_10_a64_e API
  3. data/raw/data_summary_{year}.txt   — Verification summary

For the IC-IOT, query the Eurostat JSON API one c_orig at a time to avoid
timeouts. The API endpoint is:
https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1

For employment, the endpoint is:
https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_a64_e

IMPORTANT: Do NOT filter by nace_r2 in the API — download all NACE codes and
post-filter in Python (the nace_r2 filter silently returns 0 rows).

After downloading, verify:
- Both files exist with sufficient rows
- Total EU-28 employment ≈ 225,677 thousand (within 5%)
- All {len(eu_countries)} EU countries present in both datasets

Write the data_summary file with actual dimensions and totals.
"""

    model = ChatAnthropic(model="claude-sonnet-4-6", max_tokens=4096)
    tools = get_tools_for_stage("s1_acquisition", timeout=TIMEOUT_S)

    agent = create_react_agent(model=model, tools=tools, prompt=system_prompt)

    t0 = time.time()
    try:
        result = agent.invoke(
            {"messages": [{"role": "user", "content": task_message}]},
            config={"recursion_limit": 30},  # max ~15 tool calls
        )
        elapsed = time.time() - t0
        log.info(f"Data acquisition agent completed in {elapsed:.1f}s")

        # Verify outputs exist
        raw_paths = {}
        if iciot_path.exists():
            raw_paths["iciot"] = str(iciot_path)
        else:
            errors.append(f"Stage 1: {iciot_path.name} not produced")

        if emp_path.exists():
            raw_paths["employment"] = str(emp_path)
        else:
            errors.append(f"Stage 1: {emp_path.name} not produced")

        summary_path = raw_dir / f"data_summary_{year}.txt"
        if summary_path.exists():
            raw_paths["summary"] = str(summary_path)
            data_summary = summary_path.read_text()
        else:
            data_summary = "Summary file not produced"

        stage_metrics["s1"] = {"elapsed_s": elapsed}

        # Check if we need human intervention (missing files)
        if len(errors) > 0:
            log.warning(f"Stage 1 produced {len(errors)} errors — human intervention may be needed")

        return {
            **state,
            "stage": 1,
            "raw_data_paths": raw_paths,
            "data_summary": data_summary,
            "errors": errors,
            "human_intervention_needed": len(errors) > 0 and not raw_paths.get("iciot"),
            "stage_metrics": stage_metrics,
        }

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Data acquisition agent failed: {exc}", exc_info=True)
        errors.append(f"Stage 1 exception: {exc}")
        stage_metrics["s1"] = {"elapsed_s": elapsed, "error": str(exc)}

        return {
            **state,
            "stage": 1,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
