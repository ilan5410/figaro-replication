"""
Stage 6: Review Agent node (LangGraph).

Invokes Claude to independently review all pipeline outputs, compare against
published benchmarks, and produce outputs/review_report.md.

This is Phase 1 of the agentic refactor — the lowest-risk, highest-signal
starting point (see FIGARO_AGENTIC_INSTRUCTIONS.md §4).
"""
import logging
import time
from pathlib import Path

from langchain_anthropic import ChatAnthropic
from langgraph.prebuilt import create_react_agent

from agents.state import PipelineState
from agents.tools import get_tools_for_stage

log = logging.getLogger("figaro.s6_review")

REPO_ROOT = Path(__file__).parent.parent.parent
PROMPTS_DIR = Path(__file__).parent.parent / "prompts"

# Stage limits (see FIGARO_AGENT_BEST_PRACTICES.md §7.1)
MAX_ITERATIONS = 3
TIMEOUT_S = 300
MAX_COST_USD = 0.30


def load_review_prompt() -> str:
    """Load the review agent system prompt from file."""
    prompt_path = PROMPTS_DIR / "review.md"
    return prompt_path.read_text(encoding="utf-8")


def run_s6_review(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Run the review agent.

    Reads pipeline outputs, invokes Claude with the review prompt and tools,
    then updates state with the review results.
    """
    log.info("=== Stage 6: Review Agent (Agentic) ===")
    cfg = state["config"]
    year = cfg.get("reference_year", 2010)

    n_total = len(cfg.get("eu_member_states", [])) * cfg.get("n_industries", 64)

    # Build the task message for the agent
    task_message = f"""
Review the FIGARO employment content replication outputs for year {year}.

**TOOL USAGE RULES — follow exactly:**
1. Use `execute_python` for ALL numerical work (loading CSVs, computing checks, printing results)
2. Use `write_file` ONLY to save outputs/review_report.md
3. NEVER use `read_file` on any CSV or matrix file — those are too large for context; load them in Python scripts instead
4. NEVER use `list_directory` — all paths are given below

**Workflow (3 steps only):**
Step 1 — execute_python: Write and run ONE script that loads all data files, runs every
         check in sections 7.1–7.5 of your system prompt, and prints structured PASS/WARN/FAIL output.
Step 2 — write_file: Save outputs/review_report.md using the printed check results.
Step 3 — Stop. Do not run more scripts or re-read anything.

Input files (all exist, load with pandas in Python):
  - data/prepared/Z_EU.csv              — {n_total}×{n_total} matrix (index_col=0)
  - data/prepared/e_nonEU.csv           — col: e_nonEU_MIO_EUR
  - data/prepared/x_EU.csv              — col: x_EU_MIO_EUR
  - data/prepared/Em_EU.csv             — col: em_EU_THS_PER
  - data/prepared/metadata.json         — keys: eu_countries, cpa_codes, n_total
  - data/model/A_EU.csv                 — technical coefficients (index_col=0)
  - data/model/L_EU.csv                 — Leontief inverse (index_col=0)
  - data/model/d_EU.csv                 — col: d_THS_PER_per_MIO_EUR
  - data/model/em_exports_total.csv     — employment content of exports (index_col=0)
  - data/model/em_exports_country_matrix.csv — 28×28 (index_col=0)
  - data/decomposition/country_decomposition.csv — cols: country, total_employment_THS,
      total_in_country_THS, total_by_country_THS, domestic_effect_THS,
      spillover_received_THS, spillover_generated_THS, spillover_share_pct

Note: Z_EU and L_EU are large ({n_total}×{n_total}). Load them with pd.read_csv(..., index_col=0)
and convert to numpy with .values for matrix operations. Avoid printing them.
"""

    # Clean up scripts from previous runs of this stage
    scripts_dir = REPO_ROOT / "scripts"
    scripts_dir.mkdir(exist_ok=True)
    cleaned = [p.unlink() or p.name for p in scripts_dir.glob("tmp_s6_*")]
    if cleaned:
        log.info(f"Cleaned {len(cleaned)} old s6 scripts")

    # Initialize the model
    model = ChatAnthropic(
        model="claude-sonnet-4-6",
        max_tokens=4096,
    )

    # Get tools for this stage (300s timeout — review doesn't do downloads)
    tools = get_tools_for_stage("s6_review", timeout=TIMEOUT_S)

    # System prompt from file
    system_prompt = load_review_prompt()

    # Create the ReAct agent
    agent = create_react_agent(
        model=model,
        tools=tools,
        prompt=system_prompt,
    )

    # Track metrics
    t0 = time.time()
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    try:
        # Invoke the agent
        result = agent.invoke(
            {"messages": [{"role": "user", "content": task_message}]},
            config={"recursion_limit": 20},  # execute_python(1) + write_file(1) + fix pass(4) = ~10 steps
        )

        elapsed = time.time() - t0
        n_messages = len(result.get("messages", []))
        n_steps = (n_messages - 1)  # first message is the user prompt
        log.info(f"Review agent completed in {elapsed:.1f}s ({n_steps} steps used of limit 20)")

        # Extract the final message
        final_message = result["messages"][-1].content if result.get("messages") else ""

        # Determine if review passed by checking the report file
        review_report_path = REPO_ROOT / "outputs" / "review_report.md"
        review_passed = False

        # Fallback: if agent forgot to write the file, save its final message as the report
        if not review_report_path.exists() and final_message:
            log.warning("Agent did not write review_report.md — saving final message as report")
            review_report_path.parent.mkdir(parents=True, exist_ok=True)
            review_report_path.write_text(
                f"# FIGARO Review Report (recovered from agent output)\n\n{final_message}"
            )

        if review_report_path.exists():
            report_text = review_report_path.read_text()
            # Parse FAIL count from report
            if "FAIL: 0" in report_text or "SUCCESSFUL REPLICATION" in report_text:
                review_passed = True
            elif "FAIL:" in report_text:
                for line in report_text.splitlines():
                    if line.startswith("- FAIL:"):
                        parts = line.split(":")
                        if len(parts) > 1:
                            count_part = parts[1].strip().split("/")[0].strip()
                            try:
                                fail_count = int(count_part)
                                review_passed = (fail_count == 0)
                            except ValueError:
                                pass
            log.info(f"Review passed: {review_passed}")
        else:
            log.warning("Review report file not found after agent run")
            errors.append("Stage 6: review_report.md not produced")

        # Track metrics
        stage_metrics["s6"] = {
            "elapsed_s": elapsed,
            "retries": 0,
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
        log.error(f"Review agent failed after {elapsed:.1f}s: {exc}", exc_info=True)
        errors.append(f"Stage 6 exception: {exc}")

        stage_metrics["s6"] = {"elapsed_s": elapsed, "retries": 0, "error": str(exc)}

        return {
            **state,
            "stage": 6,
            "review_report_path": None,
            "review_passed": False,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
