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

    # Build the task message for the agent
    # Keep it specific and goal-oriented (see FIGARO_AGENT_BEST_PRACTICES.md §3.3)
    task_message = f"""
Review the FIGARO employment content replication outputs for year {year}.

Run all verification checks described in your system prompt:
  - 7.1 Data integrity checks
  - 7.2 Leontief model checks
  - 7.3 Accounting identity checks
  - 7.4 Cross-checks against paper values
  - 7.5 Reasonableness checks

Expected files location:
  - data/prepared/: Z_EU.csv, e_nonEU.csv, x_EU.csv, Em_EU.csv, metadata.json
  - data/model/: A_EU.csv, L_EU.csv, d_EU.csv, em_exports_total.csv, em_exports_country_matrix.csv
  - data/decomposition/: country_decomposition.csv, annex_c_matrix.csv, industry_table4.csv

Save the complete review report to: outputs/review_report.md

After writing the report, read it back to confirm it was written correctly,
then report:
  - How many PASS / WARN / FAIL checks
  - Whether any FAIL checks were found (True/False)
  - A 2-3 sentence overall assessment
"""

    # Initialize the model
    model = ChatAnthropic(
        model="claude-sonnet-4-6",
        max_tokens=8192,
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
            config={"recursion_limit": MAX_ITERATIONS * 15},  # approx tool calls per iteration
        )

        elapsed = time.time() - t0
        log.info(f"Review agent completed in {elapsed:.1f}s")

        # Extract the final message
        final_message = result["messages"][-1].content if result.get("messages") else ""

        # Determine if review passed by checking the report file
        review_report_path = REPO_ROOT / "outputs" / "review_report.md"
        review_passed = False

        if review_report_path.exists():
            report_text = review_report_path.read_text()
            # Parse FAIL count from report
            if "FAIL: 0" in report_text or "SUCCESSFUL REPLICATION" in report_text:
                review_passed = True
            elif "FAIL:" in report_text:
                # Extract the FAIL count
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
