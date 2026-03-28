"""
Stage 5: Output Generation — deterministic node (no LLM call).

Wraps src/stage5_output_generation.py. All figures and tables are
fully specified by the data schema — no reasoning required.

Replaced agentic approach because:
  - Output format is 100% specified (fixed columns, fixed plot specs)
  - Agent was hitting recursion limits producing partial outputs
  - Cost: ~$0.15/run eliminated; runtime: ~3s vs ~4min
"""
import importlib
import logging
import sys
import time
from pathlib import Path

from agents.state import PipelineState

log = logging.getLogger("figaro.s5_output")

REPO_ROOT = Path(__file__).parent.parent.parent


def run_s5_output_generation(state: PipelineState) -> PipelineState:
    """LangGraph node: Run deterministic output generation."""
    log.info("=== Stage 5: Output Generation (Deterministic) ===")
    cfg_path = REPO_ROOT / "config.yaml"
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    t0 = time.time()
    try:
        sys.path.insert(0, str(REPO_ROOT))
        mod = importlib.import_module("src.stage5_output_generation")

        old_argv = sys.argv
        sys.argv = ["stage5_output_generation", "--config", str(cfg_path)]
        try:
            mod.main()
        finally:
            sys.argv = old_argv

        elapsed = time.time() - t0
        log.info(f"Output generation completed in {elapsed:.1f}s")

        figures_dir = REPO_ROOT / "outputs" / "figures"
        tables_dir = REPO_ROOT / "outputs" / "tables"
        figures = [str(p) for p in sorted(figures_dir.glob("*.png"))] if figures_dir.exists() else []
        tables = [str(p) for p in sorted(tables_dir.glob("*.csv"))] if tables_dir.exists() else []

        # Check all expected outputs exist
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

    except SystemExit as e:
        elapsed = time.time() - t0
        if e.code != 0:
            msg = f"Stage 5 exited with code {e.code}"
            log.error(msg)
            errors.append(msg)
            stage_metrics["s5"] = {"elapsed_s": elapsed, "error": msg}
            return {**state, "stage": 5, "errors": errors,
                    "human_intervention_needed": True, "stage_metrics": stage_metrics}
        stage_metrics["s5"] = {"elapsed_s": elapsed}
        return {**state, "stage": 5, "stage_metrics": stage_metrics, "errors": errors}

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Output generation failed: {exc}", exc_info=True)
        errors.append(f"Stage 5 exception: {exc}")
        stage_metrics["s5"] = {"elapsed_s": elapsed, "error": str(exc)}
        return {
            **state,
            "stage": 5,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
