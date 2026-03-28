"""
Stage 4: Decomposition — deterministic node (no LLM call).

Wraps the existing src/stage4_decomposition.py logic.
This stage is pure block-matrix arithmetic and MUST stay deterministic.
See FIGARO_AGENTIC_INSTRUCTIONS.md §8.
"""
import importlib
import logging
import sys
import time
from pathlib import Path

from agents.state import PipelineState

log = logging.getLogger("figaro.s4_decomposition")

REPO_ROOT = Path(__file__).parent.parent.parent


def run_s4_decomposition(state: PipelineState) -> PipelineState:
    """LangGraph node: Run deterministic decomposition."""
    log.info("=== Stage 4: Decomposition (Deterministic) ===")
    cfg_path = REPO_ROOT / "config.yaml"
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    t0 = time.time()
    try:
        sys.path.insert(0, str(REPO_ROOT))
        mod = importlib.import_module("src.stage4_decomposition")

        old_argv = sys.argv
        sys.argv = ["stage4_decomposition", "--config", str(cfg_path)]
        try:
            mod.main()
        finally:
            sys.argv = old_argv

        elapsed = time.time() - t0
        log.info(f"Decomposition completed in {elapsed:.1f}s")

        decomp_dir = REPO_ROOT / "data" / "decomposition"
        decomp_paths = {}
        for fname, key in [
            ("country_decomposition.csv", "country_decomposition"),
            ("annex_c_matrix.csv", "annex_c_matrix"),
            ("industry_table4.csv", "industry_table4"),
            ("industry_figure3.csv", "industry_figure3"),
        ]:
            p = decomp_dir / fname
            if p.exists():
                decomp_paths[key] = str(p)
            else:
                errors.append(f"Stage 4: {fname} not produced")

        stage_metrics["s4"] = {"elapsed_s": elapsed}

        return {
            **state,
            "stage": 4,
            "decomposition_paths": decomp_paths,
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    except SystemExit as e:
        elapsed = time.time() - t0
        if e.code != 0:
            msg = f"Stage 4 exited with code {e.code}"
            log.error(msg)
            errors.append(msg)
            stage_metrics["s4"] = {"elapsed_s": elapsed, "error": msg}
            return {**state, "stage": 4, "errors": errors,
                    "human_intervention_needed": True, "stage_metrics": stage_metrics}
        stage_metrics["s4"] = {"elapsed_s": elapsed}
        return {**state, "stage": 4, "stage_metrics": stage_metrics, "errors": errors}

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Decomposition failed: {exc}", exc_info=True)
        errors.append(f"Stage 4 exception: {exc}")
        stage_metrics["s4"] = {"elapsed_s": elapsed, "error": str(exc)}
        return {
            **state,
            "stage": 4,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
