"""
Stage 3: Model Construction — deterministic node (no LLM call).

Wraps the existing src/stage3_model_construction.py logic.
This stage is pure linear algebra (A, L, d) and MUST stay deterministic.
See FIGARO_AGENT_WORST_PRACTICES.md §1.1 and FIGARO_AGENTIC_INSTRUCTIONS.md §8.

Exposed as a tool: build_leontief_model(Z, x, Em) -> (A, L, d)
"""
import importlib
import logging
import sys
import time
from pathlib import Path

from agents.state import PipelineState

log = logging.getLogger("figaro.s3_model")

REPO_ROOT = Path(__file__).parent.parent.parent


def run_s3_model_construction(state: PipelineState) -> PipelineState:
    """
    LangGraph node: Run deterministic model construction.

    Calls the existing src/stage3_model_construction.py main() function.
    """
    log.info("=== Stage 3: Model Construction (Deterministic) ===")
    cfg_path = REPO_ROOT / "config.yaml"
    errors = list(state.get("errors", []))
    stage_metrics = dict(state.get("stage_metrics") or {})

    t0 = time.time()
    try:
        # Import and call the existing deterministic stage
        sys.path.insert(0, str(REPO_ROOT))
        mod = importlib.import_module("src.stage3_model_construction")

        old_argv = sys.argv
        sys.argv = ["stage3_model_construction", "--config", str(cfg_path)]
        try:
            mod.main()
        finally:
            sys.argv = old_argv

        elapsed = time.time() - t0
        log.info(f"Model construction completed in {elapsed:.1f}s")

        model_dir = REPO_ROOT / "data" / "model"
        model_paths = {}
        for fname, key in [
            ("A_EU.csv", "A_EU"),
            ("L_EU.csv", "L_EU"),
            ("d_EU.csv", "d_EU"),
            ("em_exports_total.csv", "em_exports_total"),
            ("em_exports_country_matrix.csv", "em_exports_country_matrix"),
        ]:
            p = model_dir / fname
            if p.exists():
                model_paths[key] = str(p)
            else:
                errors.append(f"Stage 3: {fname} not produced")

        stage_metrics["s3"] = {"elapsed_s": elapsed}

        return {
            **state,
            "stage": 3,
            "model_paths": model_paths,
            "errors": errors,
            "stage_metrics": stage_metrics,
        }

    except SystemExit as e:
        elapsed = time.time() - t0
        if e.code != 0:
            msg = f"Stage 3 exited with code {e.code}"
            log.error(msg)
            errors.append(msg)
            stage_metrics["s3"] = {"elapsed_s": elapsed, "error": msg}
            return {**state, "stage": 3, "errors": errors,
                    "human_intervention_needed": True, "stage_metrics": stage_metrics}
        # code 0 = success
        stage_metrics["s3"] = {"elapsed_s": elapsed}
        return {**state, "stage": 3, "stage_metrics": stage_metrics, "errors": errors}

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"Model construction failed: {exc}", exc_info=True)
        errors.append(f"Stage 3 exception: {exc}")
        stage_metrics["s3"] = {"elapsed_s": elapsed, "error": str(exc)}
        return {
            **state,
            "stage": 3,
            "errors": errors,
            "human_intervention_needed": True,
            "stage_metrics": stage_metrics,
        }
