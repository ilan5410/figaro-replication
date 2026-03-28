"""
Agentic pipeline entry point.

Usage:
  # Full pipeline (all 6 stages)
  python agents/run_agentic.py --config config.yaml

  # Resume from a specific stage (e.g., if data is already downloaded)
  python agents/run_agentic.py --config config.yaml --start-stage 3

  # Run a single stage
  python agents/run_agentic.py --config config.yaml --stage 6

  # Run without LLM checkpointing (faster for testing)
  python agents/run_agentic.py --config config.yaml --no-checkpoint

  # Produce feasibility report only (requires prior run)
  python agents/run_agentic.py --config config.yaml --report-only

Environment:
  ANTHROPIC_API_KEY must be set.
"""
import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path

import yaml

# Ensure repo root is on path
REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT))

from agents.orchestrator import compile_pipeline
from agents.state import PipelineState


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"agentic_pipeline_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger("figaro.run_agentic")
    log.info(f"Log file: {log_file}")
    return log


def build_initial_state(cfg: dict, start_stage: int) -> PipelineState:
    """Build the initial pipeline state from config."""
    year = cfg.get("reference_year", 2010)
    raw_dir = REPO_ROOT / "data" / "raw"
    prepared_dir = REPO_ROOT / "data" / "prepared"
    model_dir = REPO_ROOT / "data" / "model"
    decomp_dir = REPO_ROOT / "data" / "decomposition"

    # Pre-populate paths for stages we're skipping
    raw_data_paths = None
    prepared_paths = None
    model_paths = None
    decomp_paths = None
    data_valid = None
    prep_valid = None

    if start_stage > 1:
        # Stage 1 outputs already exist
        raw_data_paths = {
            "iciot": str(raw_dir / f"figaro_iciot_{year}.csv"),
            "employment": str(raw_dir / f"employment_{year}.csv"),
            "summary": str(raw_dir / f"data_summary_{year}.txt"),
        }
        data_valid = True

    if start_stage > 2:
        # Stage 2 outputs already exist
        prepared_paths = {
            "Z_EU": str(prepared_dir / "Z_EU.csv"),
            "e_nonEU": str(prepared_dir / "e_nonEU.csv"),
            "x_EU": str(prepared_dir / "x_EU.csv"),
            "Em_EU": str(prepared_dir / "Em_EU.csv"),
            "metadata": str(prepared_dir / "metadata.json"),
        }
        prep_valid = True

    if start_stage > 3:
        # Stage 3 outputs already exist
        model_paths = {
            "A_EU": str(model_dir / "A_EU.csv"),
            "L_EU": str(model_dir / "L_EU.csv"),
            "d_EU": str(model_dir / "d_EU.csv"),
            "em_exports_total": str(model_dir / "em_exports_total.csv"),
            "em_exports_country_matrix": str(model_dir / "em_exports_country_matrix.csv"),
        }

    if start_stage > 4:
        # Stage 4 outputs already exist
        decomp_paths = {
            "country_decomposition": str(decomp_dir / "country_decomposition.csv"),
            "annex_c_matrix": str(decomp_dir / "annex_c_matrix.csv"),
            "industry_table4": str(decomp_dir / "industry_table4.csv"),
            "industry_figure3": str(decomp_dir / "industry_figure3.csv"),
        }

    return PipelineState(
        config=cfg,
        stage=start_stage,
        raw_data_paths=raw_data_paths,
        data_summary=None,
        data_valid=data_valid,
        prepared_paths=prepared_paths,
        preparation_valid=prep_valid,
        model_paths=model_paths,
        decomposition_paths=decomp_paths,
        output_paths=None,
        review_report_path=None,
        review_passed=None,
        errors=[],
        human_intervention_needed=False,
        stage_metrics={},
    )


def print_metrics_summary(state: PipelineState, log: logging.Logger) -> None:
    """Print per-stage cost and timing summary."""
    metrics = state.get("stage_metrics") or {}
    log.info("\n" + "=" * 60)
    log.info("PIPELINE METRICS SUMMARY")
    log.info("=" * 60)
    for stage, m in sorted(metrics.items()):
        elapsed = m.get("elapsed_s", 0)
        error = m.get("error", None)
        if error:
            log.info(f"  {stage}: {elapsed:.1f}s  [FAILED: {error[:60]}]")
        else:
            extra = ""
            if "figures_produced" in m:
                extra = f"  figures={m['figures_produced']}, tables={m['tables_produced']}"
            elif "report_exists" in m:
                extra = f"  report={'✓' if m['report_exists'] else '✗'}"
            log.info(f"  {stage}: {elapsed:.1f}s{extra}")


def run_report_only(cfg: dict, log: logging.Logger) -> None:
    """Produce feasibility report based on existing outputs."""
    year = cfg.get("reference_year", 2010)
    outputs_dir = REPO_ROOT / "outputs"
    report_path = outputs_dir / "review_report.md"

    if not report_path.exists():
        log.error(f"No review report found at {report_path}")
        log.error("Run the full pipeline first, then use --report-only")
        sys.exit(1)

    log.info(f"Existing review report: {report_path}")
    report_text = report_path.read_text()

    # Count pass/warn/fail
    pass_count = warn_count = fail_count = 0
    for line in report_text.splitlines():
        if line.startswith("- PASS:"):
            try:
                pass_count = int(line.split(":")[1].strip().split("/")[0])
            except Exception:
                pass
        elif line.startswith("- WARN:"):
            try:
                warn_count = int(line.split(":")[1].strip().split("/")[0])
            except Exception:
                pass
        elif line.startswith("- FAIL:"):
            try:
                fail_count = int(line.split(":")[1].strip().split("/")[0])
            except Exception:
                pass

    log.info(f"Review summary: PASS={pass_count}, WARN={warn_count}, FAIL={fail_count}")

    feasibility_path = REPO_ROOT / "docs" / "feasibility.md"
    if feasibility_path.exists():
        log.info(f"Feasibility assessment: {feasibility_path}")
    else:
        log.info("No feasibility.md found — run full pipeline to generate one")


def main():
    parser = argparse.ArgumentParser(
        description="FIGARO Agentic Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--start-stage", type=int, default=1, metavar="N",
                        help="Start from this stage (1-6). Earlier stages must have run already.")
    parser.add_argument("--stage", type=int, metavar="N",
                        help="Run only this single stage. Overrides --start-stage.")
    parser.add_argument("--no-checkpoint", action="store_true",
                        help="Disable SQLite checkpointing")
    parser.add_argument("--report-only", action="store_true",
                        help="Print summary of existing outputs, do not run pipeline")
    args = parser.parse_args()

    # Verify API key
    if not os.environ.get("ANTHROPIC_API_KEY") and not args.report_only:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set.")
        print("  export ANTHROPIC_API_KEY='sk-ant-...'")
        sys.exit(1)

    # Load config
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    log_dir = REPO_ROOT / "logs"
    log = setup_logging(log_dir)

    log.info("=== FIGARO Agentic Pipeline ===")
    log.info(f"Config: {config_path}")
    log.info(f"Reference year: {cfg.get('reference_year', 2010)}")
    log.info(f"Table type: {cfg.get('iot_table_type', 'product-by-product')}")

    if args.report_only:
        run_report_only(cfg, log)
        return

    # Determine start stage
    start_stage = args.start_stage
    end_stage = 6
    if args.stage is not None:
        start_stage = args.stage
        end_stage = args.stage
        log.info(f"Running single stage: {args.stage}")
    else:
        log.info(f"Running stages {start_stage} → {end_stage}")

    # Build initial state
    initial_state = build_initial_state(cfg, start_stage)

    # Compile pipeline
    use_checkpointing = not args.no_checkpoint
    graph = compile_pipeline(start_stage=start_stage, use_checkpointing=use_checkpointing)

    # Run pipeline
    t_start = time.time()
    log.info("Starting pipeline execution...")

    try:
        config_dict = {"configurable": {"thread_id": f"figaro_{cfg.get('reference_year', 2010)}"}}
        final_state = graph.invoke(initial_state, config=config_dict)
    except Exception as exc:
        log.error(f"Pipeline crashed: {exc}", exc_info=True)
        sys.exit(1)

    total_elapsed = time.time() - t_start

    # Results
    log.info(f"\nPipeline completed in {total_elapsed:.1f}s")

    if final_state.get("human_intervention_needed"):
        log.error("⚠️  Human intervention required. See errors above.")
        for err in final_state.get("errors", []):
            log.error(f"  {err}")
        print_metrics_summary(final_state, log)
        sys.exit(1)

    if final_state.get("review_passed"):
        log.info("✅ Review passed — replication successful")
    elif final_state.get("review_report_path"):
        log.warning("⚠️  Review completed with warnings. See outputs/review_report.md")
    else:
        log.warning("Review stage did not produce a report")

    print_metrics_summary(final_state, log)

    # Save final state summary
    summary_path = REPO_ROOT / "outputs" / "pipeline_state_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Serialize only the scalar/path parts of state (not large matrices)
        summary = {
            k: v for k, v in final_state.items()
            if k not in ("config",) and not isinstance(v, (bytes, bytearray))
        }
        with open(summary_path, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        log.info(f"Pipeline state saved to {summary_path}")
    except Exception as e:
        log.warning(f"Could not save state summary: {e}")

    log.info("=== Pipeline complete ===")
    log.info(f"Outputs: outputs/tables/, outputs/figures/, outputs/review_report.md")


if __name__ == "__main__":
    main()
