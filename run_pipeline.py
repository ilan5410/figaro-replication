"""
Master orchestrator: runs all 6 pipeline stages in sequence.
Usage: python run_pipeline.py --config config.yaml [--start-stage N] [--end-stage N]
"""
import argparse
import importlib
import logging
import sys
import time
from pathlib import Path

STAGES = [
    (1, "stage1_data_acquisition",  "Data Acquisition"),
    (2, "stage2_data_preparation",  "Data Preparation"),
    (3, "stage3_model_construction", "Model Construction"),
    (4, "stage4_decomposition",     "Decomposition"),
    (5, "stage5_output_generation", "Output Generation"),
    (6, "stage6_review_agent",      "Review Agent"),
]


def main():
    parser = argparse.ArgumentParser(description="FIGARO Replication Pipeline")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--start-stage", type=int, default=1, metavar="N",
                        help="Start from this stage number (default: 1)")
    parser.add_argument("--end-stage", type=int, default=6, metavar="N",
                        help="Stop after this stage number (default: 6)")
    args = parser.parse_args()

    log_dir = Path(args.config).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_dir / f"pipeline_{ts}.log"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    log = logging.getLogger("pipeline")
    log.info("=== FIGARO Employment Content Replication Pipeline ===")
    log.info(f"Config: {args.config}")
    log.info(f"Stages: {args.start_stage} → {args.end_stage}")

    t_start = time.time()
    for stage_num, module_name, stage_label in STAGES:
        if stage_num < args.start_stage or stage_num > args.end_stage:
            continue

        log.info(f"\n{'='*60}")
        log.info(f"STAGE {stage_num}: {stage_label}")
        log.info(f"{'='*60}")

        t0 = time.time()
        try:
            mod = importlib.import_module(f"src.{module_name}")
            # Temporarily replace sys.argv for argparse in each stage
            old_argv = sys.argv
            sys.argv = [module_name, "--config", args.config]
            try:
                mod.main()
            finally:
                sys.argv = old_argv
        except SystemExit as e:
            if e.code != 0:
                log.error(f"Stage {stage_num} exited with code {e.code}")
                log.error("Pipeline halted. Check logs above.")
                sys.exit(e.code)
        except Exception as e:
            log.error(f"Stage {stage_num} failed with exception: {e}", exc_info=True)
            sys.exit(1)

        elapsed = time.time() - t0
        log.info(f"Stage {stage_num} completed in {elapsed:.1f}s")

    total = time.time() - t_start
    log.info(f"\n{'='*60}")
    log.info(f"Pipeline complete in {total:.1f}s")
    log.info(f"{'='*60}")
    log.info("Outputs: outputs/tables/, outputs/figures/, outputs/review_report.md")


if __name__ == "__main__":
    main()
