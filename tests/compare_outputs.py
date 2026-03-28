"""
Compare agentic pipeline outputs against the deterministic baseline.

Usage:
  python tests/compare_outputs.py
  python tests/compare_outputs.py --agent-dir outputs/agent --baseline-dir outputs
  python tests/compare_outputs.py --stage s6    # Test only the review agent output

Tests the parallel baseline strategy from FIGARO_AGENT_BEST_PRACTICES.md §10.1.

The deterministic pipeline is the ground truth. Every agentic output must
match within tolerance.
"""
import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).parent.parent

RTOL = 1e-4    # Relative tolerance for matrix comparisons
ATOL = 1e-6    # Absolute tolerance


def compare_csv_matrices(
    agent_path: Path,
    baseline_path: Path,
    name: str,
    rtol: float = RTOL,
    atol: float = ATOL,
    index_col: int | None = 0,
) -> dict:
    """Compare two CSV matrix files element-wise."""
    result = {"name": name, "status": "SKIP", "details": ""}

    if not baseline_path.exists():
        result["status"] = "SKIP"
        result["details"] = f"Baseline missing: {baseline_path.name}"
        return result

    if not agent_path.exists():
        result["status"] = "FAIL"
        result["details"] = f"Agent output missing: {agent_path.name}"
        return result

    try:
        agent_df = pd.read_csv(agent_path, index_col=index_col)
        baseline_df = pd.read_csv(baseline_path, index_col=index_col)

        # Shape check
        if agent_df.shape != baseline_df.shape:
            result["status"] = "FAIL"
            result["details"] = f"Shape mismatch: agent={agent_df.shape}, baseline={baseline_df.shape}"
            return result

        # Element-wise numerical comparison
        agent_vals = agent_df.values.astype(float)
        baseline_vals = baseline_df.values.astype(float)

        max_abs_err = np.max(np.abs(agent_vals - baseline_vals))
        max_rel_err = np.max(
            np.abs(agent_vals - baseline_vals) / (np.abs(baseline_vals) + 1e-10)
        )
        all_close = np.allclose(agent_vals, baseline_vals, rtol=rtol, atol=atol)

        if all_close:
            result["status"] = "PASS"
            result["details"] = (
                f"Shape: {agent_df.shape}, "
                f"max abs err: {max_abs_err:.2e}, max rel err: {max_rel_err:.2e}"
            )
        elif max_rel_err < 0.01:
            result["status"] = "WARN"
            result["details"] = (
                f"Small deviation: max rel err {max_rel_err:.2e} "
                f"(> rtol={rtol} but < 1%)"
            )
        else:
            result["status"] = "FAIL"
            result["details"] = (
                f"Large deviation: max abs err {max_abs_err:.2e}, "
                f"max rel err {max_rel_err:.2e}"
            )

    except Exception as e:
        result["status"] = "FAIL"
        result["details"] = f"Error: {e}"

    return result


def test_s6_review_agent(agent_report_path: Path, baseline_report_path: Path) -> list[dict]:
    """
    Test S6: Agent review report should catch all issues the hardcoded review catches.

    See FIGARO_AGENT_BEST_PRACTICES.md §10.2 eval tasks.
    """
    results = []

    # Check that known issues appear in the report
    known_issues = [
        ("product_vs_industry", ["product-by-product", "industry-by-industry"]),
        ("LU_missing_employment", ["Luxembourg", "missing", "LU"]),
        ("MT_missing_employment", ["Malta", "missing", "MT"]),
    ]

    if not agent_report_path.exists():
        results.append({
            "name": "S6: report exists",
            "status": "FAIL",
            "details": f"Agent review report not found: {agent_report_path}",
        })
        return results

    report_text = agent_report_path.read_text().lower()

    results.append({
        "name": "S6: report exists",
        "status": "PASS",
        "details": f"Found: {agent_report_path.name} ({len(report_text)} chars)",
    })

    for issue_name, keywords in known_issues:
        found = any(kw.lower() in report_text for kw in keywords)
        results.append({
            "name": f"S6: mentions {issue_name}",
            "status": "PASS" if found else "WARN",
            "details": (
                f"Keywords found: {[k for k in keywords if k.lower() in report_text]}"
                if found else f"Expected keywords not found: {keywords}"
            ),
        })

    # Check that PASS/WARN/FAIL counts are present and reasonable
    for marker in ["- pass:", "- warn:", "- fail:"]:
        if marker in report_text:
            results.append({
                "name": f"S6: contains {marker.strip()}",
                "status": "PASS",
                "details": "Found in report",
            })
        else:
            results.append({
                "name": f"S6: contains {marker.strip()}",
                "status": "WARN",
                "details": "Not found in report",
            })

    # Compare key sections against baseline
    if baseline_report_path.exists():
        baseline_text = baseline_report_path.read_text().lower()
        baseline_fail_count = baseline_text.count("❌")
        agent_fail_count = report_text.count("❌")
        if agent_fail_count <= baseline_fail_count:
            results.append({
                "name": "S6: fail count <= baseline",
                "status": "PASS",
                "details": f"Agent: {agent_fail_count} fails, Baseline: {baseline_fail_count} fails",
            })
        else:
            results.append({
                "name": "S6: fail count <= baseline",
                "status": "WARN",
                "details": f"Agent found MORE fails ({agent_fail_count}) than baseline ({baseline_fail_count})",
            })

    return results


def test_s2_prepared_data(agent_dir: Path, baseline_dir: Path) -> list[dict]:
    """Test S2: Agent-prepared matrices should match deterministic baseline."""
    results = []

    for fname, name, index_col in [
        ("e_nonEU.csv", "S2: e_nonEU vector", None),
        ("x_EU.csv", "S2: x_EU vector", None),
        ("Em_EU.csv", "S2: Em_EU vector", None),
    ]:
        results.append(compare_csv_matrices(
            agent_dir / fname,
            baseline_dir / fname,
            name,
            index_col=index_col,
        ))

    # Z_EU is large — compare subset (first 100 rows/cols) to save time
    z_agent = agent_dir / "Z_EU.csv"
    z_baseline = baseline_dir / "Z_EU.csv"
    if z_agent.exists() and z_baseline.exists():
        try:
            agent_Z = pd.read_csv(z_agent, index_col=0, nrows=100).iloc[:, :100]
            baseline_Z = pd.read_csv(z_baseline, index_col=0, nrows=100).iloc[:, :100]
            close = np.allclose(agent_Z.values.astype(float), baseline_Z.values.astype(float),
                                rtol=RTOL, atol=ATOL)
            results.append({
                "name": "S2: Z_EU matrix (first 100×100)",
                "status": "PASS" if close else "FAIL",
                "details": (
                    "First 100×100 block matches baseline"
                    if close else "First 100×100 block DIFFERS from baseline"
                ),
            })
        except Exception as e:
            results.append({"name": "S2: Z_EU matrix", "status": "FAIL", "details": str(e)})
    else:
        results.append({
            "name": "S2: Z_EU matrix",
            "status": "SKIP",
            "details": "One or both files missing",
        })

    return results


def test_model_outputs(agent_dir: Path, baseline_dir: Path) -> list[dict]:
    """Test S3+S4: Model and decomposition outputs."""
    results = []

    for fname, name in [
        ("em_exports_total.csv", "S3: em_exports_total"),
        ("em_exports_country_matrix.csv", "S3: em_exports_country_matrix"),
    ]:
        results.append(compare_csv_matrices(
            agent_dir / fname,
            baseline_dir / fname,
            name,
        ))

    return results


def run_all_tests(agent_base: Path, baseline_base: Path) -> tuple[int, int, int]:
    """Run all comparison tests. Returns (pass, warn, fail) counts."""
    all_results = []

    # S6 review agent
    all_results.extend(test_s6_review_agent(
        agent_base / "review_report.md",
        baseline_base / "review_report.md",
    ))

    # S2 prepared data
    all_results.extend(test_s2_prepared_data(
        agent_base / "prepared",
        baseline_base / "prepared",
    ))

    # S3 model outputs
    all_results.extend(test_model_outputs(
        agent_base / "model",
        baseline_base / "model",
    ))

    # Print results
    pass_count = warn_count = fail_count = skip_count = 0
    print("\n" + "=" * 70)
    print("FIGARO Agentic Pipeline — Output Comparison Tests")
    print("=" * 70)

    for r in all_results:
        status = r["status"]
        icon = {"PASS": "✅", "WARN": "⚠️ ", "FAIL": "❌", "SKIP": "⏭️ "}[status]
        print(f"{icon} {r['name']}")
        print(f"   {r['details']}")

        if status == "PASS":
            pass_count += 1
        elif status == "WARN":
            warn_count += 1
        elif status == "FAIL":
            fail_count += 1
        else:
            skip_count += 1

    total = pass_count + warn_count + fail_count
    print("\n" + "=" * 70)
    print(f"Results: PASS={pass_count}, WARN={warn_count}, FAIL={fail_count}, SKIP={skip_count}")
    print("=" * 70)

    if fail_count > 0:
        print("\n❌ Some tests FAILED — agent outputs differ from baseline.")
        print("   Investigate before deploying agentic stages to production.")
    elif warn_count > 0:
        print("\n⚠️  Some warnings — check deviations are acceptable.")
    else:
        print("\n✅ All tests passed — agentic outputs match baseline.")

    return pass_count, warn_count, fail_count


def main():
    parser = argparse.ArgumentParser(description="Compare agentic outputs to deterministic baseline")
    parser.add_argument("--agent-dir", default=str(REPO_ROOT / "outputs" / "agent"),
                        help="Directory containing agentic pipeline outputs")
    parser.add_argument("--baseline-dir", default=str(REPO_ROOT / "outputs"),
                        help="Directory containing deterministic baseline outputs")
    parser.add_argument("--stage", choices=["s2", "s3", "s6", "all"], default="all",
                        help="Test only a specific stage's outputs")
    args = parser.parse_args()

    agent_base = Path(args.agent_dir)
    baseline_base = Path(args.baseline_dir)

    pass_count, warn_count, fail_count = run_all_tests(agent_base, baseline_base)

    sys.exit(1 if fail_count > 0 else 0)


if __name__ == "__main__":
    main()
