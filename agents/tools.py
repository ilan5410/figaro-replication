"""
Shared tool definitions for FIGARO pipeline agents.

Tool set (minimal by design — see FIGARO_AGENT_WORST_PRACTICES.md §2.1):
  - execute_python   : Write code to a temp .py file, run it, return stdout/stderr
  - read_file        : Read file contents with smart truncation
  - write_file       : Write/overwrite a file
  - list_directory   : List directory contents with sizes

All tools run with the repo root as the working directory.
"""
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path

from langchain_core.tools import tool

log = logging.getLogger("figaro.tools")

# Repo root — all relative paths in agent scripts are relative to this
REPO_ROOT = Path(__file__).parent.parent


# ─── execute_python ─────────────────────────────────────────────────────────

def make_execute_python(timeout: int = 300, stage_name: str = "unknown"):
    """
    Factory that returns an execute_python tool with a specific timeout.

    Args:
        timeout: Maximum execution time in seconds.
                 S1 downloads need 2400; S2 needs 600; others 300.
        stage_name: For logging only.
    """
    @tool
    def execute_python(code: str) -> dict:
        """
        Write Python code to a temporary .py file, execute it, and return
        the stdout/stderr. Use print() for anything you want to inspect.

        The script runs with the repo root as the working directory, so
        relative paths like 'data/raw/employment_2010.csv' work correctly.

        Returns a dict with keys:
          stdout     - Last 5000 chars of standard output
          stderr     - Last 2000 chars of standard error
          returncode - 0 = success, non-zero = failure
          script_path - Path to the saved script (read it back for debugging)
        """
        scripts_dir = REPO_ROOT / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)

        ts = time.strftime("%Y%m%d_%H%M%S_%f")
        script_path = scripts_dir / f"tmp_{stage_name}_{ts}.py"

        with open(script_path, "w") as f:
            f.write(code)

        log.info(f"[{stage_name}] Executing {script_path}")

        try:
            result = subprocess.run(
                ["python3", str(script_path)],
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(REPO_ROOT),
            )
            stdout = result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout
            stderr = result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr

            if result.returncode != 0:
                log.warning(f"[{stage_name}] Script failed (rc={result.returncode})")
                log.warning(f"  stderr: {stderr[:500]}")
            else:
                log.info(f"[{stage_name}] Script succeeded")

            return {
                "stdout": stdout,
                "stderr": stderr,
                "returncode": result.returncode,
                "script_path": str(script_path),
            }

        except subprocess.TimeoutExpired:
            log.error(f"[{stage_name}] Script timed out after {timeout}s")
            return {
                "stdout": "",
                "stderr": f"TIMEOUT: Script exceeded {timeout}s limit.",
                "returncode": -1,
                "script_path": str(script_path),
            }
        except Exception as exc:
            log.error(f"[{stage_name}] Unexpected error: {exc}")
            return {
                "stdout": "",
                "stderr": f"ERROR: {exc}",
                "returncode": -2,
                "script_path": str(script_path),
            }

    execute_python.__name__ = f"execute_python_{stage_name}"
    return execute_python


# ─── read_file ───────────────────────────────────────────────────────────────

@tool
def read_file(path: str, max_chars: int = 5000) -> dict:
    """
    Read the contents of a file. Paths are relative to the repo root.

    For large files (CSV, parquet), returns shape/dtype/head info rather than
    the raw contents to avoid flooding the context window.

    Returns a dict with keys:
      content      - File contents (or summary for large files)
      size_bytes   - File size on disk
      exists       - True if file exists
      error        - Error message if something went wrong (else None)
    """
    full_path = REPO_ROOT / path if not Path(path).is_absolute() else Path(path)

    if not full_path.exists():
        available = []
        parent = full_path.parent
        if parent.exists():
            available = [str(p.name) for p in sorted(parent.iterdir())[:20]]
        return {
            "content": None,
            "size_bytes": 0,
            "exists": False,
            "error": (
                f"File not found: {path}. "
                f"Available in {parent.name}/: {available}"
            ),
        }

    size = full_path.stat().st_size

    # For large CSV files, return a smart summary
    if full_path.suffix in (".csv", ".tsv") and size > 50_000:
        try:
            import pandas as pd
            df = pd.read_csv(full_path, nrows=5)
            full_df = pd.read_csv(full_path)
            summary = (
                f"Shape: {full_df.shape}\n"
                f"Columns: {list(full_df.columns)}\n"
                f"dtypes: {dict(full_df.dtypes.astype(str))}\n"
                f"Head (5 rows):\n{df.to_string()}"
            )
            return {"content": summary[:max_chars], "size_bytes": size, "exists": True, "error": None}
        except Exception as e:
            pass  # Fall through to plain text read

    # For parquet files
    if full_path.suffix == ".parquet":
        try:
            import pandas as pd
            df = pd.read_parquet(full_path)
            summary = (
                f"Shape: {df.shape}\n"
                f"Columns: {list(df.columns)}\n"
                f"dtypes: {dict(df.dtypes.astype(str))}\n"
                f"Head (5 rows):\n{df.head().to_string()}"
            )
            return {"content": summary[:max_chars], "size_bytes": size, "exists": True, "error": None}
        except Exception as e:
            return {"content": None, "size_bytes": size, "exists": True, "error": str(e)}

    # Plain text / JSON / markdown
    try:
        text = full_path.read_text(encoding="utf-8", errors="replace")
        if len(text) > max_chars:
            text = text[:max_chars] + f"\n... [truncated, total {len(text)} chars]"
        return {"content": text, "size_bytes": size, "exists": True, "error": None}
    except Exception as e:
        return {"content": None, "size_bytes": size, "exists": True, "error": str(e)}


# ─── write_file ──────────────────────────────────────────────────────────────

@tool
def write_file(path: str, content: str) -> dict:
    """
    Write content to a file. Paths are relative to the repo root.
    Creates parent directories as needed.

    Use this for small files: config, metadata JSON, summary text files,
    markdown reports. For large data (matrices, CSVs), use execute_python
    to write them from pandas.

    Returns a dict with keys:
      success    - True if written successfully
      path       - Absolute path written to
      size_bytes - Number of bytes written
      error      - Error message if failed (else None)
    """
    full_path = REPO_ROOT / path if not Path(path).is_absolute() else Path(path)
    full_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        full_path.write_text(content, encoding="utf-8")
        size = full_path.stat().st_size
        log.info(f"Wrote {size} bytes to {full_path}")
        return {"success": True, "path": str(full_path), "size_bytes": size, "error": None}
    except Exception as e:
        log.error(f"write_file failed for {path}: {e}")
        return {"success": False, "path": str(full_path), "size_bytes": 0, "error": str(e)}


# ─── list_directory ──────────────────────────────────────────────────────────

@tool
def list_directory(path: str = ".") -> dict:
    """
    List files and subdirectories in a directory. Paths are relative to the
    repo root. Returns file names with sizes (in bytes).

    Returns a dict with keys:
      entries    - List of {name, size_bytes, is_dir} dicts (up to 100)
      total      - Total number of entries (before cap)
      error      - Error message if failed (else None)
    """
    full_path = REPO_ROOT / path if not Path(path).is_absolute() else Path(path)

    if not full_path.exists():
        return {"entries": [], "total": 0, "error": f"Directory not found: {path}"}
    if not full_path.is_dir():
        return {"entries": [], "total": 0, "error": f"Not a directory: {path}"}

    try:
        items = sorted(full_path.iterdir())
        entries = []
        for item in items[:100]:
            try:
                size = item.stat().st_size if item.is_file() else 0
            except OSError:
                size = 0
            entries.append({
                "name": item.name,
                "size_bytes": size,
                "is_dir": item.is_dir(),
            })
        return {"entries": entries, "total": len(items), "error": None}
    except Exception as e:
        return {"entries": [], "total": 0, "error": str(e)}


# ─── Tool sets per stage ─────────────────────────────────────────────────────

def get_tools_for_stage(stage: str, timeout: int = 300) -> list:
    """Return the tool list for a given stage."""
    exec_py = make_execute_python(timeout=timeout, stage_name=stage)
    return [exec_py, read_file, write_file, list_directory]
