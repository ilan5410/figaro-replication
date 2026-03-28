# Architecture: FIGARO Agentic IO Analysis Pipeline

## Overview

The FIGARO agentic pipeline refactors the original deterministic 6-stage
pipeline into a hybrid LangGraph + Anthropic API system. Agentic stages handle
tasks requiring judgment and adaptation; deterministic stages handle pure
linear algebra where correctness must be exact.

## Directory Structure

```
figaro-replication/
├── agents/
│   ├── __init__.py
│   ├── orchestrator.py          # LangGraph StateGraph definition
│   ├── run_agentic.py           # Entry point
│   ├── state.py                 # PipelineState TypedDict
│   ├── tools.py                 # Shared tool definitions
│   ├── prompts/
│   │   ├── data_acquisition.md  # S1 agent system prompt
│   │   ├── data_preparation.md  # S2 agent system prompt
│   │   ├── output_generation.md # S5 agent system prompt
│   │   └── review.md            # S6 agent system prompt
│   └── nodes/
│       ├── s1_data_acquisition.py   # Agent node
│       ├── s2_data_preparation.py   # Agent node
│       ├── s3_model_construction.py # Deterministic node
│       ├── s4_decomposition.py      # Deterministic node
│       ├── s5_output_generation.py  # Agent node
│       ├── s6_review.py             # Agent node
│       └── validators.py            # Deterministic post-checks
├── docs/
│   ├── architecture.md          # This document
│   └── feasibility.md           # Feasibility assessment
├── src/                         # Original deterministic pipeline (ground truth)
├── tests/
│   └── compare_outputs.py       # Agent vs. baseline comparison tests
└── scripts/                     # Temp .py files written by execute_python tool
```

## Pipeline Flow

```
START
  │
  ▼
[S1] Data Acquisition Agent
  │  ↳ Downloads FIGARO IC-IOT + employment from Eurostat
  │  ↳ Tools: execute_python (2400s timeout), read_file, write_file, list_directory
  │
  ▼
[GATE] validate_stage1 (deterministic)
  │  ↳ Checks: file existence, row counts, EU-28 country presence
  │
  ├─ FAIL ──→ human_escalation → END
  │
  ▼
[S2] Data Preparation Agent
  │  ↳ Parses IC-IOT → Z_EU (1792×1792), e_nonEU, x_EU, Em_EU
  │  ↳ Tools: execute_python (600s timeout), read_file, write_file, list_directory
  │
  ▼
[GATE] validate_stage2 (deterministic)
  │  ↳ Checks: Z shape = (1792,1792), Em sum ≈ 225,677, no negatives
  │
  ├─ FAIL ──→ human_escalation → END
  │
  ▼
[S3] Model Construction (DETERMINISTIC — no LLM)
  │  ↳ A = Z·diag(x)^{-1}, L = (I-A)^{-1}, d = diag(x)^{-1}·Em
  │  ↳ Calls src/stage3_model_construction.py directly
  │
  ▼
[S4] Decomposition (DETERMINISTIC — no LLM)
  │  ↳ Domestic/spillover/direct/indirect block decomposition
  │  ↳ Calls src/stage4_decomposition.py directly
  │
  ▼
[GATE] validate_stage3 (deterministic)
  │  ↳ Checks: A col sums < 1, L diagonal ≥ 1, export employment reasonable
  │
  ├─ FAIL ──→ human_escalation → END
  │
  ▼
[S5] Output Generation Agent
  │  ↳ Produces tables + figures (matplotlib, pandas, openpyxl)
  │  ↳ Tools: execute_python (300s timeout), read_file, write_file, list_directory
  │
  ▼
[S6] Review Agent
  │  ↳ Verifies all outputs, compares to paper benchmarks
  │  ↳ Produces outputs/review_report.md
  │  ↳ Tools: execute_python (300s timeout), read_file, write_file, list_directory
  │
  ├─ FAIL + human needed ──→ human_escalation → END
  │
  ▼
  END (review_report.md produced)
```

## Key Design Decisions

### 1. File paths, not data, in state

The `PipelineState` TypedDict contains file paths to intermediate outputs,
NOT the matrices themselves. A 1792×1792 float64 matrix is 25MB — it must not
be serialized into LangGraph state.

```python
# RIGHT
state["prepared_paths"]["Z_EU"] = "data/prepared/Z_EU.csv"

# WRONG
state["Z_EU"] = Z.tolist()  # 3.2M floats in JSON state
```

### 2. execute_python is the core tool

All agents write Python scripts to `scripts/tmp_{stage}_{timestamp}.py` and
execute them via subprocess. This:
- Keeps the agent's context clean (no raw data in context)
- Preserves scripts for debugging
- Allows timeout control per stage
- Enables truncated stdout (agent sees last 5000 chars only)

### 3. Deterministic validators are mandatory

After each agentic stage, a deterministic function validates outputs with hard
numerical assertions. Agent self-evaluation is supplementary, not sufficient.

### 4. SQLite checkpointing

LangGraph's SQLite checkpointer allows resuming from any stage. Critical for
Stage 1's 30-minute downloads: if Stage 2 fails, restart from Stage 2 without
re-downloading.

### 5. Agent prompts are stage-specific

Each agent receives only the context relevant to its stage (100-200 lines),
not the full 648-line specification. This keeps per-call token costs low
and attention focused.

## Tool Architecture

```python
# One tool factory per stage (different timeouts)
tools_s1 = get_tools_for_stage("s1_acquisition", timeout=2400)  # 40 min
tools_s2 = get_tools_for_stage("s2_preparation", timeout=600)   # 10 min
tools_s5 = get_tools_for_stage("s5_output", timeout=300)        # 5 min
tools_s6 = get_tools_for_stage("s6_review", timeout=300)        # 5 min
```

All tool sets include: `execute_python`, `read_file`, `write_file`, `list_directory`.

## Generalization Pathway

The architecture is designed for generic IO analysis, not just FIGARO 2010:

1. **New year**: Change `reference_year` in config.yaml. Agent prompts are
   parameterized — same agents, same code.

2. **New satellite account**: Replace `nama_10_a64_e` with the relevant
   Eurostat table in the S1 agent prompt. S3/S4 math is unchanged.

3. **New IO table** (WIOD, OECD ICIO): New S1 agent with different API
   knowledge; S2 agent with different structural parsing; S7 concordance
   agent for classification mapping.

4. **S7 Concordance Agent** (future): Builds CPA↔NACE, NACE↔ISIC, and other
   classification bridge matrices by reading Eurostat RAMON correspondence
   tables. This is the key to full generalization.

## Testing Strategy

Run `python tests/compare_outputs.py` after any agentic stage change.
The deterministic pipeline (`python run_pipeline.py`) is the ground truth.
All agent outputs must match within `rtol=1e-4, atol=1e-6`.

See `docs/feasibility.md` for full testing protocol and reliability estimates.
