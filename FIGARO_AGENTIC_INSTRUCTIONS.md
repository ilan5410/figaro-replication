# FIGARO Agentic Replication — Claude Code Instructions

## Context

You are working on `figaro-replication` (https://github.com/ilan5410/figaro-replication), an existing Python pipeline that replicates Rémond-Tiedrez et al. (2019) — computing the employment content of EU exports using FIGARO input-output tables. The pipeline currently works end-to-end as hardcoded Python across 6 stages.

**Your mission**: Refactor this pipeline so that LLM agents (via LangGraph + Anthropic API) replace the stages where agentic automation is feasible. The end goal is a generic multi-agent system for input-output analysis, built incrementally.

Read `figaro_replication_instructions.md` and `CLAUDE.md` in the repo before starting. They contain the full mathematical specification and current architecture.

---

## 1. Project goals (in priority order)

1. **Identify and replace** pipeline stages (or sub-steps) with LangGraph agents that write and execute code autonomously, with human review of outputs only.
2. **Produce a feasibility assessment** — a clear document (`docs/feasibility.md`) explaining what's agentic-ready, what's borderline, and what must stay hardcoded, with rationale.
3. **Build toward generality** — the agentic architecture should be designed so it can later handle other IO analyses (different years, different country sets, different satellite accounts), not just this one paper replication.

---

## 2. Current pipeline — stage-by-stage feasibility analysis

Below is the assessment you should use as your starting framework. Adjust based on what you find in the code.

### Stage 1: Data Acquisition — GOOD CANDIDATE for agents

**Current implementation**: Hardcoded Eurostat API calls with retry logic, downloading IC-IOT and employment data.

**Why agentic works here**:
- The task is "given a Eurostat table code and filters, write a Python script to download the data via the JSON API, handle pagination, retries, and save to CSV."
- An LLM can reason about API documentation, construct query parameters, handle errors, and adapt if the API schema changes.
- This is the stage most likely to break over time (URL changes, API versioning, new data vintages), so an agent that can read docs and adapt is genuinely more robust than hardcoded URLs.

**Agent design**:
- **Role**: `data_acquisition_agent`
- **Input**: A structured request: `{table_code: "naio_10_fcp_ip1", filters: {unit: "MIO_EUR", ...}, reference_year: 2010, output_path: "data/raw/"}`
- **Tools available**: `execute_python` (write and run a .py file), `web_fetch` (read Eurostat API docs or metadata endpoints), `file_write`, `file_read`
- **Expected behavior**: Agent reads the Eurostat API docs if needed, writes a download script, executes it, verifies row counts, logs discrepancies.
- **Guardrails**: Must produce a `data_summary.txt` with dimensions and checksums. Orchestrator validates file existence and minimum row counts before proceeding.

**Risk level**: LOW. Eurostat APIs are well-documented; the agent can fall back to asking the human for a URL.

---

### Stage 2: Data Preparation — PARTIAL CANDIDATE

**Current implementation**: Parses 11M rows from the IC-IOT, extracts Z matrix (EU-only), export vector, employment vector, ensures consistent ordering.

**Why partially agentic**:
- The *logic* is well-defined mathematically (extract Z^EU, compute e, align ordering).
- An LLM can write the pandas code to reshape and filter — this is standard data wrangling.
- BUT: the FIGARO file structure has specific quirks (CPA product codes in `prd_ava`/`prd_use`, value-added rows, final demand columns) that require precise structural knowledge. An agent that misaligns countries or industries will produce silently wrong results.

**Recommended split**:
- **Agentic sub-task** (code generation): Given a schema description of the raw data, write the reshaping/filtering code. Agent can iterate by inspecting column headers, checking dimensions.
- **Hardcoded/templated sub-task** (validation): The ordering alignment check and dimension assertions should be deterministic — the agent writes the code but the validation logic is a fixed contract.

**Agent design**:
- **Role**: `data_preparation_agent`
- **Input**: `{raw_data_paths: [...], config: {...}, expected_dimensions: {Z: [1792, 1792], e: [1792, 1], ...}}`
- **Tools**: `execute_python`, `file_read` (inspect headers of raw CSVs), `file_write`
- **Mandatory post-check (deterministic, not agentic)**:
  - Z_EU is 1792×1792
  - Employment vector is 1792×1
  - Country ordering in Z matches employment vector
  - No negative values in Z, e, Em
  - Total employment ≈ 225,677 ths (±5%)

**Risk level**: MEDIUM. Matrix alignment is the critical failure mode. The deterministic post-checks are essential.

---

### Stage 3: Model Construction — POOR CANDIDATE (keep hardcoded)

**Current implementation**: Computes A = Z·diag(x)^{-1}, L = (I-A)^{-1}, d = diag(x)^{-1}·Em.

**Why NOT agentic**:
- This is pure linear algebra: matrix inversion, diagonal operations. There is no ambiguity, no judgment call, no adaptation needed.
- An LLM generating numpy code for `np.linalg.inv(I - A)` adds latency, cost, and hallucination risk for zero benefit. The code is 30 lines and will never need to change.
- The mathematical operations are identical for any IO analysis — this is already generic.

**Recommendation**: Keep as deterministic Python. Wrap it in a clean function with a stable interface so the orchestrator can call it. No agent needed.

**One exception — make it a tool**: Expose `build_leontief_model(Z, x, Em) -> (A, L, d)` as a **tool** that agents in other stages can call, rather than reimplementing.

**Risk level if made agentic**: HIGH. An agent that makes a subtle error in matrix algebra (e.g., wrong axis in diag(), transposition error) would corrupt all downstream results silently.

---

### Stage 4: Decomposition — POOR CANDIDATE (keep hardcoded)

**Current implementation**: Block-matrix operations to decompose domestic/spillover/direct/indirect effects.

**Same logic as Stage 3**: This is deterministic linear algebra with no ambiguity. The 28×28 block extraction and summation is mechanical. Making it agentic adds risk with no benefit.

**Recommendation**: Keep hardcoded. Expose as a tool: `decompose_employment(L, d, e, country_list) -> decomposition_dict`.

---

### Stage 5: Output Generation — GOOD CANDIDATE for agents

**Current implementation**: matplotlib charts and CSV/Excel table formatting.

**Why agentic works here**:
- "Produce a bar chart with two series per country, ordered by X, with this color scheme" is exactly the kind of task LLMs excel at.
- The output format may need to change (different chart styles, client-specific branding, additional annotations) — an agent can adapt without code changes.
- This stage is purely about presentation; errors are visible and non-catastrophic (a wrong axis label doesn't corrupt the analysis).

**Agent design**:
- **Role**: `output_generation_agent`
- **Input**: Decomposition results (dataframes), output specs from config (which tables/figures to produce, styling preferences).
- **Tools**: `execute_python` (matplotlib/seaborn), `file_write`, `file_read`
- **Expected outputs**: PNG/PDF figures in `outputs/figures/`, CSV + Excel tables in `outputs/tables/`
- **Quality check**: Agent should open and inspect its own outputs (read back the CSV, check image file size > 0).

**Risk level**: LOW. Visual outputs are human-reviewed anyway.

---

### Stage 6: Review Agent — EXCELLENT CANDIDATE

**Current implementation**: Already designed as a "review agent" — runs validation checks against known benchmarks.

**Why agentic works here**:
- This is the most natural agent task: "inspect these outputs, compare against expected values, write a diagnostic report."
- An LLM can provide richer, more contextual analysis than hardcoded threshold checks — e.g., "Luxembourg's spillover share is 42% vs expected 46.7%; this 10% deviation likely reflects the product-by-product vs industry-by-industry table difference."
- The agent can also flag unexpected patterns that hardcoded checks wouldn't catch.

**Agent design**:
- **Role**: `review_agent`
- **Input**: All intermediate and final outputs, benchmark values from config/paper.
- **Tools**: `execute_python` (load and compare data), `file_read`, `file_write`
- **Output**: `outputs/review_report.md` with PASS/WARN/FAIL per check plus narrative interpretation.
- **Escalation**: If any FAIL, the agent should set a flag that stops the pipeline and alerts the human.

**Risk level**: LOW. The review agent's job is to catch errors, and its output is always human-reviewed.

---

## 3. Target architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    LangGraph Orchestrator                     │
│                  (StateGraph, checkpointing)                 │
├──────────────┬──────────────┬──────────────┬─────────────────┤
│              │              │              │                 │
│  ┌───────┐   │  ┌───────┐   │  ┌───────┐   │  ┌───────┐     │
│  │ Agent │   │  │ Agent │   │  │ DETER │   │  │ Agent │     │
│  │  S1   │   │  │  S2   │   │  │ S3+S4 │   │  │  S5   │     │
│  │ Data  │──▶│  │ Data  │──▶│  │ Model │──▶│  │Output │     │
│  │ Acq.  │   │  │ Prep  │   │  │ +Decom│   │  │ Gen   │     │
│  └───────┘   │  └───────┘   │  └───────┘   │  └───────┘     │
│              │              │              │        │        │
│              │              │              │  ┌─────▼─────┐  │
│              │              │              │  │   Agent    │  │
│              │              │              │  │   S6       │  │
│              │              │              │  │  Review    │  │
│              │              │              │  └───────────┘  │
└──────────────────────────────────────────────────────────────┘

Legend:
  Agent = LangGraph node backed by Claude (tool-calling)
  DETER = Deterministic Python function (no LLM call)
```

### LangGraph state schema

```python
from typing import TypedDict, Optional
from langgraph.graph import StateGraph

class PipelineState(TypedDict):
    config: dict                    # Parsed config.yaml
    stage: int                      # Current stage number
    
    # Stage 1 outputs
    raw_data_paths: Optional[dict]  # {"iciot": "data/raw/...", "employment": "data/raw/..."}
    data_summary: Optional[str]     # Verification summary
    
    # Stage 2 outputs
    prepared_paths: Optional[dict]  # {"Z_EU": "...", "e": "...", "Em": "...", ...}
    preparation_valid: Optional[bool]
    
    # Stage 3+4 outputs (deterministic)
    model_paths: Optional[dict]     # {"A": "...", "L": "...", "d": "..."}
    decomposition_paths: Optional[dict]
    
    # Stage 5 outputs
    output_paths: Optional[dict]    # {"figures": [...], "tables": [...]}
    
    # Stage 6 outputs
    review_report_path: Optional[str]
    review_passed: Optional[bool]
    
    # Error handling
    errors: list[str]
    human_intervention_needed: bool
```

### Key design decisions

1. **Each agent node gets**: the current state, a set of tools, and a system prompt describing its role + the mathematical/methodological context it needs.

2. **Tools shared across agents**:
   - `execute_python(code: str) -> stdout/stderr` — write a temp .py file, run it, return output
   - `read_file(path: str) -> contents`
   - `write_file(path: str, contents: str)`
   - `list_directory(path: str) -> list[str]`

3. **Deterministic nodes** (S3, S4): These are plain Python functions registered as LangGraph nodes. They take state, run the computation, update state. No LLM call.

4. **Conditional edges**: After S2, a validation gate checks `preparation_valid`. If False, routes to a `human_intervention` node. After S6, checks `review_passed` — if False, routes back to S2 (retry) or to human intervention.

---

## 4. Implementation plan — phased

### Phase 0: Setup (do this first)

```
Tasks:
  - [ ] Clone the repo, verify the existing pipeline runs end-to-end
  - [ ] Create a new branch: `feature/agentic-refactor`
  - [ ] Set up dependencies: `pip install langgraph langchain-anthropic`
  - [ ] Create the project structure (see below)
  - [ ] Write `docs/feasibility.md` based on Section 2 above, adjusted for
        what you find in the actual code
```

**New project structure** (additions to existing repo):

```
figaro-replication/
├── agents/
│   ├── __init__.py
│   ├── orchestrator.py          # LangGraph StateGraph definition
│   ├── state.py                 # PipelineState TypedDict
│   ├── tools.py                 # Shared tool definitions
│   ├── prompts/
│   │   ├── data_acquisition.md  # System prompt for S1 agent
│   │   ├── data_preparation.md  # System prompt for S2 agent
│   │   ├── output_generation.md # System prompt for S5 agent
│   │   └── review.md            # System prompt for S6 agent
│   ├── nodes/
│   │   ├── s1_data_acquisition.py   # Agent node
│   │   ├── s2_data_preparation.py   # Agent node
│   │   ├── s3_model_construction.py # Deterministic node (wraps existing code)
│   │   ├── s4_decomposition.py      # Deterministic node (wraps existing code)
│   │   ├── s5_output_generation.py  # Agent node
│   │   ├── s6_review.py             # Agent node
│   │   └── validators.py           # Deterministic post-checks
│   └── run_agentic.py           # Entry point
├── docs/
│   ├── feasibility.md           # Feasibility assessment
│   └── architecture.md          # Architecture decisions
├── src/                          # Existing pipeline (keep as reference/fallback)
│   ├── stage1_data_acquisition.py
│   ├── ...
```

### Phase 1: Review Agent (S6) — start here

**Rationale**: The review agent is the highest-value, lowest-risk starting point. It doesn't touch the computational pipeline, its output is always human-reviewed, and you can benchmark it against the existing hardcoded review.

```
Tasks:
  - [ ] Extract the benchmark values and check logic from existing stage6_review_agent.py
  - [ ] Write the system prompt (agents/prompts/review.md) giving the agent:
        - The paper's benchmark values
        - The mathematical identities that must hold
        - Instructions to load intermediate outputs, run checks, write report
  - [ ] Implement the agent node using LangGraph + tool-calling
  - [ ] Test: run the existing pipeline stages 1-5, then run the agentic S6
  - [ ] Compare: does the agentic review report catch the same issues as the 
        hardcoded one? Does it catch more? Does it hallucinate problems?
  - [ ] Document findings in docs/feasibility.md
```

### Phase 2: Output Generation Agent (S5)

```
Tasks:
  - [ ] Write the system prompt describing each figure/table specification
  - [ ] Implement agent node — the agent receives decomposition results and 
        produces charts/tables
  - [ ] Test against existing outputs: visual comparison
  - [ ] Measure: token cost per run, latency, output quality
```

### Phase 3: Data Acquisition Agent (S1)

```
Tasks:
  - [ ] Write the system prompt with Eurostat API documentation
  - [ ] Implement agent node with web_fetch and execute_python tools
  - [ ] Test: does the agent successfully download the same data as the 
        hardcoded script?
  - [ ] Stress test: change the reference year to 2013 — does it adapt?
  - [ ] Edge case: what if the API returns an error? Does the agent retry 
        or escalate?
```

### Phase 4: Data Preparation Agent (S2)

```
Tasks:
  - [ ] Write the system prompt with FIGARO structural documentation
  - [ ] Implement agent node + deterministic validator
  - [ ] Test: compare prepared matrices against existing pipeline outputs
        (element-wise numerical comparison, tolerance < 1e-6)
  - [ ] This is the riskiest agent — document failure modes carefully
```

### Phase 5: Orchestrator Integration

```
Tasks:
  - [ ] Wire all nodes into the LangGraph StateGraph
  - [ ] Implement conditional edges (validation gates, human escalation)
  - [ ] Add LangGraph checkpointing (SQLite) so runs can be resumed
  - [ ] End-to-end test: full pipeline, agentic mode
  - [ ] Compare outputs against the deterministic pipeline
```

### Phase 6: Generalization

```
Tasks:
  - [ ] Parameterize agent prompts so they work for any reference year
  - [ ] Test with 2011, 2012, 2013
  - [ ] Abstract the satellite account (employment → energy, CO2, etc.)
  - [ ] Write a config schema for "IO analysis requests" that the 
        orchestrator can consume generically
  - [ ] Document the generalization pathway in docs/architecture.md
```

---

## 5. Agent system prompts — guidelines

Each agent's system prompt should include:

1. **Role description**: What this agent does, in one paragraph.
2. **Mathematical context**: The relevant formulae and definitions (copy from `figaro_replication_instructions.md`).
3. **Input specification**: What files/state the agent receives.
4. **Output specification**: What files the agent must produce, with exact naming and format.
5. **Quality checks**: What the agent should verify about its own work before declaring success.
6. **Failure protocol**: When to retry, when to escalate to human.
7. **Style constraints**: "Write multi-line logic as script files, then execute them. Do not write inline bash one-liners."

**Critical**: Do NOT put the full 648-line instruction doc into every agent's prompt. Each agent should get only the context relevant to its stage. The review agent gets the most context (benchmark values, all identities).

---

## 6. Tool implementation — `execute_python`

This is the most important tool. Implement it carefully:

```python
import subprocess
import tempfile
from pathlib import Path

def execute_python(code: str, timeout: int = 300) -> dict:
    """Write code to a temp file, execute it, return stdout/stderr."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', 
                                       dir='scripts/', delete=False) as f:
        f.write(code)
        f.flush()
        try:
            result = subprocess.run(
                ['python3', f.name],
                capture_output=True, text=True, timeout=timeout,
                cwd=str(Path(__file__).parent.parent)  # repo root
            )
            return {
                "stdout": result.stdout[-5000:],  # truncate
                "stderr": result.stderr[-2000:],
                "returncode": result.returncode,
                "script_path": f.name
            }
        except subprocess.TimeoutExpired:
            return {"stdout": "", "stderr": "TIMEOUT", "returncode": -1, 
                    "script_path": f.name}
```

**Guardrails**:
- Timeout of 300s (Stage 1 downloads can take 30 min — increase for that agent specifically, or have the agent handle chunked downloads with intermediate saves).
- Truncate stdout to avoid blowing up the context window.
- Log every script execution with timestamp and script content.

---

## 7. Cost and performance expectations

| Stage | Est. tokens/run | Est. cost (Claude Sonnet) | Latency |
|-------|-----------------|---------------------------|---------|
| S1 (acquisition) | ~20K | ~$0.10 | 30-40 min (download-bound) |
| S2 (preparation) | ~30K | ~$0.15 | 5-10 min |
| S3+S4 (deterministic) | 0 | $0.00 | ~15s |
| S5 (output gen) | ~25K | ~$0.12 | 2-5 min |
| S6 (review) | ~15K | ~$0.08 | 1-2 min |
| **Total** | **~90K** | **~$0.45** | **~40 min** |

Compare to the existing deterministic pipeline: ~35 min, $0.00. The agentic version adds ~$0.50 per run and ~5-10 min of LLM overhead. This is acceptable for a proof-of-concept but should be optimized for production (cache agent outputs, skip agents when inputs haven't changed).

---

## 8. What NOT to make agentic — firm boundaries

1. **Matrix algebra (S3, S4)**: Never. The Leontief inverse computation, employment coefficient calculation, and block decomposition are deterministic, well-tested, and must be numerically exact. An agent that introduces a transposition error or wrong axis produces silently catastrophic results.

2. **Data alignment/ordering**: The mapping from CPA product codes to matrix row/column indices must be deterministic. An agent can *write* the mapping code, but the mapping itself must be validated against a fixed reference.

3. **Config parsing**: Reading `config.yaml` is trivial and deterministic. Don't add LLM overhead.

4. **File I/O plumbing**: Moving files between directories, creating folders, etc. Keep as plain Python.

---

## 9. Testing strategy

For each agentic stage, maintain a **parallel deterministic baseline**:

```python
# In the test suite
def test_s1_agent_matches_baseline():
    """Agent-downloaded data should match hardcoded download."""
    agent_data = pd.read_csv("data/raw/agent/employment_2010.csv")
    baseline_data = pd.read_csv("data/raw/baseline/employment_2010.csv")
    pd.testing.assert_frame_equal(agent_data, baseline_data)

def test_s6_agent_catches_known_issues():
    """Agent review should flag all issues the hardcoded review flags."""
    agent_report = Path("outputs/agent/review_report.md").read_text()
    for check in ["LU_missing_employment", "product_vs_industry_deviation"]:
        assert check_mentioned(agent_report, check)
```

Run these tests after every agent implementation change. The deterministic pipeline is your ground truth.

---

## 10. Reporting deliverable — `docs/feasibility.md`

After completing Phase 1 (minimum) or all phases, produce this document:

```markdown
# Feasibility Assessment: Agentic IO Analysis Pipeline

## Executive Summary
[2-3 sentences: what works, what doesn't, recommendation]

## Stage-by-Stage Assessment

### Stage 1: Data Acquisition
- Feasibility: HIGH / MEDIUM / LOW
- Agent reliability: X/10 runs succeeded without intervention
- Token cost: X per run
- Comparison to hardcoded: [faster/slower, more/less robust]
- Recommendation: [replace / augment / keep hardcoded]

[...repeat for each stage...]

## Generalization Potential
- Can this architecture handle other IO analyses? [yes/partially/no]
- What would need to change for: different year? different satellite? 
  different country set?

## Cost-Benefit Analysis
- Total agentic pipeline cost: $X/run
- Maintenance benefit: [agent adapts to API changes vs. manual code fixes]
- Risk: [what can go wrong, mitigation]

## Recommendation for Team
[Clear recommendation on next steps]
```

---

## 11. Commands reference

```bash
# Existing pipeline (baseline)
python run_pipeline.py --config config.yaml

# Agentic pipeline (once built)
python agents/run_agentic.py --config config.yaml

# Run a single agentic stage
python agents/run_agentic.py --config config.yaml --stage 6

# Compare outputs
python tests/compare_outputs.py --agent outputs/agent/ --baseline outputs/

# Generate feasibility report
python agents/run_agentic.py --config config.yaml --report-only
```

---

## 12. Environment setup

```bash
# Python dependencies
pip install langgraph langchain-anthropic langchain-core \
            pyyaml numpy pandas matplotlib openpyxl

# Anthropic API key
export ANTHROPIC_API_KEY="sk-ant-..."

# Verify
python -c "from langchain_anthropic import ChatAnthropic; print('OK')"
```

---

## 13. Future Stage 7: Concordance / Classification Mapping Agent

### 13.1 Why this matters

The most time-consuming desk research in IO analysis is **classification
concordance** — building the mapping between different industry/product
taxonomies. In the current pipeline, the mapping from CPA product codes
(used in the FIGARO IC-IOT) to NACE industry codes (used in employment
data) is implicit and relatively straightforward at the A*64 level, because
CPA and NACE are aligned by construction. But as soon as you generalize —
different IO tables, different satellite accounts, different country sets
— the mapping problem explodes:

- **CPA ↔ NACE**: Aligned at 2-digit, diverge below. The current pipeline
  handles this implicitly — a concordance agent would make it explicit.
- **NACE Rev. 2 ↔ ISIC Rev. 4**: Needed for OECD ICIO or WIOD tables.
- **CPA ↔ HS/SITC**: When incorporating detailed trade data.
- **Temporal concordances**: NACE Rev. 1.1 → Rev. 2 (pre/post ~2008).
- **National classifications**: NAF (France), Ateco (Italy), NAICS (US).
- **Satellite account mapping**: Energy, emissions, or other satellite data
  from statistical sources with different classification granularity.

**The core challenge**: Official correspondence tables exist (Eurostat RAMON,
UN, R `concordance` package) but are rarely plug-and-play. Many-to-many
mappings require allocation weights. NSIs construct "bridge matrices" using
confidential micro-data they don't publish. At the aggregate level (A*64),
mappings are clean; at finer detail, every cell is a judgment call.

### 13.2 Why an agent is the right tool

A concordance mapping agent is arguably the **highest-value agent** in the
entire system, because:

1. It can **read and interpret** official correspondence tables from Eurostat
   RAMON, UNSD, or academic papers (HTML, CSV, PDF).
2. It can **reason about many-to-many cases**: "CPA 26 maps to both NACE 26
   and NACE 27 — given the context of this analysis (employment in
   electronics), which split makes sense?"
3. It can **document every mapping decision** with source and rationale,
   creating an audit trail that a hardcoded dictionary never provides.
4. It can **flag ambiguous cases** for human review rather than silently
   choosing.
5. It can **adapt when classifications change** — a new CPA revision doesn't
   require rewriting a Python dictionary; the agent reads the new
   correspondence table.

### 13.3 Agent design sketch

```yaml
role: concordance_mapping_agent
goal: >
  Given a source classification and target classification, produce a complete
  concordance table with mapping rationale, and flag many-to-many or ambiguous
  cases for human review.
tools:
  - execute_python
  - web_fetch (Eurostat RAMON, UNSD classification pages)
  - read_file
  - write_file
input:
  source_classification: "CPA 2.1"
  target_classification: "NACE Rev. 2"
  level_of_detail: "64 industries (A*64)"
  analysis_context: "Employment satellite for FIGARO IC-IOT, year 2010"
output:
  concordance_table: "data/concordances/cpa21_to_nace2_a64.csv"
  mapping_report: "data/concordances/cpa21_to_nace2_a64_report.md"
  flagged_cases: "data/concordances/cpa21_to_nace2_a64_flagged.csv"
```

### 13.4 Implementation timeline

This is Phase 6 work — after the core pipeline is agentic and tested. The
concordance agent is the bridge to generality: once it works, the pipeline
can handle any IO table + any satellite account combination, because the
mapping layer is automated.

Build it in two sub-phases:
1. **Sub-phase 6a**: Agent builds the CPA↔NACE concordance for the existing
   FIGARO pipeline. Verify it matches the implicit mapping already in the code.
2. **Sub-phase 6b**: Agent builds a NACE↔ISIC concordance for a different
   IO table (e.g., OECD ICIO). This tests generalization.

---

## 14. Companion documents

Two additional documents provide guidelines for building agents correctly:

1. **`FIGARO_AGENT_BEST_PRACTICES.md`** — Architecture, tool design,
   orchestration, memory, and testing patterns adapted for this project.
   Read sections 1–4 before writing any agent code.

2. **`FIGARO_AGENT_WORST_PRACTICES.md`** — Anti-patterns and failure modes
   specific to IO analysis pipelines. Read sections 1, 5, and 8 (IO-specific)
   before writing any agent code.

These are adapted from a comprehensive survey of production agent systems
(LangGraph, CrewAI, Anthropic, OpenAI, SWE-agent) and are tailored to the
specific risks and opportunities of this project.

---

## 15. Key files to read first

Before writing any code, read these files in this order:

1. `figaro_replication_instructions.md` — full mathematical spec
2. `CLAUDE.md` — current architecture, API quirks, validation benchmarks
3. `config.yaml` — all parameters
4. `FIGARO_AGENT_BEST_PRACTICES.md` — how to build agents correctly
5. `FIGARO_AGENT_WORST_PRACTICES.md` — what not to do
6. `src/stage6_review_agent.py` — you'll refactor this first
7. `src/stage3_model_construction.py` — understand the math you're NOT replacing
8. `src/stage1_data_acquisition.py` — understand the Eurostat API patterns
9. `run_pipeline.py` — understand the orchestration pattern
