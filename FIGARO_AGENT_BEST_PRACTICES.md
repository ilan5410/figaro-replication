# Agent Best Practices — FIGARO IO Analysis Pipeline

> Adapted for the figaro-replication agentic refactor project. Derived from
> Anthropic, OpenAI, LangGraph, SWE-agent, and production failure analysis
> literature (2024–2026). Consult the general best practices memory file for
> full detail on any pattern referenced here.

---

## 1. ARCHITECTURE — APPLIED TO THIS PROJECT

### 1.1 Start with Workflows, Not Agents

The FIGARO pipeline has 6 sequential stages. This is a **workflow** — the steps
are known, ordered, and deterministic in structure. Do NOT build 6 autonomous
agents from day one.

**Correct progression for this project:**
1. Wrap existing stages as deterministic functions (prompt chain / workflow)
2. Replace individual stages with tool-calling agents only where justified
3. Keep deterministic stages (S3: model construction, S4: decomposition) as
   plain Python functions — never agentic

The hierarchy for each stage:
```
Can a fixed script do it?  →  YES  →  Keep as deterministic function
                           →  NO   →  Does it require adaptation/judgment?
                                      →  YES  →  Make it an agent node
                                      →  NO   →  Keep as parameterized function
```

### 1.2 Agents vs. Deterministic Nodes — The Decision Matrix

| Stage | Nature | Decision | Rationale |
|-------|--------|----------|-----------|
| S1 Data Acquisition | API navigation, error recovery, adaptation to schema changes | **Agent** | Needs judgment when APIs change or return unexpected structures |
| S2 Data Preparation | Data wrangling with structural quirks | **Agent + deterministic validator** | Agent writes the code; validator enforces invariants |
| S3 Model Construction | Pure linear algebra (A, L, d) | **Deterministic** | 30 lines of numpy. Zero ambiguity. Agent adds only risk. |
| S4 Decomposition | Block-matrix arithmetic | **Deterministic** | Same as S3. Mechanical, well-defined, must be exact. |
| S5 Output Generation | Chart/table production to match a spec | **Agent** | Presentation task; agent can adapt styling, handle new formats |
| S6 Review | Inspect outputs, compare to benchmarks, write diagnostic report | **Agent** | Most natural agent task — open-ended analysis with judgment |
| S7 Concordance Mapping (future) | Classification research, bridge matrix construction | **Agent** | High desk-research content; perfect for LLM-assisted analysis |

### 1.3 The Core Loop for Each Agent Node

Each agent in this pipeline follows the same loop:
```
1. Receive state (input paths, config, expected outputs)
2. Plan approach (what code to write, what checks to run)
3. Write a Python script to disk
4. Execute it
5. Inspect results (read outputs, check dimensions, validate)
6. If OK → update state, return
7. If not OK → diagnose, rewrite script, retry (max 3 attempts)
8. If still failing → set human_intervention_needed = True
```

### 1.4 State Must Be Explicit — LangGraph State Schema

The pipeline state is a TypedDict passed between nodes. Every node reads
what it needs and writes what it produces. No implicit state.

```python
class PipelineState(TypedDict):
    config: dict
    stage: int
    raw_data_paths: Optional[dict]
    prepared_paths: Optional[dict]
    model_paths: Optional[dict]
    decomposition_paths: Optional[dict]
    output_paths: Optional[dict]
    review_report_path: Optional[str]
    review_passed: Optional[bool]
    errors: list[str]
    human_intervention_needed: bool
```

---

## 2. ORCHESTRATION — THE RIGHT PATTERN FOR THIS PIPELINE

### 2.1 Use Prompt Chaining with Validation Gates

This pipeline is sequential with validation between stages. Use LangGraph's
`StateGraph` with conditional edges:

```
S1 → validate_data → S2 → validate_preparation → S3 → S4 → S5 → S6
                ↓                    ↓
          human_escalation     human_escalation
```

This is **not** an orchestrator-worker pattern (no dynamic task decomposition).
It's **not** parallelization (stages are sequential). It's prompt chaining
with validation gates — the simplest multi-step pattern.

### 2.2 Conditional Edges — Validation Gates

After each agentic stage, a deterministic validator runs:

```python
def should_proceed_after_s2(state: PipelineState) -> str:
    """Deterministic gate — no LLM call."""
    if state["preparation_valid"]:
        return "s3_model_construction"
    elif len(state["errors"]) < 3:
        return "s2_retry"
    else:
        return "human_escalation"
```

After S6 (review), check `review_passed`:
- If True → pipeline complete
- If False with WARNINGs only → complete with advisory
- If False with FAILs → route to human

### 2.3 Checkpointing

Use LangGraph's SQLite checkpointer so runs can be resumed:
```python
from langgraph.checkpoint.sqlite import SqliteSaver
memory = SqliteSaver.from_conn_string("data/checkpoints.db")
graph = builder.compile(checkpointer=memory)
```

This is critical for Stage 1, which downloads data for 30+ minutes. If Stage
2 fails, you should be able to re-run from Stage 2 without re-downloading.

---

## 3. AGENT DEFINITION — PATTERNS FOR THIS PROJECT

### 3.1 System Prompts — What Each Agent Needs

Every agent prompt must include:
1. **Role** (one paragraph): "You are the data acquisition agent for an
   input-output analysis pipeline..."
2. **Mathematical context** (only what's relevant): S1 doesn't need the
   Leontief formula. S6 needs all the benchmark values.
3. **Input spec**: What files/state you receive, with exact paths.
4. **Output spec**: What files you must produce, with exact naming.
5. **Quality checks**: What you must verify before declaring success.
6. **Failure protocol**: When to retry, when to escalate.

**Do NOT put the full 648-line instruction doc into any agent's prompt.**
Each agent gets only its relevant context.

### 3.2 Dynamic Instructions (Parameterize for Generality)

System prompts should accept runtime parameters so the same agent works for
different analyses:

```python
def s1_instructions(config: dict) -> str:
    return f"""You are the data acquisition agent. Download:
    1. FIGARO IC-IOT for year {config['reference_year']}
       Table code: naio_10_fcp_ip1
    2. Employment data for {len(config['eu_member_states'])} EU countries
       Table code: nama_10_a64_e
    Save to: data/raw/
    """
```

This makes the agent work for 2010, 2011, 2012, 2013 without code changes.

### 3.3 Expected Output Specification (CrewAI Insight)

Always define `expected_output` — it dramatically improves quality:

```python
# In the agent's system prompt or task config:
expected_output = """
Files:
  - data/raw/figaro_iciot_2010.csv (>1M rows)
  - data/raw/employment_2010.csv (~28 countries × 64 industries)
  - data/raw/data_summary_2010.txt (dimensions, checksums)
Success criteria:
  - Employment total within 5% of 225,677 thousand
  - IC-IOT contains all 28 EU countries
"""
```

---

## 4. TOOL DESIGN — CRITICAL FOR THIS PROJECT

### 4.1 The Execute-Python Tool Is Everything

This project lives or dies on the `execute_python` tool. Design it carefully:

```python
def execute_python(code: str, timeout: int = 300) -> dict:
    """
    Write code to a temp .py file, execute it, return stdout/stderr.
    
    Args:
        code: Python code to execute. Must be a complete, runnable script.
              Import statements at top. Use print() for output the agent
              needs to see.
        timeout: Maximum execution time in seconds. Default 300.
                 Stage 1 downloads may need 1800 (30 min).
    
    Returns:
        stdout: Last 5000 chars of stdout (truncated to save context)
        stderr: Last 2000 chars of stderr
        returncode: 0 = success, non-zero = failure
        script_path: Path to the saved script (for debugging)
    
    Example:
        execute_python('''
        import pandas as pd
        df = pd.read_csv("data/raw/employment_2010.csv")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        ''')
    """
```

**Design principles (from Anthropic's tool design guide):**
- Return helpful errors: "pandas not installed. Run: pip install pandas"
- Truncate output to prevent context flooding
- Save scripts to disk for debugging (the agent can re-read them)
- Working directory = repo root (so relative paths work)

### 4.2 Other Tools — Keep Them Few and High-Level

| Tool | Purpose | Design notes |
|------|---------|-------------|
| `execute_python` | Write and run code | Core tool. All agents use it. |
| `read_file` | Inspect file contents | Truncate to 5000 chars. For CSVs, return head + shape + dtypes. |
| `write_file` | Create/overwrite files | For config, metadata, reports. Not for large data (use execute_python for that). |
| `list_directory` | See what files exist | Return names + sizes. Cap at 100 entries. |

**Do NOT add:** `download_url` (agent writes download code via execute_python),
`run_numpy` (use execute_python), `make_chart` (agent writes matplotlib via
execute_python). Fewer tools = less confusion.

### 4.3 Tool Response Best Practices

- Return structured, readable output: `"Shape: (1792, 1792), dtype: float64, min: 0.0, max: 4521.3"` — not raw numpy array dumps
- Truncate aggressively: the agent's context is its RAM
- Include actionable next steps in errors: `"File not found: data/raw/employment_2010.csv. Did Stage 1 run? Check data/raw/ directory."`

---

## 5. MEMORY & CONTEXT — PATTERNS FOR LONG-RUNNING PIPELINE

### 5.1 Structured State via Files

Each stage writes a summary file for downstream agents:

```
data/raw/data_summary_2010.txt        ← S1 writes, S2 reads
data/prepared/preparation_summary.txt ← S2 writes, S3 reads (metadata, dims)
data/model/model_summary.txt          ← S3 writes, S6 reads (model diagnostics)
outputs/review_report.md              ← S6 writes, human reads
```

This is the "progress.txt" pattern from Anthropic's long-running agent guide,
adapted to a pipeline where each "session" is a stage.

### 5.2 Don't Pass Raw Data Between Agents

Agents communicate via **file paths and metadata**, not raw matrices.
The state contains `prepared_paths: {"Z_EU": "data/prepared/Z_EU.parquet"}`
— not the actual matrix. Agents load data from disk when they need it.

### 5.3 Context Budget Per Agent

| Agent | Estimated context budget | Notes |
|-------|------------------------|-------|
| S1 | ~20K tokens | API docs, download logic, error handling |
| S2 | ~30K tokens | Largest — needs to understand FIGARO structure |
| S5 | ~25K tokens | Figure/table specs, styling |
| S6 | ~15K tokens | Benchmark values, check logic, report writing |

If an agent's context exceeds budget, split its work into sub-tasks with
intermediate checkpoints.

---

## 6. PLANNING & TASK DECOMPOSITION

### 6.1 Stage 2 Is the Hardest — Decompose It

S2 (data preparation) has multiple sub-tasks:
1. Parse IC-IOT file structure (inspect headers, identify blocks)
2. Extract Z^EU matrix (filter to EU countries, intermediate use only)
3. Construct export vector e (sum deliveries to non-EU countries)
4. Parse employment data and align ordering with IC-IOT
5. Validate all dimensions and orderings

The agent should tackle these sequentially, validating after each step.
This is the "break into phases, validate between phases" pattern.

### 6.2 Iterative Planning with Replanning

Agents should:
1. Write a plan (as comments in their Python script)
2. Execute step 1
3. Inspect results
4. Adjust plan if needed (e.g., "column names are different than expected")
5. Continue

"Replan is a feature, not a failure." The Eurostat API may return data in
a different format than the agent expects. The agent should adapt.

---

## 7. ERROR HANDLING & RESILIENCE

### 7.1 Stopping Conditions — Mandatory for Every Agent

```python
AGENT_LIMITS = {
    "s1_data_acquisition": {"max_iterations": 5, "timeout_s": 2400, "max_cost_usd": 1.00},
    "s2_data_preparation": {"max_iterations": 5, "timeout_s": 600, "max_cost_usd": 0.50},
    "s5_output_generation": {"max_iterations": 4, "timeout_s": 300, "max_cost_usd": 0.50},
    "s6_review":            {"max_iterations": 3, "timeout_s": 300, "max_cost_usd": 0.30},
}
```

### 7.2 Validation at Every Stage Boundary

**This is the single most important safety mechanism.** After every agentic
stage, a deterministic validator checks the outputs before the pipeline
proceeds. If validation fails, the agent can retry (up to max_iterations),
then escalates to human.

```python
def validate_stage2(state: PipelineState) -> bool:
    """Deterministic checks — no LLM call."""
    Z = load_matrix(state["prepared_paths"]["Z_EU"])
    assert Z.shape == (1792, 1792), f"Z shape: {Z.shape}"
    assert (Z >= 0).all(), "Negative values in Z"
    Em = load_vector(state["prepared_paths"]["Em_EU"])
    assert Em.shape == (1792,), f"Em shape: {Em.shape}"
    total_emp = Em.sum()
    assert abs(total_emp - 225677) / 225677 < 0.05, f"Employment: {total_emp}"
    return True
```

### 7.3 Human Escalation Triggers (From Instructions)

The existing pipeline defines 5 human intervention points. Preserve all of them:
1. Data download failure
2. Employment total deviates > 5% from paper
3. Any EU country entirely missing
4. Review agent finds FAIL-level errors (> 25% deviation)
5. Reference year changed from 2010 (no cross-checks available)

### 7.4 Error Messages in Tools

```python
# BAD
return {"error": "FileNotFoundError"}

# GOOD
return {
    "error": "File not found: data/raw/employment_2010.csv",
    "suggestion": "Stage 1 may not have run. Check data/raw/ for downloaded files.",
    "available_files": os.listdir("data/raw/")
}
```

---

## 8. GUARDRAILS — PROJECT-SPECIFIC

### 8.1 Numerical Guardrails (Critical for IO Analysis)

IO analysis has mathematical invariants that must always hold. These are
non-negotiable checks — no agent should be trusted to verify them alone:

- A matrix column sums < 1 (otherwise Leontief system diverges)
- L matrix all elements ≥ 0, diagonal ≥ 1
- L·(I−A) ≈ I (max error < 1e-6)
- Employment coefficients d ≥ 0
- Total export-supported employment sums correctly across decomposition

Implement these as deterministic validators, not agent self-checks.

### 8.2 Sandboxing

All `execute_python` calls run in the project directory with no network access
(except Stage 1, which needs HTTP for Eurostat). Consider using subprocess
with restricted permissions.

### 8.3 No Sensitive Data Triad

This project processes only public Eurostat data — no customer data, no
credentials (except the Anthropic API key, which stays in .env). The security
risk is low, but still: never put the API key in agent prompts or tool
responses.

---

## 9. CONCORDANCE MAPPING — THE FUTURE AGENT (S7)

### 9.1 Why This Is the Highest-Value Agentic Task

In IO analysis, the most time-consuming desk research is **classification
concordance** — mapping between different industry/product taxonomies:

- **CPA ↔ NACE**: FIGARO uses CPA (products) in product-by-product tables,
  but employment data uses NACE (industries). These are aligned at the 2-digit
  level but diverge below.
- **NACE Rev. 2 ↔ ISIC Rev. 4**: For international comparisons (WIOD, OECD ICIO).
- **CPA ↔ HS/SITC**: When incorporating trade data classified by commodity codes.
- **National classifications**: NAF (France), Ateco (Italy), NAICS (US) each
  have their own deviations from the international standard.
- **Temporal concordance**: NACE Rev. 1.1 → Rev. 2 (classification break circa 2008).
- **Satellite account mapping**: Mapping employment, energy, emissions, or
  other satellite data to the IO industry classification when they come from
  different statistical sources with different granularity.

**The challenge**: Concordance tables exist (Eurostat RAMON, UN, R's `concordance`
package) but are rarely plug-and-play. Many-to-many mappings, partial coverage
indicators, manual adjustments for edge cases, and "bridge matrices" that NSIs
construct with confidential micro-data but don't publish.

### 9.2 Why an Agent Can Help

An LLM agent is well-suited because:
- It can **read and interpret** official correspondence tables (HTML, CSV, PDF)
- It can **reason about many-to-many cases**: "CPA 26 maps to NACE 26 and 27 —
  which split should I use given the context of this analysis?"
- It can **search for** and **fetch** concordance tables from Eurostat RAMON,
  UNSD, or academic sources
- It can **construct bridge matrices** with human-readable rationale for each
  mapping decision
- It can **document edge cases** and flag them for human review

A hardcoded mapping script breaks every time a classification changes. An agent
that reads the docs can adapt.

### 9.3 Agent Design for Concordance Mapping

```yaml
role: concordance_mapping_agent
goal: >
  Given a source classification and a target classification, produce a
  complete concordance table with mapping rationale, and flag any
  many-to-many or ambiguous cases for human review.
tools:
  - execute_python
  - web_fetch (Eurostat RAMON, UNSD classification pages)
  - read_file
  - write_file
input:
  source_classification: "CPA 2.1"
  target_classification: "NACE Rev. 2"
  level_of_detail: "64 industries (A*64)"
  context: "Employment satellite account for FIGARO IO table"
output:
  concordance_table: "data/concordances/cpa21_to_nace2_a64.csv"
  mapping_report: "data/concordances/cpa21_to_nace2_a64_report.md"
  ambiguous_cases: "data/concordances/cpa21_to_nace2_a64_flagged.csv"
```

### 9.4 How This Enables Generalization

The concordance agent is the key to making the pipeline generic:
- Different year? Same concordance (usually).
- Different satellite (energy instead of employment)? Need a new concordance
  between energy statistics classification and IO classification.
- Different country set (OECD ICIO instead of FIGARO)? Need ISIC↔CPA concordance.
- Different IO table (WIOD)? Different base classification, need new concordance.

The agent handles the mapping research; the deterministic stages handle the math.

---

## 10. TESTING & EVALUATION — PROJECT-SPECIFIC

### 10.1 The Parallel Baseline Strategy

Keep the existing deterministic pipeline as ground truth. Every agentic stage
must produce outputs that match the baseline within tolerance:

```python
def test_s2_agent_matches_baseline():
    agent_Z = pd.read_parquet("data/prepared/agent/Z_EU.parquet")
    baseline_Z = pd.read_parquet("data/prepared/baseline/Z_EU.parquet")
    np.testing.assert_allclose(agent_Z, baseline_Z, rtol=1e-6)
```

### 10.2 Eval Tasks for Each Agent

| Agent | Eval task | Success metric |
|-------|-----------|---------------|
| S1 | Download data for 2010 | Files exist, correct dimensions, employment ≈ 225,677 |
| S1 | Download data for 2013 (generalization test) | Files exist, dimensions consistent |
| S2 | Prepare matrices from baseline raw data | Element-wise match to baseline (rtol < 1e-6) |
| S5 | Produce Figure 1 from baseline decomposition | PNG exists, correct size, visual inspection |
| S6 | Review baseline outputs | Report catches known issues (LU/MT missing data, product-vs-industry deviation) |

### 10.3 Metrics to Track

Per-run, log:
- Token consumption (input + output) per agent
- Wall-clock time per agent
- Number of tool calls per agent
- Number of retries (indication of fragility)
- Validation pass/fail at each gate
- Total pipeline cost in USD

---

## 11. MODEL SELECTION

### 11.1 Recommended Configuration

- **Agentic stages (S1, S2, S5, S6)**: Claude Sonnet (claude-sonnet-4-20250514)
  — good balance of capability, speed, and cost for code generation tasks.
- **Deterministic stages (S3, S4)**: No model needed. Plain Python.
- **Future: concordance agent (S7)**: Claude Sonnet or Opus depending on
  complexity of the classification research.

### 11.2 Cost Controls

```python
# In orchestrator config
MAX_COST_PER_RUN_USD = 2.00   # Circuit breaker for total pipeline
MAX_COST_PER_STAGE_USD = 1.00  # Per-stage limit
```

Track cumulative cost in the state and abort if exceeded.

---

## 12. THE PHASED IMPLEMENTATION PLAN

**Phase order is critical.** Start with the safest, highest-signal experiment:

1. **Phase 1 — Review Agent (S6)**: Lowest risk, highest signal. Tests the
   entire LangGraph + tool-calling pattern against known-good data.
2. **Phase 2 — Output Generation (S5)**: Low risk, visible results.
3. **Phase 3 — Data Acquisition (S1)**: Medium risk, high long-term value.
4. **Phase 4 — Data Preparation (S2)**: Highest risk, proceed with caution.
5. **Phase 5 — Integration**: Wire everything into the StateGraph.
6. **Phase 6 — Generalization**: Different years, satellite accounts,
   concordance agent.

**For each phase:**
1. Build the agent
2. Test against baseline
3. Document findings in `docs/feasibility.md`
4. Get human sign-off before proceeding to next phase

---

## 13. KEY PRINCIPLES — SUMMARY

1. **Deterministic where possible, agentic where valuable.** S3 and S4 stay
   as Python functions. Period.
2. **Validate at every boundary.** Deterministic validators between every stage.
   Never trust agent self-evaluation for numerical correctness.
3. **Start with the review agent.** It's the safest test of the full pattern.
4. **Agent prompts are stage-specific.** Each agent gets only the context it
   needs, not the entire 648-line instructions file.
5. **The execute_python tool is the core capability.** Design it well,
   truncate its output, save scripts for debugging.
6. **File paths, not data, in state.** Agents communicate via the filesystem.
7. **Human escalation is a feature.** 5 defined escalation triggers from the
   original pipeline spec. Preserve all of them.
8. **Track cost from day one.** Per-agent, per-run, with circuit breakers.
9. **Keep the deterministic baseline.** Every agentic output must match the
   baseline within tolerance. This is your ground truth.
10. **Design for generality.** Parameterize prompts, build the concordance
    agent, abstract the satellite account. The FIGARO replication is the
    narrow scope; generic IO analysis is the goal.
