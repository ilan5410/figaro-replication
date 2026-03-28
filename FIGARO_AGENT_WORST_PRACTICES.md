# Agent Worst Practices — FIGARO IO Analysis Pipeline

> What NOT to do when building the agentic refactor. Each anti-pattern is
> mapped to a concrete risk in this project. Adapted from production failure
> analysis and the general worst practices memory file.

---

## 1. ARCHITECTURE ANTI-PATTERNS

### 1.1 ❌ Making Stages 3 and 4 Agentic

**The #1 risk for this project specifically.** Stages 3 (model construction)
and 4 (decomposition) are pure linear algebra: `A = Z·diag(x)^{-1}`,
`L = (I−A)^{-1}`, block decomposition. An agent that generates this code
introduces hallucination risk for zero benefit.

**What goes wrong:**
- Agent transposes a matrix (silently catastrophic — wrong economic meaning)
- Agent uses `np.linalg.solve` instead of `np.linalg.inv` with wrong arguments
- Agent applies `diag()` along the wrong axis
- Results are numerically plausible but economically wrong — and nobody catches
  it because the numbers are in the right ballpark

**The rule:** If the code is <50 lines of well-defined linear algebra, it must
be a deterministic function. Full stop.

### 1.2 ❌ Jumping to 6 Agents from Day One

Building all 4 agentic stages simultaneously. You'll spend weeks debugging
inter-agent communication while having no confidence in any individual agent.

**What to do:** Phase 1 = review agent only, tested against known-good data.
Don't build S1/S2/S5 agents until S6 is working and benchmarked.

### 1.3 ❌ Treating the Pipeline as a "Multi-Agent Conversation"

This is NOT a conversation between agents. It's a sequential pipeline where
each stage produces files consumed by the next. Agents don't talk to each
other — they talk to the filesystem.

**What goes wrong if you treat it as a conversation:**
- Growing context windows across agent hops (by agent 4, the context is
  polluted with S1's download logs)
- Cascading errors (S1's slightly wrong column naming propagates silently)
- No checkpointing (you can't resume from S3 if the "conversation" is one
  continuous thread)

**The right pattern:** Shared state via LangGraph `StateGraph`. Each agent
reads state, does its work, writes outputs to disk, updates state. Clean
separation.

### 1.4 ❌ Over-Abstracting the Framework

Wrapping LangGraph in custom abstraction layers, creating an "AgentFactory"
class hierarchy, building a generic "IOAnalysisAgentFramework" before you
have a single working agent.

**What to do:** Write the simplest possible LangGraph graph with 6 nodes.
Make it work. Then refactor for generality.

### 1.5 ❌ Creating Agents That Don't Provide Meaningful Specialization

If you're tempted to create a "config parsing agent" or a "file management
agent" — don't. Reading YAML and creating directories is trivial Python.
An agent that just calls `yaml.safe_load()` is adding latency and cost for
no value.

**Test:** Would removing this agent and replacing it with 3 lines of Python
degrade the system? If not, it's not an agent.

---

## 2. TOOL DESIGN ANTI-PATTERNS

### 2.1 ❌ Creating Too Many Tools

Tempting: `download_eurostat`, `download_figaro`, `parse_csv`, `reshape_matrix`,
`compute_leontief`, `make_barchart`, `format_excel`, `validate_dimensions`...

**What goes wrong:** Agent gets confused choosing between overlapping tools.
`parse_csv` vs `read_file` — which one for a FIGARO data file?

**The right set for this project:** `execute_python`, `read_file`, `write_file`,
`list_directory`. That's it. Everything else happens inside Python scripts
the agent writes and executes.

### 2.2 ❌ Returning Raw Data from Tools

```python
# BAD: Agent calls read_file on a 200MB CSV
return open("data/raw/figaro_iciot_2010.csv").read()  # 200MB in context window
```

**What goes wrong:** Context window overflow. Agent can't process the response.
The LLM's "RAM" is 128K–200K tokens, not 200MB.

**The right approach:**
```python
# GOOD: Return summary, not raw data
df = pd.read_csv(path, nrows=5)
return f"Shape: {full_shape}, Columns: {list(df.columns)}, Head:\n{df.to_string()}"
```

### 2.3 ❌ Unhelpful Tool Errors

```python
# BAD
return {"error": "subprocess returned non-zero exit code"}

# GOOD
return {
    "error": "Python script failed",
    "stderr": "ModuleNotFoundError: No module named 'openpyxl'",
    "suggestion": "Install with: pip install openpyxl",
    "script_path": "scripts/tmp_s5_chart.py"  # so agent can re-read its code
}
```

### 2.4 ❌ No Timeout on Execute-Python

Stage 1 downloads data for 30 minutes. Without a timeout, a stuck download
hangs the entire pipeline forever.

Stage 3 inverts a 1792×1792 matrix. Without a timeout, a numerical instability
could cause an infinite loop (unlikely with numpy, but defensive coding matters).

**Every `execute_python` call must have a timeout.** Stage-specific:
- S1: 2400s (40 min)
- S2: 600s (10 min)
- S3/S4: 120s (2 min — if matrix ops take longer, something is wrong)
- S5: 300s (5 min)
- S6: 300s (5 min)

---

## 3. PROMPT & INSTRUCTION ANTI-PATTERNS

### 3.1 ❌ Putting the Full Spec in Every Agent's Prompt

The `figaro_replication_instructions.md` is 648 lines. Putting it in every
agent's system prompt wastes ~8K tokens per call, dilutes attention, and
confuses agents with irrelevant context.

**What to do:** Each agent gets ONLY the sections relevant to its stage:
- S1: §2 (Data Acquisition), §1 (Config), Eurostat API quirks from CLAUDE.md
- S2: §3 (Data Preparation), §4.4 (Arto 2015 export definition), matrix dimensions
- S5: §6 (Output Generation), figure/table specs
- S6: §7 (Review Agent), benchmark values, mathematical identities

### 3.2 ❌ Over-Prescribing the Agent's Code

```
# BAD — too prescriptive
"Write a Python script that uses pandas read_csv with sep='\t' to read the
file, then filters columns where c_orig is in the EU list, then pivots using
pd.pivot_table with values='OBS_VALUE'..."
```

**Why it fails:** If the file format changes (tabs to commas, different column
names), the agent can't adapt because it was told exactly what to do.

**Better:**
```
"Download the FIGARO IC-IOT for 2010 from the Eurostat API. The table code is
naio_10_fcp_ip1. Inspect the response structure, parse it into a DataFrame,
and save it. Verify it contains data for all 28 EU countries."
```

State the goal. Let the agent figure out the implementation.

### 3.3 ❌ Being Too Vague About Success Criteria

```
# BAD — no success criteria
"Download the employment data and prepare it for analysis."
```

```
# GOOD — specific success criteria
"Download employment data (table nama_10_a64_e, unit THS_PER, na_item EMP_DC)
for all 28 EU member states for year 2010. Save to data/raw/employment_2010.csv.
Verify:
  - At least 28 × 64 = 1792 non-null observations
  - Total EU-28 employment is within 5% of 225,677 thousand persons
  - No country has entirely missing data"
```

### 3.4 ❌ Asking One Agent to Do Too Many Things

Tempting: "Download the data, parse it, build the IO table, compute the
Leontief inverse, and produce all charts."

**What goes wrong:** Agent runs out of context mid-task, leaves half-finished
files, next session has no idea what was completed. This is Anthropic's
"one-shot complex task" failure mode.

**The rule:** One agent, one stage, one clear deliverable.

---

## 4. MEMORY & CONTEXT ANTI-PATTERNS

### 4.1 ❌ Passing Raw Matrices in State

```python
# BAD — 1792×1792 matrix serialized as JSON in the state dict
state["Z_EU"] = Z_EU.tolist()  # 3.2M floats in the state
```

**What goes wrong:** State object explodes. LangGraph checkpoint becomes huge.
Serialization/deserialization takes longer than the computation itself.

**The right pattern:** State contains file paths. Agents load data from disk.
```python
state["prepared_paths"]["Z_EU"] = "data/prepared/Z_EU.parquet"
```

### 4.2 ❌ No State Summary Between Stages

Agent S2 finishes and writes 5 parquet files. Agent S3 starts and has no idea
what dimensions they are, what ordering was used, what countries are included.

**What to do:** Each stage writes a human-readable summary:
```
# data/prepared/preparation_summary.txt
Countries: AT, BE, BG, ... (28 total)
Industries: CPA_A01, CPA_A02, ... (64 total)
Z_EU: 1792 × 1792, dtype float64, min 0.0, max 4521.3
Em_EU: 1792 × 1, total 225,412 thousands
Export vector e: 1792 × 1, total 1,847,293 M EUR
```

### 4.3 ❌ Growing Context Window Across Agent Iterations

Agent S2 tries to prepare data, fails, retries 3 times. By the 3rd attempt,
the context contains all previous failed scripts, error messages, and
reasoning. The agent starts getting confused by its own earlier mistakes.

**What to do:** On retry, give the agent a clean context with only:
1. The original task description
2. A concise summary of what went wrong ("Previous attempt failed because
   column 'c_orig' was not found — file uses 'geo' instead")
3. No raw error dumps from earlier attempts

---

## 5. EXECUTION & LOOP ANTI-PATTERNS

### 5.1 ❌ No Maximum Iterations

An agent stuck in a retry loop because the Eurostat API is down. Without
a max iteration count, it will retry forever, burning API credits.

**Every agent MUST have:**
```python
max_iterations = 5
max_cost_usd = 1.00
timeout_seconds = 2400  # varies by stage
```

### 5.2 ❌ Self-Evaluation Without Objective Checks

```
# BAD — agent "reviews" its own matrices
Agent S2: "I've inspected the Z matrix and it looks correct."
```

An LLM cannot verify that a 1792×1792 matrix is numerically correct by
"looking at it." It will rationalize.

**The rule:** Agent self-checks are supplementary. Deterministic validators
are mandatory. The validator checks dimensions, signs, sums, and tolerances
with exact numerical assertions. The agent's "opinion" is irrelevant for
numerical correctness.

### 5.3 ❌ Not Testing End-to-End

Agent S2 produces matrices that pass all dimension checks. But the country
ordering is Belgium, Austria, Bulgaria... instead of the IC-IOT's ordering
of Austria, Belgium, Bulgaria... The Leontief inverse is computed on
misaligned data, and the results are nonsensical.

Dimension checks pass. Positivity checks pass. Sum checks are close enough.
But the economics are completely wrong because the mapping is wrong.

**What to do:** End-to-end test: run the full pipeline (with deterministic S3/S4)
and compare final outputs against the baseline. If total export-supported
employment deviates by > 5%, something is misaligned.

### 5.4 ❌ The "Vibes Fade" Problem in IO Analysis

Agent S2 works perfectly for 2010. You generalize to 2013. The FIGARO data
for 2013 has Croatia joining the EU (28th member), slightly different CPA
codes, and a revised employment vintage. The agent's approach from 2010
silently produces wrong results because it assumed the same structure.

**The fix:**
- The agent must inspect the actual data headers, not assume them
- Deterministic validators must check against the config's expected country list
- Run the full pipeline for each new year and compare against published
  Eurostat aggregates

---

## 6. MULTI-AGENT ANTI-PATTERNS

### 6.1 ❌ Sharing Mutable State Between Concurrent Agents

This pipeline is sequential, so this shouldn't be an issue — but if you
parallelize (e.g., S1 downloads IC-IOT and employment simultaneously),
don't let both agents write to the same state dict without synchronization.

### 6.2 ❌ Cascading Errors Through the Pipeline

S1 downloads data but misses Malta. S2 doesn't notice (it just builds a
1728×1728 matrix instead of 1792×1792). S3 computes the Leontief inverse.
S4 decomposes. S5 produces charts. S6 catches the error — but now you've
burned 30 minutes and $0.50 re-running everything.

**The fix:** Validate after EVERY stage. The validation gate after S1 should
check: "Do we have data for all 28 countries in the config?" Fail fast.

### 6.3 ❌ No Fallback When an Agent Fails

Agent S1 can't reach the Eurostat API (server is down, happens regularly).
Without a fallback, the pipeline just fails.

**Implement for S1 specifically:**
1. Retry with exponential backoff (3 attempts)
2. Try alternative API endpoint (bulk download vs JSON API)
3. Escalate to human: "Eurostat API is unreachable. Please provide the
   data file manually at data/raw/"

---

## 7. CONCORDANCE / MAPPING ANTI-PATTERNS

### 7.1 ❌ Hardcoding Concordance Tables

Embedding the CPA→NACE mapping as a Python dictionary in source code.

**Why it fails:**
- Classification revisions (CPA 2.1 → CPA 2.2) break everything
- Different IO tables use different classifications
- The mapping is specific to the level of aggregation (A*64 vs A*10 vs A*38)

**What to do:** Store concordances as CSV files with provenance metadata.
Use the concordance agent (S7) to build them, with human review.

### 7.2 ❌ Ignoring Many-to-Many Mappings

"CPA 26 maps to NACE 26" — but at finer detail, CPA 26.1 maps to NACE 26.1
AND NACE 27.1. The agent produces a clean 1:1 mapping that is wrong at
the margin.

**What to do:** The concordance agent must flag all many-to-many cases and
produce a bridge matrix (not a 1:1 lookup table) with allocation weights.
Ambiguous cases go in a separate "flagged" file for human review.

### 7.3 ❌ Trusting the Agent's Classification Knowledge

LLMs have training data about CPA, NACE, and ISIC — but this knowledge may
be outdated, incomplete, or wrong at the 4-digit level. The agent should
ALWAYS verify against official correspondence tables (Eurostat RAMON, UNSD)
rather than relying on its parametric knowledge.

### 7.4 ❌ Not Documenting Mapping Decisions

The concordance agent builds a CPA→NACE bridge matrix. Six months later,
someone asks "why did you allocate 60% of CPA 26.3 to NACE 26 and 40% to
NACE 27?" If the rationale isn't documented, the mapping is a black box.

**The rule:** Every mapping decision must have a documented rationale in the
mapping report, including the source (which correspondence table, which version).

---

## 8. IO-ANALYSIS-SPECIFIC ANTI-PATTERNS

### 8.1 ❌ Confusing Product-by-Product and Industry-by-Industry Tables

The paper uses industry-by-industry IC-IOT (not publicly available). This
pipeline uses product-by-product (publicly available). An agent that doesn't
understand this distinction will:
- Apply industry classification codes to product data
- Compare results to paper benchmarks without noting the table type difference
- Produce misleading discrepancy reports

**The rule:** Every output document, review report, and log must note the
table type being used and its implications.

### 8.2 ❌ Getting the Export Definition Wrong

The Arto (2015) export definition is subtle:
- Intra-EU intermediate flows → inside the Leontief inverse (endogenous)
- Intra-EU final demand + all exports to non-EU → export vector (exogenous)

An agent that treats ALL exports as exogenous (including intra-EU intermediate)
will double-count and overestimate employment effects by ~30%.

**The rule:** The export definition is specified in `config.yaml` and must
be enforced in S2 (data preparation). The deterministic validator checks
that the Z matrix contains only EU×EU flows and that the export vector
contains the correct components.

### 8.3 ❌ Misaligning Country/Industry Ordering

The single most dangerous silent error in IO analysis. If country ordering
in the Z matrix doesn't match the employment vector, the Leontief inverse
is applied to wrong data. Results look plausible (right magnitude, reasonable
patterns) but assign Germany's employment to France and vice versa.

**Detection is hard.** You can't catch this with dimension checks or sum
checks. The only reliable detection is:
1. Explicit ordering metadata saved by S2 and checked by S3
2. End-to-end comparison against published country-level aggregates
3. Spot checks: "Does Germany have the highest export-supported employment?"

---

## 9. THE DEFINITIVE DON'T LIST — THIS PROJECT

1. **Don't** make S3 (Leontief inverse) or S4 (decomposition) agentic
2. **Don't** build all agents at once — start with S6 (review)
3. **Don't** put the full 648-line spec in every agent's prompt
4. **Don't** pass matrices through LangGraph state — use file paths
5. **Don't** trust agent self-evaluation for numerical correctness
6. **Don't** skip deterministic validators between stages
7. **Don't** let agents run without max iterations, timeouts, and cost limits
8. **Don't** treat this as an agent conversation — it's a pipeline with state
9. **Don't** hardcode concordance tables in source code
10. **Don't** ignore the Arto (2015) export definition distinction
11. **Don't** assume country/industry ordering is correct without verification
12. **Don't** compare results to paper benchmarks without noting table type
13. **Don't** return raw data files through tool responses
14. **Don't** give the S2 agent the same context window as the S6 agent
15. **Don't** retry agents with their full error history in context (summarize)
16. **Don't** skip the end-to-end comparison against the deterministic baseline
17. **Don't** generalize to other years without re-running all validators
18. **Don't** build a generic "IOAnalysisFramework" before you have one working pipeline
19. **Don't** forget that the Eurostat API is flaky — build retries into S1
20. **Don't** assume the concordance agent's classification knowledge is correct — verify against official sources
