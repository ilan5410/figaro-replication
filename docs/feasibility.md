# Feasibility Assessment: Agentic IO Analysis Pipeline

**Project**: FIGARO Employment Content Replication
**Date**: 2026-03-28
**Status**: Phase 0–1 complete (architecture + review agent implemented)

---

## Executive Summary

The FIGARO pipeline is a strong candidate for partial agentic automation.
Two stages (S6 review, S5 output generation) are excellent fits for LLM agents
and carry low risk. Two more (S1 data acquisition, S2 data preparation) are
viable with proper guardrails. Two stages (S3 model construction, S4
decomposition) must remain deterministic — making them agentic would introduce
hallucination risk for linear algebra with no benefit.

**Recommendation**: Proceed with the phased rollout. Start with S6 (review
agent) in production, then S5, then S1. S2 requires careful validation and
should be the last agent deployed.

---

## Stage-by-Stage Assessment

### Stage 1: Data Acquisition

- **Feasibility**: HIGH
- **Rationale**: Downloading from a public REST API — reasoning about query
  parameters, retry logic, error handling, and adapting to schema changes — is
  exactly what LLMs do well. The Eurostat API has documented quirks (e.g., the
  `nace_r2` filter silently returning 0 rows) that an agent can detect and work
  around by inspecting actual results.
- **Key risk**: Downloads take ~30 minutes. Agent must write scripts that save
  incrementally so a failure midway doesn't lose progress.
- **Guardrail**: Deterministic validator checks file existence, row counts,
  and EU-28 country presence before proceeding.
- **Generalization**: Parameterize by `reference_year` and `eu_member_states`.
  The same agent handles 2010–2013 without code changes.
- **Recommendation**: Replace hardcoded stage. Use execute_python with 2400s
  timeout. Agent reliability expected: 8/10 (Eurostat API is occasionally
  unreachable).

### Stage 2: Data Preparation

- **Feasibility**: MEDIUM (agentic code generation + deterministic validation)
- **Rationale**: The data wrangling logic (pivot, filter, reshape) is well
  within LLM capability. But the FIGARO file structure has specific quirks
  (CPA product codes, value-added rows, final demand columns) and the
  country/industry ordering alignment is a silent-failure risk.
- **Key risk**: If country ordering in Z_EU doesn't match Em_EU, the Leontief
  inverse will produce numerically plausible but economically wrong results.
  Dimension checks won't catch this.
- **Guardrail**: Hard: deterministic validator checks Z_EU is 1792×1792,
  Em_EU sums to ~225,677, no negatives. Soft: agent must save explicit
  ordering metadata; S3 must verify alignment before computing.
- **Recommendation**: Deploy with caution. The agent writes the parsing code;
  the validator enforces invariants. Run element-wise comparison against
  deterministic baseline after each deployment. Agent reliability expected:
  6/10 without extensive testing.

### Stage 3: Model Construction

- **Feasibility**: LOW (keep deterministic — permanently)
- **Rationale**: This is `A = Z·diag(x)^{-1}`, `L = (I−A)^{-1}`,
  `d = diag(x)^{-1}·Em`. The code is 30 lines of numpy. An agent that generates
  this introduces hallucination risk (transposition errors, wrong axis in diag)
  with zero benefit over the deterministic implementation.
- **Recommendation**: Permanent deterministic function. Expose as a
  `build_leontief_model(Z, x, Em) -> (A, L, d)` tool for agents to call.
  Never agentic.

### Stage 4: Decomposition

- **Feasibility**: LOW (keep deterministic — permanently)
- **Rationale**: Same as Stage 3. Block-matrix arithmetic with no ambiguity.
  The domestic/spillover/direct/indirect decomposition is mechanical and
  well-defined. An agent adds only risk.
- **Recommendation**: Permanent deterministic function. Expose as a
  `decompose_employment(L, d, e, country_list) -> decomposition_dict` tool.
  Never agentic.

### Stage 5: Output Generation

- **Feasibility**: HIGH
- **Rationale**: Producing bar charts with specific styling, ordering, and
  annotation is exactly what LLMs excel at. The output format may change
  (different color schemes, client branding, additional annotations) — an agent
  adapts without code changes. Errors are immediately visible on inspection.
- **Key risk**: Headless matplotlib requires `matplotlib.use('Agg')`. Agent
  must be told this explicitly in the prompt.
- **Guardrail**: Agent reads back each output file and checks file size > 0.
- **Recommendation**: Replace hardcoded stage. Very low risk — visual outputs
  are always human-reviewed. Agent reliability expected: 9/10.

### Stage 6: Review

- **Feasibility**: EXCELLENT
- **Rationale**: This is the most natural agent task in the entire pipeline:
  inspect outputs, compare against benchmarks, write diagnostic report. An
  LLM provides richer, more contextual analysis than hardcoded threshold
  checks — e.g., explaining WHY Luxembourg's spillover share deviates
  (missing confidential employment data) rather than just flagging the number.
- **Guardrails**: Hardcoded benchmark values in prompt prevent the agent from
  inventing wrong thresholds. All checks are independently verifiable by
  reading the report.
- **Recommendation**: Replace hardcoded stage immediately. This is the Phase 1
  deployment. Agent reliability expected: 9/10.

---

## Generalization Potential

**Can this architecture handle other IO analyses?** Yes, with the following
parameterizations:

| Change | What's needed |
|--------|--------------|
| Different year (2011–2013) | Change `reference_year` in config.yaml |
| Different satellite (energy instead of employment) | New S1 download + new Em vector; S3/S4 math is identical |
| Different country set | Change `eu_member_states` in config.yaml; agent prompts are parameterized |
| Different IO table (WIOD, OECD ICIO) | New S1 agent with different API; S2 agent with different structural knowledge; concordance agent (S7) |

**Future Stage 7 — Concordance Mapping Agent**: The highest-value future
addition. Maps between CPA product codes (IC-IOT) and other classification
systems (NACE, ISIC, HS, national classifications). This is the desk research
task most prone to error and most time-consuming in IO analysis. An agent that
reads Eurostat RAMON correspondence tables and builds bridge matrices with
documented rationale is genuinely more robust than a hardcoded dictionary.

---

## Cost-Benefit Analysis

| Stage | Tokens/run | Cost (Sonnet) | Latency | vs. Deterministic |
|-------|-----------|---------------|---------|-------------------|
| S1 (acquisition) | ~20K | ~$0.10 | 30–40 min (download-bound) | More robust to API changes |
| S2 (preparation) | ~30K | ~$0.15 | 5–10 min | Higher risk, more flexible |
| S3+S4 (deterministic) | 0 | $0.00 | ~15s | No change |
| S5 (output gen) | ~25K | ~$0.12 | 2–5 min | More adaptable styling |
| S6 (review) | ~15K | ~$0.08 | 1–2 min | Richer narrative analysis |
| **Total** | **~90K** | **~$0.45** | **~40 min** | $0.45/run added cost |

The deterministic pipeline costs $0.00/run and takes ~35 min (dominated by
the download). The agentic version adds ~$0.45 per run and ~5–10 min of
LLM overhead. This is acceptable for a proof-of-concept.

**Production optimization**: Cache S1 outputs (data doesn't change year-over-year).
Skip agent stages when inputs haven't changed (hash-based caching). Estimated
cached cost: ~$0.20/run (only S2, S5, S6 re-run).

**Maintenance benefit**: The deterministic pipeline requires manual code fixes
when Eurostat changes API endpoints, adds countries, or revises data formats.
The S1 agent can adapt by reading the API documentation and inspecting the
actual response structure. Historical precedent: the Eurostat JSON API has
changed response format 3 times since 2019.

---

## Risk Assessment

| Risk | Severity | Mitigation |
|------|----------|-----------|
| Agent hallucination in matrix ops | Critical | S3/S4 stay deterministic; never agentic |
| Country ordering misalignment | Critical | Explicit metadata + deterministic validator |
| Eurostat API downtime (S1) | High | Retry with backoff; human escalation fallback |
| Context window overflow in S2 | Medium | execute_python writes scripts; raw data never in context |
| LLM generates wrong chart ordering | Low | Human review of visual outputs |
| Benchmark values in prompt are wrong | Medium | Benchmark values sourced from paper directly |

---

## Implementation Status

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 0: Setup | ✅ Complete | Directory structure, dependencies, this document |
| Phase 1: S6 Review Agent | ✅ Implemented | `agents/nodes/s6_review.py` |
| Phase 2: S5 Output Agent | ✅ Implemented | `agents/nodes/s5_output_generation.py` |
| Phase 3: S1 Acquisition Agent | ✅ Implemented | `agents/nodes/s1_data_acquisition.py` |
| Phase 4: S2 Preparation Agent | ✅ Implemented | `agents/nodes/s2_data_preparation.py` |
| Phase 5: Orchestrator Integration | ✅ Implemented | `agents/orchestrator.py`, `agents/run_agentic.py` |
| Phase 6: Generalization | 🔲 Pending | Concordance agent, multi-year testing |

---

## Recommendations for Next Steps

1. **Run Phase 1 immediately**: The S6 review agent is ready to test. Run
   stages 1–5 with the deterministic pipeline, then run the agentic review
   and compare its output to the hardcoded review. This is low-risk, high-value.

2. **Benchmark S6 before deploying S5**: Run the agentic review 5 times and
   compare against the hardcoded review. Does it catch the same issues? Does
   it hallucinate problems? Document findings.

3. **Deploy S5 after S6 is stable**: S5 output generation is the next
   safest deployment. Visual comparison of agent-produced vs. hardcoded charts.

4. **Test S1 with 2013 data**: Before deploying S1 in production, test the
   generalization by changing `reference_year` to 2013. Does the agent adapt
   to any structural differences in the 2013 data?

5. **S2 requires extensive testing**: Run element-wise comparison of
   agent-prepared matrices against the deterministic baseline. Only deploy if
   `np.testing.assert_allclose(agent_Z, baseline_Z, rtol=1e-6)` passes
   consistently across multiple runs.

6. **Build S7 concordance agent**: Once the core pipeline is stable, the
   concordance agent unlocks generalization to other IO tables and satellite
   accounts. This is the highest long-term value addition.
