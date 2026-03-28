"""
FIGARO Agentic Pipeline Orchestrator.

Defines the LangGraph StateGraph connecting all 6 stages with:
  - Agent nodes: S1, S2, S5, S6
  - Deterministic nodes: S3, S4
  - Conditional edges: validation gates after S1, S2
  - Human escalation routing
  - SQLite checkpointing for resumable runs

Architecture diagram:
  S1_acq → validate_s1 → S2_prep → validate_s2 → S3_model → S4_decomp
                ↓                       ↓
          human_escalation        human_escalation
                                                    ↓
                                               S5_output → S6_review → END
                                                                ↓
                                                         human_escalation

See FIGARO_AGENTIC_INSTRUCTIONS.md §3 and FIGARO_AGENT_BEST_PRACTICES.md §2.
"""
import logging
from pathlib import Path
from typing import Literal

from langgraph.graph import END, START, StateGraph

from agents.nodes.s1_data_acquisition import run_s1_data_acquisition
from agents.nodes.s2_data_preparation import run_s2_data_preparation
from agents.nodes.s3_model_construction import run_s3_model_construction
from agents.nodes.s4_decomposition import run_s4_decomposition
from agents.nodes.s5_output_generation import run_s5_output_generation
from agents.nodes.s6_review import run_s6_review
from agents.nodes.validators import validate_stage1, validate_stage2, validate_stage3
from agents.state import PipelineState

log = logging.getLogger("figaro.orchestrator")


# ─── Validation gate nodes ───────────────────────────────────────────────────

def gate_after_s1(state: PipelineState) -> PipelineState:
    """Deterministic validation gate after Stage 1."""
    valid, errors = validate_stage1(state)
    new_errors = list(state.get("errors", [])) + errors
    log.info(f"S1 validation: {'PASS' if valid else 'FAIL'} — {errors}")
    return {**state, "data_valid": valid, "errors": new_errors}


def gate_after_s2(state: PipelineState) -> PipelineState:
    """Deterministic validation gate after Stage 2."""
    valid, errors = validate_stage2(state)
    new_errors = list(state.get("errors", [])) + errors
    log.info(f"S2 validation: {'PASS' if valid else 'FAIL'} — {errors}")
    return {**state, "preparation_valid": valid, "errors": new_errors}


def gate_after_s3s4(state: PipelineState) -> PipelineState:
    """Deterministic validation gate after Stage 3+4."""
    valid, errors = validate_stage3(state)
    new_errors = list(state.get("errors", [])) + errors
    log.info(f"S3/S4 validation: {'PASS' if valid else 'FAIL'} — {errors}")
    if not valid:
        new_errors.append("Stage 3/4 validation failed — model outputs may be corrupt")
    return {**state, "errors": new_errors}


def human_escalation(state: PipelineState) -> PipelineState:
    """Terminal node: pipeline halted, human intervention required."""
    log.error("=== PIPELINE HALTED — Human Intervention Required ===")
    for err in state.get("errors", []):
        log.error(f"  {err}")
    log.error("Fix the issues above and re-run from the appropriate stage.")
    return {**state, "human_intervention_needed": True}


# ─── Conditional edge routing functions ──────────────────────────────────────

def route_after_s1_gate(state: PipelineState) -> Literal["s2_data_preparation", "human_escalation"]:
    if state.get("data_valid") and not state.get("human_intervention_needed"):
        return "s2_data_preparation"
    return "human_escalation"


def route_after_s2_gate(state: PipelineState) -> Literal["s3_model_construction", "human_escalation"]:
    if state.get("preparation_valid") and not state.get("human_intervention_needed"):
        return "s3_model_construction"
    return "human_escalation"


def route_after_s3s4_gate(state: PipelineState) -> Literal["s5_output_generation", "human_escalation"]:
    if not state.get("human_intervention_needed") and not any(
        "Stage 3" in e or "Stage 4" in e for e in state.get("errors", [])
    ):
        return "s5_output_generation"
    return "human_escalation"


def route_after_s6(state: PipelineState) -> Literal["__end__", "human_escalation"]:
    if state.get("review_passed"):
        return "__end__"
    # If only warnings (no FAIL), still complete the pipeline
    if not state.get("human_intervention_needed"):
        return "__end__"
    return "human_escalation"


# ─── Build the graph ─────────────────────────────────────────────────────────

def build_pipeline(start_stage: int = 1, end_stage: int = 6) -> StateGraph:
    """
    Build and return the compiled LangGraph StateGraph.

    Args:
        start_stage: First stage to run (1-6). Earlier stages are skipped.
        end_stage:   Last stage to run (1-6). Later stages are skipped.
    """
    builder = StateGraph(PipelineState)

    # ── Add all nodes ──
    builder.add_node("s1_data_acquisition", run_s1_data_acquisition)
    builder.add_node("gate_after_s1", gate_after_s1)
    builder.add_node("s2_data_preparation", run_s2_data_preparation)
    builder.add_node("gate_after_s2", gate_after_s2)
    builder.add_node("s3_model_construction", run_s3_model_construction)
    builder.add_node("s4_decomposition", run_s4_decomposition)
    builder.add_node("gate_after_s3s4", gate_after_s3s4)
    builder.add_node("s5_output_generation", run_s5_output_generation)
    builder.add_node("s6_review", run_s6_review)
    builder.add_node("human_escalation", human_escalation)

    # ── Wire edges ──
    # S1 → gate → S2 (or escalate)
    builder.add_edge(START, "s1_data_acquisition")
    builder.add_edge("s1_data_acquisition", "gate_after_s1")
    builder.add_conditional_edges("gate_after_s1", route_after_s1_gate)

    # S2 → gate → S3 (or escalate)
    builder.add_edge("s2_data_preparation", "gate_after_s2")
    builder.add_conditional_edges("gate_after_s2", route_after_s2_gate)

    # S3 → S4 → gate → S5 (or escalate)
    builder.add_edge("s3_model_construction", "s4_decomposition")
    builder.add_edge("s4_decomposition", "gate_after_s3s4")
    builder.add_conditional_edges("gate_after_s3s4", route_after_s3s4_gate)

    # S5 → S6 → END (or escalate)
    builder.add_edge("s5_output_generation", "s6_review")
    builder.add_conditional_edges("s6_review", route_after_s6)

    # Escalation → END
    builder.add_edge("human_escalation", END)

    return builder


def build_pipeline_from_stage(start_stage: int) -> StateGraph:
    """
    Build a graph that starts from a specific stage (for --start-stage).

    Stages before start_stage are skipped; their outputs are expected
    to already exist on disk.
    """
    builder = StateGraph(PipelineState)

    # Add all nodes
    builder.add_node("s1_data_acquisition", run_s1_data_acquisition)
    builder.add_node("gate_after_s1", gate_after_s1)
    builder.add_node("s2_data_preparation", run_s2_data_preparation)
    builder.add_node("gate_after_s2", gate_after_s2)
    builder.add_node("s3_model_construction", run_s3_model_construction)
    builder.add_node("s4_decomposition", run_s4_decomposition)
    builder.add_node("gate_after_s3s4", gate_after_s3s4)
    builder.add_node("s5_output_generation", run_s5_output_generation)
    builder.add_node("s6_review", run_s6_review)
    builder.add_node("human_escalation", human_escalation)

    # Determine start node based on start_stage
    stage_to_node = {
        1: "s1_data_acquisition",
        2: "s2_data_preparation",
        3: "s3_model_construction",
        4: "s4_decomposition",
        5: "s5_output_generation",
        6: "s6_review",
    }
    start_node = stage_to_node.get(start_stage, "s1_data_acquisition")
    builder.add_edge(START, start_node)

    # Full edge set (same as build_pipeline)
    if start_stage <= 1:
        builder.add_edge("s1_data_acquisition", "gate_after_s1")
        builder.add_conditional_edges("gate_after_s1", route_after_s1_gate)
    if start_stage <= 2:
        builder.add_edge("s2_data_preparation", "gate_after_s2")
        builder.add_conditional_edges("gate_after_s2", route_after_s2_gate)
    if start_stage <= 3:
        builder.add_edge("s3_model_construction", "s4_decomposition")
    if start_stage <= 4:
        builder.add_edge("s4_decomposition", "gate_after_s3s4")
        builder.add_conditional_edges("gate_after_s3s4", route_after_s3s4_gate)
    if start_stage <= 5:
        builder.add_edge("s5_output_generation", "s6_review")
    if start_stage <= 6:
        builder.add_conditional_edges("s6_review", route_after_s6)

    builder.add_edge("human_escalation", END)

    return builder


def compile_pipeline(start_stage: int = 1, use_checkpointing: bool = True):
    """
    Compile the pipeline graph, optionally with SQLite checkpointing.

    SQLite checkpointing enables resuming from a specific stage without
    re-running earlier stages (critical for S1's 30-min downloads).
    """
    builder = build_pipeline_from_stage(start_stage)

    if use_checkpointing:
        try:
            from langgraph.checkpoint.sqlite import SqliteSaver
            checkpoint_path = Path(__file__).parent.parent / "data" / "checkpoints.db"
            checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            memory = SqliteSaver.from_conn_string(str(checkpoint_path))
            graph = builder.compile(checkpointer=memory)
            log.info(f"Compiled pipeline with SQLite checkpointing: {checkpoint_path}")
            return graph
        except ImportError:
            log.warning("langgraph.checkpoint.sqlite not available — running without checkpointing")

    graph = builder.compile()
    log.info("Compiled pipeline without checkpointing")
    return graph
