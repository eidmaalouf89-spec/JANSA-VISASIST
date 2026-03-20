"""
Module 5 — Top-Level Runner (run_module5 orchestrator).

[Plan §7.1] Orchestrates Phase 1 → Phase 2 → Phase 3.
Returns (suggestion_results, s1_report, s2_report, s3_report).

Constraints:
  - Entire M5 execution MUST NOT modify m3_queue, m4_results, or g1_report.
  - Consume-only guarantee [V2.2.2 §2.4].
  - GP10: Fully functional without AI/LLM.
  - Deterministic: identical inputs → bit-level identical outputs [PATCH 6].
"""

import logging
from typing import Any, Tuple

import pandas as pd

from .engine import compute_suggestion
from .reports import build_report_s1, build_report_s2, build_report_s3
from .schemas import S1_COLUMNS, S2_COLUMNS, S3_COLUMNS
from .validation import (
    build_g1_blocker_index,
    build_m4_index,
    validate_m5_inputs,
)

logger = logging.getLogger(__name__)


def run_module5(
    m3_queue: pd.DataFrame,
    m4_results: list[dict],
    g1_report: pd.DataFrame,
    pipeline_run_id: str,
) -> Tuple[list[dict[str, Any]], pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Execute Module 5: Suggestion Engine.

    [Plan §7.1] Top-level entry point. Orchestrates:
      Phase 1: Input validation & index construction
      Phase 2: Per-item suggestion computation (Layers 0–6)
      Phase 3: Report generation (S1, S2, S3)

    Args:
        m3_queue: M3 priority queue DataFrame (all queued items).
        m4_results: List of M4 per-item analysis result dicts.
        g1_report: M4 G1 systemic blocker report DataFrame.
        pipeline_run_id: Non-empty string for cache keying.

    Returns:
        Tuple of:
          - List[Dict]: Per-item SuggestionResult for every M3 queue item
            (except EXCLUDED items which are omitted).
          - DataFrame: S1 Action Distribution Report.
          - DataFrame: S2 VISA Recommendation Report.
          - DataFrame: S3 Communication / Relance Report.

    Raises:
        ValueError: If critical input validation fails (M5 cannot run).
    """
    logger.info(
        "M5: Starting Module 5 — Suggestion Engine. pipeline_run_id=%s",
        pipeline_run_id,
    )

    # ================================================================
    # Phase 1: Input Validation & Index Construction [Plan §Phase 1]
    # ================================================================

    validate_m5_inputs(m3_queue, m4_results, g1_report, pipeline_run_id)

    # Early return for empty queue [Plan §1.1]
    if m3_queue.empty:
        logger.warning("M5: Empty m3_queue. Returning empty outputs.")
        return (
            [],
            pd.DataFrame(columns=S1_COLUMNS),
            pd.DataFrame(columns=S2_COLUMNS),
            pd.DataFrame(columns=S3_COLUMNS),
        )

    # Build indexes [Plan §1.4, §1.5]
    m4_index: dict[str, dict] = build_m4_index(m4_results)
    g1_blocker_index: dict[str, dict] = build_g1_blocker_index(g1_report)

    logger.info(
        "M5 Phase 1 complete: m3_queue=%d rows, m4_index=%d entries, "
        "g1_blocker_index=%d entries.",
        len(m3_queue), len(m4_index), len(g1_blocker_index),
    )

    # ================================================================
    # Phase 2: Per-Item Suggestion Computation [Plan §Phase 2]
    # ================================================================

    suggestion_results: list[dict[str, Any]] = []
    excluded_count: int = 0

    for _, m3_row in m3_queue.iterrows():
        row_id = str(m3_row.get("row_id", "?"))
        m3_item = m3_row.to_dict()

        # Look up M4 result [Plan §2.1]
        m4_result = m4_index.get(row_id)
        if m4_result is None:
            logger.error(
                "M5: No M4 result for row_id=%s. Treating as analysis_degraded=true.",
                row_id,
            )
            # Build a minimal degraded M4 result
            m4_result = {
                "row_id": row_id,
                "lifecycle_state": "ON_HOLD",
                "analysis_degraded": True,
                "failed_blocks": ["ALL"],
                "agreement": {},
                "conflict": {},
                "missing": {},
                "blocking": {},
                "delta": {},
                "time": {},
            }

        # Compute suggestion (Layers 0–6) [Plan §2.1]
        result = compute_suggestion(
            m3_item=m3_item,
            m4_result=m4_result,
            g1_blocker_index=g1_blocker_index,
            pipeline_run_id=pipeline_run_id,
        )

        if result is None:
            # Layer 0: EXCLUDED item — omitted
            excluded_count += 1
            continue

        # Enrich with M3 pass-through fields for S3 report
        # (source_sheet and document are needed by reports.py)
        result["source_sheet"] = m3_item.get("source_sheet", "LOT_UNKNOWN")
        result["document"] = m3_item.get("document", "DOC_UNKNOWN")

        suggestion_results.append(result)

    # Count assertion [Plan §Phase 2]
    expected_count = len(m3_queue) - excluded_count
    actual_count = len(suggestion_results)
    if actual_count != expected_count:
        logger.error(
            "M5: Count mismatch — expected %d results, got %d. "
            "(%d EXCLUDED items omitted).",
            expected_count, actual_count, excluded_count,
        )
    if excluded_count > 0:
        logger.warning(
            "M5: %d EXCLUDED items found in M3 queue — upstream filtering anomaly.",
            excluded_count,
        )

    logger.info(
        "M5 Phase 2 complete: %d suggestions produced (%d EXCLUDED omitted).",
        actual_count, excluded_count,
    )

    # ================================================================
    # Phase 3: Report Generation [Plan §Phase 3]
    # ================================================================

    s1_report = build_report_s1(suggestion_results)
    s2_report = build_report_s2(suggestion_results)
    s3_report = build_report_s3(suggestion_results)

    logger.info(
        "M5 Phase 3 complete: S1=%d rows, S2=%d rows, S3=%d rows.",
        len(s1_report), len(s2_report), len(s3_report),
    )

    logger.info("M5: Module 5 — Suggestion Engine complete.")

    return (suggestion_results, s1_report, s2_report, s3_report)
