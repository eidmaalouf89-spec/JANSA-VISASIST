"""
Module 5 — Input Validation & Index Construction.

[Plan §Phase 1] Validates M3/M4 inputs and builds O(1) lookup indexes.

CRITICAL: M4 field path corrections applied here.
Actual M4 uses block names "agreement", "missing", "blocking", "time"
(NOT "A1", "A3", "A4", "A6" as referenced in the plan's §1.0 table).

Corrected mapping (plan path → actual M4 path):
  - m4_result["A1"]["agreement_ratio"]  → COMPUTED from m4_result["agreement"]["approve_count"] / (approve + reject)
  - m4_result["A3"]["missing_count"]    → m4_result["missing"]["total_missing"]
  - m4_result["A3"]["response_rate"]    → COMPUTED from M3 replied / total_assigned
  - m4_result["A4"]["consecutive_rejections"] → m4_result["blocking"]["consecutive_rejections"]
  - m4_result["A6"]["days_since_last_action"] → m4_result["time"]["days_since_diffusion"]
"""

import ast
import logging
import math
from typing import Any, Optional

import pandas as pd

from .enums import validate_enum

logger = logging.getLogger(__name__)


# ============================================================================
# M3 Required Columns [Plan §1.2]
# ============================================================================

M3_REQUIRED_COLUMNS: list[str] = [
    "row_id",
    "category",
    "consensus_type",
    "priority_score",
    "is_overdue",
    "days_overdue",
    "has_deadline",
    "missing_approvers",
    "blocking_approvers",
    "total_assigned",
    "replied",
    "pending",
    "relevant_approvers",
    "days_since_diffusion",
    "source_sheet",
    "document",
]
"""Critical M3 columns consumed by M5. Missing = fatal."""


# ============================================================================
# Input Validation [Plan §1.1]
# ============================================================================

def validate_m5_inputs(
    m3_queue: pd.DataFrame,
    m4_results: list[dict],
    g1_report: pd.DataFrame,
    pipeline_run_id: str,
) -> bool:
    """Validate all M5 inputs. Raises on critical failures.

    [Plan §1.1, §1.2, §1.3]

    Args:
        m3_queue: M3 priority queue DataFrame.
        m4_results: List of M4 per-item analysis result dicts.
        g1_report: M4 G1 systemic blocker report DataFrame.
        pipeline_run_id: Non-empty string identifying the pipeline run.

    Returns:
        True if all validations pass.

    Raises:
        ValueError: If critical validation fails (M5 cannot run).
    """
    # 1.1a — m3_queue non-null, non-empty DataFrame
    if m3_queue is None or not isinstance(m3_queue, pd.DataFrame):
        raise ValueError("M5 input error: m3_queue is None or not a DataFrame.")
    if m3_queue.empty:
        logger.warning("M5: m3_queue is empty. Will return empty outputs.")
        return True  # Empty is valid but produces no results

    # 1.1b — m4_results non-null list
    if m4_results is None or not isinstance(m4_results, list):
        raise ValueError("M5 input error: m4_results is None or not a list.")

    # 1.1c — Length match check
    if len(m4_results) != len(m3_queue):
        logger.error(
            "M5: m4_results length (%d) does not match m3_queue row count (%d). "
            "Items without M4 results will be treated as analysis_degraded=true.",
            len(m4_results), len(m3_queue),
        )

    # 1.1d — pipeline_run_id non-null, non-empty
    if not pipeline_run_id or not isinstance(pipeline_run_id, str):
        raise ValueError("M5 input error: pipeline_run_id is null or empty.")

    # 1.1e — g1_report DataFrame (may be empty)
    if g1_report is not None and not isinstance(g1_report, pd.DataFrame):
        logger.error(
            "M5: g1_report is not a DataFrame (type=%s). "
            "Treating as empty — Layer 5 systemic rules will be skipped.",
            type(g1_report).__name__,
        )

    # 1.2 — M3 Required Column Check
    missing_cols = [
        col for col in M3_REQUIRED_COLUMNS
        if col not in m3_queue.columns
    ]
    if missing_cols:
        raise ValueError(
            f"M5 input error: m3_queue is missing critical columns: {missing_cols}. "
            "M5 cannot run."
        )

    return True


# ============================================================================
# Index Construction [Plan §1.4, §1.5]
# ============================================================================

def build_m4_index(m4_results: list[dict]) -> dict[str, dict]:
    """Build O(1) lookup: row_id → M4 analysis_result dict.

    [Plan §1.4] Single pass. Duplicate row_id → log ERROR, keep first.

    Args:
        m4_results: List of M4 per-item analysis result dicts.

    Returns:
        Dict mapping row_id → analysis_result.
    """
    index: dict[str, dict] = {}
    for result in m4_results:
        row_id = result.get("row_id")
        if row_id is None:
            logger.error("M5: M4 result missing 'row_id'. Skipping: %s", result)
            continue
        row_id_str = str(row_id)
        if row_id_str in index:
            logger.error(
                "M5: Duplicate row_id '%s' in m4_results. Keeping first occurrence.",
                row_id_str,
            )
            continue
        index[row_id_str] = result
    return index


def build_g1_blocker_index(g1_report: Optional[pd.DataFrame]) -> dict[str, dict]:
    """Build O(1) lookup: approver_key → G1 blocker data.

    [Plan §1.5] If g1_report is empty/None → return empty dict.
    Layer 5 systemic escalation rules will be skipped (conservative).

    Args:
        g1_report: M4 G1 systemic blocker report DataFrame.

    Returns:
        Dict mapping approver_key → {is_systemic_blocker, total_blocking,
        blocked_families, severity}.
    """
    index: dict[str, dict] = {}

    if g1_report is None or not isinstance(g1_report, pd.DataFrame) or g1_report.empty:
        if g1_report is not None and isinstance(g1_report, pd.DataFrame) and g1_report.empty:
            logger.info("M5: G1 report is empty. Systemic escalation data unavailable.")
        else:
            logger.warning(
                "M5: G1 report unavailable or invalid. "
                "Layer 5 systemic escalation rules will be skipped."
            )
        return index

    for _, row in g1_report.iterrows():
        approver_key = row.get("approver_key")
        if approver_key is None:
            continue
        approver_key_str = str(approver_key)
        index[approver_key_str] = {
            "is_systemic_blocker": bool(row.get("is_systemic_blocker", False)),
            "total_blocking": int(row.get("total_blocking", 0)),
            "blocked_families": row.get("blocked_families", []),
            "severity": row.get("severity"),
        }

    return index


# ============================================================================
# Safe M4 Field Accessors [Plan §1.0 — corrected paths]
# ============================================================================

def safe_get_m4_lifecycle_state(m4_result: dict) -> str:
    """Extract lifecycle_state from M4 result with validation.

    [Plan §1.0] Direct top-level field.

    Args:
        m4_result: M4 analysis_result dict for one item.

    Returns:
        Validated lifecycle_state string, or "ON_HOLD" if invalid/missing.
    """
    value = m4_result.get("lifecycle_state")
    if value is None:
        logger.error(
            "M5: M4 result missing 'lifecycle_state' for row_id=%s. "
            "Defaulting to ON_HOLD.",
            m4_result.get("row_id", "?"),
        )
        return "ON_HOLD"

    value_str = str(value)
    if not validate_enum(value_str, "lifecycle_state"):
        logger.error(
            "M5: Invalid lifecycle_state '%s' for row_id=%s. "
            "Treating as analysis_degraded.",
            value_str, m4_result.get("row_id", "?"),
        )
        return "ON_HOLD"

    return value_str


def safe_get_m4_analysis_degraded(m4_result: dict) -> bool:
    """Extract analysis_degraded from M4 result.

    [Plan §1.0] Direct top-level field. Fallback: assume true (conservative).

    Args:
        m4_result: M4 analysis_result dict for one item.

    Returns:
        Boolean analysis_degraded flag.
    """
    value = m4_result.get("analysis_degraded")
    if value is None:
        logger.error(
            "M5: M4 result missing 'analysis_degraded' for row_id=%s. "
            "Assuming true (conservative).",
            m4_result.get("row_id", "?"),
        )
        return True
    return bool(value)


def safe_get_m4_agreement_ratio(m4_result: dict) -> float:
    """Compute score_consensus from M4 agreement block.

    [Plan §1.0 — CORRECTED PATH]
    Plan says: m4_result["A1"]["agreement_ratio"]
    Actual M4: m4_result["agreement"]["approve_count"] / (approve + reject)

    The agreement block does NOT contain a precomputed agreement_ratio.
    We derive it: approve_count / max(approve_count + reject_count, 1).

    Args:
        m4_result: M4 analysis_result dict for one item.

    Returns:
        Float in [0.0, 1.0]. Defaults to 0.0 if data missing.
    """
    agreement = m4_result.get("agreement")
    if agreement is None or not isinstance(agreement, dict):
        logger.error(
            "M5: M4 result missing 'agreement' block for row_id=%s. "
            "Defaulting agreement_ratio to 0.0.",
            m4_result.get("row_id", "?"),
        )
        return 0.0

    approve_count = _safe_int(agreement.get("approve_count"), 0)
    reject_count = _safe_int(agreement.get("reject_count"), 0)
    total_opinionated = approve_count + reject_count

    if total_opinionated == 0:
        return 0.0

    return approve_count / total_opinionated


def safe_get_m4_missing_count(m4_result: dict) -> int:
    """Extract missing_count from M4 missing block.

    [Plan §1.0 — CORRECTED PATH]
    Plan says: m4_result["A3"]["missing_count"]
    Actual M4: m4_result["missing"]["total_missing"]

    Args:
        m4_result: M4 analysis_result dict for one item.

    Returns:
        Integer >= 0. Defaults to 0.
    """
    missing = m4_result.get("missing")
    if missing is None or not isinstance(missing, dict):
        logger.error(
            "M5: M4 result missing 'missing' block for row_id=%s. "
            "Defaulting missing_count to 0.",
            m4_result.get("row_id", "?"),
        )
        return 0

    return _safe_int(missing.get("total_missing"), 0)


def safe_get_m4_consecutive_rejections(m4_result: dict) -> int:
    """Extract consecutive_rejections from M4 blocking block.

    [Plan §1.0 — CORRECTED PATH]
    Plan says: m4_result["A4"]["consecutive_rejections"]
    Actual M4: m4_result["blocking"]["consecutive_rejections"]

    Args:
        m4_result: M4 analysis_result dict for one item.

    Returns:
        Integer >= 0. Defaults to 0 (conservative).
    """
    blocking = m4_result.get("blocking")
    if blocking is None or not isinstance(blocking, dict):
        logger.error(
            "M5: M4 result missing 'blocking' block for row_id=%s. "
            "Defaulting consecutive_rejections to 0.",
            m4_result.get("row_id", "?"),
        )
        return 0

    value = blocking.get("consecutive_rejections")
    if value is None:
        return 0
    return _safe_int(value, 0)


def safe_get_m4_days_since_last_action(m4_result: dict) -> Optional[int]:
    """Extract days_since_last_action from M4 time block.

    [Plan §1.0 — CORRECTED PATH]
    Plan says: m4_result["A6"]["days_since_last_action"]
    Actual M4: m4_result["time"]["days_since_diffusion"]

    Args:
        m4_result: M4 analysis_result dict for one item.

    Returns:
        Integer or None (informational only).
    """
    time_block = m4_result.get("time")
    if time_block is None or not isinstance(time_block, dict):
        return None

    value = time_block.get("days_since_diffusion")
    if value is None:
        return None

    try:
        if isinstance(value, float) and math.isnan(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_get_m3_response_rate(m3_item: dict) -> float:
    """Compute response_rate from M3 fields.

    [Plan §1.0 — CORRECTED PATH]
    Plan says: m4_result["A3"]["response_rate"]
    Actual: NOT in M4. Computed from M3 replied / total_assigned.

    Args:
        m3_item: M3 row as dict.

    Returns:
        Float in [0.0, 1.0]. Defaults to 0.0.
    """
    replied = _safe_int(m3_item.get("replied"), 0)
    total_assigned = _safe_int(m3_item.get("total_assigned"), 0)

    if total_assigned <= 0:
        return 0.0

    return min(replied / total_assigned, 1.0)


# ============================================================================
# Safe M3 Field Accessors
# ============================================================================

def safe_get_m3_field(m3_item: dict, field: str, default: Any = None) -> Any:
    """Safely extract a field from an M3 item dict.

    Args:
        m3_item: M3 row as dict.
        field: Field name to extract.
        default: Default value if field is missing or None.

    Returns:
        Field value or default.
    """
    value = m3_item.get(field)
    if value is None:
        return default
    # Handle pandas NaN
    if isinstance(value, float) and math.isnan(value):
        return default
    return value


def safe_get_m3_list(m3_item: dict, field: str) -> list[str]:
    """Safely extract a list field from M3 item, handling string/None.

    Args:
        m3_item: M3 row as dict.
        field: Field name to extract.

    Returns:
        List of strings. Empty list if missing/None.
    """
    value = m3_item.get(field)
    if value is None:
        return []
    if isinstance(value, str):
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
        except (ValueError, SyntaxError):
            return []
    if isinstance(value, list):
        return [str(x) for x in value]
    return []


def safe_get_m3_int(m3_item: dict, field: str, default: int = 0) -> int:
    """Safely extract an integer field from M3 item.

    Args:
        m3_item: M3 row as dict.
        field: Field name.
        default: Default value if missing/NaN.

    Returns:
        Integer value.
    """
    return _safe_int(m3_item.get(field), default)


def safe_get_m3_bool(m3_item: dict, field: str, default: bool = False) -> bool:
    """Safely extract a boolean field from M3 item.

    Args:
        m3_item: M3 row as dict.
        field: Field name.
        default: Default value if missing.

    Returns:
        Boolean value.
    """
    value = m3_item.get(field)
    if value is None:
        return default
    return bool(value)


# ============================================================================
# Internal Helpers
# ============================================================================

def _safe_int(value: Any, default: int = 0) -> int:
    """Convert a value to int safely, handling NaN and None.

    Args:
        value: Value to convert.
        default: Default if conversion fails.

    Returns:
        Integer value or default.
    """
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
