"""
JANSA VISASIST — Module 4 Output Contract Schemas
Central schema definitions for M4 outputs. All downstream consumers (M5/M6)
import from here. Module 4 itself validates against these schemas.

Schema version tracks breaking changes. Bump MAJOR on column removal/rename,
MINOR on column addition, PATCH on validation tightening.
"""

import logging
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set

import pandas as pd

logger = logging.getLogger("jansa.m4.schema")


# ============================================================================
# 1. SCHEMA VERSIONS
# ============================================================================

ANALYSIS_RESULT_SCHEMA_VERSION = "1.0.0"
G1_SCHEMA_VERSION = "1.0.0"
G2_SCHEMA_VERSION = "1.0.0"
G3_SCHEMA_VERSION = "1.0.0"
G4_SCHEMA_VERSION = "1.0.0"


# ============================================================================
# 2. ENUM REGISTRIES
# ============================================================================

VALID_LIFECYCLE_STATES: Set[str] = frozenset({
    "NOT_STARTED", "WAITING_RESPONSES", "READY_TO_ISSUE",
    "READY_TO_REJECT", "NEEDS_ARBITRATION", "CHRONIC_BLOCKED",
    "ON_HOLD", "EXCLUDED",
})

VALID_BLOCK_STATUS: Set[str] = frozenset({"OK", "FAILED"})

VALID_AGREEMENT_TYPES: Set[str] = frozenset({
    "NO_DATA", "AWAITING", "FULL_APPROVAL", "FULL_REJECTION",
    "PARTIAL_APPROVAL", "PARTIAL_REJECTION", "CONFLICT", "UNKNOWN",
})

VALID_CONFLICT_SEVERITIES: Set[str] = frozenset({"HIGH", "MEDIUM", "LOW"})

VALID_URGENCIES: Set[str] = frozenset({"CRITICAL", "HIGH", "MEDIUM", "LOW"})

VALID_BLOCKING_PATTERNS: Set[str] = frozenset({
    "NOT_BLOCKED", "CHRONIC_BLOCK", "FIRST_REJECTION",
    "PARTIAL_BLOCK", "BLOCK_WITH_PENDING",
})

VALID_DEADLINE_STATUSES: Set[str] = frozenset({
    "NO_DEADLINE", "COMFORTABLE", "APPROACHING", "URGENT",
    "DUE_TODAY", "OVERDUE", "SEVERELY_OVERDUE", "CRITICALLY_OVERDUE",
})

VALID_AGE_BRACKETS: Set[str] = frozenset({
    "UNKNOWN_AGE", "FRESH", "NORMAL", "AGING", "STALE",
})

VALID_SEVERITIES: Set[str] = frozenset({"HIGH", "MEDIUM", "LOW"})

VALID_MAJORITY_POSITIONS: Set[str] = frozenset({"APPROVE", "REJECT", "TIED"})

# Unified registry keyed by field name — mirrors module4.VALID_ENUMS
VALID_ENUMS: Dict[str, Set[str]] = {
    "agreement_type": VALID_AGREEMENT_TYPES,
    "conflict_severity": VALID_CONFLICT_SEVERITIES,
    "urgency": VALID_URGENCIES,
    "blocking_pattern": VALID_BLOCKING_PATTERNS,
    "deadline_status": VALID_DEADLINE_STATUSES,
    "age_bracket": VALID_AGE_BRACKETS,
    "lifecycle_state": VALID_LIFECYCLE_STATES,
    "severity": VALID_SEVERITIES,
    "majority_position": VALID_MAJORITY_POSITIONS,
}

# Enum safe defaults — mirrors module4.ENUM_SAFE_DEFAULTS
ENUM_SAFE_DEFAULTS: Dict[str, Optional[str]] = {
    "agreement_type": "UNKNOWN",
    "blocking_pattern": "NOT_BLOCKED",
    "deadline_status": "NO_DEADLINE",
    "age_bracket": "UNKNOWN_AGE",
    "severity": "LOW",
    "lifecycle_state": "ON_HOLD",
    "majority_position": None,
    "conflict_severity": None,
    "worst_urgency": None,
}


# ============================================================================
# 3. ANALYSIS RESULT SCHEMA (per-item dict)
# ============================================================================

ANALYSIS_RESULT_REQUIRED_KEYS = (
    "row_id",
    "agreement",
    "conflict",
    "missing",
    "blocking",
    "delta",
    "time",
    "lifecycle_state",
    "analysis_degraded",
    "failed_blocks",
)

ANALYSIS_RESULT_BLOCK_NAMES = (
    "agreement", "conflict", "missing", "blocking", "delta", "time",
)

# Each A-block must contain at minimum block_status
BLOCK_REQUIRED_KEYS = ("block_status",)


# ============================================================================
# 4. DATAFRAME SCHEMAS (G1–G4)
# ============================================================================
# OrderedDict enforces canonical column order.
# Each entry: {dtype, nullable, col_type}
# col_type ∈ {identifier, string, numeric, bool, list, enum}

G1_SCHEMA = OrderedDict([
    ("approver_key",          {"dtype": "str",   "nullable": False, "col_type": "identifier"}),
    ("display_name",          {"dtype": "str",   "nullable": False, "col_type": "identifier"}),
    ("total_latest_assigned", {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("total_responded",       {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("total_blocking",        {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("blocking_rate",         {"dtype": "float", "nullable": False, "col_type": "numeric"}),
    ("avg_response_days",     {"dtype": "float", "nullable": True,  "col_type": "numeric"}),
    ("is_systemic_blocker",   {"dtype": "bool",  "nullable": False, "col_type": "bool"}),
    ("blocked_families",      {"dtype": "list",  "nullable": False, "col_type": "list"}),
    ("severity",              {"dtype": "severity", "nullable": False, "col_type": "enum"}),
])

G2_SCHEMA = OrderedDict([
    ("doc_family_key",      {"dtype": "str",  "nullable": False, "col_type": "identifier"}),
    ("source_sheet",        {"dtype": "str",  "nullable": False, "col_type": "identifier"}),
    ("document",            {"dtype": "str",  "nullable": True,  "col_type": "string"}),
    ("titre",               {"dtype": "str",  "nullable": True,  "col_type": "string"}),
    ("is_looping",          {"dtype": "bool", "nullable": False, "col_type": "bool"}),
    ("loop_length",         {"dtype": "int",  "nullable": False, "col_type": "numeric"}),
    ("loop_start_ind",      {"dtype": "str",  "nullable": True,  "col_type": "string"}),
    ("loop_end_ind",        {"dtype": "str",  "nullable": True,  "col_type": "string"}),
    ("persistent_blockers", {"dtype": "list", "nullable": False, "col_type": "list"}),
    ("latest_visa_global",  {"dtype": "str",  "nullable": True,  "col_type": "string"}),
])

G3_SCHEMA = OrderedDict([
    ("row_id",                {"dtype": "str",  "nullable": False, "col_type": "identifier"}),
    ("risk_score",            {"dtype": "int",  "nullable": False, "col_type": "numeric"}),
    ("is_high_risk",          {"dtype": "bool", "nullable": False, "col_type": "bool"}),
    ("contributing_factors",  {"dtype": "list", "nullable": False, "col_type": "list"}),
    ("factor_details",        {"dtype": "list", "nullable": False, "col_type": "list"}),
])

G4_SCHEMA = OrderedDict([
    ("source_sheet",          {"dtype": "str",   "nullable": False, "col_type": "identifier"}),
    ("total_documents",       {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("total_pending",         {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("total_overdue",         {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("total_high_risk",       {"dtype": "int",   "nullable": False, "col_type": "numeric"}),
    ("category_distribution", {"dtype": "dict",  "nullable": False, "col_type": "list"}),
    ("approval_rate",         {"dtype": "float", "nullable": False, "col_type": "numeric"}),
    ("avg_priority_score",    {"dtype": "float", "nullable": True,  "col_type": "numeric"}),
    ("avg_days_pending",      {"dtype": "float", "nullable": True,  "col_type": "numeric"}),
    ("is_high_risk_cluster",  {"dtype": "bool",  "nullable": False, "col_type": "bool"}),
    ("health_score",          {"dtype": "float", "nullable": False, "col_type": "numeric"}),
])

# Canonical column order tuples — for downstream column enforcement
G1_COLUMNS = tuple(G1_SCHEMA.keys())
G2_COLUMNS = tuple(G2_SCHEMA.keys())
G3_COLUMNS = tuple(G3_SCHEMA.keys())
G4_COLUMNS = tuple(G4_SCHEMA.keys())

# Map schema name -> (schema dict, column tuple, version)
SCHEMA_REGISTRY = {
    "G1": (G1_SCHEMA, G1_COLUMNS, G1_SCHEMA_VERSION),
    "G2": (G2_SCHEMA, G2_COLUMNS, G2_SCHEMA_VERSION),
    "G3": (G3_SCHEMA, G3_COLUMNS, G3_SCHEMA_VERSION),
    "G4": (G4_SCHEMA, G4_COLUMNS, G4_SCHEMA_VERSION),
}


# ============================================================================
# 5. VALIDATION EXCEPTIONS
# ============================================================================

class M4SchemaValidationError(Exception):
    """Raised when M4 output fails contract validation."""

    def __init__(self, errors: List[str], context: str = ""):
        self.errors = errors
        self.context = context
        msg = f"M4 schema validation failed ({context}): {len(errors)} error(s)"
        if errors:
            msg += f" — first: {errors[0]}"
        super().__init__(msg)


# ============================================================================
# 6. VALIDATION HELPERS
# ============================================================================

def validate_analysis_result_schema(result: dict) -> List[str]:
    """Validate a single analysis_result dict against the contract.

    Returns a list of validation error strings (empty = valid).
    Does NOT raise — caller decides how to handle errors.
    """
    errors = []

    # 6.1 Required top-level keys
    for k in ANALYSIS_RESULT_REQUIRED_KEYS:
        if k not in result:
            errors.append(f"Missing top-level key: {k}")

    # 6.2 analysis_degraded must be bool
    ad = result.get("analysis_degraded")
    if ad is not None and not isinstance(ad, bool):
        errors.append(f"analysis_degraded is not bool: {type(ad).__name__}")

    # 6.3 failed_blocks must be list
    fb = result.get("failed_blocks")
    if fb is not None and not isinstance(fb, list):
        errors.append(f"failed_blocks is not a list: {type(fb).__name__}")

    # 6.4 All 6 block sub-dicts must be present and be dicts
    for block_name in ANALYSIS_RESULT_BLOCK_NAMES:
        block = result.get(block_name)
        if block is None:
            errors.append(f"Block '{block_name}' is missing")
        elif not isinstance(block, dict):
            errors.append(f"Block '{block_name}' is not a dict: {type(block).__name__}")

    # 6.5 Every block must have block_status ∈ VALID_BLOCK_STATUS
    for block_name in ANALYSIS_RESULT_BLOCK_NAMES:
        block = result.get(block_name)
        if isinstance(block, dict):
            bs = block.get("block_status")
            if bs is None:
                errors.append(f"Block '{block_name}' missing block_status")
            elif bs not in VALID_BLOCK_STATUS:
                errors.append(f"Block '{block_name}' invalid block_status: {bs!r}")

    # 6.6 lifecycle_state must be valid enum
    ls = result.get("lifecycle_state")
    if ls is not None and ls not in VALID_LIFECYCLE_STATES:
        errors.append(f"Invalid lifecycle_state: {ls!r}")

    # 6.7 agreement_type
    ag = result.get("agreement")
    if isinstance(ag, dict):
        at = ag.get("agreement_type")
        if at is not None and at not in VALID_AGREEMENT_TYPES:
            errors.append(f"Invalid agreement_type: {at!r}")

    # 6.8 blocking_pattern
    bl = result.get("blocking")
    if isinstance(bl, dict):
        bp = bl.get("blocking_pattern")
        if bp is not None and bp not in VALID_BLOCKING_PATTERNS:
            errors.append(f"Invalid blocking_pattern: {bp!r}")

    # 6.9 deadline_status and age_bracket
    tm = result.get("time")
    if isinstance(tm, dict):
        ds = tm.get("deadline_status")
        if ds is not None and ds not in VALID_DEADLINE_STATUSES:
            errors.append(f"Invalid deadline_status: {ds!r}")
        ab = tm.get("age_bracket")
        if ab is not None and ab not in VALID_AGE_BRACKETS:
            errors.append(f"Invalid age_bracket: {ab!r}")

    # 6.10 failed_blocks consistency with analysis_degraded
    if isinstance(fb, list) and isinstance(ad, bool):
        if len(fb) > 0 and not ad:
            errors.append(
                f"failed_blocks has {len(fb)} entries but analysis_degraded is False"
            )
        if len(fb) == 0 and ad:
            # This is allowed (schema validation failure can set degraded=True)
            # but we still log for awareness
            pass

    return errors


def validate_analysis_results_list(results: List[dict]) -> None:
    """Validate the full per_item_results list. Raises M4SchemaValidationError on fatal issues."""
    if not isinstance(results, list):
        raise M4SchemaValidationError(
            [f"per_item_results is not a list: {type(results).__name__}"],
            context="per_item_results",
        )

    all_errors = []
    for i, result in enumerate(results):
        if not isinstance(result, dict):
            all_errors.append(f"Item {i} is not a dict: {type(result).__name__}")
            continue
        errs = validate_analysis_result_schema(result)
        for e in errs:
            row_id = result.get("row_id", f"index_{i}")
            all_errors.append(f"row={row_id}: {e}")

    if all_errors:
        for e in all_errors:
            logger.error("Contract validation: %s", e)
        # Do NOT raise — follow existing deterministic fallback philosophy.
        # Caller (module4) marks items degraded individually.


def validate_dataframe_schema(
    df: pd.DataFrame,
    schema_name: str,
    strict_columns: bool = True,
) -> List[str]:
    """Validate a G1/G2/G3/G4 DataFrame against its registered schema.

    Args:
        df: The DataFrame to validate.
        schema_name: One of "G1", "G2", "G3", "G4".
        strict_columns: If True, warn on extra columns not in schema.

    Returns:
        List of validation error strings (empty = valid).
    """
    reg = SCHEMA_REGISTRY.get(schema_name)
    if reg is None:
        return [f"Unknown schema: {schema_name!r}"]

    schema, expected_cols, version = reg
    errors = []
    existing_cols = set(df.columns)
    expected_set = set(expected_cols)

    # Check missing columns
    missing = expected_set - existing_cols
    if missing:
        for col in missing:
            col_spec = schema[col]
            col_type = col_spec.get("col_type", "string")
            if col_type == "identifier":
                errors.append(
                    f"{schema_name}: Missing required identifier column '{col}'. "
                    "Cannot synthesize safe default."
                )
            else:
                errors.append(f"{schema_name}: Missing column '{col}' (type={col_type})")

    # Check extra columns (warn, don't error — tolerated but noted)
    if strict_columns:
        extra = existing_cols - expected_set
        if extra:
            logger.warning(
                "%s v%s: Extra columns not in schema: %s", schema_name, version, extra,
            )

    # Column order check — must match canonical order for deterministic output
    if not missing:
        actual_order = [c for c in df.columns if c in expected_set]
        canonical_order = list(expected_cols)
        if actual_order != canonical_order:
            errors.append(
                f"{schema_name}: Column order mismatch. "
                f"Expected {canonical_order}, got {actual_order}"
            )

    # Nullability checks on existing columns
    for col_name, col_spec in schema.items():
        if col_name not in existing_cols:
            continue
        nullable = col_spec.get("nullable", True)
        if not nullable:
            null_count = df[col_name].isna().sum() if hasattr(df[col_name], "isna") else 0
            if null_count > 0:
                errors.append(
                    f"{schema_name}: Column '{col_name}' has {null_count} null values (non-nullable)"
                )

    # Enum value checks
    for col_name, col_spec in schema.items():
        if col_name not in existing_cols:
            continue
        if col_spec.get("col_type") != "enum":
            continue
        dtype_key = col_spec.get("dtype", col_name)
        allowed = VALID_ENUMS.get(col_name) or VALID_ENUMS.get(dtype_key)
        if not allowed:
            continue
        for i, val in enumerate(df[col_name]):
            if val is not None and val not in allowed:
                errors.append(
                    f"{schema_name}: Invalid enum value in '{col_name}' row {i}: {val!r}"
                )

    return errors


def enforce_column_order(df: pd.DataFrame, schema_name: str) -> pd.DataFrame:
    """Reorder DataFrame columns to match canonical schema order.
    Extra columns are appended at the end. Missing columns are NOT added.
    Returns a new DataFrame (does not mutate in place).
    """
    reg = SCHEMA_REGISTRY.get(schema_name)
    if reg is None:
        return df
    _, expected_cols, _ = reg
    existing = set(df.columns)
    ordered = [c for c in expected_cols if c in existing]
    extra = [c for c in df.columns if c not in set(expected_cols)]
    return df[ordered + extra]


def empty_dataframe(schema_name: str) -> pd.DataFrame:
    """Return an empty DataFrame with canonical column order for the given schema."""
    reg = SCHEMA_REGISTRY.get(schema_name)
    if reg is None:
        return pd.DataFrame()
    _, expected_cols, _ = reg
    return pd.DataFrame(columns=list(expected_cols))
