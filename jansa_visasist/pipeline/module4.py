"""
JANSA VISASIST — Module 4: Analysis Engine
V2.2.2 E1–E3 compliant. Implementation Plan V3 (9 patches, coding-ready).

Tier 2 in the pipeline orchestration. Consumes M1 + M2 + M3 outputs and produces:
  Part A: 6 per-item analysis blocks (A1–A6) for every M3 queue item
  Part B: 4 global analyses (G1–G4) computed once per pipeline run
  Part C: A lifecycle_state per item (V2.2.2 E2)

All outputs are deterministic. Zero AI. Computed once per pipeline run, persisted.
Authority level: DERIVED AUTHORITATIVE.
"""

import datetime
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from jansa_visasist.schemas.m4_schema import (
    ANALYSIS_RESULT_SCHEMA_VERSION,
    G1_SCHEMA_VERSION,
    G2_SCHEMA_VERSION,
    G3_SCHEMA_VERSION,
    G4_SCHEMA_VERSION,
    VALID_BLOCK_STATUS as SCHEMA_VALID_BLOCK_STATUS,
    VALID_LIFECYCLE_STATES as SCHEMA_VALID_LIFECYCLE_STATES,
    ANALYSIS_RESULT_REQUIRED_KEYS,
    ANALYSIS_RESULT_BLOCK_NAMES,
    G1_SCHEMA as SCHEMA_G1,
    G2_SCHEMA as SCHEMA_G2,
    G3_SCHEMA as SCHEMA_G3,
    G4_SCHEMA as SCHEMA_G4,
    G1_COLUMNS,
    G2_COLUMNS,
    G3_COLUMNS,
    G4_COLUMNS,
    SCHEMA_REGISTRY,
    validate_analysis_result_schema as schema_validate_analysis_result,
    validate_analysis_results_list as schema_validate_results_list,
    validate_dataframe_schema as schema_validate_dataframe,
    enforce_column_order,
    empty_dataframe,
    M4SchemaValidationError,
)

logger = logging.getLogger("jansa.m4")

# ============================================================================
# 1. CONSTANTS AND ENUMS
# ============================================================================

# Thresholds — named constants, never magic numbers
SYSTEMIC_BLOCKER_THRESHOLD = 3
HIGH_RISK_THRESHOLD = 3
HIGH_RISK_CLUSTER_THRESHOLD = 5

# Risk factor weights [SPEC]
RISK_WEIGHTS = {"F1": 3, "F2": 3, "F3": 2, "F4": 2, "F5": 1, "F6": 1}

# Health score component weights [SPEC]
HEALTH_OVERDUE_WEIGHT = 40
HEALTH_RISK_WEIGHT = 30
HEALTH_APPROVAL_WEIGHT = 30

# Canonical approver dictionary (14 keys) [SPEC]
CANONICAL_APPROVERS = [
    "MOEX_GEMO", "ARCHI_MOX", "BET_STR_TERRELL", "BET_GEOLIA_G4",
    "ACOUSTICIEN_AVLS", "AMO_HQE_LE_SOMMER", "BET_POLLUTION_DIE",
    "SOCOTEC", "BET_ELIOTH", "BET_EGIS", "BET_ASCAUDIT",
    "BET_ASCENSEUR", "BET_SPK", "PAYSAGISTE_MUGO",
]
CANONICAL_APPROVER_SET = set(CANONICAL_APPROVERS)

# Display names — hardcoded canonical dictionary [SPEC — Patch V2]
APPROVER_DISPLAY_NAMES = {
    "MOEX_GEMO": "MOEX GEMO",
    "ARCHI_MOX": "ARCHI MOX",
    "BET_STR_TERRELL": "BET STR-TERRELL",
    "BET_GEOLIA_G4": "BET GEOLIA - G4",
    "ACOUSTICIEN_AVLS": "ACOUSTICIEN AVLS",
    "AMO_HQE_LE_SOMMER": "AMO HQE LE SOMMER",
    "BET_POLLUTION_DIE": "BET POLLUTION DIE",
    "SOCOTEC": "SOCOTEC",
    "BET_ELIOTH": "BET ELIOTH",
    "BET_EGIS": "BET EGIS",
    "BET_ASCAUDIT": "BET ASCAUDIT",
    "BET_ASCENSEUR": "BET ASCENSEUR",
    "BET_SPK": "BET SPK",
    "PAYSAGISTE_MUGO": "PAYSAGISTE MUGO",
}

# Enum safe defaults (Fix #21)
ENUM_SAFE_DEFAULTS = {
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

# lifecycle_state <-> M3 category compatibility [SPEC — Phase 2 spec §3.3, Patch V2/V3]
LIFECYCLE_CATEGORY_COMPAT = {
    "NOT_STARTED": "NOT_STARTED",
    "WAITING_RESPONSES": "WAITING",
    "READY_TO_ISSUE": "EASY_WIN_APPROVE",
    "READY_TO_REJECT": "FAST_REJECT",
    "NEEDS_ARBITRATION": "CONFLICT",
    "CHRONIC_BLOCKED": "BLOCKED",
    "ON_HOLD": None,  # compatible with any category
}

# Reverse: M3 category -> implied lifecycle_state (for contradiction override)
CATEGORY_TO_LIFECYCLE = {
    "EASY_WIN_APPROVE": "READY_TO_ISSUE",
    "FAST_REJECT": "READY_TO_REJECT",
    "BLOCKED": "CHRONIC_BLOCKED",
    "CONFLICT": "NEEDS_ARBITRATION",
    "WAITING": "WAITING_RESPONSES",
    "NOT_STARTED": "NOT_STARTED",
}

# Fix #1: agreement_type -> expected consensus_type normative mapping
AGREEMENT_CONSENSUS_MAP = {
    "NO_DATA": "NOT_STARTED",
    "AWAITING": "INCOMPLETE",
    "FULL_APPROVAL": "ALL_APPROVE",
    "FULL_REJECTION": "ALL_REJECT",
    "PARTIAL_APPROVAL": "INCOMPLETE",
    "PARTIAL_REJECTION": "INCOMPLETE",
    "CONFLICT": "MIXED",
    "UNKNOWN": None,  # no validation for UNKNOWN
}

# Allowed enum values [GP8]
VALID_ENUMS = {
    "agreement_type": {"NO_DATA", "AWAITING", "FULL_APPROVAL", "FULL_REJECTION",
                       "PARTIAL_APPROVAL", "PARTIAL_REJECTION", "CONFLICT", "UNKNOWN"},
    "conflict_severity": {"HIGH", "MEDIUM", "LOW"},
    "urgency": {"CRITICAL", "HIGH", "MEDIUM", "LOW"},
    "blocking_pattern": {"NOT_BLOCKED", "CHRONIC_BLOCK", "FIRST_REJECTION",
                         "PARTIAL_BLOCK", "BLOCK_WITH_PENDING"},
    "deadline_status": {"NO_DEADLINE", "COMFORTABLE", "APPROACHING", "URGENT",
                        "DUE_TODAY", "OVERDUE", "SEVERELY_OVERDUE", "CRITICALLY_OVERDUE"},
    "age_bracket": {"UNKNOWN_AGE", "FRESH", "NORMAL", "AGING", "STALE"},
    "lifecycle_state": {"NOT_STARTED", "WAITING_RESPONSES", "READY_TO_ISSUE",
                        "READY_TO_REJECT", "NEEDS_ARBITRATION", "CHRONIC_BLOCKED",
                        "ON_HOLD", "EXCLUDED"},
    "severity": {"HIGH", "MEDIUM", "LOW"},
    "majority_position": {"APPROVE", "REJECT", "TIED"},
}

# Statut classification sets
APPROVE_STATUTS = {"VSO", "VAO"}
REJECT_STATUTS = {"REF"}
HM_STATUTS = {"HM"}
CLASSIFIED_STATUTS = APPROVE_STATUTS | REJECT_STATUTS | HM_STATUTS

# Urgency ordering for comparison
URGENCY_ORDER = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}

# Non-rejection visa_global values that break the chain (BK1)
BK1_NON_REJECTION_VISAS = {"VAO", "VSO", "SUS", "DEF", "FAV"}


# ============================================================================
# 2. SAFE DEFAULT DEFINITIONS (Fix #21)
# ============================================================================

def _a1_safe_defaults() -> dict:
    """[Fix #21] Safe defaults for A1 block failure."""
    return {
        "agreement_type": "UNKNOWN", "approve_count": 0, "reject_count": 0,
        "pending_count": 0, "hm_count": 0, "non_classifiable_count": 0,
        "approve_list": [], "reject_list": [], "pending_list": [],
        "non_classifiable_list": [], "agreement_detail": "Analyse indisponible",
        "consensus_match": False, "block_status": "FAILED",
    }


def _a2_safe_defaults() -> dict:
    """[Fix #21] Safe defaults for A2 block failure."""
    return {
        "conflict_detected": False, "conflict_severity": None,
        "majority_position": None, "approvers_against_majority": [],
        "conflict_detail": "Analyse indisponible", "block_status": "FAILED",
    }


def _a3_safe_defaults() -> dict:
    """[Fix #21] Safe defaults for A3 block failure."""
    return {
        "missing_approvers": [], "total_missing": 0, "worst_urgency": None,
        "critical_missing": [], "missing_summary": "Analyse indisponible",
        "block_status": "FAILED",
    }


def _a4_safe_defaults() -> dict:
    """[Fix #21] Safe defaults for A4 block failure."""
    return {
        "is_blocked": False, "blocking_pattern": "NOT_BLOCKED",
        "blocking_approvers": [], "is_systemic_blocker": [],
        "consecutive_rejections": None, "blocking_detail": "Analyse indisponible",
        "block_status": "FAILED",
    }


def _a5_safe_defaults() -> dict:
    """[Fix #21] Safe defaults for A5 block failure."""
    return {
        "has_previous": False, "previous_ind": None, "visa_global_change": None,
        "approver_changes": [], "total_changed": 0, "new_responses": 0,
        "lost_responses": 0, "reversals": 0, "delta_summary": "Analyse indisponible",
        "block_status": "FAILED",
    }


def _a6_safe_defaults() -> dict:
    """[Fix #21] Safe defaults for A6 block failure."""
    return {
        "days_since_diffusion": None, "days_until_deadline": None,
        "is_overdue": False, "days_overdue": 0, "has_deadline": False,
        "deadline_status": "NO_DEADLINE", "age_bracket": "UNKNOWN_AGE",
        "time_summary": "Analyse indisponible", "block_status": "FAILED",
    }


BLOCK_SAFE_DEFAULTS = {
    "A1": _a1_safe_defaults, "A2": _a2_safe_defaults, "A3": _a3_safe_defaults,
    "A4": _a4_safe_defaults, "A5": _a5_safe_defaults, "A6": _a6_safe_defaults,
}


def apply_block_safe_defaults(block_id: str) -> dict:
    """[Fix #21] Return exact safe default dict for a given block ID."""
    return BLOCK_SAFE_DEFAULTS[block_id]()


# ============================================================================
# 3. VALIDATION HELPERS
# ============================================================================

M1_REQUIRED_COLS = {
    "row_id", "visa_global", "date_diffusion", "date_contractuelle_visa",
    "assigned_approvers", "ind", "ind_raw", "titre", "document", "document_raw",
    "source_sheet", "source_row", "lot", "type_doc", "row_quality",
}

M2_EXTRA_COLS = {
    "doc_family_key", "doc_version_key", "revision_count", "is_latest",
    "previous_version_key", "is_cross_lot", "cross_lot_sheets",
    "duplicate_flag", "ind_sort_order",
}

M3_EXTRA_COLS = {
    "priority_score", "category", "consensus_type", "days_since_diffusion",
    "days_until_deadline", "is_overdue", "days_overdue", "has_deadline",
    "total_assigned", "total_replied", "total_pending",
    "approvers_vso", "approvers_vao", "approvers_ref", "approvers_hm",
    "relevant_approvers", "missing_approvers", "blocking_approvers",
}


def validate_m4_inputs(
    m1_master: pd.DataFrame,
    m2_enriched: pd.DataFrame,
    m3_queue: pd.DataFrame,
    reference_date: datetime.date,
) -> None:
    """[SAFEGUARD] Verify M1/M2/M3 DataFrames have required columns. Raises on fatal issues."""
    if m1_master is None or not isinstance(m1_master, pd.DataFrame):
        raise ValueError("M4 input error: m1_master is not a valid DataFrame")
    if m2_enriched is None or not isinstance(m2_enriched, pd.DataFrame):
        raise ValueError("M4 input error: m2_enriched is not a valid DataFrame")
    if m3_queue is None or not isinstance(m3_queue, pd.DataFrame):
        raise ValueError("M4 input error: m3_queue is not a valid DataFrame")
    if not isinstance(reference_date, datetime.date):
        raise ValueError("M4 input error: reference_date is not a valid date")

    # Check M1 columns
    m1_cols = set(m1_master.columns)
    missing_m1 = M1_REQUIRED_COLS - m1_cols
    if missing_m1:
        raise ValueError(f"M4 input error: M1 missing critical columns: {missing_m1}")

    # Check M2 columns (M1 + M2 extras)
    m2_cols = set(m2_enriched.columns)
    missing_m2 = (M1_REQUIRED_COLS | M2_EXTRA_COLS) - m2_cols
    if missing_m2:
        raise ValueError(f"M4 input error: M2 missing critical columns: {missing_m2}")

    # Check M3 columns (M2 + M3 extras)
    m3_cols = set(m3_queue.columns)
    missing_m3 = M3_EXTRA_COLS - m3_cols
    if missing_m3:
        raise ValueError(f"M4 input error: M3 missing critical columns: {missing_m3}")


def validate_enum_value(value: Any, enum_name: str) -> str:
    """[GP8] Check value against allowed enum set. Returns value or safe default."""
    if value is None and ENUM_SAFE_DEFAULTS.get(enum_name) is None:
        return None
    allowed = VALID_ENUMS.get(enum_name)
    if allowed is None:
        return value
    if value in allowed:
        return value
    logger.error("Invalid enum value: field=%s value=%r allowed=%s", enum_name, value, allowed)
    return ENUM_SAFE_DEFAULTS.get(enum_name, value)


# ============================================================================
# 4. INDEX BUILDERS
# ============================================================================

def _safe_get(row_dict: dict, key: str, default=None):
    """Safely retrieve a value from a row dict, returning default if absent or NaN."""
    val = row_dict.get(key, default)
    if val is None:
        return default
    if isinstance(val, float) and pd.isna(val):
        return default
    return val


def _row_to_dict(row) -> dict:
    """Convert a DataFrame row (Series) to a plain dict, replacing NaN with None."""
    d = row.to_dict() if hasattr(row, "to_dict") else dict(row)
    return {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in d.items()}


def build_version_index(m2_enriched: pd.DataFrame) -> dict:
    """[IMPLEMENTATION] Build doc_version_key -> M2 row dict. O(1) lookup for A4/A5."""
    idx = {}
    for _, row in m2_enriched.iterrows():
        rd = _row_to_dict(row)
        key = rd.get("doc_version_key")
        if key is None:
            continue
        if key in idx:
            logger.error("Duplicate doc_version_key in M2: %s", key)
            continue  # keep first
        idx[key] = rd
    return idx


def build_chain_index(m2_enriched: pd.DataFrame) -> dict:
    """[IMPLEMENTATION] Build (doc_family_key, source_sheet) -> sorted revision list."""
    idx = {}
    for _, row in m2_enriched.iterrows():
        rd = _row_to_dict(row)
        fk = rd.get("doc_family_key")
        ss = rd.get("source_sheet")
        if fk is None or ss is None:
            continue
        key = (fk, ss)
        if key not in idx:
            idx[key] = []
        idx[key].append(rd)
    # Sort each chain by ind_sort_order ascending
    for key in idx:
        idx[key].sort(key=lambda r: r.get("ind_sort_order") or 0)
    return idx


def build_queue_index(m3_queue: pd.DataFrame) -> dict:
    """[IMPLEMENTATION] Build row_id -> M3 queue row dict."""
    idx = {}
    for _, row in m3_queue.iterrows():
        rd = _row_to_dict(row)
        rid = rd.get("row_id")
        if rid is not None:
            idx[rid] = rd
    return idx


def build_sheet_index(m3_queue: pd.DataFrame) -> dict:
    """[IMPLEMENTATION] Build source_sheet -> list of queue row_ids."""
    idx = {}
    for _, row in m3_queue.iterrows():
        ss = row.get("source_sheet")
        rid = row.get("row_id")
        if ss is not None:
            if isinstance(ss, float) and pd.isna(ss):
                continue
            if ss not in idx:
                idx[ss] = []
            idx[ss].append(rid)
    return idx


def build_latest_index(m2_enriched: pd.DataFrame) -> dict:
    """[IMPLEMENTATION] Build (doc_family_key, source_sheet) -> is_latest row dict."""
    idx = {}
    for _, row in m2_enriched.iterrows():
        if not row.get("is_latest"):
            continue
        rd = _row_to_dict(row)
        fk = rd.get("doc_family_key")
        ss = rd.get("source_sheet")
        if fk is not None and ss is not None:
            idx[(fk, ss)] = rd
    return idx


# ============================================================================
# 5. APPROVER COLUMN DISCOVERY
# ============================================================================

def discover_approver_columns(m2_enriched: pd.DataFrame) -> dict:
    """[SPEC] Discover approver columns dynamically from assigned_approvers.
    Returns approver_col_map: {approver_key: {statut_col, date_col, ...}}
    """
    all_keys = set()
    for val in m2_enriched["assigned_approvers"]:
        if isinstance(val, list):
            all_keys.update(val)
        elif isinstance(val, str):
            # Handle case where it might be stored as string repr
            try:
                import ast
                parsed = ast.literal_eval(val)
                if isinstance(parsed, list):
                    all_keys.update(parsed)
            except (ValueError, SyntaxError):
                pass

    col_set = set(m2_enriched.columns)
    approver_col_map = {}

    for key in all_keys:
        statut_col = f"{key}_statut"
        date_col = f"{key}_date"
        n_col = f"{key}_n"
        raw_col = f"{key}_statut_raw"

        found = {}
        if statut_col in col_set:
            found["statut_col"] = statut_col
        else:
            logger.warning("Approver %s: expected column %s not found in M2", key, statut_col)
        if date_col in col_set:
            found["date_col"] = date_col
        if n_col in col_set:
            found["n_col"] = n_col
        if raw_col in col_set:
            found["raw_col"] = raw_col

        approver_col_map[key] = found

        # Validate against canonical set
        if key not in CANONICAL_APPROVER_SET:
            logger.warning("Unexpected approver key discovered: %s", key)

    # Check for canonical keys never seen
    for ck in CANONICAL_APPROVERS:
        if ck not in all_keys:
            logger.info("Canonical approver %s never appears in assigned_approvers", ck)

    return approver_col_map


# ============================================================================
# 6. APPROVER SET PARTITIONING
# ============================================================================

@dataclass
class ApproverSets:
    """Partitioned approver sets for a single item. [SPEC]"""
    approve_set: List[str] = field(default_factory=list)
    reject_set: List[str] = field(default_factory=list)
    pending_set: List[str] = field(default_factory=list)
    hm_set: List[str] = field(default_factory=list)
    non_classifiable_set: List[str] = field(default_factory=list)
    # Derived
    opinionated: List[str] = field(default_factory=list)
    responded_non_hm: List[str] = field(default_factory=list)


def partition_approver_sets(
    item_row: dict,
    approver_col_map: dict,
) -> ApproverSets:
    """[SPEC] Partition assigned_approvers into 5 mutually exclusive primary sets + 2 derived.

    [SAFEGUARD — Patch V2] If approver key has no matching statut column ->
    treat as pending (null statut), log ERROR (upstream schema anomaly, NOT M4 failure).
    """
    assigned = item_row.get("assigned_approvers") or []
    if isinstance(assigned, str):
        try:
            import ast
            assigned = ast.literal_eval(assigned)
        except (ValueError, SyntaxError):
            assigned = []

    sets = ApproverSets()
    row_id = item_row.get("row_id", "?")

    for key in assigned:
        col_info = approver_col_map.get(key, {})
        statut_col = col_info.get("statut_col")

        if statut_col is None:
            # [Patch V2] Upstream schema anomaly — log ERROR, treat as pending
            logger.error(
                "Missing statut column for approver %s on row %s "
                "(expected %s_statut). Treating as pending. "
                "This is an upstream schema anomaly, not an M4 computation failure.",
                key, row_id, key,
            )
            sets.pending_set.append(key)
            continue

        statut = _safe_get(item_row, statut_col)

        if statut is None:
            sets.pending_set.append(key)
        elif statut in APPROVE_STATUTS:
            sets.approve_set.append(key)
        elif statut in REJECT_STATUTS:
            sets.reject_set.append(key)
        elif statut in HM_STATUTS:
            sets.hm_set.append(key)
        else:
            # SUS, DEF, FAV, or unknown -> non_classifiable
            sets.non_classifiable_set.append(key)

    # Derived sets
    sets.opinionated = sets.approve_set + sets.reject_set
    sets.responded_non_hm = sets.approve_set + sets.reject_set + sets.non_classifiable_set

    return sets


# ============================================================================
# 7. A-BLOCK FUNCTIONS
# ============================================================================

# --- A1: Agreement Detection ---

def compute_agreement(
    item_row: dict,
    approver_sets: ApproverSets,
    consensus_type: Optional[str],
) -> dict:
    """[SPEC] A1: Agreement Detection. R1-R8 first-match-wins + Fix #1 validation."""
    approve_count = len(approver_sets.approve_set)
    reject_count = len(approver_sets.reject_set)
    pending_count = len(approver_sets.pending_set)
    hm_count = len(approver_sets.hm_set)
    nc_count = len(approver_sets.non_classifiable_set)
    opinionated_empty = len(approver_sets.opinionated) == 0

    # R1-R8: first match wins [SPEC]
    if opinionated_empty and pending_count == 0:
        agreement_type = "NO_DATA"           # R1
    elif opinionated_empty and pending_count > 0:
        agreement_type = "AWAITING"          # R2
    elif reject_count == 0 and approve_count > 0 and pending_count == 0:
        agreement_type = "FULL_APPROVAL"     # R3
    elif approve_count == 0 and reject_count > 0 and pending_count == 0:
        agreement_type = "FULL_REJECTION"    # R4
    elif reject_count == 0 and approve_count > 0 and pending_count > 0:
        agreement_type = "PARTIAL_APPROVAL"  # R5
    elif approve_count == 0 and reject_count > 0 and pending_count > 0:
        agreement_type = "PARTIAL_REJECTION" # R6
    elif reject_count > 0 and approve_count > 0:
        agreement_type = "CONFLICT"          # R7
    else:
        agreement_type = "UNKNOWN"           # R8 fallback — should not occur
        logger.error("A1 R8 fallback reached for row %s", item_row.get("row_id"))

    agreement_type = validate_enum_value(agreement_type, "agreement_type")

    # Fix #1: Normative mapping validation
    expected_ct = AGREEMENT_CONSENSUS_MAP.get(agreement_type)
    if expected_ct is not None and consensus_type is not None:
        consensus_match = (expected_ct == consensus_type)
        if not consensus_match:
            logger.error(
                "Fix #1 consensus mismatch: row=%s agreement_type=%s expected_consensus=%s actual_consensus=%s",
                item_row.get("row_id"), agreement_type, expected_ct, consensus_type,
            )
    else:
        consensus_match = True  # UNKNOWN or null consensus -> no validation

    total = approve_count + reject_count + pending_count + hm_count + nc_count
    detail = (
        f"{total} intervenants assignés : {approve_count} approbation(s), "
        f"{reject_count} rejet(s), {pending_count} en attente, "
        f"{hm_count} HM, {nc_count} autre(s)."
    )

    return {
        "agreement_type": agreement_type,
        "approve_count": approve_count,
        "reject_count": reject_count,
        "pending_count": pending_count,
        "hm_count": hm_count,
        "non_classifiable_count": nc_count,
        "approve_list": list(approver_sets.approve_set),
        "reject_list": list(approver_sets.reject_set),
        "pending_list": list(approver_sets.pending_set),
        "non_classifiable_list": list(approver_sets.non_classifiable_set),
        "agreement_detail": detail,
        "consensus_match": consensus_match,
        "block_status": "OK",
    }


# --- A2: Conflict Detection ---

def compute_conflict(a1_result: dict) -> dict:
    """[SPEC] A2: Conflict Detection. S1-S4 first-match-wins."""
    if a1_result.get("agreement_type") != "CONFLICT":
        return {
            "conflict_detected": False, "conflict_severity": None,
            "majority_position": None, "approvers_against_majority": [],
            "conflict_detail": None, "block_status": "OK",
        }

    approve_count = a1_result["approve_count"]
    reject_count = a1_result["reject_count"]
    pending_count = a1_result["pending_count"]
    approve_list = a1_result["approve_list"]
    reject_list = a1_result["reject_list"]

    # S1-S4: first match wins [SPEC]
    if reject_count >= approve_count and reject_count >= 2:
        severity = "HIGH"       # S1
    elif reject_count >= approve_count and reject_count == 1:
        severity = "MEDIUM"     # S2
    elif approve_count > reject_count and pending_count > 0:
        severity = "MEDIUM"     # S3
    else:  # S4: approve > reject and pending == 0
        severity = "LOW"        # S4

    severity = validate_enum_value(severity, "conflict_severity")

    # Majority position [SPEC]
    if approve_count > reject_count:
        majority = "APPROVE"
        against = list(reject_list)
    elif reject_count > approve_count:
        majority = "REJECT"
        against = list(approve_list)
    else:
        majority = "TIED"
        against = list(approve_list) + list(reject_list)  # [IMPLEMENTATION]

    detail = (
        f"Conflit {severity} : {reject_count} rejet(s) vs {approve_count} approbation(s). "
        f"Position majoritaire : {majority}. Dissidents : {', '.join(against) or 'aucun'}."
    )

    return {
        "conflict_detected": True,
        "conflict_severity": severity,
        "majority_position": majority,
        "approvers_against_majority": against,
        "conflict_detail": detail,
        "block_status": "OK",
    }


# --- A3: Missing Approver Analysis ---

def compute_missing_approvers(
    pending_set: List[str],
    date_diffusion: Optional[datetime.date],
    date_contractuelle_visa: Optional[datetime.date],
    reference_date: datetime.date,
) -> dict:
    """[SPEC] A3: Missing Approver Analysis. U1-U5 first-match-wins per approver."""
    if not pending_set:
        return {
            "missing_approvers": [], "total_missing": 0, "worst_urgency": None,
            "critical_missing": [], "missing_summary": "Aucun intervenant manquant.",
            "block_status": "OK",
        }

    # Compute shared time metrics once
    dsd = None
    if date_diffusion is not None:
        try:
            dsd = (reference_date - date_diffusion).days
        except (TypeError, AttributeError):
            dsd = None

    dpd = None
    if date_contractuelle_visa is not None:
        try:
            dpd = (reference_date - date_contractuelle_visa).days
        except (TypeError, AttributeError):
            dpd = None

    missing_list = []
    worst_ord = 0
    worst_label = None
    critical = []

    for key in pending_set:
        # U1-U5: first match wins per approver [SPEC]
        if dpd is not None and dpd > 14:
            urgency = "CRITICAL"     # U1
        elif dpd is not None and dpd > 0 and dpd <= 14:
            urgency = "HIGH"         # U2
        elif dpd is not None and dpd <= 0 and dpd > -3:
            urgency = "MEDIUM"       # U3
        elif dpd is None and dsd is not None and dsd > 21:
            urgency = "MEDIUM"       # U4
        else:
            urgency = "LOW"          # U5

        urgency = validate_enum_value(urgency, "urgency")
        urg_ord = URGENCY_ORDER.get(urgency, 0)
        if urg_ord > worst_ord:
            worst_ord = urg_ord
            worst_label = urgency
        if urgency == "CRITICAL":
            critical.append(key)

        missing_list.append({
            "approver_key": key,
            "days_since_diffusion": dsd,
            "days_past_deadline": dpd,
            "urgency": urgency,
        })

    critical_str = ", ".join(critical) if critical else "aucun"
    summary = (
        f"{len(pending_set)} intervenant(s) en attente. "
        f"Urgence maximale : {worst_label or 'N/A'}. "
        f"Critiques : {critical_str}."
    )

    return {
        "missing_approvers": missing_list,
        "total_missing": len(pending_set),
        "worst_urgency": worst_label,
        "critical_missing": critical,
        "missing_summary": summary,
        "block_status": "OK",
    }


# ============================================================================
# 8. SHARED CHAIN SCANNER [Fix #3 — single implementation for A4 and G2]
# ============================================================================

def scan_rejection_chain(
    start_row: dict,
    version_index: dict,
    approver_col_map: dict,
) -> Tuple[int, List[dict]]:
    """[SPEC — Fix #2, Fix #3] Walk backward from start_row via previous_version_key.

    Returns (consecutive_rejections, qualifying_revision_rows).

    Rejection qualification (per revision):
    - visa_global = REF, OR
    - visa_global is null AND reject_set non-empty AND approve_set empty
    - Both empty (zero opinionated) is NOT a rejection -> BK3 break

    Chain breaks BK1-BK4 (stop counting):
    - BK1: visa_global IN (VAO, VSO, SUS, DEF, FAV)
    - BK2: visa_global null AND at least one approver statut IN (VSO, VAO)
    - BK3: visa_global null AND all assigned approver statuts are null
    - BK4: previous_version_key is null (start of chain)

    Count includes the start_row if it qualifies.
    This function is used by BOTH A4 (compute_blocking) and G2 (compute_g2_loop_report).
    """
    qualifying = []
    current = start_row

    while current is not None:
        vg = _safe_get(current, "visa_global")
        assigned = current.get("assigned_approvers") or []
        if isinstance(assigned, str):
            try:
                import ast
                assigned = ast.literal_eval(assigned)
            except (ValueError, SyntaxError):
                assigned = []

        # Check chain break conditions first
        # BK1: non-rejection visa
        if vg is not None and vg in BK1_NON_REJECTION_VISAS:
            break

        # Need to evaluate approver statuts for BK2, BK3, and rejection qualification
        approve_any = False
        reject_any = False
        all_null = True

        for key in assigned:
            col_info = approver_col_map.get(key, {})
            statut_col = col_info.get("statut_col")
            if statut_col is None:
                continue
            statut = _safe_get(current, statut_col)
            if statut is not None:
                all_null = False
                if statut in APPROVE_STATUTS:
                    approve_any = True
                elif statut in REJECT_STATUTS:
                    reject_any = True

        # BK2: visa_global null AND at least one approver approved
        if vg is None and approve_any:
            break

        # BK3: visa_global null AND all assigned statuts are null (no responses)
        if vg is None and all_null and len(assigned) > 0:
            break

        # Rejection qualification
        is_rejection = False
        if vg == "REF":
            is_rejection = True
        elif vg is None and reject_any and not approve_any:
            is_rejection = True

        if is_rejection:
            qualifying.append(current)
        else:
            # Not a rejection and not a break -> still break the chain
            break

        # Walk backward
        pvk = _safe_get(current, "previous_version_key")
        if pvk is None:
            break  # BK4

        current = version_index.get(pvk)
        if current is None:
            break  # Cannot resolve previous — treat as BK4

    return len(qualifying), qualifying


# --- A4: Blocking Logic ---

def compute_blocking(
    a1_result: dict,
    item_row: dict,
    version_index: dict,
    approver_col_map: dict,
    blocker_index: dict,
) -> dict:
    """[SPEC] A4: Blocking Logic. B1-B5 first-match-wins + chain scan + systemic lookup."""
    reject_count = a1_result["reject_count"]
    approve_count = a1_result["approve_count"]
    pending_count = a1_result["pending_count"]
    reject_list = a1_result["reject_list"]
    revision_count = item_row.get("revision_count") or 1

    # B1-B5: first match wins [SPEC]
    if reject_count == 0:
        pattern = "NOT_BLOCKED"                  # B1
    elif reject_count > 0 and approve_count == 0 and pending_count == 0 and revision_count > 1:
        pattern = "CHRONIC_BLOCK"                # B2
    elif reject_count > 0 and approve_count == 0 and pending_count == 0 and revision_count == 1:
        pattern = "FIRST_REJECTION"              # B3
    elif reject_count > 0 and approve_count > 0:
        pattern = "PARTIAL_BLOCK"                # B4
    elif reject_count > 0 and pending_count > 0 and approve_count == 0:
        pattern = "BLOCK_WITH_PENDING"           # B5
    else:
        pattern = "NOT_BLOCKED"                  # Fallback

    pattern = validate_enum_value(pattern, "blocking_pattern")

    consecutive = None
    chain_rows = []
    if pattern == "CHRONIC_BLOCK":
        consecutive, chain_rows = scan_rejection_chain(
            item_row, version_index, approver_col_map,
        )
        # [SAFEGUARD] Validate >= 2 for CHRONIC_BLOCK
        if consecutive < 2:
            logger.warning(
                "CHRONIC_BLOCK with consecutive_rejections=%d < 2 for row %s",
                consecutive, item_row.get("row_id"),
            )

    # Systemic blocker lookup from G1 [SPEC]
    systemic_list = []
    for key in reject_list:
        bi = blocker_index.get(key, {})
        if bi.get("is_systemic_blocker", False):
            systemic_list.append(key)

    is_blocked = pattern != "NOT_BLOCKED"

    # Template
    if pattern == "CHRONIC_BLOCK":
        systemic_str = ", ".join(systemic_list) if systemic_list else "aucun"
        detail = (
            f"Blocage chronique : {len(reject_list)} approbateur(s) bloquant(s) "
            f"({', '.join(reject_list)}). "
            f"{consecutive} rejet(s) consécutif(s). Systémiques : {systemic_str}."
        )
    elif is_blocked:
        detail = (
            f"{pattern} : {len(reject_list)} approbateur(s) bloquant(s) "
            f"({', '.join(reject_list)})."
        )
    else:
        detail = "Aucun blocage détecté."

    return {
        "is_blocked": is_blocked,
        "blocking_pattern": pattern,
        "blocking_approvers": list(reject_list),
        "is_systemic_blocker": systemic_list,
        "consecutive_rejections": consecutive,
        "blocking_detail": detail,
        "block_status": "OK",
    }


# --- A5: Revision Delta ---

def compute_revision_delta(
    item_row: dict,
    version_index: dict,
    approver_col_map: dict,
) -> dict:
    """[SPEC] A5: Revision Delta. Compare current vs previous revision."""
    pvk = _safe_get(item_row, "previous_version_key")
    if pvk is None:
        return {
            "has_previous": False, "previous_ind": None, "visa_global_change": None,
            "approver_changes": [], "total_changed": 0, "new_responses": 0,
            "lost_responses": 0, "reversals": 0, "delta_summary": "Première révision.",
            "block_status": "OK",
        }

    prev_row = version_index.get(pvk)
    if prev_row is None:
        logger.warning("A5: previous_version_key %s not found in version_index for row %s",
                        pvk, item_row.get("row_id"))
        return {
            "has_previous": False, "previous_ind": None, "visa_global_change": None,
            "approver_changes": [], "total_changed": 0, "new_responses": 0,
            "lost_responses": 0, "reversals": 0,
            "delta_summary": "Révision précédente introuvable.",
            "block_status": "OK",
        }

    prev_ind = _safe_get(prev_row, "ind")

    # visa_global comparison
    curr_vg = _safe_get(item_row, "visa_global")
    prev_vg = _safe_get(prev_row, "visa_global")
    vg_change = None
    if curr_vg != prev_vg:
        vg_change = f"{prev_vg} → {curr_vg}"

    # Per-approver comparison
    assigned = item_row.get("assigned_approvers") or []
    if isinstance(assigned, str):
        try:
            import ast
            assigned = ast.literal_eval(assigned)
        except (ValueError, SyntaxError):
            assigned = []

    changes = []
    total_changed = 0
    new_responses = 0
    lost_responses = 0
    reversals = 0

    for key in assigned:
        col_info = approver_col_map.get(key, {})
        statut_col = col_info.get("statut_col")
        if statut_col is None:
            continue
        curr_s = _safe_get(item_row, statut_col)
        prev_s = _safe_get(prev_row, statut_col)
        changed = curr_s != prev_s

        if changed:
            total_changed += 1
            if prev_s is None and curr_s is not None:
                new_responses += 1
            elif prev_s is not None and curr_s is None:
                lost_responses += 1
            # Reversals: approve <-> reject flip
            if prev_s in APPROVE_STATUTS and curr_s in REJECT_STATUTS:
                reversals += 1
            elif prev_s in REJECT_STATUTS and curr_s in APPROVE_STATUTS:
                reversals += 1

        changes.append({
            "approver_key": key,
            "previous_statut": prev_s,
            "current_statut": curr_s,
            "changed": changed,
        })

    summary = (
        f"Changements vs IND {prev_ind or '?'} : {total_changed} avis modifié(s), "
        f"{new_responses} nouvelle(s) réponse(s), {reversals} retournement(s)."
    )

    return {
        "has_previous": True,
        "previous_ind": prev_ind,
        "visa_global_change": vg_change,
        "approver_changes": changes,
        "total_changed": total_changed,
        "new_responses": new_responses,
        "lost_responses": lost_responses,
        "reversals": reversals,
        "delta_summary": summary,
        "block_status": "OK",
    }


# --- A6: Time Analysis ---

def compute_time_analysis(item_row: dict, reference_date: datetime.date) -> dict:
    """[SPEC] A6: Time Analysis. D1-D8 deadline_status + age bracket rules."""
    dsd = _safe_get(item_row, "days_since_diffusion")
    dud = _safe_get(item_row, "days_until_deadline")
    is_overdue = bool(_safe_get(item_row, "is_overdue"))
    days_overdue = _safe_get(item_row, "days_overdue") or 0
    has_deadline = bool(_safe_get(item_row, "has_deadline"))

    # Convert to int where needed
    if isinstance(dsd, float):
        dsd = int(dsd) if not pd.isna(dsd) else None
    if isinstance(dud, float):
        dud = int(dud) if not pd.isna(dud) else None
    if isinstance(days_overdue, float):
        days_overdue = int(days_overdue) if not pd.isna(days_overdue) else 0

    # D1-D8: deadline_status, first match wins [SPEC]
    if not has_deadline:
        deadline_status = "NO_DEADLINE"            # D1
    elif dud is not None and dud > 14:
        deadline_status = "COMFORTABLE"            # D2
    elif dud is not None and dud > 7:
        deadline_status = "APPROACHING"            # D3
    elif dud is not None and dud > 0:
        deadline_status = "URGENT"                 # D4
    elif dud is not None and dud == 0:
        deadline_status = "DUE_TODAY"              # D5
    elif days_overdue > 0 and days_overdue <= 14:
        deadline_status = "OVERDUE"                # D6
    elif days_overdue > 14 and days_overdue <= 30:
        deadline_status = "SEVERELY_OVERDUE"       # D7
    elif days_overdue > 30:
        deadline_status = "CRITICALLY_OVERDUE"     # D8
    else:
        deadline_status = "NO_DEADLINE"            # Fallback

    deadline_status = validate_enum_value(deadline_status, "deadline_status")

    # Age bracket: first match wins [SPEC]
    if dsd is None:
        age_bracket = "UNKNOWN_AGE"
    elif dsd <= 7:
        age_bracket = "FRESH"
    elif dsd <= 21:
        age_bracket = "NORMAL"
    elif dsd <= 60:
        age_bracket = "AGING"
    else:
        age_bracket = "STALE"

    age_bracket = validate_enum_value(age_bracket, "age_bracket")

    # Template
    if is_overdue:
        deadline_part = f"{days_overdue} jour(s) en retard ({deadline_status})."
    elif has_deadline and dud is not None:
        deadline_part = f"{dud} jour(s) restant(s) ({deadline_status})."
    else:
        deadline_part = f"Échéance : {deadline_status}."

    summary = f"{age_bracket}. {deadline_part}"

    return {
        "days_since_diffusion": dsd,
        "days_until_deadline": dud,
        "is_overdue": is_overdue,
        "days_overdue": days_overdue,
        "has_deadline": has_deadline,
        "deadline_status": deadline_status,
        "age_bracket": age_bracket,
        "time_summary": summary,
        "block_status": "OK",
    }


# ============================================================================
# 9. LIFECYCLE_STATE DERIVATION
# ============================================================================

def derive_lifecycle_state(
    consensus_type: Optional[str],
    revision_count: int,
    analysis_degraded: bool,
    m3_category: Optional[str],
) -> str:
    """[SPEC — V2.2.2 E2, Phase 2 spec §3.2-3.3, Patch V2/V3]
    Derive lifecycle_state per item. Override on contradiction with M3 category.
    """
    # Step 1: Check analysis_degraded first [IMPLEMENTATION]
    if analysis_degraded:
        computed = "ON_HOLD"
    # Step 2: ALL_HM guard [SAFEGUARD — Patch V2]
    elif consensus_type == "ALL_HM":
        logger.error(
            "ALL_HM item found in M3 queue — should have been excluded by M3 filtering. "
            "Defaulting lifecycle_state to ON_HOLD."
        )
        computed = "ON_HOLD"
    # Step 3: Consensus type mapping [SPEC]
    elif consensus_type == "NOT_STARTED":
        computed = "NOT_STARTED"
    elif consensus_type == "INCOMPLETE":
        computed = "WAITING_RESPONSES"
    elif consensus_type == "ALL_APPROVE":
        computed = "READY_TO_ISSUE"
    elif consensus_type == "ALL_REJECT" and revision_count == 1:
        computed = "READY_TO_REJECT"
    elif consensus_type == "MIXED":
        computed = "NEEDS_ARBITRATION"
    elif consensus_type == "ALL_REJECT" and revision_count > 1:
        computed = "CHRONIC_BLOCKED"
    else:
        # Step 4: No rule matched [SAFEGUARD]
        logger.error(
            "lifecycle_state: no rule matched for consensus_type=%s revision_count=%s. "
            "Defaulting to ON_HOLD.",
            consensus_type, revision_count,
        )
        computed = "ON_HOLD"

    computed = validate_enum_value(computed, "lifecycle_state")

    # Step 5: Contradiction validation [SPEC — Phase 2 spec §3.2-3.3, Patch V2/V3]
    if computed != "ON_HOLD" and m3_category is not None:
        expected_cat = LIFECYCLE_CATEGORY_COMPAT.get(computed)
        if expected_cat is not None and expected_cat != m3_category:
            # Contradiction: override from M3 category
            implied = CATEGORY_TO_LIFECYCLE.get(m3_category)
            if implied is not None:
                logger.error(
                    "lifecycle_state contradiction: computed=%s (expects category=%s) "
                    "but M3 category=%s. Overriding to %s (M3 authoritative).",
                    computed, expected_cat, m3_category, implied,
                )
                computed = implied
            else:
                logger.error(
                    "lifecycle_state contradiction: computed=%s but M3 category=%s "
                    "has no reverse mapping. Keeping computed value.",
                    computed, m3_category,
                )

    return computed


# ============================================================================
# 10. PER-ITEM ASSEMBLY
# ============================================================================

def assemble_analysis_result(
    row_id: str,
    a1: dict, a2: dict, a3: dict, a4: dict, a5: dict, a6: dict,
    lifecycle_state: str,
    analysis_degraded: bool,
    failed_blocks: List[str],
) -> dict:
    """[SPEC] Assemble composite analysis_result dict for one queue item."""
    return {
        "row_id": row_id,
        "agreement": a1,
        "conflict": a2,
        "missing": a3,
        "blocking": a4,
        "delta": a5,
        "time": a6,
        "lifecycle_state": lifecycle_state,
        "analysis_degraded": analysis_degraded,
        "failed_blocks": failed_blocks,
    }


# ============================================================================
# 11. GLOBAL REPORT FUNCTIONS
# ============================================================================

def compute_g1_blocker_report(
    latest_index: dict,
    approver_col_map: dict,
    reference_date: datetime.date,
) -> Tuple[pd.DataFrame, dict]:
    """[SPEC] G1: Systemic Blocker Report. 14 rows, one per canonical approver.
    Counting grain: (doc_family_key, source_sheet) [Patch V3 — resolved decision].
    Returns (g1_dataframe, blocker_index).
    """
    rows = []
    blocker_idx = {}

    for approver_key in CANONICAL_APPROVERS:
        col_info = approver_col_map.get(approver_key, {})
        statut_col = col_info.get("statut_col")
        date_col = col_info.get("date_col")

        total_assigned = 0
        total_responded = 0
        total_blocking = 0
        blocked_fams = []
        response_days_list = []

        for (fk, ss), row_d in latest_index.items():
            assigned_list = row_d.get("assigned_approvers") or []
            if isinstance(assigned_list, str):
                try:
                    import ast
                    assigned_list = ast.literal_eval(assigned_list)
                except (ValueError, SyntaxError):
                    assigned_list = []

            if approver_key not in assigned_list:
                continue

            total_assigned += 1

            if statut_col is None:
                continue

            statut = _safe_get(row_d, statut_col)
            if statut is not None:
                total_responded += 1

                # avg_response_days computation
                if date_col is not None:
                    a_date = _safe_get(row_d, date_col)
                    d_diff = _safe_get(row_d, "date_diffusion")
                    if a_date is not None and d_diff is not None:
                        try:
                            delta = (a_date - d_diff).days
                            response_days_list.append(delta)
                        except (TypeError, AttributeError):
                            pass

            if statut == "REF":
                total_blocking += 1
                blocked_fams.append((fk, ss))

        blocking_rate = total_blocking / total_assigned if total_assigned > 0 else 0.0
        avg_resp = None
        if response_days_list:
            avg_resp = sum(response_days_list) / len(response_days_list)

        is_systemic = total_blocking >= SYSTEMIC_BLOCKER_THRESHOLD

        if blocking_rate > 0.5:
            severity = "HIGH"
        elif blocking_rate > 0.25:
            severity = "MEDIUM"
        else:
            severity = "LOW"

        display_name = APPROVER_DISPLAY_NAMES.get(approver_key)
        if display_name is None:
            logger.error("Unknown approver key in G1: %s. Using raw key as display_name.", approver_key)
            display_name = approver_key

        row_data = {
            "approver_key": approver_key,
            "display_name": display_name,
            "total_latest_assigned": total_assigned,
            "total_responded": total_responded,
            "total_blocking": total_blocking,
            "blocking_rate": round(blocking_rate, 4),
            "avg_response_days": round(avg_resp, 2) if avg_resp is not None else None,
            "is_systemic_blocker": is_systemic,
            "blocked_families": blocked_fams,
            "severity": severity,
        }
        rows.append(row_data)

        blocker_idx[approver_key] = {
            "is_systemic_blocker": is_systemic,
            "total_blocking": total_blocking,
            "blocked_families": blocked_fams,
            "severity": severity,
        }

    g1_df = pd.DataFrame(rows)
    g1_df = enforce_column_order(g1_df, "G1")
    return g1_df, blocker_idx


def compute_g2_loop_report(
    latest_index: dict,
    version_index: dict,
    approver_col_map: dict,
) -> Tuple[pd.DataFrame, dict]:
    """[SPEC — Fix #3] G2: Loop Detection Report. Reuses scan_rejection_chain.
    One row per (doc_family_key, source_sheet) with is_latest=true.
    Returns (g2_dataframe, loop_index).
    """
    rows = []
    loop_idx = {}

    for (fk, ss), latest_row in latest_index.items():
        consecutive, chain_rows = scan_rejection_chain(
            latest_row, version_index, approver_col_map,
        )
        is_looping = consecutive >= 2
        loop_length = consecutive if is_looping else 0

        loop_start_ind = None
        loop_end_ind = None
        persistent_blockers = []

        if is_looping and chain_rows:
            loop_end_ind = _safe_get(chain_rows[0], "ind")   # latest = first in chain_rows
            loop_start_ind = _safe_get(chain_rows[-1], "ind")  # earliest = last

            # persistent_blockers: REF in ALL loop revisions [IMPLEMENTATION]
            blocker_sets = []
            for rev in chain_rows:
                assigned = rev.get("assigned_approvers") or []
                if isinstance(assigned, str):
                    try:
                        import ast
                        assigned = ast.literal_eval(assigned)
                    except (ValueError, SyntaxError):
                        assigned = []
                rev_blockers = set()
                for key in assigned:
                    ci = approver_col_map.get(key, {})
                    sc = ci.get("statut_col")
                    if sc and _safe_get(rev, sc) == "REF":
                        rev_blockers.add(key)
                blocker_sets.append(rev_blockers)

            if blocker_sets:
                persistent_blockers = sorted(set.intersection(*blocker_sets))

        row_data = {
            "doc_family_key": fk,
            "source_sheet": ss,
            "document": _safe_get(latest_row, "document"),
            "titre": _safe_get(latest_row, "titre"),
            "is_looping": is_looping,
            "loop_length": loop_length,
            "loop_start_ind": loop_start_ind,
            "loop_end_ind": loop_end_ind,
            "persistent_blockers": persistent_blockers,
            "latest_visa_global": _safe_get(latest_row, "visa_global"),
        }
        rows.append(row_data)
        loop_idx[(fk, ss)] = row_data

    g2_df = pd.DataFrame(rows) if rows else empty_dataframe("G2")
    g2_df = enforce_column_order(g2_df, "G2")
    return g2_df, loop_idx


def compute_g3_risk_scores(
    m3_queue: pd.DataFrame,
    result_index: dict,
    blocker_index: dict,
    loop_index: dict,
) -> pd.DataFrame:
    """[SPEC — Fix #13] G3: Risk Score Per Item. Scoped to M3 queue items only.
    6 risk factors with defined weights.
    """
    rows = []
    for _, qrow in m3_queue.iterrows():
        rd = _row_to_dict(qrow)
        row_id = rd.get("row_id")
        fk = rd.get("doc_family_key")
        ss = rd.get("source_sheet")
        ar = result_index.get(row_id, {})

        # Evaluate 6 factors
        factors = []
        score = 0

        # F1: days_overdue > 14 (weight 3)
        do = _safe_get(rd, "days_overdue") or 0
        f1_met = do > 14
        factors.append({"factor_id": "F1", "label": "Retard > 14 jours", "weight": RISK_WEIGHTS["F1"], "condition_met": f1_met})
        if f1_met:
            score += RISK_WEIGHTS["F1"]

        # F2: is_looping (weight 3)
        li = loop_index.get((fk, ss), {})
        f2_met = li.get("is_looping", False)
        factors.append({"factor_id": "F2", "label": "Boucle de rejets", "weight": RISK_WEIGHTS["F2"], "condition_met": f2_met})
        if f2_met:
            score += RISK_WEIGHTS["F2"]

        # F3: any blocking_approver is systemic (weight 2)
        blocking_data = ar.get("blocking", {})
        f3_met = len(blocking_data.get("is_systemic_blocker", [])) > 0
        factors.append({"factor_id": "F3", "label": "Bloqueur systémique", "weight": RISK_WEIGHTS["F3"], "condition_met": f3_met})
        if f3_met:
            score += RISK_WEIGHTS["F3"]

        # F4: revision_count > 3 (weight 2)
        rc = _safe_get(rd, "revision_count") or 1
        f4_met = rc > 3
        factors.append({"factor_id": "F4", "label": "Révisions > 3", "weight": RISK_WEIGHTS["F4"], "condition_met": f4_met})
        if f4_met:
            score += RISK_WEIGHTS["F4"]

        # F5: agreement_type = CONFLICT (weight 1)
        agreement_data = ar.get("agreement", {})
        f5_met = agreement_data.get("agreement_type") == "CONFLICT"
        factors.append({"factor_id": "F5", "label": "Conflit", "weight": RISK_WEIGHTS["F5"], "condition_met": f5_met})
        if f5_met:
            score += RISK_WEIGHTS["F5"]

        # F6: has_deadline=false AND days_since_diffusion > 30 (weight 1)
        hd = bool(_safe_get(rd, "has_deadline"))
        dsd = _safe_get(rd, "days_since_diffusion")
        f6_met = (not hd) and (dsd is not None and dsd > 30)
        factors.append({"factor_id": "F6", "label": "Sans échéance + ancien", "weight": RISK_WEIGHTS["F6"], "condition_met": f6_met})
        if f6_met:
            score += RISK_WEIGHTS["F6"]

        contributing = [f["factor_id"] for f in factors if f["condition_met"]]

        rows.append({
            "row_id": row_id,
            "risk_score": score,
            "is_high_risk": score >= HIGH_RISK_THRESHOLD,
            "contributing_factors": contributing,
            "factor_details": factors,
        })

    g3_df = pd.DataFrame(rows) if rows else empty_dataframe("G3")
    g3_df = enforce_column_order(g3_df, "G3")
    return g3_df


def compute_g4_lot_health(
    m2_enriched: pd.DataFrame,
    m3_queue: pd.DataFrame,
    g3_report: pd.DataFrame,
    sheet_index: dict,
    latest_index: dict,
) -> pd.DataFrame:
    """[SPEC] G4: Lot Health Report. One row per source_sheet."""
    # Precompute G3 high-risk by sheet
    g3_by_sheet = {}
    if not g3_report.empty and "row_id" in g3_report.columns:
        # Build row_id -> source_sheet from queue
        rid_sheet = {}
        for _, qr in m3_queue.iterrows():
            rid_sheet[qr.get("row_id")] = qr.get("source_sheet")
        for _, g3r in g3_report.iterrows():
            rid = g3r.get("row_id")
            ss = rid_sheet.get(rid)
            if ss is not None:
                if ss not in g3_by_sheet:
                    g3_by_sheet[ss] = 0
                if g3r.get("is_high_risk", False):
                    g3_by_sheet[ss] += 1

    # Get all source_sheets from M2
    all_sheets = set()
    for _, r in m2_enriched.iterrows():
        s = r.get("source_sheet")
        if s is not None and not (isinstance(s, float) and pd.isna(s)):
            all_sheets.add(s)

    # Precompute is_latest counts per sheet and approval counts
    sheet_total_docs = {}
    sheet_approved = {}
    sheet_visa_filled = {}
    for (fk, ss), row_d in latest_index.items():
        sheet_total_docs[ss] = sheet_total_docs.get(ss, 0) + 1
        vg = _safe_get(row_d, "visa_global")
        if vg is not None:
            sheet_visa_filled[ss] = sheet_visa_filled.get(ss, 0) + 1
            if vg in APPROVE_STATUTS:
                sheet_approved[ss] = sheet_approved.get(ss, 0) + 1

    rows = []
    for ss in sorted(all_sheets):
        queue_rids = sheet_index.get(ss, [])
        total_pending = len(queue_rids)

        # Queue items for this sheet
        total_overdue = 0
        cat_dist = {
            "EASY_WIN_APPROVE": 0, "BLOCKED": 0, "FAST_REJECT": 0,
            "CONFLICT": 0, "WAITING": 0, "NOT_STARTED": 0,
        }
        pscores = []
        days_pending_list = []

        for _, qr in m3_queue.iterrows():
            qrd = _row_to_dict(qr)
            if qrd.get("source_sheet") != ss:
                continue
            if _safe_get(qrd, "is_overdue"):
                total_overdue += 1
            cat = _safe_get(qrd, "category")
            if cat in cat_dist:
                cat_dist[cat] += 1
            ps = _safe_get(qrd, "priority_score")
            if ps is not None:
                pscores.append(ps)
            dsd = _safe_get(qrd, "days_since_diffusion")
            if dsd is not None:
                days_pending_list.append(dsd)

        total_docs = sheet_total_docs.get(ss, 0)
        total_hr = g3_by_sheet.get(ss, 0)

        approved_c = sheet_approved.get(ss, 0)
        filled_c = sheet_visa_filled.get(ss, 0)
        approval_rate = approved_c / filled_c if filled_c > 0 else 0.0

        avg_ps = sum(pscores) / len(pscores) if pscores else None
        avg_dp = sum(days_pending_list) / len(days_pending_list) if days_pending_list else None

        is_hrc = total_hr >= HIGH_RISK_CLUSTER_THRESHOLD

        # Health score formula [SPEC]
        denom_pending = max(total_pending, 1)
        overdue_pct = total_overdue / denom_pending
        hr_pct = total_hr / denom_pending
        health = 100 - (overdue_pct * HEALTH_OVERDUE_WEIGHT + hr_pct * HEALTH_RISK_WEIGHT + (1 - approval_rate) * HEALTH_APPROVAL_WEIGHT)
        health = max(0.0, min(100.0, health))

        rows.append({
            "source_sheet": ss,
            "total_documents": total_docs,
            "total_pending": total_pending,
            "total_overdue": total_overdue,
            "total_high_risk": total_hr,
            "category_distribution": cat_dist,
            "approval_rate": round(approval_rate, 4),
            "avg_priority_score": round(avg_ps, 2) if avg_ps is not None else None,
            "avg_days_pending": round(avg_dp, 2) if avg_dp is not None else None,
            "is_high_risk_cluster": is_hrc,
            "health_score": round(health, 2),
        })

    g4_df = pd.DataFrame(rows) if rows else empty_dataframe("G4")
    g4_df = enforce_column_order(g4_df, "G4")
    return g4_df


# ============================================================================
# 12. SCHEMA VALIDATION
# ============================================================================

def validate_analysis_result_schema(result: dict) -> List[str]:
    """[GP9] Verify a single analysis_result dict. Returns list of validation errors."""
    errors = []
    required_top = ["row_id", "agreement", "conflict", "missing", "blocking",
                     "delta", "time", "lifecycle_state", "analysis_degraded", "failed_blocks"]
    for k in required_top:
        if k not in result:
            errors.append(f"Missing top-level key: {k}")

    # Check analysis_degraded is bool
    ad = result.get("analysis_degraded")
    if ad is not None and not isinstance(ad, bool):
        errors.append(f"analysis_degraded is not bool: {type(ad).__name__}")

    # Check failed_blocks is a list
    fb = result.get("failed_blocks")
    if fb is not None and not isinstance(fb, list):
        errors.append(f"failed_blocks is not a list: {type(fb).__name__}")

    # Check all 6 block sub-dicts are present and are dicts
    for block_name in ["agreement", "conflict", "missing", "blocking", "delta", "time"]:
        block = result.get(block_name)
        if block is None:
            errors.append(f"Block '{block_name}' is missing")
        elif not isinstance(block, dict):
            errors.append(f"Block '{block_name}' is not a dict: {type(block).__name__}")

    # Check every block has block_status ∈ {"OK", "FAILED"}
    for block_name in ["agreement", "conflict", "missing", "blocking", "delta", "time"]:
        block = result.get(block_name)
        if isinstance(block, dict):
            bs = block.get("block_status")
            if bs is None:
                errors.append(f"Block '{block_name}' missing block_status")
            elif bs not in ("OK", "FAILED"):
                errors.append(f"Block '{block_name}' invalid block_status: {bs!r}")

    # Validate lifecycle_state enum
    ls = result.get("lifecycle_state")
    if ls and ls not in VALID_ENUMS.get("lifecycle_state", set()):
        errors.append(f"Invalid lifecycle_state: {ls}")

    # Validate agreement_type
    ag = result.get("agreement", {})
    at = ag.get("agreement_type")
    if at and at not in VALID_ENUMS.get("agreement_type", set()):
        errors.append(f"Invalid agreement_type: {at}")

    # Validate blocking_pattern
    bl = result.get("blocking", {})
    bp = bl.get("blocking_pattern")
    if bp and bp not in VALID_ENUMS.get("blocking_pattern", set()):
        errors.append(f"Invalid blocking_pattern: {bp}")

    # Validate deadline_status
    tm = result.get("time", {})
    ds = tm.get("deadline_status")
    if ds and ds not in VALID_ENUMS.get("deadline_status", set()):
        errors.append(f"Invalid deadline_status: {ds}")

    ab = tm.get("age_bracket")
    if ab and ab not in VALID_ENUMS.get("age_bracket", set()):
        errors.append(f"Invalid age_bracket: {ab}")

    return errors


def validate_global_dataframe_schema(df: pd.DataFrame, report_name: str, schema: dict) -> List[str]:
    """[GP9] Verify a G1/G2/G3/G4 DataFrame. Returns list of errors.
    Schema: {col_name: {dtype: str, nullable: bool}}
    Applies Phase 5 fallback rules (7 rules, Patch V3).
    """
    errors = []
    existing_cols = set(df.columns)

    for col_name, col_spec in schema.items():
        nullable = col_spec.get("nullable", True)
        dtype = col_spec.get("dtype", "any")
        col_type = col_spec.get("col_type", "string")  # string, numeric, bool, list, enum, identifier

        if col_name not in existing_cols:
            # Apply fallback rules
            if nullable:
                # Rule 1: nullable -> fill with None
                df[col_name] = None
            elif col_type == "numeric":
                # Rule 2: non-nullable numeric
                default_val = 0.0 if dtype == "float" else 0
                df[col_name] = default_val
            elif col_type == "bool":
                # Rule 3: non-nullable boolean
                df[col_name] = False
            elif col_type == "list":
                # Rule 4: non-nullable list
                df[col_name] = [[] for _ in range(len(df))]
            elif col_type == "enum":
                # Rule 5: non-nullable enum
                safe_val = ENUM_SAFE_DEFAULTS.get(col_name, ENUM_SAFE_DEFAULTS.get(dtype))
                if safe_val is not None:
                    df[col_name] = safe_val
                else:
                    errors.append(f"{report_name}: Missing enum column {col_name} with no safe default")
            elif col_type == "identifier":
                # Rule 6: required identifier — fatal
                errors.append(
                    f"{report_name}: Missing required identifier column {col_name}. "
                    "Cannot synthesize safe default."
                )
            else:
                errors.append(f"{report_name}: Missing column {col_name} with unknown type {col_type}")
        else:
            # Rule 7: Check existing enum columns for invalid values
            if col_type == "enum" and col_name in existing_cols:
                allowed = VALID_ENUMS.get(col_name) or VALID_ENUMS.get(dtype)
                if allowed:
                    for i, val in enumerate(df[col_name]):
                        if val is not None and val not in allowed:
                            safe = ENUM_SAFE_DEFAULTS.get(col_name, ENUM_SAFE_DEFAULTS.get(dtype))
                            logger.error(
                                "%s: Invalid enum value in column %s row %d: %r -> %r",
                                report_name, col_name, i, val, safe,
                            )
                            df.at[i, col_name] = safe

            # Check non-nullable violations
            if not nullable and col_name in existing_cols:
                null_count = df[col_name].isna().sum() if hasattr(df[col_name], "isna") else 0
                if null_count > 0:
                    errors.append(f"{report_name}: Column {col_name} has {null_count} null values (non-nullable)")

    return errors


# G1–G4 schema definitions — imported from central schema module [V3.2 contract lock]
G1_SCHEMA = SCHEMA_G1
G2_SCHEMA = SCHEMA_G2
G3_SCHEMA = SCHEMA_G3
G4_SCHEMA = SCHEMA_G4


# ============================================================================
# 13. SUMMARY LOGGING
# ============================================================================

def log_m4_summary(
    per_item_results: List[dict],
    g1_report: pd.DataFrame,
    g2_report: pd.DataFrame,
    g3_report: pd.DataFrame,
    g4_report: pd.DataFrame,
    phase_timings: dict,
) -> None:
    """[IMPLEMENTATION] Log M4 summary statistics."""
    total = len(per_item_results)
    degraded = sum(1 for r in per_item_results if r.get("analysis_degraded"))
    block_failures = {"A1": 0, "A2": 0, "A3": 0, "A4": 0, "A5": 0, "A6": 0}
    consensus_mismatches = 0

    for r in per_item_results:
        for fb in r.get("failed_blocks", []):
            block_failures[fb] = block_failures.get(fb, 0) + 1
        if not r.get("agreement", {}).get("consensus_match", True):
            consensus_mismatches += 1

    systemic_count = 0
    if not g1_report.empty and "is_systemic_blocker" in g1_report.columns:
        systemic_count = g1_report["is_systemic_blocker"].sum()

    loops_count = 0
    if not g2_report.empty and "is_looping" in g2_report.columns:
        loops_count = g2_report["is_looping"].sum()

    high_risk_count = 0
    max_risk = 0
    if not g3_report.empty and "is_high_risk" in g3_report.columns:
        high_risk_count = g3_report["is_high_risk"].sum()
        max_risk = g3_report["risk_score"].max() if "risk_score" in g3_report.columns else 0

    cluster_count = 0
    if not g4_report.empty and "is_high_risk_cluster" in g4_report.columns:
        cluster_count = g4_report["is_high_risk_cluster"].sum()

    total_time = sum(phase_timings.values())

    logger.info(
        "M4 SUMMARY: items=%d degraded=%d | block_failures=%s | "
        "consensus_mismatches=%d | systemic_blockers=%d loops=%d "
        "high_risk=%d (max_score=%d) clusters=%d | "
        "timing: total=%.3fs phases=%s",
        total, degraded, block_failures, consensus_mismatches,
        systemic_count, loops_count, high_risk_count, max_risk, cluster_count,
        total_time, {k: f"{v:.3f}s" for k, v in phase_timings.items()},
    )


# ============================================================================
# 14. ENTRY POINT
# ============================================================================

def _empty_g1() -> pd.DataFrame:
    return empty_dataframe("G1")

def _empty_g2() -> pd.DataFrame:
    return empty_dataframe("G2")

def _empty_g3() -> pd.DataFrame:
    return empty_dataframe("G3")

def _empty_g4() -> pd.DataFrame:
    return empty_dataframe("G4")


def run_module4(
    m1_master: pd.DataFrame,
    m2_enriched: pd.DataFrame,
    m3_queue: pd.DataFrame,
    reference_date: datetime.date,
) -> Tuple[List[Dict], pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Module 4: Analysis Engine entry point.

    [SPEC — V2.2.2 E1-E3]
    Execution order: Phase 1 (indexes) -> Phase 2 (G1) -> Phase 3 (per-item A1-A6)
    -> Phase 4 (G2, G3, G4) -> Phase 5 (validation + logging).

    Returns: (per_item_results, g1_report, g2_report, g3_report, g4_report)
    """
    phase_timings = {}

    # ── Phase 1: Input Validation, Indexing, Approver Discovery ──
    t0 = time.monotonic()
    logger.info("M4 Phase 1: Input validation and index construction")

    validate_m4_inputs(m1_master, m2_enriched, m3_queue, reference_date)

    if m3_queue.empty:
        logger.warning("M4: m3_queue is empty. Returning empty outputs.")
        return [], _empty_g1(), _empty_g2(), _empty_g3(), _empty_g4()

    approver_col_map = discover_approver_columns(m2_enriched)
    version_index = build_version_index(m2_enriched)
    chain_index = build_chain_index(m2_enriched)
    queue_index = build_queue_index(m3_queue)
    sheet_index = build_sheet_index(m3_queue)
    latest_index = build_latest_index(m2_enriched)

    # Phase 1 index integrity checks [SAFEGUARD]
    assert len(queue_index) == len(m3_queue), (
        f"queue_index size mismatch: {len(queue_index)} vs {len(m3_queue)} queue rows"
    )
    for key, chain in chain_index.items():
        sort_orders = [r.get("ind_sort_order", 0) for r in chain]
        assert sort_orders == sorted(sort_orders), (
            f"chain_index not sorted for {key}: {sort_orders}"
        )

    phase_timings["phase1"] = time.monotonic() - t0
    logger.info("M4 Phase 1 complete: %d version entries, %d chains, %d queue items, %d latest entries",
                len(version_index), len(chain_index), len(queue_index), len(latest_index))

    # ── Phase 2: G1 Systemic Blocker Report ──
    t0 = time.monotonic()
    logger.info("M4 Phase 2: G1 systemic blocker computation")

    try:
        g1_report, blocker_index = compute_g1_blocker_report(
            latest_index, approver_col_map, reference_date,
        )
    except Exception:
        logger.exception("M4 Phase 2: G1 computation failed. Using empty report.")
        g1_report = _empty_g1()
        blocker_index = {}

    phase_timings["phase2"] = time.monotonic() - t0
    logger.info("M4 Phase 2 complete: G1 has %d rows", len(g1_report))

    # ── Phase 3: Per-Item Analysis (A1-A6 + lifecycle_state) ──
    t0 = time.monotonic()
    logger.info("M4 Phase 3: Per-item analysis for %d queue items", len(m3_queue))

    per_item_results = []
    result_index = {}

    for _, q_row in m3_queue.iterrows():
        item = _row_to_dict(q_row)
        row_id = item.get("row_id", "unknown")

        try:
            # --- A1: Agreement Detection ---
            try:
                approver_sets = partition_approver_sets(item, approver_col_map)
                a1 = compute_agreement(item, approver_sets, item.get("consensus_type"))
            except Exception:
                logger.exception("A1 failed for row %s", row_id)
                a1 = apply_block_safe_defaults("A1")
                approver_sets = ApproverSets()  # empty sets for downstream

            # --- A2: Conflict Detection ---
            try:
                a2 = compute_conflict(a1)
            except Exception:
                logger.exception("A2 failed for row %s", row_id)
                a2 = apply_block_safe_defaults("A2")

            # --- A3: Missing Approver Analysis ---
            try:
                a3 = compute_missing_approvers(
                    approver_sets.pending_set,
                    _safe_get(item, "date_diffusion"),
                    _safe_get(item, "date_contractuelle_visa"),
                    reference_date,
                )
            except Exception:
                logger.exception("A3 failed for row %s", row_id)
                a3 = apply_block_safe_defaults("A3")

            # --- A4: Blocking Logic ---
            try:
                a4 = compute_blocking(
                    a1, item, version_index, approver_col_map, blocker_index,
                )
            except Exception:
                logger.exception("A4 failed for row %s", row_id)
                a4 = apply_block_safe_defaults("A4")

            # --- A5: Revision Delta ---
            try:
                a5 = compute_revision_delta(item, version_index, approver_col_map)
            except Exception:
                logger.exception("A5 failed for row %s", row_id)
                a5 = apply_block_safe_defaults("A5")

            # --- A6: Time Analysis ---
            try:
                a6 = compute_time_analysis(item, reference_date)
            except Exception:
                logger.exception("A6 failed for row %s", row_id)
                a6 = apply_block_safe_defaults("A6")

            # --- Step 3.7b: analysis_degraded + failed_blocks [Patch V2] ---
            blocks = {"A1": a1, "A2": a2, "A3": a3, "A4": a4, "A5": a5, "A6": a6}
            failed_blocks = [bid for bid, bdata in blocks.items() if bdata.get("block_status") == "FAILED"]
            analysis_degraded = len(failed_blocks) > 0

            # --- Step 3.8: lifecycle_state ---
            revision_count = item.get("revision_count") or 1
            lifecycle = derive_lifecycle_state(
                item.get("consensus_type"),
                revision_count,
                analysis_degraded,
                item.get("category"),
            )

            # --- Step 3.9: Assembly ---
            result = assemble_analysis_result(
                row_id, a1, a2, a3, a4, a5, a6,
                lifecycle, analysis_degraded, failed_blocks,
            )

        except Exception:
            # Full item-level failure [Fix #21]
            logger.exception("M4 per-item computation failed entirely for row %s", row_id)
            a1 = apply_block_safe_defaults("A1")
            a2 = apply_block_safe_defaults("A2")
            a3 = apply_block_safe_defaults("A3")
            a4 = apply_block_safe_defaults("A4")
            a5 = apply_block_safe_defaults("A5")
            a6 = apply_block_safe_defaults("A6")
            result = assemble_analysis_result(
                row_id, a1, a2, a3, a4, a5, a6,
                "ON_HOLD", True, ["A1", "A2", "A3", "A4", "A5", "A6"],
            )

        per_item_results.append(result)
        result_index[row_id] = result

    phase_timings["phase3"] = time.monotonic() - t0
    logger.info("M4 Phase 3 complete: %d items processed", len(per_item_results))

    # ── Phase 4: Global Post-Item Analyses (G2, G3, G4) ──
    t0 = time.monotonic()
    logger.info("M4 Phase 4: Global analyses G2, G3, G4")

    # G2: Loop Detection [Fix #3 — uses same scan_rejection_chain]
    try:
        g2_report, loop_index = compute_g2_loop_report(
            latest_index, version_index, approver_col_map,
        )
    except Exception:
        logger.exception("M4 Phase 4: G2 computation failed. Using empty report.")
        g2_report = _empty_g2()
        loop_index = {}

    # G3: Risk Scores (depends on G1, G2, per-item results)
    try:
        g3_report = compute_g3_risk_scores(
            m3_queue, result_index, blocker_index, loop_index,
        )
    except Exception:
        logger.exception("M4 Phase 4: G3 computation failed. Using empty report.")
        g3_report = _empty_g3()

    # G4: Lot Health (depends on G3)
    try:
        g4_report = compute_g4_lot_health(
            m2_enriched, m3_queue, g3_report, sheet_index, latest_index,
        )
    except Exception:
        logger.exception("M4 Phase 4: G4 computation failed. Using empty report.")
        g4_report = _empty_g4()

    phase_timings["phase4"] = time.monotonic() - t0
    logger.info("M4 Phase 4 complete: G2=%d rows, G3=%d rows, G4=%d rows",
                len(g2_report), len(g3_report), len(g4_report))

    # ── Phase 5: Schema Validation & Summary Logging ──
    t0 = time.monotonic()
    logger.info("M4 Phase 5: Schema validation and summary (contract v%s)",
                ANALYSIS_RESULT_SCHEMA_VERSION)

    # 5.1 Per-item schema validation — internal (existing M4 validator) [GP9]
    for result in per_item_results:
        errs = validate_analysis_result_schema(result)
        if errs:
            for e in errs:
                logger.error("Schema validation: row=%s %s", result.get("row_id"), e)
            if not result.get("analysis_degraded"):
                result["analysis_degraded"] = True

    # 5.2 Per-item schema validation — contract (m4_schema) [V3.2 contract lock]
    schema_validate_results_list(per_item_results)

    # 5.3 Global DataFrame validation — internal (existing M4 fallback rules) [GP9, Patch V3]
    g1_errs = validate_global_dataframe_schema(g1_report, "G1", G1_SCHEMA)
    g2_errs = validate_global_dataframe_schema(g2_report, "G2", G2_SCHEMA)
    g3_errs = validate_global_dataframe_schema(g3_report, "G3", G3_SCHEMA)
    g4_errs = validate_global_dataframe_schema(g4_report, "G4", G4_SCHEMA)

    for errs, name, empty_fn in [
        (g1_errs, "G1", _empty_g1), (g2_errs, "G2", _empty_g2),
        (g3_errs, "G3", _empty_g3), (g4_errs, "G4", _empty_g4),
    ]:
        for e in errs:
            logger.error("Schema validation %s: %s", name, e)
        # Rule 6: if identifier column missing, return empty DataFrame
        if any("required identifier column" in e for e in errs):
            logger.error("%s has fatal schema errors. Returning empty DataFrame.", name)
            if name == "G1":
                g1_report = empty_fn()
            elif name == "G2":
                g2_report = empty_fn()
            elif name == "G3":
                g3_report = empty_fn()
            elif name == "G4":
                g4_report = empty_fn()

    # 5.4 Global DataFrame validation — contract (m4_schema) [V3.2 contract lock]
    for name, df_ref in [("G1", g1_report), ("G2", g2_report),
                         ("G3", g3_report), ("G4", g4_report)]:
        contract_errs = schema_validate_dataframe(df_ref, name)
        if contract_errs:
            for e in contract_errs:
                logger.error("Contract validation %s v%s: %s",
                             name, SCHEMA_REGISTRY[name][2], e)
            # Fatal identifier errors -> replace with empty schema-valid DataFrame
            if any("required identifier column" in e for e in contract_errs):
                logger.error("%s contract validation fatal. Replacing with empty DataFrame.", name)
                if name == "G1":
                    g1_report = _empty_g1()
                elif name == "G2":
                    g2_report = _empty_g2()
                elif name == "G3":
                    g3_report = _empty_g3()
                elif name == "G4":
                    g4_report = _empty_g4()

    # 5.5 Final column order enforcement [V3.2 contract lock]
    g1_report = enforce_column_order(g1_report, "G1")
    g2_report = enforce_column_order(g2_report, "G2")
    g3_report = enforce_column_order(g3_report, "G3")
    g4_report = enforce_column_order(g4_report, "G4")

    phase_timings["phase5"] = time.monotonic() - t0

    # Summary log
    log_m4_summary(per_item_results, g1_report, g2_report, g3_report, g4_report, phase_timings)

    return per_item_results, g1_report, g2_report, g3_report, g4_report


# ============================================================================
# HARDENING PATCH — V3.1
# Patch A: 2 index integrity assertions after Phase 1 construction
# Patch B: 4 validation checks in validate_analysis_result_schema
#   - analysis_degraded type check
#   - failed_blocks type check
#   - block sub-dict presence and type check
#   - block_status presence and value check
# ============================================================================
# CONTRACT LOCK PATCH — V3.2
# - Import central schema from jansa_visasist.schemas.m4_schema
# - Replace inline G1–G4 schema dicts with imported OrderedDicts
# - Replace _empty_g* helpers with empty_dataframe() from schema module
# - Enforce canonical column order on all G1–G4 DataFrame outputs
# - Add contract validation layer (schema_validate_*) in Phase 5
# - No business logic changes. No return signature changes.
# ============================================================================
