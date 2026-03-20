"""
Module 5 — Comprehensive Hardening & Validation Test Suite.

Covers:
  1. Decision logic: every lifecycle_state × consensus_type combination
  2. Layer 1 hard overrides
  3. Catch-all fallback (row 2o)
  4. VISA mapping: every action × consensus pair
  5. Confidence formula: correctness, edge cases, worked examples
  6. Escalation: all thresholds (MOEX, DIRECTION), G1 missing
  7. Relance: template selection T1–T6, empty targets, message length
  8. Action priority: boost calculations, clamping
  9. Schema enforcement: all fields, types, sorted lists, reason_details
  10. Determinism & idempotency: bit-level identical JSON
  11. Failure safety: exception fallback, report failure isolation
  12. Integration: full M3→M4→M5 pipeline

Usage:
    python -m jansa_visasist.pipeline.module5.test_m5_hardening
"""

import json
import sys
import traceback
from typing import Any

import pandas as pd


# ============================================================================
# Test Helpers
# ============================================================================

_pass_count = 0
_fail_count = 0
_test_name = ""


def _section(name: str) -> None:
    print(f"\n{'─' * 70}")
    print(f"  {name}")
    print(f"{'─' * 70}")


def _test(name: str) -> None:
    global _test_name
    _test_name = name


def _ok(detail: str = "") -> None:
    global _pass_count
    _pass_count += 1
    suffix = f" — {detail}" if detail else ""
    print(f"  [OK]   {_test_name}{suffix}")


def _fail(detail: str) -> None:
    global _fail_count
    _fail_count += 1
    print(f"  [FAIL] {_test_name} — {detail}")


def _assert_eq(actual: Any, expected: Any, label: str = "") -> bool:
    if actual == expected:
        return True
    _fail(f"{label}: got {actual!r}, expected {expected!r}")
    return False


def _assert_true(cond: bool, label: str = "") -> bool:
    if cond:
        return True
    _fail(label)
    return False


# ============================================================================
# Synthetic Data Builders
# ============================================================================

def _m3_item(**overrides: Any) -> dict[str, Any]:
    """Build a synthetic M3 item dict with defaults."""
    base = {
        "row_id": "TEST-001",
        "category": "WAITING",
        "consensus_type": "INCOMPLETE",
        "priority_score": 50,
        "is_overdue": False,
        "days_overdue": 0,
        "has_deadline": True,
        "missing_approvers": ["BET"],
        "blocking_approvers": [],
        "total_assigned": 4,
        "replied": 3,
        "pending": 1,
        "relevant_approvers": 4,
        "days_since_diffusion": 10,
        "source_sheet": "LOT_01",
        "document": "DOC-X",
        "assigned_approvers": ["BET", "CES", "OPC", "STD"],
    }
    base.update(overrides)
    return base


def _m4_result(**overrides: Any) -> dict[str, Any]:
    """Build a synthetic M4 result dict with defaults."""
    base: dict[str, Any] = {
        "row_id": "TEST-001",
        "lifecycle_state": "WAITING_RESPONSES",
        "analysis_degraded": False,
        "failed_blocks": [],
        "agreement": {
            "agreement_type": "PARTIAL_APPROVAL",
            "approve_count": 3,
            "reject_count": 0,
            "pending_count": 1,
            "hm_count": 0,
            "non_classifiable_count": 0,
            "block_status": "OK",
        },
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 1, "worst_urgency": "LOW", "block_status": "OK"},
        "blocking": {
            "is_blocked": False,
            "blocking_pattern": "NOT_BLOCKED",
            "consecutive_rejections": None,
            "block_status": "OK",
        },
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 10, "block_status": "OK"},
    }
    # Apply overrides — support nested keys like "blocking.consecutive_rejections"
    for key, val in overrides.items():
        if "." in key:
            parts = key.split(".", 1)
            if parts[0] in base and isinstance(base[parts[0]], dict):
                base[parts[0]][parts[1]] = val
            else:
                base[key] = val
        else:
            base[key] = val
    return base


def _g1_index(**entries: dict) -> dict[str, dict]:
    """Build a G1 blocker index from keyword args."""
    return entries


def _run_m5_single(m3: dict, m4: dict, g1: dict | None = None,
                    pipeline_run_id: str = "TEST") -> dict | None:
    """Run compute_suggestion on a single item."""
    from jansa_visasist.pipeline.module5.engine import compute_suggestion
    return compute_suggestion(m3, m4, g1 or {}, pipeline_run_id)


# ============================================================================
# 1. DECISION LOGIC: Every lifecycle × consensus combination
# ============================================================================

def test_decision_logic() -> None:
    _section("1. Decision Logic — lifecycle × consensus")

    from jansa_visasist.pipeline.module5.engine import resolve_action

    # Spec rows 2a–2n (valid combinations)
    cases = [
        # (lifecycle, consensus, consec_rej, overdue, expected_action, expected_reason)
        ("NOT_STARTED",       "NOT_STARTED", 0, False, "HOLD",            "NOT_YET_STARTED"),
        ("NOT_STARTED",       "NOT_STARTED", 0, True,  "HOLD",            "NOT_YET_STARTED"),
        ("WAITING_RESPONSES", "INCOMPLETE",  0, False, "CHASE_APPROVERS", "MISSING_RESPONSES"),
        ("WAITING_RESPONSES", "INCOMPLETE",  0, True,  "CHASE_APPROVERS", "OVERDUE"),
        ("READY_TO_ISSUE",    "ALL_APPROVE", 0, False, "ISSUE_VISA",      "CONSENSUS_APPROVAL"),
        ("READY_TO_ISSUE",    "ALL_APPROVE", 0, True,  "ISSUE_VISA",      "CONSENSUS_APPROVAL"),
        ("READY_TO_REJECT",   "ALL_REJECT",  0, False, "ISSUE_VISA",      "CONSENSUS_REJECTION"),
        ("READY_TO_REJECT",   "ALL_REJECT",  0, True,  "ISSUE_VISA",      "CONSENSUS_REJECTION"),
        ("NEEDS_ARBITRATION", "MIXED",       0, False, "ARBITRATE",       "MIXED_CONFLICT"),
        ("NEEDS_ARBITRATION", "MIXED",       0, True,  "ARBITRATE",       "MIXED_CONFLICT"),
        ("CHRONIC_BLOCKED",   "ALL_REJECT",  3, False, "ESCALATE",        "BLOCKING_LOOP"),
        ("CHRONIC_BLOCKED",   "ALL_REJECT",  3, True,  "ESCALATE",        "BLOCKING_LOOP"),
        ("CHRONIC_BLOCKED",   "ALL_REJECT",  1, False, "ESCALATE",        "BLOCKING_LOOP"),
        ("CHRONIC_BLOCKED",   "ALL_REJECT",  1, True,  "ESCALATE",        "BLOCKING_LOOP"),
    ]

    for ls, ct, cr, ov, exp_act, exp_rc in cases:
        _test(f"L2 {ls}×{ct} ov={ov}")
        action, reason = resolve_action(ls, ct, cr, ov, False)
        ok = _assert_eq(action, exp_act, "action") and _assert_eq(reason, exp_rc, "reason")
        if ok:
            _ok()

    # Row 2o: Catch-all — mismatched lifecycle/consensus
    catch_all_cases = [
        ("READY_TO_ISSUE",    "ALL_REJECT"),
        ("READY_TO_REJECT",   "ALL_APPROVE"),
        ("WAITING_RESPONSES", "MIXED"),
        ("NEEDS_ARBITRATION", "INCOMPLETE"),
        ("CHRONIC_BLOCKED",   "MIXED"),
        ("NOT_STARTED",       "ALL_APPROVE"),
    ]

    for ls, ct in catch_all_cases:
        _test(f"L2 catch-all {ls}×{ct}")
        action, reason = resolve_action(ls, ct, 0, False, False)
        ok = _assert_eq(action, "HOLD", "action") and _assert_eq(reason, "DEGRADED_ANALYSIS", "reason")
        if ok:
            _ok()


# ============================================================================
# 2. LAYER 1: Hard Overrides
# ============================================================================

def test_layer1_overrides() -> None:
    _section("2. Layer 1 — Hard Overrides")

    from jansa_visasist.pipeline.module5.engine import resolve_action

    # Row 1a: ON_HOLD
    _test("L1 ON_HOLD override")
    a, r = resolve_action("ON_HOLD", "INCOMPLETE", 0, False, False)
    if _assert_eq(a, "HOLD") and _assert_eq(r, "DEGRADED_ANALYSIS"):
        _ok()

    # Row 1b: analysis_degraded
    _test("L1 analysis_degraded override")
    a, r = resolve_action("READY_TO_ISSUE", "ALL_APPROVE", 0, False, True)
    if _assert_eq(a, "HOLD") and _assert_eq(r, "DEGRADED_ANALYSIS"):
        _ok()

    # ON_HOLD takes precedence even with analysis_degraded=False
    _test("L1 ON_HOLD before analysis_degraded check")
    a, r = resolve_action("ON_HOLD", "ALL_APPROVE", 0, False, False)
    if _assert_eq(a, "HOLD"):
        _ok()


# ============================================================================
# 3. VISA MAPPING: Every action × consensus pair
# ============================================================================

def test_visa_mapping() -> None:
    _section("3. Layer 3 — VISA Mapping")

    from jansa_visasist.pipeline.module5.engine import resolve_visa

    cases = [
        ("ISSUE_VISA",      "ALL_APPROVE", "APPROVE"),
        ("ISSUE_VISA",      "ALL_REJECT",  "REJECT"),
        ("CHASE_APPROVERS", "INCOMPLETE",  "WAIT"),
        ("CHASE_APPROVERS", "NOT_STARTED", "WAIT"),
        ("ARBITRATE",       "MIXED",       "NONE"),
        ("ESCALATE",        "ALL_REJECT",  "NONE"),
        ("ESCALATE",        "MIXED",       "NONE"),
        ("ESCALATE",        "INCOMPLETE",  "NONE"),
        ("HOLD",            "NOT_STARTED", "NONE"),
        # HOLD with any other consensus
        ("HOLD",            "ALL_APPROVE", "NONE"),
        ("HOLD",            "MIXED",       "NONE"),
        ("HOLD",            "INCOMPLETE",  "NONE"),
        ("HOLD",            "ALL_REJECT",  "NONE"),
    ]

    for action, consensus, expected_visa in cases:
        _test(f"L3 {action}×{consensus}")
        visa = resolve_visa(action, consensus)
        if _assert_eq(visa, expected_visa):
            _ok()

    # Fallback (row 3l)
    _test("L3 fallback unknown pair")
    visa = resolve_visa("UNKNOWN_ACTION", "UNKNOWN_CONSENSUS")
    if _assert_eq(visa, "NONE"):
        _ok()


# ============================================================================
# 4. CONFIDENCE FORMULA
# ============================================================================

def test_confidence() -> None:
    _section("4. Layer 6 — Confidence Formula")

    from jansa_visasist.pipeline.module5.confidence import compute_confidence

    # Worked example 1: ALL_APPROVE, complete, no issues → 0.8500
    _test("Confidence: ALL_APPROVE complete")
    m3 = _m3_item(consensus_type="ALL_APPROVE", is_overdue=False, days_overdue=0,
                   relevant_approvers=4, replied=4, total_assigned=4)
    m4 = _m4_result(
        lifecycle_state="READY_TO_ISSUE",
        **{"agreement.approve_count": 4, "agreement.reject_count": 0,
           "missing.total_missing": 0}
    )
    c = compute_confidence(m3, m4, False, "READY_TO_ISSUE")
    if _assert_eq(c, 0.85, "confidence"):
        _ok()

    # Worked example 2: NOT_STARTED, no responses → 0.3000
    _test("Confidence: NOT_STARTED zero responses")
    m3 = _m3_item(consensus_type="NOT_STARTED", replied=0, total_assigned=3,
                   relevant_approvers=3, is_overdue=False, days_overdue=0)
    m4 = _m4_result(
        lifecycle_state="NOT_STARTED",
        **{"agreement.approve_count": 0, "agreement.reject_count": 0,
           "missing.total_missing": 3}
    )
    c = compute_confidence(m3, m4, False, "NOT_STARTED")
    # 0.30 + 0.35*0 + 0.20*0 - 0.15*(3/3) - 0 - 0 - 0 = 0.30 - 0.15 = 0.15
    if _assert_eq(c, 0.15, "confidence"):
        _ok()

    # Hard override: degraded → 0.0
    _test("Confidence: degraded override")
    c = compute_confidence(m3, m4, True, "ON_HOLD")
    if _assert_eq(c, 0.0):
        _ok()

    # Hard override: ON_HOLD → 0.0
    _test("Confidence: ON_HOLD override")
    c = compute_confidence(m3, m4, False, "ON_HOLD")
    if _assert_eq(c, 0.0):
        _ok()

    # MIXED conflict penalty
    _test("Confidence: MIXED conflict penalty")
    m3 = _m3_item(consensus_type="MIXED", replied=4, total_assigned=4,
                   relevant_approvers=4, is_overdue=True, days_overdue=15)
    m4 = _m4_result(
        **{"agreement.approve_count": 2, "agreement.reject_count": 2,
           "missing.total_missing": 0}
    )
    c = compute_confidence(m3, m4, False, "NEEDS_ARBITRATION")
    # 0.30 + 0.35*0.5 + 0.20*1.0 - 0.15*0 - 0.25*1.0 - 0.10*(15/60) - 0
    # = 0.30 + 0.175 + 0.20 - 0.25 - 0.025 = 0.40
    if _assert_eq(c, 0.4, "confidence"):
        _ok()

    # Clamp: never below 0
    _test("Confidence: clamp to 0")
    m3 = _m3_item(consensus_type="MIXED", is_overdue=True, days_overdue=100,
                   replied=0, total_assigned=4, relevant_approvers=4)
    m4 = _m4_result(
        **{"agreement.approve_count": 0, "agreement.reject_count": 0,
           "missing.total_missing": 4}
    )
    c = compute_confidence(m3, m4, False, "NEEDS_ARBITRATION")
    if _assert_true(c >= 0.0, f"clamp to 0: got {c}"):
        _ok()

    # Precision: always 4 decimal places
    _test("Confidence: 4 decimal precision")
    if _assert_eq(len(str(c).split(".")[-1]) <= 4, True, "decimal places"):
        _ok()


# ============================================================================
# 5. ESCALATION THRESHOLDS
# ============================================================================

def test_escalation() -> None:
    _section("5. Layer 5 — Escalation")

    from jansa_visasist.pipeline.module5.escalation import resolve_escalation

    # Rule 1: consec >= 3 → DIRECTION
    _test("Esc R1: consec_rej=3 → DIRECTION")
    level, _ = resolve_escalation(3, 0, False, [], {}, "ESCALATE")
    if _assert_eq(level, "DIRECTION"):
        _ok()

    # Rule 2: days_overdue >= 60 → DIRECTION (no is_overdue gate)
    _test("Esc R2: days_overdue=60 → DIRECTION")
    level, _ = resolve_escalation(0, 60, False, [], {}, "HOLD")
    if _assert_eq(level, "DIRECTION"):
        _ok()

    # Rule 2 fires even with is_overdue=False
    _test("Esc R2: days_overdue=60 + is_overdue=False → still DIRECTION")
    level, _ = resolve_escalation(0, 65, False, [], {}, "HOLD")
    if _assert_eq(level, "DIRECTION"):
        _ok()

    # Rule 3: G1 systemic >= 10 → DIRECTION
    _test("Esc R3: G1 systemic ≥10 → DIRECTION")
    g1 = {"BET": {"is_systemic_blocker": True, "total_blocking": 12}}
    level, syst = resolve_escalation(0, 0, False, ["BET"], g1, "HOLD")
    if _assert_eq(level, "DIRECTION") and _assert_eq(syst, ["BET"]):
        _ok()

    # Rule 4: consec >= 2 → MOEX
    _test("Esc R4: consec_rej=2 → MOEX")
    level, _ = resolve_escalation(2, 0, False, [], {}, "ESCALATE")
    if _assert_eq(level, "MOEX"):
        _ok()

    # Rule 5: days_overdue >= 30 → MOEX (no is_overdue gate)
    _test("Esc R5: days_overdue=30 → MOEX")
    level, _ = resolve_escalation(0, 30, False, [], {}, "HOLD")
    if _assert_eq(level, "MOEX"):
        _ok()

    # Rule 6: G1 systemic >= 5 → MOEX
    _test("Esc R6: G1 systemic ≥5 → MOEX")
    g1 = {"CES": {"is_systemic_blocker": True, "total_blocking": 6}}
    level, _ = resolve_escalation(0, 0, False, ["CES"], g1, "HOLD")
    if _assert_eq(level, "MOEX"):
        _ok()

    # Rule 7: ESCALATE action → at least MOEX
    _test("Esc R7: ESCALATE action → MOEX")
    level, _ = resolve_escalation(0, 0, False, [], {}, "ESCALATE")
    if _assert_eq(level, "MOEX"):
        _ok()

    # Rule 8: no threshold → NONE
    _test("Esc R8: no threshold → NONE")
    level, _ = resolve_escalation(0, 0, False, [], {}, "HOLD")
    if _assert_eq(level, "NONE"):
        _ok()

    # Edge Case #12: G1 empty → skip systemic rules
    _test("Esc: G1 empty → skip systemic, fall through to R7")
    level, _ = resolve_escalation(0, 0, False, ["BET"], {}, "ESCALATE")
    if _assert_eq(level, "MOEX"):
        _ok()

    # Priority ordering: R1 before R4
    _test("Esc priority: consec=3 hits R1 before R4")
    level, _ = resolve_escalation(3, 35, True, [], {}, "ESCALATE")
    if _assert_eq(level, "DIRECTION"):
        _ok()


# ============================================================================
# 6. RELANCE: Template Selection & Edge Cases
# ============================================================================

def test_relance() -> None:
    _section("6. Layer 4 — Relance")

    from jansa_visasist.pipeline.module5.relance import (
        generate_relance_message,
        resolve_relance,
    )

    # T1: CHASE not overdue
    _test("Relance T1: CHASE + not overdue")
    m3 = _m3_item(is_overdue=False, missing_approvers=["BET", "CES"])
    m4 = _m4_result()
    r = resolve_relance(m3, m4, "CHASE_APPROVERS", False, "WAITING_RESPONSES", 0, {})
    if _assert_eq(r["relance_required"], True) and _assert_eq(r["relance_template_id"], "T1"):
        _ok()

    # T2: CHASE overdue — verify {days} uses days_since_diffusion [FIX 2]
    _test("Relance T2: CHASE + overdue uses days_since_diffusion")
    m3 = _m3_item(is_overdue=True, days_overdue=20, days_since_diffusion=25,
                   missing_approvers=["BET"])
    r = resolve_relance(m3, m4, "CHASE_APPROVERS", True, "WAITING_RESPONSES", 0, {})
    if _assert_eq(r["relance_template_id"], "T2"):
        # Message should contain "25" (days_since_diffusion) not "20" (days_overdue)
        if _assert_true("25 jours" in r["relance_message"], "T2 uses days_since_diffusion=25"):
            _ok()

    # T3: ESCALATE
    _test("Relance T3: ESCALATE")
    m3 = _m3_item(blocking_approvers=["BET"], days_overdue=45)
    r = resolve_relance(m3, m4, "ESCALATE", True, "CHRONIC_BLOCKED", 3, {})
    if _assert_eq(r["relance_template_id"], "T3"):
        _ok()

    # T4: ARBITRATE
    _test("Relance T4: ARBITRATE")
    m3 = _m3_item(assigned_approvers=["BET", "CES", "OPC"], missing_approvers=[])
    r = resolve_relance(m3, m4, "ARBITRATE", False, "NEEDS_ARBITRATION", 0, {})
    if _assert_eq(r["relance_template_id"], "T4"):
        # Targets should be all assigned (since no missing)
        if _assert_eq(r["relance_targets"], ["BET", "CES", "OPC"]):
            _ok()

    # T5: HOLD NOT_STARTED + days >= 7
    _test("Relance T5: HOLD NOT_STARTED days=10")
    m3 = _m3_item(days_since_diffusion=10, assigned_approvers=["BET", "CES"])
    r = resolve_relance(m3, m4, "HOLD", False, "NOT_STARTED", 0, {})
    if _assert_eq(r["relance_template_id"], "T5") and _assert_eq(r["relance_required"], True):
        _ok()

    # No relance: HOLD NOT_STARTED + days < 7
    _test("Relance: HOLD NOT_STARTED days=3 → no relance")
    m3 = _m3_item(days_since_diffusion=3)
    r = resolve_relance(m3, m4, "HOLD", False, "NOT_STARTED", 0, {})
    if _assert_eq(r["relance_required"], False):
        _ok()

    # No relance: ISSUE_VISA
    _test("Relance: ISSUE_VISA → no relance")
    r = resolve_relance(m3, m4, "ISSUE_VISA", False, "READY_TO_ISSUE", 0, {})
    if _assert_eq(r["relance_required"], False):
        _ok()

    # Edge Case #11: CHASE with empty missing_approvers
    _test("Relance: CHASE + empty missing → no relance")
    m3 = _m3_item(missing_approvers=[])
    r = resolve_relance(m3, m4, "CHASE_APPROVERS", False, "WAITING_RESPONSES", 0, {})
    if _assert_eq(r["relance_required"], False):
        _ok()

    # Message length ≤ 200
    _test("Relance: all templates ≤ 200 chars")
    all_ok = True
    for tid in ["T1", "T2", "T3", "T4", "T5", "T6"]:
        params = {"document": "DOC-LONGNAME-TEST", "lot": "LOT_LONG", "days": "999"}
        msg = generate_relance_message(tid, params, "TEST")
        if msg and len(msg) > 200:
            _fail(f"Template {tid} exceeds 200 chars: {len(msg)}")
            all_ok = False
    if all_ok:
        _ok()

    # Targets always sorted
    _test("Relance: targets sorted alphabetically")
    m3 = _m3_item(missing_approvers=["OPC", "BET", "CES"])
    r = resolve_relance(m3, m4, "CHASE_APPROVERS", False, "WAITING_RESPONSES", 0, {})
    if _assert_eq(r["relance_targets"], ["BET", "CES", "OPC"]):
        _ok()


# ============================================================================
# 7. ACTION PRIORITY
# ============================================================================

def test_action_priority() -> None:
    _section("7. Action Priority")

    from jansa_visasist.pipeline.module5.priority import (
        compute_action_priority,
        get_priority_band,
    )

    # Base only
    _test("Priority: base=50, no boosts")
    p = compute_action_priority(50, "NONE", False, 0, [])
    if _assert_eq(p, 50):
        _ok()

    # Escalation boost MOEX=10
    _test("Priority: base=50 + MOEX=10 → 60")
    p = compute_action_priority(50, "MOEX", False, 0, [])
    if _assert_eq(p, 60):
        _ok()

    # Escalation boost DIRECTION=20
    _test("Priority: base=50 + DIRECTION=20 → 70")
    p = compute_action_priority(50, "DIRECTION", False, 0, [])
    if _assert_eq(p, 70):
        _ok()

    # Overdue boost: min(20,30)*0.5 = 10
    _test("Priority: base=50 + overdue 20d → 60")
    p = compute_action_priority(50, "NONE", True, 20, [])
    if _assert_eq(p, 60):
        _ok()

    # Overdue cap: min(50,30)*0.5 = 15
    _test("Priority: overdue cap at 30d → +15")
    p = compute_action_priority(50, "NONE", True, 50, [])
    if _assert_eq(p, 65):
        _ok()

    # Blocking boost: +5
    _test("Priority: blocking boost → +5")
    p = compute_action_priority(50, "NONE", False, 0, ["BET"])
    if _assert_eq(p, 55):
        _ok()

    # Clamp at 100
    _test("Priority: clamp at 100")
    p = compute_action_priority(95, "DIRECTION", True, 30, ["BET"])
    if _assert_eq(p, 100):
        _ok()

    # Clamp at 0
    _test("Priority: clamp at 0")
    p = compute_action_priority(0, "NONE", False, 0, [])
    if _assert_eq(p, 0):
        _ok()

    # Priority bands
    _test("Priority bands: thresholds")
    ok = (
        _assert_eq(get_priority_band(100), "CRITICAL", "100")
        and _assert_eq(get_priority_band(80), "CRITICAL", "80")
        and _assert_eq(get_priority_band(79), "HIGH", "79")
        and _assert_eq(get_priority_band(60), "HIGH", "60")
        and _assert_eq(get_priority_band(59), "MEDIUM", "59")
        and _assert_eq(get_priority_band(40), "MEDIUM", "40")
        and _assert_eq(get_priority_band(39), "LOW", "39")
        and _assert_eq(get_priority_band(0), "LOW", "0")
    )
    if ok:
        _ok()


# ============================================================================
# 8. SCHEMA ENFORCEMENT
# ============================================================================

def test_schema_enforcement() -> None:
    _section("8. Schema Enforcement")

    from jansa_visasist.pipeline.module5.schemas import (
        REASON_DETAILS_KEYS,
        SUGGESTION_RESULT_FIELDS,
    )

    m3 = _m3_item()
    m4 = _m4_result()
    sr = _run_m5_single(m3, m4)

    # All 18 fields present
    _test("Schema: all 18 fields present")
    missing_fields = [f for f in SUGGESTION_RESULT_FIELDS if f not in sr]
    if _assert_eq(missing_fields, [], "missing fields"):
        _ok()

    # No extra fields (except source_sheet/document added by runner)
    _test("Schema: no unexpected fields")
    expected_keys = set(SUGGESTION_RESULT_FIELDS)
    actual_keys = set(sr.keys())
    extra = actual_keys - expected_keys
    if _assert_eq(extra, set(), "extra fields"):
        _ok()

    # Type checks
    _test("Schema: type checks")
    ok = True
    if not isinstance(sr["row_id"], str):
        _fail("row_id not str"); ok = False
    if not isinstance(sr["suggested_action"], str):
        _fail("suggested_action not str"); ok = False
    if not isinstance(sr["action_priority"], int):
        _fail("action_priority not int"); ok = False
    if not isinstance(sr["confidence"], float):
        _fail("confidence not float"); ok = False
    if not isinstance(sr["reason_details"], dict):
        _fail("reason_details not dict"); ok = False
    if not isinstance(sr["blocking_approvers"], list):
        _fail("blocking_approvers not list"); ok = False
    if not isinstance(sr["missing_approvers"], list):
        _fail("missing_approvers not list"); ok = False
    if not isinstance(sr["relance_required"], bool):
        _fail("relance_required not bool"); ok = False
    if not isinstance(sr["escalation_required"], bool):
        _fail("escalation_required not bool"); ok = False
    if not isinstance(sr["analysis_degraded"], bool):
        _fail("analysis_degraded not bool"); ok = False
    if ok:
        _ok()

    # reason_details: exactly 7 keys, alphabetically sorted
    _test("Schema: reason_details 7 keys, sorted")
    rd = sr["reason_details"]
    rd_keys = list(rd.keys())
    if _assert_eq(len(rd_keys), 7, "key count") and _assert_eq(rd_keys, sorted(rd_keys), "sorted"):
        if _assert_eq(rd_keys, REASON_DETAILS_KEYS, "exact keys"):
            _ok()

    # Lists sorted
    _test("Schema: lists sorted alphabetically")
    ok = True
    for field in ["blocking_approvers", "missing_approvers", "relance_targets"]:
        lst = sr[field]
        if lst != sorted(lst):
            _fail(f"{field} not sorted: {lst}")
            ok = False
    if ok:
        _ok()

    # Confidence: 4 decimal places
    _test("Schema: confidence 4 decimal places")
    c_str = f"{sr['confidence']:.4f}"
    if _assert_eq(sr["confidence"], float(c_str), "precision"):
        _ok()

    # Null policy: never-null fields
    _test("Schema: never-null fields")
    never_null = [
        "row_id", "suggested_action", "action_priority", "proposed_visa",
        "confidence", "reason_code", "reason_details", "blocking_approvers",
        "missing_approvers", "relance_required", "relance_targets",
        "escalation_required", "escalation_level", "based_on_lifecycle",
        "analysis_degraded", "pipeline_run_id",
    ]
    ok = True
    for field in never_null:
        if sr[field] is None:
            _fail(f"{field} is None"); ok = False
    if ok:
        _ok()


# ============================================================================
# 9. DETERMINISM & IDEMPOTENCY
# ============================================================================

def test_idempotency() -> None:
    _section("9. Determinism & Idempotency")

    from jansa_visasist.pipeline.module5.runner import run_module5

    # Build multi-item dataset
    m3_rows = [
        _m3_item(row_id="A", consensus_type="ALL_APPROVE", priority_score=90,
                  blocking_approvers=[], missing_approvers=[]),
        _m3_item(row_id="B", consensus_type="INCOMPLETE", is_overdue=True,
                  days_overdue=20, priority_score=60),
        _m3_item(row_id="C", consensus_type="ALL_REJECT", priority_score=70,
                  blocking_approvers=["BET"], missing_approvers=[]),
    ]
    m4_list = [
        _m4_result(row_id="A", lifecycle_state="READY_TO_ISSUE",
                    **{"agreement.approve_count": 4, "agreement.reject_count": 0,
                       "missing.total_missing": 0}),
        _m4_result(row_id="B", lifecycle_state="WAITING_RESPONSES"),
        _m4_result(row_id="C", lifecycle_state="READY_TO_REJECT",
                    **{"agreement.approve_count": 0, "agreement.reject_count": 3,
                       "blocking.consecutive_rejections": 1}),
    ]
    m3_df = pd.DataFrame(m3_rows)
    g1_df = pd.DataFrame([
        {"approver_key": "BET", "is_systemic_blocker": False, "total_blocking": 1},
    ])

    # Run twice
    _test("Idempotency: two runs produce identical JSON")
    r1, s1a, s2a, s3a = run_module5(m3_df, m4_list, g1_df, "IDEM-001")
    r2, s1b, s2b, s3b = run_module5(m3_df, m4_list, g1_df, "IDEM-001")
    j1 = json.dumps(r1, sort_keys=True, ensure_ascii=False)
    j2 = json.dumps(r2, sort_keys=True, ensure_ascii=False)
    if _assert_eq(j1, j2, "bit-level JSON"):
        _ok()

    # Dict keys sorted in serialized JSON (spec §10.3: "sorted on serialization")
    # Note: in-memory insertion order may differ (runner enriches with source_sheet/document
    # after engine assembly), but json.dumps(sort_keys=True) guarantees sorted output.
    _test("Idempotency: serialized JSON keys sorted")
    for sr in r1:
        serialized = json.dumps(sr, sort_keys=True, ensure_ascii=False)
        roundtripped = json.loads(serialized)
        rt_keys = list(roundtripped.keys())
        if rt_keys != sorted(rt_keys):
            _fail(f"serialized keys not sorted for {sr['row_id']}: {rt_keys}")
            return
    _ok()

    # Reports identical
    _test("Idempotency: S1 report identical")
    if _assert_eq(s1a.to_json(), s1b.to_json()):
        _ok()
    _test("Idempotency: S2 report identical")
    if _assert_eq(s2a.to_json(), s2b.to_json()):
        _ok()
    _test("Idempotency: S3 report identical")
    if _assert_eq(s3a.to_json(), s3b.to_json()):
        _ok()


# ============================================================================
# 10. FAILURE SAFETY
# ============================================================================

def test_failure_safety() -> None:
    _section("10. Failure Safety")

    from jansa_visasist.pipeline.module5.engine import compute_suggestion
    from jansa_visasist.pipeline.module5.runner import run_module5

    # Item with completely broken M4 → safe defaults, no crash
    _test("Failure: broken M4 → safe defaults")
    m3 = _m3_item(row_id="BROKEN-001")
    m4 = {"row_id": "BROKEN-001"}  # Missing everything
    sr = compute_suggestion(m3, m4, {}, "FAIL-TEST")
    if sr is not None:
        ok = (
            _assert_eq(sr["suggested_action"], "HOLD", "action")
            and _assert_eq(sr["proposed_visa"], "NONE", "visa")
            and _assert_eq(sr["confidence"], 0.0, "confidence")
        )
        if ok:
            _ok()
    else:
        _fail("returned None instead of safe defaults")

    # M4 with None blocks → no crash
    _test("Failure: M4 None blocks → no crash")
    m4 = _m4_result(agreement=None, missing=None, blocking=None, time=None)
    sr = compute_suggestion(_m3_item(), m4, {}, "FAIL-TEST")
    if _assert_true(sr is not None, "should not be None"):
        _ok()

    # Empty M3 → empty results, no crash
    _test("Failure: empty M3 queue → empty results")
    empty_m3 = pd.DataFrame(columns=[
        "row_id", "category", "consensus_type", "priority_score",
        "is_overdue", "days_overdue", "has_deadline", "missing_approvers",
        "blocking_approvers", "total_assigned", "replied", "pending",
        "relevant_approvers", "days_since_diffusion", "source_sheet", "document",
    ])
    results, s1, s2, s3 = run_module5(empty_m3, [], pd.DataFrame(), "EMPTY")
    if _assert_eq(len(results), 0) and _assert_eq(len(s1), 0):
        _ok()

    # Report failure isolation: corrupt a result dict, reports should still work
    _test("Failure: report generation with edge data → no crash")
    m3_rows = [_m3_item(row_id="OK-001")]
    m4_list = [_m4_result(row_id="OK-001")]
    m3_df = pd.DataFrame(m3_rows)
    try:
        results, s1, s2, s3 = run_module5(m3_df, m4_list, pd.DataFrame(), "SAFE")
        _assert_true(len(results) == 1, "1 result produced")
        _ok()
    except Exception as e:
        _fail(f"crashed: {e}")


# ============================================================================
# 11. UPSTREAM COMPATIBILITY (M4 field paths)
# ============================================================================

def test_upstream_compatibility() -> None:
    _section("11. Upstream Compatibility — M4 field paths")

    from jansa_visasist.pipeline.module5.validation import (
        safe_get_m4_agreement_ratio,
        safe_get_m4_consecutive_rejections,
        safe_get_m4_days_since_last_action,
        safe_get_m4_missing_count,
    )

    # Test with actual M4 output structure (block names, not A1/A3/A4/A6)
    m4 = {
        "row_id": "COMPAT-001",
        "agreement": {"approve_count": 3, "reject_count": 1},
        "missing": {"total_missing": 2},
        "blocking": {"consecutive_rejections": 4},
        "time": {"days_since_diffusion": 15},
    }

    _test("M4 compat: agreement_ratio from agreement block")
    ratio = safe_get_m4_agreement_ratio(m4)
    if _assert_eq(ratio, 0.75, "3/(3+1)"):
        _ok()

    _test("M4 compat: missing_count from missing.total_missing")
    mc = safe_get_m4_missing_count(m4)
    if _assert_eq(mc, 2):
        _ok()

    _test("M4 compat: consecutive_rejections from blocking block")
    cr = safe_get_m4_consecutive_rejections(m4)
    if _assert_eq(cr, 4):
        _ok()

    _test("M4 compat: days_since_last_action from time.days_since_diffusion")
    d = safe_get_m4_days_since_last_action(m4)
    if _assert_eq(d, 15):
        _ok()

    # Graceful fallback on missing blocks
    _test("M4 compat: empty dict → safe defaults")
    empty = {"row_id": "EMPTY"}
    ok = (
        _assert_eq(safe_get_m4_agreement_ratio(empty), 0.0, "ratio")
        and _assert_eq(safe_get_m4_missing_count(empty), 0, "missing")
        and _assert_eq(safe_get_m4_consecutive_rejections(empty), 0, "consec")
        and _assert_eq(safe_get_m4_days_since_last_action(empty), None, "days")
    )
    if ok:
        _ok()


# ============================================================================
# 12. ENUM GP8 COMPLIANCE
# ============================================================================

def test_gp8_compliance() -> None:
    _section("12. GP8 Enum Compliance")

    from jansa_visasist.pipeline.module5.enums import validate_enum

    # Valid values pass
    _test("GP8: all valid suggested_action values")
    ok = all(validate_enum(v, "suggested_action")
             for v in ["ISSUE_VISA", "CHASE_APPROVERS", "ARBITRATE", "ESCALATE", "HOLD"])
    if _assert_true(ok):
        _ok()

    _test("GP8: all valid proposed_visa values")
    ok = all(validate_enum(v, "proposed_visa") for v in ["APPROVE", "REJECT", "WAIT", "NONE"])
    if _assert_true(ok):
        _ok()

    _test("GP8: all valid escalation_level values")
    ok = all(validate_enum(v, "escalation_level") for v in ["NONE", "MOEX", "DIRECTION"])
    if _assert_true(ok):
        _ok()

    # Invalid values fail
    _test("GP8: invalid enum value → False")
    if _assert_eq(validate_enum("INVALID", "suggested_action"), False):
        _ok()

    _test("GP8: unknown enum name → False")
    if _assert_eq(validate_enum("VALUE", "nonexistent_enum"), False):
        _ok()


# ============================================================================
# 13. INTEGRATION: Full pipeline
# ============================================================================

def test_integration() -> None:
    _section("13. Integration — Full M3→M4→M5 pipeline")

    from jansa_visasist.pipeline.module5.runner import run_module5
    from jansa_visasist.pipeline.module5.schemas import S1_COLUMNS, S2_COLUMNS, S3_COLUMNS

    # Build realistic multi-lot dataset
    m3_rows = [
        _m3_item(row_id="INT-001", consensus_type="ALL_APPROVE", priority_score=85,
                  blocking_approvers=[], missing_approvers=[], source_sheet="LOT_A"),
        _m3_item(row_id="INT-002", consensus_type="INCOMPLETE", priority_score=60,
                  is_overdue=True, days_overdue=25, source_sheet="LOT_A"),
        _m3_item(row_id="INT-003", consensus_type="ALL_REJECT", priority_score=70,
                  blocking_approvers=["BET"], missing_approvers=[], source_sheet="LOT_B"),
        _m3_item(row_id="INT-004", consensus_type="MIXED", priority_score=55,
                  blocking_approvers=["BET"], missing_approvers=[], source_sheet="LOT_B"),
        _m3_item(row_id="INT-005", consensus_type="NOT_STARTED", priority_score=30,
                  days_since_diffusion=3, source_sheet="LOT_C",
                  missing_approvers=["BET", "CES", "OPC"]),
    ]
    m4_list = [
        _m4_result(row_id="INT-001", lifecycle_state="READY_TO_ISSUE",
                    **{"agreement.approve_count": 4, "agreement.reject_count": 0,
                       "missing.total_missing": 0}),
        _m4_result(row_id="INT-002", lifecycle_state="WAITING_RESPONSES"),
        _m4_result(row_id="INT-003", lifecycle_state="READY_TO_REJECT",
                    **{"agreement.approve_count": 0, "agreement.reject_count": 3,
                       "blocking.consecutive_rejections": 1}),
        _m4_result(row_id="INT-004", lifecycle_state="NEEDS_ARBITRATION",
                    **{"agreement.approve_count": 2, "agreement.reject_count": 2}),
        _m4_result(row_id="INT-005", lifecycle_state="NOT_STARTED",
                    **{"agreement.approve_count": 0, "agreement.reject_count": 0,
                       "missing.total_missing": 3}),
    ]
    m3_df = pd.DataFrame(m3_rows)
    g1_df = pd.DataFrame([
        {"approver_key": "BET", "is_systemic_blocker": True, "total_blocking": 7,
         "blocked_families": [], "severity": "HIGH"},
    ])

    results, s1, s2, s3 = run_module5(m3_df, m4_list, g1_df, "INTEG-001")

    _test("Integration: correct result count")
    if _assert_eq(len(results), 5):
        _ok()

    # S1 schema
    _test("Integration: S1 schema matches")
    if _assert_eq(list(s1.columns), S1_COLUMNS) and _assert_true(len(s1) > 0, "non-empty"):
        _ok()

    # S2 schema
    _test("Integration: S2 schema matches")
    if _assert_eq(list(s2.columns), S2_COLUMNS) and _assert_true(len(s2) > 0):
        _ok()

    # S3 schema
    _test("Integration: S3 schema matches")
    if _assert_eq(list(s3.columns), S3_COLUMNS):
        _ok()

    # S3 count matches relance_required items
    _test("Integration: S3 count = relance items")
    relance_count = sum(1 for r in results if r["relance_required"])
    if _assert_eq(len(s3), relance_count, "S3 row count"):
        _ok()

    # No mutation check: verify m3_df unchanged
    _test("Integration: M3 not mutated")
    if _assert_eq(len(m3_df), 5) and _assert_eq(list(m3_df.columns), list(pd.DataFrame(m3_rows).columns)):
        _ok()

    # Every result passes schema validation
    _test("Integration: every result valid")
    ok = True
    for sr in results:
        if sr["confidence"] < 0.0 or sr["confidence"] > 1.0:
            _fail(f"{sr['row_id']}: confidence={sr['confidence']} out of range")
            ok = False
        if sr["action_priority"] < 0 or sr["action_priority"] > 100:
            _fail(f"{sr['row_id']}: action_priority={sr['action_priority']} out of range")
            ok = False
        if not isinstance(sr["reason_details"], dict) or len(sr["reason_details"]) != 7:
            _fail(f"{sr['row_id']}: reason_details malformed")
            ok = False
    if ok:
        _ok()


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    global _pass_count, _fail_count

    print("=" * 70)
    print("  MODULE 5 — HARDENING & VALIDATION TEST SUITE")
    print("=" * 70)

    test_functions = [
        test_decision_logic,
        test_layer1_overrides,
        test_visa_mapping,
        test_confidence,
        test_escalation,
        test_relance,
        test_action_priority,
        test_schema_enforcement,
        test_idempotency,
        test_failure_safety,
        test_upstream_compatibility,
        test_gp8_compliance,
        test_integration,
    ]

    for fn in test_functions:
        try:
            fn()
        except Exception:
            print(f"\n  [CRASH] {fn.__name__} raised an unhandled exception:")
            traceback.print_exc()
            _fail_count += 1

    print(f"\n{'=' * 70}")
    print(f"  RESULTS: {_pass_count} passed, {_fail_count} failed")
    print(f"{'=' * 70}")

    return 1 if _fail_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
