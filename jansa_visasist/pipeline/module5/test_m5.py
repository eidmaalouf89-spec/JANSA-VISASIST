"""
Module 5 — Smoke Test.

Creates synthetic M3 queue + M4 results covering all lifecycle states,
runs run_module5(), and validates outputs against plan expectations.

Usage:
    python -m jansa_visasist.pipeline.module5.test_m5
"""

import json
import sys
import pandas as pd

# ---------------------------------------------------------------------------
# 1. Synthetic M3 Queue (one row per lifecycle path)
# ---------------------------------------------------------------------------

M3_ROWS = [
    # Row 0: READY_TO_ISSUE — should produce ISSUE_VISA / APPROVE
    {
        "row_id": "ITEM-001", "category": "EASY_WIN_APPROVE",
        "consensus_type": "ALL_APPROVE", "priority_score": 85,
        "is_overdue": False, "days_overdue": 0, "has_deadline": True,
        "missing_approvers": [], "blocking_approvers": [],
        "total_assigned": 4, "replied": 4, "pending": 0,
        "relevant_approvers": 4, "days_since_diffusion": 10,
        "source_sheet": "LOT_01", "document": "DOC-A",
        "assigned_approvers": ["BET", "CES", "OPC", "STD"],
    },
    # Row 1: WAITING_RESPONSES + overdue — CHASE_APPROVERS / OVERDUE
    {
        "row_id": "ITEM-002", "category": "WAITING",
        "consensus_type": "INCOMPLETE", "priority_score": 60,
        "is_overdue": True, "days_overdue": 20, "has_deadline": True,
        "missing_approvers": ["BET", "CES"], "blocking_approvers": [],
        "total_assigned": 4, "replied": 2, "pending": 2,
        "relevant_approvers": 4, "days_since_diffusion": 25,
        "source_sheet": "LOT_02", "document": "DOC-B",
        "assigned_approvers": ["BET", "CES", "OPC", "STD"],
    },
    # Row 2: READY_TO_REJECT — ISSUE_VISA / REJECT
    {
        "row_id": "ITEM-003", "category": "FAST_REJECT",
        "consensus_type": "ALL_REJECT", "priority_score": 70,
        "is_overdue": False, "days_overdue": 0, "has_deadline": True,
        "missing_approvers": [], "blocking_approvers": ["BET", "CES"],
        "total_assigned": 3, "replied": 3, "pending": 0,
        "relevant_approvers": 3, "days_since_diffusion": 15,
        "source_sheet": "LOT_01", "document": "DOC-C",
        "assigned_approvers": ["BET", "CES", "OPC"],
    },
    # Row 3: NEEDS_ARBITRATION — ARBITRATE / MIXED
    {
        "row_id": "ITEM-004", "category": "CONFLICT",
        "consensus_type": "MIXED", "priority_score": 55,
        "is_overdue": False, "days_overdue": 0, "has_deadline": True,
        "missing_approvers": [], "blocking_approvers": ["BET"],
        "total_assigned": 4, "replied": 4, "pending": 0,
        "relevant_approvers": 4, "days_since_diffusion": 12,
        "source_sheet": "LOT_03", "document": "DOC-D",
        "assigned_approvers": ["BET", "CES", "OPC", "STD"],
    },
    # Row 4: CHRONIC_BLOCKED — ESCALATE / BLOCKING_LOOP
    {
        "row_id": "ITEM-005", "category": "BLOCKED",
        "consensus_type": "ALL_REJECT", "priority_score": 75,
        "is_overdue": True, "days_overdue": 45, "has_deadline": True,
        "missing_approvers": [], "blocking_approvers": ["BET"],
        "total_assigned": 3, "replied": 3, "pending": 0,
        "relevant_approvers": 3, "days_since_diffusion": 50,
        "source_sheet": "LOT_01", "document": "DOC-E",
        "assigned_approvers": ["BET", "CES", "OPC"],
    },
    # Row 5: NOT_STARTED — HOLD / NOT_YET_STARTED
    {
        "row_id": "ITEM-006", "category": "NOT_STARTED",
        "consensus_type": "NOT_STARTED", "priority_score": 30,
        "is_overdue": False, "days_overdue": 0, "has_deadline": False,
        "missing_approvers": ["BET", "CES", "OPC"], "blocking_approvers": [],
        "total_assigned": 3, "replied": 0, "pending": 3,
        "relevant_approvers": 3, "days_since_diffusion": 3,
        "source_sheet": "LOT_02", "document": "DOC-F",
        "assigned_approvers": ["BET", "CES", "OPC"],
    },
    # Row 6: NOT_STARTED + 10 days — HOLD but relance T5
    {
        "row_id": "ITEM-007", "category": "NOT_STARTED",
        "consensus_type": "NOT_STARTED", "priority_score": 35,
        "is_overdue": False, "days_overdue": 0, "has_deadline": False,
        "missing_approvers": ["BET", "CES"], "blocking_approvers": [],
        "total_assigned": 2, "replied": 0, "pending": 2,
        "relevant_approvers": 2, "days_since_diffusion": 10,
        "source_sheet": "LOT_03", "document": "DOC-G",
        "assigned_approvers": ["BET", "CES"],
    },
    # Row 7: WAITING_RESPONSES + not overdue — CHASE / MISSING_RESPONSES
    {
        "row_id": "ITEM-008", "category": "WAITING",
        "consensus_type": "INCOMPLETE", "priority_score": 50,
        "is_overdue": False, "days_overdue": 0, "has_deadline": True,
        "missing_approvers": ["OPC"], "blocking_approvers": [],
        "total_assigned": 3, "replied": 2, "pending": 1,
        "relevant_approvers": 3, "days_since_diffusion": 8,
        "source_sheet": "LOT_01", "document": "DOC-H",
        "assigned_approvers": ["BET", "CES", "OPC"],
    },
]

# ---------------------------------------------------------------------------
# 2. Synthetic M4 Results (matching M3 rows)
# ---------------------------------------------------------------------------

M4_RESULTS = [
    # ITEM-001: READY_TO_ISSUE
    {
        "row_id": "ITEM-001", "lifecycle_state": "READY_TO_ISSUE",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "FULL_APPROVAL", "approve_count": 4,
                       "reject_count": 0, "pending_count": 0, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 0, "worst_urgency": None, "block_status": "OK"},
        "blocking": {"is_blocked": False, "blocking_pattern": "NOT_BLOCKED",
                      "consecutive_rejections": None, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 10, "deadline_status": "APPROACHING",
                  "block_status": "OK"},
    },
    # ITEM-002: WAITING_RESPONSES
    {
        "row_id": "ITEM-002", "lifecycle_state": "WAITING_RESPONSES",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "PARTIAL_APPROVAL", "approve_count": 2,
                       "reject_count": 0, "pending_count": 2, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 2, "worst_urgency": "HIGH", "block_status": "OK"},
        "blocking": {"is_blocked": False, "blocking_pattern": "NOT_BLOCKED",
                      "consecutive_rejections": None, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 25, "deadline_status": "OVERDUE",
                  "block_status": "OK"},
    },
    # ITEM-003: READY_TO_REJECT
    {
        "row_id": "ITEM-003", "lifecycle_state": "READY_TO_REJECT",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "FULL_REJECTION", "approve_count": 0,
                       "reject_count": 3, "pending_count": 0, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 0, "worst_urgency": None, "block_status": "OK"},
        "blocking": {"is_blocked": True, "blocking_pattern": "FIRST_REJECTION",
                      "consecutive_rejections": 1, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 15, "deadline_status": "COMFORTABLE",
                  "block_status": "OK"},
    },
    # ITEM-004: NEEDS_ARBITRATION
    {
        "row_id": "ITEM-004", "lifecycle_state": "NEEDS_ARBITRATION",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "CONFLICT", "approve_count": 2,
                       "reject_count": 2, "pending_count": 0, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": True, "conflict_severity": "MEDIUM",
                      "block_status": "OK"},
        "missing": {"total_missing": 0, "worst_urgency": None, "block_status": "OK"},
        "blocking": {"is_blocked": True, "blocking_pattern": "PARTIAL_BLOCK",
                      "consecutive_rejections": None, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 12, "deadline_status": "APPROACHING",
                  "block_status": "OK"},
    },
    # ITEM-005: CHRONIC_BLOCKED
    {
        "row_id": "ITEM-005", "lifecycle_state": "CHRONIC_BLOCKED",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "FULL_REJECTION", "approve_count": 0,
                       "reject_count": 3, "pending_count": 0, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 0, "worst_urgency": None, "block_status": "OK"},
        "blocking": {"is_blocked": True, "blocking_pattern": "CHRONIC_BLOCK",
                      "consecutive_rejections": 3, "block_status": "OK"},
        "delta": {"has_previous": True, "block_status": "OK"},
        "time": {"days_since_diffusion": 50, "deadline_status": "SEVERELY_OVERDUE",
                  "block_status": "OK"},
    },
    # ITEM-006: NOT_STARTED (< 7 days)
    {
        "row_id": "ITEM-006", "lifecycle_state": "NOT_STARTED",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "NO_DATA", "approve_count": 0,
                       "reject_count": 0, "pending_count": 0, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 3, "worst_urgency": "LOW", "block_status": "OK"},
        "blocking": {"is_blocked": False, "blocking_pattern": "NOT_BLOCKED",
                      "consecutive_rejections": None, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 3, "deadline_status": "NO_DEADLINE",
                  "block_status": "OK"},
    },
    # ITEM-007: NOT_STARTED (>= 7 days → T5 relance)
    {
        "row_id": "ITEM-007", "lifecycle_state": "NOT_STARTED",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "NO_DATA", "approve_count": 0,
                       "reject_count": 0, "pending_count": 0, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 2, "worst_urgency": "LOW", "block_status": "OK"},
        "blocking": {"is_blocked": False, "blocking_pattern": "NOT_BLOCKED",
                      "consecutive_rejections": None, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 10, "deadline_status": "NO_DEADLINE",
                  "block_status": "OK"},
    },
    # ITEM-008: WAITING_RESPONSES (not overdue)
    {
        "row_id": "ITEM-008", "lifecycle_state": "WAITING_RESPONSES",
        "analysis_degraded": False, "failed_blocks": [],
        "agreement": {"agreement_type": "PARTIAL_APPROVAL", "approve_count": 2,
                       "reject_count": 0, "pending_count": 1, "hm_count": 0,
                       "non_classifiable_count": 0, "block_status": "OK"},
        "conflict": {"conflict_detected": False, "block_status": "OK"},
        "missing": {"total_missing": 1, "worst_urgency": "LOW", "block_status": "OK"},
        "blocking": {"is_blocked": False, "blocking_pattern": "NOT_BLOCKED",
                      "consecutive_rejections": None, "block_status": "OK"},
        "delta": {"has_previous": False, "block_status": "OK"},
        "time": {"days_since_diffusion": 8, "deadline_status": "COMFORTABLE",
                  "block_status": "OK"},
    },
]

# ---------------------------------------------------------------------------
# 3. Synthetic G1 Report (systemic blocker data)
# ---------------------------------------------------------------------------

G1_DATA = [
    {"approver_key": "BET", "is_systemic_blocker": True, "total_blocking": 7,
     "blocked_families": ["FAM1", "FAM2"], "severity": "HIGH"},
    {"approver_key": "CES", "is_systemic_blocker": False, "total_blocking": 1,
     "blocked_families": [], "severity": "LOW"},
    {"approver_key": "OPC", "is_systemic_blocker": False, "total_blocking": 0,
     "blocked_families": [], "severity": None},
]


# ---------------------------------------------------------------------------
# 4. Expected Results (action / visa / relance for each item)
# ---------------------------------------------------------------------------

EXPECTED = {
    "ITEM-001": {"action": "ISSUE_VISA",      "visa": "APPROVE", "relance": False},
    "ITEM-002": {"action": "CHASE_APPROVERS",  "visa": "WAIT",    "relance": True, "template": "T2"},
    "ITEM-003": {"action": "ISSUE_VISA",      "visa": "REJECT",  "relance": False},
    "ITEM-004": {"action": "ARBITRATE",       "visa": "NONE",    "relance": True, "template": "T4"},
    "ITEM-005": {"action": "ESCALATE",        "visa": "NONE",    "relance": True, "template": "T3"},
    "ITEM-006": {"action": "HOLD",            "visa": "NONE",    "relance": False},
    "ITEM-007": {"action": "HOLD",            "visa": "NONE",    "relance": True, "template": "T5"},
    "ITEM-008": {"action": "CHASE_APPROVERS",  "visa": "WAIT",    "relance": True, "template": "T1"},
}


# ---------------------------------------------------------------------------
# 5. Run Test
# ---------------------------------------------------------------------------

def main() -> int:
    from jansa_visasist.pipeline.module5.runner import run_module5

    m3_queue = pd.DataFrame(M3_ROWS)
    g1_report = pd.DataFrame(G1_DATA)
    pipeline_run_id = "TEST-RUN-001"

    print("=" * 70)
    print("MODULE 5 — SMOKE TEST")
    print("=" * 70)

    # --- Run M5 ---
    results, s1, s2, s3 = run_module5(m3_queue, M4_RESULTS, g1_report, pipeline_run_id)

    print(f"\nResults count: {len(results)} (expected {len(M3_ROWS)})")

    # --- Validate per-item results ---
    failures = 0
    for sr in results:
        rid = sr["row_id"]
        exp = EXPECTED.get(rid)
        if exp is None:
            print(f"  [?] {rid}: unexpected row_id in results")
            failures += 1
            continue

        ok = True

        # Check action
        if sr["suggested_action"] != exp["action"]:
            print(f"  [FAIL] {rid}: action={sr['suggested_action']} expected={exp['action']}")
            ok = False

        # Check visa
        if sr["proposed_visa"] != exp["visa"]:
            print(f"  [FAIL] {rid}: visa={sr['proposed_visa']} expected={exp['visa']}")
            ok = False

        # Check relance
        if sr["relance_required"] != exp["relance"]:
            print(f"  [FAIL] {rid}: relance={sr['relance_required']} expected={exp['relance']}")
            ok = False

        # Check template if relance expected
        if exp["relance"] and "template" in exp:
            if sr["relance_template_id"] != exp["template"]:
                print(f"  [FAIL] {rid}: template={sr['relance_template_id']} expected={exp['template']}")
                ok = False

        # Check mandatory fields never null
        for field in ["suggested_action", "proposed_visa", "confidence",
                       "reason_code", "escalation_level", "pipeline_run_id"]:
            if sr.get(field) is None:
                print(f"  [FAIL] {rid}: {field} is None (must never be null)")
                ok = False

        # Check confidence range
        conf = sr["confidence"]
        if not (0.0 <= conf <= 1.0):
            print(f"  [FAIL] {rid}: confidence={conf} out of [0,1] range")
            ok = False

        # Check action_priority range
        ap = sr["action_priority"]
        if not (0 <= ap <= 100):
            print(f"  [FAIL] {rid}: action_priority={ap} out of [0,100] range")
            ok = False

        # Check reason_details has exactly 7 keys
        rd = sr.get("reason_details", {})
        if len(rd) != 7:
            print(f"  [FAIL] {rid}: reason_details has {len(rd)} keys (expected 7)")
            ok = False

        # Check lists are sorted
        for listfield in ["blocking_approvers", "missing_approvers", "relance_targets"]:
            lst = sr.get(listfield, [])
            if lst != sorted(lst):
                print(f"  [FAIL] {rid}: {listfield} not sorted: {lst}")
                ok = False

        if ok:
            print(f"  [OK]   {rid}: action={sr['suggested_action']:20s} visa={sr['proposed_visa']:8s} "
                  f"conf={conf:.4f} prio={ap:3d} "
                  f"esc={sr['escalation_level']:10s} relance={str(sr['relance_required']):5s} "
                  f"tmpl={sr['relance_template_id'] or '-':3s}")
        else:
            failures += 1

    # --- Idempotency check [PATCH 6] ---
    print(f"\n--- Idempotency Check ---")
    results2, _, _, _ = run_module5(m3_queue, M4_RESULTS, g1_report, pipeline_run_id)
    json1 = json.dumps([r for r in results], sort_keys=True, ensure_ascii=False)
    json2 = json.dumps([r for r in results2], sort_keys=True, ensure_ascii=False)
    if json1 == json2:
        print("  [OK]   Bit-level identical on second run")
    else:
        print("  [FAIL] Outputs differ between runs!")
        failures += 1

    # --- Report checks ---
    print(f"\n--- Reports ---")
    print(f"  S1 (Action Distribution): {len(s1)} rows, columns={list(s1.columns)}")
    print(f"  S2 (VISA Recommendation): {len(s2)} rows, columns={list(s2.columns)}")
    print(f"  S3 (Relance):             {len(s3)} rows, columns={list(s3.columns)}")

    if len(s1) == 0:
        print("  [FAIL] S1 is empty")
        failures += 1
    if len(s2) == 0:
        print("  [FAIL] S2 is empty")
        failures += 1

    # S3 should only have relance_required=true items
    relance_count = sum(1 for sr in results if sr["relance_required"])
    if len(s3) != relance_count:
        print(f"  [FAIL] S3 has {len(s3)} rows but {relance_count} items have relance=true")
        failures += 1
    else:
        print(f"  [OK]   S3 row count matches relance items ({relance_count})")

    # --- Summary ---
    print(f"\n{'=' * 70}")
    if failures == 0:
        print("ALL CHECKS PASSED")
    else:
        print(f"{failures} CHECK(S) FAILED")
    print(f"{'=' * 70}")

    return 1 if failures > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
