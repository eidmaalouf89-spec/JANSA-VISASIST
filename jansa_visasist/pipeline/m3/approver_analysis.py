"""
Step 3: Approver Response Analysis
[SPEC] V2.2 §3 Step 3
"""

import json
import logging
from typing import List, Callable

import pandas as pd

from ...config_m3 import (
    CONSENSUS_VSO_VAO_STATUSES,
    CONSENSUS_REF_STATUSES,
    CONSENSUS_HM_STATUSES,
)

logger = logging.getLogger(__name__)


def _analyze_single_row(
    assigned_list: List[str],
    statut_getter: Callable,
) -> dict:
    """
    Analyze approver responses for one row.
    Returns dict with all spec-mandated counts/lists and internal fields.
    statut_getter(key) -> Optional[str] returns the statut for an approver key.
    """
    # Classify each assigned approver
    vso_list = []
    vao_list = []
    ref_list = []
    hm_list = []
    null_list = []       # all assigned with null statut
    non_driving_list = []  # replied but not in consensus buckets

    for key in assigned_list:
        statut = statut_getter(key)
        if statut is None or (isinstance(statut, float) and pd.isna(statut)):
            null_list.append(key)
        elif statut in CONSENSUS_VSO_VAO_STATUSES:
            if statut == "VSO":
                vso_list.append(key)
            else:
                vao_list.append(key)
        elif statut in CONSENSUS_REF_STATUSES:
            ref_list.append(key)
        elif statut in CONSENSUS_HM_STATUSES:
            hm_list.append(key)
        else:
            # Non-driving status (SUS, DEF, FAV, etc.)
            non_driving_list.append((key, statut))

    # Spec-mandated counts
    total_assigned = len(assigned_list)
    replied = total_assigned - len(null_list)
    pending = len(null_list)
    approvers_vso = len(vso_list)
    approvers_vao = len(vao_list)
    approvers_ref = len(ref_list)
    approvers_hm = len(hm_list)
    relevant_approvers = total_assigned - approvers_hm

    # Internal: relevant approvers list (assigned minus HM)
    hm_set = set(hm_list)
    relevant_list = [k for k in assigned_list if k not in hm_set]

    # missing_approvers = relevant with null statut
    missing_approvers = [k for k in relevant_list
                         if statut_getter(k) is None
                         or (isinstance(statut_getter(k), float) and pd.isna(statut_getter(k)))]

    # blocking_approvers = relevant with REF
    blocking_approvers = ref_list[:]

    # pending_among_relevant = count of relevant with null statut (for consensus)
    pending_among_relevant = len(missing_approvers)

    # replied_among_relevant = relevant who have replied (any non-null statut)
    replied_among_relevant = relevant_approvers - pending_among_relevant

    # Build compact summary string
    summary_parts = [f"{replied}/{total_assigned} replied"]
    if pending > 0:
        summary_parts.append(f"{pending} pending")
    else:
        summary_parts.append("0 pending")

    # Status detail
    detail_parts = []
    if approvers_ref > 0:
        detail_parts.append(f"{approvers_ref} REF")
    if approvers_vso > 0:
        detail_parts.append(f"{approvers_vso} VSO")
    if approvers_vao > 0:
        detail_parts.append(f"{approvers_vao} VAO")
    if approvers_hm > 0:
        detail_parts.append(f"{approvers_hm} HM")
    for key, st in non_driving_list:
        detail_parts.append(f"1 {st}")

    summary = ", ".join(summary_parts)
    if detail_parts:
        summary += " (" + ", ".join(detail_parts) + ")"

    return {
        # Spec-mandated output columns
        "total_assigned": total_assigned,
        "replied": replied,
        "pending": pending,
        "approvers_vso": approvers_vso,
        "approvers_vao": approvers_vao,
        "approvers_ref": approvers_ref,
        "approvers_hm": approvers_hm,
        "relevant_approvers": relevant_approvers,
        "missing_approvers": missing_approvers,
        "blocking_approvers": blocking_approvers,
        "approver_response_summary": summary,
        # Internal fields for consensus engine
        "_pending_among_relevant": pending_among_relevant,
        "_replied_among_relevant": replied_among_relevant,
        "_non_driving_list": non_driving_list,
    }


def add_approver_analysis(df: pd.DataFrame, ctx=None) -> pd.DataFrame:
    """
    Analyze approver responses for each row.
    Adds spec columns + internal columns.
    Logs WARNING for non-driving statuses.
    """
    if df.empty:
        for col in ["total_assigned", "replied", "pending",
                     "approvers_vso", "approvers_vao", "approvers_ref",
                     "approvers_hm", "relevant_approvers",
                     "missing_approvers", "blocking_approvers",
                     "approver_response_summary",
                     "_pending_among_relevant", "_replied_among_relevant"]:
            df[col] = 0 if col not in ("missing_approvers", "blocking_approvers",
                                        "approver_response_summary",
                                        "_pending_among_relevant",
                                        "_replied_among_relevant") else (
                [] if "approvers" in col and "summary" not in col else (
                    "" if "summary" in col else 0
                )
            )
        return df

    non_driving_warning_count = 0

    # Process each row
    results = []
    for idx in df.index:
        assigned_list = df.at[idx, "_assigned_list"]
        row = df.loc[idx]

        def statut_getter(key, _row=row):
            col = f"{key}_statut"
            val = _row.get(col)
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return None
            return str(val)

        analysis = _analyze_single_row(assigned_list, statut_getter)

        # Log warnings for non-driving statuses
        for key, st in analysis["_non_driving_list"]:
            logger.warning(
                "Approver %s has non-driving status '%s' — "
                "counted as replied but excluded from consensus buckets.",
                key, st,
            )
            non_driving_warning_count += 1

        results.append(analysis)

    if ctx is not None:
        ctx.non_driving_status_warnings = non_driving_warning_count

    # Assign columns from results
    for col in ["total_assigned", "replied", "pending",
                "approvers_vso", "approvers_vao", "approvers_ref",
                "approvers_hm", "relevant_approvers",
                "missing_approvers", "blocking_approvers",
                "approver_response_summary",
                "_pending_among_relevant", "_replied_among_relevant"]:
        df[col] = [r[col] for r in results]

    logger.info(
        "Step 3: Approver analysis complete. %d non-driving status warnings.",
        non_driving_warning_count,
    )
    return df
