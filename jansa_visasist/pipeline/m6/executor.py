"""
Module 6 — C1-C12 Execution Functions.

Each command is a pure function operating on the Module6Context.
All functions return (results: List[Dict], result_count: int, extra: Dict).
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from jansa_visasist.config import CANONICAL_APPROVERS
from jansa_visasist.config_m6 import (
    FIELDS_C6_M3, FIELDS_C6_M4, FIELDS_C6_M5, FIELDS_C6_DISAMBIGUATION,
    FIELDS_C7, FIELDS_C8, FIELDS_C9, FIELDS_FILTER_ROW,
    WARNING_AMBIGUOUS_DOC, WARNING_M4_UNAVAILABLE, WARNING_M5_UNAVAILABLE,
)
from jansa_visasist.context_m6 import Module6Context
from jansa_visasist.pipeline.m6.indexes import _normalize_doc

logger = logging.getLogger("jansa.m6.executor")


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def _pick_fields(item: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    """Extract specified fields from a dict."""
    return {k: item.get(k) for k in fields}


def _filter_by_lot(ctx: Module6Context, lot: str) -> List[Dict[str, Any]]:
    """Get items for a given lot (source_sheet), sorted by priority_score desc."""
    row_ids = ctx.lot_index.get(lot, [])
    return [ctx.queue_index[rid] for rid in row_ids if rid in ctx.queue_index]


def _apply_filters(
    items: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Apply category and status filters to a list of items."""
    result = items

    if "category" in params:
        cat = params["category"]
        result = [it for it in result if it.get("category") == cat]

    if "status_filter" in params:
        sf = params["status_filter"]
        filter_field = sf["filter"]
        if "operator" in sf:
            op = sf["operator"]
            val = sf["value"]
            if op == ">=":
                result = [it for it in result if (it.get(filter_field, 0) or 0) >= val]
            elif op == "<=":
                result = [it for it in result if (it.get(filter_field, 0) or 0) <= val]
        else:
            val = sf["value"]
            result = [it for it in result if it.get(filter_field) == val]

    return result


def _sort_by_priority(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Sort items by priority_score descending."""
    return sorted(items, key=lambda x: x.get("priority_score", 0), reverse=True)


# ──────────────────────────────────────────────
# C1: Filter by lot
# ──────────────────────────────────────────────

def execute_c1(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C1: List pending items for a lot."""
    lot = params.get("lot", "")
    items = _filter_by_lot(ctx, lot)
    items = _sort_by_priority(items)
    result = [_pick_fields(it, FIELDS_FILTER_ROW) for it in items]
    return result, len(result), {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C2: Filter by category
# ──────────────────────────────────────────────

def execute_c2(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C2: List items matching a category."""
    cat = params.get("category", "")
    items = [it for it in ctx.queue_data if it.get("category") == cat]
    items = _sort_by_priority(items)
    result = [_pick_fields(it, FIELDS_FILTER_ROW) for it in items]
    return result, len(result), {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C3: Filter by approver
# ──────────────────────────────────────────────

def execute_c3(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C3: Items where approver is missing or blocking."""
    approver = params.get("approver", "")

    # Combine missing + blocking row_ids (union)
    missing_ids = set(ctx.approver_missing_index.get(approver, []))
    blocking_ids = set(ctx.approver_blocking_index.get(approver, []))
    all_ids = missing_ids | blocking_ids

    items = [ctx.queue_index[rid] for rid in all_ids if rid in ctx.queue_index]
    items = _sort_by_priority(items)
    result = [_pick_fields(it, FIELDS_FILTER_ROW) for it in items]
    return result, len(result), {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C4: Filter by overdue
# ──────────────────────────────────────────────

def execute_c4(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C4: List overdue items."""
    items = [it for it in ctx.queue_data if it.get("is_overdue") is True]
    items = _sort_by_priority(items)
    result = [_pick_fields(it, FIELDS_FILTER_ROW) for it in items]
    return result, len(result), {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C5: Combined filter
# ──────────────────────────────────────────────

def execute_c5(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C5: Multiple filters (lot + category, lot + status, etc.)."""
    lot = params.get("lot")
    if lot:
        items = _filter_by_lot(ctx, lot)
    else:
        items = list(ctx.queue_data)

    # Apply approver filter if present
    if "approver" in params:
        approver = params["approver"]
        missing_ids = set(ctx.approver_missing_index.get(approver, []))
        blocking_ids = set(ctx.approver_blocking_index.get(approver, []))
        all_ids = missing_ids | blocking_ids
        items = [it for it in items if str(it.get("row_id")) in all_ids]

    items = _apply_filters(items, params)
    items = _sort_by_priority(items)
    result = [_pick_fields(it, FIELDS_FILTER_ROW) for it in items]
    return result, len(result), {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C6: Document lookup
# ──────────────────────────────────────────────

def execute_c6(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C6: Full detail for a document.

    THREE outcomes:
    - 0 matches: not found
    - 1 match: enriched with M4/M5
    - 2+ matches: disambiguation list
    """
    doc_ref = params.get("document", "")
    norm_doc = _normalize_doc(doc_ref) if doc_ref else ""

    row_ids = ctx.doc_index.get(norm_doc, [])
    warnings: List[str] = []
    sources = ["M3"]

    # 0 matches: not found
    if not row_ids:
        return [], 0, {"sources_used": sources, "warnings": warnings, "disambiguation": False}

    # 2+ matches: disambiguation
    if len(row_ids) > 1:
        items = [ctx.queue_index[rid] for rid in row_ids if rid in ctx.queue_index]
        sheets = list(set(it.get("source_sheet", "") for it in items))
        warnings.append(WARNING_AMBIGUOUS_DOC.format(n=len(row_ids), sheets=", ".join(sorted(sheets))))
        result = [_pick_fields(it, FIELDS_C6_DISAMBIGUATION) for it in items]
        return result, len(result), {
            "sources_used": sources, "warnings": warnings, "disambiguation": True,
        }

    # 1 match: enriched
    row_id = row_ids[0]
    item = ctx.queue_index.get(row_id, {})
    result_item = _pick_fields(item, FIELDS_C6_M3)

    # Enrich with M4
    if row_id in ctx.m4_data:
        m4 = ctx.m4_data[row_id]
        for f in FIELDS_C6_M4:
            result_item[f] = m4.get(f)
        sources.append("M4")
    else:
        warnings.append(WARNING_M4_UNAVAILABLE)

    # Enrich with M5
    if row_id in ctx.m5_data:
        m5 = ctx.m5_data[row_id]
        for f in FIELDS_C6_M5:
            result_item[f] = m5.get(f)
        sources.append("M5")
    else:
        warnings.append(WARNING_M5_UNAVAILABLE)

    return [result_item], 1, {
        "sources_used": sources, "warnings": warnings, "disambiguation": False,
    }


# ──────────────────────────────────────────────
# C7: Explain priority
# ──────────────────────────────────────────────

def execute_c7(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C7: Why a document has its score/category."""
    doc_ref = params.get("document", "")
    norm_doc = _normalize_doc(doc_ref) if doc_ref else ""

    row_ids = ctx.doc_index.get(norm_doc, [])
    warnings: List[str] = []
    sources = ["M3"]

    # 0 matches
    if not row_ids:
        return [], 0, {"sources_used": sources, "warnings": warnings, "disambiguation": False}

    # 2+ matches: disambiguation
    if len(row_ids) > 1:
        items = [ctx.queue_index[rid] for rid in row_ids if rid in ctx.queue_index]
        sheets = list(set(it.get("source_sheet", "") for it in items))
        warnings.append(WARNING_AMBIGUOUS_DOC.format(n=len(row_ids), sheets=", ".join(sorted(sheets))))
        result = [_pick_fields(it, FIELDS_C6_DISAMBIGUATION) for it in items]
        return result, len(result), {
            "sources_used": sources, "warnings": warnings, "disambiguation": True,
        }

    # 1 match
    row_id = row_ids[0]
    item = ctx.queue_index.get(row_id, {})
    result_item = _pick_fields(item, FIELDS_C7)

    # C7 uses M4 for explanation
    if row_id in ctx.m4_data:
        sources.append("M4")
    else:
        warnings.append(WARNING_M4_UNAVAILABLE)

    return [result_item], 1, {
        "sources_used": sources, "warnings": warnings, "disambiguation": False,
    }


# ──────────────────────────────────────────────
# C8: Lot summary
# ──────────────────────────────────────────────

def execute_c8(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C8: Summary stats for a lot."""
    lot = params.get("lot", "")
    items = _filter_by_lot(ctx, lot)

    if not items:
        summary = {
            "source_sheet": lot,
            "total_items": 0,
            "categories": {},
            "overdue_count": 0,
            "avg_priority": 0.0,
            "max_priority": 0.0,
            "avg_days_overdue": 0.0,
        }
        return [summary], 1, {"sources_used": ["M3"]}

    # Compute summary stats
    categories = {}
    overdue_count = 0
    total_priority = 0.0
    max_priority = 0.0
    total_days_overdue = 0
    overdue_items = 0

    for it in items:
        cat = it.get("category", "UNKNOWN")
        categories[cat] = categories.get(cat, 0) + 1
        if it.get("is_overdue"):
            overdue_count += 1
            overdue_items += 1
            total_days_overdue += (it.get("days_overdue", 0) or 0)
        ps = it.get("priority_score", 0) or 0
        total_priority += ps
        if ps > max_priority:
            max_priority = ps

    summary = {
        "source_sheet": lot,
        "total_items": len(items),
        "categories": categories,
        "overdue_count": overdue_count,
        "avg_priority": round(total_priority / len(items), 2),
        "max_priority": max_priority,
        "avg_days_overdue": round(total_days_overdue / max(overdue_items, 1), 2),
    }

    return [summary], 1, {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C9: Approver summary
# ──────────────────────────────────────────────

def execute_c9(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C9: Summary stats for an approver.

    Uses all 3 approver indexes for assigned/replied/missing/blocking counts.
    """
    approver = params.get("approver", "")

    assigned_ids = set(ctx.approver_assigned_index.get(approver, []))
    missing_ids = set(ctx.approver_missing_index.get(approver, []))
    blocking_ids = set(ctx.approver_blocking_index.get(approver, []))

    # Compute replied: assigned items where {APPROVER}_statut is not null
    statut_col = f"{approver}_statut"
    replied_count = 0
    affected_lots = set()
    total_priority = 0.0

    for rid in assigned_ids:
        item = ctx.queue_index.get(rid, {})
        if item.get(statut_col) is not None:
            replied_count += 1
        ss = item.get("source_sheet")
        if ss:
            affected_lots.add(ss)
        total_priority += (item.get("priority_score", 0) or 0)

    summary = {
        "approver": approver,
        "total_assigned": len(assigned_ids),
        "total_replied": replied_count,
        "total_missing": len(missing_ids),
        "total_blocking": len(blocking_ids),
        "affected_lots": sorted(affected_lots),
        "avg_priority": round(total_priority / max(len(assigned_ids), 1), 2),
    }

    return [summary], 1, {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C10: Count
# ──────────────────────────────────────────────

def execute_c10(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C10: Count items matching criteria. Returns count only, results=[]."""
    # Apply same filtering as C5
    lot = params.get("lot")
    if lot:
        items = _filter_by_lot(ctx, lot)
    else:
        items = list(ctx.queue_data)

    if "approver" in params:
        approver = params["approver"]
        missing_ids = set(ctx.approver_missing_index.get(approver, []))
        blocking_ids = set(ctx.approver_blocking_index.get(approver, []))
        all_ids = missing_ids | blocking_ids
        items = [it for it in items if str(it.get("row_id")) in all_ids]

    items = _apply_filters(items, params)

    # C10: results=[], just the count
    return [], len(items), {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C11: Top N
# ──────────────────────────────────────────────

def execute_c11(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C11: Top N items by priority, with filters. C5 filters first, then slice [:n]."""
    n = params.get("n", 10)

    # Apply C5-style filtering
    lot = params.get("lot")
    if lot:
        items = _filter_by_lot(ctx, lot)
    else:
        items = list(ctx.queue_data)

    if "approver" in params:
        approver = params["approver"]
        missing_ids = set(ctx.approver_missing_index.get(approver, []))
        blocking_ids = set(ctx.approver_blocking_index.get(approver, []))
        all_ids = missing_ids | blocking_ids
        items = [it for it in items if str(it.get("row_id")) in all_ids]

    items = _apply_filters(items, params)
    items = _sort_by_priority(items)

    # Slice to top N
    total = len(items)
    items = items[:n]

    result = [_pick_fields(it, FIELDS_FILTER_ROW) for it in items]
    return result, total, {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# C12: Export (delegates to exporter for file writing)
# ──────────────────────────────────────────────

def execute_c12(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """C12: CSV export. Returns results=[], export_metadata populated.

    The actual file writing is done by the exporter module.
    This function returns the filtered data for the exporter.
    """
    # Apply C5-style filtering
    lot = params.get("lot")
    if lot:
        items = _filter_by_lot(ctx, lot)
    else:
        items = list(ctx.queue_data)

    if "approver" in params:
        approver = params["approver"]
        missing_ids = set(ctx.approver_missing_index.get(approver, []))
        blocking_ids = set(ctx.approver_blocking_index.get(approver, []))
        all_ids = missing_ids | blocking_ids
        items = [it for it in items if str(it.get("row_id")) in all_ids]

    items = _apply_filters(items, params)
    items = _sort_by_priority(items)

    # C12: results=[], file IS the result
    return items, len(items), {"sources_used": ["M3"], "raw_items": items}


# ──────────────────────────────────────────────
# REJECTED
# ──────────────────────────────────────────────

def execute_rejected(ctx: Module6Context, params: Dict[str, Any]) -> Tuple[List[Dict], int, Dict]:
    """REJECTED: Query could not be resolved."""
    return [], 0, {"sources_used": ["M3"]}


# ──────────────────────────────────────────────
# Dispatcher
# ──────────────────────────────────────────────

COMMAND_EXECUTORS = {
    "C1": execute_c1,
    "C2": execute_c2,
    "C3": execute_c3,
    "C4": execute_c4,
    "C5": execute_c5,
    "C6": execute_c6,
    "C7": execute_c7,
    "C8": execute_c8,
    "C9": execute_c9,
    "C10": execute_c10,
    "C11": execute_c11,
    "C12": execute_c12,
    "REJECTED": execute_rejected,
}


def execute_command(
    ctx: Module6Context,
    command_id: str,
    params: Dict[str, Any],
) -> Tuple[List[Dict], int, Dict]:
    """Dispatch to the appropriate command executor."""
    executor = COMMAND_EXECUTORS.get(command_id, execute_rejected)
    return executor(ctx, params)
