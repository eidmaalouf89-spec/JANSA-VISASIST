"""
Module 6 — Index builders.

Builds queue_index, doc_index (list!), lot_index, and 3 approver indexes
from M3 priority queue data.
"""

import logging
import re
import unicodedata
from collections import defaultdict
from typing import Any, Dict, List

logger = logging.getLogger("jansa.m6.indexes")


def _normalize_doc(doc: str) -> str:
    """Normalize a document reference for indexing.

    Lowercase, strip whitespace, remove spaces/hyphens but preserve underscores.
    NFKD accent removal.
    """
    if not doc:
        return ""
    doc = doc.strip().lower()
    doc = unicodedata.normalize("NFKD", doc)
    doc = "".join(c for c in doc if not unicodedata.combining(c))
    # Remove spaces and hyphens for matching, keep underscores
    doc = re.sub(r"[\s\-]+", "", doc)
    return doc


def build_queue_index(queue_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build row_id -> row dict index."""
    index = {}
    for item in queue_data:
        row_id = item.get("row_id")
        if row_id:
            index[str(row_id)] = item
    logger.info("Built queue_index with %d items", len(index))
    return index


def build_doc_index(queue_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Build normalized_document -> list[row_ids] index.

    Cross-lot disambiguation: same document name in different lots
    produces multiple row_ids under the same key.
    """
    index: Dict[str, List[str]] = defaultdict(list)
    for item in queue_data:
        doc = item.get("document")
        row_id = item.get("row_id")
        if doc and row_id:
            norm = _normalize_doc(doc)
            if norm:
                index[norm].append(str(row_id))

    result = dict(index)
    multi = sum(1 for v in result.values() if len(v) > 1)
    logger.info("Built doc_index with %d entries (%d multi-match)", len(result), multi)
    return result


def build_lot_index(queue_data: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Build source_sheet -> sorted list of row_ids (sorted by priority_score desc)."""
    index: Dict[str, List[Dict]] = defaultdict(list)
    for item in queue_data:
        ss = item.get("source_sheet")
        row_id = item.get("row_id")
        if ss and row_id:
            index[ss].append(item)

    result = {}
    for sheet, items in index.items():
        sorted_items = sorted(items, key=lambda x: x.get("priority_score", 0), reverse=True)
        result[sheet] = [str(it["row_id"]) for it in sorted_items]

    logger.info("Built lot_index with %d lots", len(result))
    return result


def build_approver_indexes(
    queue_data: List[Dict[str, Any]],
) -> tuple:
    """Build three approver indexes from queue data.

    Returns:
        (approver_missing_index, approver_blocking_index, approver_assigned_index)
        Each is Dict[str, List[str]] mapping canonical approver key -> list of row_ids.
    """
    missing_idx: Dict[str, List[str]] = defaultdict(list)
    blocking_idx: Dict[str, List[str]] = defaultdict(list)
    assigned_idx: Dict[str, List[str]] = defaultdict(list)

    for item in queue_data:
        row_id = str(item.get("row_id", ""))
        if not row_id:
            continue

        for approver in (item.get("missing_approvers") or []):
            missing_idx[approver].append(row_id)

        for approver in (item.get("blocking_approvers") or []):
            blocking_idx[approver].append(row_id)

        for approver in (item.get("assigned_approvers") or []):
            assigned_idx[approver].append(row_id)

    logger.info(
        "Built approver indexes: missing=%d, blocking=%d, assigned=%d approvers",
        len(missing_idx), len(blocking_idx), len(assigned_idx),
    )
    return dict(missing_idx), dict(blocking_idx), dict(assigned_idx)
