"""
Module 5 — Report Generation (S1, S2, S3).

[Plan §Phase 3] Aggregate per-item suggestion_results into three global DataFrames.
All report schemas are contract-locked.

S1: Action Distribution Report — GROUP BY (suggested_action, source_sheet, priority_band)
S2: VISA Recommendation Report — GROUP BY (proposed_visa, source_sheet)
S3: Communication / Relance Report — FILTER WHERE relance_required=true
"""

import logging
from collections import defaultdict
from typing import Any

import pandas as pd

from .priority import get_priority_band
from .schemas import S1_COLUMNS, S2_COLUMNS, S3_COLUMNS

logger = logging.getLogger(__name__)


# ============================================================================
# S1 — Action Distribution Report [Plan §Phase 3]
# ============================================================================

def build_report_s1(suggestion_results: list[dict[str, Any]]) -> pd.DataFrame:
    """Build S1 action distribution report.

    [Plan §Phase 3] GROUP BY (suggested_action, source_sheet, priority_band).
    Single pass over suggestion_results.
    Output sorted by (suggested_action ASC, source_sheet ASC,
    priority_band DESC — CRITICAL > HIGH > MEDIUM > LOW).

    Args:
        suggestion_results: List of SuggestionResult dicts.

    Returns:
        S1 DataFrame with columns: suggested_action, source_sheet, priority_band,
        item_count, avg_confidence, avg_action_priority, escalated_count.
    """
    try:
        if not suggestion_results:
            return _empty_s1()

        # Single-pass aggregation
        groups: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
            lambda: {
                "confidences": [],
                "priorities": [],
                "escalated": 0,
            }
        )

        for sr in suggestion_results:
            action = sr.get("suggested_action", "HOLD")
            source = sr.get("source_sheet", "LOT_UNKNOWN")
            priority_band = get_priority_band(sr.get("action_priority", 0))

            key = (action, source, priority_band)
            groups[key]["confidences"].append(sr.get("confidence", 0.0))
            groups[key]["priorities"].append(sr.get("action_priority", 0))
            if sr.get("escalation_level", "NONE") != "NONE":
                groups[key]["escalated"] += 1

        # Build rows
        rows: list[dict[str, Any]] = []
        for (action, source, band), data in groups.items():
            confs = data["confidences"]
            prios = data["priorities"]
            rows.append({
                "suggested_action": action,
                "source_sheet": source,
                "priority_band": band,
                "item_count": len(confs),
                "avg_confidence": round(sum(confs) / len(confs), 4) if confs else 0.0,
                "avg_action_priority": round(sum(prios) / len(prios), 2) if prios else 0.0,
                "escalated_count": data["escalated"],
            })

        df = pd.DataFrame(rows, columns=S1_COLUMNS)

        # Sort: suggested_action ASC, source_sheet ASC, priority_band DESC
        band_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        df["_band_sort"] = df["priority_band"].map(band_order).fillna(4)
        df = df.sort_values(
            by=["suggested_action", "source_sheet", "_band_sort"],
            ascending=[True, True, True],
        ).drop(columns=["_band_sort"]).reset_index(drop=True)

        return df

    except Exception:
        logger.error("M5: S1 report generation failed.", exc_info=True)
        return _empty_s1()


def _empty_s1() -> pd.DataFrame:
    """Return empty S1 DataFrame with correct schema."""
    return pd.DataFrame(columns=S1_COLUMNS)


# ============================================================================
# S2 — VISA Recommendation Report [Plan §Phase 3]
# ============================================================================

def build_report_s2(suggestion_results: list[dict[str, Any]]) -> pd.DataFrame:
    """Build S2 VISA recommendation report.

    [Plan §Phase 3] GROUP BY (proposed_visa, source_sheet).
    Single pass. Output sorted by (source_sheet ASC, proposed_visa ASC).

    Args:
        suggestion_results: List of SuggestionResult dicts.

    Returns:
        S2 DataFrame with columns: proposed_visa, source_sheet,
        item_count, avg_confidence, pct_of_lot.
    """
    try:
        if not suggestion_results:
            return _empty_s2()

        # Count items per lot for pct_of_lot computation
        lot_counts: dict[str, int] = defaultdict(int)
        for sr in suggestion_results:
            source = sr.get("source_sheet", "LOT_UNKNOWN")
            lot_counts[source] += 1

        # Single-pass aggregation
        groups: dict[tuple[str, str], list[float]] = defaultdict(list)
        for sr in suggestion_results:
            visa = sr.get("proposed_visa", "NONE")
            source = sr.get("source_sheet", "LOT_UNKNOWN")
            key = (visa, source)
            groups[key].append(sr.get("confidence", 0.0))

        # Build rows
        rows: list[dict[str, Any]] = []
        for (visa, source), confs in groups.items():
            item_count = len(confs)
            total_in_lot = lot_counts.get(source, 1)
            pct = round(item_count / max(total_in_lot, 1), 4)
            rows.append({
                "proposed_visa": visa,
                "source_sheet": source,
                "item_count": item_count,
                "avg_confidence": round(sum(confs) / len(confs), 4) if confs else 0.0,
                "pct_of_lot": pct,
            })

        df = pd.DataFrame(rows, columns=S2_COLUMNS)
        df = df.sort_values(
            by=["source_sheet", "proposed_visa"],
            ascending=[True, True],
        ).reset_index(drop=True)

        return df

    except Exception:
        logger.error("M5: S2 report generation failed.", exc_info=True)
        return _empty_s2()


def _empty_s2() -> pd.DataFrame:
    """Return empty S2 DataFrame with correct schema."""
    return pd.DataFrame(columns=S2_COLUMNS)


# ============================================================================
# S3 — Communication / Relance Report [Plan §Phase 3]
# ============================================================================

def build_report_s3(suggestion_results: list[dict[str, Any]]) -> pd.DataFrame:
    """Build S3 communication/relance report.

    [Plan §Phase 3] FILTER WHERE relance_required=true.
    Single pass. Output sorted by (action_priority DESC, row_id ASC).

    Args:
        suggestion_results: List of SuggestionResult dicts.

    Returns:
        S3 DataFrame with columns: row_id, document, source_sheet,
        suggested_action, relance_required, relance_targets,
        relance_template_id, relance_message, escalation_level, action_priority.
    """
    try:
        if not suggestion_results:
            return _empty_s3()

        rows: list[dict[str, Any]] = []
        for sr in suggestion_results:
            if not sr.get("relance_required", False):
                continue

            # relance_targets as comma-separated sorted string
            targets = sr.get("relance_targets", [])
            targets_str = ", ".join(sorted(targets)) if targets else ""

            rows.append({
                "row_id": sr.get("row_id", "?"),
                "document": sr.get("document", "DOC_UNKNOWN"),
                "source_sheet": sr.get("source_sheet", "LOT_UNKNOWN"),
                "suggested_action": sr.get("suggested_action", "HOLD"),
                "relance_required": True,
                "relance_targets": targets_str,
                "relance_template_id": sr.get("relance_template_id"),
                "relance_message": sr.get("relance_message"),
                "escalation_level": sr.get("escalation_level", "NONE"),
                "action_priority": sr.get("action_priority", 0),
            })

        if not rows:
            return _empty_s3()

        df = pd.DataFrame(rows, columns=S3_COLUMNS)
        df = df.sort_values(
            by=["action_priority", "row_id"],
            ascending=[False, True],
        ).reset_index(drop=True)

        return df

    except Exception:
        logger.error("M5: S3 report generation failed.", exc_info=True)
        return _empty_s3()


def _empty_s3() -> pd.DataFrame:
    """Return empty S3 DataFrame with correct schema."""
    return pd.DataFrame(columns=S3_COLUMNS)
