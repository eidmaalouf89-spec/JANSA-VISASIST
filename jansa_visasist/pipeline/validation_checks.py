"""
Step 10: Validation Checks
Row count, schema completeness, orphan rows, date sanity.
"""

import logging
from typing import Dict, List

import pandas as pd

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import CORE_COLUMNS, CANONICAL_APPROVERS, APPROVER_OUTPUT_SUFFIXES, DATE_SANITY_MIN, DATE_SANITY_MAX

logger = logging.getLogger(__name__)


def run_validation_checks(
    master_df: pd.DataFrame,
    ctx: PipelineContext,
) -> Dict[str, dict]:
    """
    Run all validation checks on the merged master dataset.
    Returns a dict of check_name -> {passed: bool, details: str}.
    """
    results: Dict[str, dict] = {}

    # Check 1: Row count consistency
    expected_total = sum(ctx.sheet_row_counts.values())
    actual_total = len(master_df)
    passed = expected_total == actual_total
    results["row_count"] = {
        "passed": passed,
        "details": f"Expected {expected_total} rows, got {actual_total}",
    }
    if not passed:
        ctx.log_sheet_level(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet="__validation__",
            row=None,
            column=None,
            severity=Severity.ERROR.value,
            category="row_count_mismatch",
            raw_value=None,
            action_taken=f"Row count mismatch: expected {expected_total}, got {actual_total}",
        ))

    # Check 2: Schema completeness
    required_cols = set(CORE_COLUMNS)
    for approver in CANONICAL_APPROVERS:
        for suffix in APPROVER_OUTPUT_SUFFIXES:
            required_cols.add(f"{approver}{suffix}")

    missing_cols = required_cols - set(master_df.columns)
    passed = len(missing_cols) == 0
    results["schema_completeness"] = {
        "passed": passed,
        "details": f"Missing columns: {sorted(missing_cols)}" if missing_cols else "All columns present",
    }

    # Check 3: No orphaned rows
    orphan_checks = ["source_sheet", "source_row", "row_id"]
    orphan_issues: List[str] = []
    for col in orphan_checks:
        if col in master_df.columns:
            null_count = master_df[col].isna().sum()
            if null_count > 0:
                orphan_issues.append(f"{col}: {null_count} nulls")

    passed = len(orphan_issues) == 0
    results["no_orphaned_rows"] = {
        "passed": passed,
        "details": "; ".join(orphan_issues) if orphan_issues else "All rows have traceability fields",
    }

    # Check 4: Date sanity
    date_columns = [c for c in master_df.columns if c.endswith("_date") or c in ["date_diffusion", "date_reception", "date_contractuelle_visa"]]
    # Exclude _raw columns
    date_columns = [c for c in date_columns if not c.endswith("_raw")]

    date_issues: List[str] = []
    for col in date_columns:
        if col not in master_df.columns:
            continue
        non_null = master_df[col].dropna()
        for idx, val in non_null.items():
            if isinstance(val, str):
                if val < DATE_SANITY_MIN or val > DATE_SANITY_MAX:
                    date_issues.append(f"Row {idx}, {col}: {val}")

    passed = len(date_issues) == 0
    results["date_sanity"] = {
        "passed": passed,
        "details": f"{len(date_issues)} dates outside [{DATE_SANITY_MIN}, {DATE_SANITY_MAX}]: {date_issues[:5]}" if date_issues else "All dates within sane range",
    }

    # Log summary
    total_checks = len(results)
    passed_checks = sum(1 for r in results.values() if r["passed"])
    logger.info("Validation: %d/%d checks passed", passed_checks, total_checks)

    return results
