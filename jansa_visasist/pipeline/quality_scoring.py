"""
Step 8: Row Quality Scoring
Derive row quality from accumulated log entries — single authority.
"""

from typing import List, Tuple

from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity, RowQuality


def score_row_quality(row_logs: List[ImportLogEntry]) -> Tuple[str, List[str]]:
    """
    Compute row quality from the accumulated log entries.
    Returns (row_quality, row_quality_details).

    Grade logic:
    - OK: zero WARNING, zero ERROR
    - WARNING: ≥1 WARNING, zero ERROR
    - ERROR: ≥1 ERROR
    """
    has_error = False
    has_warning = False
    details: List[str] = []

    for entry in row_logs:
        if entry.severity == Severity.ERROR.value:
            has_error = True
            if entry.category not in details:
                details.append(entry.category)
        elif entry.severity == Severity.WARNING.value:
            has_warning = True
            if entry.category not in details:
                details.append(entry.category)

    if has_error:
        return RowQuality.ERROR.value, details
    elif has_warning:
        return RowQuality.WARNING.value, details
    else:
        return RowQuality.OK.value, details
