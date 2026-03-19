"""
JANSA VISASIST — Module3Context
Centralized mutable state for the Module 3 pipeline.
"""

import datetime
from dataclasses import dataclass, field
from typing import List, Dict

from .models.exclusion_entry import ExclusionEntry


@dataclass
class Module3Context:
    output_dir: str
    reference_date: datetime.date
    exclusion_log: List[ExclusionEntry] = field(default_factory=list)

    # Counters for pipeline report
    input_rows: int = 0
    pending_count: int = 0
    excluded_count: int = 0
    overdue_count: int = 0
    non_driving_status_warnings: int = 0
    category_counts: Dict[str, int] = field(default_factory=dict)

    def log_exclusion(self, entry: ExclusionEntry) -> None:
        """Append an exclusion entry to the log."""
        self.exclusion_log.append(entry)

    def next_exclusion_id(self) -> str:
        """Auto-incrementing exclusion ID."""
        return str(len(self.exclusion_log) + 1)
