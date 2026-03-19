"""
JANSA VISASIST — Module2Context
Centralized mutable state for the Module 2 pipeline.
"""

from dataclasses import dataclass, field
from typing import List

from .models.linking_anomaly import LinkingAnomalyEntry


@dataclass
class Module2Context:
    output_dir: str
    anomaly_log: List[LinkingAnomalyEntry] = field(default_factory=list)
    family_count: int = 0
    cross_lot_count: int = 0
    unparseable_count: int = 0
    duplicate_exact_count: int = 0
    duplicate_suspect_count: int = 0

    def log_anomaly(self, entry: LinkingAnomalyEntry) -> None:
        """Append an anomaly entry to the log."""
        self.anomaly_log.append(entry)

    def next_anomaly_id(self) -> str:
        """Auto-incrementing anomaly ID."""
        return str(len(self.anomaly_log) + 1)
