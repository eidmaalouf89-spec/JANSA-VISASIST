"""
JANSA VISASIST — PipelineContext
Centralized mutable state passed through every pipeline step.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional

from .models.log_entry import ImportLogEntry
from .models.header_report import HeaderMappingReport


@dataclass
class PipelineContext:
    # --- Configuration (immutable after init) ---
    input_path: str
    output_dir: str

    # --- Accumulated state (mutated during pipeline) ---
    import_log: List[ImportLogEntry] = field(default_factory=list)
    header_reports: List[HeaderMappingReport] = field(default_factory=list)
    sheet_row_counts: Dict[str, int] = field(default_factory=dict)
    sheets_processed: int = 0
    sheets_skipped: int = 0
    total_rows: int = 0

    # --- Per-row log accumulator (reset per row) ---
    _current_row_logs: List[ImportLogEntry] = field(default_factory=list)

    def log(self, entry: ImportLogEntry) -> None:
        """Append a log entry to both current-row buffer and global log."""
        self._current_row_logs.append(entry)
        self.import_log.append(entry)

    def log_sheet_level(self, entry: ImportLogEntry) -> None:
        """Append a sheet-level log entry (no row context)."""
        self.import_log.append(entry)

    def begin_row(self) -> None:
        """Reset per-row accumulator before processing a new row."""
        self._current_row_logs = []

    def end_row(self) -> List[ImportLogEntry]:
        """Return accumulated logs for the current row (for quality scoring)."""
        return list(self._current_row_logs)

    def next_log_id(self) -> str:
        """Auto-incrementing log ID."""
        return str(len(self.import_log) + 1)
