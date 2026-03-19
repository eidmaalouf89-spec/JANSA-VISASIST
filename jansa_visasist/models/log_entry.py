"""Import log entry data model."""

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class ImportLogEntry:
    log_id: str
    sheet: str
    row: Optional[int]
    column: Optional[str]
    severity: str  # ERROR | WARNING | INFO
    category: str
    raw_value: Optional[str]
    action_taken: str
    confidence: Optional[float] = None

    def to_dict(self) -> dict:
        return asdict(self)
