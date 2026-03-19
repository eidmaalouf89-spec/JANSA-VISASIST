"""Header mapping report data model."""

from dataclasses import dataclass, field, asdict
from typing import List


@dataclass
class HeaderMappingReport:
    sheet: str
    header_row_detected: int
    core_columns_mapped: int
    core_columns_missing: List[str] = field(default_factory=list)
    approvers_detected: List[str] = field(default_factory=list)
    mapping_confidence: str = "HIGH"  # HIGH | MEDIUM | LOW | FAILED
    unmapped_columns: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)
