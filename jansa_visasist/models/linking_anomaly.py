"""Linking anomaly entry data model for Module 2."""

from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any


@dataclass
class LinkingAnomalyEntry:
    anomaly_id: str
    anomaly_type: str            # One of the 7 defined types
    doc_family_key: str
    source_sheet: Optional[str]
    row_id: Optional[str]        # Specific row (for MISSING_IND, UNPARSEABLE)
    ind: Optional[str]           # Relevant IND value
    severity: str                # WARNING (all M2 anomalies are WARNING)
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)
