"""Exclusion entry data model for Module 3."""

from dataclasses import dataclass, asdict
from typing import Optional


@dataclass
class ExclusionEntry:
    row_id: str
    doc_family_key: str
    source_sheet: str
    exclusion_reason: str       # One of EXCLUSION_REASONS
    visa_global: Optional[str]

    def to_dict(self) -> dict:
        return asdict(self)
