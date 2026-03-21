"""
JANSA VISASIST — Module 6 Context
Dataclass holding loaded indexes and dictionaries for chatbot operations.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Module6Context:
    """Holds all loaded data, indexes, and dictionaries for M6 chatbot."""

    # Raw queue data
    queue_data: List[Dict[str, Any]] = field(default_factory=list)

    # Primary indexes
    queue_index: Dict[str, Dict[str, Any]] = field(default_factory=dict)       # row_id -> row
    doc_index: Dict[str, List[str]] = field(default_factory=dict)              # normalized_doc -> [row_ids]
    lot_index: Dict[str, List[str]] = field(default_factory=dict)              # source_sheet -> sorted row_ids

    # Approver indexes (P9)
    approver_missing_index: Dict[str, List[str]] = field(default_factory=dict)   # approver -> row_ids
    approver_blocking_index: Dict[str, List[str]] = field(default_factory=dict)  # approver -> row_ids
    approver_assigned_index: Dict[str, List[str]] = field(default_factory=dict)  # approver -> row_ids

    # Lookup dictionaries
    lot_aliases: Dict[str, str] = field(default_factory=dict)         # alias -> source_sheet
    approver_aliases: Dict[str, str] = field(default_factory=dict)    # alias -> canonical key
    category_aliases: Dict[str, str] = field(default_factory=dict)    # alias -> category enum
    status_synonyms: Dict[str, Dict] = field(default_factory=dict)    # keyword -> filter spec
    action_keywords: Dict[str, str] = field(default_factory=dict)     # keyword -> action type

    # Optional M4/M5 data (keyed by row_id)
    m4_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    m5_data: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    # Export directory
    export_dir: str = "output/m6"
