"""
Step 4: Column Mapping
Normalize raw headers and map to canonical schema using 4-level matching.
"""

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, List

from rapidfuzz import fuzz

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import CANONICAL_COLUMN_MAP, FUZZY_THRESHOLD

logger = logging.getLogger(__name__)

# ── Pre-computed lookup tables (built once at module import) ──
# These avoid re-iterating CANONICAL_COLUMN_MAP on every header.
_EXACT_LOOKUP: Dict[str, str] = {}
for _key, _spec in CANONICAL_COLUMN_MAP.items():
    for _alias in _spec.get("exact", []):
        _EXACT_LOOKUP[_alias] = _key

# Pre-compute all fuzzy targets once (not per header)
_FUZZY_TARGETS: List[tuple] = []
for _key, _spec in CANONICAL_COLUMN_MAP.items():
    _FUZZY_TARGETS.append((_key, _key.replace("_", " ")))
    for _alias in _spec.get("exact", []):
        _FUZZY_TARGETS.append((_key, _alias))


@dataclass
class ColumnMapping:
    col_index: int
    raw_header: str
    canonical_key: Optional[str]
    confidence: float
    match_level: int  # 1=exact, 2=keyword, 3=fuzzy, 4=unmatched


def normalize_header(raw: str) -> str:
    """Normalize a raw header string for matching."""
    val = raw.strip()
    val = val.replace('\n', ' ').replace('\r', ' ')
    val = val.lower()
    val = unicodedata.normalize('NFKD', val)
    val = ''.join(c for c in val if not unicodedata.combining(c))
    val = re.sub(r'\s+', ' ', val).strip()
    val = re.sub(r'\(.*?\)', '', val).strip()
    val = re.sub(r'[.,;:]+$', '', val).strip()
    return val


def _match_exact(normalized: str) -> Optional[str]:
    """Level 1: O(1) lookup in pre-computed exact dict."""
    return _EXACT_LOOKUP.get(normalized)


def _match_keyword(normalized: str) -> Optional[str]:
    """Level 2: all keywords present in normalized header."""
    for canonical_key, spec in CANONICAL_COLUMN_MAP.items():
        keywords = spec.get("keywords", [])
        if keywords and all(kw in normalized for kw in keywords):
            return canonical_key
    return None


def _match_fuzzy(normalized: str) -> Optional[tuple]:
    """Level 3: fuzzy matching against pre-computed target list.
    Returns (key, score) or None. Only used during header mapping (once per sheet),
    never inside row loops."""
    best_key = None
    best_score = 0.0

    for canonical_key, target in _FUZZY_TARGETS:
        score = fuzz.ratio(normalized, target) / 100.0
        if score >= FUZZY_THRESHOLD and score > best_score:
            best_score = score
            best_key = canonical_key

    if best_key is not None:
        return (best_key, best_score)
    return None


def map_columns(
    ws,
    header_row: int,
    sheet_name: str,
    ctx: PipelineContext,
) -> Dict[int, ColumnMapping]:
    """
    Read row R headers and map each to canonical schema.
    Returns dict mapping column_index -> ColumnMapping.
    """
    mappings: Dict[int, ColumnMapping] = {}
    already_mapped: set = set()  # Track canonical keys to prevent duplicates

    # Read header row as a single tuple — one iter_rows call instead
    # of max_col individual ws.cell() calls.
    header_values = None
    for row_tuple in ws.iter_rows(
        min_row=header_row, max_row=header_row, values_only=True
    ):
        header_values = row_tuple
        break

    if header_values is None:
        return mappings

    for col_idx, cell_value in enumerate(header_values, start=1):
        if cell_value is None or str(cell_value).strip() == "":
            continue

        raw_header = str(cell_value)
        normalized = normalize_header(raw_header)

        if not normalized:
            continue

        # Level 1: Exact match
        exact_match = _match_exact(normalized)
        if exact_match and exact_match not in already_mapped:
            mappings[col_idx] = ColumnMapping(
                col_index=col_idx,
                raw_header=raw_header,
                canonical_key=exact_match,
                confidence=1.0,
                match_level=1,
            )
            already_mapped.add(exact_match)
            continue

        # Level 2: Keyword match
        keyword_match = _match_keyword(normalized)
        if keyword_match and keyword_match not in already_mapped:
            mappings[col_idx] = ColumnMapping(
                col_index=col_idx,
                raw_header=raw_header,
                canonical_key=keyword_match,
                confidence=0.85,
                match_level=2,
            )
            already_mapped.add(keyword_match)
            continue

        # Level 3: Fuzzy match
        fuzzy_result = _match_fuzzy(normalized)
        if fuzzy_result and fuzzy_result[0] not in already_mapped:
            key, score = fuzzy_result
            mappings[col_idx] = ColumnMapping(
                col_index=col_idx,
                raw_header=raw_header,
                canonical_key=key,
                confidence=round(score, 2),
                match_level=3,
            )
            already_mapped.add(key)
            ctx.log_sheet_level(ImportLogEntry(
                log_id=ctx.next_log_id(),
                sheet=sheet_name,
                row=None,
                column=raw_header,
                severity=Severity.INFO.value,
                category="fuzzy_match",
                raw_value=raw_header,
                action_taken=f"Fuzzy matched '{normalized}' -> '{key}' (score={score:.2f})",
                confidence=round(score, 2),
            ))
            continue

        # Level 4: Unmatched
        unmapped_key = f"unmapped_{col_idx}"
        mappings[col_idx] = ColumnMapping(
            col_index=col_idx,
            raw_header=raw_header,
            canonical_key=unmapped_key,
            confidence=0.0,
            match_level=4,
        )
        ctx.log_sheet_level(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=None,
            column=raw_header,
            severity=Severity.WARNING.value,
            category="unmapped_column",
            raw_value=raw_header,
            action_taken=f"Column '{raw_header}' (normalized: '{normalized}') not matched — stored as {unmapped_key}",
        ))

    return mappings
