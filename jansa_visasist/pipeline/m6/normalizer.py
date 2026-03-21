"""
Module 6 — Query normalization.

1. Trim, lowercase, NFKD (remove accents), collapse spaces
2. Extract document references (P17... pattern)
3. Extract N value ("top N", "N premiers")
4. Tokenize remaining text
"""

import re
import unicodedata
from dataclasses import dataclass, field
from typing import List, Optional

from jansa_visasist.config_m6 import PROJECT_DOC_PREFIX


@dataclass
class NormalizedQuery:
    """Result of query normalization."""
    original: str
    normalized: str
    doc_ref: Optional[str] = None      # Extracted document reference (normalized)
    n_value: Optional[int] = None       # Extracted N for top-N queries
    tokens: List[str] = field(default_factory=list)


def normalize_text(text: str) -> str:
    """Normalize text: trim, lowercase, NFKD accent removal, collapse spaces."""
    if not text:
        return ""
    text = text.strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = re.sub(r"\s+", " ", text)
    return text


def _extract_doc_ref(text: str) -> tuple:
    """Extract document reference from query.

    Looks for P17... pattern (alphanumeric + underscores).
    Returns (doc_ref_normalized, text_without_doc_ref).
    """
    prefix = PROJECT_DOC_PREFIX.lower()

    # Match P17 followed by alphanumeric/underscore characters (document references)
    pattern = rf"({re.escape(prefix)}[a-z0-9_]+)"
    match = re.search(pattern, text)
    if match:
        doc_ref = match.group(1)
        # Remove spaces/hyphens from doc_ref for matching
        doc_ref_clean = re.sub(r"[\s\-]+", "", doc_ref)
        remaining = text[:match.start()] + text[match.end():]
        remaining = re.sub(r"\s+", " ", remaining).strip()
        return doc_ref_clean, remaining

    return None, text


def _extract_n_value(text: str) -> tuple:
    """Extract N value from top-N patterns.

    Patterns: "top N", "top N ...", "N premiers", "les N premiers"
    Returns (n_value, text_without_n_pattern).
    """
    # "top N" pattern
    match = re.search(r"\btop\s+(\d+)\b", text)
    if match:
        n = int(match.group(1))
        remaining = text[:match.start()] + text[match.end():]
        remaining = re.sub(r"\s+", " ", remaining).strip()
        return n, remaining

    # "N premiers" / "les N premiers" pattern
    match = re.search(r"\b(?:les?\s+)?(\d+)\s+premiers?\b", text)
    if match:
        n = int(match.group(1))
        remaining = text[:match.start()] + text[match.end():]
        remaining = re.sub(r"\s+", " ", remaining).strip()
        return n, remaining

    # Standalone number at start: "5 blocked" -> n=5
    match = re.match(r"^(\d+)\s+", text)
    if match:
        n = int(match.group(1))
        remaining = text[match.end():]
        remaining = re.sub(r"\s+", " ", remaining).strip()
        return n, remaining

    return None, text


def normalize_query(query: str) -> NormalizedQuery:
    """Full query normalization pipeline.

    Steps:
    1. Trim, lowercase, NFKD, collapse spaces
    2. Extract document reference
    3. Extract N value
    4. Tokenize remaining text
    """
    original = query
    normalized = normalize_text(query)

    # Extract document reference
    doc_ref, remaining = _extract_doc_ref(normalized)

    # Extract N value
    n_value, remaining = _extract_n_value(remaining)

    # Tokenize
    tokens = remaining.split() if remaining else []

    return NormalizedQuery(
        original=original,
        normalized=normalized,
        doc_ref=doc_ref,
        n_value=n_value,
        tokens=tokens,
    )
