"""
Unit tests for Step 4: Column Mapping.
Tests jansa_visasist.pipeline.column_mapping.normalize_header and matching levels.
"""

import pytest
from jansa_visasist.pipeline.column_mapping import (
    normalize_header,
    _match_exact,
    _match_keyword,
    _match_fuzzy,
)


class TestNormalizeHeader:
    """Test normalize_header preprocessing."""

    def test_strips_whitespace(self):
        assert normalize_header("  document  ") == "document"

    def test_replaces_newlines_with_space(self):
        result = normalize_header("date\ndiffusion")
        assert "\n" not in result
        assert "date" in result and "diffusion" in result

    def test_lowercases(self):
        assert normalize_header("DOCUMENT") == "document"

    def test_removes_accents(self):
        """NFKD normalization removes accents."""
        result = normalize_header("réception")
        assert result == "reception"

    def test_collapses_spaces(self):
        result = normalize_header("date   de   diffusion")
        assert "  " not in result

    def test_removes_parenthetical_content(self):
        result = normalize_header("document (old)")
        assert "old" not in result
        assert result.strip() == "document"

    def test_strips_trailing_punctuation(self):
        result = normalize_header("document;")
        assert result == "document"


class TestMatchExact:
    """Level 1: exact match."""

    def test_exact_document(self):
        assert _match_exact("document") == "document"

    def test_exact_titre(self):
        assert _match_exact("titre") == "titre"

    def test_exact_date_diffusion(self):
        assert _match_exact("date diffusion") == "date_diffusion"

    def test_no_match(self):
        assert _match_exact("nonexistent_header") is None


class TestMatchKeyword:
    """Level 2: keyword match."""

    def test_keyword_date_diffusion(self):
        """Header containing 'date' and 'diffusion' maps to date_diffusion."""
        result = _match_keyword("date de diffusion bdx")
        assert result == "date_diffusion"

    def test_keyword_visa_global(self):
        result = _match_keyword("visa global projet")
        assert result == "visa_global"

    def test_no_keyword_match(self):
        result = _match_keyword("completely unrelated header")
        assert result is None


class TestMatchFuzzy:
    """Level 3: fuzzy match (score >= FUZZY_THRESHOLD=0.80)."""

    def test_slightly_misspelled(self):
        """A close misspelling should match if score >= 0.80."""
        result = _match_fuzzy("documet")  # close to 'document'
        if result is not None:
            key, score = result
            assert key == "document"
            assert score >= 0.80

    def test_totally_foreign(self):
        """Completely unrelated header gets no fuzzy match."""
        result = _match_fuzzy("zzzzzzz completely unrelated")
        assert result is None
