"""Tests for Step 4: Column Mapping."""

import pytest

from jansa_visasist.pipeline.column_mapping import normalize_header, _match_exact, _match_keyword, _match_fuzzy


class TestHeaderNormalization:
    """Test header normalization function."""

    def test_basic_normalization(self):
        assert normalize_header("  DOCUMENT  ") == "document"

    def test_newline_removal(self):
        assert normalize_header("VISA\nGLOBAL") == "visa global"

    def test_accent_removal(self):
        assert normalize_header("référence") == "reference"

    def test_parenthetical_removal(self):
        assert normalize_header("Date (jours)") == "date"

    def test_trailing_punctuation(self):
        assert normalize_header("Titre:") == "titre"

    def test_collapse_spaces(self):
        assert normalize_header("date   diffusion") == "date diffusion"


class TestExactMatch:
    """Test Level 1: exact matching."""

    def test_document_match(self):
        assert _match_exact("document") == "document"

    def test_titre_match(self):
        assert _match_exact("titre") == "titre"

    def test_no_match(self):
        assert _match_exact("foobar") is None


class TestKeywordMatch:
    """Test Level 2: keyword matching."""

    def test_date_diffusion(self):
        assert _match_keyword("date de diffusion") == "date_diffusion"

    def test_visa_global(self):
        result = _match_keyword("visa global")
        assert result == "visa_global"


class TestFuzzyMatch:
    """Test Level 3: fuzzy matching."""

    def test_close_match(self):
        result = _match_fuzzy("observtion")  # missing 'a'
        assert result is not None
        key, score = result
        assert key == "observations"
        assert score >= 0.80

    def test_too_different(self):
        result = _match_fuzzy("xyz_abc")
        assert result is None
