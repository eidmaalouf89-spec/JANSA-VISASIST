"""
Unit tests for Steps 7c + 7d: VISA GLOBAL and Approver STATUT normalization.
Tests jansa_visasist.pipeline.status_cleaning.
"""

import pytest
from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.status_cleaning import (
    normalize_visa_global,
    normalize_approver_statut,
)


@pytest.fixture
def ctx(tmp_path):
    return PipelineContext(input_path="test.xlsx", output_dir=str(tmp_path))


class TestNormalizeVisaGlobal:
    """Step 7c: visa_global normalization."""

    @pytest.mark.parametrize("raw,expected", [
        ("VAO", "VAO"),
        ("REF", "REF"),
        ("VSO", "VSO"),
        ("HM", "HM"),
        ("SUS", "SUS"),
        ("DEF", "DEF"),
        ("FAV", "FAV"),
    ])
    def test_valid_values(self, raw, expected, ctx):
        """Each valid visa_global value returns itself."""
        assert normalize_visa_global(raw, "LOT_01", 10, ctx) == expected

    def test_case_normalization(self, ctx):
        """Lowercase input is uppercased to match."""
        assert normalize_visa_global("vao", "LOT_01", 10, ctx) == "VAO"

    def test_unknown_value(self, ctx):
        """Unknown value returns None + WARNING with unknown_status."""
        result = normalize_visa_global("C", "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "unknown_status"]
        assert len(warns) == 1
        assert warns[0].severity == "WARNING"

    def test_none_returns_none(self, ctx):
        """GP2: None returns None silently."""
        result = normalize_visa_global(None, "LOT_01", 10, ctx)
        assert result is None
        assert len(ctx.import_log) == 0

    def test_empty_string_returns_none(self, ctx):
        """GP2: Empty string returns None silently."""
        result = normalize_visa_global("", "LOT_01", 10, ctx)
        assert result is None
        assert len(ctx.import_log) == 0


class TestNormalizeApproverStatut:
    """Step 7d: approver STATUT normalization."""

    @pytest.mark.parametrize("raw,expected", [
        ("V.S.O", "VSO"),
        ("HORS MARCHE", "HM"),  # spaces stripped → "HORSMARCHE" → matches synonym
        ("REFUSE", "REF"),
        ("REFUS", "REF"),
    ])
    def test_known_synonyms(self, raw, expected, ctx):
        """Known synonyms map to canonical values."""
        result = normalize_approver_statut(raw, "BET", "LOT_01", 10, ctx)
        assert result == expected, f"'{raw}' should map to '{expected}', got {result!r}"

    def test_ambiguous_typo(self, ctx):
        """Ambiguous typo returns None + WARNING with ambiguous_status."""
        result = normalize_approver_statut("VSA", "BET", "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "ambiguous_status"]
        assert len(warns) == 1
        assert warns[0].severity == "WARNING"

    def test_multi_value_newline(self, ctx):
        """Multi-value (newline separated) takes last value + INFO log."""
        result = normalize_approver_statut("VAO\nREF", "BET", "LOT_01", 10, ctx)
        assert result == "REF", "Should take last value"
        info_logs = [e for e in ctx.import_log if e.category == "multi_value_status"]
        assert len(info_logs) == 1
        assert info_logs[0].severity == "INFO"

    def test_leading_punctuation_stripped(self, ctx):
        """Leading punctuation is stripped."""
        result = normalize_approver_statut(".VAO", "BET", "LOT_01", 10, ctx)
        assert result == "VAO"

    def test_internal_spaces_removed(self, ctx):
        """Internal spaces are removed."""
        result = normalize_approver_statut("V A O", "BET", "LOT_01", 10, ctx)
        assert result == "VAO"

    def test_none_returns_none(self, ctx):
        """GP2: None returns None."""
        result = normalize_approver_statut(None, "BET", "LOT_01", 10, ctx)
        assert result is None

    def test_empty_returns_none(self, ctx):
        """GP2: Empty string returns None."""
        result = normalize_approver_statut("", "BET", "LOT_01", 10, ctx)
        assert result is None

    def test_unknown_status_warns(self, ctx):
        """Completely unknown status returns None + WARNING."""
        result = normalize_approver_statut("ZZZZZ", "BET", "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log
                 if e.category in ("unknown_status", "ambiguous_status")]
        assert len(warns) >= 1
