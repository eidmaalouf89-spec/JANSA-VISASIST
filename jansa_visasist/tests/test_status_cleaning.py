"""Tests for Steps 7c + 7d: Status Normalization."""

import pytest

from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.status_cleaning import (
    normalize_visa_global,
    normalize_approver_statut,
)


@pytest.fixture
def ctx():
    return PipelineContext(input_path="test.xlsx", output_dir="/tmp/test")


class TestVisaGlobal:
    """Test VISA GLOBAL normalization (Step 7c)."""

    def test_exact_match(self, ctx):
        ctx.begin_row()
        assert normalize_visa_global("VAO", "Sheet1", 10, ctx) == "VAO"

    def test_case_insensitive(self, ctx):
        ctx.begin_row()
        assert normalize_visa_global("vao", "Sheet1", 10, ctx) == "VAO"

    def test_all_valid_values(self, ctx):
        for val in ["VSO", "VAO", "REF", "HM", "SUS", "DEF", "FAV"]:
            ctx.begin_row()
            assert normalize_visa_global(val, "Sheet1", 10, ctx) == val

    def test_unknown_c_returns_null(self, ctx):
        """Unknown value 'C' → null + WARNING."""
        ctx.begin_row()
        result = normalize_visa_global("C", "Sheet1", 10, ctx)
        assert result is None
        logs = ctx.end_row()
        warning_logs = [l for l in logs if l.category == "unknown_status"]
        assert len(warning_logs) == 1

    def test_blank_returns_null(self, ctx):
        ctx.begin_row()
        assert normalize_visa_global(None, "Sheet1", 10, ctx) is None

    def test_whitespace_returns_null(self, ctx):
        ctx.begin_row()
        assert normalize_visa_global("  ", "Sheet1", 10, ctx) is None


class TestApproverStatut:
    """Test approver STATUT normalization (Step 7d)."""

    def test_exact_match(self, ctx):
        ctx.begin_row()
        assert normalize_approver_statut("VSO", "MOEX_GEMO", "Sheet1", 10, ctx) == "VSO"

    def test_with_leading_punctuation(self, ctx):
        ctx.begin_row()
        result = normalize_approver_statut(".VAO", "MOEX_GEMO", "Sheet1", 10, ctx)
        assert result == "VAO"

    def test_with_internal_spaces(self, ctx):
        ctx.begin_row()
        result = normalize_approver_statut("V S O", "MOEX_GEMO", "Sheet1", 10, ctx)
        assert result == "VSO"

    def test_ambiguous_vsa_returns_null(self, ctx):
        """Ambiguous typo 'VSA' → null + WARNING."""
        ctx.begin_row()
        result = normalize_approver_statut("VSA", "MOEX_GEMO", "Sheet1", 10, ctx)
        assert result is None
        logs = ctx.end_row()
        assert any(l.category == "ambiguous_status" for l in logs)

    def test_multi_value_takes_last(self, ctx):
        """Multi-value with newline: take last value."""
        ctx.begin_row()
        result = normalize_approver_statut("VSO\nVAO", "MOEX_GEMO", "Sheet1", 10, ctx)
        assert result == "VAO"
        logs = ctx.end_row()
        assert any(l.category == "multi_value_status" for l in logs)

    def test_blank_returns_null(self, ctx):
        ctx.begin_row()
        assert normalize_approver_statut(None, "MOEX_GEMO", "Sheet1", 10, ctx) is None

    def test_synonym_mapping(self, ctx):
        """Known synonym maps correctly."""
        ctx.begin_row()
        result = normalize_approver_statut("REFUSE", "MOEX_GEMO", "Sheet1", 10, ctx)
        assert result == "REF"

    def test_case_insensitive(self, ctx):
        ctx.begin_row()
        result = normalize_approver_statut("ref", "MOEX_GEMO", "Sheet1", 10, ctx)
        assert result == "REF"
