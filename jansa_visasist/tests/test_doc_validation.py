"""Tests for Step 7a: Structural Document Validation."""

import pytest

from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.doc_validation import validate_document


@pytest.fixture
def ctx():
    return PipelineContext(input_path="test.xlsx", output_dir="/tmp/test")


class TestDocValidation:
    """Test structural document validation rules R1-R4."""

    def test_valid_document_passes(self, ctx):
        """A normal document reference passes all rules."""
        ctx.begin_row()
        result = validate_document(
            "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
            "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
            "Sheet1", 10, ctx
        )
        assert result == "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001"
        # No warnings should be logged
        logs = ctx.end_row()
        warning_logs = [l for l in logs if l.severity == "WARNING"]
        assert len(warning_logs) == 0

    def test_two_s_fails_r1(self, ctx):
        """'²S' (after normalization) fails R1: too short."""
        ctx.begin_row()
        # After normalization, ²S would become something like "²S" — 2 chars
        result = validate_document("²S", "²S", "LOT 42-PLB-UTB", 244, ctx)
        assert result is None
        logs = ctx.end_row()
        warning_logs = [l for l in logs if l.category == "unparseable_document"]
        assert len(warning_logs) == 1
        assert "R1" in warning_logs[0].action_taken

    def test_none_input_passes_through(self, ctx):
        """None document (already flagged) is passed through."""
        ctx.begin_row()
        result = validate_document(None, None, "Sheet1", 10, ctx)
        assert result is None

    def test_no_digit_fails_r2(self, ctx):
        """String with no digits fails R2."""
        ctx.begin_row()
        result = validate_document("ABCDEFGHIJK", "ABCDEFGHIJK", "Sheet1", 10, ctx)
        assert result is None
        logs = ctx.end_row()
        assert any("R2" in l.action_taken for l in logs)

    def test_no_letter_fails_r3(self, ctx):
        """String with no letters fails R3."""
        ctx.begin_row()
        result = validate_document("1234567890", "1234567890", "Sheet1", 10, ctx)
        assert result is None
        logs = ctx.end_row()
        assert any("R3" in l.action_taken for l in logs)

    def test_high_noise_fails_r4(self, ctx):
        """String with high noise ratio fails R4."""
        ctx.begin_row()
        # More than 30% non-alnum (excluding underscores)
        result = validate_document("A1!@#$%^&*()B", "A1!@#$%^&*()B", "Sheet1", 10, ctx)
        assert result is None
        logs = ctx.end_row()
        assert any("R4" in l.action_taken for l in logs)

    def test_short_but_valid_chars_still_fails_r1(self, ctx):
        """Short string even with valid characters fails R1."""
        ctx.begin_row()
        result = validate_document("P17_ABC", "P17_ABC", "Sheet1", 10, ctx)
        assert result is None  # Length 7 < 10
