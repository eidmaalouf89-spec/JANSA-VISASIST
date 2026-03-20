"""
Unit tests for Step 7a: Structural Document Validation.
Tests jansa_visasist.pipeline.doc_validation.validate_document.
"""

import pytest
from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.doc_validation import validate_document
from jansa_visasist.config import DOC_MIN_LENGTH, DOC_MAX_NOISE_RATIO


@pytest.fixture
def ctx(tmp_path):
    return PipelineContext(input_path="test.xlsx", output_dir=str(tmp_path))


class TestValidateDocument:
    """Step 7a structural validation rules R1-R4."""

    def test_valid_document_passthrough(self, ctx):
        """A valid 31+ char reference with letters and digits passes."""
        doc = "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001"
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result == doc, f"Valid doc should pass through, got {result!r}"
        assert len(ctx.import_log) == 0, "No warnings for valid doc"

    def test_r1_too_short(self, ctx):
        """R1: Document shorter than DOC_MIN_LENGTH (10) returns None + WARNING."""
        doc = "AB3"
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result is None, "Short doc should be rejected"
        warns = [e for e in ctx.import_log if e.category == "unparseable_document"]
        assert len(warns) == 1
        assert warns[0].severity == "WARNING"
        assert "R1" in warns[0].action_taken

    def test_r2_no_digit(self, ctx):
        """R2: Document with no digit returns None + WARNING."""
        doc = "ABCDEFGHIJKLM"  # 13 chars, no digit
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "unparseable_document"]
        assert len(warns) == 1
        assert "R2" in warns[0].action_taken

    def test_r3_no_letter(self, ctx):
        """R3: Document with no letter returns None + WARNING."""
        doc = "1234567890123"  # 13 chars, no letter
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "unparseable_document"]
        assert len(warns) == 1
        assert "R3" in warns[0].action_taken

    def test_r4_high_noise(self, ctx):
        """R4: Document with >30% non-alnum non-underscore chars returns None."""
        # Build a string where >30% is noise (e.g., special chars)
        doc = "A1B2!!@@##$$"  # 12 chars total, 8 non-alnum-non-_ = 66% noise
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "unparseable_document"]
        assert len(warns) == 1
        assert "R4" in warns[0].action_taken

    def test_real_case_2s(self, ctx):
        """The ²S case from real data — fails R1 (too short)."""
        result = validate_document("²S", "²S", "LOT 42-PLB-UTB", 244, ctx)
        assert result is None

    def test_already_none_passthrough(self, ctx):
        """None input returns None with no additional logs."""
        result = validate_document(None, "something_raw", "LOT_01", 10, ctx)
        assert result is None
        assert len(ctx.import_log) == 0, "No logs for already-None doc"

    def test_underscore_exclusion_in_r4(self, ctx):
        """Underscores don't count in noise ratio — a clean underscore-rich doc passes."""
        doc = "P17_T2_TEST_1"  # 13 chars, underscores excluded from noise calc
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result == doc
        assert len(ctx.import_log) == 0

    def test_exact_min_length_passes(self, ctx):
        """Document exactly at DOC_MIN_LENGTH (10) with both letters and digits passes."""
        doc = "P17_TEST_1"  # 10 chars
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result == doc

    def test_just_below_min_length_fails(self, ctx):
        """Document at DOC_MIN_LENGTH - 1 fails R1."""
        doc = "P17_TST_1"  # 9 chars
        result = validate_document(doc, doc, "LOT_01", 10, ctx)
        assert result is None
