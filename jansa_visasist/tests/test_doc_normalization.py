"""
Unit tests for Step 3: Document Reference Normalization.
Tests jansa_visasist.pipeline.doc_normalization.normalize_document.
"""

import pytest
from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.doc_normalization import normalize_document


@pytest.fixture
def ctx(tmp_path):
    return PipelineContext(input_path="test.xlsx", output_dir=str(tmp_path))


class TestNormalizeDocument:
    """Step 3 normalization rules from spec V2.2 §1.4."""

    def test_no_change_passthrough(self, ctx):
        """Full-length reference that needs no transformation stays the same."""
        raw = "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result == raw.upper(), f"Expected unchanged passthrough, got {result!r}"

    def test_compressed_format_preserved(self, ctx):
        """Compressed (no underscore) reference stays the same."""
        raw = "P17T2INEXEVTPTERI001MTDTZFD026001"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result == raw.upper()

    def test_missing_p_prefix(self, ctx):
        """Reference starting with '17_T2...' gets P prepended; INFO log with missing_p_prefix."""
        raw = "17_T2_GE_EXE_LGD_FAC_B006_NDC_BZ_TX_132000"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert result.startswith("P"), f"Expected P prefix, got {result!r}"
        assert result == "P17_T2_GE_EXE_LGD_FAC_B006_NDC_BZ_TX_132000"
        info_logs = [e for e in ctx.import_log if e.category == "missing_p_prefix"]
        assert len(info_logs) == 1, "Expected exactly 1 missing_p_prefix log"
        assert info_logs[0].severity == "INFO"

    def test_hyphens_spaces_to_underscores(self, ctx):
        """Hyphens and spaces are replaced with underscores."""
        raw = "P17-T2-BX-EXE-LGD-GOE - B006 -PLN-BZ-TN-132100"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert "-" not in result, "Hyphens should be replaced"
        assert result == "P17_T2_BX_EXE_LGD_GOE_B006_PLN_BZ_TN_132100"

    def test_trailing_dot_stripped(self, ctx):
        """Trailing dot is stripped; INFO log with trailing_punctuation."""
        raw = "P17_T2_GE_EXE_FER_SER_I014_PLN_TZ_R0_000001_A."
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert not result.endswith("."), f"Trailing dot not stripped: {result!r}"
        punct_logs = [e for e in ctx.import_log if e.category == "trailing_punctuation"]
        assert len(punct_logs) == 1
        assert punct_logs[0].severity == "INFO"

    def test_trailing_comma_stripped(self, ctx):
        """Trailing comma is stripped; INFO log with trailing_punctuation."""
        raw = "P17_T2_TEST_REF_001,"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert not result.endswith(",")
        punct_logs = [e for e in ctx.import_log if e.category == "trailing_punctuation"]
        assert len(punct_logs) == 1

    def test_none_returns_none(self, ctx):
        """None input returns None; ERROR log with missing_field."""
        result = normalize_document(None, "LOT_01", 10, ctx)
        assert result is None
        err_logs = [e for e in ctx.import_log if e.category == "missing_field"]
        assert len(err_logs) == 1
        assert err_logs[0].severity == "ERROR"

    def test_empty_string_returns_none(self, ctx):
        """Empty string returns None; ERROR log with missing_field."""
        result = normalize_document("", "LOT_01", 10, ctx)
        assert result is None
        err_logs = [e for e in ctx.import_log if e.category == "missing_field"]
        assert len(err_logs) == 1

    def test_whitespace_only_returns_none(self, ctx):
        """Whitespace-only string returns None after normalization."""
        result = normalize_document("   ", "LOT_01", 10, ctx)
        assert result is None

    def test_uppercase_enforcement(self, ctx):
        """Lowercase input is uppercased."""
        raw = "p17_t2_in_exe_test_doc_001"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert result == result.upper(), f"Expected uppercase, got {result!r}"

    def test_collapse_repeated_underscores(self, ctx):
        """Double underscores collapsed to single."""
        raw = "P17__T2__TEST"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert "__" not in result, f"Double underscore found in {result!r}"

    def test_strip_leading_trailing_underscores(self, ctx):
        """Leading and trailing underscores are stripped."""
        raw = "_P17_T2_TEST_"
        result = normalize_document(raw, "LOT_01", 10, ctx)
        assert result is not None
        assert not result.startswith("_"), f"Leading underscore: {result!r}"
        assert not result.endswith("_"), f"Trailing underscore: {result!r}"
