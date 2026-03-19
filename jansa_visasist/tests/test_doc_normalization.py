"""Tests for Step 3: Document Reference Normalization."""

import pytest

from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.doc_normalization import normalize_document


@pytest.fixture
def ctx():
    return PipelineContext(input_path="test.xlsx", output_dir="/tmp/test")


class TestDocNormalization:
    """Test document reference normalization pipeline."""

    def test_no_change(self, ctx):
        """Clean document reference passes through unchanged."""
        ctx.begin_row()
        result = normalize_document(
            "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
            "Sheet1", 10, ctx
        )
        assert result == "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001"

    def test_compressed_format_preserved(self, ctx):
        """Compressed format (no underscores) is preserved."""
        ctx.begin_row()
        result = normalize_document(
            "P17T2INEXEVTPTERI001MTDTZFD026001",
            "Sheet1", 10, ctx
        )
        assert result == "P17T2INEXEVTPTERI001MTDTZFD026001"

    def test_missing_p_prefix(self, ctx):
        """Document starting with 17_T2 gets P prepended."""
        ctx.begin_row()
        result = normalize_document(
            "17_T2_GE_EXE_LGD_FAC_B006_NDC_BZ_TX_132000",
            "Sheet1", 10, ctx
        )
        assert result == "P17_T2_GE_EXE_LGD_FAC_B006_NDC_BZ_TX_132000"
        # Check INFO log was emitted
        logs = ctx.end_row()
        info_logs = [l for l in logs if l.category == "missing_p_prefix"]
        assert len(info_logs) == 1

    def test_missing_p_prefix_no_underscore(self, ctx):
        """Document starting with 17T2 (no underscore) gets P prepended."""
        ctx.begin_row()
        result = normalize_document(
            "17T2GEEEXELGDFACB006NDCBZTX132000",
            "Sheet1", 10, ctx
        )
        assert result.startswith("P17T2")

    def test_hyphens_spaces_to_underscores(self, ctx):
        """Hyphens and spaces replaced with underscores."""
        ctx.begin_row()
        result = normalize_document(
            "P17-T2-BX-EXE-LGD-GOE - B006 -PLN-BZ-TN-132100",
            "Sheet1", 10, ctx
        )
        assert result == "P17_T2_BX_EXE_LGD_GOE_B006_PLN_BZ_TN_132100"

    def test_trailing_dot_stripped(self, ctx):
        """Trailing dot is stripped and logged."""
        ctx.begin_row()
        result = normalize_document(
            "P17_T2_GE_EXE_FER_SER_I014_PLN_TZ_R0_000001_A.",
            "Sheet1", 10, ctx
        )
        assert result == "P17_T2_GE_EXE_FER_SER_I014_PLN_TZ_R0_000001_A"
        logs = ctx.end_row()
        info_logs = [l for l in logs if l.category == "trailing_punctuation"]
        assert len(info_logs) == 1

    def test_none_input(self, ctx):
        """None input returns None with ERROR log."""
        ctx.begin_row()
        result = normalize_document(None, "Sheet1", 10, ctx)
        assert result is None
        logs = ctx.end_row()
        error_logs = [l for l in logs if l.severity == "ERROR"]
        assert len(error_logs) == 1

    def test_empty_string_input(self, ctx):
        """Empty string returns None."""
        ctx.begin_row()
        result = normalize_document("   ", "Sheet1", 10, ctx)
        assert result is None

    def test_collapse_repeated_underscores(self, ctx):
        """Multiple underscores are collapsed to single."""
        ctx.begin_row()
        result = normalize_document(
            "P17__T2__GE__EXE",
            "Sheet1", 10, ctx
        )
        assert "__" not in result
        assert result == "P17_T2_GE_EXE"

    def test_uppercase(self, ctx):
        """Result is uppercased."""
        ctx.begin_row()
        result = normalize_document(
            "p17_t2_ge_exe_lgd_fac_b006_ndc_bz_tx_132000",
            "Sheet1", 10, ctx
        )
        assert result == result.upper()
