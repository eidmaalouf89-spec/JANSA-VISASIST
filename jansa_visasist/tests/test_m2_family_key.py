"""
Test M2 — doc_family_key resolution.
Verifies that compressed and underscore formats resolve to the same family key.
This is the core grouping mechanism of Module 2 (88 naming-format pairs).

Note: The actual API is build_family_keys(df, ctx) which operates on DataFrames.
We wrap it in a helper build_family_key() to test single-document resolution.
"""
import pytest
import pandas as pd

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.pipeline.m2.family_key import build_family_keys
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX


@pytest.fixture
def ctx(tmp_path):
    return Module2Context(output_dir=str(tmp_path))


def build_family_key(document, ctx=None, document_raw=None, source_sheet="LOT_01", source_row=1):
    """
    Helper: resolve a single document string to its doc_family_key
    by wrapping build_family_keys(df, ctx).
    """
    import tempfile
    if ctx is None:
        ctx = Module2Context(output_dir=tempfile.mkdtemp())
    df = pd.DataFrame([{
        "document": document,
        "document_raw": document_raw,
        "source_sheet": source_sheet,
        "source_row": source_row,
        "row_id": f"1_{source_row}",
        "ind": None,
    }])
    result = build_family_keys(df, ctx)
    return result.at[0, "doc_family_key"]


NAMING_PAIRS = [
    (
        "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
        "P17T2INEXEVTPTERI001MTDTZFD026001",
    ),
    (
        "P17_T2_GE_EXE_LGD_FAC_B006_NDC_BZ_TX_132000",
        "P17T2GEEXELGDFACB006NDCBZTX132000",
    ),
    (
        "P17_T2_BX_EXE_LGD_GOE_B006_PLN_BZ_TN_132100",
        "P17T2BXEXELGDGOEB006PLNBZTN132100",
    ),
]


@pytest.mark.parametrize("underscore_fmt,compressed_fmt", NAMING_PAIRS)
def test_naming_pair_same_family_key(underscore_fmt, compressed_fmt):
    """Underscore and compressed formats must resolve to the same doc_family_key."""
    assert build_family_key(underscore_fmt) == build_family_key(compressed_fmt), (
        f"Family key mismatch:\n"
        f"  underscore : {underscore_fmt}\n"
        f"  compressed : {compressed_fmt}\n"
        f"  key_a      : {build_family_key(underscore_fmt)}\n"
        f"  key_b      : {build_family_key(compressed_fmt)}"
    )


def test_null_document_uses_fallback_key():
    """None document must not crash and must return an UNPARSEABLE key."""
    key = build_family_key(None, document_raw="\u00b2S", source_sheet="LOT 42-PLB-UTB", source_row=244)
    assert key.startswith(UNPARSEABLE_PREFIX)
    assert len(key) > 15


def test_family_key_is_uppercase_flat():
    """doc_family_key must be uppercase with no underscores."""
    key = build_family_key("P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001")
    assert "_" not in key
    assert key == key.upper()
