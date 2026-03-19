"""Unit tests for Step 1: doc_family_key construction."""

import pandas as pd
import pytest

from jansa_visasist.context_m2 import Module2Context
from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX
from jansa_visasist.pipeline.m2.family_key import build_family_keys


def _make_df(rows):
    return pd.DataFrame(rows)


def _ctx():
    return Module2Context(output_dir="/tmp/test")


class TestFamilyKey:
    def test_underscore_format(self):
        df = _make_df([{
            "document": "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
            "document_raw": "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
            "source_sheet": "LOT 06", "source_row": 10, "row_id": "0_10", "ind": "A",
        }])
        df = build_family_keys(df, _ctx())
        assert df.iloc[0]["doc_family_key"] == "P17T2INEXEVTPTERI001MTDTZFD026001"

    def test_compressed_format(self):
        df = _make_df([{
            "document": "P17T2INEXEVTPTERI001MTDTZFD026001",
            "document_raw": "P17T2INEXEVTPTERI001MTDTZFD026001",
            "source_sheet": "LOT 06", "source_row": 10, "row_id": "0_10", "ind": "A",
        }])
        df = build_family_keys(df, _ctx())
        assert df.iloc[0]["doc_family_key"] == "P17T2INEXEVTPTERI001MTDTZFD026001"

    def test_88_pair_identity(self):
        """Both naming formats produce identical doc_family_key."""
        df = _make_df([
            {"document": "P17_T2_IN_EXE_VTP_TER_I001_MTD_TZ_FD_026001",
             "document_raw": "x", "source_sheet": "LOT 06", "source_row": 10, "row_id": "0_10", "ind": "A"},
            {"document": "P17T2INEXEVTPTERI001MTDTZFD026001",
             "document_raw": "y", "source_sheet": "LOT 06", "source_row": 11, "row_id": "0_11", "ind": "A"},
        ])
        df = build_family_keys(df, _ctx())
        assert df.iloc[0]["doc_family_key"] == df.iloc[1]["doc_family_key"]

    def test_null_document_unparseable(self):
        df = _make_df([{
            "document": None,
            "document_raw": "xS",
            "source_sheet": "LOT 42", "source_row": 244, "row_id": "24_244", "ind": None,
        }])
        ctx = _ctx()
        df = build_family_keys(df, ctx)
        key = df.iloc[0]["doc_family_key"]
        assert key.startswith(UNPARSEABLE_PREFIX)
        assert len(ctx.anomaly_log) == 1
        assert ctx.anomaly_log[0].anomaly_type == "UNPARSEABLE_DOCUMENT"

    def test_two_null_documents_different_keys(self):
        df = _make_df([
            {"document": None, "document_raw": "xS",
             "source_sheet": "LOT 42", "source_row": 244, "row_id": "24_244", "ind": None},
            {"document": None, "document_raw": "",
             "source_sheet": "LOT 42", "source_row": 245, "row_id": "24_245", "ind": None},
        ])
        df = build_family_keys(df, _ctx())
        assert df.iloc[0]["doc_family_key"] != df.iloc[1]["doc_family_key"]

    def test_blank_document_raw_unparseable(self):
        df = _make_df([{
            "document": None,
            "document_raw": None,
            "source_sheet": "LOT 01", "source_row": 5, "row_id": "0_5", "ind": "A",
        }])
        df = build_family_keys(df, _ctx())
        assert df.iloc[0]["doc_family_key"].startswith(UNPARSEABLE_PREFIX)

    def test_deterministic(self):
        """Same input -> same output."""
        rows = [{"document": None, "document_raw": "xS",
                 "source_sheet": "LOT 42", "source_row": 244, "row_id": "24_244", "ind": None}]
        key1 = build_family_keys(_make_df(rows), _ctx()).iloc[0]["doc_family_key"]
        key2 = build_family_keys(_make_df(rows), _ctx()).iloc[0]["doc_family_key"]
        assert key1 == key2
