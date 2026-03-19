"""Tests for Step 6: Row Extraction (permissive) — iter_rows API.

The optimized extract_rows uses ws.iter_rows(values_only=True)
instead of ws.cell().  The mock must provide iter_rows, not cell.
"""

import pytest
from unittest.mock import MagicMock

from jansa_visasist.pipeline.row_extraction import extract_rows, EMPTY_ROW_THRESHOLD
from jansa_visasist.pipeline.column_mapping import ColumnMapping
from jansa_visasist.pipeline.approver_detection import ApproverBlock


def _make_mock_worksheet(rows_as_tuples, data_start=1):
    """
    Create a mock worksheet that supports iter_rows(min_row, values_only).
    rows_as_tuples: list of tuples, one per row starting at data_start.
    """
    ws = MagicMock()

    def iter_rows_impl(min_row=1, max_row=None, values_only=False):
        # Compute offset into the rows list
        offset = min_row - data_start
        if offset < 0:
            offset = 0
        for row_tuple in rows_as_tuples[offset:]:
            yield row_tuple

    ws.iter_rows = iter_rows_impl
    return ws


class TestRowExtraction:
    """Test permissive row extraction with iter_rows API."""

    def test_non_empty_col_a_extracted(self):
        """Rows with non-empty column A are extracted."""
        rows = [
            ("P17_DOC_001", "Some title"),
            ("P17_DOC_002", "Another title"),
        ]
        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            2: ColumnMapping(2, "TITRE", "titre", 1.0, 1),
        }
        result = extract_rows(ws, 10, col_mappings, [], "Sheet1")
        assert len(result) == 2
        assert result[0]["document"] == "P17_DOC_001"
        assert result[1]["titre"] == "Another title"

    def test_empty_col_a_skipped(self):
        """Rows with empty column A are skipped."""
        rows = [
            ("P17_DOC_001", "Title"),
            (None, "No doc"),
            ("  ", "Whitespace doc"),
            ("P17_DOC_002", "Title 2"),
        ]
        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            2: ColumnMapping(2, "TITRE", "titre", 1.0, 1),
        }
        result = extract_rows(ws, 10, col_mappings, [], "Sheet1")
        assert len(result) == 2

    def test_malformed_document_still_extracted(self):
        """'²S' is extracted (permissive) — validation happens later."""
        rows = [
            ("²S", "BX : PLAN DE RESEAU"),
        ]
        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            2: ColumnMapping(2, "TITRE", "titre", 1.0, 1),
        }
        result = extract_rows(ws, 10, col_mappings, [], "Sheet1")
        assert len(result) == 1
        assert result[0]["document"] == "²S"

    def test_source_row_tracked(self):
        """Each row records its correct 1-based source row number."""
        rows = [("DOC_A", "Title")]
        ws = _make_mock_worksheet(rows, data_start=5)
        col_mappings = {1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1)}
        result = extract_rows(ws, 5, col_mappings, [], "Sheet1")
        assert len(result) == 1
        assert result[0]["source_row"] == 5

    def test_early_stopping_after_threshold_empty_rows(self):
        """Extraction stops after EMPTY_ROW_THRESHOLD consecutive empty rows."""
        # 2 real rows, then EMPTY_ROW_THRESHOLD empty rows, then 1 more real row
        rows = [
            ("DOC_1", "Title 1"),
            ("DOC_2", "Title 2"),
        ]
        rows += [(None, None)] * EMPTY_ROW_THRESHOLD
        rows += [("DOC_3_UNREACHABLE", "Title 3")]

        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            2: ColumnMapping(2, "TITRE", "titre", 1.0, 1),
        }
        result = extract_rows(ws, 10, col_mappings, [], "Sheet1")
        # DOC_3 is after the threshold gap → should NOT be reached
        assert len(result) == 2
        assert all(r["document"] != "DOC_3_UNREACHABLE" for r in result)

    def test_sparse_empty_rows_do_not_trigger_early_stop(self):
        """Scattered empty rows (fewer than threshold consecutive) don't stop extraction."""
        rows = [
            ("DOC_1", "T1"),
            (None, None),   # 1 empty
            (None, None),   # 2 empty
            ("DOC_2", "T2"),  # resets counter
            (None, None),   # 1 empty
            ("DOC_3", "T3"),
        ]
        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            2: ColumnMapping(2, "TITRE", "titre", 1.0, 1),
        }
        result = extract_rows(ws, 10, col_mappings, [], "Sheet1")
        assert len(result) == 3

    def test_approver_columns_extracted(self):
        """Approver date/n/statut columns are extracted via tuple index."""
        # Columns: 1=doc, 2=titre, 3=approver_date, 4=approver_n, 5=approver_statut
        rows = [
            ("DOC_1", "Title", "2024-01-15", "BDX001", "VSO"),
        ]
        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            2: ColumnMapping(2, "TITRE", "titre", 1.0, 1),
        }
        approver_blocks = [
            ApproverBlock(
                canonical_key="MOEX_GEMO",
                raw_name="MOEX GEMO",
                date_col=3,
                n_col=4,
                statut_col=5,
            )
        ]
        result = extract_rows(ws, 10, col_mappings, approver_blocks, "Sheet1")
        assert len(result) == 1
        assert result[0]["MOEX_GEMO_date_src"] == "2024-01-15"
        assert result[0]["MOEX_GEMO_n_src"] == "BDX001"
        assert result[0]["MOEX_GEMO_statut_src"] == "VSO"

    def test_short_tuple_safe_get(self):
        """Columns beyond tuple length return None (safe_get)."""
        rows = [
            ("DOC_1",),  # Only 1 column in tuple
        ]
        ws = _make_mock_worksheet(rows, data_start=10)
        col_mappings = {
            1: ColumnMapping(1, "DOCUMENT", "document", 1.0, 1),
            5: ColumnMapping(5, "NIV", "niv", 1.0, 1),  # index 4, beyond tuple
        }
        result = extract_rows(ws, 10, col_mappings, [], "Sheet1")
        assert len(result) == 1
        assert result[0]["document"] == "DOC_1"
        assert result[0]["niv"] is None  # safe_get returns None
