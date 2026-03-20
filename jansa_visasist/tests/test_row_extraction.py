"""
Unit tests for Step 6: Row Extraction.
Tests jansa_visasist.pipeline.row_extraction.extract_rows.
"""

import pytest
from unittest.mock import MagicMock
from jansa_visasist.pipeline.row_extraction import extract_rows, EMPTY_ROW_THRESHOLD
from jansa_visasist.pipeline.column_mapping import ColumnMapping


def _make_col_mapping(col_idx, canonical_key):
    return ColumnMapping(
        col_index=col_idx,
        raw_header=canonical_key,
        canonical_key=canonical_key,
        confidence=1.0,
        match_level=1,
    )


def _make_ws(data_rows):
    """Create a mock worksheet with iter_rows returning given data."""
    ws = MagicMock()
    ws.iter_rows = MagicMock(return_value=iter(data_rows))
    return ws


class TestExtractRows:
    """Step 6: permissive row extraction."""

    def test_normal_extraction(self):
        """Rows with non-empty column A are included."""
        data = [
            ("DOC_001", "Title A", "2024-01-01"),
            ("DOC_002", "Title B", "2024-01-02"),
        ]
        ws = _make_ws(data)
        mappings = {
            1: _make_col_mapping(1, "document"),
            2: _make_col_mapping(2, "titre"),
            3: _make_col_mapping(3, "date_diffusion"),
        }
        rows = extract_rows(ws, 2, mappings, [], "LOT_01")
        assert len(rows) == 2
        assert rows[0]["document"] == "DOC_001"
        assert rows[1]["document"] == "DOC_002"

    def test_empty_column_a_skipped(self):
        """Rows with None/empty in column A are not extracted."""
        data = [
            ("DOC_001", "Title A"),
            (None, "Ghost row"),
            ("", "Another ghost"),
            ("DOC_002", "Title B"),
        ]
        ws = _make_ws(data)
        mappings = {1: _make_col_mapping(1, "document"), 2: _make_col_mapping(2, "titre")}
        rows = extract_rows(ws, 2, mappings, [], "LOT_01")
        assert len(rows) == 2
        assert rows[0]["document"] == "DOC_001"
        assert rows[1]["document"] == "DOC_002"

    def test_early_stop_after_empty_threshold(self):
        """Scanning stops after EMPTY_ROW_THRESHOLD (30) consecutive empty rows."""
        data = [("DOC_001", "Title")]
        # Add 30 empty rows then one more valid row
        data += [(None,)] * EMPTY_ROW_THRESHOLD
        data += [("DOC_AFTER_GAP", "Should not be seen")]
        ws = _make_ws(data)
        mappings = {1: _make_col_mapping(1, "document"), 2: _make_col_mapping(2, "titre")}
        rows = extract_rows(ws, 2, mappings, [], "LOT_01")
        assert len(rows) == 1, f"Expected 1 row (early stop), got {len(rows)}"

    def test_source_row_tracking(self):
        """Extracted rows carry correct 1-based Excel row numbers."""
        data = [
            ("DOC_001", "Title A"),
            (None,),  # skipped
            ("DOC_002", "Title B"),
        ]
        ws = _make_ws(data)
        mappings = {1: _make_col_mapping(1, "document")}
        rows = extract_rows(ws, 5, mappings, [], "LOT_01")  # data_start=5
        assert rows[0]["source_row"] == 5, "First data row at row 5"
        assert rows[1]["source_row"] == 7, "Third data row at row 7 (row 6 was empty)"
