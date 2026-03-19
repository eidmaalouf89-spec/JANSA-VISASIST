"""Tests for Step 8: Row Quality Scoring."""

import pytest

from jansa_visasist.models.log_entry import ImportLogEntry
from jansa_visasist.pipeline.quality_scoring import score_row_quality


def _make_log(severity, category):
    return ImportLogEntry(
        log_id="1", sheet="S", row=1, column="col",
        severity=severity, category=category,
        raw_value=None, action_taken="test"
    )


class TestQualityScoring:
    """Test row quality scoring from accumulated logs."""

    def test_clean_row_is_ok(self):
        """No logs → OK quality."""
        quality, details = score_row_quality([])
        assert quality == "OK"
        assert details == []

    def test_info_only_is_ok(self):
        """INFO-only logs → OK (INFO doesn't affect quality)."""
        logs = [_make_log("INFO", "trailing_punctuation")]
        quality, details = score_row_quality(logs)
        assert quality == "OK"
        assert details == []

    def test_warning_gives_warning(self):
        """WARNING log → WARNING quality."""
        logs = [_make_log("WARNING", "unparseable_document")]
        quality, details = score_row_quality(logs)
        assert quality == "WARNING"
        assert "unparseable_document" in details

    def test_error_gives_error(self):
        """ERROR log → ERROR quality."""
        logs = [_make_log("ERROR", "corrupted_date")]
        quality, details = score_row_quality(logs)
        assert quality == "ERROR"
        assert "corrupted_date" in details

    def test_error_takes_precedence(self):
        """Row with both WARNING and ERROR → ERROR."""
        logs = [
            _make_log("WARNING", "unknown_status"),
            _make_log("ERROR", "corrupted_date"),
        ]
        quality, details = score_row_quality(logs)
        assert quality == "ERROR"
        assert "corrupted_date" in details
        assert "unknown_status" in details

    def test_details_deduplicated(self):
        """Duplicate categories appear only once in details."""
        logs = [
            _make_log("WARNING", "unknown_status"),
            _make_log("WARNING", "unknown_status"),
        ]
        quality, details = score_row_quality(logs)
        assert quality == "WARNING"
        assert details.count("unknown_status") == 1

    def test_bad_doc_valid_data_is_warning(self):
        """Row with bad document but valid other data → WARNING not ERROR."""
        logs = [_make_log("WARNING", "unparseable_document")]
        quality, details = score_row_quality(logs)
        assert quality == "WARNING"

    def test_corrupted_date_and_unknown_status_is_error(self):
        """Row with corrupted date AND unknown status → ERROR."""
        logs = [
            _make_log("ERROR", "corrupted_date"),
            _make_log("WARNING", "unknown_status"),
        ]
        quality, details = score_row_quality(logs)
        assert quality == "ERROR"
