"""
Unit tests for Step 8: Row Quality Scoring.
Tests jansa_visasist.pipeline.quality_scoring.score_row_quality.
"""

import pytest
from jansa_visasist.pipeline.quality_scoring import score_row_quality
from jansa_visasist.models.log_entry import ImportLogEntry
from jansa_visasist.models.enums import Severity, RowQuality


def _make_log(severity: str, category: str) -> ImportLogEntry:
    return ImportLogEntry(
        log_id="1",
        sheet="LOT_01",
        row=10,
        column="document",
        severity=severity,
        category=category,
        raw_value="test",
        action_taken="test action",
    )


class TestScoreRowQuality:
    """Step 8 quality scoring from log entries."""

    def test_no_logs(self):
        """No logs → OK quality, empty details."""
        quality, details = score_row_quality([])
        assert quality == RowQuality.OK.value
        assert details == []

    def test_only_info_logs(self):
        """Only INFO logs → OK quality (INFO doesn't affect quality)."""
        logs = [_make_log(Severity.INFO.value, "missing_p_prefix")]
        quality, details = score_row_quality(logs)
        assert quality == RowQuality.OK.value
        assert details == []

    def test_one_warning(self):
        """One WARNING → WARNING quality, category in details."""
        logs = [_make_log(Severity.WARNING.value, "unparseable_document")]
        quality, details = score_row_quality(logs)
        assert quality == RowQuality.WARNING.value
        assert "unparseable_document" in details

    def test_one_error(self):
        """One ERROR → ERROR quality, category in details."""
        logs = [_make_log(Severity.ERROR.value, "corrupted_date")]
        quality, details = score_row_quality(logs)
        assert quality == RowQuality.ERROR.value
        assert "corrupted_date" in details

    def test_mixed_warning_and_error(self):
        """Mix of WARNING and ERROR → ERROR quality (ERROR dominates)."""
        logs = [
            _make_log(Severity.WARNING.value, "unknown_status"),
            _make_log(Severity.ERROR.value, "corrupted_date"),
        ]
        quality, details = score_row_quality(logs)
        assert quality == RowQuality.ERROR.value
        assert "corrupted_date" in details
        assert "unknown_status" in details

    def test_multiple_warnings_deduplicated(self):
        """Multiple WARNINGs with same category → deduplicated in details."""
        logs = [
            _make_log(Severity.WARNING.value, "unknown_status"),
            _make_log(Severity.WARNING.value, "unknown_status"),
            _make_log(Severity.WARNING.value, "date_out_of_range"),
        ]
        quality, details = score_row_quality(logs)
        assert quality == RowQuality.WARNING.value
        assert details.count("unknown_status") == 1, "Categories should be deduplicated"
        assert "date_out_of_range" in details
