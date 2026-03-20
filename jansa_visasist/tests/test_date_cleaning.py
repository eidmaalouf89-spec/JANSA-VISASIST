"""
Unit tests for Step 7b: Date Cleaning.
Tests jansa_visasist.pipeline.date_cleaning.clean_date.
"""

import pytest
from datetime import datetime, date, timedelta

from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.date_cleaning import clean_date, EXCEL_EPOCH
from jansa_visasist.config import (
    EXCEL_DATE_MIN, EXCEL_DATE_MAX,
    DATE_SANITY_MIN, DATE_SANITY_MAX,
)


@pytest.fixture
def ctx(tmp_path):
    return PipelineContext(input_path="test.xlsx", output_dir=str(tmp_path))


class TestCleanDate:
    """Step 7b date cleaning rules."""

    def test_none_returns_none(self, ctx):
        """GP2: None input returns None silently."""
        result = clean_date(None, "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        assert len(ctx.import_log) == 0

    def test_empty_string_returns_none(self, ctx):
        """GP2: Empty string returns None silently."""
        result = clean_date("", "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        assert len(ctx.import_log) == 0

    def test_whitespace_string_returns_none(self, ctx):
        """GP2: Whitespace-only string returns None."""
        result = clean_date("   ", "date_diffusion", "LOT_01", 10, ctx)
        assert result is None

    def test_valid_excel_serial(self, ctx):
        """Valid Excel serial number converts to ISO date."""
        serial = 45658
        expected_dt = EXCEL_EPOCH + timedelta(days=serial)
        expected_iso = expected_dt.strftime("%Y-%m-%d")
        result = clean_date(serial, "date_diffusion", "LOT_01", 10, ctx)
        assert result == expected_iso, f"Expected {expected_iso}, got {result}"

    def test_corrupted_serial_too_large(self, ctx):
        """Serial > EXCEL_DATE_MAX returns None + ERROR log."""
        result = clean_date(6705339, "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        errs = [e for e in ctx.import_log if e.category == "corrupted_date"]
        assert len(errs) == 1
        assert errs[0].severity == "ERROR"

    def test_corrupted_serial_negative(self, ctx):
        """Negative serial returns None + ERROR log."""
        result = clean_date(-1, "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        errs = [e for e in ctx.import_log if e.category == "corrupted_date"]
        assert len(errs) == 1

    def test_datetime_object(self, ctx):
        """datetime object converts to ISO string."""
        dt = datetime(2024, 6, 15)
        result = clean_date(dt, "date_diffusion", "LOT_01", 10, ctx)
        assert result == "2024-06-15"

    def test_date_object(self, ctx):
        """date object converts to ISO string."""
        d = date(2024, 6, 15)
        result = clean_date(d, "date_diffusion", "LOT_01", 10, ctx)
        assert result == "2024-06-15"

    def test_string_iso_format(self, ctx):
        """ISO date string parses correctly."""
        result = clean_date("2024-06-15", "date_diffusion", "LOT_01", 10, ctx)
        assert result == "2024-06-15"

    def test_string_french_format(self, ctx):
        """French dd/mm/yyyy date string parses correctly."""
        result = clean_date("15/06/2024", "date_diffusion", "LOT_01", 10, ctx)
        assert result == "2024-06-15"

    def test_unparseable_string(self, ctx):
        """Unparseable string returns None + ERROR log."""
        result = clean_date("not-a-date", "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        errs = [e for e in ctx.import_log if e.category == "corrupted_date"]
        assert len(errs) == 1
        assert errs[0].severity == "ERROR"

    def test_date_sanity_too_early(self, ctx):
        """Date before DATE_SANITY_MIN (2020-01-01) returns None + WARNING."""
        # Serial that resolves to 2019-12-31
        target_date = datetime.strptime("2019-12-31", "%Y-%m-%d")
        serial = (target_date - EXCEL_EPOCH).days
        result = clean_date(serial, "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "date_out_of_range"]
        assert len(warns) == 1
        assert warns[0].severity == "WARNING"

    def test_date_sanity_too_late(self, ctx):
        """Date after DATE_SANITY_MAX (2030-12-31) returns None + WARNING."""
        target_date = datetime.strptime("2031-01-01", "%Y-%m-%d")
        serial = (target_date - EXCEL_EPOCH).days
        result = clean_date(serial, "date_diffusion", "LOT_01", 10, ctx)
        assert result is None
        warns = [e for e in ctx.import_log if e.category == "date_out_of_range"]
        assert len(warns) == 1

    def test_date_at_sanity_min_boundary(self, ctx):
        """Date exactly at DATE_SANITY_MIN passes."""
        result = clean_date("2020-01-01", "date_diffusion", "LOT_01", 10, ctx)
        assert result == "2020-01-01"

    def test_date_at_sanity_max_boundary(self, ctx):
        """Date exactly at DATE_SANITY_MAX passes."""
        result = clean_date("2030-12-31", "date_diffusion", "LOT_01", 10, ctx)
        assert result == "2030-12-31"
