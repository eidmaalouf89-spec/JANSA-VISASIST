"""Tests for Step 7b: Date Cleaning (with project-date sanity gate)."""

import pytest
from datetime import datetime, date

from jansa_visasist.context import PipelineContext
from jansa_visasist.pipeline.date_cleaning import clean_date


@pytest.fixture
def ctx():
    return PipelineContext(input_path="test.xlsx", output_dir="/tmp/test")


# ── Helper ──────────────────────────────────────────────────────

def _logs_with_category(ctx, category):
    """Return log entries matching a given category from the current row."""
    return [e for e in ctx.end_row() if e.category == category]


# ── Basic parsing (unchanged behaviour) ─────────────────────────

class TestDateParsingBasics:
    """Verify parsing paths still produce correct results."""

    def test_none_returns_none(self, ctx):
        ctx.begin_row()
        assert clean_date(None, "d", "S", 1, ctx) is None

    def test_empty_string_returns_none(self, ctx):
        ctx.begin_row()
        assert clean_date("  ", "d", "S", 1, ctx) is None

    def test_valid_datetime_object(self, ctx):
        ctx.begin_row()
        assert clean_date(datetime(2024, 6, 15), "d", "S", 1, ctx) == "2024-06-15"

    def test_valid_date_object(self, ctx):
        ctx.begin_row()
        assert clean_date(date(2025, 3, 1), "d", "S", 1, ctx) == "2025-03-01"

    def test_valid_iso_string(self, ctx):
        ctx.begin_row()
        assert clean_date("2024-01-15", "d", "S", 1, ctx) == "2024-01-15"

    def test_french_date_string(self, ctx):
        ctx.begin_row()
        assert clean_date("15/01/2024", "d", "S", 1, ctx) == "2024-01-15"

    def test_valid_excel_serial(self, ctx):
        """Serial 45000 ≈ 2023-03-15 — within project range."""
        ctx.begin_row()
        result = clean_date(45000, "d", "S", 1, ctx)
        assert result is not None
        assert result.startswith("2023")

    def test_unparseable_string(self, ctx):
        ctx.begin_row()
        result = clean_date("not-a-date", "d", "S", 1, ctx)
        assert result is None
        assert len(_logs_with_category(ctx, "corrupted_date")) == 1


# ── Serial range errors (kept from V1) ──────────────────────────

class TestSerialRangeErrors:
    """Serials outside [1, 2958465] produce ERROR + null."""

    def test_serial_too_high(self, ctx):
        ctx.begin_row()
        assert clean_date(6_000_000, "d", "S", 1, ctx) is None
        assert len(_logs_with_category(ctx, "corrupted_date")) == 1

    def test_serial_negative(self, ctx):
        ctx.begin_row()
        assert clean_date(-5, "d", "S", 1, ctx) is None
        assert len(_logs_with_category(ctx, "corrupted_date")) == 1


# ── NEW: Project-date sanity gate ────────────────────────────────

class TestDateSanityGate:
    """
    Dates that parse successfully but fall outside [2020-01-01, 2030-12-31]
    must be nullified with a WARNING log preserving the raw value.
    """

    # --- Via Excel serial ---

    def test_small_serial_produces_1900_date_nullified(self, ctx):
        """Serial 6 → 1900-01-05, outside project range → null + WARNING."""
        ctx.begin_row()
        result = clean_date(6, "date_diffusion", "Sheet1", 10, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1
        assert "1900" in logs[0].action_taken
        assert logs[0].raw_value == "6"  # raw value preserved

    def test_serial_15_nullified(self, ctx):
        """Serial 15 → 1900-01-14, outside project range → null."""
        ctx.begin_row()
        result = clean_date(15, "date_reception", "S", 5, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1

    def test_serial_21400_gives_1958_nullified(self, ctx):
        """Serial ~21400 → 1958-era date → null + WARNING."""
        ctx.begin_row()
        result = clean_date(21400, "date_diffusion", "Sheet1", 10, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1
        assert "1958" in logs[0].action_taken

    # --- Via datetime object ---

    def test_datetime_1958_nullified(self, ctx):
        """datetime(1958, 8, 4) → outside range → null + WARNING."""
        ctx.begin_row()
        result = clean_date(datetime(1958, 8, 4), "date_diffusion", "S", 1, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1

    def test_datetime_2019_nullified(self, ctx):
        """datetime(2019, 12, 31) → one day before range → null."""
        ctx.begin_row()
        assert clean_date(datetime(2019, 12, 31), "d", "S", 1, ctx) is None

    def test_datetime_2031_nullified(self, ctx):
        """datetime(2031, 1, 1) → one day after range → null."""
        ctx.begin_row()
        assert clean_date(datetime(2031, 1, 1), "d", "S", 1, ctx) is None

    # --- Via string parse ---

    def test_string_1900_01_15_nullified(self, ctx):
        """ISO string '1900-01-15' → parsed but outside range → null."""
        ctx.begin_row()
        result = clean_date("1900-01-15", "date_diffusion", "Sheet1", 10, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1
        assert logs[0].raw_value == "1900-01-15"

    def test_string_0205_10_23_nullified(self, ctx):
        """'0205-10-23' → year 205 → null + WARNING."""
        ctx.begin_row()
        result = clean_date("0205-10-23", "date_diffusion", "Sheet1", 10, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1
        assert "0205" in logs[0].action_taken

    def test_string_1015_06_18_nullified(self, ctx):
        """'1015-06-18' → year 1015 → null + WARNING."""
        ctx.begin_row()
        result = clean_date("1015-06-18", "date_diffusion", "Sheet1", 10, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1

    # --- Boundary tests ---

    def test_boundary_min_exact(self, ctx):
        """2020-01-01 is the minimum valid date — passes."""
        ctx.begin_row()
        assert clean_date("2020-01-01", "d", "S", 1, ctx) == "2020-01-01"

    def test_boundary_max_exact(self, ctx):
        """2030-12-31 is the maximum valid date — passes."""
        ctx.begin_row()
        assert clean_date("2030-12-31", "d", "S", 1, ctx) == "2030-12-31"

    def test_boundary_just_inside(self, ctx):
        """2025-06-15 — clearly in range — passes."""
        ctx.begin_row()
        assert clean_date("2025-06-15", "d", "S", 1, ctx) == "2025-06-15"

    # --- Verify raw value preserved in log ---

    def test_raw_value_preserved_for_serial(self, ctx):
        """The original raw serial appears in the log entry."""
        ctx.begin_row()
        clean_date(6, "date_diffusion", "Sheet1", 42, ctx)
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert logs[0].raw_value == "6"
        assert logs[0].sheet == "Sheet1"
        assert logs[0].row == 42

    def test_raw_value_preserved_for_string(self, ctx):
        """The original raw string appears in the log entry."""
        ctx.begin_row()
        clean_date("15/01/1990", "date_reception", "LOT 3", 99, ctx)
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert logs[0].raw_value == "15/01/1990"
        assert logs[0].column == "date_reception"

    # --- Valid dates still work ---

    def test_valid_serial_44927(self, ctx):
        """Serial 44927 → 2023-01-01 → in range → returned."""
        ctx.begin_row()
        result = clean_date(44927, "d", "S", 1, ctx)
        assert result == "2023-01-01"

    def test_valid_french_string(self, ctx):
        ctx.begin_row()
        assert clean_date("01/06/2025", "d", "S", 1, ctx) == "2025-06-01"


# ── Edge: serial boundary still works for range errors ───────────

class TestSerialBoundaryStillWorks:
    """Serial = 1 and serial = 2958465 parse but may fail sanity."""

    def test_serial_1_parses_but_fails_sanity(self, ctx):
        """Serial 1 → 1899-12-31 → outside range → null."""
        ctx.begin_row()
        result = clean_date(1, "d", "S", 1, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1

    def test_serial_max_parses_but_fails_sanity(self, ctx):
        """Serial 2958465 → year 9999 → outside range → null."""
        ctx.begin_row()
        result = clean_date(2958465, "d", "S", 1, ctx)
        assert result is None
        logs = _logs_with_category(ctx, "date_out_of_range")
        assert len(logs) == 1
