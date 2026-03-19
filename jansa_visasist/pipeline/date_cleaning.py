"""
Step 7b: Date Cleaning
Convert raw date values (Excel serials, strings, datetime) to ISO format.

Every successfully parsed date is passed through a project-date sanity
check before being returned.  Dates outside [DATE_SANITY_MIN, DATE_SANITY_MAX]
are treated as corrupted: set to None, raw value preserved, WARNING logged.
"""

from datetime import datetime, timedelta, date as date_type
from typing import Optional

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity
from ..config import (
    EXCEL_DATE_MIN,
    EXCEL_DATE_MAX,
    DATE_FORMATS,
    DATE_SANITY_MIN,
    DATE_SANITY_MAX,
)

# Excel epoch: day 1 = 1900-01-01, but Excel has a bug treating 1900 as leap year
EXCEL_EPOCH = datetime(1899, 12, 30)

# Pre-parse sanity bounds once at import time
_SANITY_MIN = datetime.strptime(DATE_SANITY_MIN, "%Y-%m-%d").date()
_SANITY_MAX = datetime.strptime(DATE_SANITY_MAX, "%Y-%m-%d").date()


def _check_date_sanity(
    iso_str: str,
    raw_value,
    field_name: str,
    sheet_name: str,
    source_row: int,
    ctx: PipelineContext,
) -> Optional[str]:
    """
    Gate that runs after every successful parse.
    If the parsed date falls outside the project sanity range, log a
    WARNING with full traceability and return None.
    Otherwise return the ISO string unchanged.
    """
    try:
        parsed_date = datetime.strptime(iso_str, "%Y-%m-%d").date()
    except ValueError:
        # Shouldn't happen — we just formatted it — but be defensive
        return iso_str

    if parsed_date < _SANITY_MIN or parsed_date > _SANITY_MAX:
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column=field_name,
            severity=Severity.WARNING.value,
            category="date_out_of_range",
            raw_value=str(raw_value),
            action_taken=(
                f"Parsed date {iso_str} outside project range "
                f"[{DATE_SANITY_MIN}, {DATE_SANITY_MAX}] — set to null"
            ),
        ))
        return None

    return iso_str


def clean_date(
    raw_value,
    field_name: str,
    sheet_name: str,
    source_row: int,
    ctx: PipelineContext,
) -> Optional[str]:
    """
    Clean a raw date value and return ISO date string or None.
    Emits log entries for any issues.
    Every successful parse is sanity-checked before returning.
    """
    # Blank/None → None (GP2)
    if raw_value is None:
        return None

    if isinstance(raw_value, str) and raw_value.strip() == "":
        return None

    # If already a datetime object
    if isinstance(raw_value, datetime):
        iso = raw_value.strftime("%Y-%m-%d")
        return _check_date_sanity(iso, raw_value, field_name, sheet_name, source_row, ctx)

    # If a date object (not datetime)
    if isinstance(raw_value, date_type):
        iso = raw_value.strftime("%Y-%m-%d")
        return _check_date_sanity(iso, raw_value, field_name, sheet_name, source_row, ctx)

    # If numeric (Excel serial)
    if isinstance(raw_value, (int, float)):
        serial = raw_value
        if EXCEL_DATE_MIN <= serial <= EXCEL_DATE_MAX:
            try:
                dt = EXCEL_EPOCH + timedelta(days=int(serial))
                iso = dt.strftime("%Y-%m-%d")
                return _check_date_sanity(iso, raw_value, field_name, sheet_name, source_row, ctx)
            except (ValueError, OverflowError):
                ctx.log(ImportLogEntry(
                    log_id=ctx.next_log_id(),
                    sheet=sheet_name,
                    row=source_row,
                    column=field_name,
                    severity=Severity.ERROR.value,
                    category="corrupted_date",
                    raw_value=str(raw_value),
                    action_taken=f"Date serial {raw_value} conversion failed — set to null",
                ))
                return None
        else:
            ctx.log(ImportLogEntry(
                log_id=ctx.next_log_id(),
                sheet=sheet_name,
                row=source_row,
                column=field_name,
                severity=Severity.ERROR.value,
                category="corrupted_date",
                raw_value=str(raw_value),
                action_taken=f"Date serial {raw_value} out of valid range [{EXCEL_DATE_MIN}, {EXCEL_DATE_MAX}] — set to null",
            ))
            return None

    # If string → attempt parse
    if isinstance(raw_value, str):
        val = raw_value.strip()
        for fmt in DATE_FORMATS:
            try:
                dt = datetime.strptime(val, fmt)
                iso = dt.strftime("%Y-%m-%d")
                return _check_date_sanity(iso, raw_value, field_name, sheet_name, source_row, ctx)
            except ValueError:
                continue

        # No format matched
        ctx.log(ImportLogEntry(
            log_id=ctx.next_log_id(),
            sheet=sheet_name,
            row=source_row,
            column=field_name,
            severity=Severity.ERROR.value,
            category="corrupted_date",
            raw_value=str(raw_value),
            action_taken=f"Date string '{raw_value}' could not be parsed — set to null",
        ))
        return None

    # All other types
    ctx.log(ImportLogEntry(
        log_id=ctx.next_log_id(),
        sheet=sheet_name,
        row=source_row,
        column=field_name,
        severity=Severity.ERROR.value,
        category="corrupted_date",
        raw_value=str(raw_value),
        action_taken=f"Unexpected date type {type(raw_value).__name__} — set to null",
    ))
    return None
