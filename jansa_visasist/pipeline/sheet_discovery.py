"""
Step 1: Sheet Discovery
Enumerate and validate sheets from the GrandFichier workbook.
"""

import logging
from typing import List, Tuple

import openpyxl

from ..context import PipelineContext
from ..models.log_entry import ImportLogEntry
from ..models.enums import Severity

logger = logging.getLogger(__name__)


def discover_sheets(ctx: PipelineContext) -> List[Tuple[str, object]]:
    """
    Open the workbook and return a list of (sheet_name, worksheet) tuples.
    Sheets that fail to load are logged and skipped.
    """
    try:
        wb = openpyxl.load_workbook(
            ctx.input_path, read_only=True, data_only=True
        )
    except Exception as e:
        logger.error("Cannot open workbook: %s", e)
        raise SystemExit(1)

    sheets: List[Tuple[str, object]] = []
    for sheet_name in wb.sheetnames:
        try:
            ws = wb[sheet_name]
            sheets.append((sheet_name, ws))
        except Exception as e:
            ctx.log_sheet_level(ImportLogEntry(
                log_id=ctx.next_log_id(),
                sheet=sheet_name,
                row=None,
                column=None,
                severity=Severity.ERROR.value,
                category="sheet_load_error",
                raw_value=None,
                action_taken=f"Sheet skipped — load error: {e}",
            ))
            ctx.sheets_skipped += 1

    return sheets
