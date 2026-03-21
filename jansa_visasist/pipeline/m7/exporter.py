"""
Module 7 — Report Export.

Exports session reports to JSON and CSV files.
"""

import csv
import io
import json
import logging
import os
import tempfile
from typing import List

from jansa_visasist.pipeline.m7.schemas import SessionReport

logger = logging.getLogger(__name__)


def _atomic_write(path: str, data: bytes) -> None:
    """Write data to file atomically via temp file + os.replace."""
    dir_name = os.path.dirname(path)
    os.makedirs(dir_name, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        os.write(fd, data)
        os.close(fd)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def export_json(report: SessionReport, output_dir: str) -> str:
    """
    Export report as JSON file.

    Args:
        report: SessionReport to export.
        output_dir: Directory to write the file to.

    Returns:
        File path of the exported JSON.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{report.session_id}_report.json"
    filepath = os.path.join(output_dir, filename)
    json_bytes = json.dumps(report.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")
    _atomic_write(filepath, json_bytes)
    logger.info("JSON report exported: %s", filepath)
    return filepath


def export_csv(report: SessionReport, output_dir: str) -> str:
    """
    Export report decisions as CSV file.

    One row per item: row_id, document, source_sheet, category,
    priority_score, decision_type, visa_value, comment, decided_at,
    decision_source.

    Args:
        report: SessionReport to export.
        output_dir: Directory to write the file to.

    Returns:
        File path of the exported CSV.
    """
    os.makedirs(output_dir, exist_ok=True)
    filename = f"{report.session_id}_report.csv"
    filepath = os.path.join(output_dir, filename)

    headers = [
        "row_id",
        "document",
        "source_sheet",
        "category",
        "priority_score",
        "decision_type",
        "visa_value",
        "comment",
        "decided_at",
        "decision_source",
    ]

    # Build CSV in memory, then atomic write
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(headers)

    for d in report.decisions:
        writer.writerow([
            d.get("row_id", ""),
            d.get("document", ""),
            d.get("source_sheet", ""),
            d.get("category", ""),
            d.get("priority_score", ""),
            d.get("decision_type", ""),
            d.get("visa_value", ""),
            d.get("comment", ""),
            d.get("decided_at", ""),
            d.get("decision_source", ""),
        ])

    csv_bytes = buffer.getvalue().encode("utf-8")
    _atomic_write(filepath, csv_bytes)
    logger.info("CSV report exported: %s", filepath)
    return filepath
