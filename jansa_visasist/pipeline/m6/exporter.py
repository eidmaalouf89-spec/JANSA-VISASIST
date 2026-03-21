"""
Module 6 — C12: CSV Export.

Writes filtered results to a CSV file.
Returns export_metadata: {export_path, format, row_count}.
"""

import csv
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("jansa.m6.exporter")


def export_to_csv(
    items: List[Dict[str, Any]],
    export_dir: str,
    filename: Optional[str] = None,
) -> Dict[str, Any]:
    """Write items to CSV and return export_metadata.

    Args:
        items: List of dicts to export.
        export_dir: Directory to write the CSV file.
        filename: Optional filename. If None, auto-generated.

    Returns:
        export_metadata dict: {export_path, format, row_count}
    """
    if not filename:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"m6_export_{ts}.csv"

    os.makedirs(export_dir, exist_ok=True)
    export_path = os.path.join(export_dir, filename)

    if not items:
        # Write empty CSV with just a header note
        with open(export_path, "w", newline="", encoding="utf-8") as f:
            f.write("# No matching items\n")
        return {
            "export_path": export_path,
            "format": "csv",
            "row_count": 0,
        }

    # Collect all keys from items
    fieldnames = []
    seen = set()
    for item in items:
        for k in item.keys():
            if k not in seen:
                fieldnames.append(k)
                seen.add(k)

    with open(export_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for item in items:
            writer.writerow(item)

    logger.info("Exported %d rows to %s", len(items), export_path)

    return {
        "export_path": export_path,
        "format": "csv",
        "row_count": len(items),
    }
