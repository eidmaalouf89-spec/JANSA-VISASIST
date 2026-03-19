#!/usr/bin/env python3
"""
Generate golden reference files from a known-good pipeline run.

Usage (from project root):
    python -m jansa_visasist.tests.generate_golden

Or with a custom workbook path:
    python -m jansa_visasist.tests.generate_golden --input path/to/GrandFichier.xlsx

Outputs are written to jansa_visasist/tests/golden/ and consist of:
    golden_snapshot.json   — compact semantic snapshot (row count, schema,
                             per-sheet counts, quality distribution, sample
                             rows, and a per-row fingerprint digest)
    golden_validation.json — full validation report from Step 10
    golden_log_summary.json — log severity counts and category counts

These files are NOT full data copies.  They capture just enough to detect
regressions without being brittle to harmless formatting differences.
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
from collections import Counter

# Allow running as `python -m jansa_visasist.tests.generate_golden`
_TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
_PACKAGE_DIR = os.path.dirname(_TESTS_DIR)
_PROJECT_ROOT = os.path.dirname(_PACKAGE_DIR)
sys.path.insert(0, _PROJECT_ROOT)

from jansa_visasist.context import PipelineContext
from jansa_visasist.main import run_pipeline


GOLDEN_DIR = os.path.join(_TESTS_DIR, "golden")


def _row_fingerprint(row: dict) -> str:
    """
    Deterministic fingerprint of a row based on its identity + key fields.
    Used for regression detection without storing full data.
    """
    parts = [
        str(row.get("row_id", "")),
        str(row.get("source_sheet", "")),
        str(row.get("source_row", "")),
        str(row.get("document", "")),
        str(row.get("document_raw", "")),
        str(row.get("ind", "")),
        str(row.get("visa_global", "")),
        str(row.get("row_quality", "")),
        str(row.get("date_diffusion", "")),
        str(row.get("date_reception", "")),
        str(row.get("titre", ""))[:80],  # truncate long titles
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def generate(input_path: str) -> None:
    """Run the pipeline and write golden reference files."""
    os.makedirs(GOLDEN_DIR, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmpdir:
        ctx = PipelineContext(input_path=input_path, output_dir=tmpdir)
        exit_code = run_pipeline(ctx)
        assert exit_code == 0, f"Pipeline failed with exit code {exit_code}"

        # ── Load outputs ──
        with open(os.path.join(tmpdir, "master_dataset.json"), encoding="utf-8") as f:
            master = json.load(f)

        with open(os.path.join(tmpdir, "validation_report.json"), encoding="utf-8") as f:
            validation = json.load(f)

        with open(os.path.join(tmpdir, "import_log.json"), encoding="utf-8") as f:
            import_log = json.load(f)

        with open(os.path.join(tmpdir, "header_mapping_report.json"), encoding="utf-8") as f:
            header_report = json.load(f)

    # ── 1. Build compact snapshot ──
    # Schema: sorted column names from first row
    schema = sorted(master[0].keys()) if master else []

    # Per-sheet row counts
    sheet_counts = Counter(row["source_sheet"] for row in master)

    # Quality distribution
    quality_dist = Counter(row["row_quality"] for row in master)

    # Visa global distribution
    visa_dist = Counter(
        row.get("visa_global") for row in master
    )
    # Convert None key to string for JSON
    visa_dist = {str(k): v for k, v in visa_dist.items()}

    # Per-row fingerprints (ordered by row_id for stability)
    fingerprints = [
        {"row_id": row["row_id"], "fp": _row_fingerprint(row)}
        for row in master
    ]

    # Sample rows: first, last, and every 500th
    sample_indices = sorted(set(
        [0, len(master) - 1] + list(range(0, len(master), 500))
    ))
    sample_rows = []
    for idx in sample_indices:
        if 0 <= idx < len(master):
            row = master[idx]
            sample_rows.append({
                "index": idx,
                "row_id": row.get("row_id"),
                "source_sheet": row.get("source_sheet"),
                "source_row": row.get("source_row"),
                "document": row.get("document"),
                "document_raw": row.get("document_raw"),
                "ind": row.get("ind"),
                "visa_global": row.get("visa_global"),
                "row_quality": row.get("row_quality"),
                "row_quality_details": row.get("row_quality_details"),
                "date_diffusion": row.get("date_diffusion"),
                "assigned_approvers": row.get("assigned_approvers"),
            })

    # Header mapping summary
    header_summary = []
    for report in header_report:
        header_summary.append({
            "sheet": report["sheet"],
            "header_row_detected": report["header_row_detected"],
            "core_columns_mapped": report["core_columns_mapped"],
            "mapping_confidence": report["mapping_confidence"],
            "approvers_detected": report["approvers_detected"],
        })

    snapshot = {
        "_generator": "jansa_visasist.tests.generate_golden",
        "_description": "Compact semantic snapshot for regression detection",
        "total_rows": len(master),
        "schema": schema,
        "sheet_counts": dict(sorted(sheet_counts.items())),
        "quality_distribution": dict(sorted(quality_dist.items())),
        "visa_distribution": dict(sorted(visa_dist.items())),
        "header_mapping_summary": header_summary,
        "sample_rows": sample_rows,
        "row_fingerprints": fingerprints,
    }

    # ── 2. Log summary ──
    severity_counts = Counter(e["severity"] for e in import_log)
    category_counts = Counter(e["category"] for e in import_log)

    log_summary = {
        "total_entries": len(import_log),
        "severity_counts": dict(sorted(severity_counts.items())),
        "category_counts": dict(sorted(category_counts.items())),
    }

    # ── Write files ──
    snapshot_path = os.path.join(GOLDEN_DIR, "golden_snapshot.json")
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    print(f"Wrote {snapshot_path}")

    validation_path = os.path.join(GOLDEN_DIR, "golden_validation.json")
    with open(validation_path, "w", encoding="utf-8") as f:
        json.dump(validation, f, ensure_ascii=False, indent=2)
    print(f"Wrote {validation_path}")

    log_path = os.path.join(GOLDEN_DIR, "golden_log_summary.json")
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_summary, f, ensure_ascii=False, indent=2)
    print(f"Wrote {log_path}")

    # Summary
    print(f"\nGolden files generated successfully:")
    print(f"  Total rows:  {len(master)}")
    print(f"  Sheets:      {len(sheet_counts)}")
    print(f"  Schema cols: {len(schema)}")
    print(f"  Quality:     {dict(quality_dist)}")
    print(f"  Log entries: {len(import_log)}")


def main():
    parser = argparse.ArgumentParser(description="Generate golden reference files")
    default_input = os.path.join(_PROJECT_ROOT, "source", "GrandFichier.xlsx")
    parser.add_argument(
        "--input", default=default_input,
        help=f"Path to GrandFichier (default: {default_input})"
    )
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"ERROR: File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    generate(args.input)


if __name__ == "__main__":
    main()
