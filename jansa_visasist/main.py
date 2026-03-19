"""
JANSA VISASIST — Module 1 Entry Point
Parse args → build context → run pipeline → write outputs.
"""

import argparse
import logging
import os
import sys
import time
from typing import List, Dict

import pandas as pd

from .context import PipelineContext
from .config import CANONICAL_APPROVERS, APPROVER_OUTPUT_SUFFIXES, MAPPABLE_CORE_KEYS
from .models.log_entry import ImportLogEntry
from .models.header_report import HeaderMappingReport
from .models.enums import Severity, MappingConfidence

from .pipeline.sheet_discovery import discover_sheets
from .pipeline.header_detection import detect_header_row
from .pipeline.column_mapping import map_columns
from .pipeline.approver_detection import detect_approver_blocks
from .pipeline.row_extraction import extract_rows
from .pipeline.doc_normalization import normalize_document
from .pipeline.doc_validation import validate_document
from .pipeline.date_cleaning import clean_date
from .pipeline.status_cleaning import normalize_visa_global, normalize_approver_statut
from .pipeline.text_cleaning import clean_text, reset_text_cache
from .pipeline.quality_scoring import score_row_quality
from .pipeline.schema_merge import merge_sheets
from .pipeline.validation_checks import run_validation_checks

from .outputs.json_writer import (
    write_master_dataset_json,
    write_import_log_json,
    write_header_mapping_report_json,
    write_validation_report_json,
)
from .outputs.csv_writer import write_master_dataset_csv, write_import_log_csv
from .outputs.excel_writer import write_master_dataset_xlsx

logger = logging.getLogger(__name__)


def _compute_mapping_confidence(col_mappings, approver_blocks) -> str:
    """Compute per-sheet mapping confidence from column match levels."""
    has_fuzzy = False
    has_unmatched = False
    for mapping in col_mappings.values():
        if mapping.match_level == 3:
            has_fuzzy = True
        elif mapping.match_level == 4:
            has_unmatched = True

    if has_unmatched:
        return MappingConfidence.LOW.value
    elif has_fuzzy:
        return MappingConfidence.MEDIUM.value
    else:
        return MappingConfidence.HIGH.value


def _build_header_report(
    sheet_name: str,
    header_row: int,
    col_mappings: dict,
    approver_blocks: list,
) -> HeaderMappingReport:
    """Build a HeaderMappingReport for a sheet."""
    mapped_core = []
    unmapped_cols = []
    for mapping in col_mappings.values():
        if mapping.match_level < 4 and mapping.canonical_key in MAPPABLE_CORE_KEYS:
            mapped_core.append(mapping.canonical_key)
        elif mapping.match_level == 4:
            unmapped_cols.append(mapping.raw_header)

    missing_core = [k for k in MAPPABLE_CORE_KEYS if k not in mapped_core]

    return HeaderMappingReport(
        sheet=sheet_name,
        header_row_detected=header_row,
        core_columns_mapped=len(mapped_core),
        core_columns_missing=sorted(missing_core),
        approvers_detected=[b.canonical_key for b in approver_blocks],
        mapping_confidence=_compute_mapping_confidence(col_mappings, approver_blocks),
        unmapped_columns=unmapped_cols,
    )


def run_pipeline(ctx: PipelineContext) -> int:
    """
    Execute the full Module 1 pipeline.
    Returns exit code: 0 = success, 1 = fatal error.
    """
    start_time = time.time()

    # Reset caches from any prior run in the same process
    reset_text_cache()

    # ─── Phase A: Sheet-Level Preprocessing ───
    logger.info("Phase A: Sheet Discovery")
    sheets = discover_sheets(ctx)
    logger.info("Found %d sheets", len(sheets))

    sheet_dfs: List[pd.DataFrame] = []

    for sheet_idx, (sheet_name, ws) in enumerate(sheets):
        logger.info("Processing sheet %d/%d: '%s'", sheet_idx + 1, len(sheets), sheet_name)

        # Step 2: Header Detection
        anchor = detect_header_row(ws, sheet_name, ctx)
        if anchor is None:
            continue  # Sheet skipped (logged in detect_header_row)

        # Step 4: Column Mapping
        col_mappings = map_columns(ws, anchor.header_row, sheet_name, ctx)

        # Step 5: Approver Block Detection
        approver_blocks = detect_approver_blocks(
            ws, anchor.approver_row, col_mappings, sheet_name, ctx
        )
        assigned_approvers = [b.canonical_key for b in approver_blocks]

        # Step 6: Row Extraction (permissive)
        raw_rows = extract_rows(ws, anchor.data_start, col_mappings, approver_blocks, sheet_name)

        # ─── Phase B: Row-Level Normalization & Cleaning ───
        cleaned_rows: List[Dict] = []

        total_raw = len(raw_rows)
        for row_num, row_data in enumerate(raw_rows):
            # Batch progress logging (every 200 rows, not per-row)
            if row_num > 0 and row_num % 200 == 0:
                logger.info("Sheet '%s': cleaning row %d/%d", sheet_name, row_num, total_raw)

            ctx.begin_row()
            source_row = row_data["source_row"]
            cleaned: Dict = {}

            # Preserve raw document
            doc_raw = row_data.get("document")
            cleaned["document_raw"] = doc_raw if doc_raw is not None else None

            # Step 3: Normalize document reference
            doc_normalized = normalize_document(doc_raw, sheet_name, source_row, ctx)

            # Step 7a: Validate document structure
            doc_validated = validate_document(doc_normalized, doc_raw, sheet_name, source_row, ctx)
            cleaned["document"] = doc_validated

            # Step 7b: Clean date fields
            date_fields = [
                ("date_diffusion", "date_diffusion"),
                ("date_reception", "date_reception"),
                ("date_contractuelle_visa", "date_contractuelle_visa"),
            ]
            for field_key, canonical_key in date_fields:
                raw_val = row_data.get(canonical_key)
                cleaned[f"{canonical_key}_raw"] = str(raw_val) if raw_val is not None else None
                cleaned[canonical_key] = clean_date(raw_val, canonical_key, sheet_name, source_row, ctx)

            # Step 7c: Normalize visa_global
            visa_raw = row_data.get("visa_global")
            cleaned["visa_global_raw"] = str(visa_raw) if visa_raw is not None else None
            cleaned["visa_global"] = normalize_visa_global(visa_raw, sheet_name, source_row, ctx)

            # Step 7d: Normalize all approver statuts
            for block in approver_blocks:
                prefix = block.canonical_key

                # Date
                date_raw = row_data.get(f"{prefix}_date_src")
                cleaned[f"{prefix}_date_raw"] = str(date_raw) if date_raw is not None else None
                cleaned[f"{prefix}_date"] = clean_date(
                    date_raw, f"{prefix}_date", sheet_name, source_row, ctx
                )

                # N° (bordereau number — text field)
                n_raw = row_data.get(f"{prefix}_n_src")
                cleaned[f"{prefix}_n"] = clean_text(n_raw)

                # Statut
                statut_raw = row_data.get(f"{prefix}_statut_src")
                cleaned[f"{prefix}_statut_raw"] = str(statut_raw) if statut_raw is not None else None
                cleaned[f"{prefix}_statut"] = normalize_approver_statut(
                    statut_raw, prefix, sheet_name, source_row, ctx
                )

            # Step 7e: Clean text fields
            cleaned["titre"] = clean_text(row_data.get("titre"))
            cleaned["observations"] = clean_text(row_data.get("observations"))

            # Simple text fields (cleaned with basic normalization)
            for simple_field in ["lot", "type_doc", "niv", "zone", "n_doc",
                                 "type_format", "ancien", "n_bdx", "non_recu_papier"]:
                cleaned[simple_field] = clean_text(row_data.get(simple_field))

            # IND field: uppercase, trimmed, null if blank
            ind_raw = row_data.get("ind")
            cleaned["ind_raw"] = str(ind_raw) if ind_raw is not None else None
            ind_cleaned = clean_text(ind_raw)
            if ind_cleaned is not None:
                ind_cleaned = ind_cleaned.upper()
            cleaned["ind"] = ind_cleaned

            # Traceability fields
            cleaned["source_sheet"] = sheet_name
            cleaned["source_row"] = source_row
            cleaned["row_id"] = f"{sheet_idx}_{source_row}"
            cleaned["assigned_approvers"] = assigned_approvers

            # Step 8: Score row quality
            row_logs = ctx.end_row()
            quality, details = score_row_quality(row_logs)
            cleaned["row_quality"] = quality
            cleaned["row_quality_details"] = details

            cleaned_rows.append(cleaned)

        # ─── Phase C: Sheet Assembly ───
        if cleaned_rows:
            sheet_df = pd.DataFrame(cleaned_rows)
            sheet_dfs.append(sheet_df)
            ctx.sheet_row_counts[sheet_name] = len(cleaned_rows)
            ctx.total_rows += len(cleaned_rows)
        else:
            ctx.sheet_row_counts[sheet_name] = 0

        # Generate header mapping report
        report = _build_header_report(sheet_name, anchor.header_row, col_mappings, approver_blocks)
        ctx.header_reports.append(report)
        ctx.sheets_processed += 1

    # ─── Phase D: Global Merge & Validation ───
    logger.info("Phase D: Merge & Validation")

    # Step 9: Schema union + merge
    master_df = merge_sheets(sheet_dfs)

    # Step 10: Validation checks
    validation_results = run_validation_checks(master_df, ctx)

    # Write all outputs
    os.makedirs(ctx.output_dir, exist_ok=True)

    write_master_dataset_json(master_df, ctx.output_dir)
    write_master_dataset_csv(master_df, ctx.output_dir)
    write_import_log_json(ctx.import_log, ctx.output_dir)
    write_import_log_csv(ctx.import_log, ctx.output_dir)
    write_header_mapping_report_json(ctx.header_reports, ctx.output_dir)
    write_validation_report_json(validation_results, ctx.output_dir)

    # Optional Excel output
    try:
        write_master_dataset_xlsx(master_df, ctx.output_dir)
    except Exception as e:
        logger.warning("Could not write Excel output: %s", e)

    elapsed = time.time() - start_time

    # Print summary
    print("\n" + "=" * 60)
    print("JANSA VISASIST — Module 1 Pipeline Summary")
    print("=" * 60)
    print(f"Input:              {ctx.input_path}")
    print(f"Output:             {ctx.output_dir}")
    print(f"Sheets processed:   {ctx.sheets_processed}")
    print(f"Sheets skipped:     {ctx.sheets_skipped}")
    print(f"Total rows:         {ctx.total_rows}")
    print(f"Log entries:        {len(ctx.import_log)}")
    print(f"  - ERROR:          {sum(1 for e in ctx.import_log if e.severity == 'ERROR')}")
    print(f"  - WARNING:        {sum(1 for e in ctx.import_log if e.severity == 'WARNING')}")
    print(f"  - INFO:           {sum(1 for e in ctx.import_log if e.severity == 'INFO')}")
    print(f"Elapsed time:       {elapsed:.2f}s")
    print(f"Validation:")
    for check_name, result in validation_results.items():
        status = "PASS" if result["passed"] else "FAIL"
        print(f"  [{status}] {check_name}: {result['details']}")
    print("=" * 60)

    return 0


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="JANSA VISASIST — Module 1: Data Ingestion & Normalization"
    )
    parser.add_argument(
        "--input", required=True,
        help="Path to the GrandFichier Excel file (.xlsx)"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory for output files"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Validate input file
    if not os.path.isfile(args.input):
        print(f"ERROR: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    # Build context and run
    ctx = PipelineContext(
        input_path=args.input,
        output_dir=args.output_dir,
    )

    exit_code = run_pipeline(ctx)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
