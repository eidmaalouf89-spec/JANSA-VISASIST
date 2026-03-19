"""
JANSA VISASIST - Module 3 Entry Point
Pilotage / Prioritization Engine.

Load M2 enriched dataset -> run M3 pipeline -> write prioritized outputs.
"""

import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Optional, Tuple

import pandas as pd

from .config_m3 import M3_REQUIRED_COLUMNS, SCORE_MIN, SCORE_MAX
from .context_m3 import Module3Context

from .pipeline.m3.filtering import (
    Module3InputError,
    validate_and_prepare,
    filter_to_pending_scope,
)
from .pipeline.m3.time_metrics import add_time_metrics
from .pipeline.m3.approver_analysis import add_approver_analysis
from .pipeline.m3.consensus import add_consensus_type
from .pipeline.m3.categories import add_categories
from .pipeline.m3.scoring import add_priority_scores
from .pipeline.m3.summaries import (
    sort_and_finalize,
    build_category_summaries,
    build_extended_summaries,
)

from .outputs.m3_writers import (
    write_priority_queue_json,
    write_priority_queue_csv,
    write_priority_queue_xlsx,
    write_category_summary_json,
    write_category_summary_csv,
    write_extended_summary_json,
    write_exclusion_log_json,
    write_exclusion_log_csv,
    write_pipeline_report,
)

logger = logging.getLogger(__name__)


def _load_enriched_dataset(path: str) -> pd.DataFrame:
    """Load Module 2 enriched master dataset from JSON or CSV."""
    if path.endswith(".json"):
        with open(path, 'r', encoding='utf-8') as f:
            records = json.load(f)
        df = pd.DataFrame(records)
    elif path.endswith(".csv"):
        df = pd.read_csv(path, encoding='utf-8')
    else:
        raise ValueError(f"Unsupported file format: {path}. Use .json or .csv")

    logger.info("Loaded M2 enriched dataset: %d rows, %d columns", len(df), len(df.columns))
    return df


def run_module3(
    enriched_df: pd.DataFrame,
    output_dir: str,
    reference_date: Optional[datetime.date] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Execute the full Module 3 pipeline.
    Returns: (priority_queue_df, category_summary_df, exclusion_log_df)
    Also writes output files to output_dir.
    If reference_date is None, uses datetime.date.today().
    Raises Module3InputError on fatal input validation failure.
    """
    start_time = time.time()

    if reference_date is None:
        reference_date = datetime.date.today()

    ctx = Module3Context(output_dir=output_dir, reference_date=reference_date)
    os.makedirs(output_dir, exist_ok=True)

    # Handle empty input
    if enriched_df.empty:
        empty_queue = pd.DataFrame()
        empty_summary = pd.DataFrame(columns=["group_type", "group_value", "count"])
        empty_excl = pd.DataFrame()
        _write_all_outputs(empty_queue, empty_summary, pd.DataFrame(), ctx, 0.0)
        return empty_queue, empty_summary, empty_excl

    # ─── Pipeline execution ───
    logger.info("Module 3: Starting pipeline (%d rows, ref_date=%s)",
                len(enriched_df), reference_date)

    # Step 0: Input validation & preparation
    df = validate_and_prepare(enriched_df, ctx)

    # Step 1: Filter to pending scope
    pending_df, exclusion_df = filter_to_pending_scope(df, ctx)

    if pending_df.empty:
        logger.info("No pending items found. Writing empty outputs.")
        empty_summary = pd.DataFrame(columns=["group_type", "group_value", "count"])
        elapsed = time.time() - start_time
        _write_all_outputs(pending_df, empty_summary, pd.DataFrame(), ctx, elapsed)
        return pending_df, empty_summary, exclusion_df

    # Step 2: Compute time metrics
    pending_df = add_time_metrics(pending_df, reference_date)
    ctx.overdue_count = int(pending_df["is_overdue"].sum())

    # Step 3: Compute approver response analysis
    pending_df = add_approver_analysis(pending_df, ctx)

    # Step 4: Consensus engine
    pending_df = add_consensus_type(pending_df)

    # Step 5: Category engine
    pending_df = add_categories(pending_df)
    ctx.category_counts = pending_df["category"].value_counts().to_dict()

    # Step 6: Scoring engine
    pending_df = add_priority_scores(pending_df)

    # Step 7: Sort, finalize, summaries
    pending_df = sort_and_finalize(pending_df)
    summary_df = build_category_summaries(pending_df)
    extended_df = build_extended_summaries(pending_df)

    # Final validations
    _run_final_validations(pending_df, enriched_df)

    elapsed = time.time() - start_time

    # Step 8: Write outputs
    _write_all_outputs(pending_df, summary_df, extended_df, ctx, elapsed)

    # ─── Summary ───
    _print_summary(pending_df, ctx, elapsed)

    return pending_df, summary_df, exclusion_df


def _run_final_validations(queue_df: pd.DataFrame, input_df: pd.DataFrame) -> None:
    """Post-pipeline validation assertions."""
    if queue_df.empty:
        return

    # V9: All scores in range
    assert queue_df["priority_score"].between(SCORE_MIN, SCORE_MAX).all(), \
        "Some priority scores are out of range"

    # V14: No pending row has is_latest != True
    if "is_latest" in queue_df.columns:
        assert (queue_df["is_latest"] == True).all(), \
            "Queue contains rows with is_latest != True"  # noqa: E712

    # V15: No pending row has duplicate_flag == "DUPLICATE"
    if "duplicate_flag" in queue_df.columns:
        assert (queue_df["duplicate_flag"] != "DUPLICATE").all(), \
            "Queue contains DUPLICATE rows"

    # V16: No pending row has non-null visa_global
    assert queue_df["visa_global"].isna().all(), \
        "Queue contains rows with non-null visa_global"


def _write_all_outputs(
    queue_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    extended_df: pd.DataFrame,
    ctx: Module3Context,
    elapsed: float,
) -> None:
    """Write all output files."""
    # Spec outputs
    write_priority_queue_json(queue_df, ctx.output_dir)
    write_priority_queue_csv(queue_df, ctx.output_dir)
    write_category_summary_json(summary_df, ctx.output_dir)
    write_category_summary_csv(summary_df, ctx.output_dir)

    # Optional Excel
    try:
        write_priority_queue_xlsx(queue_df, ctx.output_dir)
    except Exception as e:
        logger.warning("Could not write Excel output: %s", e)

    # Diagnostic outputs
    write_exclusion_log_json(ctx.exclusion_log, ctx.output_dir)
    write_exclusion_log_csv(ctx.exclusion_log, ctx.output_dir)
    if not extended_df.empty if isinstance(extended_df, pd.DataFrame) else True:
        write_extended_summary_json(extended_df, ctx.output_dir)

    # Pipeline report
    report = _build_pipeline_report(ctx, elapsed)
    write_pipeline_report(report, ctx.output_dir)


def _build_pipeline_report(ctx: Module3Context, elapsed: float) -> dict:
    """Build JSON-serializable pipeline report dict."""
    # Count exclusions by reason
    exclusion_counts = {}
    for entry in ctx.exclusion_log:
        reason = entry.exclusion_reason
        exclusion_counts[reason] = exclusion_counts.get(reason, 0) + 1

    return {
        "module": "module_3",
        "reference_date": str(ctx.reference_date),
        "input_rows": ctx.input_rows,
        "pending_count": ctx.pending_count,
        "excluded_count": ctx.excluded_count,
        "excluded_by_reason": exclusion_counts,
        "category_distribution": ctx.category_counts,
        "overdue_count": ctx.overdue_count,
        "non_driving_status_warnings": ctx.non_driving_status_warnings,
        "elapsed_seconds": round(elapsed, 3),
    }


def _print_summary(queue_df: pd.DataFrame, ctx: Module3Context, elapsed: float) -> None:
    """Print human-readable pipeline summary."""
    print("\n" + "=" * 60)
    print("JANSA VISASIST - Module 3 Pipeline Summary")
    print("=" * 60)
    print(f"Reference date:     {ctx.reference_date}")
    print(f"Input rows:         {ctx.input_rows}")
    print(f"Pending (queued):   {ctx.pending_count}")
    print(f"Excluded:           {ctx.excluded_count}")
    print(f"Overdue:            {ctx.overdue_count}")
    if not queue_df.empty:
        print(f"Score range:        {queue_df['priority_score'].min():.1f} – "
              f"{queue_df['priority_score'].max():.1f}")
        print(f"Score mean:         {queue_df['priority_score'].mean():.1f}")
        print("Category distribution:")
        for cat, cnt in sorted(ctx.category_counts.items()):
            print(f"  {cat:25s} {cnt}")
    print(f"Elapsed time:       {elapsed:.2f}s")
    print("=" * 60)


def main():
    """CLI entry point with argparse. Wraps run_module3() and converts
    exceptions to exit codes: 0 = success, 1 = fatal error."""
    parser = argparse.ArgumentParser(
        description="JANSA VISASIST - Module 3: Pilotage / Prioritization Engine"
    )
    parser.add_argument(
        "--input-enriched", required=True,
        help="Path to M2 enriched dataset (.json or .csv)"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory for M3 output files"
    )
    parser.add_argument(
        "--reference-date", default=None,
        help="Reference date for overdue calculation (YYYY-MM-DD, default: today)"
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not os.path.isfile(args.input_enriched):
        print(f"ERROR: Input file not found: {args.input_enriched}", file=sys.stderr)
        sys.exit(1)

    ref_date = None
    if args.reference_date:
        try:
            ref_date = datetime.date.fromisoformat(args.reference_date)
        except ValueError:
            print(f"ERROR: Invalid date format: {args.reference_date}", file=sys.stderr)
            sys.exit(1)

    try:
        enriched_df = _load_enriched_dataset(args.input_enriched)
        run_module3(enriched_df, args.output_dir, ref_date)
        sys.exit(0)
    except Module3InputError as e:
        logger.error("Module 3 input error: %s", e)
        sys.exit(1)
    except Exception as e:
        logger.error("Module 3 failed: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
