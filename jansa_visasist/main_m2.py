"""
JANSA VISASIST - Module 2 Entry Point
Data Model & Revision Linking.

Load M1 master dataset -> run M2 pipeline -> write enriched outputs.
"""

import argparse
import json
import logging
import os
import sys
import time

import pandas as pd

from .config_m2 import M2_REQUIRED_COLUMNS, M2_OPTIONAL_COLUMNS
from .context_m2 import Module2Context

from .pipeline.m2.family_key import build_family_keys
from .pipeline.m2.ind_normalization import compute_sort_orders
from .pipeline.m2.duplicate_detection import detect_duplicates
from .pipeline.m2.chain_linking import link_chains
from .pipeline.m2.cross_lot import detect_cross_lot
from .pipeline.m2.anomaly_detection import detect_anomalies

from .outputs.m2_writers import (
    write_enriched_master_json,
    write_enriched_master_csv,
    write_enriched_master_xlsx,
    build_family_index,
    write_family_index_json,
    write_family_index_csv,
    write_anomalies_json,
    write_anomalies_csv,
)

logger = logging.getLogger(__name__)


class Module2InputError(Exception):
    """Raised when M1 output is missing required columns."""
    pass


def _load_master_dataset(path: str) -> pd.DataFrame:
    """Load Module 1 master dataset from JSON or CSV."""
    if path.endswith(".json"):
        with open(path, 'r', encoding='utf-8') as f:
            records = json.load(f)
        df = pd.DataFrame(records)
    elif path.endswith(".csv"):
        df = pd.read_csv(path, encoding='utf-8')
    else:
        raise ValueError(f"Unsupported file format: {path}. Use .json or .csv")

    logger.info("Loaded M1 master dataset: %d rows, %d columns", len(df), len(df.columns))
    return df


def _validate_input(df: pd.DataFrame) -> None:
    """
    [SAFEGUARD] Validate that required M1 columns exist.
    Not spec-defined - defensive engineering for robustness.
    """
    if df.empty:
        logger.info("Empty DataFrame - will return empty outputs.")
        return

    missing_required = M2_REQUIRED_COLUMNS - set(df.columns)
    if missing_required:
        raise Module2InputError(
            f"Module 1 output is missing required columns: {sorted(missing_required)}. "
            f"Cannot proceed with Module 2."
        )

    missing_optional = M2_OPTIONAL_COLUMNS - set(df.columns)
    if missing_optional:
        for col in missing_optional:
            logger.warning(
                "Optional column '%s' missing from M1 output. "
                "Related features will degrade gracefully.", col
            )
            df[col] = None


def run_module2(master_df: pd.DataFrame, output_dir: str) -> int:
    """
    Execute the full Module 2 pipeline.
    Returns exit code: 0 = success, 1 = fatal error.
    """
    start_time = time.time()

    # [SAFEGUARD] Input validation
    _validate_input(master_df)

    if master_df.empty:
        os.makedirs(output_dir, exist_ok=True)
        # Write empty outputs
        write_enriched_master_json(master_df, output_dir)
        write_enriched_master_csv(master_df, output_dir)
        write_family_index_json(pd.DataFrame(), output_dir)
        write_family_index_csv(pd.DataFrame(), output_dir)
        write_anomalies_json([], output_dir)
        write_anomalies_csv([], output_dir)
        return 0

    ctx = Module2Context(output_dir=output_dir)

    # Make a copy to avoid mutating the input
    df = master_df.copy()

    # ─── Pipeline execution (spec order with Step 5 before Step 3) ───
    logger.info("Module 2: Starting pipeline (%d rows)", len(df))

    # Step 1: doc_family_key construction
    df = build_family_keys(df, ctx)

    # Step 2: ind_sort_order computation
    df = compute_sort_orders(df, ctx)

    # Step 5: duplicate detection (BEFORE chain linking per spec)
    df = detect_duplicates(df, ctx)

    # Step 3: family grouping & chain linking
    df = link_chains(df, ctx)

    # Step 4: cross-lot detection
    df = detect_cross_lot(df, ctx)

    # Step 6: anomaly detection
    df = detect_anomalies(df, ctx)

    # ─── Build family index ───
    family_index = build_family_index(df, ctx.anomaly_log)

    # ─── Write all outputs ───
    os.makedirs(output_dir, exist_ok=True)

    write_enriched_master_json(df, output_dir)
    write_enriched_master_csv(df, output_dir)
    write_family_index_json(family_index, output_dir)
    write_family_index_csv(family_index, output_dir)
    write_anomalies_json(ctx.anomaly_log, output_dir)
    write_anomalies_csv(ctx.anomaly_log, output_dir)

    # Optional Excel
    try:
        write_enriched_master_xlsx(df, output_dir)
    except Exception as e:
        logger.warning("Could not write Excel output: %s", e)

    elapsed = time.time() - start_time

    # ─── Summary ───
    print("\n" + "=" * 60)
    print("JANSA VISASIST - Module 2 Pipeline Summary")
    print("=" * 60)
    print(f"Input rows:         {len(df)}")
    print(f"Output:             {output_dir}")
    print(f"Family-sheet groups:{ctx.family_count}")
    print(f"Cross-lot families: {ctx.cross_lot_count}")
    print(f"UNPARSEABLE rows:   {ctx.unparseable_count}")
    print(f"DUPLICATE rows:     {ctx.duplicate_exact_count}")
    print(f"SUSPECT rows:       {ctx.duplicate_suspect_count}")
    print(f"Total anomalies:    {len(ctx.anomaly_log)}")
    print(f"Family index rows:  {len(family_index)}")
    print(f"Elapsed time:       {elapsed:.2f}s")
    print("=" * 60)

    return 0


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="JANSA VISASIST - Module 2: Data Model & Revision Linking"
    )
    parser.add_argument(
        "--input-master", required=True,
        help="Path to M1 master dataset (.json or .csv)"
    )
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory for M2 output files"
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

    if not os.path.isfile(args.input_master):
        print(f"ERROR: Input file not found: {args.input_master}", file=sys.stderr)
        sys.exit(1)

    master_df = _load_master_dataset(args.input_master)
    exit_code = run_module2(master_df, args.output_dir)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
