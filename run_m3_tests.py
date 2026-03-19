#!/usr/bin/env python3
"""
JANSA VISASIST — Module 3 Test Runner & Pipeline Executor.
Run this script to:
  1. Clean up __pycache__ and free disk space
  2. Run M3 unit tests
  3. Run M3 integration tests (if M2 output exists)
  4. Print summary of results

Usage:
    python run_m3_tests.py
    python run_m3_tests.py --run-pipeline   # Also runs the full pipeline

NOTE: On Windows, use the Python that has pandas/pytest installed (Python 3.11).
      If 'python3' maps to a different version, use the full path or 'py -3.11'.
"""

import os
import sys
import shutil
import subprocess
import argparse
import tempfile
import json
import datetime

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
JANSA_PKG = os.path.join(PROJECT_ROOT, "jansa_visasist")
M2_OUTPUT = os.path.join(PROJECT_ROOT, "output", "m2", "enriched_master_dataset.json")
M3_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output", "m3")


def cleanup():
    """Remove __pycache__ directories and .pyc files to free space."""
    count = 0
    for root, dirs, files in os.walk(PROJECT_ROOT):
        for d in list(dirs):
            if d == "__pycache__":
                p = os.path.join(root, d)
                shutil.rmtree(p, ignore_errors=True)
                count += 1
    print(f"  Cleaned {count} __pycache__ directories.")


def run_unit_tests():
    """Run M3 unit tests."""
    print("\n" + "=" * 60)
    print("RUNNING MODULE 3 UNIT TESTS")
    print("=" * 60)
    result = subprocess.run(
        [sys.executable, "-m", "pytest",
         os.path.join(JANSA_PKG, "tests", "test_m3_unit.py"),
         "-v", "--tb=short"],
        cwd=PROJECT_ROOT,
        capture_output=False,
    )
    return result.returncode


def run_integration_tests():
    """Run M3 integration tests."""
    if not os.path.exists(M2_OUTPUT):
        print(f"\n  SKIP: M2 output not found at {M2_OUTPUT}")
        return 0

    print("\n" + "=" * 60)
    print("RUNNING MODULE 3 INTEGRATION TESTS")
    print("=" * 60)
    result = subprocess.run(
        [sys.executable, "-m", "pytest",
         os.path.join(JANSA_PKG, "tests", "test_m3_integration.py"),
         "-v", "--tb=short"],
        cwd=PROJECT_ROOT,
        capture_output=False,
    )
    return result.returncode


def run_pipeline():
    """Run the full M3 pipeline and print results."""
    if not os.path.exists(M2_OUTPUT):
        print(f"\n  SKIP: M2 output not found at {M2_OUTPUT}")
        return 1

    print("\n" + "=" * 60)
    print("RUNNING MODULE 3 PIPELINE")
    print("=" * 60)

    # Add project root to path
    sys.path.insert(0, PROJECT_ROOT)

    from jansa_visasist.main_m3 import run_module3, _load_enriched_dataset

    enriched_df = _load_enriched_dataset(M2_OUTPUT)
    os.makedirs(M3_OUTPUT_DIR, exist_ok=True)

    queue_df, summary_df, exclusion_df = run_module3(
        enriched_df, M3_OUTPUT_DIR
    )

    # Print detailed results
    print("\n" + "=" * 60)
    print("MODULE 3 DETAILED RESULTS")
    print("=" * 60)

    if not queue_df.empty:
        print(f"\nQueue size: {len(queue_df)}")
        print(f"Excluded:   {len(exclusion_df)}")

        print("\nCategory distribution:")
        cat_counts = queue_df["category"].value_counts()
        for cat, cnt in cat_counts.items():
            print(f"  {cat:25s} {cnt:5d}")

        print(f"\nConsensus distribution:")
        cons_counts = queue_df["consensus_type"].value_counts()
        for ct, cnt in cons_counts.items():
            print(f"  {ct:25s} {cnt:5d}")

        print(f"\nScore statistics:")
        print(f"  Min:    {queue_df['priority_score'].min():.1f}")
        print(f"  Max:    {queue_df['priority_score'].max():.1f}")
        print(f"  Mean:   {queue_df['priority_score'].mean():.1f}")
        print(f"  Median: {queue_df['priority_score'].median():.1f}")

        overdue_count = queue_df["is_overdue"].sum()
        print(f"\nOverdue items: {overdue_count}")
        no_deadline = (~queue_df["has_deadline"]).sum()
        print(f"No deadline:   {no_deadline}")

        print(f"\nOutput files written to: {M3_OUTPUT_DIR}")
        for f in sorted(os.listdir(M3_OUTPUT_DIR)):
            fpath = os.path.join(M3_OUTPUT_DIR, f)
            size = os.path.getsize(fpath)
            print(f"  {f:45s} {size:>8,} bytes")
    else:
        print("Queue is empty — no pending items found.")

    return 0


def main():
    parser = argparse.ArgumentParser(description="M3 test runner & pipeline executor")
    parser.add_argument("--run-pipeline", action="store_true",
                        help="Also run the full M3 pipeline on real data")
    parser.add_argument("--skip-tests", action="store_true",
                        help="Skip tests, only run pipeline")
    args = parser.parse_args()

    print("=" * 60)
    print("JANSA VISASIST — Module 3 Test Runner")
    print("=" * 60)

    # Step 1: Cleanup
    print("\nStep 1: Cleaning up...")
    cleanup()

    exit_code = 0

    if not args.skip_tests:
        # Step 2: Unit tests
        rc = run_unit_tests()
        if rc != 0:
            exit_code = 1
            print("\n  UNIT TESTS FAILED")
        else:
            print("\n  UNIT TESTS PASSED")

        # Step 3: Integration tests
        rc = run_integration_tests()
        if rc != 0:
            exit_code = 1
            print("\n  INTEGRATION TESTS FAILED")
        else:
            print("\n  INTEGRATION TESTS PASSED")

    # Step 4: Pipeline run
    if args.run_pipeline or args.skip_tests:
        rc = run_pipeline()
        if rc != 0:
            exit_code = 1

    print("\n" + "=" * 60)
    if exit_code == 0:
        print("ALL CHECKS PASSED")
    else:
        print("SOME CHECKS FAILED")
    print("=" * 60)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
