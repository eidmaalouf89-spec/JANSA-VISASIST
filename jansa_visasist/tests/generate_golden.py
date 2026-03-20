#!/usr/bin/env python3
"""
JANSA VISASIST — Golden Snapshot Generator

Runs M1 → M2 → M3 pipeline against data/GrandFichier_1.xlsx
and writes 5 golden JSON files used by integration tests.

Usage:
    python -m jansa_visasist.tests.generate_golden

The script expects:
    data/GrandFichier_1.xlsx  (relative to project root)

Outputs:
    jansa_visasist/tests/golden/golden_snapshot.json
    jansa_visasist/tests/golden/golden_log_summary.json
    jansa_visasist/tests/golden/golden_validation.json
    jansa_visasist/tests/golden/golden_m2_snapshot.json
    jansa_visasist/tests/golden/golden_m3_snapshot.json
"""

import datetime
import json
import os
import sys
import tempfile

import pandas as pd


def _project_root() -> str:
    """Return the project root (parent of jansa_visasist/)."""
    here = os.path.dirname(os.path.abspath(__file__))
    # here = jansa_visasist/tests/
    return os.path.abspath(os.path.join(here, "..", ".."))


def _golden_dir() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "golden")


def _write_golden(filename: str, data: dict) -> None:
    path = os.path.join(_golden_dir(), filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"  Written: {path}")


def generate_m1_golden(input_path: str, m1_output_dir: str) -> pd.DataFrame:
    """Run M1 pipeline and generate golden_snapshot.json, golden_log_summary.json, golden_validation.json."""
    from jansa_visasist.context import PipelineContext
    from jansa_visasist.main import run_pipeline

    ctx = PipelineContext(input_path=input_path, output_dir=m1_output_dir)
    exit_code = run_pipeline(ctx)
    assert exit_code == 0, f"M1 pipeline failed with exit code {exit_code}"

    # Load master dataset
    master_path = os.path.join(m1_output_dir, "master_dataset.json")
    with open(master_path, "r", encoding="utf-8") as f:
        records = json.load(f)
    master_df = pd.DataFrame(records)

    # ── golden_snapshot.json ──
    row_quality_dist = master_df["row_quality"].value_counts().to_dict()
    visa_dist = master_df["visa_global"].fillna("null").value_counts().to_dict()
    ind_null_count = int(master_df["ind"].isna().sum())
    doc_null_count = int(master_df["document"].isna().sum())

    snapshot = {
        "total_rows": len(master_df),
        "sheets_count": ctx.sheets_processed,
        "row_quality_distribution": {k: int(v) for k, v in row_quality_dist.items()},
        "visa_global_distribution": {k: int(v) for k, v in visa_dist.items()},
        "has_document_null_count": doc_null_count,
        "has_ind_null_count_lte": ind_null_count,
        "source_sheets_unique_count": int(master_df["source_sheet"].nunique()),
        "columns_present": sorted(master_df.columns.tolist()),
    }
    _write_golden("golden_snapshot.json", snapshot)

    # ── golden_log_summary.json ──
    severity_dist = {}
    category_dist = {}
    for entry in ctx.import_log:
        sev = entry.severity
        severity_dist[sev] = severity_dist.get(sev, 0) + 1
        cat = entry.category
        category_dist[cat] = category_dist.get(cat, 0) + 1

    top_categories = sorted(category_dist.keys(), key=lambda k: category_dist[k], reverse=True)

    log_summary = {
        "total_entries": len(ctx.import_log),
        "severity_distribution": severity_dist,
        "category_distribution": {k: int(v) for k, v in category_dist.items()},
        "top_categories": top_categories[:5],
    }
    _write_golden("golden_log_summary.json", log_summary)

    # ── golden_validation.json ──
    validation_path = os.path.join(m1_output_dir, "validation_report.json")
    with open(validation_path, "r", encoding="utf-8") as f:
        validation_data = json.load(f)

    total_checks = len(validation_data)
    passed = sum(1 for v in validation_data.values() if v.get("passed", False))
    failed = total_checks - passed

    validation = {
        "checks": validation_data,
        "total_checks": total_checks,
        "passed_checks": passed,
        "failed_checks": failed,
    }
    _write_golden("golden_validation.json", validation)

    return master_df


def generate_m2_golden(master_df: pd.DataFrame, m2_output_dir: str) -> pd.DataFrame:
    """Run M2 pipeline and generate golden_m2_snapshot.json."""
    from jansa_visasist.main_m2 import run_module2

    exit_code = run_module2(master_df, m2_output_dir)
    assert exit_code == 0, f"M2 pipeline failed with exit code {exit_code}"

    # Load enriched dataset
    enriched_path = os.path.join(m2_output_dir, "enriched_master.json")
    with open(enriched_path, "r", encoding="utf-8") as f:
        records = json.load(f)
    enriched_df = pd.DataFrame(records)

    # Load anomalies
    anomalies_path = os.path.join(m2_output_dir, "anomalies.json")
    with open(anomalies_path, "r", encoding="utf-8") as f:
        anomalies = json.load(f)

    # Build snapshot
    dup_dist = enriched_df["duplicate_flag"].value_counts().to_dict()
    cross_lot_count = int(enriched_df["is_cross_lot"].sum()) if "is_cross_lot" in enriched_df.columns else 0
    family_count = int(enriched_df["doc_family_key"].nunique())
    is_latest_count = int(enriched_df["is_latest"].sum()) if "is_latest" in enriched_df.columns else 0
    unparseable_count = int(
        enriched_df["doc_family_key"].str.startswith("UNPARSEABLE::").sum()
    ) if "doc_family_key" in enriched_df.columns else 0

    anomaly_types = {}
    for a in anomalies:
        atype = a.get("anomaly_type", "UNKNOWN")
        anomaly_types[atype] = anomaly_types.get(atype, 0) + 1

    # Determine M2-added columns
    m2_columns = [
        c for c in enriched_df.columns
        if c in {
            "doc_family_key", "ind_sort_order", "doc_version_key",
            "previous_version_key", "is_latest", "revision_count",
            "duplicate_flag", "is_cross_lot", "cross_lot_sheets",
        }
    ]

    m2_snapshot = {
        "total_rows": len(enriched_df),
        "duplicate_flag_distribution": {k: int(v) for k, v in dup_dist.items()},
        "cross_lot_count": cross_lot_count,
        "anomaly_count": len(anomalies),
        "anomaly_type_distribution": anomaly_types,
        "family_count_gte": family_count,
        "is_latest_true_count_gte": is_latest_count,
        "unparseable_count_lte": unparseable_count,
        "columns_added": sorted(m2_columns),
    }
    _write_golden("golden_m2_snapshot.json", m2_snapshot)

    return enriched_df


def generate_m3_golden(enriched_df: pd.DataFrame, m3_output_dir: str) -> None:
    """Run M3 pipeline and generate golden_m3_snapshot.json."""
    from jansa_visasist.main_m3 import run_module3

    # Use a fixed reference date for reproducibility
    ref_date = datetime.date(2025, 1, 15)

    queue_df, summary_df, exclusion_df = run_module3(
        enriched_df, m3_output_dir, reference_date=ref_date
    )

    # Build snapshot
    cat_dist = {}
    if not queue_df.empty and "category" in queue_df.columns:
        cat_dist = queue_df["category"].value_counts().to_dict()

    exclusion_count = len(exclusion_df) if exclusion_df is not None and not exclusion_df.empty else 0
    # Try loading from file if exclusion_df is empty but file has entries
    excl_path = os.path.join(m3_output_dir, "exclusion_log.json")
    if os.path.isfile(excl_path):
        with open(excl_path, "r", encoding="utf-8") as f:
            excl_data = json.load(f)
        exclusion_count = max(exclusion_count, len(excl_data))

    score_min = float(queue_df["priority_score"].min()) if not queue_df.empty else 0.0
    score_max = float(queue_df["priority_score"].max()) if not queue_df.empty else 0.0

    all_latest = bool(
        (queue_df["is_latest"] == True).all()
    ) if not queue_df.empty and "is_latest" in queue_df.columns else True

    no_dup = bool(
        (queue_df["duplicate_flag"] != "DUPLICATE").all()
    ) if not queue_df.empty and "duplicate_flag" in queue_df.columns else True

    all_visa_null = bool(
        queue_df["visa_global"].isna().all()
    ) if not queue_df.empty and "visa_global" in queue_df.columns else True

    m3_snapshot = {
        "queue_size": len(queue_df),
        "exclusion_count": exclusion_count,
        "category_distribution": {k: int(v) for k, v in cat_dist.items()},
        "score_min_gte": score_min,
        "score_max_lte": score_max,
        "all_is_latest_true": all_latest,
        "no_duplicate_in_queue": no_dup,
        "all_visa_global_null": all_visa_null,
    }
    _write_golden("golden_m3_snapshot.json", m3_snapshot)


def main():
    root = _project_root()
    input_path = os.path.join(root, "data", "GrandFichier_1.xlsx")

    if not os.path.isfile(input_path):
        print(f"ERROR: GrandFichier not found at: {input_path}")
        print("Place GrandFichier_1.xlsx in the data/ directory and re-run.")
        sys.exit(1)

    print("=" * 60)
    print("JANSA VISASIST — Golden Snapshot Generator")
    print("=" * 60)
    print(f"Input: {input_path}")

    with tempfile.TemporaryDirectory(prefix="jansa_golden_") as tmpdir:
        m1_dir = os.path.join(tmpdir, "m1_output")
        m2_dir = os.path.join(tmpdir, "m2_output")
        m3_dir = os.path.join(tmpdir, "m3_output")

        # M1
        print("\n[1/3] Running Module 1 pipeline...")
        master_df = generate_m1_golden(input_path, m1_dir)
        print(f"  M1 complete: {len(master_df)} rows")

        # M2
        print("\n[2/3] Running Module 2 pipeline...")
        enriched_df = generate_m2_golden(master_df, m2_dir)
        print(f"  M2 complete: {len(enriched_df)} rows")

        # M3
        print("\n[3/3] Running Module 3 pipeline...")
        generate_m3_golden(enriched_df, m3_dir)
        print("  M3 complete")

    print("\n" + "=" * 60)
    print("All 5 golden snapshots generated successfully.")
    print(f"Location: {_golden_dir()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
