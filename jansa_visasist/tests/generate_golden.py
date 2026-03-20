#!/usr/bin/env python3
"""
JANSA VISASIST — Golden Snapshot Generator

Runs M1 → M2 pipeline against data/GrandFichier_1.xlsx
and writes 4 golden JSON files used by integration tests.

Usage:
    python -m jansa_visasist.tests.generate_golden

The script expects:
    data/GrandFichier_1.xlsx  (relative to project root)

Outputs:
    jansa_visasist/tests/golden/golden_snapshot.json
    jansa_visasist/tests/golden/golden_log_summary.json
    jansa_visasist/tests/golden/golden_validation.json
    jansa_visasist/tests/golden/golden_m2_snapshot.json

Note: M3 output is date-dependent so no golden file is generated for it.
      E2E tests validate structural invariants instead.
"""

import json
import os
import sys
import tempfile

import pandas as pd


def _project_root() -> str:
    """Return the project root (parent of jansa_visasist/)."""
    here = os.path.dirname(os.path.abspath(__file__))
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
    """Run M1 pipeline and generate golden_snapshot, golden_log_summary, golden_validation."""
    from jansa_visasist.context import PipelineContext
    from jansa_visasist.main import run_pipeline

    ctx = PipelineContext(input_path=input_path, output_dir=m1_output_dir)
    exit_code = run_pipeline(ctx)
    assert exit_code == 0, f"M1 pipeline failed with exit code {exit_code}"

    # Load master dataset
    with open(os.path.join(m1_output_dir, "master_dataset.json"), "r", encoding="utf-8") as f:
        records = json.load(f)
    master_df = pd.DataFrame(records)

    # ── golden_snapshot.json ──
    rq_dist = master_df["row_quality"].value_counts().to_dict()
    vg_dist = master_df["visa_global"].dropna().value_counts().to_dict()

    snapshot = {
        "total_rows": len(master_df),
        "sheets_count": ctx.sheets_processed,
        "row_quality_distribution": {k: int(v) for k, v in rq_dist.items()},
        "visa_global_distribution": {k: int(v) for k, v in vg_dist.items()},
        "visa_global_nulls": int(master_df["visa_global"].isna().sum()),
        "document_nulls": int(master_df["document"].isna().sum()),
        "ind_nulls": int(master_df["ind"].isna().sum()),
    }
    _write_golden("golden_snapshot.json", snapshot)

    # ── golden_log_summary.json ──
    sev_dist = {}
    cat_dist = {}
    for entry in ctx.import_log:
        sev_dist[entry.severity] = sev_dist.get(entry.severity, 0) + 1
        cat_dist[entry.category] = cat_dist.get(entry.category, 0) + 1

    log_summary = {
        "total_entries": len(ctx.import_log),
        "severity_distribution": sev_dist,
        "category_distribution": {k: int(v) for k, v in cat_dist.items()},
    }
    _write_golden("golden_log_summary.json", log_summary)

    # ── golden_validation.json ──
    with open(os.path.join(m1_output_dir, "validation_report.json"), "r", encoding="utf-8") as f:
        validation_data = json.load(f)
    _write_golden("golden_validation.json", validation_data)

    return master_df


def generate_m2_golden(master_df: pd.DataFrame, m2_output_dir: str) -> pd.DataFrame:
    """Run M2 pipeline and generate golden_m2_snapshot."""
    from jansa_visasist.main_m2 import run_module2
    from jansa_visasist.config_m2 import UNPARSEABLE_PREFIX

    exit_code = run_module2(master_df, m2_output_dir)
    assert exit_code == 0, f"M2 pipeline failed with exit code {exit_code}"

    # Load enriched dataset
    with open(os.path.join(m2_output_dir, "enriched_master_dataset.json"), "r", encoding="utf-8") as f:
        records = json.load(f)
    enriched_df = pd.DataFrame(records)

    # Load anomalies
    with open(os.path.join(m2_output_dir, "linking_anomalies.json"), "r", encoding="utf-8") as f:
        anomalies = json.load(f)

    # Build snapshot
    dup_dist = enriched_df["duplicate_flag"].value_counts().to_dict()
    unp_count = int(enriched_df["doc_family_key"].str.startswith(UNPARSEABLE_PREFIX).sum())
    cross_lot_rows = int(enriched_df["is_cross_lot"].sum()) if "is_cross_lot" in enriched_df.columns else 0
    latest_count = int(enriched_df["is_latest"].sum()) if "is_latest" in enriched_df.columns else 0

    anom_types = {}
    for a in anomalies:
        t = a.get("anomaly_type", "UNKNOWN")
        anom_types[t] = anom_types.get(t, 0) + 1

    m2_snapshot = {
        "total_rows": len(enriched_df),
        "duplicate_flag_distribution": {k: int(v) for k, v in dup_dist.items()},
        "unparseable_count": unp_count,
        "cross_lot_rows": cross_lot_rows,
        "is_latest_count": latest_count,
        "anomaly_type_distribution": anom_types,
        "total_anomalies": len(anomalies),
    }
    _write_golden("golden_m2_snapshot.json", m2_snapshot)

    return enriched_df


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

        # M1
        print("\n[1/2] Running Module 1 pipeline...")
        master_df = generate_m1_golden(input_path, m1_dir)
        print(f"  M1 complete: {len(master_df)} rows")

        # M2
        print("\n[2/2] Running Module 2 pipeline...")
        generate_m2_golden(master_df, m2_dir)
        print("  M2 complete")

    print("\n" + "=" * 60)
    print("All 4 golden snapshots generated successfully.")
    print(f"Location: {_golden_dir()}")
    print("=" * 60)


if __name__ == "__main__":
    main()
