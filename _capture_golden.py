#!/usr/bin/env python3
"""
Temporary script: Run M1+M2 pipeline against GrandFichier_1.xlsx
and write golden snapshot files with the CORRECT schema.
"""
import json
import os
import sys
import tempfile

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd

from jansa_visasist.context import PipelineContext
from jansa_visasist.main import run_pipeline
from jansa_visasist.main_m2 import run_module2

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "jansa_visasist", "tests", "golden")

def main():
    input_path = os.path.join(os.path.dirname(__file__), "data", "GrandFichier_1.xlsx")
    if not os.path.isfile(input_path):
        print(f"ERROR: {input_path} not found")
        sys.exit(1)

    with tempfile.TemporaryDirectory(prefix="jansa_golden_") as tmpdir:
        m1_dir = os.path.join(tmpdir, "m1")
        m2_dir = os.path.join(tmpdir, "m2")

        # ── M1 ──
        print("Running M1 pipeline...")
        ctx = PipelineContext(input_path=input_path, output_dir=m1_dir)
        exit_code = run_pipeline(ctx)
        print(f"M1 exit code: {exit_code}")

        # Load M1 outputs
        with open(os.path.join(m1_dir, "master_dataset.json"), "r", encoding="utf-8") as f:
            m1_records = json.load(f)
        master_df = pd.DataFrame(m1_records)

        with open(os.path.join(m1_dir, "validation_report.json"), "r", encoding="utf-8") as f:
            validation = json.load(f)

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
        _write(GOLDEN_DIR, "golden_snapshot.json", snapshot)

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
        _write(GOLDEN_DIR, "golden_log_summary.json", log_summary)

        # ── golden_validation.json ──
        _write(GOLDEN_DIR, "golden_validation.json", validation)

        # ── M2 ──
        print("Running M2 pipeline...")
        m2_exit = run_module2(master_df, m2_dir)
        print(f"M2 exit code: {m2_exit}")

        # Load M2 outputs
        with open(os.path.join(m2_dir, "enriched_master_dataset.json"), "r", encoding="utf-8") as f:
            m2_records = json.load(f)
        enriched_df = pd.DataFrame(m2_records)

        with open(os.path.join(m2_dir, "linking_anomalies.json"), "r", encoding="utf-8") as f:
            anomalies = json.load(f)

        # ── golden_m2_snapshot.json ──
        dup_dist = enriched_df["duplicate_flag"].value_counts().to_dict()
        unp_count = int(enriched_df["doc_family_key"].str.startswith("UNPARSEABLE::").sum())
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
        _write(GOLDEN_DIR, "golden_m2_snapshot.json", m2_snapshot)

        print("\n=== ALL GOLDEN FILES WRITTEN ===")
        for name in ["golden_snapshot.json", "golden_log_summary.json",
                      "golden_validation.json", "golden_m2_snapshot.json"]:
            path = os.path.join(GOLDEN_DIR, name)
            with open(path, "r") as f:
                data = json.load(f)
            print(f"\n--- {name} ---")
            print(json.dumps(data, indent=2, ensure_ascii=False))


def _write(golden_dir, filename, data):
    os.makedirs(golden_dir, exist_ok=True)
    path = os.path.join(golden_dir, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"  Written: {path}")


if __name__ == "__main__":
    main()
