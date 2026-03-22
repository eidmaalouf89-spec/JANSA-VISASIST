"""
JANSA VISASIST — Flask API Server
Serves pipeline JSON outputs from output/ via HTTP endpoints.
CORS enabled for Vite dev server (port 5173).

Usage:
    python api.py
    # or: flask --app api run --port 5000
"""

import json
import os
import datetime
from pathlib import Path
from flask import Flask, jsonify, abort, request
from flask_cors import CORS

# ── Configuration ────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"

app = Flask(__name__)
CORS(app, origins=["http://localhost:5173", "http://127.0.0.1:5173"])


# ── Helpers ──────────────────────────────────────────────────────────────

def load_json(filepath: Path):
    """Load a JSON file. Returns None if file doesn't exist."""
    if not filepath.exists():
        return None
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def require_json(filepath: Path):
    """Load a JSON file, abort 404 if missing."""
    data = load_json(filepath)
    if data is None:
        abort(404, description=f"Output file not found: {filepath.name}")
    return data


# ── Endpoint: GET /api/pipeline/run ──────────────────────────────────────
# Returns current pipeline run metadata derived from m3_pipeline_report.json

@app.route("/api/pipeline/run")
def pipeline_run():
    report = load_json(OUTPUT_DIR / "m3" / "m3_pipeline_report.json")
    if report is None:
        # Fallback: check if any output exists at all
        if not OUTPUT_DIR.exists():
            abort(404, description="No pipeline outputs found")
        return jsonify({
            "run_id": "unknown",
            "run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "status": "unknown",
            "lot_count": 0,
            "doc_count": 0,
        })

    # Derive lot_count from category_summary if available
    cat_summary = load_json(OUTPUT_DIR / "m3" / "m3_category_summary.json")
    lot_count = 0
    if cat_summary and isinstance(cat_summary, list):
        lot_count = sum(1 for r in cat_summary if r.get("group_type") == "lot")

    return jsonify({
        "run_id": f"run-{report.get('reference_date', 'unknown')}",
        "run_at": f"{report.get('reference_date', '')}T00:00:00Z",
        "status": "completed",
        "lot_count": lot_count,
        "doc_count": report.get("input_rows", 0),
        "elapsed_seconds": report.get("elapsed_seconds", None),
        "pending_count": report.get("pending_count", 0),
        "excluded_count": report.get("excluded_count", 0),
        "overdue_count": report.get("overdue_count", 0),
    })


# ── Endpoint: GET /api/dashboard/summary ─────────────────────────────────
# Returns M3 category summary + pipeline report (the dashboard adapter
# in the UI will compose the actual DashboardSummary shape)

@app.route("/api/dashboard/summary")
def dashboard_summary():
    cat_summary = require_json(OUTPUT_DIR / "m3" / "m3_category_summary.json")
    pipeline_report = require_json(OUTPUT_DIR / "m3" / "m3_pipeline_report.json")
    return jsonify({
        "category_summary": cat_summary,
        "pipeline_report": pipeline_report,
    })


# ── Endpoint: GET /api/queue ─────────────────────────────────────────────
# Returns the M3 priority queue (raw rows — the queue adapter in the UI
# validates and adapts each row)

@app.route("/api/queue")
def queue():
    data = require_json(OUTPUT_DIR / "m3" / "m3_priority_queue.json")
    return jsonify(data)


# ── Endpoint: GET /api/documents/<doc_version_key> ───────────────────────
# Returns a single document row from the M2 enriched dataset

@app.route("/api/documents/<path:doc_version_key>")
def document_detail(doc_version_key: str):
    data = require_json(OUTPUT_DIR / "m2" / "enriched_master_dataset.json")
    if not isinstance(data, list):
        abort(500, description="enriched_master_dataset.json is not an array")

    # Find the document by doc_version_key
    for row in data:
        if isinstance(row, dict) and row.get("doc_version_key") == doc_version_key:
            return jsonify(row)

    abort(404, description=f"Document not found: {doc_version_key}")


# ── Endpoint: GET /api/logs ──────────────────────────────────────────────
# Returns the M1 import log (anomalies/warnings)

@app.route("/api/logs")
def logs():
    data = require_json(OUTPUT_DIR / "m1" / "import_log.json")
    # Support ?severity= filter
    severity = request.args.get("severity")
    if severity and isinstance(data, list):
        data = [r for r in data if r.get("severity") == severity.upper()]
    # Support ?limit= pagination
    limit = request.args.get("limit", type=int)
    if limit and isinstance(data, list):
        data = data[:limit]
    return jsonify(data)


# ── Endpoint: GET /api/pipeline/history ──────────────────────────────────
# Returns pipeline run history. Currently returns the latest run only
# (multi-run history not yet stored by jansa pipeline).

@app.route("/api/pipeline/history")
def pipeline_history():
    report = load_json(OUTPUT_DIR / "m3" / "m3_pipeline_report.json")
    if report is None:
        return jsonify([])

    # Build a single-entry history from the current run
    return jsonify([
        {
            "run_id": f"run-{report.get('reference_date', 'unknown')}",
            "run_at": f"{report.get('reference_date', '')}T00:00:00Z",
            "pending_count": report.get("pending_count", 0),
        }
    ])


# ── Endpoint: GET /api/suggestions/<doc_version_key> ─────────────────────
# M5 AI suggestions — not yet produced by pipeline.
# Returns null until M5 is implemented.

@app.route("/api/suggestions/<path:doc_version_key>")
def suggestion(doc_version_key: str):
    return jsonify(None)


# ── Endpoint: GET /api/m4/<group> ────────────────────────────────────────
# Returns M4 analysis outputs (blockers, risk, trend, cluster, per-item)

@app.route("/api/m4/<group>")
def m4_data(group: str):
    valid_groups = {
        "blockers": "m4_g1_blockers.json",
        "risk": "m4_g2_risk.json",
        "trend": "m4_g3_trend.json",
        "cluster": "m4_g4_cluster.json",
        "per-item": "m4_per_item_results.json",
    }
    filename = valid_groups.get(group)
    if not filename:
        abort(404, description=f"Unknown M4 group: {group}")
    data = require_json(OUTPUT_DIR / "m4" / filename)
    return jsonify(data)


# ── Health check ─────────────────────────────────────────────────────────

@app.route("/api/health")
def health():
    """Quick health check — verifies output directory exists."""
    outputs_exist = OUTPUT_DIR.exists()
    m3_exists = (OUTPUT_DIR / "m3" / "m3_priority_queue.json").exists()
    return jsonify({
        "status": "ok" if outputs_exist and m3_exists else "degraded",
        "output_dir": str(OUTPUT_DIR),
        "outputs_found": outputs_exist,
        "m3_queue_found": m3_exists,
    })


# ── Main ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"[VISASIST API] Serving outputs from: {OUTPUT_DIR}")
    print(f"[VISASIST API] CORS allowed origins: http://localhost:5173")
    app.run(host="0.0.0.0", port=5000, debug=True)
