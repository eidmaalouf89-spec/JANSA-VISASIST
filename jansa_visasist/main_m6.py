"""
JANSA VISASIST — Module 6: Chatbot (Constrained) — Entry Point.

Usage:
    ctx = init_chatbot("output/m3/m3_priority_queue.json")
    response = process_query(ctx, "top 5 en retard")
"""

import json
import logging
import os
from typing import Optional

from jansa_visasist.config_m6 import LARGE_EXPORT_THRESHOLD, WARNING_LARGE_EXPORT
from jansa_visasist.context_m6 import Module6Context
from jansa_visasist.pipeline.m6.dictionaries import (
    build_lot_aliases,
    build_approver_aliases,
    build_category_aliases,
    build_status_synonyms,
    build_action_keywords,
    validate_dictionaries,
)
from jansa_visasist.pipeline.m6.indexes import (
    build_queue_index,
    build_doc_index,
    build_lot_index,
    build_approver_indexes,
)
from jansa_visasist.pipeline.m6.normalizer import normalize_query
from jansa_visasist.pipeline.m6.classifier import classify_query
from jansa_visasist.pipeline.m6.executor import execute_command
from jansa_visasist.pipeline.m6.formatter import ChatbotResponse, format_response
from jansa_visasist.pipeline.m6.exporter import export_to_csv

logger = logging.getLogger("jansa.m6")


def init_chatbot(
    m3_path: str,
    m4_path: Optional[str] = None,
    m5_path: Optional[str] = None,
    export_dir: str = "output/m6",
) -> Module6Context:
    """Initialize the chatbot context by loading data and building indexes.

    Args:
        m3_path: Path to m3_priority_queue.json
        m4_path: Optional path to M4 results (JSON, list of dicts with row_id key)
        m5_path: Optional path to M5 results (JSON, list of dicts with row_id key)
        export_dir: Directory for CSV exports

    Returns:
        Module6Context ready for process_query calls.
    """
    logger.info("Initializing M6 chatbot from %s", m3_path)

    # Load M3 queue data
    with open(m3_path, "r", encoding="utf-8") as f:
        queue_data = json.load(f)

    if not isinstance(queue_data, list):
        raise ValueError(f"M3 queue data must be a list, got {type(queue_data)}")

    logger.info("Loaded %d queue items from M3", len(queue_data))

    # Build dictionaries
    lot_aliases = build_lot_aliases(queue_data)
    approver_aliases = build_approver_aliases()
    category_aliases = build_category_aliases()
    status_synonyms = build_status_synonyms()
    action_keywords = build_action_keywords()

    # Validate
    warnings = validate_dictionaries(
        lot_aliases, approver_aliases, category_aliases,
        status_synonyms, action_keywords,
    )
    for w in warnings:
        logger.warning("Dictionary validation: %s", w)

    # Build indexes
    queue_index = build_queue_index(queue_data)
    doc_index = build_doc_index(queue_data)
    lot_index = build_lot_index(queue_data)
    approver_missing_idx, approver_blocking_idx, approver_assigned_idx = build_approver_indexes(queue_data)

    # Load M4 data if available
    m4_data = {}
    if m4_path and os.path.exists(m4_path):
        try:
            with open(m4_path, "r", encoding="utf-8") as f:
                m4_list = json.load(f)
            if isinstance(m4_list, list):
                for item in m4_list:
                    rid = item.get("row_id")
                    if rid:
                        m4_data[str(rid)] = item
            logger.info("Loaded %d M4 results", len(m4_data))
        except Exception as e:
            logger.warning("Failed to load M4 data: %s", e)

    # Load M5 data if available
    m5_data = {}
    if m5_path and os.path.exists(m5_path):
        try:
            with open(m5_path, "r", encoding="utf-8") as f:
                m5_list = json.load(f)
            if isinstance(m5_list, list):
                for item in m5_list:
                    rid = item.get("row_id")
                    if rid:
                        m5_data[str(rid)] = item
            logger.info("Loaded %d M5 results", len(m5_data))
        except Exception as e:
            logger.warning("Failed to load M5 data: %s", e)

    ctx = Module6Context(
        queue_data=queue_data,
        queue_index=queue_index,
        doc_index=doc_index,
        lot_index=lot_index,
        approver_missing_index=approver_missing_idx,
        approver_blocking_index=approver_blocking_idx,
        approver_assigned_index=approver_assigned_idx,
        lot_aliases=lot_aliases,
        approver_aliases=approver_aliases,
        category_aliases=category_aliases,
        status_synonyms=status_synonyms,
        action_keywords=action_keywords,
        m4_data=m4_data,
        m5_data=m5_data,
        export_dir=export_dir,
    )

    logger.info("M6 chatbot initialized successfully")
    return ctx


def process_query(ctx: Module6Context, query: str) -> ChatbotResponse:
    """Process a single user query and return a ChatbotResponse.

    Args:
        ctx: Module6Context (from init_chatbot)
        query: User query string (free text, French or English)

    Returns:
        ChatbotResponse with all fields populated.
    """
    # Step 1: Normalize query
    nq = normalize_query(query)
    logger.debug("Normalized query: %s", nq)

    # Step 2: Classify (L1 -> L2 -> L3)
    classification = classify_query(
        nq,
        ctx.lot_aliases,
        ctx.approver_aliases,
        ctx.category_aliases,
        ctx.status_synonyms,
        ctx.action_keywords,
    )

    command_id = classification.command_id
    params = classification.parameters
    layer = classification.layer
    confidence = classification.confidence
    class_warnings = classification.warnings

    # Step 3: Execute command
    results, result_count, extra = execute_command(ctx, command_id, params)

    # Merge classification warnings into extra
    exec_warnings = extra.get("warnings", [])
    all_warnings = class_warnings + exec_warnings
    extra["warnings"] = all_warnings

    # Step 4: Handle C12 export
    export_metadata = None
    if command_id == "C12":
        raw_items = extra.get("raw_items", results)
        export_metadata = export_to_csv(raw_items, ctx.export_dir)
        result_count = export_metadata["row_count"]
        results = []  # C12: results=[]

        # Large export warning
        if result_count > LARGE_EXPORT_THRESHOLD:
            all_warnings.append(WARNING_LARGE_EXPORT.format(count=result_count))
            extra["warnings"] = all_warnings

    # Step 5: Format response
    response = format_response(
        command_id=command_id,
        layer=layer,
        confidence=confidence,
        params=params,
        results=results,
        result_count=result_count,
        extra=extra,
        export_metadata=export_metadata,
    )

    return response
