"""
Module 6 — L1/L2/L3 Classification Pipeline Orchestrator.

Flow: L1 (AI stub) -> if confidence >= 0.70 accept, else L2 -> if success accept, else L3 REJECTED.
GP10: L1 is a stub returning confidence 0.0 (always falls through to L2).
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from jansa_visasist.config_m6 import (
    AI_CLASSIFIER_CONFIDENCE_THRESHOLD,
    WARNING_AI_LOW_CONFIDENCE,
)
from jansa_visasist.pipeline.m6.normalizer import NormalizedQuery
from jansa_visasist.pipeline.m6.parser import ParseResult, parse_query

logger = logging.getLogger("jansa.m6.classifier")


@dataclass
class ClassificationResult:
    """Result of the 3-layer classification pipeline."""
    command_id: str
    layer: str                      # "L1", "L2", "L3"
    confidence: Optional[float]
    parameters: Dict[str, Any]
    warnings: List[str]


def _l1_classify(nq: NormalizedQuery) -> Optional[ParseResult]:
    """L1 AI Classifier — STUB.

    GP10: Returns confidence 0.0 always (falls through to L2).
    """
    return ParseResult(
        command_id="UNKNOWN",
        action="UNKNOWN",
        parameters={},
        confidence=0.0,
    )


def classify_query(
    nq: NormalizedQuery,
    lot_aliases: Dict[str, str],
    approver_aliases: Dict[str, str],
    category_aliases: Dict[str, str],
    status_synonyms: Dict[str, Dict],
    action_keywords: Dict[str, str],
) -> ClassificationResult:
    """Run the 3-layer classification pipeline.

    L1 -> L2 -> L3.
    """
    warnings: List[str] = []

    # L1: AI classifier (stub)
    l1_result = _l1_classify(nq)
    if l1_result and l1_result.confidence >= AI_CLASSIFIER_CONFIDENCE_THRESHOLD:
        return ClassificationResult(
            command_id=l1_result.command_id,
            layer="L1",
            confidence=l1_result.confidence,
            parameters=l1_result.parameters,
            warnings=warnings,
        )

    # L1 low confidence fallback
    if l1_result and l1_result.confidence > 0.0:
        warnings.append(WARNING_AI_LOW_CONFIDENCE)

    # L2: Deterministic parser
    l2_result = parse_query(
        nq, lot_aliases, approver_aliases,
        category_aliases, status_synonyms, action_keywords,
    )

    if l2_result.command_id != "REJECTED":
        return ClassificationResult(
            command_id=l2_result.command_id,
            layer="L2",
            confidence=l2_result.confidence,
            parameters=l2_result.parameters,
            warnings=warnings,
        )

    # L3: Rejection
    return ClassificationResult(
        command_id="REJECTED",
        layer="L3",
        confidence=0.0,
        parameters=l2_result.parameters,
        warnings=warnings,
    )
