"""
Module 6 — L2 Deterministic Parser.

Action-first parsing:
  Step 1: Detect action type (EXPLAIN > EXPORT > COUNT > SUMMARY > RETRIEVE)
  Step 2: Extract entities (lot, approver, category, status)
  Step 2b: Top-N override
  Step 3: Command resolution table
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from jansa_visasist.pipeline.m6.normalizer import NormalizedQuery, normalize_text

logger = logging.getLogger("jansa.m6.parser")


@dataclass
class ParseResult:
    """Result of L2 deterministic parsing."""
    command_id: str                  # "C1"-"C12" or "REJECTED"
    action: str                      # EXPLAIN, EXPORT, COUNT, SUMMARY, RETRIEVE, or NONE
    parameters: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 1.0          # L2 always 1.0 or 0.0


def _detect_action(
    tokens: List[str],
    normalized: str,
    action_keywords: Dict[str, str],
) -> str:
    """Detect action type from tokens. Priority: EXPLAIN > EXPORT > COUNT > SUMMARY > RETRIEVE.

    Uses multi-word matching first, then single-token matching.
    """
    priority = ["EXPLAIN", "EXPORT", "COUNT", "SUMMARY"]

    # Check multi-word keywords first (e.g. "how many", "en retard")
    for keyword, action in sorted(action_keywords.items(), key=lambda x: -len(x[0])):
        if " " in keyword and keyword in normalized:
            if action in priority:
                return action

    # Check single-word keywords
    best_action = None
    best_priority = len(priority)

    for token in tokens:
        # Check each token against action_keywords
        if token in action_keywords:
            action = action_keywords[token]
            if action in priority:
                idx = priority.index(action)
                if idx < best_priority:
                    best_priority = idx
                    best_action = action
        else:
            # Partial match for stems (e.g. "expliqu" matches "explique", "expliquer")
            for keyword, action in action_keywords.items():
                if " " not in keyword and (token.startswith(keyword) or keyword.startswith(token)):
                    if action in priority:
                        idx = priority.index(action)
                        if idx < best_priority:
                            best_priority = idx
                            best_action = action

    return best_action or "RETRIEVE"


def _extract_entities(
    tokens: List[str],
    normalized: str,
    lot_aliases: Dict[str, str],
    approver_aliases: Dict[str, str],
    category_aliases: Dict[str, str],
    status_synonyms: Dict[str, Dict],
) -> Dict[str, Any]:
    """Extract entities from tokens using dictionaries.

    Returns dict with keys: lot, approver, category, status_filter
    """
    entities: Dict[str, Any] = {}

    # Try multi-word matches first (longer matches win)
    # Build all possible n-grams from tokens
    ngrams = []
    for size in range(min(4, len(tokens)), 0, -1):
        for i in range(len(tokens) - size + 1):
            ngram = " ".join(tokens[i:i + size])
            ngrams.append((ngram, i, size))

    matched_positions = set()

    for ngram, start, size in ngrams:
        # Skip if any position already matched
        positions = set(range(start, start + size))
        if positions & matched_positions:
            continue

        # Check lot aliases
        if "lot" not in entities and ngram in lot_aliases:
            entities["lot"] = lot_aliases[ngram]
            matched_positions |= positions
            continue

        # Also check "lot X" patterns
        if "lot" not in entities and ngram.startswith("lot "):
            lot_part = ngram[4:].strip()
            if lot_part in lot_aliases:
                entities["lot"] = lot_aliases[lot_part]
                matched_positions |= positions
                continue

        # Check approver aliases
        if "approver" not in entities and ngram in approver_aliases:
            entities["approver"] = approver_aliases[ngram]
            matched_positions |= positions
            continue

        # Check category aliases
        if "category" not in entities and ngram in category_aliases:
            entities["category"] = category_aliases[ngram]
            matched_positions |= positions
            continue

        # Check status synonyms
        if "status_filter" not in entities and ngram in status_synonyms:
            entities["status_filter"] = status_synonyms[ngram]
            matched_positions |= positions
            continue

    # Single token matches for remaining unmatched tokens
    for i, token in enumerate(tokens):
        if i in matched_positions:
            continue

        if "lot" not in entities and token in lot_aliases:
            entities["lot"] = lot_aliases[token]
            continue

        if "lot" not in entities and token.startswith("lot"):
            # "lot42" -> try "42"
            rest = token[3:]
            if rest and rest in lot_aliases:
                entities["lot"] = lot_aliases[rest]
                continue

        if "approver" not in entities and token in approver_aliases:
            entities["approver"] = approver_aliases[token]
            continue

        if "category" not in entities and token in category_aliases:
            entities["category"] = category_aliases[token]
            continue

        if "status_filter" not in entities and token in status_synonyms:
            entities["status_filter"] = status_synonyms[token]
            continue

    # Also check multi-word status in full normalized text
    if "status_filter" not in entities:
        for key, val in status_synonyms.items():
            if " " in key and key in normalized:
                entities["status_filter"] = val
                break

    # Also check multi-word category in full normalized text
    if "category" not in entities:
        for key, val in category_aliases.items():
            if " " in key and key in normalized:
                entities["category"] = val
                break

    return entities


def _resolve_command(
    action: str,
    has_doc: bool,
    has_n: bool,
    entities: Dict[str, Any],
) -> str:
    """Resolve command from action + entities using the command resolution table.

    Returns command_id ("C1"-"C12" or "REJECTED").
    """
    has_lot = "lot" in entities
    has_approver = "approver" in entities
    has_category = "category" in entities
    has_status = "status_filter" in entities

    # Check if status is "overdue" type
    is_overdue_status = False
    if has_status:
        sf = entities["status_filter"]
        is_overdue_status = sf.get("filter") == "is_overdue"

    # Command resolution table (first match wins)
    # Row 1: EXPLAIN + doc -> C7
    if action == "EXPLAIN" and has_doc:
        return "C7"
    # Row 2: EXPLAIN + no doc -> REJECTED
    if action == "EXPLAIN" and not has_doc:
        return "REJECTED"
    # Row 3: EXPORT -> C12
    if action == "EXPORT":
        return "C12"
    # Row 4: COUNT -> C10
    if action == "COUNT":
        return "C10"
    # Row 5: SUMMARY + lot -> C8
    if action == "SUMMARY" and has_lot:
        return "C8"
    # Row 6: SUMMARY + approver -> C9
    if action == "SUMMARY" and has_approver:
        return "C9"
    # Row 7: SUMMARY + no lot + no approver -> REJECTED
    if action == "SUMMARY":
        return "REJECTED"
    # Row 8: RETRIEVE + N -> C11 (Top-N override, P2)
    if action == "RETRIEVE" and has_n:
        return "C11"
    # Row 9: RETRIEVE + doc -> C6
    if action == "RETRIEVE" and has_doc:
        return "C6"
    # Row 10: RETRIEVE + lot + category -> C5
    if action == "RETRIEVE" and has_lot and has_category:
        return "C5"
    # Row 11: RETRIEVE + lot + status -> C5
    if action == "RETRIEVE" and has_lot and has_status:
        return "C5"
    # Row 12: RETRIEVE + lot only -> C1
    if action == "RETRIEVE" and has_lot:
        return "C1"
    # Row 13: RETRIEVE + category only -> C2
    if action == "RETRIEVE" and has_category:
        return "C2"
    # Row 14: RETRIEVE + approver only -> C3
    if action == "RETRIEVE" and has_approver:
        return "C3"
    # Row 15: RETRIEVE + overdue status -> C4
    if action == "RETRIEVE" and is_overdue_status:
        return "C4"
    # Row 16: RETRIEVE + other status (urgent/critical) -> C5 with status filter
    if action == "RETRIEVE" and has_status:
        return "C5"
    # Row 17: RETRIEVE + nothing -> REJECTED
    return "REJECTED"


def parse_query(
    nq: NormalizedQuery,
    lot_aliases: Dict[str, str],
    approver_aliases: Dict[str, str],
    category_aliases: Dict[str, str],
    status_synonyms: Dict[str, Dict],
    action_keywords: Dict[str, str],
) -> ParseResult:
    """L2 deterministic parser.

    Steps:
    1. Detect action type
    2. Extract entities
    2b. Top-N override
    3. Command resolution
    """
    # Step 1: Detect action
    action = _detect_action(nq.tokens, nq.normalized, action_keywords)

    # Step 2: Extract entities
    entities = _extract_entities(
        nq.tokens, nq.normalized,
        lot_aliases, approver_aliases,
        category_aliases, status_synonyms,
    )

    has_doc = nq.doc_ref is not None
    has_n = nq.n_value is not None

    # Step 3: Resolve command
    command_id = _resolve_command(action, has_doc, has_n, entities)

    # Build parameters
    params: Dict[str, Any] = {}
    if nq.doc_ref:
        params["document"] = nq.doc_ref
    if nq.n_value:
        params["n"] = nq.n_value
    if "lot" in entities:
        params["lot"] = entities["lot"]
    if "approver" in entities:
        params["approver"] = entities["approver"]
    if "category" in entities:
        params["category"] = entities["category"]
    if "status_filter" in entities:
        params["status_filter"] = entities["status_filter"]

    confidence = 0.0 if command_id == "REJECTED" else 1.0

    logger.debug(
        "Parse: action=%s, entities=%s, command=%s",
        action, entities, command_id,
    )

    return ParseResult(
        command_id=command_id,
        action=action,
        parameters=params,
        confidence=confidence,
    )
