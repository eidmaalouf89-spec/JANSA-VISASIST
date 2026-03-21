"""
Module 6 — Dictionary builders and validators.

Builds lot_aliases, approver_aliases from data.
Validates all dictionaries for ambiguity (collision = ERROR, keep neither).
"""

import logging
import re
import unicodedata
from typing import Any, Dict, List, Set, Tuple

from jansa_visasist.config import CANONICAL_APPROVERS, APPROVER_VARIANT_MAP
from jansa_visasist.config_m6 import CATEGORY_ALIASES, STATUS_SYNONYMS, ACTION_KEYWORDS

logger = logging.getLogger("jansa.m6.dictionaries")


def _normalize_key(text: str) -> str:
    """Normalize a dictionary key: lowercase, NFKD (strip accents), strip whitespace."""
    if not text:
        return ""
    text = text.strip().lower()
    # NFKD decomposition: remove combining marks (accents)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)
    return text


def build_lot_aliases(queue_data: List[Dict[str, Any]]) -> Dict[str, str]:
    """Build lot alias dictionary from M3 queue source_sheet values.

    For each source_sheet (e.g. "LOT 42-PLB-UTB"):
      - Exact (normalized): "lot 42-plb-utb" -> "LOT 42-PLB-UTB"
      - Number only: "42" -> "LOT 42-PLB-UTB"
      - Abbreviation (3-letter code after number): "plb" -> "LOT 42-PLB-UTB"

    Collision = ERROR, keep neither.
    """
    sheets: Set[str] = set()
    for item in queue_data:
        ss = item.get("source_sheet")
        if ss:
            sheets.add(ss)

    aliases: Dict[str, str] = {}
    collisions: Set[str] = set()

    for sheet in sorted(sheets):
        candidates: List[Tuple[str, str]] = []

        # Exact normalized
        norm = _normalize_key(sheet)
        candidates.append((norm, sheet))

        # Parse LOT pattern: "LOT XX-YYY-ZZZ" or "LOT I01-VDSTP" etc.
        # Extract number and abbreviation parts
        match = re.match(r"lot\s+(\S+?)[-\s](\w+)", norm)
        if match:
            number_part = match.group(1)
            abbrev_part = match.group(2)
            candidates.append((_normalize_key(number_part), sheet))
            candidates.append((_normalize_key(abbrev_part), sheet))

            # Also try extracting just digits if present
            digits = re.findall(r"\d+", number_part)
            for d in digits:
                if d != number_part:
                    candidates.append((d, sheet))

        # Also try: everything after "lot "
        if norm.startswith("lot "):
            rest = norm[4:].strip()
            if rest:
                candidates.append((rest, sheet))

        for alias_key, target in candidates:
            if not alias_key:
                continue
            if alias_key in collisions:
                continue
            if alias_key in aliases:
                if aliases[alias_key] != target:
                    logger.warning(
                        "Lot alias collision: '%s' maps to both '%s' and '%s' — removing",
                        alias_key, aliases[alias_key], target,
                    )
                    collisions.add(alias_key)
                    del aliases[alias_key]
            else:
                aliases[alias_key] = target

    logger.info("Built %d lot aliases from %d sheets", len(aliases), len(sheets))
    return aliases


def build_approver_aliases() -> Dict[str, str]:
    """Build approver alias dictionary from config APPROVER_VARIANT_MAP.

    Adds:
      - All existing variants (normalized to lowercase)
      - Short forms: last meaningful segment of canonical key

    Collision = ERROR, keep neither.
    """
    aliases: Dict[str, str] = {}
    collisions: Set[str] = set()

    def _add(alias_key: str, canonical: str):
        alias_key = _normalize_key(alias_key)
        if not alias_key:
            return
        if alias_key in collisions:
            return
        if alias_key in aliases:
            if aliases[alias_key] != canonical:
                logger.warning(
                    "Approver alias collision: '%s' maps to both '%s' and '%s' — removing",
                    alias_key, aliases[alias_key], canonical,
                )
                collisions.add(alias_key)
                del aliases[alias_key]
        else:
            aliases[alias_key] = canonical

    # From APPROVER_VARIANT_MAP
    for variant, canonical in APPROVER_VARIANT_MAP.items():
        _add(variant, canonical)

    # Add short forms for each canonical approver
    short_forms = {
        "MOEX_GEMO": ["gemo", "moex"],
        "ARCHI_MOX": ["mox", "archi"],
        "BET_STR_TERRELL": ["terrell", "str"],
        "BET_GEOLIA_G4": ["geolia", "g4"],
        "ACOUSTICIEN_AVLS": ["avls", "acousticien"],
        "AMO_HQE_LE_SOMMER": ["le sommer", "sommer", "hqe"],
        "BET_POLLUTION_DIE": ["die", "pollution"],
        "SOCOTEC": ["socotec"],
        "BET_ELIOTH": ["elioth"],
        "BET_EGIS": ["egis"],
        "BET_ASCAUDIT": ["ascaudit"],
        "BET_ASCENSEUR": ["ascenseur"],
        "BET_SPK": ["spk"],
        "PAYSAGISTE_MUGO": ["mugo", "paysagiste"],
    }

    for canonical, shorts in short_forms.items():
        for s in shorts:
            _add(s, canonical)

    logger.info("Built %d approver aliases", len(aliases))
    return aliases


def build_category_aliases() -> Dict[str, str]:
    """Return normalized category aliases (static dictionary)."""
    result = {}
    for key, val in CATEGORY_ALIASES.items():
        result[_normalize_key(key)] = val
    return result


def build_status_synonyms() -> Dict[str, Dict]:
    """Return normalized status synonyms (static dictionary)."""
    result = {}
    for key, val in STATUS_SYNONYMS.items():
        result[_normalize_key(key)] = val
    return result


def build_action_keywords() -> Dict[str, str]:
    """Return normalized action keywords (static dictionary)."""
    result = {}
    for key, val in ACTION_KEYWORDS.items():
        result[_normalize_key(key)] = val
    return result


def validate_dictionaries(
    lot_aliases: Dict[str, str],
    approver_aliases: Dict[str, str],
    category_aliases: Dict[str, str],
    status_synonyms: Dict[str, Dict],
    action_keywords: Dict[str, str],
) -> List[str]:
    """Validate all dictionaries. Returns list of warnings (empty = OK)."""
    warnings = []
    if not lot_aliases:
        warnings.append("lot_aliases is empty")
    if not approver_aliases:
        warnings.append("approver_aliases is empty")
    if not category_aliases:
        warnings.append("category_aliases is empty")
    if not status_synonyms:
        warnings.append("status_synonyms is empty")
    if not action_keywords:
        warnings.append("action_keywords is empty")

    logger.info(
        "Dictionary summary: lots=%d, approvers=%d, categories=%d, statuses=%d, actions=%d",
        len(lot_aliases), len(approver_aliases), len(category_aliases),
        len(status_synonyms), len(action_keywords),
    )
    return warnings
