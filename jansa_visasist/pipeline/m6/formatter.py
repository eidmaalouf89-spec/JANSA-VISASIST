"""
Module 6 — Response formatting.

1. Apply truncation (RL1/RL2/RL3)
2. Generate response_text from per-command templates
3. Build data_references from FINAL results
4. Assemble ChatbotResponse
"""

import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

from jansa_visasist.config_m6 import (
    CHATBOT_INLINE_LIMIT,
    CHATBOT_PREVIEW_COUNT,
    FIELDS_FILTER_ROW,
    FIELDS_C6_M3, FIELDS_C6_M4, FIELDS_C6_M5, FIELDS_C6_DISAMBIGUATION,
    FIELDS_C7, FIELDS_C8, FIELDS_C9,
    FIELDS_TRUNCATION_AGGREGATE,
    FIELDS_C10, FIELDS_C12,
)

logger = logging.getLogger("jansa.m6.formatter")


@dataclass
class ChatbotResponse:
    """Standard response schema for every M6 response."""
    command_id: str                                 # "C1"-"C12" | "REJECTED"
    classification_layer: str                       # "L1" | "L2" | "L3"
    classification_confidence: Optional[float]
    parameters: Dict[str, Any]
    result_count: int                               # Total before truncation
    results: List[Dict]                             # After truncation/preview
    results_truncated: bool
    response_text: str
    sources_used: List[str]                         # ["M3"] or ["M3","M4","M5"]
    data_references: List[Dict]
    export_metadata: Optional[Dict]                 # C12 only
    warnings: List[str]                             # Empty list if no warnings

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


def _get_fields_used(command_id: str, disambiguation: bool = False) -> List[str]:
    """Get the fields_used list for a given command."""
    if command_id in ("C1", "C2", "C3", "C4", "C5", "C11"):
        return FIELDS_FILTER_ROW
    if command_id == "C6":
        if disambiguation:
            return FIELDS_C6_DISAMBIGUATION
        return FIELDS_C6_M3  # May also include M4/M5 but those are separate refs
    if command_id == "C7":
        return FIELDS_C7
    if command_id == "C8":
        return FIELDS_C8
    if command_id == "C9":
        return FIELDS_C9
    if command_id == "C10":
        return FIELDS_C10
    if command_id == "C12":
        return FIELDS_C12
    return []


def _apply_truncation(
    results: List[Dict],
    result_count: int,
    command_id: str,
) -> tuple:
    """Apply truncation rules.

    Returns (truncated_results, results_truncated).
    """
    # RL3: C12 always returns results=[], truncated=false
    if command_id == "C12":
        return [], False

    # C10: always returns results=[], truncated=false
    if command_id == "C10":
        return [], False

    # RL1: count <= INLINE_LIMIT -> all inline
    if result_count <= CHATBOT_INLINE_LIMIT:
        return results, False

    # RL2: count > INLINE_LIMIT -> preview
    return results[:CHATBOT_PREVIEW_COUNT], True


def _build_data_references(
    final_results: List[Dict],
    result_count: int,
    command_id: str,
    results_truncated: bool,
    disambiguation: bool = False,
) -> List[Dict]:
    """Build data_references from FINAL results only (P7).

    Each entry: {row_id, document, source_sheet, fields_used, source_module}
    """
    refs = []
    fields_used = _get_fields_used(command_id, disambiguation)

    # C10 and C12: aggregate reference only
    if command_id in ("C10", "C12"):
        refs.append({
            "row_id": None,
            "document": None,
            "source_sheet": None,
            "fields_used": fields_used,
            "source_module": "M3",
        })
        return refs

    # C8, C9: summary reference
    if command_id in ("C8", "C9"):
        for item in final_results:
            refs.append({
                "row_id": None,
                "document": None,
                "source_sheet": item.get("source_sheet"),
                "fields_used": fields_used,
                "source_module": "M3",
            })
        return refs

    # Per-row references for the final (possibly truncated) results
    for item in final_results:
        ref = {
            "row_id": item.get("row_id"),
            "document": item.get("document"),
            "source_sheet": item.get("source_sheet"),
            "fields_used": fields_used,
            "source_module": "M3",
        }
        refs.append(ref)

    # If truncated, add ONE aggregate entry
    if results_truncated:
        refs.append({
            "row_id": None,
            "document": None,
            "source_sheet": None,
            "fields_used": FIELDS_TRUNCATION_AGGREGATE,
            "source_module": "M3",
        })

    return refs


def _generate_response_text(
    command_id: str,
    result_count: int,
    results_truncated: bool,
    params: Dict[str, Any],
    disambiguation: bool = False,
) -> str:
    """Generate human-readable response text."""
    if command_id == "REJECTED":
        return "Desole, je n'ai pas compris votre demande. Essayez : 'lot 42', 'bloques', 'top 10 en retard', 'resume socotec'."

    if command_id == "C1":
        lot = params.get("lot", "?")
        if result_count == 0:
            return f"Aucun document en attente pour {lot}."
        msg = f"{result_count} document(s) en attente pour {lot}."
        if results_truncated:
            msg += f" Affichage des {CHATBOT_PREVIEW_COUNT} premiers. Utilisez 'export {lot}' pour tout telecharger."
        return msg

    if command_id == "C2":
        cat = params.get("category", "?")
        if result_count == 0:
            return f"Aucun document avec la categorie {cat}."
        msg = f"{result_count} document(s) en categorie {cat}."
        if results_truncated:
            msg += f" Affichage des {CHATBOT_PREVIEW_COUNT} premiers."
        return msg

    if command_id == "C3":
        approver = params.get("approver", "?")
        if result_count == 0:
            return f"Aucun document en attente pour {approver}."
        msg = f"{result_count} document(s) en attente de {approver}."
        if results_truncated:
            msg += f" Affichage des {CHATBOT_PREVIEW_COUNT} premiers."
        return msg

    if command_id == "C4":
        if result_count == 0:
            return "Aucun document en retard."
        msg = f"{result_count} document(s) en retard."
        if results_truncated:
            msg += f" Affichage des {CHATBOT_PREVIEW_COUNT} premiers."
        return msg

    if command_id == "C5":
        if result_count == 0:
            return "Aucun document correspondant aux criteres."
        msg = f"{result_count} document(s) correspondant aux criteres."
        if results_truncated:
            msg += f" Affichage des {CHATBOT_PREVIEW_COUNT} premiers."
        return msg

    if command_id == "C6":
        if result_count == 0:
            doc = params.get("document", "?")
            return f"Document '{doc}' non trouve dans la file d'attente."
        if disambiguation:
            return f"Document ambigu : {result_count} correspondances trouvees. Precisez le lot."
        return "Detail du document."

    if command_id == "C7":
        if result_count == 0:
            doc = params.get("document", "?")
            return f"Document '{doc}' non trouve. Impossible d'expliquer."
        if disambiguation:
            return f"Document ambigu : {result_count} correspondances trouvees. Precisez le lot."
        return "Explication de la priorite du document."

    if command_id == "C8":
        lot = params.get("lot", "?")
        return f"Resume du lot {lot}."

    if command_id == "C9":
        approver = params.get("approver", "?")
        return f"Resume de l'approbateur {approver}."

    if command_id == "C10":
        return f"Nombre de documents correspondants : {result_count}."

    if command_id == "C11":
        n = params.get("n", "?")
        return f"Top {n} documents (sur {result_count} correspondants)."

    if command_id == "C12":
        return f"Export genere : {result_count} lignes."

    return f"Commande {command_id} executee. {result_count} resultat(s)."


def format_response(
    command_id: str,
    layer: str,
    confidence: Optional[float],
    params: Dict[str, Any],
    results: List[Dict],
    result_count: int,
    extra: Dict[str, Any],
    export_metadata: Optional[Dict] = None,
) -> ChatbotResponse:
    """Assemble the full ChatbotResponse."""
    sources_used = extra.get("sources_used", ["M3"])
    exec_warnings = extra.get("warnings", [])
    disambiguation = extra.get("disambiguation", False)

    # Apply truncation
    truncated_results, results_truncated = _apply_truncation(results, result_count, command_id)

    # Generate response text
    response_text = _generate_response_text(
        command_id, result_count, results_truncated, params, disambiguation,
    )

    # Build data_references from FINAL results
    data_references = _build_data_references(
        truncated_results, result_count, command_id, results_truncated, disambiguation,
    )

    # Combine warnings
    warnings = list(exec_warnings)

    return ChatbotResponse(
        command_id=command_id,
        classification_layer=layer,
        classification_confidence=confidence,
        parameters=params,
        result_count=result_count,
        results=truncated_results,
        results_truncated=results_truncated,
        response_text=response_text,
        sources_used=sources_used,
        data_references=data_references,
        export_metadata=export_metadata,
        warnings=warnings,
    )
