"""
Module 7 — Data Schemas.

Dataclasses for batch workflow session management.
All schemas use to_dict() / from_dict() for JSON serialization.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import copy

from jansa_visasist.config_m7 import (
    VALID_DECISIONS,
    VALID_VISA_VALUES,
    VALID_DECISION_SOURCES,
    VALID_SESSION_STATUSES,
    SESSION_SCHEMA_VERSION,
)


@dataclass
class BatchDecision:
    """A single decision on a batch item."""

    decision_type: str
    visa_value: Optional[str]
    comment: Optional[str]
    decided_at: str
    suggested_action: Optional[str]
    proposed_visa: Optional[str]
    decision_source: str

    def __post_init__(self) -> None:
        if self.decision_type not in VALID_DECISIONS:
            raise ValueError(
                f"Invalid decision_type '{self.decision_type}'. "
                f"Must be one of {VALID_DECISIONS}"
            )
        if self.decision_type == "VISA_ISSUED":
            if self.visa_value not in VALID_VISA_VALUES:
                raise ValueError(
                    f"Invalid visa_value '{self.visa_value}' for VISA_ISSUED. "
                    f"Must be one of {VALID_VISA_VALUES}"
                )
        else:
            if self.visa_value is not None:
                raise ValueError(
                    f"visa_value must be None for decision_type '{self.decision_type}'"
                )
        if self.decision_source not in VALID_DECISION_SOURCES:
            raise ValueError(
                f"Invalid decision_source '{self.decision_source}'. "
                f"Must be one of {VALID_DECISION_SOURCES}"
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_type": self.decision_type,
            "visa_value": self.visa_value,
            "comment": self.comment,
            "decided_at": self.decided_at,
            "suggested_action": self.suggested_action,
            "proposed_visa": self.proposed_visa,
            "decision_source": self.decision_source,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BatchDecision":
        return cls(
            decision_type=data["decision_type"],
            visa_value=data.get("visa_value"),
            comment=data.get("comment"),
            decided_at=data["decided_at"],
            suggested_action=data.get("suggested_action"),
            proposed_visa=data.get("proposed_visa"),
            decision_source=data.get("decision_source", "manual"),
        )


@dataclass
class BatchItem:
    """A single item in a batch session."""

    row_id: str
    document: Optional[str]
    titre: Optional[str]
    source_sheet: str
    category: str
    priority_score: float
    consensus_type: str
    is_overdue: bool
    decision: Optional[BatchDecision]
    order_index: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "row_id": self.row_id,
            "document": self.document,
            "titre": self.titre,
            "source_sheet": self.source_sheet,
            "category": self.category,
            "priority_score": self.priority_score,
            "consensus_type": self.consensus_type,
            "is_overdue": self.is_overdue,
            "decision": self.decision.to_dict() if self.decision else None,
            "order_index": self.order_index,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BatchItem":
        decision_data = data.get("decision")
        decision = BatchDecision.from_dict(decision_data) if decision_data else None
        return cls(
            row_id=data["row_id"],
            document=data.get("document"),
            titre=data.get("titre"),
            source_sheet=data["source_sheet"],
            category=data["category"],
            priority_score=float(data["priority_score"]),
            consensus_type=data["consensus_type"],
            is_overdue=bool(data["is_overdue"]),
            decision=decision,
            order_index=int(data["order_index"]),
        )


@dataclass
class BatchSession:
    """A batch workflow session."""

    session_id: str
    batch_id: str
    status: str
    session_schema_version: str
    dataset_signature: str
    pipeline_run_id: str
    user_id: Optional[str]
    created_at: str
    updated_at: str
    completed_at: Optional[str]
    invalidated_at: Optional[str]
    invalidated_reason: Optional[str]
    items: List[BatchItem]
    current_index: int
    filter_params: Optional[Dict]
    total_items: int
    decided_count: int
    deferred_count: int
    skipped_count: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "batch_id": self.batch_id,
            "status": self.status,
            "session_schema_version": self.session_schema_version,
            "dataset_signature": self.dataset_signature,
            "pipeline_run_id": self.pipeline_run_id,
            "user_id": self.user_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "completed_at": self.completed_at,
            "invalidated_at": self.invalidated_at,
            "invalidated_reason": self.invalidated_reason,
            "items": [item.to_dict() for item in self.items],
            "current_index": self.current_index,
            "filter_params": copy.deepcopy(self.filter_params) if self.filter_params else None,
            "total_items": self.total_items,
            "decided_count": self.decided_count,
            "deferred_count": self.deferred_count,
            "skipped_count": self.skipped_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BatchSession":
        items = [BatchItem.from_dict(i) for i in data.get("items", [])]
        return cls(
            session_id=data["session_id"],
            batch_id=data["batch_id"],
            status=data["status"],
            session_schema_version=data["session_schema_version"],
            dataset_signature=data["dataset_signature"],
            pipeline_run_id=data["pipeline_run_id"],
            user_id=data.get("user_id"),
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            completed_at=data.get("completed_at"),
            invalidated_at=data.get("invalidated_at"),
            invalidated_reason=data.get("invalidated_reason"),
            items=items,
            current_index=int(data.get("current_index", 0)),
            filter_params=data.get("filter_params"),
            total_items=int(data["total_items"]),
            decided_count=int(data.get("decided_count", 0)),
            deferred_count=int(data.get("deferred_count", 0)),
            skipped_count=int(data.get("skipped_count", 0)),
        )


@dataclass
class SessionReport:
    """Report generated at session completion."""

    session_id: str
    batch_id: str
    created_at: str
    completed_at: str
    duration_seconds: int
    dataset_signature: str
    pipeline_run_id: str
    invalidated_reason: Optional[str]
    total_items: int
    decided_count: int
    deferred_count: int
    skipped_count: int
    visa_breakdown: Dict[str, int]
    category_breakdown: Dict[str, Dict]
    decision_source_breakdown: Dict[str, int]
    decisions: List[Dict]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "batch_id": self.batch_id,
            "created_at": self.created_at,
            "completed_at": self.completed_at,
            "duration_seconds": self.duration_seconds,
            "dataset_signature": self.dataset_signature,
            "pipeline_run_id": self.pipeline_run_id,
            "invalidated_reason": self.invalidated_reason,
            "total_items": self.total_items,
            "decided_count": self.decided_count,
            "deferred_count": self.deferred_count,
            "skipped_count": self.skipped_count,
            "visa_breakdown": dict(self.visa_breakdown),
            "category_breakdown": {k: dict(v) for k, v in self.category_breakdown.items()},
            "decision_source_breakdown": dict(self.decision_source_breakdown),
            "decisions": list(self.decisions),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SessionReport":
        return cls(
            session_id=data["session_id"],
            batch_id=data["batch_id"],
            created_at=data["created_at"],
            completed_at=data["completed_at"],
            duration_seconds=int(data["duration_seconds"]),
            dataset_signature=data["dataset_signature"],
            pipeline_run_id=data["pipeline_run_id"],
            invalidated_reason=data.get("invalidated_reason"),
            total_items=int(data["total_items"]),
            decided_count=int(data.get("decided_count", 0)),
            deferred_count=int(data.get("deferred_count", 0)),
            skipped_count=int(data.get("skipped_count", 0)),
            visa_breakdown=data.get("visa_breakdown", {}),
            category_breakdown=data.get("category_breakdown", {}),
            decision_source_breakdown=data.get("decision_source_breakdown", {}),
            decisions=data.get("decisions", []),
        )


@dataclass
class OperationResult:
    """Standardized return type for all M7 public functions."""

    status: str  # "OK" | "ERROR" | "ALREADY_DECIDED"
    error_code: Optional[str]
    message: str
    data: Any = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "status": self.status,
            "error_code": self.error_code,
            "message": self.message,
        }
        if self.data is not None:
            if hasattr(self.data, "to_dict"):
                result["data"] = self.data.to_dict()
            else:
                result["data"] = self.data
        else:
            result["data"] = None
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OperationResult":
        return cls(
            status=data["status"],
            error_code=data.get("error_code"),
            message=data["message"],
            data=data.get("data"),
        )
