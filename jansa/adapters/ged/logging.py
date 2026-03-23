"""Centralized log_event() interface (GP-LOG).

All modules (NM1-NM7) must use this interface exclusively.
Never use print(), logging.getLogger(), or local log lists.
"""

from datetime import datetime
from typing import Optional

_event_log: list = []


def log_event(
    doc_id: Optional[int],
    module: str,
    severity: str,
    code: str,
    message: str,
    raw_value: Optional[str] = None,
    field: Optional[str] = None,
) -> None:
    """Emit a pipeline event to the centralized log."""
    _event_log.append({
        'timestamp': datetime.utcnow().isoformat(),
        'doc_id': doc_id,
        'module': module,
        'severity': severity,
        'code': code,
        'message': message,
        'raw_value': raw_value,
        'field': field,
    })


def get_log() -> list:
    """Return all accumulated log events."""
    return list(_event_log)


def clear_log() -> None:
    """Clear log - call at start of each pipeline run."""
    _event_log.clear()


def get_log_as_dataframe():
    """Return log as pandas DataFrame for export."""
    import pandas as pd
    return pd.DataFrame(_event_log)
