"""
Module 7 — Session Persistence.

JSON file storage for batch sessions with atomic writes.
"""

import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

from jansa_visasist.config_m7 import SESSION_STORAGE_DIR, TERMINAL_STATUSES
from jansa_visasist.pipeline.m7.schemas import BatchSession

logger = logging.getLogger(__name__)


def _get_storage_dir() -> str:
    """Return absolute path to session storage directory."""
    return os.path.abspath(SESSION_STORAGE_DIR)


def ensure_storage_dir() -> None:
    """Create SESSION_STORAGE_DIR if it does not exist."""
    storage_dir = _get_storage_dir()
    os.makedirs(storage_dir, exist_ok=True)
    logger.debug("Storage directory ensured: %s", storage_dir)


def _atomic_write(path: str, data: bytes) -> None:
    """Write data to file atomically via temp file + os.replace."""
    dir_name = os.path.dirname(path)
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        os.write(fd, data)
        os.close(fd)
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _session_path(session_id: str) -> str:
    """Return file path for a session."""
    return os.path.join(_get_storage_dir(), f"{session_id}.json")


def save_session(session: BatchSession) -> None:
    """Serialize and atomically write session to JSON file."""
    ensure_storage_dir()
    path = _session_path(session.session_id)
    data_dict = session.to_dict()
    json_bytes = json.dumps(data_dict, indent=2, ensure_ascii=False).encode("utf-8")
    _atomic_write(path, json_bytes)
    logger.info("Session saved: %s → %s", session.session_id, path)


def load_session(session_id: str) -> Optional[BatchSession]:
    """Load and deserialize a session from JSON file. Return None if not found."""
    path = _session_path(session_id)
    if not os.path.exists(path):
        logger.warning("Session file not found: %s", path)
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return BatchSession.from_dict(data)
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        logger.error("Failed to load session %s: %s", session_id, exc)
        return None


def list_sessions() -> List[Dict[str, Any]]:
    """List all session files with summary info."""
    storage_dir = _get_storage_dir()
    if not os.path.isdir(storage_dir):
        return []
    results: List[Dict[str, Any]] = []
    for filename in sorted(os.listdir(storage_dir)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(storage_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            results.append({
                "session_id": data.get("session_id", filename.replace(".json", "")),
                "status": data.get("status", "UNKNOWN"),
                "created_at": data.get("created_at", ""),
                "updated_at": data.get("updated_at", ""),
            })
        except (json.JSONDecodeError, KeyError) as exc:
            logger.warning("Skipping corrupt session file %s: %s", filename, exc)
    return results


def find_active_session() -> Optional[BatchSession]:
    """Find the single CREATED or IN_PROGRESS session. Return None if none."""
    storage_dir = _get_storage_dir()
    if not os.path.isdir(storage_dir):
        return None
    for filename in sorted(os.listdir(storage_dir)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(storage_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            status = data.get("status", "")
            if status not in TERMINAL_STATUSES:
                return BatchSession.from_dict(data)
        except (json.JSONDecodeError, KeyError, ValueError) as exc:
            logger.warning("Skipping corrupt session file %s: %s", filename, exc)
    return None
