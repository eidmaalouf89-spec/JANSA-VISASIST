"""
Step 7e: Text Cleaning — with dict cache
Trim, remove control characters, collapse spaces.

Performance: many cells repeat the same raw value across rows
(e.g. lot codes, status codes, format types).  A dict cache
avoids redundant regex work.  The cache is bounded per-pipeline
run (cleared between runs via reset_text_cache).
"""

import re
from typing import Optional, Dict

# Pre-compile regexes once (avoids re-compile on every call)
_RE_CONTROL = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f]')
_RE_MULTI_SPACE = re.compile(r' +')

# Module-level cache: raw string → cleaned result
_TEXT_CACHE: Dict[str, Optional[str]] = {}


def reset_text_cache() -> None:
    """Clear the text cleaning cache.  Call between pipeline runs."""
    _TEXT_CACHE.clear()


def clean_text(raw_value) -> Optional[str]:
    """
    Clean a raw text field.
    Returns cleaned string or None (GP2 enforcement).
    Uses a dict cache to skip redundant regex work on repeated values.
    """
    if raw_value is None:
        return None

    # Convert to string once
    key = str(raw_value)

    # Cache hit → return immediately
    cached = _TEXT_CACHE.get(key)
    if cached is not None:
        return cached
    # We need a sentinel for "cached as None" since None is a valid result
    if key in _TEXT_CACHE:
        return None

    val = key.strip()
    val = _RE_CONTROL.sub('', val)
    val = _RE_MULTI_SPACE.sub(' ', val)

    if val == "":
        _TEXT_CACHE[key] = None
        return None

    _TEXT_CACHE[key] = val
    return val
