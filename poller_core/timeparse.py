from datetime import datetime
from typing import Optional

from dateutil import parser as _du_parser


def parse_iso_dt(value) -> datetime:
    """
    Fast ISO parser:
    - first try datetime.fromisoformat (fast path)
    - fallback to dateutil.isoparse for compatibility
    """
    if isinstance(value, datetime):
        return value
    s = str(value or "").strip()
    if not s:
        raise ValueError("empty datetime value")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return _du_parser.isoparse(s)


def parse_iso_dt_or_none(value) -> Optional[datetime]:
    try:
        return parse_iso_dt(value)
    except Exception:
        return None

