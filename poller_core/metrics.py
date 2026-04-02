import threading
from collections import deque
from typing import Dict


_LOCK = threading.Lock()
_WINDOW = 4000
_STORE: Dict[str, deque] = {}


def observe_ms(name: str, value_ms):
    try:
        v = float(value_ms)
    except Exception:
        return
    if v < 0:
        return
    key = str(name)
    with _LOCK:
        q = _STORE.get(key)
        if q is None:
            q = deque(maxlen=_WINDOW)
            _STORE[key] = q
        q.append(v)


def _percentile(sorted_vals, p: float):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    p = max(0.0, min(100.0, float(p)))
    idx = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


def snapshot(name: str):
    key = str(name)
    with _LOCK:
        vals = list(_STORE.get(key, []))
    if not vals:
        return {"count": 0, "p50": None, "p95": None}
    vals.sort()
    return {
        "count": len(vals),
        "p50": _percentile(vals, 50),
        "p95": _percentile(vals, 95),
    }


def format_line(name: str) -> str:
    s = snapshot(name)
    if not s["count"]:
        return f"{name}: n=0"
    p50 = int(round(float(s["p50"] or 0)))
    p95 = int(round(float(s["p95"] or 0)))
    return f"{name}: n={s['count']} p50={p50}ms p95={p95}ms"

