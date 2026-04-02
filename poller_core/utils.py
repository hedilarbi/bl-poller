import json
import re
from typing import Optional, Iterable, List, Tuple
from datetime import datetime, timedelta
from dateutil.tz import gettz

from .config import DEBUG_ENDS
from .timeparse import parse_iso_dt


def _to_str(x):
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x)


def _to_int(x, default=0):
    try:
        if isinstance(x, (int, float)):
            return int(x)
        s = _to_str(x)
        if s is None:
            return default
        m = re.search(r"-?\d+", s)
        return int(m.group(0)) if m else default
    except Exception:
        return default


def _parse_hhmm(s):
    """Parse 'HH:MM' → (h,m) or None. Robust to bytes and junk."""
    try:
        s = _to_str(s)
        parts = (s or "").split(":")
        if len(parts) < 2:
            return None
        hh = _to_int(parts[0], None)
        mm = _to_int(parts[1], None)
        if hh is None or mm is None:
            return None
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        return hh, mm
    except Exception:
        return None


def _time_in_interval(t, start_s, end_s):
    if start_s is None or end_s is None:
        return False
    shsm = _parse_hhmm(start_s)
    ehm = _parse_hhmm(end_s)
    if not shsm or not ehm:
        return False
    sh, sm = shsm
    eh, em = ehm
    cur = (t.hour, t.minute)
    start = (sh, sm)
    end = (eh, em)
    if start <= end:
        return start <= cur < end
    return cur >= start or cur < end  # wraps midnight


def _prio(row):
    return _to_int((row or {}).get("priority", 0), 0)


def _esc(s: Optional[str]) -> str:
    if s is None:
        return "—"
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _fmt_money(price, currency) -> str:
    if price is None:
        return "—"
    try:
        return f"{float(price):.2f} {currency or ''}".strip()
    except Exception:
        return f"{price} {currency or ''}".strip()


def _fmt_km(meters) -> str:
    if meters is None:
        return "—"
    try:
        return f"{float(meters)/1000.0:.3f} km"
    except Exception:
        return str(meters)


def _fmt_minutes(mins) -> str:
    if mins is None:
        return "—"
    try:
        return f"{float(mins):.0f} min"
    except Exception:
        return str(mins)


def _split_chunks(text: str, limit: int = 4096) -> Iterable[str]:
    t = text
    while len(t) > limit:
        cut = t.rfind("\n", 0, limit)
        if cut == -1:
            cut = limit
        yield t[:cut]
        t = t[cut:]
    if t:
        yield t


def _strip_html_tags(text: str) -> str:
    text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.I)
    return re.sub(r"</?[^>]+>", "", text)


def _fmt_dt_local(s: str, tz_name: Optional[str]) -> str:
    if not s:
        return "—"
    try:
        dt = parse_iso_dt(s)
        tzinfo = gettz(tz_name) if tz_name else None
        if tzinfo:
            return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
        return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
            tzinfo = gettz(tz_name) if tz_name else None
            if tzinfo:
                return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
            return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")
        except Exception:
            return s


def _fmt_dt_local_from_dt(dt: datetime, tz_name: Optional[str]) -> str:
    tzinfo = gettz(tz_name) if tz_name else None
    if tzinfo:
        return dt.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M %Z")
    return dt.astimezone().strftime("%Y-%m-%d %H:%M %Z")


# =============================================================
#  Duration helpers & formula normalization
# =============================================================

def _duration_minutes_from_rid(rid: dict) -> Optional[float]:
    """
    Return ride duration in minutes from whatever the API provides.
    Supports minutes or seconds (camelCase/snake_case).
    """
    if not isinstance(rid, dict):
        return None

    # minutes candidates
    for k in (
        "durationMinutes",
        "estimatedDurationMinutes",
        "duration_minutes",
        "estimated_duration_minutes",
    ):
        v = rid.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass

    # seconds candidates -> convert to minutes
    for k in (
        "estimatedDurationSeconds",
        "durationSeconds",
        "estimated_duration_seconds",
        "duration_seconds",
        "estimated_duration",  # sometimes this is seconds
        "estimatedDuration",
    ):
        v = rid.get(k)
        if v is not None:
            try:
                v = float(v)
                return v / 60.0 if v > 1000 else v
            except Exception:
                pass

    return None


def _normalize_formulas(rows):
    """Sanitize admin formula rows from DB (bytes/strings -> clean types)."""
    out = []
    for r0 in (rows or []):
        r = dict(r0 or {})
        r["start"] = _to_str(r.get("start"))
        r["end"] = _to_str(r.get("end"))
        r["priority"] = _to_int(r.get("priority"), 0)
        try:
            r["speed_kmh"] = float(_to_str(r.get("speed_kmh") or 0) or 0)
        except Exception:
            r["speed_kmh"] = 0.0
        try:
            r["bonus_min"] = float(_to_str(r.get("bonus_min") or 0) or 0)
        except Exception:
            r["bonus_min"] = 0.0
        out.append(r)
    return out


def _pick_formula_for_pickup(filters: dict, pickup_dt: datetime, tz_name: str):
    formulas = filters.get("__endtime_formulas__") or []
    if not formulas:
        return None
    local_t = pickup_dt.astimezone(gettz(tz_name)).time()
    fallback = None

    for row in sorted(formulas, key=_prio):
        if not isinstance(row, dict):
            continue
        st = _to_str(row.get("start"))
        en = _to_str(row.get("end"))
        if st and en:
            if _time_in_interval(local_t, st, en):
                return row
        elif not st and not en:
            fallback = row
    return fallback


def _compute_ends_at(offer: dict, filters: dict, pickup_dt: datetime, tz_name: str):
    """
    Compute endsAt for an offer using either:
      - Hourly: pickup + durationMinutes
      - Transfer: admin formula (speed_kmh, bonus_min) if available, else provided duration
    Returns (ends_at_iso_or_None, details_dict_or_None)
    """
    rid = (offer.get("rides") or [{}])[0]
    otype = (rid.get("type") or "").lower()

    if otype == "hourly":
        dur_min = _duration_minutes_from_rid(rid)
        if dur_min:
            ends_at = pickup_dt + timedelta(minutes=float(dur_min))
            if DEBUG_ENDS:
                _log(f"ENDSAT[hourly]: pickup={pickup_dt.isoformat()} + {dur_min:.1f}min = {ends_at.isoformat()}")
            return ends_at.isoformat(), {
                "duration_minutes": float(dur_min),
                "formula": "pickup + durationMinutes",
            }
        return None, None

    if otype == "transfer":
        dist_m = rid.get("estimatedDistanceMeters")
        try:
            if dist_m is not None:
                dist_m = float(dist_m)
        except Exception:
            dist_m = None

        # try admin formulas first
        rule = _pick_formula_for_pickup(filters, pickup_dt, tz_name)
        if rule and dist_m is not None:
            try:
                speed = float(rule.get("speed_kmh") or 0.0)
                bonus = float(rule.get("bonus_min") or 0.0)
                dist_km = float(dist_m) / 1000.0
                one_way_min = (dist_km / speed) * 60.0 if speed > 0 else 0.0
                total_min = one_way_min * 2.0 + bonus
                ends_at = pickup_dt + timedelta(minutes=total_min)
                if DEBUG_ENDS:
                    _log(
                        "ENDSAT[transfer:formula]: dist_km={:.3f} speed_kmh={} "
                        "one_way={:.2f}min total={:.2f}min => {}".format(
                            dist_km, speed, one_way_min, total_min, ends_at.isoformat()
                        )
                    )
                return ends_at.isoformat(), {
                    "distance_km": round(dist_km, 3),
                    "speed_kmh": float(speed),
                    "one_way_minutes": round(one_way_min, 2),
                    "bonus_minutes": bonus,
                    "total_minutes": round(total_min, 2),
                    "formula": "((distance_km / speed_kmh) * 60) * 2 + bonus_minutes",
                }
            except Exception:
                pass

        # fallback: use provided duration (minutes or seconds)
        dur = _duration_minutes_from_rid(rid)
        if dur is not None:
            try:
                ends_at = pickup_dt + timedelta(minutes=float(dur))
                if DEBUG_ENDS:
                    _log(f"ENDSAT[transfer:fallback]: pickup={pickup_dt.isoformat()} + {float(dur):.1f}min = {ends_at.isoformat()}")
                return ends_at.isoformat(), {
                    "duration_minutes": float(dur),
                    "formula": "pickupTime + (durationMinutes|estimatedDuration)",
                }
            except Exception:
                pass

        return None, None

    return None, None


def _parse_user_slot_local(dt_str: str, tz_name: str) -> Optional[datetime]:
    if not dt_str:
        return None
    dt_str = _to_str(dt_str).strip()
    tzinfo = gettz(tz_name)
    fmts = ["%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M"]
    for fmt in fmts:
        try:
            naive = datetime.strptime(dt_str, fmt)
            return naive.replace(tzinfo=tzinfo)
        except Exception:
            pass
    try:
        dt = parse_iso_dt(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tzinfo)
        else:
            dt = dt.astimezone(tzinfo)
        return dt
    except Exception:
        return None


def _fmt_local_iso(iso_or_none: Optional[str], tz_name: str) -> str:
    if not iso_or_none:
        return "—"
    try:
        dt = parse_iso_dt(iso_or_none)
        return _fmt_dt_local_from_dt(dt, tz_name)
    except Exception:
        return iso_or_none


def _log(msg: str):
    return None


def _extract_addr(loc: dict) -> str:
    if not loc:
        return "—"
    return loc.get("address") or loc.get("name") or "—"
