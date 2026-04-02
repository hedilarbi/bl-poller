from typing import List, Tuple, Optional
from datetime import datetime, timedelta

from .config import DUMP_RIDES_IN_LOGS, DUMP_RIDES_IN_TELEGRAM, MAX_RIDES_SHOWN
from .utils import (
    _esc,
    _fmt_dt_local,
    _fmt_km,
    _fmt_minutes,
    _duration_minutes_from_rid,
    _extract_addr,
)
from .timeparse import parse_iso_dt_or_none
from .notify import maybe_send_message
from .p2_client import _safe_attr, _find_included, _extract_loc_from_included


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print

def _extract_intervals_from_rides(rides: list) -> List[Tuple[datetime, Optional[datetime]]]:
    out: List[Tuple[datetime, Optional[datetime]]] = []
    for it in (rides or []):
        rid = it if isinstance(it, dict) else {}

        # start time (accept common keys)
        start_s = (
            rid.get("pickupTime") or rid.get("pickup_time") or
            rid.get("starts_at") or rid.get("start_time") or
            rid.get("pickup") or rid.get("start")
        )
        if not start_s:
            continue
        start_dt = parse_iso_dt_or_none(start_s)
        if start_dt is None:
            continue

        # end time or duration
        end_dt = None
        end_s = rid.get("endsAt") or rid.get("ends_at") or rid.get("end_time")
        if end_s:
            end_dt = parse_iso_dt_or_none(end_s)
        if not end_dt:
            dur_min = _duration_minutes_from_rid(rid)
            if dur_min is not None:
                try:
                    end_dt = start_dt + timedelta(minutes=float(dur_min))
                except Exception:
                    end_dt = None

        out.append((start_dt, end_dt))
    return out


def _rides_snapshot_from_athena_payload(payload: dict, tz_name: str) -> str:
    data = (payload or {}).get("data") or []
    inc = (payload or {}).get("included") or []
    lines = [f"🛰️ Athena rides (planned) – showing {min(len(data), MAX_RIDES_SHOWN)}/{len(data)}"]
    for raw in data[:MAX_RIDES_SHOWN]:
        attrs = raw.get("attributes") or {}
        rel = raw.get("relationships") or {}
        rid = str(raw.get("id") or "—")
        starts_at = attrs.get("starts_at")
        booking_type = (attrs.get("booking_type") or "—").lower()
        est_dur = attrs.get("estimated_duration")
        try:
            dur_min = float(est_dur) / 60.0 if est_dur is not None else None
        except Exception:
            dur_min = None
        distance = attrs.get("distance")

        pu_rel = _safe_attr(rel, "pickup_location", "data")
        do_rel = _safe_attr(rel, "dropoff_location", "data")

        pu = do = {}
        if pu_rel and pu_rel.get("id") and pu_rel.get("type"):
            inc_pu = _find_included(inc, pu_rel["type"], pu_rel["id"])
            pu = _extract_loc_from_included(inc_pu)
        if do_rel and do_rel.get("id") and do_rel.get("type"):
            inc_do = _find_included(inc, do_rel["type"], do_rel["id"])
            do = _extract_loc_from_included(inc_do)

        lines.append(
            "• <b>{typ}</b> · 🕒 {when}\n"
            "  ⬆️ {pu}\n"
            "  ⬇️ {do}\n"
            "  ⏱️ {dur} · 📏 {dist}\n"
            "  id: <code>{rid}</code>".format(
                typ=_esc(booking_type),
                when=_esc(_fmt_dt_local(starts_at, tz_name)),
                pu=_esc(pu.get("address") or pu.get("name") or "—"),
                do=_esc(do.get("address") or do.get("name") or "—"),
                dur=_esc(_fmt_minutes(dur_min)),
                dist=_esc(_fmt_km(distance)),
                rid=_esc(rid),
            )
        )
    return "\n".join(lines)


def _rides_snapshot_from_p1_list(rides: list, tz_name: str) -> str:
    lines = [f"📱 Mobile rides – showing {min(len(rides), MAX_RIDES_SHOWN)}/{len(rides)}"]
    for raw in rides[:MAX_RIDES_SHOWN]:
        starts = raw.get("pickupTime") or raw.get("pickup_time") or raw.get("start") or raw.get("starts_at")
        dur = _duration_minutes_from_rid(raw)
        pu = _extract_addr((raw.get("pickUpLocation") or {}))
        do = _extract_addr((raw.get("dropOffLocation") or {}))
        lines.append(
            "• 🕒 {when}\n  ⬆️ {pu}\n  ⬇️ {do}\n  ⏱️ {dur}".format(
                when=_esc(_fmt_dt_local(starts, tz_name)),
                pu=_esc(pu),
                do=_esc(do),
                dur=_esc(_fmt_minutes(dur)),
            )
        )
    return "\n".join(lines)


def _dump_rides(bot_id: str, telegram_id: int, text: str, platform: str):
    if DUMP_RIDES_IN_LOGS:
        # strip tags for logs
        print(f"[{datetime.now()}] {text.replace('<b>','').replace('</b>','').replace('<code>','').replace('</code>','')}")
    if DUMP_RIDES_IN_TELEGRAM:
        maybe_send_message(bot_id, telegram_id, "accepted", text, platform)
