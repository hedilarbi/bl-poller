from typing import List, Tuple, Optional
from datetime import datetime, timedelta

from .config import DUMP_RIDES_IN_TELEGRAM, MAX_RIDES_SHOWN
from .utils import (
    _esc,
    _fmt_dt_local,
    _fmt_km,
    _fmt_minutes,
    _duration_minutes_from_rid,
    _extract_addr,
    _pick_formula_for_pickup,
)
from .timeparse import parse_iso_dt_or_none
from .notify import maybe_send_message
from .p2_client import _safe_attr, _find_included, _extract_loc_from_included


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print

def _ride_interval(
    rid: dict,
    filters: Optional[dict] = None,
    tz_name: Optional[str] = None,
) -> Optional[Tuple[datetime, Optional[datetime], str]]:
    """
    (pickup_dt, end_dt, how) for one scheduled ride, or None without a usable pickup.
    `how` explains how end_dt was obtained (logged so the computation can be checked).

    end_dt resolution order (first match wins):
      1a. hourly: duration + 2h safety buffer (mirrors _compute_ends_at)
      1b. transfer: estimatedDistanceMeters (or distance in km) + admin formula
      2.  endsAt / ends_at / end_time from the API
      3.  API duration
      4.  None — _find_conflict then uses a 4h safety window
    """
    # start time (accept common keys)
    start_s = (
        rid.get("pickupTime") or rid.get("pickup_time") or
        rid.get("starts_at") or rid.get("start_time") or
        rid.get("pickup") or rid.get("start")
    )
    if not start_s:
        return None
    start_dt = parse_iso_dt_or_none(start_s)
    if start_dt is None:
        return None

    end_dt = None
    how = "unknown end -> 4h safety window"
    ride_type = str(rid.get("rideType") or rid.get("type") or "").lower()

    # 1a. Hourly ride: durationMinutes + 2h safety buffer (mirrors _compute_ends_at)
    if filters and tz_name and not end_dt:
        try:
            if ride_type == "hourly":
                _SAFETY_BUFFER_MIN = 120.0
                dur_min = _duration_minutes_from_rid(rid)
                if dur_min is not None:
                    end_dt = start_dt + timedelta(minutes=float(dur_min) + _SAFETY_BUFFER_MIN)
                    how = f"hourly {float(dur_min):.0f}min + 120min safety"
                else:
                    # No duration info at all → 2h safety buffer alone
                    end_dt = start_dt + timedelta(minutes=_SAFETY_BUFFER_MIN)
                    how = "hourly, no duration -> 120min safety"
        except Exception:
            end_dt = None

    # 1b. Transfer ride: admin formula (distance / speed * 2 + bonus)
    if filters and tz_name and not end_dt:
        try:
            # Prefer meters (same field/unit as offers) — without it the formula was
            # skipped and the ride ended at pickup + one-way API duration, too early.
            dist_m = rid.get("estimatedDistanceMeters")
            dist_km = float(dist_m) / 1000.0 if dist_m is not None else rid.get("distance")  # "distance" is km
            if dist_km is not None and ride_type == "transfer":
                dist_km = float(dist_km)
                rule = _pick_formula_for_pickup(filters, start_dt, tz_name)
                if rule:
                    speed = float(rule.get("speed_kmh") or 0.0)
                    bonus = float(rule.get("bonus_min") or 0.0)
                    if speed > 0:
                        one_way_min = (dist_km / speed) * 60.0
                        total_min = one_way_min * 2.0 + bonus
                        end_dt = start_dt + timedelta(minutes=total_min)
                        window = f"{rule.get('start')}-{rule.get('end')}" if rule.get("start") else "else"
                        how = (
                            f"formula [{window}] {dist_km:.1f}km @ {speed:g}km/h x2 + {bonus:g}min"
                            f" = {total_min:.0f}min"
                        )
        except Exception:
            end_dt = None

    # 2. endsAt from API
    if not end_dt:
        end_s = rid.get("endsAt") or rid.get("ends_at") or rid.get("end_time")
        if end_s:
            end_dt = parse_iso_dt_or_none(end_s)
            if end_dt:
                how = "API endsAt (no formula applicable)"

    # 3. estimatedDurationMinutes from API
    if not end_dt:
        dur_min = _duration_minutes_from_rid(rid)
        if dur_min is not None:
            try:
                end_dt = start_dt + timedelta(minutes=float(dur_min))
                how = f"API duration {float(dur_min):.0f}min (no formula applicable)"
            except Exception:
                end_dt = None

    return start_dt, end_dt, how


def _extract_intervals_from_rides(
    rides: list,
    filters: Optional[dict] = None,
    tz_name: Optional[str] = None,
) -> List[Tuple[datetime, Optional[datetime]]]:
    """Convert a list of scheduled rides into (pickup_dt, end_dt) intervals (see _ride_interval)."""
    out: List[Tuple[datetime, Optional[datetime]]] = []
    for it in (rides or []):
        iv = _ride_interval(it if isinstance(it, dict) else {}, filters, tz_name)
        if iv:
            out.append((iv[0], iv[1]))
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
    if DUMP_RIDES_IN_TELEGRAM:
        maybe_send_message(bot_id, telegram_id, "accepted", text, platform)
