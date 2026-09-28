from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, List, Set
import base64
import builtins as _builtins
import json as _json
import os
import threading
import time

from .config import RESERVED_RIDES_FILE
from .timeparse import parse_iso_dt_or_none


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print

inmem_lock = threading.Lock()

# Athena token/etag helpers
_athena_offers_etag: Dict[Tuple[str, int], Optional[str]] = {}  # (bot_id, telegram_id) -> etag (offers)
_filters_cache: Dict[Tuple[str, int], Dict[str, object]] = {}

# In-memory dedupe for accepted per bot/user and rejected per bot/user/platform.
accepted_per_user = defaultdict(lambda: defaultdict(set))
rejected_per_user = defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
_ACCEPTED_RESET_INTERVAL = timedelta(hours=24)
_REJECTED_RESET_INTERVAL = timedelta(minutes=1)
_accepted_last_reset = datetime.now(timezone.utc)
_rejected_last_reset = datetime.now(timezone.utc)

# Rides cache: {(bot_id, telegram_id) → {"rides": {ride_id: (pickup_dt, end_dt)}, "fetched": bool, "last_cleanup": float}}
# We store only (ride_id → pickup+end) per chauffeur. Never refreshed automatically —
# initialized once on first poll, updated locally after each successful reservation.
_rides_cache: Dict[Tuple[str, int], Dict] = {}

# Invalid token set: (bot_id, telegram_id) → (token_fingerprint, cache_version)
# When a /offers call returns 401/403 we park the user here and skip until token changes.
_invalid_token_users: Dict[Tuple[str, int], Tuple[str, int]] = {}

_user_runtime_cache: Dict[Tuple[str, int], Dict[str, object]] = {}
_recent_not_valid_cache: Dict[Tuple[str, int, str, str, int], float] = {}
_NOT_VALID_TTL_S = 60.0
_RIDES_CLEANUP_INTERVAL_S = 86400.0  # 24 hours


# ── Invalid token helpers ──────────────────────────────────────────────────────

def mark_token_invalid(bot_id: str, telegram_id: int, token: Optional[str], cache_version: int):
    """Park this user: skip polling until token or cache_version changes."""
    fp = str(token or "")[:32]
    _invalid_token_users[(str(bot_id), int(telegram_id))] = (fp, int(cache_version))


def clear_token_invalid(bot_id: str, telegram_id: int):
    _invalid_token_users.pop((str(bot_id), int(telegram_id)), None)


def is_token_invalid(bot_id: str, telegram_id: int, token: Optional[str], cache_version: int) -> bool:
    """Return True if this user's token is still marked invalid (unchanged)."""
    entry = _invalid_token_users.get((str(bot_id), int(telegram_id)))
    if not entry:
        return False
    fp, cv = entry
    current_fp = str(token or "")[:32]
    # Clear automatically if the token or settings changed
    if current_fp != fp or int(cache_version) != cv:
        _invalid_token_users.pop((str(bot_id), int(telegram_id)), None)
        return False
    return True


# ── Rides cache helpers ────────────────────────────────────────────────────────

def get_rides_intervals(bot_id: str, telegram_id: int) -> Optional[List[Tuple[datetime, Optional[datetime]]]]:
    """Return None if /rides hasn't been fetched yet, or the list of (pickup, end) intervals."""
    entry = _rides_cache.get((str(bot_id), int(telegram_id)))
    if not entry or not entry.get("fetched"):
        return None
    return list(entry["rides"].values())


def set_rides_fetched(bot_id: str, telegram_id: int, rides_dict: Dict[str, Tuple[datetime, Optional[datetime]]]):
    """Store the initial /rides result (keyed by ride_id)."""
    key = (str(bot_id), int(telegram_id))
    now = time.time()
    _rides_cache[key] = {
        "rides": dict(rides_dict),
        "fetched": True,
        "last_fetch": now,
        "last_cleanup": now,
    }


def get_rides_cache_age_s(bot_id: str, telegram_id: int) -> Optional[float]:
    """Return seconds since the last /rides fetch, or None if never fetched."""
    entry = _rides_cache.get((str(bot_id), int(telegram_id)))
    if not entry or not entry.get("fetched"):
        return None
    last = entry.get("last_fetch")
    if last is None:
        return None
    return time.time() - float(last)


def add_ride_to_cache(bot_id: str, telegram_id: int, ride_id: str, pickup_dt: datetime, end_dt: Optional[datetime]):
    """Add a newly reserved ride to the local cache without calling /rides."""
    key = (str(bot_id), int(telegram_id))
    entry = _rides_cache.get(key)
    if entry and entry.get("fetched"):
        entry["rides"][str(ride_id)] = (pickup_dt, end_dt)


# ── Locally reserved rides ─────────────────────────────────────────────────────
# Rides this poller reserved, per chauffeur, keyed by offer_key ("p1:<id>").
# The conflict check's source of truth for the bot's own acceptances: it never depends
# on /rides (which can fail, lag, or not list P2 rides). A ride blocks its slot from the
# moment its reserve is SUBMITTED, across P1/P2 calls and poll cycles, and is persisted
# to RESERVED_RIDES_FILE so a restart does not forget it. Removed only when the ride is
# over, or when Blacklane explicitly refused the reserve.
# Entry: (pickup_dt, end_dt, expires_ts or None = until the ride is over).
_local_reserved: Dict[Tuple[str, int], Dict[str, Tuple[datetime, Optional[datetime], Optional[float]]]] = {}
_local_reserved_lock = threading.Lock()
_local_reserved_loaded = False
_LOCAL_RESERVED_UNKNOWN_END = timedelta(hours=4)  # same safety window as _find_conflict


def _load_local_reserved_locked():
    """Load the persisted registry once (caller holds _local_reserved_lock)."""
    global _local_reserved_loaded
    if _local_reserved_loaded:
        return
    _local_reserved_loaded = True
    try:
        with open(RESERVED_RIDES_FILE) as f:
            records = _json.load(f)
    except FileNotFoundError:
        return
    except Exception as e:
        _builtins.print(f"[{datetime.now()}] ⚠️ reserved rides file unreadable ({RESERVED_RIDES_FILE}): {e}")
        return
    for r in records or []:
        try:
            s = parse_iso_dt_or_none(r.get("pickup"))
            if s is None:
                continue
            e = parse_iso_dt_or_none(r.get("end")) if r.get("end") else None
            exp = float(r["expires_ts"]) if r.get("expires_ts") is not None else None
            rides = _local_reserved.setdefault((str(r["bot_id"]), int(r["telegram_id"])), {})
            rides[str(r["offer_key"])] = (s, e, exp)
        except Exception:
            continue
    _builtins.print(f"[{datetime.now()}] 📌 loaded {len(records or [])} reserved ride(s) from {RESERVED_RIDES_FILE}")


def _save_local_reserved_locked():
    """Atomically persist the registry (caller holds _local_reserved_lock)."""
    records = [
        {
            "bot_id": b,
            "telegram_id": t,
            "offer_key": k,
            "pickup": s.isoformat(),
            "end": e.isoformat() if e else None,
            "expires_ts": exp,
        }
        for (b, t), rides in _local_reserved.items()
        for k, (s, e, exp) in rides.items()
    ]
    try:
        os.makedirs(os.path.dirname(RESERVED_RIDES_FILE) or ".", exist_ok=True)
        tmp = RESERVED_RIDES_FILE + ".tmp"
        with open(tmp, "w") as f:
            _json.dump(records, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, RESERVED_RIDES_FILE)
    except Exception as e:
        _builtins.print(f"[{datetime.now()}] ⚠️ could not persist reserved rides ({RESERVED_RIDES_FILE}): {e}")


def add_local_reserved(
    bot_id: str,
    telegram_id: int,
    offer_key: str,
    pickup_dt: datetime,
    end_dt: Optional[datetime],
    ttl_s: Optional[float] = None,
):
    expires_ts = time.time() + float(ttl_s) if ttl_s is not None else None
    with _local_reserved_lock:
        _load_local_reserved_locked()
        rides = _local_reserved.setdefault((str(bot_id), int(telegram_id)), {})
        rides[str(offer_key)] = (pickup_dt, end_dt, expires_ts)
        _save_local_reserved_locked()


def remove_local_reserved(bot_id: str, telegram_id: int, offer_key: str):
    with _local_reserved_lock:
        _load_local_reserved_locked()
        rides = _local_reserved.get((str(bot_id), int(telegram_id)))
        if rides and rides.pop(str(offer_key), None) is not None:
            _save_local_reserved_locked()


def get_local_reserved(bot_id: str, telegram_id: int) -> Dict[str, Tuple[datetime, Optional[datetime]]]:
    """Return {offer_key: (pickup, end)}, dropping rides already over or past their TTL
    (a ride that has ended cannot overlap any future offer)."""
    now_ts = time.time()
    now_dt = datetime.now(timezone.utc)
    with _local_reserved_lock:
        _load_local_reserved_locked()
        rides = _local_reserved.get((str(bot_id), int(telegram_id)))
        if not rides:
            return {}
        pruned = False
        for k, (s, e, exp) in list(rides.items()):
            if exp is not None and now_ts >= exp:
                del rides[k]
                pruned = True
                continue
            try:
                if (e or s + _LOCAL_RESERVED_UNKNOWN_END) < now_dt:
                    del rides[k]
                    pruned = True
            except TypeError:
                pass  # naive datetime — keep it: blocking a slot is safer than double-booking
        if pruned:
            _save_local_reserved_locked()
        return {k: (s, e) for k, (s, e, _) in rides.items()}


def maybe_cleanup_rides(bot_id: str, telegram_id: int):
    """Once per day, remove past rides from cache (in-memory, no API call)."""
    key = (str(bot_id), int(telegram_id))
    entry = _rides_cache.get(key)
    if not entry or not entry.get("fetched"):
        return
    now_ts = time.time()
    if now_ts - entry.get("last_cleanup", 0.0) < _RIDES_CLEANUP_INTERVAL_S:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    entry["rides"] = {
        k: (s, e)
        for k, (s, e) in entry["rides"].items()
        if (e or s).replace(tzinfo=timezone.utc if (e or s).tzinfo is None else (e or s).tzinfo) > cutoff
    }
    entry["last_cleanup"] = now_ts


def get_rides_cache(bot_id: str, telegram_id: int):
    """Legacy compat shim — returns (intervals_or_None, ts_or_None)."""
    entry = _rides_cache.get((str(bot_id), int(telegram_id)))
    if not entry or not entry.get("fetched"):
        return None, None
    return list(entry["rides"].values()), entry.get("last_cleanup", 0.0)


def get_offers_etag(bot_id: str, telegram_id: int) -> Optional[str]:
    return _athena_offers_etag.get((bot_id, telegram_id))


def set_offers_etag(bot_id: str, telegram_id: int, etag: Optional[str]) -> None:
    if etag:
        _athena_offers_etag[(bot_id, telegram_id)] = etag


def get_filters_cache(bot_id: str, telegram_id: int):
    return _filters_cache.get((bot_id, telegram_id))


def set_filters_cache(bot_id: str, telegram_id: int, key: str, filters: dict, ts: Optional[float] = None):
    _filters_cache[(bot_id, telegram_id)] = {
        "key": key,
        "filters": filters,
        "ts": time.time() if ts is None else ts,
    }


def set_rides_cache(bot_id: str, telegram_id: int, intervals: List[Tuple[datetime, Optional[datetime]]], ts: Optional[float] = None):
    """Legacy compat — converts flat interval list to the new keyed structure."""
    rides_dict: Dict[str, Tuple[datetime, Optional[datetime]]] = {}
    for i, pair in enumerate(intervals or []):
        rides_dict[f"_legacy_{i}"] = pair
    set_rides_fetched(str(bot_id), int(telegram_id), rides_dict)


def invalidate_rides_cache(bot_id: str, telegram_id: int):
    _rides_cache.pop((str(bot_id), int(telegram_id)), None)


def touch_rides_cache_ts(bot_id: str, telegram_id: int):
    """Reset last_fetch to now so the stale check doesn't fire again for RIDES_REFRESH_INTERVAL_S."""
    key = (str(bot_id), int(telegram_id))
    entry = _rides_cache.get(key)
    if entry and entry.get("fetched"):
        entry["last_fetch"] = time.time()


def get_user_runtime_cache(bot_id: str, telegram_id: int, cache_version: int):
    entry = _user_runtime_cache.get((bot_id, telegram_id))
    if not entry:
        return None
    if int(entry.get("cache_version", -1)) != int(cache_version):
        return None
    return entry.get("data")


def set_user_runtime_cache(bot_id: str, telegram_id: int, cache_version: int, data: dict):
    _user_runtime_cache[(bot_id, telegram_id)] = {
        "cache_version": int(cache_version),
        "data": data or {},
        "ts": time.time(),
    }


def invalidate_user_runtime_cache(bot_id: str, telegram_id: int):
    _user_runtime_cache.pop((bot_id, telegram_id), None)


def is_recent_not_valid(
    bot_id: str,
    telegram_id: int,
    platform: str,
    offer_id: str,
    cache_version: int = 0,
    now_ts: Optional[float] = None,
) -> bool:
    now = time.time() if now_ts is None else float(now_ts)
    key = (str(bot_id), int(telegram_id), str(platform), str(offer_id), int(cache_version))
    exp = _recent_not_valid_cache.get(key)
    if exp is None:
        return False
    if now >= exp:
        _recent_not_valid_cache.pop(key, None)
        return False
    return True


def mark_not_valid_cached(
    bot_id: str,
    telegram_id: int,
    platform: str,
    offer_id: str,
    cache_version: int = 0,
    ttl_s: float = _NOT_VALID_TTL_S,
    now_ts: Optional[float] = None,
):
    now = time.time() if now_ts is None else float(now_ts)
    key = (str(bot_id), int(telegram_id), str(platform), str(offer_id), int(cache_version))
    _recent_not_valid_cache[key] = now + float(ttl_s)
    # Opportunistic cleanup to avoid unbounded growth
    if len(_recent_not_valid_cache) > 20000:
        cleanup_not_valid_cache(now_ts=now)


def cleanup_not_valid_cache(now_ts: Optional[float] = None):
    now = time.time() if now_ts is None else float(now_ts)
    for k, exp in list(_recent_not_valid_cache.items()):
        if now >= float(exp):
            _recent_not_valid_cache.pop(k, None)
    for k, (_, seen) in list(_rejected_offer_fp.items()):
        if now - seen >= _REJECTED_FP_KEEP_S:
            _rejected_offer_fp.pop(k, None)


# ── Filter-rejected offers ─────────────────────────────────────────────────────
# Fingerprint of the last filter-rejected version of each offer. The offer is re-checked every
# REJECTED_OFFER_RECHECK_S; if it is still rejected and did not change, the user is not notified
# again. Not keyed by cache_version: a config/token change triggers a re-check, not a new message.
_rejected_offer_fp: Dict[Tuple[str, int, str, str], Tuple[str, float]] = {}  # → (fingerprint, last seen)
_REJECTED_FP_KEEP_S = 86400.0


def remember_rejected_offer(bot_id: str, telegram_id: int, platform: str, offer_id: str, fingerprint: str) -> bool:
    """Record a filter-rejected offer version. True if this exact version was already rejected."""
    key = (str(bot_id), int(telegram_id), str(platform), str(offer_id))
    prev = _rejected_offer_fp.get(key)
    _rejected_offer_fp[key] = (fingerprint, time.time())
    return prev is not None and prev[0] == fingerprint


def forget_rejected_offer(bot_id: str, telegram_id: int, platform: str, offer_id: str):
    _rejected_offer_fp.pop((str(bot_id), int(telegram_id), str(platform), str(offer_id)), None)


def is_rejected_recheck_due(bot_id: str, telegram_id: int, platform: str, offer_id: str, cache_version: int = 0) -> bool:
    """True for a previously filter-rejected offer whose re-check delay is over."""
    key = (str(bot_id), int(telegram_id), str(platform), str(offer_id))
    return key in _rejected_offer_fp and not is_recent_not_valid(
        bot_id, telegram_id, platform, offer_id, cache_version=cache_version
    )


def maybe_reset_inmem_caches():
    """Clear accepted daily; clear rejected every minute."""
    global _accepted_last_reset, _rejected_last_reset
    now = datetime.now(timezone.utc)
    if now - _accepted_last_reset >= _ACCEPTED_RESET_INTERVAL:
        accepted_per_user.clear()
        _accepted_last_reset = now
    if now - _rejected_last_reset >= _REJECTED_RESET_INTERVAL:
        rejected_per_user.clear()
        _rejected_last_reset = now


# ── Portal token in-memory cache ──────────────────────────────────────────────
# Eliminates DB read on every 200ms cycle for P2 users.
# Keyed by (bot_id, telegram_id) → (token, expires_at_monotonic)

_portal_token_mem: Dict[Tuple[str, int], Tuple[str, float]] = {}
_PORTAL_TOKEN_FALLBACK_TTL_S = 3000.0  # ~50 min fallback when JWT exp unreadable


def _jwt_exp_ts(token: str) -> Optional[float]:
    """Best-effort read of JWT 'exp' (Unix seconds). Returns None if unreadable."""
    try:
        parts = (token or "").split(".")
        if len(parts) != 3:
            return None
        payload = _json.loads(base64.urlsafe_b64decode(parts[1] + "==="))
        exp = payload.get("exp")
        return float(exp) if isinstance(exp, (int, float)) else None
    except Exception:
        return None


def get_portal_token_mem(bot_id: str, telegram_id: int) -> Optional[str]:
    """Return cached portal token if still valid, else None."""
    entry = _portal_token_mem.get((str(bot_id), int(telegram_id)))
    if not entry:
        return None
    tok, exp_ts = entry
    if time.time() >= exp_ts:
        _portal_token_mem.pop((str(bot_id), int(telegram_id)), None)
        return None
    return tok


def set_portal_token_mem(bot_id: str, telegram_id: int, token: str, skew_s: float = 300.0):
    """Cache portal token until its JWT expiry minus skew_s (default 5 min)."""
    exp = _jwt_exp_ts(token)
    if exp is None:
        exp = time.time() + _PORTAL_TOKEN_FALLBACK_TTL_S
    _portal_token_mem[(str(bot_id), int(telegram_id))] = (token, exp - skew_s)


def clear_portal_token_mem(bot_id: str, telegram_id: int):
    _portal_token_mem.pop((str(bot_id), int(telegram_id)), None)


# ── Token-ok in-memory flag ────────────────────────────────────────────────────
# After the first successful P1 poll, skip set_token_status("valid") + unpin_warning DB ops
# on every subsequent cycle. Reset automatically when cache_version changes (token updated).

_token_ok_mem: Dict[Tuple[str, int, int], bool] = {}


def is_token_ok_mem(bot_id: str, telegram_id: int, cache_version: int) -> bool:
    """Return True if we already confirmed this token is valid + warnings cleared."""
    return _token_ok_mem.get((str(bot_id), int(telegram_id), int(cache_version)), False)


def set_token_ok_mem(bot_id: str, telegram_id: int, cache_version: int):
    """Mark token as confirmed valid + warnings cleared. Evict stale versions."""
    key = (str(bot_id), int(telegram_id), int(cache_version))
    _token_ok_mem[key] = True
    stale = [
        k for k in list(_token_ok_mem)
        if k[0] == str(bot_id) and k[1] == int(telegram_id) and k[2] != int(cache_version)
    ]
    for k in stale:
        _token_ok_mem.pop(k, None)
