from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional, List, Set
import base64
import json as _json
import threading
import time


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
    _rides_cache[key] = {
        "rides": dict(rides_dict),
        "fetched": True,
        "last_cleanup": time.time(),
    }


def add_ride_to_cache(bot_id: str, telegram_id: int, ride_id: str, pickup_dt: datetime, end_dt: Optional[datetime]):
    """Add a newly reserved ride to the local cache without calling /rides."""
    key = (str(bot_id), int(telegram_id))
    entry = _rides_cache.get(key)
    if entry and entry.get("fetched"):
        entry["rides"][str(ride_id)] = (pickup_dt, end_dt)


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


def maybe_reset_inmem_caches():
    """Clear accepted daily; clear rejected every minute."""
    global _accepted_last_reset, _rejected_last_reset
    now = datetime.now(timezone.utc)
    if now - _accepted_last_reset >= _ACCEPTED_RESET_INTERVAL:
        accepted_per_user.clear()
        _accepted_last_reset = now
        print(f"[{datetime.now()}] 🔁 Cleared in-memory accept cache (24h rotation)")
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
