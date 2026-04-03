import json
import time
import random
import traceback
import threading
import builtins as _builtins
from typing import Optional, List, Tuple, Dict, Set
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import db as _db_shim  # VPS shim — used to prime config cache on cache-miss
from poller_core import ec2_api as _ec2_api  # VPS HTTP client

from .config import (
    ALWAYS_POLL_REAL_ORDERS,
    USE_MOCK_P1,
    USE_MOCK_P2,
    ENABLE_P1,
    ENABLE_P2,
    POLL_INTERVAL,
    P2_POLL_INTERVAL_S,
    P2_BURST_INTERVAL_S,
    P2_BURST_ACTIVE_DURATION,
    P2_BURST_CALM_DURATION,
    BURST_POLL_INTERVAL_S,
    BURST_DURATION_S,
    MAX_WORKERS,
    LOG_OFFERS_PAYLOAD,
    MAX_LOGGED_OFFERS,
    OFFER_MEMORY_DEDUPE,
    ATHENA_USE_OFFERS_ETAG,
)
from .state import (
    maybe_reset_inmem_caches,
    cleanup_not_valid_cache,
    get_rides_intervals,
    maybe_cleanup_rides,
    invalidate_rides_cache,
    get_offers_etag,
    set_offers_etag,
    get_user_runtime_cache,
    set_user_runtime_cache,
    mark_token_invalid,
    clear_token_invalid,
    is_token_invalid,
    get_portal_token_mem,
    set_portal_token_mem,
    clear_portal_token_mem,
    is_token_ok_mem,
    set_token_ok_mem,
)
from .utils import _normalize_formulas
from .p1_client import get_offers_p1
from .p1_auth import get_playwright_p1_token, save_playwright_p1_token
from .p2_client import (
    _map_portal_offer,
    _athena_get_offers,
    _ensure_portal_token,
    warmup_p2_reserve_connection,
)
from .processing import debug_print_offers, _process_offers_for_user, _init_rides_cache_async
from .p1_client import warmup_p1_reserve_connection
from .filters import _get_enabled_filter_slugs
from .metrics import observe_ms
from .notify import (
    pin_warning_if_needed,
    unpin_warning_if_any,
    _resolve_bot_token,
    tg_unpin_message,
    tg_send_message,
)
from db import (
    init_db,
    get_all_users_with_bot_admin_active,
    get_booked_slots,
    get_vehicle_classes_state,
    get_user_timezone,
    get_blocked_days,
    get_pinned_warnings,
    set_token_status,
    clear_pinned_warning,
    get_endtime_formulas,
    get_bl_uuid,
    get_bl_account_full,
    get_mobile_headers,
    get_token_auto_refresh,
    set_token_auto_refresh,
)


def _quiet_print(*args, **kwargs):
    return None


def _quiet_exc(*args, **kwargs):
    return None


print = _quiet_print

_burst_until = 0.0
_burst_lock = threading.Lock()
# Single-user VPS: no per-user executor needed.

# Per-user P1 401/403 failure timestamps for concurrent-failure detection.
# Each entry: {"n": int, "ts": float} — ts is the last-increment timestamp so
# stale entries (from a previous episode hours ago) are not mistaken for a
# concurrent multi-user failure when checking for IP-level blocks.
_p1_fail_counts: Dict[Tuple[str, int], dict] = {}
_p1_fail_counts_lock = threading.Lock()
_P1_FAIL_COUNT_TTL_S = 120.0   # entries older than 2 min are stale
# Consecutive auto-refresh failures per (bot_id, telegram_id).
# When auto_refresh mode is ON and Playwright re-login fails this many times,
# the mode is disabled and the user is notified to update manually.
_auto_refresh_fail_counts: Dict[Tuple[str, int], int] = {}
_AUTO_REFRESH_FAIL_THRESHOLD = 3
# Minimum seconds between two Playwright re-login attempts for the same user.
# Prevents hammering Blacklane login with rapid retries when the session is broken.
_ar_last_attempt: Dict[Tuple[str, int], float] = {}
_AR_MIN_INTERVAL_S = 600.0  # 10 minutes
# Global Playwright serializer: only one browser session at a time.
# Concurrent sessions from the same IP amplify Blacklane's abuse detection.
# Non-blocking tryacquire: if busy, the caller skips and retries next cycle.
_playwright_lock = threading.Lock()
# When Playwright is in cooldown, skip ALL P1 API calls for this user until the
# cooldown expires. Prevents the 1→8→1→8 spin that spams Blacklane with 403s.
_p1_skip_until: Dict[Tuple[str, int], float] = {}
# P2 presence tracking: offer IDs currently visible in Athena per user.
# An offer is processed only when it first appears; re-processed only after it disappears and comes back.
_p2_active_offers: Dict[Tuple[str, int], Set[str]] = {}
# P2 rate-limit cooldown: earliest time the next real request is allowed per user.
_p2_next_poll: Dict[Tuple[str, int], float] = {}
# min seconds between successful P2 requests per user (configurable via P2_POLL_INTERVAL_S in .env)
# Global IP-level 429 backoff — shared across ALL users/bots on this server.
# When Athena rate-limits by IP, blocking one user blocks everyone; this ensures
# all P2 requests pause together so the IP has a chance to recover.
_p2_global_blocked_until: float = 0.0
_p2_global_429_count: int = 0
_p2_backoff_logged: bool = False  # True while we're inside a backoff window — suppresses per-cycle spam
_p2_last_429_ts: float = 0.0          # monotonic timestamp of most recent global 429
_p2_global_lock = threading.Lock()
_P2_BACKOFF_429_BASE_S = 10.0   # initial wait after first global 429
_P2_BACKOFF_429_MAX_S  = 300.0  # cap at 5 minutes
# How long the IP must be 429-free before a 200 is allowed to decrement the counter.
# Without this guard, a single allowed request after a 20s pause resets the counter to 0,
# preventing backoff from ever growing past count=2 / 20s — the oscillation seen in prod.
_P2_RECOVERY_WINDOW_S = 120.0   # 2 minutes of clean polling before count decrements
# P2 burst/calm cycle: poll aggressively for P2_BURST_ACTIVE_DURATION seconds,
# then calm down for P2_BURST_CALM_DURATION seconds, and repeat.
# The cycle is wall-clock based so all users stay in sync.
_P2_CYCLE_S: float = P2_BURST_ACTIVE_DURATION + P2_BURST_CALM_DURATION


def _p2_current_interval() -> float:
    """Return the per-user P2 poll interval based on where we are in the burst cycle."""
    phase = time.time() % _P2_CYCLE_S
    return P2_BURST_INTERVAL_S if phase < P2_BURST_ACTIVE_DURATION else P2_POLL_INTERVAL_S
# Dedicated executor for parallel P1+P2 fetch inside poll_user.
# Needs MAX_WORKERS*2 slots so all concurrent users can fetch both platforms simultaneously.
_fetch_executor = ThreadPoolExecutor(max_workers=MAX_WORKERS * 2 + 5)

# Cache for the active-users DB query: refreshed every 3 seconds instead of every 200ms.
_USERS_CACHE_TTL_S = 3.0
_users_cache_data: Optional[list] = None
_users_cache_ts: float = 0.0


def _get_users_cached() -> list:
    global _users_cache_data, _users_cache_ts
    now = time.time()
    if _users_cache_data is None or (now - _users_cache_ts) >= _USERS_CACHE_TTL_S:
        _users_cache_data = get_all_users_with_bot_admin_active()
        _users_cache_ts = now
    return _users_cache_data or []


def _bump_burst():
    global _burst_until
    if BURST_DURATION_S <= 0:
        return
    with _burst_lock:
        new_until = time.time() + BURST_DURATION_S
        if new_until > _burst_until:
            _burst_until = new_until


def _sleep_interval() -> float:
    if BURST_POLL_INTERVAL_S <= 0:
        return POLL_INTERVAL
    with _burst_lock:
        if time.time() < _burst_until:
            return BURST_POLL_INTERVAL_S
    return POLL_INTERVAL


def _poll_log(msg: str):
    _builtins.print(f"[{datetime.now()}] {msg}")


def _log_offers_found(platform: str, telegram_id: int, offers: List[dict]):
    if not offers:
        return
    _bump_burst()


def _read_portal_creds(bot_id: str, telegram_id: int) -> Tuple[Optional[str], Optional[str]]:
    try:
        creds = get_bl_account_full(bot_id, telegram_id)
    except Exception:
        creds = None
    email = password = None
    if isinstance(creds, dict):
        email = creds.get("email") or creds.get("bl_email")
        password = creds.get("password") or creds.get("bl_password")
    elif isinstance(creds, (list, tuple)):
        if len(creds) >= 2:
            email, password = creds[0], creds[1]
        elif len(creds) == 1:
            email = creds[0]
    return (email, password)


def poll_user(user):
    if len(user) >= 7:
        bot_id, telegram_id, token, filters_json, active, bot_admin_active, cache_version = user[:7]
    else:
        bot_id, telegram_id, token, filters_json, active, bot_admin_active = user[:6]
        cache_version = 0

    if not bot_admin_active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive bot {bot_id} (admin inactive)")
        return
    if not active:
        print(f"[{datetime.now()}] ⏩ Skipping inactive user {telegram_id}")
        return

    runtime_cached = get_user_runtime_cache(bot_id, telegram_id, cache_version)
    if runtime_cached:
        tz_name = runtime_cached.get("tz_name") or "UTC"
        mobile_headers = runtime_cached.get("mobile_headers")
        filters = runtime_cached.get("filters") or {}
        if "user_cfilters" in runtime_cached:
            user_cfilters = runtime_cached.get("user_cfilters") or {}
        else:
            user_cfilters = _get_enabled_filter_slugs(bot_id, telegram_id)
        class_state = runtime_cached.get("class_state") or {}
        booked_slots = runtime_cached.get("booked_slots") or []
        blocked_days = set(runtime_cached.get("blocked_days") or set())
        bl_uuid = runtime_cached.get("bl_uuid")
        email = runtime_cached.get("email")
        password = runtime_cached.get("password")
    else:
        # VPS: fetch all user config in one API call and prime the shim so
        # every getter below reads from local memory instead of the network.
        _ec2_cfg = _ec2_api.get_user_config(str(bot_id), int(telegram_id))
        _db_shim.prime_config_cache(bot_id, telegram_id, _ec2_cfg)
        tz_name = get_user_timezone(bot_id, telegram_id) or "UTC"
        mobile_headers = get_mobile_headers(bot_id, telegram_id)
        formulas_raw = get_endtime_formulas(bot_id, telegram_id)
        filters = json.loads(filters_json) if filters_json else {}
        filters["__endtime_formulas__"] = _normalize_formulas(formulas_raw)
        user_cfilters = _get_enabled_filter_slugs(bot_id, telegram_id)
        class_state = get_vehicle_classes_state(bot_id, telegram_id)
        booked_slots = get_booked_slots(bot_id, telegram_id)
        blocked_days = {d["day"] for d in get_blocked_days(bot_id, telegram_id)}
        bl_uuid = get_bl_uuid(bot_id, telegram_id)
        email, password = _read_portal_creds(bot_id, telegram_id)
        set_user_runtime_cache(
            bot_id,
            telegram_id,
            cache_version,
            {
                "tz_name": tz_name,
                "mobile_headers": mobile_headers,
                "filters": filters,
                "user_cfilters": user_cfilters,
                "class_state": class_state,
                "booked_slots": booked_slots,
                "blocked_days": list(blocked_days),
                "bl_uuid": bl_uuid,
                "email": email,
                "password": password,
            },
        )

    # ---------- Build busy intervals from Rides (Athena preferred) ----------
    accepted_intervals: List[Tuple[datetime, Optional[datetime]]] = []

    has_portal_creds = bool(email and password)
    portal_token = None

    poll_real_orders = ALWAYS_POLL_REAL_ORDERS and not (USE_MOCK_P1 and USE_MOCK_P2)

    # Gate P1 only — P2 still polls independently even when P1 is in cooldown or
    # has an invalid token, so offers on Athena are never silently missed.
    _p1_in_cooldown = False
    _p1_skip_key = (str(bot_id), int(telegram_id))
    if time.time() < _p1_skip_until.get(_p1_skip_key, 0.0):
        _p1_in_cooldown = True
    elif is_token_invalid(str(bot_id), int(telegram_id), token, int(cache_version)):
        _ar = get_token_auto_refresh(str(bot_id), int(telegram_id))
        if _ar and email and password:
            # Auto-refresh is ON: clear invalid mark so the next 403 triggers Playwright.
            clear_token_invalid(str(bot_id), int(telegram_id))
        else:
            if _ar:
                _poll_log(f"⚠️ [AUTO-REFRESH] [{bot_id}] Token invalid but BL credentials missing")
            _p1_in_cooldown = True

    def _set_token_problem(kind: str):
        """Mark token invalid in memory + send pinned warning. kind: 'no_token' | 'expired'"""
        mark_token_invalid(bot_id, telegram_id, token, cache_version)
        set_token_status(bot_id, telegram_id, "expired" if kind == "expired" else "unknown")
        existing = get_pinned_warnings(bot_id, telegram_id)
        target_id = existing.get("expired_msg_id") if kind == "expired" else existing.get("no_token_msg_id")
        if not target_id:
            other = "no_token" if kind == "expired" else "expired"
            other_id = existing.get("no_token_msg_id") if kind == "expired" else existing.get("expired_msg_id")
            if other_id:
                bot_tok = _resolve_bot_token(bot_id, telegram_id)
                tg_unpin_message(bot_tok, telegram_id, other_id)
                clear_pinned_warning(bot_id, telegram_id, other)
            pin_warning_if_needed(bot_id, telegram_id, kind)

    # ---------- PLATFORM 1 OFFERS ----------
    offers_p1: List[dict] = []
    if ENABLE_P1 and USE_MOCK_P1:
        offers_p1 = [
            {
                "type": "ride",
                "id": "mock-hourly-1",
                "price": 120.9,
                "currency": "USD",
                "actions": [{"label": "Accept", "action": "accept", "parameters": []}],
                "vehicleClass": "van",
                "_platform": "p1",
                "rides": [
                    {
                        "type": "hourly",
                        "createdAt": "2025-09-17T19:40:19Z",
                        "pickUpLocation": {
                            "name": "la Vie en Rose Quartiers Dix 30",
                            "address": "la Vie en Rose Quartiers Dix 30, Avenue des Lumières 1600, J4Y 0A5 Brossard, Québec",
                        },
                        "pickupTime": "2025-12-24T20:45:00-04:00",
                        "kmIncluded": 80,
                        "durationMinutes": 120,
                        "guestRequests": ["Baby seat", "VIP pickup"],
                        "flight": {"number": "EK 001"},
                    }
                ],
            },
            {
                "type": "ride",
                "id": "mock-transfer-1",
                "price": 90.05,
                "currency": "USD",
                "actions": [{"label": "Accept", "action": "accept", "parameters": []}],
                "vehicleClass": "business",
                "_platform": "p1",
                "rides": [
                    {
                        "type": "transfer",
                        "createdAt": "2025-08-31T19:34:08Z",
                        "pickUpLocation": {
                            "name": "Centropolis",
                            "address": "Centropolis, Avenue Pierre-Péladeau 1799, H7T 2Y5 Laval, Québec",
                        },
                        "dropOffLocation": {
                            "name": "CF Carrefour Laval",
                            "address": "CF Carrefour Laval, Boulevard le Carrefour 3003, H7T 1C7 Laval, Québec",
                        },
                        "pickupTime": "2025-09-19T22:30:00-04:00",
                        "estimatedDurationMinutes": 32,
                        "estimatedDistanceMeters": 22266,
                        "guestRequests": [
                            "EK Complimentary",
                            "2 Guest(s)",
                            "[6432 40E AVENUE H1T 2V7 MONTREAL]"
                        ],
                        "flight": {"number": "EK 243"},
                    }
                ],
            },
        ]

    def _fetch_p1_offers_real():
        nonlocal token, mobile_headers
        if _p1_in_cooldown:
            return []
        if not token or not str(token).strip():
            # No token at all — try auto-refresh before giving up.
            _ar_key = (str(bot_id), int(telegram_id))
            _auto_refresh = get_token_auto_refresh(str(bot_id), int(telegram_id))
            _poll_log(f"🔍 [P1] [{bot_id}] no token: auto_refresh={_auto_refresh} email={'SET' if email else 'MISSING'} pwd={'SET' if password else 'MISSING'}")
            if _auto_refresh and email and password:
                _last = _ar_last_attempt.get(_ar_key, 0.0)
                _now_t = time.time()
                if _now_t - _last < _AR_MIN_INTERVAL_S:
                    _p1_skip_until[_ar_key] = _last + _AR_MIN_INTERVAL_S
                    return []
                _poll_log(f"⚠️ [AUTO-REFRESH] [{bot_id}] No token — triggering Playwright login")
                _ar_last_attempt[_ar_key] = _now_t
                if not _playwright_lock.acquire(blocking=False):
                    _poll_log(f"⏸ [AUTO-REFRESH] [{bot_id}] Playwright busy — will retry next cycle")
                    _ar_last_attempt.pop(_ar_key, None)
                    return []
                try:
                    ok, new_token, new_refresh, _ = get_playwright_p1_token(bot_id, telegram_id, email, password)
                finally:
                    _playwright_lock.release()
                if ok and new_token:
                    status_code2, results2 = get_offers_p1(new_token, headers=mobile_headers)
                    if status_code2 == 200:
                        save_playwright_p1_token(bot_id, telegram_id, new_token, new_refresh, mobile_headers)
                        token = new_token
                        _auto_refresh_fail_counts.pop(_ar_key, None)
                        _ar_last_attempt.pop(_ar_key, None)
                        _p1_skip_until.pop(_ar_key, None)
                        set_token_status(bot_id, telegram_id, "valid")
                        set_token_ok_mem(bot_id, telegram_id, cache_version)
                        unpin_warning_if_any(bot_id, telegram_id, "expired")
                        unpin_warning_if_any(bot_id, telegram_id, "no_token")
                        bot_tok = _resolve_bot_token(bot_id, telegram_id)
                        tg_send_message(bot_tok, telegram_id, "✅ <b>Token refreshed successfully</b> — bot is back online.")
                        _log_offers_found("P1", telegram_id, results2 or [])
                        return results2 or []
                _ar_n = _auto_refresh_fail_counts.get(_ar_key, 0) + 1
                _auto_refresh_fail_counts[_ar_key] = _ar_n
                if _ar_n >= _AUTO_REFRESH_FAIL_THRESHOLD:
                    set_token_auto_refresh(str(bot_id), int(telegram_id), False)
                    _auto_refresh_fail_counts.pop(_ar_key, None)
                    _poll_log(f"❌ [AUTO-REFRESH] [{bot_id}] Disabled after {_AUTO_REFRESH_FAIL_THRESHOLD} failures")
                    _set_token_problem("no_token")
                return []
            _set_token_problem("no_token")
            return []
        t0 = time.perf_counter()
        status_code, results = get_offers_p1(token, headers=mobile_headers)
        observe_ms("p1_fetch_ms", (time.perf_counter() - t0) * 1000.0)
        if status_code in (401, 403):
            _fail_key = (str(bot_id), int(telegram_id))
            _now_fail = time.time()
            with _p1_fail_counts_lock:
                _p1_fail_counts[_fail_key] = {"n": 1, "ts": _now_fail}
            _poll_log(f"⚠️ P1 [{bot_id}] status={status_code} (fail)")
            # Immediately check if other users are also failing — indicates IP-level block.
            with _p1_fail_counts_lock:
                _other_users_failing = sum(
                    1 for k, v in _p1_fail_counts.items()
                    if k != _fail_key and (_now_fail - v["ts"]) <= _P1_FAIL_COUNT_TTL_S
                )
            if _other_users_failing >= 1:
                _poll_log(
                    f"⚠️ [{bot_id}] {_other_users_failing} other user(s) also failing "
                    f"— IP-level block suspected, entering cooldown"
                )
                _p1_skip_until[_fail_key] = _now_fail + _AR_MIN_INTERVAL_S
                _p1_fail_counts.pop(_fail_key, None)
                return []
            # Single-user failure — token expired.
            auto_refresh = get_token_auto_refresh(str(bot_id), int(telegram_id))
            _ar_key = (str(bot_id), int(telegram_id))
            if auto_refresh and email and password:
                # Enforce minimum interval between Playwright attempts to avoid Blacklane login detection
                _last = _ar_last_attempt.get(_ar_key, 0.0)
                _now = time.time()
                if _now - _last < _AR_MIN_INTERVAL_S:
                    # Too soon — silence all P1 calls for this user until cooldown expires
                    _p1_skip_until[_ar_key] = _last + _AR_MIN_INTERVAL_S
                    _p1_fail_counts.pop(_fail_key, None)
                    return []
                _ar_last_attempt[_ar_key] = _now
                # Serialize Playwright: only one browser session per process at a time.
                # Concurrent sessions from the same IP amplify abuse detection signals.
                if not _playwright_lock.acquire(blocking=False):
                    _poll_log(f"⏸ [AUTO-REFRESH] [{bot_id}] Playwright busy — will retry next cycle")
                    _ar_last_attempt.pop(_ar_key, None)  # don't burn the attempt slot
                    _p1_fail_counts.pop(_fail_key, None)
                    return []
                try:
                    ok, new_token, new_refresh, _ = get_playwright_p1_token(bot_id, telegram_id, email, password)
                finally:
                    _playwright_lock.release()
                if ok and new_token:
                    status_code2, results2 = get_offers_p1(new_token, headers=mobile_headers)
                    if status_code2 == 200:
                        # Token works — save and reset counters
                        save_playwright_p1_token(bot_id, telegram_id, new_token, new_refresh, mobile_headers)
                        token = new_token
                        _p1_fail_counts.pop(_fail_key, None)
                        _auto_refresh_fail_counts.pop(_ar_key, None)
                        _ar_last_attempt.pop(_ar_key, None)
                        _p1_skip_until.pop(_ar_key, None)
                        set_token_status(bot_id, telegram_id, "valid")
                        set_token_ok_mem(bot_id, telegram_id, cache_version)
                        unpin_warning_if_any(bot_id, telegram_id, "expired")
                        unpin_warning_if_any(bot_id, telegram_id, "no_token")
                        bot_tok = _resolve_bot_token(bot_id, telegram_id)
                        tg_send_message(bot_tok, telegram_id, "✅ <b>Token refreshed successfully</b> — bot is back online.")
                        _log_offers_found("P1", telegram_id, results2 or [])
                        return results2 or []
                    if status_code2 is None:
                        # Verification timed out (network error, not an auth rejection).
                        # The Playwright token is fresh and likely valid — save it and
                        # let the next cycle confirm.
                        _poll_log(f"⚠️ [AUTO-REFRESH] [{bot_id}] P1 verification timed out — saving new token, will confirm next cycle")
                        save_playwright_p1_token(bot_id, telegram_id, new_token, new_refresh, mobile_headers)
                        token = new_token
                        _p1_fail_counts.pop(_fail_key, None)
                        _p1_skip_until.pop(_ar_key, None)
                        return []
                    _poll_log(f"❌ [AUTO-REFRESH] [{bot_id}] New token still {status_code2} after Playwright login")
                # Auto-refresh attempt failed (401/403 from new token, or Playwright itself failed)
                _ar_n = _auto_refresh_fail_counts.get(_ar_key, 0) + 1
                _auto_refresh_fail_counts[_ar_key] = _ar_n
                if _ar_n >= _AUTO_REFRESH_FAIL_THRESHOLD:
                    # Disable auto-refresh and notify user with pinned warning
                    set_token_auto_refresh(str(bot_id), int(telegram_id), False)
                    _auto_refresh_fail_counts.pop(_ar_key, None)
                    _poll_log(f"❌ [AUTO-REFRESH] [{bot_id}] Disabled after {_AUTO_REFRESH_FAIL_THRESHOLD} consecutive failures")
                    _set_token_problem("expired")
                else:
                    # Let polling continue so another attempt can be made
                    _p1_fail_counts.pop(_fail_key, None)
                return []
            else:
                # Auto-refresh OFF or no credentials: notify user to update manually
                if auto_refresh and not (email and password):
                    _poll_log(f"❌ [AUTO-REFRESH] [{bot_id}] ON but BL credentials missing — falling back to manual")
                _set_token_problem("expired")
                return []
        if status_code == 200:
            _p1_fail_counts.pop((str(bot_id), int(telegram_id)), None)
            if not is_token_ok_mem(bot_id, telegram_id, cache_version):
                set_token_status(bot_id, telegram_id, "valid")
                unpin_warning_if_any(bot_id, telegram_id, "expired")
                unpin_warning_if_any(bot_id, telegram_id, "no_token")
                set_token_ok_mem(bot_id, telegram_id, cache_version)
            offers = results or []
            _poll_log(f"✅ P1 [{bot_id}] 200 OK ({len(offers)} offers)")
            _log_offers_found("P1", telegram_id, offers)
            return offers
        if status_code is None:
            err_detail = results.get("error") if isinstance(results, dict) else None
            if err_detail:
                _poll_log(f"⚠️ P1 [{bot_id}] status=None | {err_detail}")
            else:
                _poll_log(f"⚠️ P1 [{bot_id}] status=None | body={results}")
        else:
            _poll_log(f"⚠️ P1 [{bot_id}] status={status_code} | body={results}")
        return []

    # ---------- PLATFORM 2 OFFERS (Portal/Athena) ----------
    offers_p2: List[dict] = []
    if USE_MOCK_P2:
        portal_sample = {
            "data": [
                {
                    "id": f"mock-van-{int(time.time())}",
                    "type": "offers",
                    "attributes": {
                        "starts_at": "2026-03-14T11:16:00-05:00",
                        "ends_at": "2026-03-14T14:00:00-05:00",
                        "created_at": "2026-03-12T16:02:16+01:00",
                        "price": "175.77",
                        "next_price_change_in": 42,
                        "currency": "USD",
                        "distance": 94363,
                        "service_class": "van",
                        "flight_number": "EK 243",
                        "special_requests": "Pieces of luggage: 1",
                        "booking_number": "764316632",
                        "legacy_id": 16183685,
                        "booked_buffer_time": None,
                        "business_district_slug": "montreal",
                        "pickup_sign": "",
                        "passenger_first_name": "",
                        "passenger_last_name": "",
                        "booking_type": "transfer",
                        "is_final_price": False,
                        "estimated_duration": 3836,
                    },
                    "relationships": {
                        "dropoff_location": {"data": {"id": "31404853", "type": "locations"}},
                        "pickup_location": {"data": {"id": "31404852", "type": "locations"}},
                    },
                },
                {
                    "id": f"mock-biz-{int(time.time())}",
                    "type": "offers",
                    "attributes": {
                        "starts_at": "2026-03-14T11:16:35-05:00",
                        "ends_at": "2026-03-14T14:00:00-05:00",
                        "created_at": "2026-03-12T15:22:00+01:00",
                        "price": "160.58",
                        "next_price_change_in": 143,
                        "currency": "USD",
                        "distance": 94363,
                        "service_class": "business",
                        "flight_number": None,
                        "special_requests": "Pieces of luggage: 1",
                        "booking_number": "227809739",
                        "legacy_id": 16183317,
                        "booked_buffer_time": None,
                        "business_district_slug": "montreal",
                        "pickup_sign": "",
                        "passenger_first_name": "",
                        "passenger_last_name": "",
                        "booking_type": "transfer",
                        "is_final_price": False,
                        "estimated_duration": 3836,
                    },
                    "relationships": {
                        "dropoff_location": {"data": {"id": "31404357", "type": "locations"}},
                        "pickup_location": {"data": {"id": "31404356", "type": "locations"}},
                    },
                }
            ],
            "included": [
                {
                    "id": "31404852",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "airport Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "formatted_address_de": "Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "latitude": "44.785985",
                        "longitude": "-73.382924",
                        "place_id": "P:Q2hJSjg4YkNpSEE4eWt3UnlHOVJ0X09hSkJn",
                        "city": "Plattsburgh",
                        "country_code": "US",
                        "airport_iata": None,
                    },
                },
                {
                    "id": "31404853",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "formatted_address_de": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "latitude": "45.500164",
                        "longitude": "-73.565964",
                        "place_id": "P:Q2hJSnlkd3NFbkFieVV3Umo5ZnFRSGZaRmgw",
                        "city": "Montréal",
                        "country_code": "CA",
                        "airport_iata": None,
                    },
                },
                {
                    "id": "31404356",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "formatted_address_de": "Point Au Roche Lodge, Point Au Roche Road 463, 12901 Plattsburgh, New York",
                        "latitude": "44.785985",
                        "longitude": "-73.382924",
                        "place_id": "P:Q2hJSjg4YkNpSEE4eWt3UnlHOVJ0X09hSkJn",
                        "city": "Plattsburgh",
                        "country_code": "US",
                        "airport_iata": None,
                    },
                },
                {
                    "id": "31404357",
                    "type": "locations",
                    "attributes": {
                        "formatted_address_en": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "formatted_address_de": "VIA Rail Canada Central Station (Gare Centrale), Rue Est 895, H3B 2M4 Montréal, Québec",
                        "latitude": "45.500164",
                        "longitude": "-73.565964",
                        "place_id": "P:Q2hJSnlkd3NFbkFieVV3Umo5ZnFRSGZaRmgw",
                        "city": "Montréal",
                        "country_code": "CA",
                        "airport_iata": None,
                    },
                },
            ],
        }
        included = portal_sample.get("included") or []
        included_idx = {(it.get("type"), str(it.get("id"))): it for it in included}
        for raw in (portal_sample.get("data") or []):
            mapped = _map_portal_offer(raw, included_idx)
            if mapped:
                offers_p2.append(mapped)

    def _fetch_p2_offers_real():
        global _p2_global_blocked_until, _p2_global_429_count
        nonlocal portal_token
        if not ENABLE_P2:
            return [], portal_token
        # Global IP-level cooldown: if any user triggered a 429 block, all P2 requests pause.
        global _p2_backoff_logged
        _p2_key = (str(bot_id), int(telegram_id))
        _now = time.time()
        with _p2_global_lock:
            if _now < _p2_global_blocked_until:
                if not _p2_backoff_logged:
                    _remaining = _p2_global_blocked_until - _now
                    _poll_log(f"⏸ P2 all users — global backoff ({_remaining:.0f}s remaining)")
                    _p2_backoff_logged = True
                return [], portal_token
            elif _p2_backoff_logged:
                _poll_log(f"▶️ P2 global backoff lifted — resuming polls")
                _p2_backoff_logged = False
                # Stagger re-entry: assign each user a random delay (0–4s) so they
                # don't all slam Athena simultaneously the moment backoff expires.
                _p2_next_poll[_p2_key] = _now + random.uniform(0, 4.0)
        # Per-user cooldown: skip if this specific user polled too recently.
        if _now < _p2_next_poll.get(_p2_key, 0):
            return [], portal_token
        # Check in-memory cache first — avoids a DB read every 200ms cycle.
        tok = portal_token or get_portal_token_mem(bot_id, telegram_id)
        if not tok and has_portal_creds:
            tok = _ensure_portal_token(bot_id, telegram_id, email, password)
            if tok:
                set_portal_token_mem(bot_id, telegram_id, tok)
        portal_token = tok
        if not tok:
            return [], tok
        etag = get_offers_etag(bot_id, telegram_id) if ATHENA_USE_OFFERS_ETAG else None
        _poll_log(f"🔄 P2 [{bot_id}] polling...")
        t0 = time.perf_counter()
        status_code, payload, new_etag = _athena_get_offers(tok, etag=etag)
        observe_ms("p2_fetch_ms", (time.perf_counter() - t0) * 1000.0)

        if status_code in (401, 403):
            _poll_log(f"⚠️ Athena token unauthorized for user {telegram_id}. Re-logging...")
            clear_portal_token_mem(bot_id, telegram_id)
            tok = _ensure_portal_token(bot_id, telegram_id, email, password)
            if tok:
                set_portal_token_mem(bot_id, telegram_id, tok)
                t1 = time.perf_counter()
                status_code, payload, new_etag = _athena_get_offers(tok)
                observe_ms("p2_fetch_ms", (time.perf_counter() - t1) * 1000.0)

        offers: List[dict] = []
        if status_code == 200 and isinstance(payload, dict):
            _poll_log(f"✅ P2 [{bot_id}] 200 OK")
            _p2_next_poll[_p2_key] = time.time() + _p2_current_interval()
            with _p2_global_lock:
                _p2_global_blocked_until = 0.0
                # Only decrement count after sustained quiet — 2 min with no 429.
                if time.time() - _p2_last_429_ts >= _P2_RECOVERY_WINDOW_S:
                    _p2_global_429_count = max(0, _p2_global_429_count - 1)
            if ATHENA_USE_OFFERS_ETAG and new_etag:
                set_offers_etag(bot_id, telegram_id, new_etag)
            included = payload.get("included") or []
            included_idx = {(it.get("type"), str(it.get("id"))): it for it in included}
            for raw in (payload.get("data") or []):
                mapped = _map_portal_offer(raw, included_idx)
                if mapped:
                    offers.append(mapped)
            _log_offers_found("P2", telegram_id, offers)
        else:
            if status_code == 429:
                with _p2_global_lock:
                    _p2_global_429_count += 1
                    _p2_last_429_ts = time.time()
                    n = _p2_global_429_count
                    backoff = min(_P2_BACKOFF_429_BASE_S * (2 ** (n - 1)), _P2_BACKOFF_429_MAX_S)
                    _p2_global_blocked_until = _p2_last_429_ts + backoff
                    _p2_backoff_logged = False  # reset so the next skipped cycle logs once
                _poll_log(f"⚠️ P2 [{bot_id}] 429 (global_consecutive={n}, backoff={backoff:.0f}s, all P2 paused)")
            else:
                _poll_log(f"⚠️ P2 [{bot_id}] status={status_code} has_token={bool(tok)}")
        return offers, tok

    if not USE_MOCK_P1 or not USE_MOCK_P2:
        if ENABLE_P1 and ENABLE_P2 and not USE_MOCK_P1 and not USE_MOCK_P2:
            # Parallel P1+P2: both requests fired simultaneously.
            # P1 result is consumed as soon as it arrives — P2 backoff/cooldown no longer
            # delays P1 offer processing (previously up to P2_POLL_INTERVAL_S = 2s extra wait).
            _f1 = _fetch_executor.submit(_fetch_p1_offers_real)
            _f2 = _fetch_executor.submit(_fetch_p2_offers_real)
            try:
                offers_p1 = _f1.result()
            except Exception:
                offers_p1 = []

            # Process P1 offers immediately while P2 fetch is still in flight.
            if offers_p1:
                _p1_ts = time.time()
                for _o in offers_p1:
                    if isinstance(_o, dict) and _o.get("_poll_ts") is None:
                        _o["_poll_ts"] = _p1_ts
                _p1_intervals = get_rides_intervals(bot_id, telegram_id) or []
                debug_print_offers(telegram_id, offers_p1)
                _process_offers_for_user(
                    bot_id, telegram_id, offers_p1, filters, class_state,
                    booked_slots, blocked_days, _p1_intervals, tz_name,
                    p1_token=token, p1_headers=mobile_headers, p2_token=portal_token,
                    cache_version=cache_version, bl_uuid=bl_uuid, user_cfilters=user_cfilters,
                    portal_email=email, portal_password=password,
                )
                offers_p1 = []  # prevent double-processing in shared path below

            try:
                _p2_res = _f2.result()
                if isinstance(_p2_res, tuple):
                    offers_p2, portal_token = _p2_res
            except Exception:
                offers_p2 = []
        else:
            if ENABLE_P1 and not USE_MOCK_P1:
                offers_p1 = _fetch_p1_offers_real()
            if ENABLE_P2 and not USE_MOCK_P2:
                offers_p2, portal_token = _fetch_p2_offers_real()

    # ---------- P2 presence tracking ----------
    # Only pass NEW P2 offers (not currently active) to processing.
    # An offer is processed when it first appears, then ignored until it disappears and reappears.
    user_key = (str(bot_id), int(telegram_id))
    current_p2_ids: Set[str] = {o["id"] for o in (offers_p2 or []) if isinstance(o, dict) and o.get("id")}
    prev_p2_ids = _p2_active_offers.get(user_key, set())
    new_p2_offers = [o for o in (offers_p2 or []) if isinstance(o, dict) and o.get("id") not in prev_p2_ids]
    _p2_active_offers[user_key] = current_p2_ids

    # ---------- Combine and process ----------
    # P1 and P2 are processed independently — same offer ID on both platforms produces 2 notifications.
    now_ts = time.time()
    all_offers: List[dict] = []
    for offer in (offers_p1 or []) + new_p2_offers:
        if not isinstance(offer, dict):
            continue
        if offer.get("_poll_ts") is None:
            offer["_poll_ts"] = now_ts
        all_offers.append(offer)

    if poll_real_orders:
        maybe_cleanup_rides(bot_id, telegram_id)
        cached_intervals = get_rides_intervals(bot_id, telegram_id)
        if cached_intervals is None:
            # First time: lazy-init rides cache in background (one-shot fetch)
            p2_init_token = portal_token if portal_token else None
            p1_init_token = token if (token and str(token).strip()) else None
            if p2_init_token or p1_init_token:
                _init_rides_cache_async(
                    bot_id,
                    telegram_id,
                    tz_name,
                    p1_init_token,
                    mobile_headers,
                    p2_init_token,
                    bl_uuid=bl_uuid,
                    portal_email=email,
                    portal_password=password,
                )
        accepted_intervals = cached_intervals or []

    if not all_offers:
        return f"Done with user {telegram_id}"

    debug_print_offers(telegram_id, all_offers)

    _process_offers_for_user(
        bot_id,
        telegram_id,
        all_offers,
        filters,
        class_state,
        booked_slots,
        blocked_days,
        accepted_intervals,
        tz_name,
        p1_token=token,
        p1_headers=mobile_headers,
        p2_token=portal_token,
        cache_version=cache_version,
        bl_uuid=bl_uuid,
        user_cfilters=user_cfilters,
        portal_email=email,
        portal_password=password,
    )

    return f"Done with user {telegram_id}"


def _user_key(user_row) -> Tuple[str, int]:
    return (str(user_row[0]), int(user_row[1]))


def _warmup_reserve_connections_async():
    """Fire-and-forget: warm up P1+P2 reserve connections using first available user token."""
    def _job():
        try:
            users = _get_users_cached()
            p1_warmed = False
            p2_warmed = False
            for u in users:
                if not p1_warmed:
                    tok = u[2] if len(u) > 2 else None
                    hdrs = None
                    try:
                        from db import get_mobile_headers as _gmh
                        hdrs = _gmh(u[0], u[1])
                    except Exception:
                        pass
                    if tok and str(tok).strip():
                        warmup_p1_reserve_connection(tok, hdrs)
                        p1_warmed = True
                if not p2_warmed:
                    p2_tok = get_portal_token_mem(u[0], u[1])
                    if p2_tok:
                        warmup_p2_reserve_connection(p2_tok)
                        p2_warmed = True
                if p1_warmed and p2_warmed:
                    break
        except Exception:
            pass
    _fetch_executor.submit(_job)


def run():
    init_db()
    _poll_log("🚀 Poller started")
    cycle_idx = 0

    _CLEANUP_CYCLE_EVERY = 200   # cleanup not_valid cache every ~20s at 300ms poll
    _WARMUP_CYCLE_EVERY  = 150   # re-warm reserve connections every ~45s
    _HEARTBEAT_CYCLE_EVERY = 600 # heartbeat log every ~3min

    # Pre-warm reserve connections immediately at startup
    _warmup_reserve_connections_async()

    while True:
        cycle_idx += 1
        if cycle_idx % _CLEANUP_CYCLE_EVERY == 0:
            cleanup_not_valid_cache()
        if cycle_idx % _WARMUP_CYCLE_EVERY == 0:
            _warmup_reserve_connections_async()
        if cycle_idx % _HEARTBEAT_CYCLE_EVERY == 0:
            with _p2_global_lock:
                _p2_remaining = max(0.0, _p2_global_blocked_until - time.time())
            _poll_log(f"💓 heartbeat | cycle={cycle_idx} p2_backoff={_p2_remaining:.0f}s")
        if OFFER_MEMORY_DEDUPE:
            maybe_reset_inmem_caches()

        users = _get_users_cached()
        if users:
            try:
                poll_user(users[0])
            except Exception as e:
                _poll_log(f"❌ Poll error: {e}")

        time.sleep(_sleep_interval())
