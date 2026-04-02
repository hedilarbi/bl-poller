"""
DB shim for VPS poller nodes.

Provides the same interface as bl-bot/db.py but routes read operations through
a local config cache (populated by loop.py on cache-miss) and write operations
through the EC2 internal API.

All poller_core/* modules import from `db` and work unchanged on the VPS.
"""
import os
import threading
from typing import Optional, Tuple

from poller_core import ec2_api

# ── Per-user config cache ──────────────────────────────────────────────────────
# Populated by loop.py when it detects a cache_version change.
# Structure: (bot_id_str, telegram_id_int) → config dict from EC2 /config endpoint.

_config_lock = threading.Lock()
_config: dict = {}  # (bot_id, telegram_id) → config dict

# Separate in-memory stores for values that change at runtime without a
# cache_version bump (portal token, pinned warnings updated mid-poll).
_portal_tokens: dict = {}  # (bot_id, telegram_id) → token str
_pinned_warnings: dict = {}  # (bot_id, telegram_id) → {no_token_msg_id, expired_msg_id}
_offer_messages: dict = {}  # (bot_id, telegram_id, message_key) → (header, full_text)

# Assignment: which user this VPS is responsible for (set from .env).
_ASSIGNED_BOT_ID: Optional[str] = os.getenv("ASSIGNED_BOT_ID")
_ASSIGNED_TELEGRAM_ID: Optional[int] = (
    int(os.getenv("ASSIGNED_TELEGRAM_ID"))
    if os.getenv("ASSIGNED_TELEGRAM_ID")
    else None
)


def _key(bot_id, telegram_id) -> tuple:
    return (str(bot_id), int(telegram_id))


def _get_cfg(bot_id, telegram_id) -> dict:
    with _config_lock:
        return dict(_config.get(_key(bot_id, telegram_id)) or {})


def _update_cfg(bot_id, telegram_id, **updates) -> None:
    k = _key(bot_id, telegram_id)
    with _config_lock:
        cfg = dict(_config.get(k) or {})
        cfg.update(updates)
        _config[k] = cfg


# Called by loop.py in the cache-miss branch to populate the shim before
# individual getter calls (avoids one EC2 API call per getter).
def prime_config_cache(bot_id, telegram_id, config: dict) -> None:
    k = _key(bot_id, telegram_id)
    with _config_lock:
        _config[k] = config
    # Prime runtime caches for fields that change without a cache_version bump.
    pt = config.get("portal_token")
    if pt:
        _portal_tokens[k] = pt
    pw = config.get("pinned_warnings")
    if pw:
        _pinned_warnings[k] = dict(pw)


# ── DB interface ────────────────────────────────────────────────────────────────

def init_db() -> None:
    pass  # no-op on VPS — DB lives on EC2


def get_all_users_with_bot_admin_active() -> list:
    """Fetch active users from EC2 and return as
    (bot_id, telegram_id, token, filters_json, active, bot_admin_active, cache_version) tuples."""
    users_data = ec2_api.get_users()
    # Filter to this VPS's assigned user if configured.
    if _ASSIGNED_BOT_ID and _ASSIGNED_TELEGRAM_ID is not None:
        users_data = [
            u for u in users_data
            if u.get("bot_id") == _ASSIGNED_BOT_ID
            and int(u.get("telegram_id", -1)) == _ASSIGNED_TELEGRAM_ID
        ]
    return [
        (
            u["bot_id"],
            u["telegram_id"],
            u.get("token"),
            u.get("filters_json"),
            u.get("active", True),
            u.get("bot_admin_active", True),
            int(u.get("cache_version") or 0),
        )
        for u in users_data
    ]


# ── Read helpers — all served from config cache ───────────────────────────────

def get_user_timezone(bot_id, telegram_id) -> str:
    return _get_cfg(bot_id, telegram_id).get("tz_name") or "UTC"


def get_mobile_headers(bot_id, telegram_id) -> Optional[dict]:
    return _get_cfg(bot_id, telegram_id).get("mobile_headers")


def get_mobile_auth(bot_id, telegram_id) -> dict:
    return _get_cfg(bot_id, telegram_id).get("mobile_auth") or {}


def get_endtime_formulas(bot_id, telegram_id) -> list:
    return _get_cfg(bot_id, telegram_id).get("endtime_formulas") or []


def get_vehicle_classes_state(bot_id, telegram_id) -> dict:
    return _get_cfg(bot_id, telegram_id).get("class_state") or {}


def get_booked_slots(bot_id, telegram_id) -> list:
    return _get_cfg(bot_id, telegram_id).get("booked_slots") or []


def get_blocked_days(bot_id, telegram_id) -> list:
    days = _get_cfg(bot_id, telegram_id).get("blocked_days") or []
    # Normalise to list of {"day": str} dicts (same shape as bl-bot DB returns).
    return [{"day": d} if isinstance(d, str) else d for d in days]


def get_bl_uuid(bot_id, telegram_id) -> Optional[str]:
    return _get_cfg(bot_id, telegram_id).get("bl_uuid")


def get_bl_account_full(bot_id, telegram_id):
    cfg = _get_cfg(bot_id, telegram_id)
    email = cfg.get("email")
    password = cfg.get("password")
    if email is None and password is None:
        return None
    return {"email": email, "password": password}


def get_token_auto_refresh(bot_id, telegram_id) -> bool:
    return bool(_get_cfg(bot_id, telegram_id).get("token_auto_refresh", False))


def get_bot_token(bot_id: str) -> Optional[str]:
    with _config_lock:
        for (bid, _), cfg in _config.items():
            if bid == str(bot_id) and cfg.get("bot_token"):
                return cfg["bot_token"]
    return None


def get_notifications(bot_id, telegram_id) -> dict:
    return _get_cfg(bot_id, telegram_id).get("notifications") or {
        "accepted": True,
        "not_accepted": True,
        "rejected": False,
    }


def get_pinned_warnings(bot_id, telegram_id) -> dict:
    k = _key(bot_id, telegram_id)
    with _config_lock:
        pw = _pinned_warnings.get(k)
    if pw is not None:
        return dict(pw)
    return {"no_token_msg_id": None, "expired_msg_id": None}


def list_user_custom_filters(bot_id, telegram_id) -> list:
    return _get_cfg(bot_id, telegram_id).get("user_custom_filters") or []


# Portal token — managed separately because it refreshes more frequently.

def get_portal_token(bot_id, telegram_id) -> Optional[str]:
    return _portal_tokens.get(_key(bot_id, telegram_id))


def update_portal_token(bot_id, telegram_id, token: str) -> None:
    _portal_tokens[_key(bot_id, telegram_id)] = token
    # Persist to EC2 so the token survives a VPS restart.
    ec2_api.save_portal_token(str(bot_id), int(telegram_id), token)


# ── Write helpers — call EC2 API + update local cache ────────────────────────

def set_token_status(bot_id, telegram_id, status: str) -> None:
    ec2_api.set_token_status(str(bot_id), int(telegram_id), status)


def update_token(bot_id, telegram_id, token: str, headers=None, auth_meta=None) -> None:
    ec2_api.save_token(str(bot_id), int(telegram_id),
                       token=token, mobile_headers=headers, auth_meta=auth_meta)
    # Keep local cache in sync so p1_auth.py sees the new token/auth_meta.
    updates = {}
    if token:
        updates["token"] = token
    if headers is not None:
        updates["mobile_headers"] = headers
    if auth_meta:
        cfg_auth = dict(_get_cfg(bot_id, telegram_id).get("mobile_auth") or {})
        cfg_auth.update(auth_meta)
        updates["mobile_auth"] = cfg_auth
    if updates:
        _update_cfg(bot_id, telegram_id, **updates)


def set_token_auto_refresh(bot_id, telegram_id, enabled: bool) -> None:
    ec2_api.set_token_auto_refresh(str(bot_id), int(telegram_id), enabled)
    _update_cfg(bot_id, telegram_id, token_auto_refresh=enabled)


def save_pinned_warning(bot_id, telegram_id, kind: str, message_id: int) -> None:
    ec2_api.save_pinned_warning(str(bot_id), int(telegram_id), kind, message_id)
    k = _key(bot_id, telegram_id)
    with _config_lock:
        pw = dict(_pinned_warnings.get(k) or {"no_token_msg_id": None, "expired_msg_id": None})
        pw["no_token_msg_id" if kind == "no_token" else "expired_msg_id"] = message_id
        _pinned_warnings[k] = pw


def clear_pinned_warning(bot_id, telegram_id, kind: str) -> None:
    ec2_api.clear_pinned_warning(str(bot_id), int(telegram_id), kind)
    k = _key(bot_id, telegram_id)
    with _config_lock:
        pw = dict(_pinned_warnings.get(k) or {"no_token_msg_id": None, "expired_msg_id": None})
        pw["no_token_msg_id" if kind == "no_token" else "expired_msg_id"] = None
        _pinned_warnings[k] = pw


def log_offer_decision(bot_id, telegram_id, offer: dict, status: str,
                       reason=None, notify_text=None) -> None:
    ec2_api.log_offer_decision(str(bot_id), int(telegram_id), offer, status, reason, notify_text)


# Offer message cache — in-memory only (single user per VPS, no persistence needed).

def save_offer_message(bot_id, telegram_id, message_key: str,
                       header_text: str, full_text: str) -> None:
    _offer_messages[(_key(bot_id, telegram_id), message_key)] = (header_text, full_text)


def get_offer_message(bot_id, telegram_id, message_key: str) -> Tuple[Optional[str], Optional[str]]:
    return _offer_messages.get((_key(bot_id, telegram_id), message_key), (None, None))
