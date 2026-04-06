"""
HTTP client for the EC2 internal poller API.
All VPS → EC2 calls go through this module.
"""
import os
import logging
import threading
import requests
from typing import Optional

EC2_API_URL: str = os.getenv("EC2_API_URL", "")
POLLER_API_KEY: str = os.getenv("POLLER_API_KEY", "")

# Shared session with connection pooling
_session = requests.Session()
_session.trust_env = False
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=4,
    pool_maxsize=8,
    max_retries=0,
)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)
_session_lock = threading.Lock()


def _headers() -> dict:
    return {
        "Authorization": f"poller {POLLER_API_KEY}",
        "Content-Type": "application/json",
    }


def _url(path: str) -> str:
    return f"{EC2_API_URL.rstrip('/')}{path}"


def _get(path: str, timeout: int = 8) -> dict | list:
    r = _session.get(_url(path), headers=_headers(), timeout=timeout)
    r.raise_for_status()
    return r.json()


def _post(path: str, payload: dict, timeout: int = 5) -> None:
    try:
        r = _session.post(_url(path), json=payload, headers=_headers(), timeout=timeout)
        r.raise_for_status()
    except Exception as e:
        logging.warning("EC2 API POST %s failed: %s", path, e)


# ── Read operations ────────────────────────────────────────────────────────────

def get_users() -> list:
    """Fetch active user list from EC2. Returns list of dicts."""
    return _get("/internal/poller/users")


def get_user_config(bot_id: str, telegram_id: int) -> dict:
    """Fetch full user config from EC2 (all polling-relevant fields)."""
    return _get(f"/internal/poller/user/{bot_id}/{telegram_id}/config")


# ── Write operations (fire-and-forget) ────────────────────────────────────────

def set_token_status(bot_id: str, telegram_id: int, status: str) -> None:
    _post(f"/internal/poller/user/{bot_id}/{telegram_id}/token-status", {"status": status})


def save_token(
    bot_id: str,
    telegram_id: int,
    token: str,
    mobile_headers: Optional[dict] = None,
    auth_meta: Optional[dict] = None,
) -> None:
    _post(
        f"/internal/poller/user/{bot_id}/{telegram_id}/token",
        {"token": token, "mobile_headers": mobile_headers, "auth_meta": auth_meta},
    )


def set_token_auto_refresh(bot_id: str, telegram_id: int, enabled: bool) -> None:
    _post(f"/internal/poller/user/{bot_id}/{telegram_id}/auto-refresh", {"enabled": enabled})


def save_portal_token(bot_id: str, telegram_id: int, portal_token: str) -> None:
    _post(
        f"/internal/poller/user/{bot_id}/{telegram_id}/portal-token",
        {"portal_token": portal_token},
    )


def save_pinned_warning(bot_id: str, telegram_id: int, kind: str, message_id: int) -> None:
    _post("/internal/poller/pinned-warning/save", {
        "bot_id": bot_id, "telegram_id": int(telegram_id),
        "kind": kind, "message_id": message_id,
    })


def clear_pinned_warning(bot_id: str, telegram_id: int, kind: str) -> None:
    _post("/internal/poller/pinned-warning/clear", {
        "bot_id": bot_id, "telegram_id": int(telegram_id), "kind": kind,
    })


def save_offer_message(
    bot_id: str,
    telegram_id: int,
    message_key: str,
    header_text: str,
    full_text: str,
) -> None:
    _post("/internal/poller/offer-message", {
        "bot_id": bot_id,
        "telegram_id": int(telegram_id),
        "message_key": message_key,
        "header_text": header_text,
        "full_text": full_text,
    })


def log_offer_decision(
    bot_id: str,
    telegram_id: int,
    offer: dict,
    status: str,
    reason: Optional[str] = None,
    notify_text: Optional[str] = None,
) -> None:
    _post("/internal/poller/offer-log", {
        "bot_id": bot_id,
        "telegram_id": int(telegram_id),
        "offer": offer,
        "status": status,
        "reason": reason,
        "notify_text": notify_text,
    })
