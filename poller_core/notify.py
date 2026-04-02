import requests
import time
from typing import Optional
from datetime import datetime

from .config import BOT_TOKEN
from .utils import _split_chunks, _strip_html_tags
from db import (
    get_notifications,
    get_bot_token,
    get_pinned_warnings,
    save_pinned_warning,
    clear_pinned_warning,
)


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print

def _platform_icon(offer_or_platform) -> str:
    # accepts offer dict or plain "p1"/"p2" string
    plat = offer_or_platform
    if isinstance(offer_or_platform, dict):
        plat = offer_or_platform.get("_platform", "p1")
    return "💻" if str(plat).lower() == "p2" else "📱"


def _resolve_bot_token(bot_id: str, telegram_id: int) -> Optional[str]:
    tok = get_bot_token(bot_id)
    if tok:
        return tok
    if BOT_TOKEN:
        print(f"[{datetime.now()}] ⚠️ Falling back to BOT_TOKEN for {bot_id}/{telegram_id} (no bot mapping).")
        return BOT_TOKEN
    print(f"[{datetime.now()}] ❌ No bot token for {bot_id}/{telegram_id}; cannot send Telegram messages.")
    return None


def _send_one(
    bot_token: str,
    chat_id: int,
    text: str,
    reply_markup: Optional[dict],
    parse_mode: Optional[str],
) -> Optional[int]:
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "disable_notification": False}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(url, json=payload, timeout=15)
    if r.status_code >= 400:
        try:
            print(f"[{datetime.now()}] ❌ Telegram error {r.status_code}: {r.json()}")
        except Exception:
            print(f"[{datetime.now()}] ❌ Telegram error {r.status_code}: {r.text}")
        r.raise_for_status()
    return r.json().get("result", {}).get("message_id")


def _retry_after_s(resp) -> float:
    if resp is None:
        return 0.0
    try:
        h = resp.headers.get("Retry-After")
        if h:
            return float(h)
    except Exception:
        pass
    try:
        j = resp.json() if resp.content else {}
        p = (j or {}).get("parameters") or {}
        ra = p.get("retry_after")
        if ra is not None:
            return float(ra)
    except Exception:
        pass
    return 0.0


def _is_html_parse_error(resp) -> bool:
    if resp is None or int(getattr(resp, "status_code", 0) or 0) != 400:
        return False
    try:
        low = (resp.text or "").lower()
    except Exception:
        low = ""
    keys = (
        "can't parse entities",
        "cant parse entities",
        "unsupported start tag",
        "can't find end tag",
        "parse entities",
    )
    return any(k in low for k in keys)


def _send_one_with_retry(
    bot_token: str,
    chat_id: int,
    text: str,
    reply_markup: Optional[dict],
    parse_mode: Optional[str],
    retries: int = 3,
) -> Optional[int]:
    backoff_s = 0.4
    for attempt in range(max(1, int(retries))):
        try:
            return _send_one(bot_token, chat_id, text, reply_markup, parse_mode)
        except requests.HTTPError as e:
            resp = getattr(e, "response", None)
            status = int(getattr(resp, "status_code", 0) or 0)
            retryable = status == 429 or (500 <= status < 600)
            if retryable and attempt < (retries - 1):
                wait_s = _retry_after_s(resp) or backoff_s
                wait_s = min(max(wait_s, 0.2), 5.0)
                time.sleep(wait_s)
                backoff_s = min(backoff_s * 2.0, 5.0)
                continue
            raise


def tg_send_message(
    bot_token: Optional[str],
    chat_id: int,
    text: str,
    reply_markup: Optional[dict] = None,
    disable_notification: bool = False,
) -> Optional[int]:
    if not bot_token:
        return None
    first_id = None
    try:
        chunks = list(_split_chunks(text, 4096))
        for i, ch in enumerate(chunks):
            mid = _send_one_with_retry(bot_token, chat_id, ch, reply_markup if i == 0 else None, "HTML")
            if first_id is None:
                first_id = mid
        return first_id
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        status = int(getattr(resp, "status_code", 0) or 0)
        if _is_html_parse_error(resp):
            print(f"[{datetime.now()}] ⚠️ Falling back to plain text due to HTML parse error: {e}")
            plain = _strip_html_tags(text)
            first_id = None
            for i, ch in enumerate(_split_chunks(plain, 4096)):
                try:
                    mid = _send_one_with_retry(bot_token, chat_id, ch, reply_markup if i == 0 else None, None)
                    if first_id is None:
                        first_id = mid
                except Exception as e2:
                    print(f"[{datetime.now()}] ❌ Telegram fallback send failed: {e2}")
                    return first_id
            return first_id
        print(f"[{datetime.now()}] ❌ Telegram sendMessage HTTP error {status}: {e}")
        return first_id
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram sendMessage error for {chat_id}: {e}")
        return None


def tg_pin_message(bot_token: Optional[str], chat_id: int, message_id: int):
    if not bot_token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/pinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id, "disable_notification": False},
            timeout=10,
        )
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram pinChatMessage error for {chat_id}: {e}")


def tg_unpin_message(bot_token: Optional[str], chat_id: int, message_id: int):
    if not bot_token:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{bot_token}/unpinChatMessage",
            json={"chat_id": chat_id, "message_id": message_id},
            timeout=10,
        )
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Telegram unpinChatMessage error for {chat_id}: {e}")


def maybe_send_message(
    bot_id: str,
    telegram_id: int,
    kind: str,
    text: str,
    platform: str,
    reply_markup: Optional[dict] = None,
    force_notify: bool = False,
):
    """
    kind: 'accepted' | 'not_accepted' | 'rejected'
    platform: 'p1' or 'p2' (required)
    Sends Telegram message only if user's notification preference for 'kind' is enabled.
    (You can later extend prefs to be per-platform if needed.)
    """
    if not force_notify:
        prefs = get_notifications(bot_id, telegram_id)
        if not prefs.get(kind, True):
            return None
    # simple platform-aware header injection (optional)
    icon = _platform_icon(platform)
    text = f"{icon} {text}"
    bot_token = _resolve_bot_token(bot_id, telegram_id)
    return tg_send_message(bot_token, telegram_id, text, reply_markup=reply_markup)


def pin_warning_if_needed(bot_id: str, telegram_id: int, kind: str):
    existing = get_pinned_warnings(bot_id, telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        return
    bot_token = _resolve_bot_token(bot_id, telegram_id)
    other = "expired" if kind == "no_token" else "no_token"
    other_id = existing["expired_msg_id"] if kind == "no_token" else existing["no_token_msg_id"]
    if other_id:
        tg_unpin_message(bot_token, telegram_id, other_id)
        clear_pinned_warning(bot_id, telegram_id, other)
    if kind == "no_token":
        text = "⚠️ <b>Bot Issue</b>: no mobile session\n\nPlease add your mobile session token."
    else:
        text = "⚠️ <b>Bot Issue</b>: mobile session expired\n\nPlease update your mobile session token."
    markup = {"inline_keyboard": [[{"text": "➕ Add/Update mobile session", "callback_data": "add_mobile_session"}]]}
    message_id = tg_send_message(bot_token, telegram_id, text, reply_markup=markup)
    if message_id:
        tg_pin_message(bot_token, telegram_id, message_id)
        save_pinned_warning(bot_id, telegram_id, kind, message_id)


def unpin_warning_if_any(bot_id: str, telegram_id: int, kind: str):
    existing = get_pinned_warnings(bot_id, telegram_id)
    msg_id = existing["no_token_msg_id"] if kind == "no_token" else existing["expired_msg_id"]
    if msg_id:
        bot_token = _resolve_bot_token(bot_id, telegram_id)
        tg_unpin_message(bot_token, telegram_id, msg_id)
        clear_pinned_warning(bot_id, telegram_id, kind)
