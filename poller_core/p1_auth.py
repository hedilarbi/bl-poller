import asyncio
import base64
import hashlib
import json
import re
import secrets
import threading
import time
import builtins as _builtins
from datetime import datetime
from typing import Optional, Tuple

import requests

from .config import (
    MOBILE_AUTH_BASE,
    MOBILE_CLIENT_ID,
    P1_POLL_TIMEOUT_S,
    P1_REFRESH_SKEW_S,
    HTTP_POOL_SIZE,
    P1_USER_AGENT,
)
from db import get_mobile_auth, update_token, set_token_status

_thread_local = threading.local()


def _get_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        sess.trust_env = False
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=HTTP_POOL_SIZE,
            pool_maxsize=HTTP_POOL_SIZE,
            max_retries=0,
        )
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        _thread_local.session = sess
    return sess


def _session_request(method: str, url: str, **kwargs):
    sess = _get_session()
    # Keep connection pooling but avoid cross-user cookie bleed on shared worker threads.
    try:
        sess.cookies.clear()
    except Exception:
        pass
    return sess.request(method=method, url=url, **kwargs)


def _normalize_bearer(token: Optional[str]) -> Optional[str]:
    if not token:
        return None
    val = str(token).strip()
    if not val:
        return None
    if val.lower().startswith("bearer "):
        return "Bearer " + val[7:].strip()
    return val


def _header_get(headers: Optional[dict], name: str) -> Optional[str]:
    if not headers:
        return None
    lname = name.lower()
    for k, v in headers.items():
        if str(k).lower() == lname:
            return v
    return None


def _header_drop(headers: dict, name: str):
    lname = name.lower()
    for k in list(headers.keys()):
        if str(k).lower() == lname:
            headers.pop(k, None)


def _jwt_payload_unverified(token: Optional[str]) -> dict:
    try:
        raw = token[7:].strip() if str(token).lower().startswith("bearer ") else token
        parts = (raw or "").split(".")
        if len(parts) != 3:
            return {}
        payload_b64 = parts[1] + "==="
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _jwt_claim(token: Optional[str], name: str) -> Optional[str]:
    val = _jwt_payload_unverified(token).get(name)
    return str(val) if val not in (None, "") else None


def _jwt_exp_unverified(token: str) -> Optional[int]:
    exp = _jwt_payload_unverified(token).get("exp")
    return int(exp) if isinstance(exp, (int, float)) else None


def _needs_refresh(token: Optional[str]) -> bool:
    if not token:
        return True
    exp = _jwt_exp_unverified(token)
    if exp is None:
        return False
    now = int(time.time())
    return now >= (exp - P1_REFRESH_SKEW_S)


def is_p1_token_expired(token: Optional[str], skew_s: int = 0) -> bool:
    tok = _normalize_bearer(token)
    if not tok:
        return False
    exp = _jwt_exp_unverified(tok)
    if exp is None:
        return False
    now = int(time.time())
    try:
        skew = int(skew_s or 0)
    except Exception:
        skew = 0
    if skew < 0:
        skew = 0
    return now >= (exp - skew)


def _build_oauth_headers(mobile_headers: Optional[dict], oauth_headers: Optional[dict]) -> dict:
    allowed = {
        "accept",
        "accept-language",
        "accept-encoding",
        "connection",
        "user-agent",
        "auth0-client",
        "cookie",
        "content-type",
        "host",
    }
    headers: dict = {}
    if mobile_headers:
        for k, v in mobile_headers.items():
            if v is not None and str(k).lower() in allowed:
                headers[k] = v
    if oauth_headers:
        for k, v in oauth_headers.items():
            if v is not None and str(k).lower() in allowed:
                headers[k] = v

    _header_drop(headers, "Authorization")
    _header_drop(headers, "Content-Length")

    auth_host = MOBILE_AUTH_BASE.replace("https://", "").replace("http://", "")
    current_host = _header_get(headers, "Host")
    if current_host and current_host.strip() != auth_host:
        _header_drop(headers, "Host")

    if not _header_get(headers, "Accept"):
        headers["Accept"] = "*/*"
    if not _header_get(headers, "Content-Type"):
        headers["Content-Type"] = "application/json"
    if not _header_get(headers, "Accept-Language"):
        headers["Accept-Language"] = "en-CA,en-US;q=0.9,en;q=0.8"
    # Never advertise brotli (the mobile headers do): without the `brotli` package requests
    # can't decode it, so a successful token response was unreadable → "upstream:bad_json".
    _header_drop(headers, "Accept-Encoding")
    headers["Accept-Encoding"] = "gzip, deflate"
    if not _header_get(headers, "Connection"):
        headers["Connection"] = "keep-alive"
    if not _header_get(headers, "User-Agent"):
        headers["User-Agent"] = P1_USER_AGENT
    return headers


def _redact_sensitive_text(raw: str, max_len: int = 800) -> str:
    text = str(raw or "")
    # JSON-like token fields
    text = re.sub(
        r'("?(?:access_token|refresh_token|id_token)"?\s*:\s*")([^"]+)(")',
        r"\1***REDACTED***\3",
        text,
        flags=re.IGNORECASE,
    )
    # Bearer values
    text = re.sub(
        r"(Bearer\s+)[A-Za-z0-9\-\._~\+/=]+",
        r"\1***REDACTED***",
        text,
        flags=re.IGNORECASE,
    )
    # Cookie header/value
    text = re.sub(
        r'("?(?:cookie|set-cookie)"?\s*:\s*")([^"]+)(")',
        r"\1***REDACTED***\3",
        text,
        flags=re.IGNORECASE,
    )
    if len(text) > max_len:
        return text[:max_len] + "...(truncated)"
    return text


def _mask_value(val: Optional[str], keep: int = 4) -> str:
    s = str(val or "").strip()
    if not s:
        return "—"
    if len(s) <= keep * 2:
        return s[:keep] + "..." if len(s) > keep else s
    return f"{s[:keep]}...{s[-keep:]}"


def _fp8(val: Optional[str]) -> str:
    s = str(val or "").strip()
    if not s:
        return "—"
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:8]


def refresh_p1_access_token(
    refresh_token: str,
    client_id: str,
    oauth_headers: Optional[dict] = None,
) -> Tuple[bool, Optional[str], Optional[str], str]:
    if not refresh_token:
        return False, None, None, "missing_refresh_token"
    if not client_id:
        return False, None, None, "missing_client_id"

    url = f"{MOBILE_AUTH_BASE}/oauth/token"
    payload = {
        "refresh_token": refresh_token,
        "client_id": client_id,
        "grant_type": "refresh_token",
    }
    headers = _build_oauth_headers(None, oauth_headers)
    timeout_s = max(5, int(P1_POLL_TIMEOUT_S))
    try:
        r = _session_request("POST", url, headers=headers, json=payload, timeout=timeout_s)
        if not (200 <= r.status_code < 300):
            _builtins.print(
                f"[{datetime.now()}] ❌ P1 refresh failed status={r.status_code} "
                f"body={_redact_sensitive_text(r.text)}"
            )
        if 200 <= r.status_code < 300:
            try:
                j = r.json() or {}
            except Exception:
                _builtins.print(
                    f"[{datetime.now()}] ❌ P1 refresh: HTTP {r.status_code} but body is not JSON | "
                    f"url={r.url} redirects={len(r.history)} | content-type={r.headers.get('Content-Type')} | "
                    f"content-encoding={r.headers.get('Content-Encoding')} | body={_redact_sensitive_text(r.text, 300)}"
                )
                return False, None, None, "upstream:bad_json"
            if not isinstance(j, dict):
                return False, None, None, "upstream:bad_json"
            root = j.get("result") if isinstance(j.get("result"), dict) else j
            access = root.get("access_token")
            new_refresh = root.get("refresh_token") or refresh_token
            if access:
                # Auth0 returns a raw JWT; the API needs "Bearer <jwt>" (sent as-is in Authorization).
                access = _normalize_bearer(access)
                if not access.lower().startswith("bearer "):
                    access = f"Bearer {access}"
                return True, access, str(new_refresh), "ok"
            return False, None, None, "upstream:no_access_token"
        if r.status_code in (400, 401, 403):
            return False, None, None, f"unauthorized:{r.status_code}"
        return False, None, None, f"upstream:{r.status_code}"
    except requests.exceptions.RequestException as e:
        _builtins.print(f"[{datetime.now()}] ❌ P1 refresh network error: {type(e).__name__}: {e}")
        return False, None, None, f"network:{type(e).__name__}"


def maybe_refresh_p1_session(
    bot_id: str,
    telegram_id: int,
    token: Optional[str],
    mobile_headers: Optional[dict],
    force: bool = False,
) -> Tuple[Optional[str], Optional[dict], bool, str]:
    current_token = _normalize_bearer(token)
    if not force and current_token and not _needs_refresh(current_token):
        return current_token, mobile_headers, False, "not_expiring"

    auth_meta = get_mobile_auth(bot_id, telegram_id) or {}
    refresh_token = auth_meta.get("refresh_token")
    client_id = auth_meta.get("client_id") or MOBILE_CLIENT_ID
    oauth_headers = auth_meta.get("oauth_headers") if isinstance(auth_meta.get("oauth_headers"), dict) else None

    if not refresh_token or not client_id:
        return current_token, mobile_headers, False, "missing_refresh_material"

    req_headers = _build_oauth_headers(mobile_headers, oauth_headers)
    ok, new_token, new_refresh, note = refresh_p1_access_token(
        refresh_token=str(refresh_token),
        client_id=str(client_id),
        oauth_headers=req_headers,
    )
    if not ok or not new_token:
        return current_token, mobile_headers, False, note

    # The stored refresh_token may come from an older session: never switch chauffeur account.
    for claim in ("chauffeur_id", "sub"):
        old_val, new_val = _jwt_claim(current_token, claim), _jwt_claim(new_token, claim)
        if old_val and new_val and old_val != new_val:
            return current_token, mobile_headers, False, f"refresh_token_other_account:{claim}"

    next_headers = dict(mobile_headers or {})
    _header_drop(next_headers, "Authorization")

    next_auth_meta = dict(auth_meta)
    next_auth_meta["client_id"] = str(client_id)
    next_auth_meta["refresh_token"] = str(new_refresh or refresh_token)
    if oauth_headers:
        next_auth_meta["oauth_headers"] = oauth_headers

    headers_to_save = next_headers if mobile_headers is not None else None
    update_token(
        bot_id,
        telegram_id,
        new_token,
        headers=headers_to_save,
        auth_meta=next_auth_meta,
    )
    set_token_status(bot_id, telegram_id, "valid")
    runtime_headers = next_headers if next_headers else None
    return new_token, runtime_headers, True, "ok"


_PKCE_REDIRECT_URI = "com.blacklane.chauffeur://login-chauffeur.blacklane.com/ios/com.blacklane.chauffeur/callback"
# Login page answered with a human-verification challenge: an automated login cannot succeed,
# and retrying only reinforces the bot suspicion. Callers stop retrying and alert the user.
PLAYWRIGHT_CAPTCHA_NOTE = "playwright:captcha"
_CAPTCHA_FRAME_HINTS = ("challenges.cloudflare.com", "turnstile", "hcaptcha", "recaptcha", "arkoselabs", "captcha")
_CAPTCHA_TEXT_HINTS = ("captcha", "verify you are human", "vérifiez que vous êtes humain", "are you a robot")


def new_pkce_login_url() -> Tuple[str, str]:
    """(authorize_url, code_verifier) for the chauffeur-app PKCE login (offline_access → refresh_token)."""
    code_verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode()
    state_val = base64.urlsafe_b64encode(secrets.token_bytes(16)).rstrip(b"=").decode()
    authorize_url = (
        f"{MOBILE_AUTH_BASE}/authorize?response_type=code&state={state_val}"
        f"&scope=openid%20profile%20email%20read:current_user%20offline_access"
        f"&code_challenge={code_challenge}&code_challenge_method=S256"
        f"&audience=https://blacklane.com"
        f"&redirect_uri={_PKCE_REDIRECT_URI}&client_id={MOBILE_CLIENT_ID}"
    )
    return authorize_url, code_verifier


def extract_pkce_code(url: Optional[str]) -> Optional[str]:
    """Authorization code from the app redirect (com.blacklane.chauffeur://...?code=...)."""
    if url and url.startswith("com.blacklane"):
        m = re.search(r"[?&]code=([^&]+)", url)
        if m:
            return m.group(1)
    return None


def exchange_pkce_code(code: str, code_verifier: str) -> Tuple[bool, Optional[str], Optional[str], str]:
    """Exchange the PKCE code for (ok, "Bearer <access>", refresh_token, note)."""
    try:
        r = _session_request(
            "POST",
            f"{MOBILE_AUTH_BASE}/oauth/token",
            json={
                "grant_type": "authorization_code",
                "client_id": MOBILE_CLIENT_ID,
                "code": code,
                "code_verifier": code_verifier,
                "redirect_uri": _PKCE_REDIRECT_URI,
            },
            headers={"Accept": "application/json", "Accept-Encoding": "gzip, deflate"},
            timeout=10,
        )
        if r.status_code == 200:
            data = r.json()
            access = data.get("access_token")
            new_refresh = data.get("refresh_token")
            if access:
                return True, f"Bearer {access.strip()}", new_refresh, "ok"
            return False, None, None, "playwright:no_access_token"
        return False, None, None, f"playwright:token_exchange_{r.status_code}"
    except Exception as e:
        return False, None, None, f"playwright:exchange_error:{type(e).__name__}"


async def _page_shows_captcha(page) -> bool:
    """True if the login page shows a human-verification challenge (captcha frame or text)."""
    try:
        if any(h in (f.url or "").lower() for f in page.frames for h in _CAPTCHA_FRAME_HINTS):
            return True
        text = (await page.inner_text("body", timeout=3000)).lower()
        return any(h in text for h in _CAPTCHA_TEXT_HINTS)
    except Exception:
        return False


async def _playwright_pkce_login(email: str, password: str) -> Tuple[bool, Optional[str], Optional[str], str]:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return False, None, None, "playwright_not_installed"

    authorize_url, code_verifier = new_pkce_login_url()

    code_holder: dict = {}
    screenshot_path: dict = {}
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                    "--disable-extensions",
                ],
            )
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_3_2 like Mac OS X) "
                    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/22D82"
                ),
                viewport={"width": 390, "height": 844},
                device_scale_factor=3,
                is_mobile=True,
                has_touch=True,
                locale="en-CA",
            )
            # Mask webdriver flag that headless Chromium exposes by default
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
            page = await context.new_page()

            def _extract_code_from_url(url: str):
                code = extract_pkce_code(url)
                if code:
                    code_holder["code"] = code

            async def _on_request(request):
                _extract_code_from_url(request.url)

            # Also catch JS-driven redirects (window.location = ...) via framenavigated
            async def _on_framenavigated(frame):
                _extract_code_from_url(frame.url)

            page.on("request", _on_request)
            page.on("framenavigated", _on_framenavigated)

            await page.goto(authorize_url)
            # Randomise typing and submission delays to avoid fixed-interval bot signatures
            await page.wait_for_timeout(int(800 + secrets.randbelow(600)))
            await page.fill('input[name="username"]', email)
            await page.wait_for_timeout(int(300 + secrets.randbelow(400)))
            await page.fill('input[name="password"]', password)
            await page.wait_for_timeout(int(200 + secrets.randbelow(300)))
            try:
                await page.click('button[type="submit"]', timeout=10000)
                await page.wait_for_timeout(int(3000 + secrets.randbelow(2000)))
            except Exception:
                pass

            # If no code yet, save a screenshot so we can diagnose what the browser sees
            if not code_holder.get("code"):
                screenshot_path["captcha"] = await _page_shows_captcha(page)
                try:
                    import os as _os
                    _snap_dir = "/tmp/bl_playwright"
                    _os.makedirs(_snap_dir, exist_ok=True)
                    _snap = f"{_snap_dir}/login_fail_{int(time.time())}.png"
                    await page.screenshot(path=_snap, full_page=True)
                    screenshot_path["path"] = _snap
                    # Also capture the current URL and page title for diagnosis
                    screenshot_path["url"] = page.url
                    screenshot_path["title"] = await page.title()
                except Exception:
                    pass

            await browser.close()
    except Exception as e:
        return False, None, None, f"playwright_browser:{type(e).__name__}"

    code = code_holder.get("code")
    if not code:
        _snap = screenshot_path.get("path", "n/a")
        _url = screenshot_path.get("url", "n/a")
        _title = screenshot_path.get("title", "n/a")
        if screenshot_path.get("captcha"):
            _builtins.print(
                f"[{datetime.now()}] ❌ Playwright blocked by human verification (captcha) | "
                f"page_url={_url} | screenshot={_snap}"
            )
            return False, None, None, PLAYWRIGHT_CAPTCHA_NOTE
        _builtins.print(
            f"[{datetime.now()}] ❌ Playwright no_code_captured | "
            f"page_url={_url} | page_title={_title!r} | screenshot={_snap}"
        )
        return False, None, None, "playwright:no_code_captured"

    return exchange_pkce_code(code, code_verifier)


def get_playwright_p1_token(
    bot_id: str,
    telegram_id: int,
    email: str,
    password: str,
) -> Tuple[bool, Optional[str], Optional[str], str]:
    """Run headless PKCE login. Returns (ok, access_token, refresh_token, note). Does NOT save to DB."""
    if not email or not password:
        return False, None, None, "missing_credentials"
    try:
        loop = asyncio.new_event_loop()
        ok, new_token, new_refresh, note = loop.run_until_complete(
            _playwright_pkce_login(email, password)
        )
        loop.close()
    except Exception as e:
        _builtins.print(f"[{datetime.now()}] ❌ P1 Playwright error for {bot_id}/{telegram_id}: {e}")
        return False, None, None, f"playwright_exception:{type(e).__name__}"

    if not ok or not new_token:
        _builtins.print(f"[{datetime.now()}] ❌ P1 Playwright failed for {bot_id}/{telegram_id}: {note}")
        return False, None, None, note

    return True, new_token, new_refresh, "ok"


def save_playwright_p1_token(
    bot_id: str,
    telegram_id: int,
    new_token: str,
    new_refresh: Optional[str],
    mobile_headers: Optional[dict] = None,
) -> None:
    """Persist a Playwright-obtained token to DB after verifying it works."""
    auth_meta = get_mobile_auth(bot_id, telegram_id) or {}
    auth_meta["client_id"] = str(auth_meta.get("client_id") or MOBILE_CLIENT_ID)
    if new_refresh:
        auth_meta["refresh_token"] = str(new_refresh)
    update_token(bot_id, telegram_id, new_token, headers=mobile_headers, auth_meta=auth_meta)
    set_token_status(bot_id, telegram_id, "valid")
