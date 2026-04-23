import base64
import hashlib
import json
import secrets
import time
import uuid
import threading
import requests
import builtins as _builtins
from typing import Optional, Tuple
from datetime import datetime

from .config import (
    API_HOST,
    P1_POLL_TIMEOUT_S,
    P1_RESERVE_TIMEOUT_S,
    LOG_RAW_API_RESPONSES,
    LOG_POLL_SUCCESS_RESPONSES,
    POLL_SUCCESS_RESPONSE_LOG_LIMIT,
    P1_STRIP_VOLATILE_HEADERS,
    P1_FORCE_FRESH_REQUEST_IDS,
    P1_USER_AGENT,
    P1_ENABLE_RUM_HEADERS,
    HTTP_POOL_SIZE,
)


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print


def _log_poll_response(label: str, status: int, body: str):
    return None


def _log_success_response(label: str, status: int, offer_count: int, body, raw_text: Optional[str] = None):
    if not LOG_POLL_SUCCESS_RESPONSES:
        return
    try:
        text = raw_text if raw_text is not None else json.dumps(body, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        text = str(body)
    text = " ".join(str(text).split())
    limit = int(POLL_SUCCESS_RESPONSE_LOG_LIMIT or 0)
    if limit > 0 and len(text) > limit:
        text = text[:limit] + "...(truncated)"
    _builtins.print(
        f"[{datetime.now()}] ✅ {label} status={status} offers={offer_count} response={text}"
    )


_thread_local = threading.local()
_trace_session_lock = threading.Lock()
_trace_session_ids = {}


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


# ── Shared reserve session ────────────────────────────────────────────────────
# NOT thread-local: shared across all _reserve_executor workers so the
# connection pool stays warm even when individual threads are idle.
# Reserves are rare (only on valid offers) so thread-local sessions go cold
# between uses, paying a full TCP+TLS handshake (~600ms) each time.
_reserve_session_lock = threading.Lock()
_reserve_session: Optional[requests.Session] = None


def _get_reserve_session() -> requests.Session:
    global _reserve_session
    if _reserve_session is None:
        with _reserve_session_lock:
            if _reserve_session is None:
                sess = requests.Session()
                sess.trust_env = False
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=HTTP_POOL_SIZE,
                    pool_maxsize=HTTP_POOL_SIZE,
                    max_retries=0,
                )
                sess.mount("https://", adapter)
                sess.mount("http://", adapter)
                _reserve_session = sess
    return _reserve_session


def warmup_p1_reserve_connection(token: str, headers: Optional[dict] = None):
    """Pre-warm the shared reserve session with a GET /offers.
    Call at startup and every ~45s to keep the TCP/TLS connection alive."""
    try:
        hdrs = _merge_headers(token, headers)
        _reserve_session_lock  # just reference to ensure module loaded
        sess = _get_reserve_session()
        try:
            sess.cookies.clear()
        except Exception:
            pass
        sess.request("GET", f"{API_HOST}/offers", headers=hdrs, timeout=max(3, int(P1_POLL_TIMEOUT_S)))
    except Exception:
        pass


def _has_header(headers: dict, name: str) -> bool:
    lname = name.lower()
    return any(k.lower() == lname for k in headers.keys())


def _header_drop(headers: dict, name: str):
    lname = name.lower()
    for k in list(headers.keys()):
        if str(k).lower() == lname:
            headers.pop(k, None)


def _is_volatile_header(name: str) -> bool:
    lname = str(name or "").lower()
    if lname.startswith("x-datadog-"):
        return True
    return lname in {
        "x-request-id",
        "x-correlation-id",
        "traceparent",
        "tracestate",
        "baggage",
        "content-length",
    }


def _is_rum_header(name: str) -> bool:
    lname = str(name or "").lower()
    return lname.startswith("x-datadog-") or lname in {
        "traceparent",
        "tracestate",
        "baggage",
    }


def _drop_rum_headers(headers: dict):
    for k in list(headers.keys()):
        if _is_rum_header(k):
            headers.pop(k, None)


def _jwt_payload_unverified(token: str) -> dict:
    try:
        raw = str(token or "").strip()
        if raw.lower().startswith("bearer "):
            raw = raw[7:].strip()
        parts = raw.split(".")
        if len(parts) != 3:
            return {}
        padded = parts[1] + ("=" * (-len(parts[1]) % 4))
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _token_fingerprint(token: str) -> str:
    try:
        return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:16]
    except Exception:
        return "unknown"


def _trace_session_id(user_id: Optional[str], token: str) -> str:
    key = str(user_id or _token_fingerprint(token))
    with _trace_session_lock:
        sid = _trace_session_ids.get(key)
        if not sid:
            sid = str(uuid.uuid4())
            _trace_session_ids[key] = sid
        return sid


def _rand64_nonzero() -> int:
    value = 0
    while value == 0:
        value = secrets.randbits(64)
    return value


def _apply_fresh_rum_headers(headers: dict, token: str):
    _drop_rum_headers(headers)

    payload = _jwt_payload_unverified(token)
    user_id = payload.get("chauffeur_id")
    if user_id is not None:
        user_id = str(user_id)

    tid_hex = f"{int(time.time()):08x}00000000"
    trace_low = _rand64_nonzero()
    parent_id = _rand64_nonzero()
    trace_low_hex = f"{trace_low:016x}"
    parent_hex = f"{parent_id:016x}"
    trace_id = f"{tid_hex}{trace_low_hex}"

    headers["x-datadog-sampling-priority"] = "1"
    headers["x-datadog-trace-id"] = str(trace_low)
    headers["x-datadog-parent-id"] = str(parent_id)
    headers["x-datadog-tags"] = f"_dd.p.tid={tid_hex},_dd.p.dm=-1"
    headers["x-datadog-origin"] = "rum"
    headers["traceparent"] = f"00-{trace_id}-{parent_hex}-01"
    headers["tracestate"] = f"dd=o:rum;p:{parent_hex};s:1;t.dm:-1"

    baggage = [f"session.id={_trace_session_id(user_id, token)}"]
    if user_id:
        baggage.append(f"user.id={user_id}")
    headers["baggage"] = ",".join(baggage)


def _merge_headers(token: str, base_headers: Optional[dict] = None) -> dict:
    if base_headers:
        headers = {}
        for k, v in base_headers.items():
            if v is None:
                continue
            if P1_STRIP_VOLATILE_HEADERS and _is_volatile_header(k):
                continue
            headers[k] = v
        # Build lowercase key set once — replaces 8 individual O(N) _has_header scans.
        _lk = {k.lower() for k in headers}
        if "host" not in _lk:
            headers["Host"] = API_HOST.replace("https://", "")
        if "accept" not in _lk:
            headers["Accept"] = "*/*"
        if "accept-language" not in _lk:
            headers["Accept-Language"] = "en-CA,en-US;q=0.9,en;q=0.8"
        if "accept-encoding" not in _lk:
            headers["Accept-Encoding"] = "gzip, deflate, br"
        if "content-type" not in _lk:
            headers["Content-Type"] = "application/json"
        if "x-operating-system" not in _lk:
            headers["X-Operating-System"] = "iOS"
        if "user-agent" not in _lk:
            headers["User-Agent"] = P1_USER_AGENT
        if "connection" not in _lk:
            headers["Connection"] = "keep-alive"
    else:
        headers = {
            "Host": API_HOST.replace("https://", ""),
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "X-Request-ID": str(uuid.uuid4()),
            "X-Correlation-ID": str(uuid.uuid4()),
            "X-Operating-System": "iOS",
            "User-Agent": P1_USER_AGENT,
            "Connection": "keep-alive",
        }
    _header_drop(headers, "User-Agent")
    headers["User-Agent"] = P1_USER_AGENT

    if P1_FORCE_FRESH_REQUEST_IDS:
        _header_drop(headers, "X-Request-ID")
        _header_drop(headers, "X-Correlation-ID")
        headers["X-Request-ID"] = str(uuid.uuid4())
        headers["X-Correlation-ID"] = str(uuid.uuid4())
    else:
        if not _has_header(headers, "X-Request-ID"):
            headers["X-Request-ID"] = str(uuid.uuid4())
        if not _has_header(headers, "X-Correlation-ID"):
            headers["X-Correlation-ID"] = str(uuid.uuid4())

    if P1_ENABLE_RUM_HEADERS:
        _apply_fresh_rum_headers(headers, token)

    headers["Authorization"] = token
    return headers


def get_rides_p1(token: str, headers: Optional[dict] = None) -> Tuple[Optional[int], Optional[list]]:
    headers = _merge_headers(token, headers)
    try:
        r = _session_request("GET", f"{API_HOST}/rides", headers=headers, timeout=P1_POLL_TIMEOUT_S)
        if 200 <= r.status_code < 300:
            try:
                data = r.json()
            except Exception:
                return 200, []
            if isinstance(data, list):
                return 200, data
            if isinstance(data, dict):
                for key in ("results", "rides", "data", "items"):
                    val = data.get(key)
                    if isinstance(val, list):
                        return 200, val
                return 200, [data] if data else []
            return 200, []
        return r.status_code, None
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        # silence poll logs
        return None, {"error": err}


def get_offers_p1(token: str, headers: Optional[dict] = None):
    headers = _merge_headers(token, headers)
    try:
        r = _session_request("GET", f"{API_HOST}/offers", headers=headers, timeout=P1_POLL_TIMEOUT_S)
        raw_text = r.text if (LOG_RAW_API_RESPONSES or LOG_POLL_SUCCESS_RESPONSES) else None
        try:
            body = r.json()
        except Exception:
            body = r.text

        if r.status_code == 200 and isinstance(body, dict):
            results = body.get("results", []) or []
            _log_success_response("P1 poll /offers", r.status_code, len(results), body, raw_text)
            if results and LOG_RAW_API_RESPONSES:
                _builtins.print(f"[{datetime.now()}] 🛰️ P1 poll /offers full response -> {raw_text}")
            for it in results:
                try:
                    it["_platform"] = "p1"
                except Exception:
                    pass
            return 200, results

        if r.status_code == 200:
            _log_success_response("P1 poll /offers", r.status_code, 0, body, raw_text)
            return 200, []

        # return status + body for diagnostics (401/403/etc)
        return r.status_code, body
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        # silence poll logs
        return None, {"error": err}


def reserve_offer_p1(token: str, offer_id: str, price: Optional[float] = None, headers: Optional[dict] = None):
    """
    Accept (reserve) an offer on Platform 1.

    Returns: (status_code, json_or_text)
      200/201 → accepted
      401/403 → token invalid/expired
      409      → conflict / already taken
      422      → cannot accept (validation)
    """
    headers = _merge_headers(token, headers)
    payload = {
        "id": offer_id,
        "action": "accept",
    }
    if price is not None:
        try:
            payload["price"] = float(price)
        except Exception:
            payload["price"] = price
    try:
        sess = _get_reserve_session()
        try:
            sess.cookies.clear()
        except Exception:
            pass
        r = sess.request(
            "POST",
            f"{API_HOST}/offers",
            headers=headers,
            json=payload,
            timeout=P1_RESERVE_TIMEOUT_S,
        )
        try:
            body = r.json()
        except Exception:
            body = r.text
        return r.status_code, body
    except requests.exceptions.RequestException as e:
        return None, {"error": f"{type(e).__name__}: {e}"}
