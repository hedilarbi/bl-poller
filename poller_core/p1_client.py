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
    P1_STRIP_VOLATILE_HEADERS,
    P1_FORCE_FRESH_REQUEST_IDS,
    HTTP_POOL_SIZE,
)


def _quiet_print(*args, **kwargs):
    return None


print = _quiet_print


def _log_poll_response(label: str, status: int, body: str):
    return None


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
            headers["User-Agent"] = "Chauffeur/18575 CFNetwork/3860.300.31 Darwin/25.2.0"
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
            "User-Agent": "Chauffeur/18575 CFNetwork/3860.300.31 Darwin/25.2.0",
            "Connection": "keep-alive",
        }
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
        raw_text = r.text if LOG_RAW_API_RESPONSES else None
        try:
            body = r.json()
        except Exception:
            body = r.text

        if r.status_code == 200 and isinstance(body, dict):
            results = body.get("results", []) or []
            if results and LOG_RAW_API_RESPONSES:
                _builtins.print(f"[{datetime.now()}] 🛰️ P1 poll /offers full response -> {raw_text}")
            for it in results:
                try:
                    it["_platform"] = "p1"
                except Exception:
                    pass
            return 200, results

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
