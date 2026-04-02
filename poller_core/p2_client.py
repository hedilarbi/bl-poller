import base64
import json
import threading
import builtins as _builtins
from typing import Optional, Tuple
from datetime import datetime
import requests

from .config import (
    ATHENA_BASE,
    PARTNER_API_BASE,
    PORTAL_CLIENT_ID,
    PORTAL_PAGE_SIZE,
    ATHENA_RELOGIN_SKEW_S,
    ATHENA_PRINT_DEBUG,
    P2_POLL_TIMEOUT_S,
    P2_RESERVE_TIMEOUT_S,
    LOG_RAW_API_RESPONSES,
    HTTP_POOL_SIZE,
)
from db import get_portal_token, update_portal_token


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


# ── Shared reserve session (P2) ──────────────────────────────────────────────
_p2_reserve_session_lock = threading.Lock()
_p2_reserve_session: Optional[requests.Session] = None


def _get_p2_reserve_session() -> requests.Session:
    global _p2_reserve_session
    if _p2_reserve_session is None:
        with _p2_reserve_session_lock:
            if _p2_reserve_session is None:
                sess = requests.Session()
                sess.trust_env = False
                adapter = requests.adapters.HTTPAdapter(
                    pool_connections=HTTP_POOL_SIZE,
                    pool_maxsize=HTTP_POOL_SIZE,
                    max_retries=0,
                )
                sess.mount("https://", adapter)
                sess.mount("http://", adapter)
                _p2_reserve_session = sess
    return _p2_reserve_session


def warmup_p2_reserve_connection(access_token: str):
    """Pre-warm the shared P2 reserve session with a GET /chauffeur/offers.
    Call at startup and every ~45s to keep the TCP/TLS connection alive."""
    try:
        sess = _get_p2_reserve_session()
        try:
            sess.cookies.clear()
        except Exception:
            pass
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "*/*",
        }
        sess.request("GET", f"{PARTNER_API_BASE}/chauffeur/offers", headers=headers, timeout=5)
    except Exception:
        pass


def _safe_attr(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


def _find_included(included: list, typ: str, id_: str):
    for it in (included or []):
        if it.get("type") == typ and str(it.get("id")) == str(id_):
            return it
    return None


def _extract_loc_from_included(included_item: dict) -> dict:
    if not isinstance(included_item, dict):
        return {}
    attrs = included_item.get("attributes") or {}
    address = (
        attrs.get("formatted_address_en")
        or attrs.get("formatted_address_de")
        or attrs.get("formatted_address")
        or ", ".join([x for x in [attrs.get("city"), attrs.get("country_code")] if x])
    )
    name = attrs.get("airport_iata") or attrs.get("city")
    out = {}
    if name:
        out["name"] = str(name)
    if address:
        out["address"] = str(address)
    return out


def _normalize_vclass(name: str) -> str:
    m = {
        "business": "Business",
        "first": "First",
        "suv": "SUV",
        "van": "VAN",
        "electric": "Electric",
        "sprinter": "Sprinter",
    }
    key = (name or "").strip().lower()
    return m.get(key, name or "")


def _map_portal_offer(raw: dict, included_idx: dict) -> Optional[dict]:
    """Convert Athena JSON:API offer into the internal shape.
    included_idx must be pre-built as {(type, str(id)): item} before the offer loop."""
    if not isinstance(raw, dict):
        return None

    attrs = raw.get("attributes") or {}
    rel = raw.get("relationships") or {}
    oid = str(raw.get("id") or "")
    if not oid:
        return None

    pickup_iso = attrs.get("starts_at") or attrs.get("pickup_time") or attrs.get("start_time")
    if not pickup_iso:
        return None

    price_raw = attrs.get("price")
    try:
        price = float(price_raw) if price_raw is not None else None
    except Exception:
        price = price_raw
    currency = attrs.get("currency") or "USD"

    vclass = _normalize_vclass(
        attrs.get("service_class") or attrs.get("vehicle_class") or attrs.get("class") or ""
    )
    rid_type = (attrs.get("booking_type") or "").strip().lower() or "transfer"

    est_dist = attrs.get("distance")
    try:
        if est_dist is not None:
            est_dist = float(est_dist)
    except Exception:
        pass

    est_dur_raw = attrs.get("estimated_duration")
    est_dur_min = None
    try:
        if est_dur_raw is not None:
            val = float(est_dur_raw)
            est_dur_min = val / 60.0 if val > 1000 else val
    except Exception:
        est_dur_min = None

    pu_rel = _safe_attr(rel, "pickup_location", "data")
    do_rel = _safe_attr(rel, "dropoff_location", "data")

    pickUpLocation = {}
    dropOffLocation = None

    if pu_rel and pu_rel.get("id") and pu_rel.get("type"):
        inc = included_idx.get((pu_rel["type"], str(pu_rel["id"])))
        pickUpLocation = _extract_loc_from_included(inc) or {}
    if do_rel and do_rel.get("id") and do_rel.get("type"):
        inc = included_idx.get((do_rel["type"], str(do_rel["id"])))
        dropOffLocation = _extract_loc_from_included(inc) or None

    flight_no = attrs.get("flight_number")
    special_reqs = attrs.get("special_requests")

    ride = {
        "type": rid_type,  # "transfer" | "hourly"
        "pickUpLocation": pickUpLocation,
        "pickupTime": pickup_iso,
    }
    if dropOffLocation:
        ride["dropOffLocation"] = dropOffLocation
    if est_dist is not None:
        ride["estimatedDistanceMeters"] = est_dist
    if est_dur_min is not None:
        ride["estimatedDurationMinutes"] = est_dur_min
        ride["durationMinutes"] = est_dur_min
    if flight_no:
        ride["flight"] = {"number": str(flight_no)}
        ride["flight_number"] = str(flight_no)
    if special_reqs:
        ride["guestRequests"] = special_reqs

    mapped = {
        "type": "ride",
        "id": oid,                 # keep real ID (no prefix)
        "price": price,
        "currency": currency,
        "vehicleClass": vclass,
        "rides": [ride],
        "_platform": "p2",         # mark platform explicitly
    }
    if flight_no:
        mapped["flight_number"] = str(flight_no)
    return mapped


def reserve_offer_p2(
    access_token: str,
    offer_id: str,
    price: float,
    bl_user_id: Optional[str] = None,
    roles: Optional[str] = None,
    extra_headers: Optional[dict] = None,
):
    """
    Accept an offer on Platform 2 (Partner Portal).

    Returns: (status_code, json_or_text)
    """
    url = f"{PARTNER_API_BASE}/chauffeur/offers"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://partner.blacklane.com",
        "Referer": "https://partner.blacklane.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "X-User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Blacklane-User-Id": str(bl_user_id or ""),
        "Blacklane-User-Roles": roles or "dispatcher,driver,provider,admin,reviewer",
        "X-Datadog-Origin": "rum",
        "X-Datadog-Sampling-Priority": "1",
    }
    if extra_headers:
        for k, v in extra_headers.items():
            if v is not None:
                headers[k] = v

    payload = {"action": "accept", "id": str(offer_id), "price": float(price)}

    try:
        sess = _get_p2_reserve_session()
        try:
            sess.cookies.clear()
        except Exception:
            pass
        r = sess.request("POST", url, headers=headers, json=payload, timeout=P2_RESERVE_TIMEOUT_S)
        try:
            body = r.json()
        except Exception:
            body = r.text
        return r.status_code, body
    except requests.exceptions.RequestException as e:
        return None, {"error": f"{type(e).__name__}: {e}"}


def _athena_login(email: str, password: str) -> Tuple[bool, Optional[str], str]:
    url = f"{ATHENA_BASE}/oauth/token"
    payload = {
        "client_id": PORTAL_CLIENT_ID,
        "username": email,
        "password": password,
        "grant_type": "implicit",
        "resource_owner_type": "driver",
    }
    headers = {"Accept": "application/json"}
    try:
        r = _session_request("POST", url, data=payload, headers=headers, timeout=P2_POLL_TIMEOUT_S)
        if 200 <= r.status_code < 300:
            try:
                j = r.json() or {}
            except Exception:
                return (False, None, "upstream:bad_json")
            token = (j.get("result") or {}).get("access_token") or j.get("access_token")
            if token:
                return (True, token, "ok")
            return (False, None, "upstream:no_token")
        if r.status_code in (401, 403):
            return (False, None, f"unauthorized:{r.status_code}")
        return (False, None, f"upstream:{r.status_code}")
    except requests.exceptions.RequestException as e:
        return (False, None, f"network:{type(e).__name__}")


def _jwt_exp_unverified(token: str) -> Optional[int]:
    """Best-effort read of 'exp' (seconds since epoch) from a JWT without verifying; None if not readable."""
    try:
        parts = (token or "").split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1] + "==="
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("utf-8")))
        exp = payload.get("exp")
        return int(exp) if isinstance(exp, (int, float)) else None
    except Exception:
        return None


def _portal_token_expired(token: Optional[str]) -> bool:
    if not token:
        return True
    exp = _jwt_exp_unverified(token)
    if exp is None:
        # If we can't tell, assume valid and let 401 drive a re-login
        return False
    now = int(datetime.now().timestamp())
    return now >= (exp - ATHENA_RELOGIN_SKEW_S)


def _ensure_portal_token(bot_id: str, telegram_id: int, email: str, password: str) -> Optional[str]:
    """Get token from DB and refresh if missing/expired."""
    portal_token = get_portal_token(bot_id, telegram_id)
    if isinstance(portal_token, (list, tuple)):
        portal_token = portal_token[0] if portal_token else None

    needs_login = _portal_token_expired(portal_token)
    if not portal_token or needs_login:
        ok, new_tok, note = _athena_login(email, password)
        if ok and new_tok:
            update_portal_token(bot_id, telegram_id, new_tok)
            portal_token = new_tok
            if ATHENA_PRINT_DEBUG:
                print(f"[{datetime.now()}] 🔐 Athena login OK for user {telegram_id}.")
        else:
            print(f"[{datetime.now()}] ❌ Portal login failed for user {telegram_id}: {note}")
            portal_token = None
    return portal_token


def _athena_get_offers(
    access_token: str,
    page: int = 1,
    page_size: int = PORTAL_PAGE_SIZE,
    etag: Optional[str] = None,
):
    """
    Returns (status_code, payload_or_None, response_etag_or_None).
    On 304, payload is None and etag may be returned by upstream.
    """
    url = (
        f"{ATHENA_BASE}/hades/offers"
        f"?page%5Bnumber%5D={page}&page%5Bsize%5D={page_size}"
        f"&include=pickup_location%2Cdropoff_location"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.api+json",
        "Connection": "keep-alive",
        "User-Agent": "BLPortal/1.0 (+poller)",
    }
    if etag:
        headers["If-None-Match"] = etag
    try:
        r = _session_request("GET", url, headers=headers, timeout=P2_POLL_TIMEOUT_S)
        raw_text = r.text if LOG_RAW_API_RESPONSES else None
        new_etag = r.headers.get("etag") or r.headers.get("ETag")
        if r.status_code == 304:
            return 304, None, new_etag
        if 200 <= r.status_code < 300:
            try:
                payload = r.json()
                if LOG_RAW_API_RESPONSES and isinstance(payload, dict) and (payload.get("data") or []):
                    _builtins.print(f"[{datetime.now()}] 🛰️ P2 poll /hades/offers full response -> {raw_text}")
                if isinstance(payload, dict) and new_etag:
                    payload["__etag"] = new_etag
                return r.status_code, payload, new_etag
            except Exception:
                return r.status_code, None, new_etag
        return r.status_code, None, new_etag
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ❌ Athena offers error: {e}")
        return None, None, None


def _athena_get_rides(
    access_token: str,
    page: int = 1,
    page_size: int = PORTAL_PAGE_SIZE,
    etag: Optional[str] = None,
):
    """
    Poll Athena 'planned' rides. Returns (status_code, payload_or_None, response_etag_or_None).
    Endpoint provided by you:
    /hades/rides?page[number]=1&page[size]=30&include=pickup_location%2Cdropoff_location%2Caccepted_by%2Cassigned_driver%2Cassigned_vehicle%2Cavailable_drivers%2Cavailable_vehicles%2Cstatus_updates&filter[group]=planned
    """
    url = (
        f"{ATHENA_BASE}/hades/rides"
        f"?page%5Bnumber%5D={page}&page%5Bsize%5D={page_size}"
        f"&include=pickup_location%2Cdropoff_location%2Caccepted_by%2Cassigned_driver%2Cassigned_vehicle%2Cavailable_drivers%2Cavailable_vehicles%2Cstatus_updates"
        f"&filter%5Bgroup%5D=planned"
    )
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/vnd.api+json",
        "Connection": "keep-alive",
        "User-Agent": "BLPortal/1.0 (+poller)",
    }
    if etag:
        headers["If-None-Match"] = etag
    try:
        r = _session_request("GET", url, headers=headers, timeout=P2_POLL_TIMEOUT_S)
        new_etag = r.headers.get("etag") or r.headers.get("ETag")
        if r.status_code == 304:
            return 304, None, new_etag
        if 200 <= r.status_code < 300:
            try:
                return r.status_code, r.json(), new_etag
            except Exception:
                return r.status_code, None, new_etag
        return r.status_code, None, new_etag
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ❌ Athena rides error: {e}")
        return None, None, None


def _athena_assigned_driver_id(raw_ride: dict) -> str | None:
    """
    Athena JSON:API ride → relationships.assigned_driver.data.id (or assignedDriver).
    """
    if not isinstance(raw_ride, dict):
        return None
    rel = raw_ride.get("relationships") or {}
    node = (rel.get("assigned_driver") or rel.get("assignedDriver") or {}).get("data")
    if isinstance(node, dict):
        did = node.get("id")
        return str(did) if did else None
    return None


def _filter_rides_by_bl_uuid(raw_items: list, bl_uuid: str) -> list:
    """
    Works for both Athena rides (JSON:API) and Mobile /rides payloads.
    Keeps only rides whose assigned driver id == bl_uuid.
    - Athena: relationships.assigned_driver.data.id
    - Mobile: item['chauffeur']['id']
    """
    if not bl_uuid:
        return []

    filtered = []
    for it in (raw_items or []):
        # Athena JSON:API?
        if isinstance(it, dict) and "relationships" in it:
            did = _athena_assigned_driver_id(it)
            if did and str(did) == str(bl_uuid):
                filtered.append(it)
            continue

        # Mobile shape
        ch = (it or {}).get("chauffeur") or {}
        did = ch.get("id")
        if did and str(did) == str(bl_uuid):
            filtered.append(it)

    return filtered
