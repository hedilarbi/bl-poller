import asyncio
import uuid
import time
from typing import Any, Dict, List, Optional

import httpx

from .config import (
    API_HOST,
    PARTNER_API_BASE,
    P1_RESERVE_TIMEOUT_S,
    P2_RESERVE_TIMEOUT_S,
    HTTP_POOL_SIZE,
    P1_STRIP_VOLATILE_HEADERS,
    P1_FORCE_FRESH_REQUEST_IDS,
)


def _has_header(headers: dict, name: str) -> bool:
    lname = name.lower()
    return any(str(k).lower() == lname for k in headers.keys())


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


def _merge_p1_headers(token: str, base_headers: Optional[dict] = None) -> dict:
    if base_headers:
        headers = {}
        for k, v in base_headers.items():
            if v is None:
                continue
            if P1_STRIP_VOLATILE_HEADERS and _is_volatile_header(k):
                continue
            headers[k] = v
        if not _has_header(headers, "Host"):
            headers["Host"] = API_HOST.replace("https://", "")
        if not _has_header(headers, "Accept"):
            headers["Accept"] = "*/*"
        if not _has_header(headers, "Accept-Language"):
            headers["Accept-Language"] = "en-CA,en-US;q=0.9,en;q=0.8"
        if not _has_header(headers, "Accept-Encoding"):
            headers["Accept-Encoding"] = "gzip, deflate, br"
        if not _has_header(headers, "Content-Type"):
            headers["Content-Type"] = "application/json"
        if not _has_header(headers, "X-Operating-System"):
            headers["X-Operating-System"] = "iOS"
        if not _has_header(headers, "User-Agent"):
            headers["User-Agent"] = "Chauffeur/18575 CFNetwork/3860.300.31 Darwin/25.2.0"
        if not _has_header(headers, "Connection"):
            headers["Connection"] = "keep-alive"
    else:
        headers = {
            "Host": API_HOST.replace("https://", ""),
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
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


def _build_p2_headers(access_token: str, bl_user_id: Optional[str], roles: Optional[str] = None) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://partner.blacklane.com",
        "Referer": "https://partner.blacklane.com/",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
        "X-User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36"
        ),
        "Blacklane-User-Id": str(bl_user_id or ""),
        "Blacklane-User-Roles": roles or "dispatcher,driver,provider,admin,reviewer",
        "X-Datadog-Origin": "rum",
        "X-Datadog-Sampling-Priority": "1",
    }


async def _reserve_p1_one(client: httpx.AsyncClient, task: dict) -> dict:
    offer_id = task.get("offer_id")
    task_key = task.get("task_key")
    token = task.get("token")
    price = task.get("price")
    headers = _merge_p1_headers(str(token or ""), task.get("headers"))
    payload: Dict[str, Any] = {"id": str(offer_id), "action": "accept"}
    if price is not None:
        try:
            payload["price"] = float(price)
        except Exception:
            payload["price"] = price
    t0 = time.perf_counter()
    try:
        r = await client.post(
            f"{API_HOST}/offers",
            headers=headers,
            json=payload,
            timeout=float(P1_RESERVE_TIMEOUT_S),
        )
        try:
            body = r.json()
        except Exception:
            body = r.text
        return {
            "task_key": task_key,
            "offer_id": offer_id,
            "status_code": r.status_code,
            "body": body,
            "latency_ms": (time.perf_counter() - t0) * 1000.0,
        }
    except Exception as e:
        return {
            "task_key": task_key,
            "offer_id": offer_id,
            "status_code": None,
            "body": {"error": f"{type(e).__name__}: {e}"},
            "latency_ms": (time.perf_counter() - t0) * 1000.0,
        }


async def _reserve_p2_one(client: httpx.AsyncClient, task: dict) -> dict:
    offer_id = task.get("offer_id")
    task_key = task.get("task_key")
    token = task.get("token")
    price = task.get("price")
    headers = _build_p2_headers(str(token or ""), task.get("bl_user_id"))
    payload = {"action": "accept", "id": str(offer_id), "price": float(price)}
    t0 = time.perf_counter()
    try:
        r = await client.post(
            f"{PARTNER_API_BASE}/chauffeur/offers",
            headers=headers,
            json=payload,
            timeout=float(P2_RESERVE_TIMEOUT_S),
        )
        try:
            body = r.json()
        except Exception:
            body = r.text
        return {
            "task_key": task_key,
            "offer_id": offer_id,
            "status_code": r.status_code,
            "body": body,
            "latency_ms": (time.perf_counter() - t0) * 1000.0,
        }
    except Exception as e:
        return {
            "task_key": task_key,
            "offer_id": offer_id,
            "status_code": None,
            "body": {"error": f"{type(e).__name__}: {e}"},
            "latency_ms": (time.perf_counter() - t0) * 1000.0,
        }


async def _reserve_batch_async(tasks: List[dict]) -> List[dict]:
    limits = httpx.Limits(
        max_connections=max(8, HTTP_POOL_SIZE),
        max_keepalive_connections=max(8, HTTP_POOL_SIZE),
    )
    async with httpx.AsyncClient(http2=True, verify=True, limits=limits, trust_env=False) as client:
        coros = []
        for t in tasks:
            plat = (t.get("platform") or "").lower()
            if plat == "p1":
                coros.append(_reserve_p1_one(client, t))
            elif plat == "p2":
                coros.append(_reserve_p2_one(client, t))
            else:
                coros.append(
                    asyncio.sleep(
                        0,
                        result={
                            "task_key": t.get("task_key"),
                            "offer_id": t.get("offer_id"),
                            "status_code": None,
                            "body": {"error": "unknown_platform"},
                            "latency_ms": 0.0,
                        },
                    )
                )
        return await asyncio.gather(*coros)


def reserve_batch(tasks: List[dict]) -> List[dict]:
    if not tasks:
        return []
    return asyncio.run(_reserve_batch_async(tasks))
