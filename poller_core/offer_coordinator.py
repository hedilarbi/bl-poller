"""
Cross-user offer coordination.

Rules:
- If user A reserves an offer that also passed filters for users B and C:
    → send "accepted" to A, send "not_accepted" to B and C
- If no one in our system reserves the offer:
    → send nothing to anyone

offer_key = f"{platform}:{offer_id}"
"""

import threading
import time
from typing import Dict, List, Optional

_lock = threading.Lock()
_TTL_S = 180.0
_last_cleanup_ts: float = 0.0    # rate-limits _cleanup_locked() to once per 10s
_CLEANUP_INTERVAL_S: float = 10.0

# offer_key -> {"candidates": {user_key: candidate_dict}, "ts": float}
_pending: Dict[str, dict] = {}

# offer_keys recently claimed (with timestamp) — lets peers detect a winner
_claimed: Dict[str, float] = {}


def _cleanup_locked() -> None:
    now = time.time()
    stale = [k for k, v in _pending.items() if now - v["ts"] > _TTL_S]
    for k in stale:
        del _pending[k]
    stale_c = [k for k, ts in _claimed.items() if now - ts > _TTL_S]
    for k in stale_c:
        del _claimed[k]


def register_candidate(
    offer_key: str,
    bot_id: str,
    telegram_id: int,
    candidate_data: dict,
) -> None:
    """
    Register intent to reserve this offer (called after filter passes, before reserve attempt).
    candidate_data must contain: bot_id, telegram_id, offer, tz_name,
                                  filter_results, platform, forced_accept.
    """
    global _last_cleanup_ts
    user_key = f"{bot_id}:{telegram_id}"
    with _lock:
        now = time.time()
        if now - _last_cleanup_ts >= _CLEANUP_INTERVAL_S:
            _cleanup_locked()
            _last_cleanup_ts = now
        if offer_key not in _pending:
            _pending[offer_key] = {"candidates": {}, "ts": now}
        _pending[offer_key]["candidates"][user_key] = candidate_data


def claim_offer(offer_key: str, winner_bot_id: str, winner_telegram_id: int) -> List[dict]:
    """
    Mark offer as claimed by this user (called on successful reserve).
    Returns list of other candidate dicts that should receive a 'not_accepted' notification.
    """
    winner_key = f"{winner_bot_id}:{winner_telegram_id}"
    with _lock:
        entry = _pending.pop(offer_key, None)
        _claimed[offer_key] = time.time()
    if entry is None:
        return []
    return [v for k, v in entry["candidates"].items() if k != winner_key]


def is_claimed_by_peer(offer_key: str) -> bool:
    """Returns True if another one of our users already claimed this offer."""
    with _lock:
        return offer_key in _claimed


def remove_candidate(offer_key: str, bot_id: str, telegram_id: int) -> None:
    """Remove a candidate (e.g. if reserve was not attempted due to missing token)."""
    user_key = f"{bot_id}:{telegram_id}"
    with _lock:
        entry = _pending.get(offer_key)
        if entry:
            entry["candidates"].pop(user_key, None)
