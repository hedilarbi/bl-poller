"""
Exact capacity / driver-matching conflict check for a multi-chauffeur fleet.

A new offer fits when every committed ride that interacts with it — rides assigned to a
chosen chauffeur (pinned to that driver) plus unassigned rides and the bot's own pending
acceptances (floating, can go to any chosen driver) — can still be scheduled on the pool of
chosen live chauffeurs together with the new ride, without any driver doing two overlapping
rides. This is an exact feasibility test (interval scheduling on K machines with some jobs
pre-pinned), not a concurrency count: a simple "overlaps < K" count is wrong when pinned
rides force a specific driver (e.g. A busy 10–11 and B busy 11–12 leave nobody free for a
single 10–12 ride even though max concurrency is 1).

Feasibility decomposes over connected components of the interval-overlap graph: two rides
that never overlap can always share a driver, so only the component containing the new ride
constrains it. We solve that component by backtracking (pinned jobs first, then floating),
which is exact and instant at this scale (a handful of rides, a few drivers).
"""
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from .timeparse import parse_iso_dt_or_none

# Same conservative window as filters._find_conflict for rides whose end time is unknown.
_UNKNOWN_END_HOURS = 4

Interval = Tuple[datetime, datetime]
Job = Tuple[datetime, datetime, Optional[str]]  # (start, end, pinned_driver_or_None)


def _norm_end(start: datetime, end) -> datetime:
    return end if isinstance(end, datetime) else (start + timedelta(hours=_UNKNOWN_END_HOURS))


def _overlap(s1: datetime, e1: datetime, s2: datetime, e2: datetime) -> bool:
    """True when the two intervals share time. Touching ends (e1 == s2) do NOT overlap."""
    return s1 < e2 and s2 < e1


def _feasible(jobs: List[Job], machines: List[str]) -> bool:
    """Can every job be placed on a machine (pinned jobs on their own) with no overlap on
    the same machine? Exact backtracking; pinned jobs first (most constrained)."""
    busy: dict = {m: [] for m in machines}
    order = sorted(range(len(jobs)), key=lambda i: (0 if jobs[i][2] is not None else 1, jobs[i][0]))

    def bt(k: int) -> bool:
        if k == len(order):
            return True
        s, e, pin = jobs[order[k]]
        candidates = [pin] if pin is not None else machines
        for m in candidates:
            lst = busy.get(m)
            if lst is None:
                # Pinned to a machine outside the pool — caller should have floated it.
                continue
            if all(not _overlap(s, e, bs, be) for (bs, be) in lst):
                lst.append((s, e))
                if bt(k + 1):
                    return True
                lst.pop()
        return False

    return bt(0)


def capacity_conflict(
    new_start: datetime,
    new_end_iso: Optional[str],
    busy_tagged: List[Tuple[datetime, Optional[datetime], Optional[str]]],
    pool: List[str],
) -> Optional[Interval]:
    """Decide whether a new offer can be taken by the fleet.

    new_start      : pickup time of the candidate offer.
    new_end_iso    : its computed end time (ISO string) or None (→ safety window).
    busy_tagged    : committed rides as (start, end, driver). driver is a chosen driver id
                     (pinned) or None/"unassigned"/"me" (floating). Ends may be None.
    pool           : chosen chauffeurs that are currently live (the K machines).

    Returns a representative conflicting (start, end) interval when the offer does NOT fit,
    or None when it fits. Returning an interval (not just a bool) lets the caller reuse the
    existing conflict-reason message.
    """
    if not pool:
        return None  # no capacity info — caller falls back to the legacy single-slot check

    pool_set = set(pool)
    new_end = _norm_end(new_start, parse_iso_dt_or_none(new_end_iso) if new_end_iso else None)

    jobs: List[Job] = []
    for (s, e, drv) in busy_tagged:
        if s is None:
            continue
        e2 = _norm_end(s, e)
        # A ride assigned to a chosen & live driver is pinned; anything else (unassigned,
        # the bot's own accepts, or a driver no longer live) floats onto any chosen driver.
        pin = drv if (drv in pool_set) else None
        jobs.append((s, e2, pin))
    new_index = len(jobs)
    jobs.append((new_start, new_end, None))

    # Connected component (interval-overlap graph) containing the new ride.
    n = len(jobs)
    comp = {new_index}
    stack = [new_index]
    while stack:
        i = stack.pop()
        si, ei, _ = jobs[i]
        for j in range(n):
            if j in comp:
                continue
            sj, ej, _ = jobs[j]
            if _overlap(si, ei, sj, ej):
                comp.add(j)
                stack.append(j)

    sub = [jobs[i] for i in sorted(comp)]
    if _feasible(sub, list(pool)):
        return None

    # Infeasible → return a committed ride overlapping the new one, for the reason message.
    for i in sorted(comp):
        if i == new_index:
            continue
        s, e, _ = jobs[i]
        if _overlap(new_start, new_end, s, e):
            return (s, e)
    return (new_start, new_end)
