"""The refreshable-window rule.

A mart partition is not final the day it is built. The partial layer keeps
absorbing late-arriving events for `late_arrival_days`, so the marts for those
days have stale inputs until that window closes. Each run therefore rebuilds
the recent window as well as its own date.

But the window is not simply `T-1 .. T-late_arrival_days`. It is bounded below
by the all_time accumulator's watermark W, and that bound is a consequence of
how sealing works rather than a safety margin:

    d >= W    the accumulator has not yet sealed past this date, so its
              all_time state is still valid for d and the partition is
              genuinely provisional -- rebuild it.

    d <  W    the accumulator sealed past d already, which means d's own
              late-arrival window closed before the seal. The partition is
              FINAL. Rebuilding it now would fold sealed state containing
              events after d into d's all_time features, i.e. leak the future.

So the refreshable window is [max(W, T - late_arrival_days), T - 1]. The two
bounds coincide on an ordinary in-order run, where W = T - late_arrival_days;
they diverge after a replay or a run that had nothing to fold, and this is what
keeps that case from turning into either lost revisions or leaked features.

The rule lives in the generator rather than in an operational script because it
is a property of what the generated models mean, not of how any one deployment
runs them. Resolving `late_arrival_days` from a registry and W from a warehouse
is the caller's job; see ../showcase/tools/revision_window.py.
"""

from __future__ import annotations

from datetime import date, timedelta


def refreshable_dates(
    target: date,
    late_arrival_days: int,
    watermark: date | None = None,
) -> list[date]:
    """As-of dates strictly before `target` whose partitions may still be rebuilt.

    `watermark` is the all_time accumulator's `_state_as_of_date`, or None when
    no accumulator exists yet -- a first run has sealed nothing, so nothing is
    final. Dates are returned newest first, which is the order a refresh should
    walk them in: the newest partition is the one most likely to be read next.
    """
    if late_arrival_days <= 0:
        return []

    oldest = target - timedelta(days=late_arrival_days)
    if watermark is not None:
        oldest = max(oldest, watermark)

    return [
        d
        for d in (target - timedelta(days=i) for i in range(1, late_arrival_days + 1))
        if d >= oldest
    ]
