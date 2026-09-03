"""Print the as-of dates whose mart partitions are still refreshable.

A partition is not final the day it is built: the partial layer keeps absorbing
late-arriving events for `late_arrival_days`, so the marts for those days have
stale inputs until that window closes. Each run therefore rebuilds the recent
window as well as its own date.

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

Emitting the dates here, rather than computing them in the DAG, keeps the rule
and its rationale in one place and lets it be tested directly.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent


def refreshable_dates(feature_name: str, target: date, db: Path) -> list[date]:
    registry = json.loads((ROOT / "registry" / f"{feature_name}.json").read_text())
    late = int(registry["settings"]["late_arrival_days"])
    if late <= 0:
        return []

    state_model = registry["models"].get("alltime_state")
    oldest = target - timedelta(days=late)

    if state_model:
        con = duckdb.connect(str(db), read_only=True)
        try:
            row = con.execute(f"select max(_state_as_of_date) from intermediate.{state_model}")
            watermark = row.fetchone()[0]
        except duckdb.Error:
            # No accumulator yet: nothing has been sealed, so nothing is final.
            watermark = None
        finally:
            con.close()
        if watermark is not None:
            oldest = max(oldest, watermark)

    return [d for d in (target - timedelta(days=i) for i in range(1, late + 1)) if d >= oldest]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("feature_name")
    ap.add_argument("target_date")
    ap.add_argument("--db", type=Path, default=ROOT / "transform" / "warehouse.duckdb")
    ap.add_argument(
        "--include-target",
        action="store_true",
        help="also emit the target date itself (for the publish step)",
    )
    a = ap.parse_args()

    target = date.fromisoformat(a.target_date)
    dates = refreshable_dates(a.feature_name, target, a.db)
    if a.include_target:
        dates = [target, *dates]
    print(" ".join(d.isoformat() for d in dates))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
