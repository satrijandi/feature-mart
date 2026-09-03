"""Measure the KMV sketch against exact ground truth.

`distinct_method: approx` trades exactness for bounded state. That trade is
only acceptable if the error is actually bounded, so this measures it rather
than asserting it: every approximate feature is compared to a straight
count(distinct ...) over the raw source.

Ground truth uses the same as-of-EVENT semantics and ingestion horizon as
tools/verify_against_bruteforce.py -- see that file for why. Filtering on
ingestion instead would compare the sketch against knowledge the pipeline was
never offered and report the difference as sketch error.

Two properties are checked.
  1. Below k distinct values the sketch retains every hash, so the estimate
     must be EXACT. A pipeline that is approximate for small counts is simply
     buggy -- and small counts are the common case.
  2. At or above k, relative error must sit inside the theoretical envelope
     for a k-minimum-values sketch, ~1/sqrt(k).
"""

from __future__ import annotations

import math
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

DB = Path("transform/warehouse.duckdb")
TARGET = sys.argv[1] if len(sys.argv) > 1 else "2026-09-03"
K = 64  # settings.kmv_k in features/fact_agg_features_login_device_v1.yml
LATE_ARRIVAL_DAYS = 3


def main() -> int:
    con = duckdb.connect(str(DB), read_only=True)

    frontier = con.execute(
        "select max(target_date) from marts.fact_agg_features_login_device_v1"
    ).fetchone()[0]
    settled = date.fromisoformat(TARGET) + timedelta(days=LATE_ARRIVAL_DAYS)
    horizon = min(settled, frontier)

    rows = con.execute(f"""
        with src as (
            select customer_id as safe_id, event_id, event_status,
                   cast(event_timestamp as date) as event_date
            from bronze_events.customer_login
            where cast(event_timestamp as date) <= date '{TARGET}'
              and _scd_valid_from <= date '{horizon}'
              and date '{TARGET}' < _scd_valid_to
              and customer_id is not null
        ),
        exact as (
            select safe_id,
                count(distinct case when event_date >= date '{TARGET}' - 6
                                    then event_id end)  as l7d,
                count(distinct case when event_date >= date '{TARGET}' - 29
                                    then event_id end)  as l30d,
                count(distinct event_id)                as all_time
            from src group by safe_id
        ),
        approx as (
            select safe_id,
                   count_distinct_event_id_l7d      as l7d,
                   count_distinct_event_id_l30d     as l30d,
                   count_distinct_event_id_all_time as all_time
            from marts.fact_agg_features_login_device_v1
            where target_date = date '{TARGET}'
        )
        select e.safe_id, w.win, w.exact_v, w.approx_v
        from exact e join approx a using (safe_id)
        cross join lateral (values
            ('l7d', e.l7d, a.l7d), ('l30d', e.l30d, a.l30d),
            ('all_time', e.all_time, a.all_time)
        ) as w(win, exact_v, approx_v)
    """).fetchall()
    con.close()

    below = [(s, w, e, a) for s, w, e, a in rows if e < K]
    at_or_above = [(s, w, e, a) for s, w, e, a in rows if e >= K]

    exact_failures = [r for r in below if r[2] != r[3]]
    errors = [abs(a - e) / e for _, _, e, a in at_or_above] if at_or_above else []

    bound = 1.0 / math.sqrt(K)
    print(f"as-of date              : {TARGET}")
    print(f"ingestion horizon       : {horizon}")
    print(f"sketch k                : {K}   (theoretical std error ~{bound:.1%})")
    print(f"comparisons             : {len(rows)}")
    print(f"  below k (must be exact): {len(below)}")
    print(f"  at/above k (estimated) : {len(at_or_above)}")

    ok = True
    if exact_failures:
        ok = False
        print(f"\nFAIL: {len(exact_failures)} sketches under k were not exact")
        for s, w, e, a in exact_failures[:5]:
            print(f"    {s} {w}: exact={e} approx={a}")
    else:
        print("\n  [PASS] every sketch holding fewer than k values is EXACT")

    if errors:
        mean_err = sum(errors) / len(errors)
        max_err = max(errors)
        p95 = sorted(errors)[int(0.95 * (len(errors) - 1))]
        print(f"  [....] relative error  mean {mean_err:.2%}  p95 {p95:.2%}  max {max_err:.2%}")
        # A KMV estimator is unbiased with std error ~1/sqrt(k); allow 3 sigma
        # on the worst single observation before calling it broken.
        if max_err > 3 * bound:
            ok = False
            print(f"  [FAIL] max error {max_err:.2%} exceeds 3 sigma ({3 * bound:.1%})")
        else:
            print(f"  [PASS] max error within 3 sigma ({3 * bound:.1%})")
    else:
        print("  [WARN] no entity reached k distinct values; estimator path not exercised")

    print("\n" + ("KMV VERIFIED" if ok else "KMV FAILED"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
