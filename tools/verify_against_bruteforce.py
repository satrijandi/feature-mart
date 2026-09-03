"""Independent end-to-end verification of the feature mart.

The generated dbt tests check that the pipeline is self-consistent. They cannot
catch a systematic error -- an off-by-one window bound, a merge that drops the
unsealed tail, a days_since derivation with a flipped sign -- because every
layer would agree on the same wrong answer.

So this recomputes a representative sample of features the naive way: one flat
query straight over the raw source at the as-of date, with no partials, no
sealing, no incrementality. If the incremental machinery is right, the two must
agree exactly, for every entity.

WHICH "TRUTH" IS THE RIGHT ONE. There are two defensible readings of a feature
dated T, and they differ precisely on late-arriving events:

  as-of-knowledge  only rows already ingested by T. A frozen snapshot of what
                   was knowable that morning.
  as-of-event      all rows whose EVENT happened on or before T, whenever they
                   were ingested.

This pipeline implements as-of-event, deliberately: rewriting the last
`late_arrival_days` of partials on every run exists to fold a late event back
into the day it actually happened. A login on Monday is a Monday login even if
it reached the warehouse on Wednesday, and a model trained on the other reading
would learn the ingestion pipeline's quirks instead of customer behaviour.

So the comparison filters on event_date, bounded by what the pipeline could
possibly have seen.

That bound is min(T + late_arrival_days, newest as-of date built), which is the
exact contract a partition carries:

  * No run has looked past the newest as-of date built, so nothing ingested
    after that can have reached any layer.
  * A partition for T is REVISED by the runs at T+1 .. T+late_arrival_days, as
    late events settle into their own event_date, and is final afterwards.
    Nothing ingested after T+late_arrival_days is ever folded into it.

A partition inside its revision window is therefore provisional by design, and
comparing it against knowledge it has not been offered yet would be testing the
wrong contract.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

DB = Path("transform/warehouse.duckdb")
TARGET = sys.argv[1] if len(sys.argv) > 1 else "2026-09-03"
LATE_ARRIVAL_DAYS = 3  # settings.late_arrival_days in the feature spec

IOS = "upper(os_name) = 'IOS'"
ANDROID = "upper(os_name) = 'ANDROID'"
OTHERS = "upper(os_name) not in ('IOS', 'ANDROID')"
SUCCESS = "upper(event_status) = 'SUCCESS'"
FAILED = "upper(event_status) = 'FAILED'"
LATE_NIGHT = "extract(hour from event_timestamp) between 0 and 4"
OFFICE = "extract(hour from event_timestamp) between 9 and 17"


def w(days: int) -> str:
    """Inclusive window: [T - (days-1), T]."""
    return f"event_date >= date '{TARGET}' - {days - 1}"


# Every distinct code path in the generator gets at least one probe:
# marginal and combined conditions, the widest window (which omits its
# predicate), the state-merge path for all_time, exact set union, both
# extremum merges, and both directions of the days_since derivation.
PROBES: dict[str, str] = {
    "count_event_id_l7d": f"count(case when {w(7)} then event_id end)",
    "count_event_id_l30d": f"count(case when {w(30)} then event_id end)",
    "count_event_id_all_time": "count(event_id)",
    "count_event_id_is_ios_is_late_night_l7d": f"count(case when {w(7)} and {IOS} and {LATE_NIGHT} then event_id end)",
    "count_event_id_is_login_success_is_android_is_office_hours_l14d": f"count(case when {w(14)} and {SUCCESS} and {ANDROID} and {OFFICE} then event_id end)",
    "count_event_id_is_others_l30d": f"count(case when {w(30)} and {OTHERS} then event_id end)",
    "count_event_id_is_login_failed_all_time": f"count(case when {FAILED} then event_id end)",
    "count_distinct_device_id_l7d": f"count(distinct case when {w(7)} then device_id end)",
    "count_distinct_device_id_all_time": "count(distinct device_id)",
    "count_distinct_device_id_is_ios_l30d": f"count(distinct case when {w(30)} and {IOS} then device_id end)",
    "min_event_timestamp_all_time": "min(event_timestamp)",
    "max_event_timestamp_l7d": f"max(case when {w(7)} then event_timestamp end)",
    "max_event_timestamp_is_login_failed_all_time": f"max(case when {FAILED} then event_timestamp end)",
    "min_days_since_login_l7d": f"date_diff('day', cast(max(case when {w(7)} then event_timestamp end) as date),"
    f" date '{TARGET}')",
    "max_days_since_login_all_time": f"date_diff('day', cast(min(event_timestamp) as date), date '{TARGET}')",
    "min_days_since_login_is_ios_l30d": f"date_diff('day', cast(max(case when {w(30)} and {IOS} then event_timestamp end) as date),"
    f" date '{TARGET}')",
}


def main() -> int:
    con = duckdb.connect(str(DB), read_only=True)

    # The ingestion horizon this partition is contractually allowed to know
    # about: it stops being revised at T + late_arrival_days, and no run has
    # looked past the newest as-of date built.
    frontier = con.execute(
        "select max(target_date) from marts.fact_agg_features_login_history_v2"
    ).fetchone()[0]
    settled = date.fromisoformat(TARGET) + timedelta(days=LATE_ARRIVAL_DAYS)
    horizon = min(settled, frontier)

    # Deliberately flat: raw source, one filter, one GROUP BY. No reuse of
    # anything the pipeline builds.
    brute = f"""
        with src as (
            select
                customer_id as safe_id,
                device_id, event_id, event_timestamp, os_name, event_status,
                cast(event_timestamp as date) as event_date
            from bronze_backend_ddb.customer_journal_login
            where cast(event_timestamp as date) <= date '{TARGET}'   -- as-of-event
              and _scd_valid_from <= date '{horizon}'                -- ingestion horizon
              and date '{TARGET}' < _scd_valid_to
              and customer_id is not null
        )
        select safe_id, {", ".join(f"{sql} as {name}" for name, sql in PROBES.items())}
        from src group by safe_id
    """
    cols = ", ".join(PROBES)
    actual = f"""
        select safe_id, {cols}
        from marts.fact_agg_features_login_history_v2
        where target_date = date '{TARGET}'
    """

    comparisons = " or ".join(f"b.{n} is distinct from a.{n}" for n in PROBES)
    diff = con.execute(f"""
        with b as ({brute}), a as ({actual})
        select coalesce(b.safe_id, a.safe_id) as safe_id,
               {", ".join(f"b.{n} as exp_{n}, a.{n} as got_{n}" for n in PROBES)}
        from b full outer join a on b.safe_id = a.safe_id
        where {comparisons} or b.safe_id is null or a.safe_id is null
    """).fetchall()

    n_entities = con.execute(f"select count(*) from ({actual})").fetchone()[0]
    n_brute = con.execute(f"select count(*) from ({brute})").fetchone()[0]

    print(f"as-of date           : {TARGET}")
    print(
        f"ingestion horizon    : {horizon}"
        f"{'  (still inside its revision window)' if horizon < settled else '  (settled)'}"
    )
    print(f"entities in mart     : {n_entities}")
    print(f"entities brute-force : {n_brute}")
    print(f"features probed      : {len(PROBES)}")
    print(f"values compared      : {n_entities * len(PROBES)}")

    if diff:
        print(f"\nMISMATCH on {len(diff)} entities:")
        names = list(PROBES)
        for row in diff[:5]:
            print(f"  safe_id={row[0]}")
            for i, name in enumerate(names):
                exp, got = row[1 + 2 * i], row[2 + 2 * i]
                if exp != got:
                    print(f"    {name}: expected={exp!r} got={got!r}")
        return 1

    print("\nEXACT MATCH on every probed feature, for every entity.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
