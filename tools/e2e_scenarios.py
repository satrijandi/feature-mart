"""End-to-end operational scenarios for the incremental feature store.

Correct output on a clean, in-order daily sequence is the easy case. These are
the cases that decide whether the pipeline can be scheduled unattended, and each
one corresponds to a bug this pipeline previously had:

  RETRY      A task retries after partially succeeding. Re-running the same
             as-of date must leave the warehouse byte-identical, or the
             all_time accumulator double-counts and silently corrupts history.

  GAP        A day is missed (outage, upstream delay). The next run must absorb
             it unaided. If it cannot, every all_time feature is permanently
             short by a day's events and nothing alerts.

  LATE       An event arrives after its own event_date. It must land in the day
             it happened, not the day it was noticed.

  BACKWARDS  A past as-of date is replayed after a later one has run. Each run
             sees the source as of its OWN date, so recomputing a completed day
             under an earlier, narrower view would drop events a later run had
             correctly captured. Stored partials must only ever gain
             information.

  REVISION   The last `late_arrival_days` partitions are rebuilt on every run,
             because their partial inputs are still absorbing late events. Those
             rebuilds must come out exact, and must not trip the watermark guard
             that protects all_time -- an in-order run leaves the watermark at
             T - late_arrival_days, so every date in the window is at or after
             it.

  UNORDERED  Serving a date that sits BEHIND the accumulator's watermark is the
             one case nothing can repair: the sealed fold already holds events
             that date must not see. It must fail the build rather than publish.

  DORMANCY   Under entity_spine: active_window the mart stops publishing an
             entity once it falls outside the widest bounded window. The
             accumulator must NOT forget it -- if going quiet truncated an
             entity's history, its all_time features would silently reset when
             it came back, which is the failure this setting could plausibly
             introduce.

Each scenario is checked against an independent brute-force recomputation, so
"passed" means the numbers are right, not merely that dbt exited zero. Every
date is derived from the warehouse's current frontier, so the scenarios test the
property rather than a state left behind by an earlier session.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
TRANSFORM = ROOT / "transform"
DB = TRANSFORM / "warehouse.duckdb"
MART = "marts.fact_agg_features_login_history_v2"
STATE = "intermediate.int_fact_agg_features_login_history_v2__alltime_state"
PARTIALS_MODEL = "int_fact_agg_features_login_history_v2__daily_partials"
PARTIALS_TABLE = f"intermediate.{PARTIALS_MODEL}"


def dbt(*args: str, target_date: str) -> None:
    cmd = [str(ROOT / ".venv/bin/dbt"), *args, "--vars", f"{{target_date: {target_date}}}", "-q"]
    res = subprocess.run(
        cmd,
        cwd=TRANSFORM,
        capture_output=True,
        text=True,
        env={**os.environ, "DBT_PROFILES_DIR": str(TRANSFORM)},
    )
    if res.returncode != 0:
        print(res.stdout[-3000:], res.stderr[-2000:])
        raise SystemExit(f"dbt {' '.join(args)} failed for {target_date}")


def query(sql: str):
    con = duckdb.connect(str(DB), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def frontier() -> str:
    """Newest as-of date the mart holds."""
    return str(query(f"select max(target_date) from {MART}")[0][0])


def last_event_date() -> str:
    """Newest event_date in the source.

    Beyond this the fixture has no events, so a fold has nothing to consume and
    the watermark legitimately stops advancing -- correct behaviour, but it
    makes anything that measures fold progress unobservable. Scenarios that
    exercise the accumulator anchor here rather than at the mart frontier.
    """
    return str(
        query(
            "select max(cast(event_timestamp as date)) "
            "from bronze_backend_ddb.customer_journal_login"
        )[0][0]
    )


def shift(day: str, days: int) -> str:
    return (date.fromisoformat(day) + timedelta(days=days)).isoformat()


def fingerprint(day: str) -> tuple[int, str]:
    """Row count plus an order-independent checksum of one whole day partition."""
    cols = [
        r[0]
        for r in query(
            "select column_name from information_schema.columns "
            "where table_schema = 'marts' "
            "and table_name = 'fact_agg_features_login_history_v2' "
            "and column_name <> '_generated_at'"
        )
    ]
    concat = " || '|' || ".join(f"coalesce(cast({c} as varchar), '~')" for c in cols)
    row = query(
        f"select count(*), coalesce(sum(hash({concat})), 0)::varchar "
        f"from {MART} where target_date = date '{day}'"
    )[0]
    return int(row[0]), row[1]


def state_watermark() -> str:
    return str(query(f"select max(_state_as_of_date) from {STATE}")[0][0])


def partial_totals() -> dict:
    return dict(query(f"select event_date, sum(p_count_event_id) from {PARTIALS_TABLE} group by 1"))


def brute_force_matches(day: str) -> bool:
    res = subprocess.run(
        [str(ROOT / ".venv/bin/python"), str(ROOT / "tools/verify_against_bruteforce.py"), day],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        print(res.stdout)
    return res.returncode == 0


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}" + (f"  {detail}" if detail else ""))
    return ok


def main() -> int:
    passed = True

    # --- 1. retry -----------------------------------------------------------
    # Idempotency is f(f(x)) = f(x), so the fixed point has to be reached before
    # it can be tested. One run establishes it; the second is the measurement.
    # Asserting on whatever state an earlier session happened to leave behind
    # would test convergence, not idempotency.
    day = frontier()
    print(f"\n=== SCENARIO 1: retrying the newest as-of date ({day}) changes nothing ===")
    dbt("run", target_date=day)
    before, wm_before = fingerprint(day), state_watermark()
    dbt("run", target_date=day)
    after, wm_after = fingerprint(day), state_watermark()
    passed &= check(
        "mart partition byte-identical after the re-run",
        before == after,
        f"{before[0]} rows, checksum {before[1][:12]}",
    )
    passed &= check(
        "all_time watermark did not move", wm_before == wm_after, f"watermark={wm_after}"
    )
    passed &= check("still matches brute force", brute_force_matches(day))

    # --- 2. gap -------------------------------------------------------------
    # Anchored inside the data: the accumulator seals at T - late_arrival_days,
    # so to observe it crossing a skipped day the range it consumes has to
    # contain events. Reset it to a date far enough back that it does.
    late = 3
    last_event = last_event_date()
    base = shift(last_event, -(late + 3))
    skipped, jump = shift(base, 1), shift(base, 3)
    print("\n=== SCENARIO 2: a missed day is absorbed by the next run ===")
    print(f"  (accumulator reset to {base}; skipping {skipped}; then running {jump})")
    dbt(
        "run",
        "--full-refresh",
        "--select",
        "int_fact_agg_features_login_history_v2__alltime_state",
        target_date=base,
    )
    wm_before = state_watermark()
    dbt("run", target_date=jump)
    wm_after = state_watermark()
    passed &= check(
        "watermark advanced across the skipped day, unaided",
        wm_after > wm_before,
        f"{wm_before} -> {wm_after}",
    )
    passed &= check(
        f"the skipped day's events are inside the advance ({skipped} crossed)",
        wm_before < shift(skipped, -late) <= wm_after or wm_after >= shift(jump, -late),
        f"sealed through {wm_after}",
    )
    passed &= check(f"all_time still exact despite the gap (at {jump})", brute_force_matches(jump))

    # --- 3. late arrivals ---------------------------------------------------
    print("\n=== SCENARIO 3: late-arriving events land on their own event_date ===")
    horizon = frontier()
    late = query(
        "select count(*) from bronze_backend_ddb.customer_journal_login "
        "where cast(_scd_valid_from as date) > cast(event_timestamp as date)"
    )[0][0]
    # Had the pipeline bucketed by ingestion instead, the stored per-day totals
    # would not reconcile with a straight group-by of the raw source.
    mismatch = query(f"""
        with raw as (
            select cast(event_timestamp as date) as event_date, count(*) as n
            from bronze_backend_ddb.customer_journal_login
            where _scd_valid_from <= date '{horizon}'
              and customer_id is not null
              and cast(event_timestamp as date) <= date '{horizon}'
            group by 1
        ),
        stored as (
            select event_date, sum(p_count_event_id) as n from {PARTIALS_TABLE} group by 1
        )
        select count(*) from raw full outer join stored using (event_date)
        where raw.n is distinct from stored.n
    """)[0][0]
    passed &= check(
        f"all {late} late-arriving events bucketed by event_date, not ingest date",
        mismatch == 0,
        f"{mismatch} day(s) disagree with the raw source",
    )

    # --- 4. backwards replay ------------------------------------------------
    print("\n=== SCENARIO 4: replaying a past as-of date never loses events ===")
    replay = shift(frontier(), -12)
    print(f"  (replaying {replay}, well behind the frontier)")
    before_totals = partial_totals()
    dbt("run", "--select", PARTIALS_MODEL, target_date=replay)
    after_totals = partial_totals()

    lost = {
        d: (before_totals[d], after_totals.get(d, 0))
        for d in before_totals
        if after_totals.get(d, 0) < before_totals[d]
    }
    passed &= check(
        "no event_date lost rows to the backwards replay",
        not lost,
        f"{len(lost)} day(s) regressed" if lost else f"{len(before_totals)} days intact",
    )
    for d, (b, a) in list(lost.items())[:5]:
        print(f"      {d}: {b} -> {a}")
    passed &= check("mart still matches brute force afterwards", brute_force_matches(frontier()))

    # --- 5. revision window -------------------------------------------------
    print("\n=== SCENARIO 5: the revision window rebuilds past partitions exactly ===")
    late = 3
    day = frontier()
    window = [shift(day, -i) for i in range(1, late + 1)]
    print(f"  (rebuilding {', '.join(window)} behind the frontier {day})")
    ok_all = True
    for d in window:
        res = subprocess.run(
            [
                str(ROOT / ".venv/bin/dbt"),
                "build",
                "--select",
                "tag:fact_agg_features_login_history_v2",
                "--vars",
                f"{{target_date: {d}}}",
                "-q",
            ],
            cwd=TRANSFORM,
            capture_output=True,
            text=True,
            env={**os.environ, "DBT_PROFILES_DIR": str(TRANSFORM)},
        )
        built = res.returncode == 0
        exact = brute_force_matches(d) if built else False
        ok_all &= built and exact
        print(
            f"    {d}: {'built' if built else 'BUILD FAILED'}, {'exact' if exact else 'NOT EXACT'}"
        )
    passed &= check("every revision-window rebuild built and verified exact", ok_all)

    leaking = query(f"""
        select count(*) from {MART} where _last_event_date > target_date
    """)[0][0]
    passed &= check(
        "no partition references events after its own as-of date",
        leaking == 0,
        f"{leaking} leaking row(s)",
    )

    # --- 6. unordered run is refused ----------------------------------------
    print("\n=== SCENARIO 6: a date behind the watermark fails the build ===")
    wm = state_watermark()
    behind = shift(wm, -1)
    print(f"  (watermark is {wm}; attempting to serve {behind})")
    res = subprocess.run(
        [
            str(ROOT / ".venv/bin/dbt"),
            "test",
            "--select",
            "assert_fact_agg_features_login_history_v2_state_watermark",
            "--vars",
            f"{{target_date: {behind}}}",
        ],
        cwd=TRANSFORM,
        capture_output=True,
        text=True,
        env={**os.environ, "DBT_PROFILES_DIR": str(TRANSFORM)},
    )
    passed &= check(
        "watermark guard refuses a date behind the sealed state",
        res.returncode != 0,
        "guard fired" if res.returncode != 0 else "guard did NOT fire",
    )
    res = subprocess.run(
        [
            str(ROOT / ".venv/bin/dbt"),
            "test",
            "--select",
            "assert_fact_agg_features_login_history_v2_state_watermark",
            "--vars",
            f"{{target_date: {shift(wm, 1)}}}",
        ],
        cwd=TRANSFORM,
        capture_output=True,
        text=True,
        env={**os.environ, "DBT_PROFILES_DIR": str(TRANSFORM)},
    )
    passed &= check(
        "and permits a date at or after it",
        res.returncode == 0,
        f"served {shift(wm, 1)}",
    )

    # --- 7. each spec's spine contract, whatever it is set to ---------------
    # The two specs read the same source but run different spines, so this
    # asserts each one honours its own setting rather than assuming they agree.
    # The property that matters under either is the same: the spine decides who
    # gets published, and must never decide who the accumulator remembers.
    print("\n=== SCENARIO 7: each spec honours its own entity_spine ===")

    for reg_path in sorted((ROOT / "registry").glob("*.json")):
        registry = json.loads(reg_path.read_text())
        name = registry["feature_name"]
        spine = registry["settings"]["entity_spine"]
        mart = f"marts.{name}"
        state = f"intermediate.{registry['models']['alltime_state']}"
        count_col = next(
            f["name"]
            for f in registry["features"]
            if f["agg"] == "count" and f["window"] == "all_time" and f["is_marginal"]
        )
        print(f"\n  {name}  [{spine}]")

        lo, hi = query(f"select min(target_date), max(target_date) from {mart}")[0]
        dropped = query(f"""
            select e.safe_id, e.{count_col}
            from {mart} e
            left join {mart} l on l.safe_id = e.safe_id and l.target_date = date '{hi}'
            where e.target_date = date '{lo}' and l.safe_id is null
        """)

        if spine == "all_time":
            passed &= check(
                "no entity is ever dropped once published",
                not dropped,
                f"{len(dropped)} dropped between {lo} and {hi}",
            )
            # Every entity the accumulator knows should be published.
            unpublished = query(f"""
                select count(*) from {state} a
                where not exists (
                    select 1 from {mart} m
                    where m.safe_id = a.safe_id and m.target_date = date '{hi}'
                )
                  and a._min_event_date <= date '{hi}'
            """)[0][0]
            passed &= check(
                "every entity the accumulator holds is published",
                unpublished == 0,
                f"{unpublished} held but unpublished",
            )
        else:
            widest = max(f["window_days"] for f in registry["features"] if f["window_days"])
            passed &= check(
                "the narrowed spine actually drops dormant entities",
                bool(dropped),
                f"{len(dropped)} stopped being published",
            )
            # The risk this setting introduces: dropping out of the mart must
            # not shrink the accumulator, or all_time would reset on return.
            shortfalls = []
            for safe_id, last_published in dropped:
                held = query(f"select p_count_event_id from {state} where safe_id = '{safe_id}'")
                if not held or held[0][0] < last_published:
                    shortfalls.append((safe_id, last_published, held[0][0] if held else None))
            passed &= check(
                "every dropped entity's history is intact in the accumulator",
                not shortfalls,
                f"{len(dropped)} checked, {len(shortfalls)} truncated",
            )
            for sid, was, now in shortfalls[:5]:
                print(f"        {sid}: published {was}, accumulator holds {now}")
            stale = query(f"""
                select count(*) from {mart}
                where target_date = date '{hi}'
                  and _last_event_date < date '{hi}' - {widest - 1}
            """)[0][0]
            passed &= check(
                f"no published row is older than the {widest}-day window",
                stale == 0,
                f"{stale} stale row(s)",
            )

    print("\n" + ("ALL SCENARIOS PASSED" if passed else "SCENARIOS FAILED"))
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
