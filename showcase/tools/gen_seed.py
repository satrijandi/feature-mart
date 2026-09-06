"""Generate the deterministic login-event fixture the local stack runs on.

The shape matters more than the volume. The fixture deliberately contains the
cases that break naive feature pipelines:

  * dormant entities        events long ago and none recently, so all_time must
                            stay populated while l7d decays to zero
  * single-event entities    min == max, every window identical
  * always-on entities       every window non-trivial
  * a device-churner         high distinct cardinality, to stress exact sets
  * NULL device_id           count and count_distinct must disagree correctly
  * os_name outside the enum so is_others is genuinely exercised
  * every hour of the day    so all four behavioural_time buckets fire
  * late-arriving rows       ingested after the event date, inside the
                            late-arrival window, to exercise seal-and-tail

The fixture is loaded straight into the warehouse as `bronze_events.customer_login`
rather than through `dbt seed`. The dbt project in the repository root is generated
output that knows nothing about any particular source data, and a seed block
naming this fixture would put that knowledge back into it. In production the
source table is written by an upstream pipeline; here it is written by this
script, and dbt sees the same thing in both cases.
"""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta

import duckdb

from tools.paths import DB, SEEDS

SEED = 20260903
START = date(2026, 7, 1)
END = date(2026, 9, 5)
OUT = SEEDS / "customer_login.csv"
SCHEMA = "bronze_events"
TABLE = "customer_login"

# Declared rather than sniffed. The timestamp columns drive both the event date
# and the SCD validity window, and a column silently inferred as VARCHAR would
# make every date comparison in the pipeline a string comparison.
COLUMN_TYPES = {
    "event_id": "VARCHAR",
    "customer_id": "VARCHAR",
    "device_id": "VARCHAR",
    "event_timestamp": "TIMESTAMP",
    "login_source": "VARCHAR",
    "os_name": "VARCHAR",
    "event_status": "VARCHAR",
    "_scd_valid_from": "TIMESTAMP",
    "_scd_valid_to": "TIMESTAMP",
}


def load(csv_path, db) -> int:
    """Replace the source table with the fixture, as an upstream load would."""
    types = ", ".join(f"'{k}': '{v}'" for k, v in COLUMN_TYPES.items())
    con = duckdb.connect(str(db))
    try:
        con.execute(f"create schema if not exists {SCHEMA}")
        con.execute(
            f"create or replace table {SCHEMA}.{TABLE} as "
            f"select * from read_csv('{csv_path}', header=true, columns={{{types}}})"
        )
        return con.execute(f"select count(*) from {SCHEMA}.{TABLE}").fetchone()[0]
    finally:
        con.close()


OS_CHOICES = ["iOS", "Android", "Android", "iOS", "HarmonyOS", "KaiOS", "android"]
SOURCES = ["mobile_app", "web", "partner_sdk"]


def main() -> None:
    rng = random.Random(SEED)
    rows: list[dict] = []
    eid = 0
    days = (END - START).days + 1

    # profile -> (n_customers, active_day_probability, events_per_active_day)
    cohorts = {
        "dormant": (25, 0.55, (1, 3)),  # only in the first third of the range
        "single": (15, 0.0, (1, 1)),  # exactly one event, placed by hand
        "always_on": (30, 0.95, (1, 6)),
        "regular": (100, 0.35, (1, 4)),
        "churner": (5, 0.6, (2, 5)),  # rotates devices constantly
    }

    cid = 0
    for cohort, (n, p_active, (lo, hi)) in cohorts.items():
        for _ in range(n):
            cid += 1
            customer = f"cust_{cid:05d}"
            n_devices = 40 if cohort == "churner" else rng.randint(1, 3)
            devices = [f"dev_{customer}_{i}" for i in range(n_devices)]

            if cohort == "single":
                offsets = [rng.randrange(0, days)]
            elif cohort == "dormant":
                # Confined to the first third: by the late target dates these
                # customers have all_time history but empty recent windows.
                offsets = [d for d in range(0, days // 3) if rng.random() < p_active]
            else:
                offsets = [d for d in range(days) if rng.random() < p_active]

            for off in offsets:
                day = START + timedelta(days=off)
                for _ in range(rng.randint(lo, hi)):
                    eid += 1
                    hour = rng.choice([1, 3, 6, 7, 10, 13, 16, 19, 21, 23])
                    ts = datetime(
                        day.year, day.month, day.day, hour, rng.randrange(60), rng.randrange(60)
                    )

                    # ~3% of rows are ingested late, inside the 3-day window the
                    # partial layer rewrites, so seal-and-tail gets exercised.
                    lag = rng.choice([1, 2]) if rng.random() < 0.03 else 0
                    valid_from = datetime(day.year, day.month, day.day) + timedelta(days=lag)

                    device = rng.choice(devices)
                    if rng.random() < 0.04:
                        device = ""  # NULL device on an otherwise valid event

                    rows.append(
                        {
                            "event_id": f"evt_{eid:07d}",
                            "customer_id": customer,
                            "device_id": device,
                            "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
                            "login_source": rng.choice(SOURCES),
                            "os_name": rng.choice(OS_CHOICES),
                            "event_status": "SUCCESS" if rng.random() < 0.82 else "FAILED",
                            "_scd_valid_from": valid_from.strftime("%Y-%m-%d %H:%M:%S"),
                            "_scd_valid_to": "9999-12-31 00:00:00",
                        }
                    )

    rows.sort(key=lambda r: (r["event_timestamp"], r["event_id"]))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    late = sum(1 for r in rows if r["_scd_valid_from"][:10] != r["event_timestamp"][:10])
    nulls = sum(1 for r in rows if not r["device_id"])
    print(f"{OUT}: {len(rows)} rows, {cid} customers, {late} late-arriving, {nulls} null device")

    loaded = load(OUT, DB)
    print(f"{DB}: {SCHEMA}.{TABLE} loaded with {loaded} rows")


if __name__ == "__main__":
    main()
