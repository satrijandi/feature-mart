"""Daily feature-store DAG, derived from the generated registry.

The DAG is not hand-maintained. It reads registry/*.json -- the same artefact
the generator commits -- and builds one task group per feature spec. Adding a
spec and running `make generate` therefore adds orchestration automatically,
and a DAG that disagrees with the models it runs is not representable.

AS-OF DATE. Every task passes `data_interval_start` as target_date, so the run
scheduled just after midnight on D+1 computes features as of day D, which is
the last day whose events are complete. Airflow's logical date is the single
source of truth: a manual run, a backfill and a scheduled run all take the same
path and produce the same numbers.

BACKFILL. The accumulator cannot be reconstructed for a date behind its
watermark, so history is loaded with an explicit backfill (`make dbt-backfill`,
which full-refreshes the partial layer from a start date) rather than by
scheduler catchup. See FS_CATCHUP below.

REVISION WINDOW. A partition is not final the day it is built. The partial layer
keeps absorbing late-arriving events for `late_arrival_days`, so the marts for
those days have stale inputs until that window closes. Each run therefore
republishes the preceding `late_arrival_days` partitions as well as its own.
Every model is idempotent for a given as-of date, so this is a refresh, not a
rewrite of history. A partition is provisional until T + late_arrival_days and
final afterwards.

ORDERING. Runs are serialised with max_active_runs=1, and deliberately NOT with
depends_on_past. The accumulator is self-healing -- it consumes
(watermark, seal] rather than assuming yesterday -- so ordering is an
efficiency concern, not a correctness one.

depends_on_past would add nothing and cost a deadlock class: one incomplete day
blocks every later day, including a manual rerun of the day that would fix it,
and a first run has no predecessor to satisfy it at all. Serialisation gives the
ordering; the watermark gives the correctness.
"""

from __future__ import annotations

import json
import os
from datetime import timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(os.getenv("FS_PROJECT_ROOT", "/opt/feature-mart"))
DBT_DIR = PROJECT_ROOT / "transform"
REGISTRY_DIR = PROJECT_ROOT / "registry"
DBT = os.getenv("FS_DBT_BIN", "dbt")

# target_date is the day the interval covers, not the day the run happens.
TARGET_DATE = "{{ data_interval_start | ds }}"

DBT_ENV = {
    "DBT_PROFILES_DIR": str(DBT_DIR),
    "DBT_TARGET": os.getenv("DBT_TARGET", "seaweed"),
    "DBT_DUCKDB_PATH": os.getenv("DBT_DUCKDB_PATH", str(DBT_DIR / "warehouse.duckdb")),
    "S3_ENDPOINT": os.getenv("S3_ENDPOINT", "seaweedfs:8333"),
    "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY", "featuremart"),
    "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY", "featuremart"),
    "FS_BRONZE_SCHEMA": os.getenv("FS_BRONZE_SCHEMA", "bronze_events"),
    "PATH": os.getenv("PATH", "/usr/local/bin:/usr/bin:/bin"),
}


def load_registry() -> list[dict]:
    """Feature specs to orchestrate, read from committed generator output."""
    if not REGISTRY_DIR.exists():
        return []
    out = []
    for path in sorted(REGISTRY_DIR.glob("*.json")):
        out.append(json.loads(path.read_text()))
    return out


def dbt_task(task_id: str, command: str, select: str, **kwargs) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} {command} "
            f"--select '{select}' "
            f'--vars \'{{"target_date": "{TARGET_DATE}"}}\' '
            f"--target $DBT_TARGET"
        ),
        env=DBT_ENV,
        append_env=True,
        **kwargs,
    )


with DAG(
    dag_id="feature_store_daily",
    description="Daily batch feature marts, one task group per generated feature spec",
    schedule="0 2 * * *",  # 02:00 UTC, after the upstream journal settles
    start_date=__import__("pendulum").datetime(2026, 8, 20, tz="UTC"),
    # Catchup is OFF by default, deliberately. The all_time accumulator is a
    # single forward-only fold, so letting the scheduler improvise its way
    # through history means asking it to build as-of dates that sit BEHIND the
    # watermark -- which the guard test correctly refuses, leaving a wall of red
    # tasks that no retry can clear. An initial load is a deliberate operation:
    #     make dbt-backfill TARGET_DATE=<first day> BACKFILL_FROM=<start>
    # then unpause from there. Set FS_CATCHUP=1 once the accumulator has been
    # reset for the window being replayed.
    catchup=os.getenv("FS_CATCHUP", "0") == "1",
    max_active_runs=1,  # the all_time accumulator is a serial fold
    # DuckDB in the local stack is a single-writer file, so feature groups must
    # not build concurrently there. A real warehouse has no such limit: raise
    # FS_MAX_ACTIVE_TASKS to fan the groups out.
    max_active_tasks=int(os.getenv("FS_MAX_ACTIVE_TASKS", "1")),
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        # Safe because every model is idempotent for a given target_date: the
        # partial layer replaces its date range, the accumulator consumes an
        # empty range on a repeat, and the mart replaces its partition.
        "retry_exponential_backoff": True,
    },
    tags=["feature-store", "dbt", "batch"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # A stale source silently produces a day of zeros that looks like genuine
    # customer inactivity. Fail before building rather than after publishing.
    freshness = BashOperator(
        task_id="check_source_freshness",
        bash_command=(
            f"cd {DBT_DIR} && {DBT} source freshness "
            f'--vars \'{{"target_date": "{TARGET_DATE}"}}\' --target $DBT_TARGET'
        ),
        env=DBT_ENV,
        append_env=True,
    )

    registry = load_registry()
    if not registry:
        start >> freshness >> end

    for entry in registry:
        name = entry["feature_name"]
        models = entry["models"]

        with TaskGroup(group_id=name, tooltip=entry.get("description", "")) as group:
            # Reusable per-day state. Rewrites the late-arrival window as well
            # as today, so events that arrived late land on their own date.
            partials = dbt_task(
                "build_daily_partials",
                "build",
                f"{models['staging']}+1 {models['daily_partials']}",
            )

            # The serial fold. Correct in any order thanks to the watermark;
            # running in order simply keeps each run's cost to a single day.
            accumulator = (
                dbt_task("fold_alltime_state", "build", models["alltime_state"])
                if models.get("alltime_state")
                else None
            )

            # Windows, unsealed tail, and the published wide table. `dbt build`
            # interleaves the generated invariant tests, so a violated
            # invariant stops the run before the partition is exposed.
            publish = dbt_task(
                "build_and_test_mart",
                "build",
                f"tag:{name}",
            )

            # Refresh the days whose inputs are still settling. Cheap, because
            # every model is a no-op for a date it has already absorbed.
            late = int(entry.get("settings", {}).get("late_arrival_days", 0))

            # Which past partitions are still refreshable is not a fixed offset:
            # it is bounded below by the accumulator's watermark, because a date
            # the accumulator has already sealed past is FINAL and rebuilding it
            # would fold future events into its all_time features. That rule and
            # its rationale live in tools/revision_window.py, which resolves the
            # dates against the warehouse at run time.
            revise = None
            if late > 0:
                revise = BashOperator(
                    task_id="refresh_revision_window",
                    bash_command=(
                        f"set -euo pipefail; cd {PROJECT_ROOT} && "
                        f"DATES=$(python tools/revision_window.py {name} {TARGET_DATE}) && "
                        f'if [ -z "$DATES" ]; then '
                        f'echo "nothing refreshable: all earlier partitions are final"; '
                        f"else cd {DBT_DIR} && for d in $DATES; do "
                        f'echo "refreshing $d" && '
                        f"{DBT} build --select 'tag:{name}' "
                        f'--vars "{{target_date: $d}}" --target $DBT_TARGET; '
                        f"done; fi"
                    ),
                    env=DBT_ENV,
                    append_env=True,
                )

            # Publish exactly the dates this run built or refreshed -- the same
            # resolver, so the two can never drift apart. Publishing a partition
            # this run did not refresh would trip the publisher's leakage gate,
            # correctly.
            offline = BashOperator(
                task_id="publish_offline_store",
                bash_command=(
                    f"set -euo pipefail; cd {PROJECT_ROOT} && "
                    f"DATES=$(python tools/revision_window.py {name} {TARGET_DATE} "
                    f"--include-target) && "
                    f"for d in $DATES; do "
                    f"python tools/publish_offline_store.py {name} $d; done"
                ),
                env=DBT_ENV,
                append_env=True,
            )

            chain = [partials]
            if accumulator is not None:
                chain.append(accumulator)
            chain.append(publish)
            if revise is not None:
                chain.append(revise)
            chain.append(offline)
            for upstream, downstream in zip(chain, chain[1:]):
                upstream >> downstream

        start >> freshness >> group >> end
