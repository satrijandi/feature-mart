# feature-mart showcase

The compiler in the repository root turns a YAML spec into a dbt project.
This directory is where that project is actually run, end to end, on a stack that looks like the one it would run on in production.

It exists as a separate thing for one reason: **nothing here is a dependency of the compiler.**
Compiling a spec needs `pyyaml` and `jinja2`.
Proving the compiled models produce the right numbers needs dbt-core, a warehouse, an object store, a scheduler and a notebook, and if those lived in the core package then embedding the compiler anywhere would drag all of them along.

```
../features/*.yml            the spec
../generator/                the compiler          <- core
../transform/                the generated project <- core, committed, read-only from here
                             |
showcase/                    |  what makes it run
  profiles.yml               +- the warehouse connection
  seeds/, tools/gen_seed.py  +- a synthetic source with the awkward cases in it
  tools/                     +- verifiers, the offline-store publisher, the scenarios
  orchestration/dags/        +- the registry-derived Airflow DAG
  infra/                     +- SeaweedFS, Airflow, JupyterLab
  notebooks/                 +- validation from a consumer's seat
```

Every run artefact stays here: `warehouse.duckdb`, `target/`, `logs/`, `seeds/*.csv`.
Running the pipeline never writes into `../transform`, and CI asserts that.

## Quick start

```bash
make setup                                     # venv: the compiler plus a dbt runtime
make seed                                      # synthetic fixture, loaded as the bronze source
make dbt-backfill TARGET_DATE=2026-08-20       # initial load
make dbt-run      TARGET_DATE=2026-08-21       # one daily increment
make dbt-revise   TARGET_DATE=2026-08-21       # refresh the still-provisional partitions
make dbt-test     TARGET_DATE=2026-08-21       # invariants + conformance

make verify TARGET_DATE=2026-08-21             # brute-force cross-check
make e2e                                       # retry / gap / late-arrival / replay scenarios
make kmv    TARGET_DATE=2026-08-21             # sketch accuracy against exact counts
```

Run as-of dates **forward**.
The pipeline tolerates gaps and replays by design, but the one thing it cannot reconstruct is `all_time` for a date behind the accumulator's watermark; that case fails the build loudly and is fixed with `--full-refresh` on the accumulator.

`make ci` runs the whole gate in one command, and is what the `showcase` job in CI runs, on the same dates.

## Where the source data comes from

`make seed` writes `seeds/customer_login.csv` and loads it into the warehouse as `bronze_events.customer_login`.
It does not go through `dbt seed`, on purpose: the dbt project is generated output that knows nothing about any particular source table, and a seed block naming this fixture would put that knowledge back into it.
In production an upstream pipeline writes that table; here this script does, and dbt sees the same thing either way.

The shape of the fixture matters more than its volume.
It deliberately contains dormant entities, single-event entities, a device churner with high distinct cardinality, NULL device ids, `os_name` values outside the enum, events in every hour of the day, and rows ingested late but inside the late-arrival window.
Each one breaks a naive feature pipeline in a different place.

## The local stack

```bash
make stack-up
```

| Service | URL | Role |
|---|---|---|
| SeaweedFS | `localhost:8433` (S3) | object store standing in for the lake |
| Airflow | `localhost:8081` | runs the registry-derived DAG (admin/admin) |
| JupyterLab | `localhost:8900` | `notebooks/01_validate_feature_store.ipynb` |

Host ports are deliberately unconventional so the stack coexists with anything already running; override with `FS_S3_PORT`, `FS_AIRFLOW_PORT`, `FS_JUPYTER_PORT`.

The containers mount the whole repository at `/opt/feature-mart` and work from `/opt/feature-mart/showcase`, so the DAG runs the same generated models that are committed in `../transform`.

## The published offline store

The mart in the warehouse is the source of truth; the offline store is the copy other teams read.

```bash
make publish TARGET_DATE=2026-08-21   # one partition, as Parquet on S3
make audit                            # the whole store, against its own contract
make audit-fix                        # republish anything that violates it
```

Publishing as partitioned Parquet on object storage, with the registry travelling beside the data, is what makes the feature store consumable without warehouse credentials.
`audit` exists because guarding the write is not the same as guarding the store: partitions written by an earlier build or a buggier branch are still what a training pipeline reads.

## The notebook

```bash
make notebook   # regenerate it from tools/build_notebook.py
```

It is written as a builder rather than a hand-edited `.ipynb` so it stays diffable in review.

The notebook reads the **published Parquet on S3**, not the warehouse, because that is the path a training pipeline actually takes.
It checks point-in-time safety, shows `all_time` persisting while bounded windows decay for dormant customers, measures the sketch against exact counts, and assembles a leak-free training set.

It deliberately has no access to the warehouse file: DuckDB takes an exclusive lock, so an idle notebook connection would block the scheduler's next dbt run.

## The DAG

`orchestration/dags/feature_store_daily.py` is not hand-maintained.
It reads `../registry/*.json` and builds one task group per feature spec, so adding a spec and running `make generate` adds orchestration with no DAG edit.

Every task passes `data_interval_start` as `target_date`, so a manual run, a backfill and a scheduled run all take the same path and produce the same numbers.
The refresh and publish tasks both resolve their dates through `tools/revision_window.py`, so the dates a run refreshes and the dates it publishes cannot drift apart.

## What this directory proves

| Scenario | The failure it would catch |
|---|---|
| retry the newest as-of date | a retried task silently double-counting every `all_time` feature |
| skip a day, then run | a missed run losing the skipped day's events permanently |
| late-arriving events | events bucketed by ingest date instead of event date |
| replay a past as-of date | a backwards run recomputing a complete day under a narrower view |
| rebuild the revision window | a provisional partition never being corrected, or a final one being corrupted |
| serve a date behind the watermark | sealed state leaking future events into an older partition |
| dormancy under each spine | `active_window` truncating an entity's history rather than just its rows |

`make e2e` runs all seven and asserts on the numbers, not on dbt's exit code.
`make verify` is the backstop underneath them: it recomputes features the naive way, one flat query over raw source, and requires an exact match on every entity.

## Pointing it somewhere else

The three files a production deployment would replace are `profiles.yml`, `orchestration/` and `infra/`.
Nothing in `../transform` changes: it declares which profile it wants and nothing about how to connect, which is what makes the same committed models run here and against Databricks or Snowflake.

`DBT_TARGET=databricks make dbt-test` is the check that matters when porting.
The conformance suite in `../transform/tests/conformance/` asserts all 25 primitive contracts against whichever adapter is configured, so it proves the dialect implementations agree rather than merely compiling.

## Known limits

- **dbt-core 1.10 emits a version-deprecation notice.**
  All project-level deprecations are resolved; upgrading the pin in `pyproject.toml` is a separate change.
- **DuckDB is a single-writer file**, so the DAG builds feature groups serially here (`FS_MAX_ACTIVE_TASKS=1`).
  A real warehouse has no such limit; raise it to fan the groups out.
- **The notebook cannot read the warehouse**, by design.
  DuckDB's exclusive lock would block the scheduler, and reading the published store is the truer rehearsal anyway.
