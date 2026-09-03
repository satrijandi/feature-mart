# =============================================================================
# feature-mart
#
# The contract: `features/*.yml` is the only file a human edits. Everything
# under transform/models, transform/tests and registry/ is generated, committed
# and drift-checked, so a pull request shows exactly which feature columns a
# spec change adds or removes.
# =============================================================================

PY            := .venv/bin/python
DBT           := ../.venv/bin/dbt
TARGET_DATE   ?= $(shell date -u +%F)
BACKFILL_FROM ?= 2026-07-01
FEATURE       ?= fact_agg_features_login_history_v2
COMPOSE       := docker compose -f infra/docker-compose.yml

export DBT_PROFILES_DIR = $(CURDIR)/transform

.DEFAULT_GOAL := help
.PHONY: help setup generate check validate explain lint test dbt-seed dbt-backfill \
        examples dbt-run dbt-revise dbt-test dbt-docs verify e2e kmv audit audit-fix \
        publish stack-up \
        stack-down \
        stack-logs ci clean

help:  ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- toolchain --------------------------------------------------------------
setup:  ## Create the venv and install everything
	uv venv --python 3.11
	uv pip install -e ".[dbt,dev,notebook]"

# --- the generator ----------------------------------------------------------
generate:  ## Compile feature specs into dbt models, tests and the registry
	$(PY) -m generator.cli generate

check:  ## Fail if generated output is stale (this is what CI runs)
	$(PY) -m generator.cli generate --check

validate:  ## Validate every spec without generating
	$(PY) -m generator.cli validate

explain:  ## Show how FEATURE=<name> expands
	$(PY) -m generator.cli explain features/$(FEATURE).yml

examples:  ## Parse and expand every spec in features/examples/
	@for f in features/examples/*.yml; do \
	  $(PY) -m generator.cli explain $$f > /dev/null || exit 1; \
	  echo "  ok   $$f"; \
	done

lint:  ## Lint the generator
	.venv/bin/ruff check generator tools tests
	.venv/bin/ruff format --check generator tools tests

test:  ## Unit-test the generator
	$(PY) -m pytest tests -q

# --- dbt --------------------------------------------------------------------
dbt-seed:  ## Load the synthetic fixture
	$(PY) tools/gen_seed.py
	cd transform && $(DBT) seed --vars '{target_date: $(TARGET_DATE)}'

dbt-backfill:  ## Initial load: build all partials from BACKFILL_FROM in one pass
	cd transform && $(DBT) run \
	  --vars '{target_date: $(TARGET_DATE), fs_backfill_from: $(BACKFILL_FROM)}'

dbt-run:  ## Build one as-of date
	cd transform && $(DBT) run --vars '{target_date: $(TARGET_DATE)}'

dbt-revise:  ## Refresh the partitions that are still provisional
	@dates=$$($(PY) tools/revision_window.py $(FEATURE) $(TARGET_DATE)); \
	if [ -z "$$dates" ]; then echo "nothing refreshable: earlier partitions are final"; else \
	  cd transform && for d in $$dates; do echo "refreshing $$d"; \
	    $(DBT) build --select 'tag:$(FEATURE)' --vars "{target_date: $$d}" -q || exit 1; \
	  done; fi

dbt-test:  ## Run schema tests, the generated invariants, and adapter conformance
	cd transform && $(DBT) test --vars '{target_date: $(TARGET_DATE)}'

dbt-docs:  ## Build and serve the dbt docs site
	cd transform && $(DBT) docs generate --vars '{target_date: $(TARGET_DATE)}' \
	  && $(DBT) docs serve --port 8082

# --- verification -----------------------------------------------------------
verify:  ## Compare the mart against an independent brute-force recomputation
	$(PY) tools/verify_against_bruteforce.py $(TARGET_DATE)

e2e:  ## Retry-idempotency, gap self-healing and late-arrival scenarios
	$(PY) tools/e2e_scenarios.py

kmv:  ## Measure the approximate-distinct sketch against exact ground truth
	$(PY) tools/verify_kmv_accuracy.py $(TARGET_DATE)

audit:  ## Audit the published offline store against its own contract
	S3_ENDPOINT=$${S3_ENDPOINT:-localhost:8433} $(PY) tools/audit_offline_store.py

audit-fix:  ## Republish any offline-store partition that violates its contract
	S3_ENDPOINT=$${S3_ENDPOINT:-localhost:8433} $(PY) tools/audit_offline_store.py --fix

publish:  ## Publish a mart partition to the offline store on S3
	S3_ENDPOINT=$${S3_ENDPOINT:-localhost:8433} \
	  $(PY) tools/publish_offline_store.py $(FEATURE) $(TARGET_DATE)

# --- local stack ------------------------------------------------------------
stack-up:  ## Start SeaweedFS, Airflow and Jupyter
	$(COMPOSE) --profile full up -d --build
	@echo ""
	@echo "  SeaweedFS S3   http://localhost:$${FS_S3_PORT:-8433}"
	@echo "  SeaweedFS UI   http://localhost:$${FS_SEAWEED_UI_PORT:-9433}"
	@echo "  Airflow        http://localhost:$${FS_AIRFLOW_PORT:-8081}  (admin/admin)"
	@echo "  JupyterLab     http://localhost:$${FS_JUPYTER_PORT:-8900}"

stack-down:  ## Stop the stack and drop its volumes
	$(COMPOSE) --profile full down -v

stack-logs:  ## Tail stack logs
	$(COMPOSE) --profile full logs -f --tail=100

# --- CI ---------------------------------------------------------------------
ci: check lint test examples  ## Everything CI runs before touching a warehouse
	@echo "generator OK"

clean:  ## Remove build artefacts
	rm -rf transform/target transform/dbt_packages transform/logs \
	       transform/warehouse.duckdb .pytest_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
