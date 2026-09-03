"""Project-level artefacts that span every feature spec.

Sources live here rather than on the renderer because two specs can legitimately
read the same physical table, and dbt requires exactly one definition per
source. Emitting one file per spec produced a duplicate-source error the moment
a second spec was added -- the kind of failure that only appears once the system
has more than one user.
"""

from __future__ import annotations

import yaml

from generator.spec import FeatureSpec

HEADER = (
    "# ============================================================================\n"
    "# GENERATED FILE - DO NOT EDIT BY HAND\n"
    "#   Merged from the `relations:` block of every feature spec.\n"
    "#   Edit a spec and run `make generate`.\n"
    "# ============================================================================\n"
)


def render_sources_yml(specs: list[FeatureSpec]) -> str:
    """One deduplicated sources.yml covering every relation any spec reads."""
    # source_name -> table_name -> consuming feature names
    tree: dict[str, dict[str, list[str]]] = {}
    for spec in specs:
        for rel in spec.relations.values():
            tree.setdefault(rel.source_name, {}).setdefault(rel.table_name, []).append(
                spec.feature_name
            )

    sources = []
    for source_name in sorted(tree):
        tables = []
        for table_name in sorted(tree[source_name]):
            consumers = sorted(set(tree[source_name][table_name]))
            tables.append(
                {
                    "name": table_name,
                    "description": (
                        "Immutable append-only event log.\n"
                        "The feature store relies on rows for a past event_date never "
                        "changing once written; the reusable daily partial layer is only "
                        "sound while that holds.\n"
                        "Consumed by: " + ", ".join(consumers)
                    ),
                    "loaded_at_field": "_scd_valid_from",
                    "freshness": {
                        "warn_after": {"count": 12, "period": "hour"},
                        "error_after": {"count": 36, "period": "hour"},
                    },
                }
            )
        sources.append(
            {
                "name": source_name,
                "description": f"Upstream append-only source feeding {len(tables)} table(s).",
                "schema": "{{ env_var('FS_BRONZE_SCHEMA', '" + source_name + "') }}",
                "tables": tables,
            }
        )

    return HEADER + yaml.safe_dump({"version": 2, "sources": sources}, sort_keys=False, width=100)
