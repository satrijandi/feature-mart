"""Where this showcase keeps its things, and how it invokes dbt.

The compiler and its generated dbt project live in the repository root; the
warehouse, the fixture, the profile and the virtualenv that has dbt in it live
here. Every tool resolves both halves through this module so that moving either
one is a single edit rather than a grep.
"""

from __future__ import annotations

import os
from pathlib import Path

SHOWCASE = Path(__file__).resolve().parent.parent
REPO = SHOWCASE.parent

# --- the core repository: generated, committed, warehouse-agnostic ----------
TRANSFORM = REPO / "transform"
REGISTRY_DIR = REPO / "registry"

# --- this showcase: the runtime the core deliberately does not carry --------
VENV = SHOWCASE / ".venv"
PYTHON = VENV / "bin" / "python"
DBT = VENV / "bin" / "dbt"
DB = SHOWCASE / "warehouse.duckdb"
SEEDS = SHOWCASE / "seeds"
PROFILES_DIR = SHOWCASE


def dbt_env() -> dict[str, str]:
    """Environment that points dbt at the core project and keeps its output here.

    The dbt project is read-only from this side: it is generated output under
    version control in the repository root. Directing target/ and logs/ into the
    showcase keeps a run from leaving artefacts in it.
    """
    return {
        **os.environ,
        "DBT_PROJECT_DIR": str(TRANSFORM),
        "DBT_PROFILES_DIR": str(PROFILES_DIR),
        "DBT_TARGET_PATH": str(SHOWCASE / "target"),
        "DBT_LOG_PATH": str(SHOWCASE / "logs"),
        "DBT_DUCKDB_PATH": str(DB),
    }
