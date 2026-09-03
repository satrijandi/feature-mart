"""The refreshable-window rule.

Getting this boundary wrong is silent in both directions: too narrow and
revisions are lost, too wide and a final partition is rebuilt with sealed state
that postdates it, leaking the future into its all_time features. So the rule is
pinned here rather than left to the DAG.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pytest

from tools.revision_window import refreshable_dates

FEATURE = "fact_agg_features_login_history_v2"
LATE = 3  # settings.late_arrival_days in the spec


@pytest.fixture
def db(tmp_path: Path):
    """A stand-in accumulator whose watermark the test controls."""

    seq = [0]

    def _make(watermark: date | None) -> Path:
        # A fresh file per call, so a sweep over watermark positions does not
        # accumulate state between iterations.
        seq[0] += 1
        path = tmp_path / f"w{seq[0]}.duckdb"
        con = duckdb.connect(str(path))
        con.execute("create schema if not exists intermediate")
        con.execute(
            f"create table intermediate.int_{FEATURE}__alltime_state "
            "(safe_id varchar, _state_as_of_date date)"
        )
        if watermark is not None:
            con.execute(
                f"insert into intermediate.int_{FEATURE}__alltime_state values ('x', ?)",
                [watermark],
            )
        con.close()
        return path

    return _make


def test_in_order_run_refreshes_the_whole_window(db):
    """W = T - late is the ordinary case, and the full window is refreshable."""
    target = date(2026, 9, 3)
    dates = refreshable_dates(FEATURE, target, db(date(2026, 8, 31)))
    assert dates == [date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31)]
    assert len(dates) == LATE


def test_dates_the_accumulator_has_sealed_past_are_final(db):
    """A partition older than the watermark is final; rebuilding it would leak."""
    target = date(2026, 9, 3)
    dates = refreshable_dates(FEATURE, target, db(date(2026, 9, 2)))
    assert dates == [date(2026, 9, 2)]


def test_window_can_be_empty(db):
    """Nothing is refreshable when the watermark has passed the whole window."""
    assert refreshable_dates(FEATURE, date(2026, 9, 3), db(date(2026, 9, 3))) == []


def test_no_date_is_ever_behind_the_watermark(db):
    """The property that matters, over a sweep of watermark positions."""
    target = date(2026, 9, 10)
    for offset in range(0, 8):
        wm = date(2026, 9, 10 - offset)
        for d in refreshable_dates(FEATURE, target, db(wm)):
            assert d >= wm, f"{d} is behind watermark {wm}"
            assert d < target, f"{d} is not in the past of {target}"


def test_window_never_exceeds_late_arrival_days(db):
    """A watermark far in the past must not widen the window."""
    dates = refreshable_dates(FEATURE, date(2026, 9, 3), db(date(2026, 1, 1)))
    assert len(dates) == LATE
    assert min(dates) == date(2026, 8, 31)


def test_missing_accumulator_means_nothing_is_sealed(db):
    """A first run has sealed nothing, so the whole window is provisional."""
    assert len(refreshable_dates(FEATURE, date(2026, 9, 3), db(None))) == LATE
