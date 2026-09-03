"""The monoid laws the whole incremental design rests on."""

import pytest

from generator.aggregates import get_aggregate
from generator.expr import Expr


@pytest.mark.parametrize("key", ["count", "sum", "min", "max"])
def test_scalar_aggregates_emit_plain_sql(key):
    agg = get_aggregate(key)
    assert not agg.partial_expr("x", "TRUE").is_jinja


def test_marginal_combo_emits_the_bare_column():
    """predicate TRUE must not generate a pointless CASE wrapper."""
    assert get_aggregate("count").partial_expr("event_id", "TRUE").render() == "count(event_id)"


def test_condition_becomes_a_case_guard():
    out = get_aggregate("count").partial_expr("event_id", "os = 'IOS'").render()
    assert out == "count(case when os = 'IOS' then event_id end)"


def test_window_is_finalize_of_state_agg():
    """The central invariant: a bounded window and all_time share one fold."""
    for key in ["count", "min", "max", "sum"]:
        agg = get_aggregate(key)
        col = Expr.sql("case when in_win then p_c end")
        assert (
            agg.window_expr("p_c", "in_win").render()
            == agg.finalize_expr(agg.state_agg_expr(col)).render()
        )


def test_count_merge_is_additive_and_null_safe():
    m = get_aggregate("count").merge_expr(Expr.sql("a"), Expr.sql("b")).render()
    assert m == "(coalesce(a, 0) + coalesce(b, 0))"


def test_extremum_merge_avoids_least_greatest():
    """LEAST/GREATEST disagree on NULL across engines; NULL here means 'no data'."""
    assert "fs_least2" in get_aggregate("min").merge_expr(Expr.sql("a"), Expr.sql("b")).render()
    assert "fs_greatest2" in get_aggregate("max").merge_expr(Expr.sql("a"), Expr.sql("b")).render()


def test_exact_distinct_casts_to_varchar_for_cross_engine_element_typing():
    assert (
        "as varchar" in get_aggregate("count_distinct", "exact").partial_expr("d", "TRUE").render()
    )


def test_approx_distinct_emits_its_own_k_at_every_call_site():
    agg = get_aggregate("count_distinct", "approx", kmv_k=1024)
    for out in (
        agg.partial_expr("d", "TRUE").render(),
        agg.state_agg_expr(Expr.sql("c")).render(),
        agg.merge_expr(Expr.sql("a"), Expr.sql("b")).render(),
        agg.finalize_expr(Expr.sql("s")).render(),
    ):
        assert "1024" in out


def test_counts_are_zero_filled_but_extrema_are_not():
    assert get_aggregate("count").zero_filled
    assert get_aggregate("count_distinct", "exact").zero_filled
    assert not get_aggregate("min").zero_filled
    assert not get_aggregate("max").zero_filled


def test_unknown_aggregate_is_rejected():
    with pytest.raises(KeyError):
        get_aggregate("median")
