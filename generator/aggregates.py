"""The aggregate algebra.

Every supported aggregation is expressed as a commutative monoid over a
partial state:

    partial   : source rows   -> state   (one state per entity, per event_date)
    state_agg : many states   -> state   (roll a range of days up)
    merge     : state, state  -> state   (fold sealed history into a fresh tail)
    finalize  : state         -> value   (what the mart publishes)

Two consequences are worth stating, because they are why the design holds
together:

1. A bounded window is exactly `finalize(state_agg(days in window))`, and
   all_time is exactly `finalize(merge(sealed_state, state_agg(unsealed
   tail)))`. There is no second code path, so `l30d` and `all_time` cannot
   drift apart -- they are the same fold over different ranges.

2. Adding an aggregation means implementing four small methods here. It does
   not mean touching the templates, the mart, or the orchestration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from generator.expr import Expr, compose, macro


def guard(value_sql: str, predicate: str) -> Expr:
    """Restrict a value to the rows a condition combo selects.

    The all-defaults combo carries the predicate TRUE, so it emits the bare
    column and the marginal (uncut) features stay readable in the generated SQL.
    """
    if predicate.strip().upper() == "TRUE":
        return Expr.sql(value_sql)
    return Expr.sql(f"case when {predicate} then {value_sql} end")


class Aggregate(ABC):
    key: str
    #: True when an entity with no matching rows should publish 0 rather than
    #: NULL. Counts are zero-filled; extrema are not, because "never happened"
    #: and "happened at time zero" are different facts.
    zero_filled: bool = False

    @abstractmethod
    def partial_expr(self, value_sql: str, predicate: str) -> Expr: ...

    @abstractmethod
    def state_agg_expr(self, col: Expr) -> Expr: ...

    @abstractmethod
    def merge_expr(self, a: Expr, b: Expr) -> Expr: ...

    def finalize_expr(self, state: Expr) -> Expr:
        return state

    @abstractmethod
    def partial_dtype(self, field_dtype: str | None) -> str: ...

    @abstractmethod
    def feature_dtype(self, field_dtype: str | None) -> str: ...

    def window_expr(self, col: str, window_predicate: str | None) -> Expr:
        """Bounded-window value: finalize the fold over states inside the window."""
        scoped = (
            Expr.sql(col)
            if window_predicate is None
            else Expr.sql(f"case when {window_predicate} then {col} end")
        )
        return self.finalize_expr(self.state_agg_expr(scoped))


class Count(Aggregate):
    key = "count"
    zero_filled = True

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return compose("count({0})", guard(value_sql, predicate))

    def state_agg_expr(self, col: Expr) -> Expr:
        return compose("sum({0})", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return compose("(coalesce({0}, 0) + coalesce({1}, 0))", a, b)

    def finalize_expr(self, state: Expr) -> Expr:
        return compose("coalesce({0}, 0)", state)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return "bigint"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"


class Sum(Aggregate):
    key = "sum"
    zero_filled = True

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return compose("sum({0})", guard(value_sql, predicate))

    def state_agg_expr(self, col: Expr) -> Expr:
        return compose("sum({0})", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return compose("(coalesce({0}, 0) + coalesce({1}, 0))", a, b)

    def finalize_expr(self, state: Expr) -> Expr:
        return compose("coalesce({0}, 0)", state)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"


class _Extremum(Aggregate):
    sql_fn: str
    merge_macro: str

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return compose(f"{self.sql_fn}({{0}})", guard(value_sql, predicate))

    def state_agg_expr(self, col: Expr) -> Expr:
        return compose(f"{self.sql_fn}({{0}})", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        # LEAST/GREATEST disagree on NULL across engines, and here NULL means
        # "no data yet" rather than "unknown", so it must not propagate.
        return macro(self.merge_macro, a, b)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return field_dtype or "double"


class Min(_Extremum):
    key = "min"
    sql_fn = "min"
    merge_macro = "fs_least2"


class Max(_Extremum):
    key = "max"
    sql_fn = "max"
    merge_macro = "fs_greatest2"


class CountDistinctExact(Aggregate):
    """Exact distinct via a retained key set.

    State is the actual set of distinct values, cast to varchar so the element
    type is identical on every engine. Sound for fields whose per-entity
    cardinality is small (device models, merchant categories);
    `distinct_method: approx` covers the rest. A generated dbt test watches the
    realised set sizes, so switching a field to approx is an evidence-driven
    decision rather than a guess.
    """

    key = "count_distinct"
    zero_filled = True

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return macro("fs_collect_set", compose("cast({0} as varchar)", guard(value_sql, predicate)))

    def state_agg_expr(self, col: Expr) -> Expr:
        return macro("fs_array_union_agg", col)

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return macro("fs_array_union2", a, b)

    def finalize_expr(self, state: Expr) -> Expr:
        return macro("fs_array_size", state)

    def partial_dtype(self, field_dtype: str | None) -> str:
        return "array<varchar>"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"


class CountDistinctApprox(Aggregate):
    """Approximate distinct via a mergeable KMV sketch. See macros/fs_kmv.sql.

    `k` is emitted explicitly at every call site rather than read from a dbt
    var, so two specs in the same project can choose different accuracy/size
    trade-offs and neither can be silently changed by a run-time flag.
    """

    key = "count_distinct"
    zero_filled = True

    def __init__(self, k: int = 256) -> None:
        self.k = k

    def partial_expr(self, value_sql: str, predicate: str) -> Expr:
        return macro("fs_kmv_build", guard(value_sql, predicate), str(self.k))

    def state_agg_expr(self, col: Expr) -> Expr:
        return macro("fs_kmv_union_agg", col, str(self.k))

    def merge_expr(self, a: Expr, b: Expr) -> Expr:
        return macro("fs_kmv_merge2", a, b, str(self.k))

    def finalize_expr(self, state: Expr) -> Expr:
        return macro("fs_kmv_estimate", state, str(self.k))

    def partial_dtype(self, field_dtype: str | None) -> str:
        return "array<double>"

    def feature_dtype(self, field_dtype: str | None) -> str:
        return "bigint"


_REGISTRY: dict[str, type[Aggregate]] = {
    "count": Count,
    "sum": Sum,
    "min": Min,
    "max": Max,
}


def get_aggregate(agg: str, distinct_method: str = "exact", kmv_k: int = 256) -> Aggregate:
    if agg == "count_distinct":
        return CountDistinctApprox(k=kmv_k) if distinct_method == "approx" else CountDistinctExact()
    try:
        return _REGISTRY[agg]()
    except KeyError:
        raise KeyError(f"no aggregate implementation for {agg!r}") from None
