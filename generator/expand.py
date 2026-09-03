"""Expand a validated spec into a concrete plan of partials and feature columns.

This is where the combinatorics happen. For each atomic field, the applied
condition categories are crossed with each other, then with the aggregations,
then with the time windows. Because every category carries a `default: TRUE`
member, the marginal (uncut) features fall out of the same cross-product rather
than needing a special case.

The plan distinguishes two kinds of output column:

  StoredFeature    folds up through the monoid from stored daily partials.
  ComputedFeature  is reconstructed in the mart from other columns, because its
                   value moves with the as-of date (days_since) or because it
                   is a ratio of two folds (avg).

A partial that exists only to feed a ComputedFeature is marked internal: it is
carried through the intermediate models but never published to the mart.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass
from dataclasses import field as dc_field

from generator.aggregates import Aggregate, get_aggregate
from generator.expr import Expr, macro
from generator.spec import AtomicField, FeatureSpec, SpecError, TimeWindow

MAX_IDENTIFIER_LEN = 255
WARN_IDENTIFIER_LEN = 128


def _join(sep: str, *parts: str) -> str:
    return sep.join(p for p in parts if p)


@dataclass(frozen=True)
class Combo:
    """One point in the cross-product of a field's applied condition categories."""

    members: tuple[tuple[str, str], ...]  # (category_name, member_name)
    label: str  # default members dropped; "" for the marginal
    predicate: str  # "TRUE" for the marginal

    @property
    def is_marginal(self) -> bool:
        return self.label == ""

    def describe(self) -> str:
        if self.is_marginal:
            return "all rows"
        return ", ".join(f"{cat}={mem}" for cat, mem in self.members)


@dataclass
class PartialColumn:
    """One column of the reusable per-entity, per-event_date partial state."""

    name: str
    dtype: str
    agg_key: str
    field: str
    combo: Combo
    value_sql: str
    distinct_method: str
    aggregate: Aggregate
    internal: bool = True

    @property
    def partial_sql(self) -> Expr:
        return self.aggregate.partial_expr(self.value_sql, self.combo.predicate)


@dataclass
class StoredFeature:
    name: str
    partial: PartialColumn
    window: TimeWindow
    dtype: str
    description: str
    internal: bool = True

    @property
    def agg_key(self) -> str:
        return self.partial.agg_key

    @property
    def field(self) -> str:
        return self.partial.field

    @property
    def combo(self) -> Combo:
        return self.partial.combo


@dataclass
class ComputedFeature:
    name: str
    window: TimeWindow
    dtype: str
    expr: Expr
    inputs: tuple[str, ...]
    description: str
    kind: str
    agg_key: str
    field: str
    combo: Combo


@dataclass
class FeaturePlan:
    spec: FeatureSpec
    partials: list[PartialColumn] = dc_field(default_factory=list)
    stored: list[StoredFeature] = dc_field(default_factory=list)
    computed: list[ComputedFeature] = dc_field(default_factory=list)
    output_order: list[str] = dc_field(default_factory=list)

    @property
    def public_stored(self) -> list[StoredFeature]:
        return [s for s in self.stored if not s.internal]

    @property
    def feature_count(self) -> int:
        return len(self.output_order)

    def outputs(self) -> list[StoredFeature | ComputedFeature]:
        """Public feature columns, in the order the spec implies."""
        by_name: dict[str, StoredFeature | ComputedFeature] = {}
        for s in self.stored:
            if not s.internal:
                by_name[s.name] = s
        for c in self.computed:
            by_name[c.name] = c
        return [by_name[n] for n in self.output_order]


def build_combos(spec: FeatureSpec, field: AtomicField, sep: str) -> list[Combo]:
    """Cross-product of the field's applied categories, declaration order preserved."""
    if not field.apply_cond_cat:
        return [Combo(members=(), label="", predicate="TRUE")]

    member_lists = [spec.categories[c].members for c in field.apply_cond_cat]
    combos: list[Combo] = []
    for picked in itertools.product(*member_lists):
        members = tuple((cat, m.name) for cat, m in zip(field.apply_cond_cat, picked, strict=True))
        non_default = [m for m in picked if not m.is_default]
        label = _join(sep, *[m.name for m in non_default])
        predicate = "TRUE" if not non_default else " and ".join(f"({m.sql})" for m in non_default)
        combos.append(Combo(members=members, label=label, predicate=predicate))
    return combos


class _Namer:
    """Allocates identifiers and refuses to let two features share a name."""

    def __init__(self, sep: str) -> None:
        self.sep = sep
        self._seen: dict[str, str] = {}
        self.warnings: list[str] = []

    def claim(self, name: str, origin: str) -> str:
        if name in self._seen:
            raise SpecError(
                f"generated name collision on {name!r}:\n"
                f"    already produced by: {self._seen[name]}\n"
                f"    also produced by:    {origin}\n"
                "Rename a condition member or an atomic field so the expansion stays unique."
            )
        if len(name) > MAX_IDENTIFIER_LEN:
            raise SpecError(
                f"generated column {name!r} is {len(name)} characters, over the "
                f"{MAX_IDENTIFIER_LEN}-character limit enforced by Snowflake. Shorten the "
                "condition member names it is built from."
            )
        if len(name) > WARN_IDENTIFIER_LEN:
            self.warnings.append(
                f"column {name!r} is {len(name)} characters; consider shorter member names"
            )
        self._seen[name] = origin
        return name


def _describe(agg: str, field: str, combo: Combo, window: TimeWindow, convention: str) -> str:
    if window.is_all_time:
        span = "over all history up to and including the as-of date"
    elif convention == "inclusive":
        span = f"over the {window.days} days ending on the as-of date (inclusive)"
    else:
        span = f"over the {window.days} days ending the day before the as-of date"
    return f"{agg} of {field} for {combo.describe()} {span}"


def build_plan(spec: FeatureSpec) -> FeaturePlan:
    sep = spec.settings.separator
    plan = FeaturePlan(spec=spec)
    namer = _Namer(sep)

    partials: dict[str, PartialColumn] = {}
    stored: dict[str, StoredFeature] = {}

    def ensure_partial(
        field_name: str,
        agg_key: str,
        combo: Combo,
        dtype_source: str | None,
        distinct_method: str,
        public: bool,
    ) -> PartialColumn:
        name = _join(sep, "p", agg_key, field_name, combo.label)
        existing = partials.get(name)
        if existing is not None:
            if public:
                existing.internal = False
            return existing
        agg = get_aggregate(agg_key, distinct_method, kmv_k=spec.settings.kmv_k)
        namer.claim(name, f"partial for {agg_key}({field_name}) [{combo.describe()}]")
        col = PartialColumn(
            name=name,
            dtype=agg.partial_dtype(dtype_source),
            agg_key=agg_key,
            field=field_name,
            combo=combo,
            value_sql=field_name,
            distinct_method=distinct_method,
            aggregate=agg,
            internal=not public,
        )
        partials[name] = col
        plan.partials.append(col)
        return col

    def ensure_stored(
        partial: PartialColumn,
        window: TimeWindow,
        dtype_source: str | None,
        public: bool,
    ) -> StoredFeature:
        name = _join(sep, partial.agg_key, partial.field, partial.combo.label, window.name)
        existing = stored.get(name)
        if existing is not None:
            if public:
                existing.internal = False
            return existing
        namer.claim(
            name, f"{partial.agg_key}({partial.field}) [{partial.combo.describe()}] {window.name}"
        )
        feat = StoredFeature(
            name=name,
            partial=partial,
            window=window,
            dtype=partial.aggregate.feature_dtype(dtype_source),
            description=_describe(
                partial.agg_key,
                partial.field,
                partial.combo,
                window,
                spec.settings.window_convention,
            ),
            internal=not public,
        )
        stored[name] = feat
        plan.stored.append(feat)
        return feat

    by_name = {f.name: f for f in spec.fields}

    for field in spec.fields:
        combos = build_combos(spec, field, sep)

        # ---- days_since derivations -----------------------------------------
        # days_since is monotonically decreasing in event time, so its minimum
        # over a window is the distance to the LATEST event and its maximum is
        # the distance to the EARLIEST. Aggregating the underlying timestamp and
        # flipping here keeps the partial layer independent of the as-of date,
        # which is the whole reason partials can be reused across runs.
        if field.is_derived:
            base_name = field.derived.from_field
            base_field = by_name.get(base_name)
            base_dtype = base_field.field_type if base_field else "timestamp"
            flip = {"min": "max", "max": "min"}
            for agg_key in field.aggs:
                base_agg = flip[agg_key]
                for combo in combos:
                    base_partial = ensure_partial(
                        base_name, base_agg, combo, base_dtype, "exact", public=False
                    )
                    base_stored = ensure_stored(base_partial, spec.windows[0], base_dtype, False)
                    for window in spec.windows:
                        base_stored = ensure_stored(base_partial, window, base_dtype, False)
                        name = namer.claim(
                            _join(sep, agg_key, field.name, combo.label, window.name),
                            f"{agg_key}({field.name}) derived [{combo.describe()}] {window.name}",
                        )
                        plan.computed.append(
                            ComputedFeature(
                                name=name,
                                window=window,
                                dtype="bigint",
                                expr=macro(
                                    "fs_datediff_day",
                                    Expr.sql(base_stored.name),
                                    Expr.jinja("fs_target_date()"),
                                ),
                                inputs=(base_stored.name,),
                                description=_describe(
                                    agg_key,
                                    field.name,
                                    combo,
                                    window,
                                    spec.settings.window_convention,
                                )
                                + f" (whole calendar days from {base_stored.name} to the as-of date)",
                                kind="days_since",
                                agg_key=agg_key,
                                field=field.name,
                                combo=combo,
                            )
                        )
                        plan.output_order.append(name)
            continue

        # ---- ordinary and desugared aggregations ----------------------------
        for agg_key in field.aggs:
            for combo in combos:
                if agg_key == "avg":
                    # avg is not a monoid on its own; carry sum and count and
                    # divide at publish time.
                    p_sum = ensure_partial(
                        field.name, "sum", combo, field.field_type, "exact", public=False
                    )
                    p_cnt = ensure_partial(
                        field.name, "count", combo, field.field_type, "exact", public=False
                    )
                    for window in spec.windows:
                        s_sum = ensure_stored(p_sum, window, field.field_type, False)
                        s_cnt = ensure_stored(p_cnt, window, field.field_type, False)
                        name = namer.claim(
                            _join(sep, "avg", field.name, combo.label, window.name),
                            f"avg({field.name}) [{combo.describe()}] {window.name}",
                        )
                        plan.computed.append(
                            ComputedFeature(
                                name=name,
                                window=window,
                                dtype="double",
                                expr=Expr.sql(
                                    f"case when {s_cnt.name} = 0 then null "
                                    f"else {s_sum.name} / cast({s_cnt.name} as double) end"
                                ),
                                inputs=(s_sum.name, s_cnt.name),
                                description=_describe(
                                    "avg",
                                    field.name,
                                    combo,
                                    window,
                                    spec.settings.window_convention,
                                ),
                                kind="avg",
                                agg_key="avg",
                                field=field.name,
                                combo=combo,
                            )
                        )
                        plan.output_order.append(name)
                    continue

                partial = ensure_partial(
                    field.name, agg_key, combo, field.field_type, field.distinct_method, public=True
                )
                for window in spec.windows:
                    feat = ensure_stored(partial, window, field.field_type, public=True)
                    plan.output_order.append(feat.name)

    plan.warnings = namer.warnings  # type: ignore[attr-defined]
    return plan
