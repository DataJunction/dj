"""Small, immutable cases shared by the graph and materialization properties."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal, TypeAlias
from collections.abc import Callable

from hypothesis import strategies as st

DimensionColumn: TypeAlias = Literal["label", "bucket"]
DimensionRow: TypeAlias = tuple[int, str | None, int | None]
FactRow: TypeAlias = tuple[int | None, int | None, int | None]
FilterValue: TypeAlias = str | int


@dataclass(frozen=True)
class MetricSpec:
    """DJ definition and separately written raw-table reference expression."""

    definition: str
    reference: str
    compound_combiner: bool = False


# The reference expressions are explicit. Deriving them by editing DJ SQL would
# let a qualification error in the test's oracle hide a DJ error.
# Sample variance/stddev and covariance/correlation remain in fixed known-bug
# repros until their decomposition and printing issues are resolved.
METRICS = (
    MetricSpec("SUM(x)", "SUM(f.x)"),
    MetricSpec("COUNT(x)", "COUNT(f.x)"),
    MetricSpec("COUNT(*)", "COUNT(*)"),
    MetricSpec("MIN(x)", "MIN(f.x)"),
    MetricSpec("MAX(x)", "MAX(f.x)"),
    MetricSpec("AVG(x)", "AVG(f.x)", compound_combiner=True),
    MetricSpec("VAR_POP(x)", "VAR_POP(f.x)", compound_combiner=True),
    MetricSpec("STDDEV_POP(x)", "STDDEV_POP(f.x)"),
    MetricSpec("COUNT(DISTINCT x)", "COUNT(DISTINCT f.x)"),
    MetricSpec("SUM(x + y)", "SUM(f.x + f.y)"),
    MetricSpec(
        "SUM(CASE WHEN x > 0 THEN x END)",
        "SUM(CASE WHEN f.x > 0 THEN f.x END)",
    ),
    MetricSpec("AVG(x) * 2", "AVG(f.x) * 2", compound_combiner=True),
    MetricSpec(
        "SUM(x) / COUNT(*)",
        "SUM(f.x) / COUNT(*)",
        compound_combiner=True,
    ),
    MetricSpec(
        "MAX(x) - MIN(y)",
        "MAX(f.x) - MIN(f.y)",
        compound_combiner=True,
    ),
)
METRIC_BY_DEFINITION = {metric.definition: metric for metric in METRICS}
# Derived metrics currently lose parentheses around compound combiners.
SIMPLE_METRICS = tuple(metric for metric in METRICS if not metric.compound_combiner)


@dataclass(frozen=True)
class FilterAtom:
    column: DimensionColumn
    operator: Literal["=", "<>", ">", "IS NULL", "IS NOT NULL", "IN"]
    value: FilterValue | tuple[FilterValue, ...] | None = None


@dataclass(frozen=True)
class OrFilter:
    left: FilterSpec
    right: FilterSpec


@dataclass(frozen=True)
class NotFilter:
    inner: FilterSpec


FilterSpec: TypeAlias = FilterAtom | OrFilter | NotFilter


def _literal(value: FilterValue) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def render_filter(
    filter_: FilterSpec,
    column_ref: Callable[[DimensionColumn], str],
) -> str:
    """Render a predicate with DJ names or physical table aliases."""
    if isinstance(filter_, OrFilter):
        return (
            f"{render_filter(filter_.left, column_ref)} OR "
            f"{render_filter(filter_.right, column_ref)}"
        )
    if isinstance(filter_, NotFilter):
        return f"NOT ({render_filter(filter_.inner, column_ref)})"

    column = column_ref(filter_.column)
    if filter_.operator in ("IS NULL", "IS NOT NULL"):
        return f"{column} {filter_.operator}"
    if filter_.operator == "IN":
        assert isinstance(filter_.value, tuple)
        values = ", ".join(_literal(value) for value in filter_.value)
        return f"{column} IN ({values})"
    assert isinstance(filter_.value, (str, int))
    return f"{column} {filter_.operator} {_literal(filter_.value)}"


def filter_columns(filter_: FilterSpec) -> set[DimensionColumn]:
    """Dimension columns a predicate needs to remain available after rollup."""
    if isinstance(filter_, OrFilter):
        return filter_columns(filter_.left) | filter_columns(filter_.right)
    if isinstance(filter_, NotFilter):
        return filter_columns(filter_.inner)
    return {filter_.column}


@dataclass(frozen=True)
class Scenario:
    dimension_rows: tuple[DimensionRow, ...]
    fact_rows: tuple[FactRow, ...]
    metrics: tuple[MetricSpec, MetricSpec]
    op: Literal["+", "-", "*", "/"]
    join_type: Literal["left", "inner"]
    dimensions: tuple[DimensionColumn, ...]
    filters: tuple[FilterSpec, ...]
    preagg_grain: tuple[DimensionColumn, ...] | None = None


DIMENSION_COLUMNS: tuple[DimensionColumn, ...] = ("label", "bucket")
ATOMS = (
    FilterAtom("label", "=", "a"),
    FilterAtom("label", "<>", "b"),
    FilterAtom("label", "IS NULL"),
    FilterAtom("label", "IS NOT NULL"),
    FilterAtom("label", "IN", ("a", "c")),
    FilterAtom("bucket", "=", 1),
    FilterAtom("bucket", ">", 0),
    FilterAtom("bucket", "IS NULL"),
    FilterAtom("bucket", "IN", (0, 2)),
)

labels = st.one_of(st.none(), st.sampled_from(["a", "b", "c"]))
buckets = st.one_of(st.none(), st.integers(0, 2))
values = st.one_of(st.none(), st.integers(-5, 5))
atom = st.sampled_from(ATOMS)
filters = st.one_of(
    atom,
    st.tuples(atom, atom).map(lambda pair: OrFilter(*pair)),
    atom.map(NotFilter),
)
dimension_subsets = st.lists(
    st.sampled_from(DIMENSION_COLUMNS),
    unique=True,
    max_size=len(DIMENSION_COLUMNS),
).map(tuple)


@st.composite
def scenarios(draw, metric_pool=METRICS):
    n_dims = draw(st.integers(1, 3))
    return Scenario(
        dimension_rows=tuple(
            (key, draw(labels), draw(buckets)) for key in range(n_dims)
        ),
        fact_rows=tuple(
            draw(
                st.lists(
                    st.tuples(
                        st.one_of(st.none(), st.integers(0, n_dims)),
                        values,
                        values,
                    ),
                    max_size=10,
                ),
            ),
        ),
        metrics=draw(
            st.tuples(st.sampled_from(metric_pool), st.sampled_from(metric_pool)),
        ),
        op=draw(st.sampled_from(["+", "-", "*", "/"])),
        join_type=draw(st.sampled_from(["left", "inner"])),
        dimensions=draw(dimension_subsets),
        filters=tuple(draw(st.lists(filters, max_size=2))),
    )


@st.composite
def materialization_scenarios(draw):
    scenario = draw(scenarios())
    required = set(scenario.dimensions)
    for filter_ in scenario.filters:
        required.update(filter_columns(filter_))
    grain = tuple(
        column
        for column in DIMENSION_COLUMNS
        if column in required or draw(st.booleans())
    )
    return replace(scenario, preagg_grain=grain)
