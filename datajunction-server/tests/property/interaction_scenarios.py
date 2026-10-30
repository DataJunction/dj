"""Bounded feature scenarios; data and formula references are independent of DJ."""

from dataclasses import dataclass
import operator

from hypothesis import strategies as st

from tests.property.scenario import METRIC_BY_DEFINITION, Scenario, values


@st.composite
def grouped_scenarios(draw):
    """A small complete dimension domain, with sparse, repeated fact rows."""
    groups = draw(st.integers(1, 3))
    dates = draw(st.integers(2, 5))
    dimensions = tuple(
        (g * dates + day, chr(97 + g), day)
        for g in range(groups)
        for day in range(dates)
    )
    rows = draw(
        st.lists(
            st.tuples(st.integers(0, len(dimensions) - 1), values, values),
            min_size=1,
            max_size=12,
        ),
    )
    return Scenario(
        dimension_rows=dimensions,
        fact_rows=tuple(rows),
        metrics=(METRIC_BY_DEFINITION["SUM(x)"], METRIC_BY_DEFINITION["SUM(x)"]),
        op="/",
        join_type="left",
        dimensions=("label", "bucket"),
        filters=(),
    )


@dataclass(frozen=True)
class Formula:
    """References point strictly backwards, so every generated graph is a DAG."""

    left: int
    op: str
    right: int

    def evaluate(self, parents):
        left, right = parents[self.left], parents[self.right]
        if left is None or right is None or (self.op == "/" and right == 0):
            return None
        return {
            "+": operator.add,
            "-": operator.sub,
            "*": operator.mul,
            "/": operator.truediv,
        }[self.op](left, right)


@st.composite
def formula_dags(draw):
    # Seed a diamond to guarantee shared dependencies, then vary depth/edges.
    nodes = [
        Formula(0, "+", 1),
        Formula(2, "*", 0),
        Formula(2, "/", 1),
        Formula(3, "+", 4),
    ]
    for index in range(6, draw(st.integers(7, 10))):
        nodes.append(
            Formula(
                index - 1,
                draw(st.sampled_from(["+", "-", "*", "/"])),
                draw(st.integers(0, index - 1)),
            ),
        )
    # The root always traverses both shared branches, including guarded division.
    nodes.append(Formula(3, "+", len(nodes) + 1))
    return tuple(nodes)


@st.composite
def time_scenarios(draw):
    """k is a partition, y a sequence position; both partitions have observations."""
    size = draw(st.integers(3, 7))
    rows = [(0, draw(values), 0), (1, draw(values), size - 1)]
    rows.extend(
        draw(
            st.lists(
                st.tuples(st.integers(0, 1), values, st.integers(0, size - 1)),
                max_size=10,
            ),
        ),
    )
    return size, tuple(rows)
