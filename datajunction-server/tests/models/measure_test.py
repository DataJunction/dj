"""
Tests for ``datajunction_server.models.measures``.
"""

import datajunction_server.sql.parsing.types as ct
from datajunction_server.models.column import Column
from datajunction_server.models.measure import AggregationRule, Measure


def test_measures_backpopulate() -> None:
    """
    Test the Measure model and that it backpopulates Column and vice versa.
    """
    column1 = Column(name="finalized_amount", type=ct.StringType())
    column2 = Column(name="final_amount", type=ct.StringType())
    measure = Measure(
        name="amount",
        columns=[column1, column2],
        additive=AggregationRule.ADDITIVE,
    )
    assert column1.measure == measure
    assert column2.measure == measure

    column3 = Column(name="amount3", type=ct.StringType(), measure=measure)
    column4 = Column(name="amount4", type=ct.StringType(), measure=measure)
    assert measure.columns == [column1, column2, column3, column4]
