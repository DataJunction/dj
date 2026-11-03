"""Every reference must bind through its explicit role before rewriting."""

import pytest

from datajunction_server.construction.build_v3.cte import _rewrite_filter_for_cte
from datajunction_server.sql.parsing.backends.antlr4 import parse


@pytest.mark.parametrize(
    "predicate, bindings, expected",
    [
        (
            "ns.date.dateint[measure] = 1",
            {"ns.date.dateint[measure]": "utc_date"},
            "impressions.utc_date = 1",
        ),
        (
            "ns.date.dateint[allocation] = 1",
            {"ns.date.dateint[measure]": "utc_date"},
            None,
        ),
        (
            "ns.date.dateint[measure] >= 1 AND ns.date.dateint[allocation] = 2",
            {"ns.date.dateint[measure]": "utc_date"},
            None,
        ),
        (
            "ns.date.dateint = 1",
            {"ns.date.dateint": "utc_date"},
            "impressions.utc_date = 1",
        ),
        (
            "ns.date.dateint = 1 OR ns.product.category = 2",
            {"ns.date.dateint": "utc_date"},
            None,
        ),
    ],
)
def test_complete_predicate_binding(predicate, bindings, expected):
    rewritten = _rewrite_filter_for_cte(
        predicate,
        bindings,
        {"utc_date"},
        parse("SELECT impressions.utc_date FROM ns.impressions AS impressions"),
    )
    assert (str(rewritten) if rewritten is not None else None) == expected
