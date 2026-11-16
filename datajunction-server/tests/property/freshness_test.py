"""Freshness eligibility is interval containment, including exact boundaries."""

from datetime import date, datetime, time, timedelta, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from datajunction_server.construction.build_v3.preagg_freshness import preagg_is_fresh
from tests.construction.build_v3.preagg_freshness_test import (
    DATE_GRAIN,
    configure,
    make_context,
    make_partitioned_revision,
    make_preagg,
)
from tests.property.budget import examples


def dateint(day):
    return int(day.strftime("%Y%m%d"))


@pytest.mark.parametrize("predicate", ["between", "comparisons", "reversed"])
@settings(max_examples=examples(40))
@given(
    start=st.dates(date(2020, 1, 1), date(2030, 1, 1)),
    width=st.integers(0, 60),
    low=st.integers(-2, 62),
    high=st.integers(0, 5),
)
def test_freshness_matches_covered_interval(mocker, predicate, start, width, low, high):
    configure(mocker, gating=True)
    end, query_start = start + timedelta(days=width), start + timedelta(days=low)
    query_end = query_start + timedelta(days=high)
    a, b = dateint(query_start), dateint(query_end)
    ref = DATE_GRAIN[0]
    filters = {
        "between": [f"{ref} BETWEEN {a} AND {b}"],
        "comparisons": [f"{ref} >= {a}", f"{ref} <= {b}"],
        "reversed": [f"{a} <= {ref}", f"{b} >= {ref}"],
    }[predicate]
    preagg = make_preagg(
        DATE_GRAIN,
        dateint(end),
        node_revision=make_partitioned_revision(),
        min_temporal_partition=[str(dateint(start))],
        max_temporal_partition=[str(dateint(end))],
    )
    assert preagg_is_fresh(make_context(filters), preagg) == (
        start <= query_start <= query_end <= end
    )


@settings(max_examples=examples(40))
@given(
    today=st.dates(date(2020, 1, 1), date(2030, 1, 1)),
    budget=st.integers(0, 10),
    lag=st.integers(0, 12),
)
def test_open_ended_freshness_obeys_staleness_budget(mocker, today, budget, lag):
    configure(mocker, gating=True, max_staleness=budget * 86400)
    covered = today - timedelta(days=lag)
    preagg = make_preagg(
        DATE_GRAIN,
        dateint(covered),
        node_revision=make_partitioned_revision(),
    )
    ctx = make_context([], datetime.combine(today, time(12), tzinfo=timezone.utc))
    assert preagg_is_fresh(ctx, preagg) == (lag <= budget)
