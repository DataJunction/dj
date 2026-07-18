"""
Tests for ``get_node_column``, in particular its handling of role-playing
dimension columns that share a bare ``name`` but differ by ``dimension_column``
(the role suffix).
"""

from types import SimpleNamespace

import pytest

from datajunction_server.database.column import Column
from datajunction_server.errors import (
    DJDoesNotExistException,
    DJInvalidInputException,
)
from datajunction_server.internal.nodes import get_node_column


def _make_node(name: str, columns):
    """Lightweight stand-in exposing the attributes ``get_node_column`` reads."""
    return SimpleNamespace(name=name, current=SimpleNamespace(columns=columns))


def _role_playing_node():
    """
    A node with two columns sharing the bare name ``...dateint`` but reached via
    two different role-played dimensions, plus a plain non-role column.
    """
    epoch_date = Column(
        name="arc.content_migration.dt_date_d.dateint",
        type="int",
        dimension_column="[epoch_date]",
        order=0,
        attributes=[],
    )
    region_date = Column(
        name="arc.content_migration.dt_date_d.dateint",
        type="int",
        dimension_column="[region_date]",
        order=1,
        attributes=[],
    )
    plain = Column(name="event_id", type="int", order=2, attributes=[])
    node = _make_node(
        "arc.content_migration.cade_batch_funnel",
        [epoch_date, region_date, plain],
    )
    return node, epoch_date, region_date, plain


def test_get_node_column_role_qualified_resolves_specific_role():
    """Role-qualified identity targets the exact role-played column."""
    node, epoch_date, region_date, _ = _role_playing_node()

    assert (
        get_node_column(
            node,
            "arc.content_migration.dt_date_d.dateint[epoch_date]",
        )
        is epoch_date
    )
    assert (
        get_node_column(
            node,
            "arc.content_migration.dt_date_d.dateint[region_date]",
        )
        is region_date
    )


def test_get_node_column_ambiguous_bare_name_raises():
    """
    A bare name matching multiple role-played columns must raise (no silent
    last-wins), listing both role-qualified options.
    """
    node, epoch_date, region_date, _ = _role_playing_node()

    with pytest.raises(DJInvalidInputException) as exc_info:
        result = get_node_column(
            node,
            "arc.content_migration.dt_date_d.dateint",
        )
        # It must NOT silently pick either column.
        assert result not in (epoch_date, region_date)

    message = exc_info.value.message
    assert "ambiguous" in message
    assert "arc.content_migration.dt_date_d.dateint[epoch_date]" in message
    assert "arc.content_migration.dt_date_d.dateint[region_date]" in message


def test_get_node_column_unique_bare_name_backward_compatible():
    """A non-colliding bare name still resolves (backward compatible)."""
    node, _, _, plain = _role_playing_node()
    assert get_node_column(node, "event_id") is plain


def test_get_node_column_unknown_column_raises_not_found():
    """An unknown column raises the existing not-found exception/message."""
    node, _, _, _ = _role_playing_node()
    with pytest.raises(DJDoesNotExistException) as exc_info:
        get_node_column(node, "does_not_exist")
    assert exc_info.value.message == (
        "Column `does_not_exist` does not exist on node "
        "`arc.content_migration.cade_batch_funnel`!"
    )
