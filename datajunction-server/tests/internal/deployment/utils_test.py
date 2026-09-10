"""
Tests for deployment graph utilities
"""

from datajunction_server.internal.deployment.utils import spec_column_names
from datajunction_server.models.deployment import CubeSpec, DimensionSpec


def _dimension(query: str) -> DimensionSpec:
    return DimensionSpec(
        name="default.us_state",
        description="US state dimension",
        query=query,
        primary_key=["state_id"],
        owners=["dj"],
    )


class TestSpecColumnNames:
    """The columns a spec names, for link targets the same push defines."""

    def test_projection_names(self):
        assert spec_column_names(
            _dimension("SELECT state_id, State_Name AS state_name FROM default.a"),
        ) == {"state_id", "state_name"}

    def test_wildcard_names_nothing(self):
        assert spec_column_names(_dimension("SELECT * FROM default.a")) is None

    def test_unnamed_projection(self):
        assert (
            spec_column_names(_dimension("SELECT state_id + 1 FROM default.a")) is None
        )

    def test_spec_without_a_query(self):
        assert (
            spec_column_names(
                CubeSpec(
                    name="default.repair_cube",
                    description="Repairs cube",
                    owners=["dj"],
                    metrics=["default.num_repair_orders"],
                    dimensions=["default.us_state.state_name"],
                ),
            )
            is None
        )
