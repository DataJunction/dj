"""Tests materialization-related models."""
from datajunction_server.models.materialization import (
    GenericMaterializationConfig,
    Partition,
)


def test_generic_materialization_config():
    """
    Test that the generic materialization config's identifier generation only
    takes into account categorical values when generating the hash for partition
    values.
    """
    mat_config = GenericMaterializationConfig(
        query="SELECT 1",
        partitions=[
            Partition(
                name="date_int",
                type_="temporal",
                range=[20200101, 20200601],
            ),
            Partition(
                name="country",
                type_="categorical",
                range=["MY"],
            ),
        ],
    )
    assert mat_config.identifier() == "date_int_country_1634115696"

    mat_config = GenericMaterializationConfig(
        query="SELECT 1",
        partitions=[
            Partition(
                name="date_int",
                type_="temporal",
                range=[20200101, 20200601],
            ),
        ],
    )
    assert mat_config.identifier() == "date_int_0"

    mat_config = GenericMaterializationConfig(
        query="SELECT 1",
        partitions=[
            Partition(
                name="country",
                type_="categorical",
                range=["MY"],
            ),
        ],
    )
    assert mat_config.identifier() == "country_1634115696"
