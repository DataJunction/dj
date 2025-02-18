"""
Integration tests to be run against the latest full demo datajunction environment
"""
# pylint: disable=protected-access

import logging
from time import sleep

import pytest

from datajunction import DJBuilder, models
from datajunction.compile import ColumnYAML
from datajunction.exceptions import DJClientException

_logger = logging.getLogger(__name__)


@pytest.mark.skipif("not config.getoption('integration')")
def test_integration():
    """
    Integration test
    """
    dj = DJBuilder()

    max_retries = 5
    initial_delay = 1  # seconds
    delay_factor = 5

    for attempt in range(max_retries):
        try:
            dj.basic_login(username="dj", password="dj")
            break
        except DJClientException:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (delay_factor**attempt)
            _logger.info(
                "Login attempt %s failed. Retrying in %d seconds...",
                attempt + 1,
                delay,
            )
            sleep(delay)

    dj._session.post("/catalogs", json={"name": "tpch"})
    dj._session.post("/engines", json={"name": "trino", "version": "451"})
    dj._session.post(
        "/catalogs/tpch/engines",
        json=[{"name": "trino", "version": "451"}],
    )
    dj.create_namespace("integration.tests")
    dj.create_namespace("integration.tests.trino")
    source = dj.create_source(
        name="integration.tests.source1",
        catalog="unknown",
        schema="db",
        table="tbl",
        display_name="Test Source with Columns",
        description="A test source node with columns",
        columns=[
            ColumnYAML(name="id", type="int"),
            ColumnYAML(name="name", type="string"),
            ColumnYAML(name="price", type="double"),
            ColumnYAML(name="created_at", type="timestamp"),
        ],
        primary_key=["id"],
        mode=models.NodeMode.PUBLISHED,
        update_if_exists=True,
    )
    assert source.name == "integration.tests.source1"
    dj.register_table(catalog="tpch", schema="sf1", table="orders")
    transform = dj.create_transform(
        name="integration.tests.trino.transform1",
        display_name="Filter to last 1000 records",
        description="The last 1000 purchases",
        mode=models.NodeMode.PUBLISHED,
        query=(
            "select custkey, totalprice, orderdate from "
            "source.tpch.sf1.orders order by orderdate desc limit 1000"
        ),
        update_if_exists=True,
    )
    assert transform.name == "integration.tests.trino.transform1"
    dimension = dj.create_dimension(
        name="integration.tests.trino.dimension1",
        display_name="Customer keys",
        description="All custkey values in the source table",
        mode=models.NodeMode.PUBLISHED,
        primary_key=[
            "id",
        ],
        tags=[],
        query="select custkey as id, 'attribute' as foo from source.tpch.sf1.orders",
        update_if_exists=True,
    )
    assert dimension.name == "integration.tests.trino.dimension1"
    dj._link_dimension_to_node(
        node_name="integration.tests.trino.transform1",
        column_name="custkey",
        dimension_name="integration.tests.trino.dimension1",
        dimension_column=None,
    )
    metric = dj.create_metric(
        name="integration.tests.trino.metric1",
        display_name="Total of last 1000 purchases",
        description="This is the total amount from the last 1000 purchases",
        mode=models.NodeMode.PUBLISHED,
        query="select sum(totalprice) from integration.tests.trino.transform1",
        update_if_exists=True,
    )
    assert metric.name == "integration.tests.trino.metric1"
    common_dimensions = dj.common_dimensions(
        metrics=["integration.tests.trino.metric1"],
    )
    assert len(common_dimensions) == 2
    assert set(
        [
            "integration.tests.trino.dimension1.id",
            "integration.tests.trino.dimension1.foo",
        ],
    ) == {attribute["name"] for attribute in common_dimensions}
    sql = dj.sql(
        metrics=["integration.tests.trino.metric1"],
        dimensions=["integration.tests.trino.dimension1.id"],
    )
    assert "SELECT" in sql
