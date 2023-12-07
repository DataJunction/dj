"""Tests for /materialization api"""
from fastapi.testclient import TestClient


def test_materialization_info(client: TestClient) -> None:
    """
    Test ``GET /materialization/info``.
    """
    response = client.get("/materialization/info")
    data = response.json()

    assert response.status_code == 200
    assert data == {
        "job_types": [
            {
                "allowed_node_types": ["transform", "dimension", "cube"],
                "description": "Spark SQL materialization job",
                "job_class": "SparkSqlMaterializationJob",
                "label": "Spark SQL",
                "name": "spark_sql",
            },
            {
                "allowed_node_types": ["cube"],
                "description": "Used to materialize a cube to Druid for "
                "low-latency access to a set of metrics and "
                "dimensions. While the logical cube definition "
                "is at the level of metrics and dimensions, a "
                "materialized Druid cube will reference "
                "measures and dimensions, with rollup "
                "configured on the measures where appropriate.",
                "job_class": "DruidCubeMaterializationJob",
                "label": "Druid Cube",
                "name": "druid_cube",
            },
        ],
        "strategies": [
            {"label": "Full", "name": "full"},
            {"label": "Snapshot", "name": "snapshot"},
            {"label": "Snapshot Partition", "name": "snapshot_partition"},
            {"label": "Incremental Time", "name": "incremental_time"},
            {"label": "View", "name": "view"},
        ],
    }
