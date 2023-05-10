"""
Cube materialization jobs
"""
import abc
from typing import Dict

from dj.models.node import DruidCubeConfig, MaterializationConfig
from dj.service_clients import QueryServiceClient


class MaterializationJob(abc.ABC):  # pylint: disable=too-few-public-methods
    """
    Base class for a materialization job
    """

    def __init__(self):
        ...

    @abc.abstractmethod
    def execute(
        self,
        materialization: MaterializationConfig,
        query_service_client: QueryServiceClient,
    ):
        """
        Kicks off the materialization job, typically done by calling a
        separate service's client with the materialization parameters.
        """


DRUID_AGG_MAPPING = {
    ("bigint", "sum"): "longSum",
    ("double", "sum"): "doubleSum",
    ("float", "sum"): "floatSum",
    ("double", "min"): "doubleMin",
    ("double", "max"): "doubleMax",
    ("float", "min"): "floatMin",
    ("float", "max"): "floatMax",
    ("bigint", "min"): "longMin",
    ("bigint", "max"): "longMax",
    ("bigint", "count"): "longSum",
    ("double", "count"): "longSum",
    ("float", "count"): "longSum",
}


class DefaultCubeMaterialization(
    MaterializationJob,
):  # pylint: disable=too-few-public-methods
    """
    Base job for a default cube materialization
    """

    def execute(
        self,
        materialization: MaterializationConfig,
        query_service_client: QueryServiceClient,
    ):
        """
        Kicks off the materialization job, typically done by calling a
        separate service's client with the materialization parameters.
        """
        return


class DruidCubeMaterialization(MaterializationJob):
    """
    Druid materialization of a cube node.
    """

    def build_druid_spec(self, cube_config: DruidCubeConfig, node_name: str) -> Dict:
        """
        Builds the Druid ingestion spec from a materialization config.
        """
        druid_datasource_name = (
            cube_config.prefix + node_name + cube_config.suffix
        )
        metrics_spec = [
            {
                "fieldName": measure["name"],
                "name": measure["name"],
                "type": DRUID_AGG_MAPPING[
                    (measure["type"].lower(), measure["agg"].lower())
                ],
            }
            for measure_group in cube_config.measures.values()
            for measure in measure_group
        ]
        metrics_spec = [
            dict(tup) for tup in {tuple(spec.items()) for spec in metrics_spec}
        ]
        druid_spec = {
            "dataSchema": {
                "dataSource": druid_datasource_name,
                "parser": {
                    "parseSpec": {
                        "format": cube_config.druid.parse_spec_format or "parquet",
                        "dimensionsSpec": {"dimensions": cube_config.dimensions},
                        "timestampSpec": {
                            "column": cube_config.druid.timestamp_column,
                            "format": "yyyyMMdd",
                        },
                    },
                },
                "metricsSpec": metrics_spec,
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": cube_config.druid.granularity,
                    "intervals": cube_config.druid.intervals,
                },
            },
        }
        return druid_spec

    def execute(
        self,
        materialization: MaterializationConfig,
        query_service_client: QueryServiceClient,
    ):
        """
        Use the query service to kick off the materialization setup.
        """
        cube_config = DruidCubeConfig.parse_obj(materialization.config)
        druid_spec = self.build_druid_spec(cube_config, materialization.node_revision.name)
        query_service_client.materialize_cube(
            node_name=materialization.node_revision.name,
            schedule=materialization.schedule,
            query=cube_config.query,
            spark_conf=cube_config.spark.__root__,
            druid_spec=druid_spec,
        )
