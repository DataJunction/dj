"""
Available materialization jobs.
"""
import abc
from typing import Optional

from dj.models.engine import Dialect
from dj.models.materialization import GenericMaterializationInput, MaterializationOutput
from dj.models.node import GenericMaterializationConfig, MaterializationConfig
from dj.service_clients import QueryServiceClient


class MaterializationJob(abc.ABC):  # pylint: disable=too-few-public-methods
    """
    Base class for a materialization job
    """

    dialect: Optional[Dialect] = None

    def __init__(self):
        ...

    @abc.abstractmethod
    def schedule(
        self,
        materialization: MaterializationConfig,
        query_service_client: QueryServiceClient,
    ) -> MaterializationOutput:
        """
        Schedules the materialization job, typically done by calling a separate service
        with the configured materialization parameters.
        """


class TrinoMaterializationJob(  # pylint: disable=too-few-public-methods # pragma: no cover
    MaterializationJob,
):
    """
    Trino materialization job. Left unimplemented for the time being.
    """

    dialect = Dialect.TRINO

    def schedule(
        self,
        materialization: MaterializationConfig,
        query_service_client: QueryServiceClient,
    ) -> MaterializationOutput:
        """
        Placeholder for the actual implementation.
        """


class SparkSqlMaterializationJob(  # pylint: disable=too-few-public-methods # pragma: no cover
    MaterializationJob,
):
    """
    Spark SQL materialization job. Left unimplemented for the time being.
    """

    dialect = Dialect.SPARK

    def schedule(
        self,
        materialization: MaterializationConfig,
        query_service_client: QueryServiceClient,
    ) -> MaterializationOutput:
        """
        Placeholder for the actual implementation.
        """
        generic_config = GenericMaterializationConfig.parse_obj(materialization.config)
        return query_service_client.materialize(
            GenericMaterializationInput(
                name=materialization.name,  # type: ignore
                node_name=materialization.node_revision.name,
                node_type=materialization.node_revision.type.value,
                schedule=materialization.schedule,
                query=generic_config.query,
                upstream_tables=generic_config.upstream_tables,
                spark_conf=materialization.config["spark"],
                partitions=[
                    partition.dict() for partition in generic_config.partitions
                ],
            ),
        )
