"""
Available materialization jobs.
"""
import abc
from typing import Optional

from dj.models.engine import Dialect
from dj.models.node import MaterializationConfig
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
    ):
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
    ):
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
    ):
        """
        Placeholder for the actual implementation.
        """
