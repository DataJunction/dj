"""
Available materialization jobs.
"""

import abc
from typing import Dict, List, Optional

from datajunction_server.database.materialization import Materialization
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import (
    GenericMaterializationConfig,
    GenericMaterializationInput,
    MaterializationInfo,
    MaterializationStrategy,
)
from datajunction_server.models.partition import PartitionBackfill
from datajunction_server.naming import amenable_name
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import get_settings

settings = get_settings()


class MaterializationJob(abc.ABC):
    """
    Base class for a materialization job
    """

    dialect: Optional[Dialect] = None

    def __init__(self): ...

    def run_backfill(
        self,
        materialization: Materialization,
        partitions: List[PartitionBackfill],
        query_service_client: QueryServiceClient,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Kicks off a backfill based on the spec using the query service
        """
        return query_service_client.run_backfill(
            materialization.node_revision.name,
            materialization.node_revision.version,
            materialization.node_revision.type,
            materialization.name,  # type: ignore
            partitions,
            request_headers=request_headers,
        )

    @abc.abstractmethod
    def schedule(
        self,
        materialization: Materialization,
        query_service_client: QueryServiceClient,
    ) -> MaterializationInfo:
        """
        Schedules the materialization job, typically done by calling a separate service
        with the configured materialization parameters.
        """


class SparkSqlMaterializationJob(  # pragma: no cover
    MaterializationJob,
):
    """
    Spark SQL materialization job.
    """

    dialect = Dialect.SPARK

    def schedule(
        self,
        materialization: Materialization,
        query_service_client: QueryServiceClient,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Placeholder for the actual implementation.
        """
        generic_config = GenericMaterializationConfig.parse_obj(materialization.config)
        temporal_partitions = materialization.node_revision.temporal_partition_columns()
        query_ast = parse(
            generic_config.query.replace(
                settings.dj_logical_timestamp_format,
                "DJ_LOGICAL_TIMESTAMP()",
            ),
        )
        final_query = query_ast
        if (
            temporal_partitions
            and materialization.strategy == MaterializationStrategy.INCREMENTAL_TIME
        ):
            temporal_partition_col = [
                col
                for col in query_ast.select.projection
                if col.alias_or_name.name.endswith(temporal_partitions[0].name)  # type: ignore
            ]
            temporal_op = (
                ast.BinaryOp(
                    left=ast.Column(
                        name=ast.Name(
                            temporal_partition_col[0].alias_or_name.name,  # type: ignore
                        ),
                    ),
                    right=temporal_partitions[0].partition.temporal_expression(),
                    op=ast.BinaryOpKind.Eq,
                )
                if not generic_config.lookback_window
                else ast.Between(
                    expr=ast.Column(
                        name=ast.Name(
                            temporal_partition_col[0].alias_or_name.name,  # type: ignore
                        ),
                    ),
                    low=temporal_partitions[0].partition.temporal_expression(
                        interval=generic_config.lookback_window,
                    ),
                    high=temporal_partitions[0].partition.temporal_expression(),
                )
            )
            final_query = ast.Query(
                select=ast.Select(
                    projection=[
                        ast.Column(name=ast.Name(col.alias_or_name.name))  # type: ignore
                        for col in query_ast.select.projection
                    ],
                    from_=query_ast.select.from_,
                    where=temporal_op,
                ),
                ctes=query_ast.ctes,
            )

            categorical_partitions = (
                materialization.node_revision.categorical_partition_columns()
            )
            if categorical_partitions:
                categorical_partition_col = [
                    col
                    for col in final_query.select.projection
                    if col.alias_or_name.name  # type: ignore
                    == amenable_name(categorical_partitions[0].name)  # type: ignore
                ]
                categorical_op = ast.BinaryOp(
                    left=ast.Column(
                        name=ast.Name(
                            categorical_partition_col[0].alias_or_name.name,  # type: ignore
                        ),
                    ),
                    right=categorical_partitions[0].partition.categorical_expression(),
                    op=ast.BinaryOpKind.Eq,
                )
                final_query.select.where = ast.BinaryOp(
                    left=temporal_op,
                    right=categorical_op,
                    op=ast.BinaryOpKind.And,
                )

        result = query_service_client.materialize(
            GenericMaterializationInput(
                name=materialization.name,  # type: ignore
                node_name=materialization.node_revision.name,
                node_version=materialization.node_revision.version,
                node_type=materialization.node_revision.type.value,
                job=materialization.job,
                strategy=materialization.strategy,
                lookback_window=generic_config.lookback_window,
                schedule=materialization.schedule,
                query=str(final_query),
                upstream_tables=generic_config.upstream_tables,
                spark_conf=generic_config.spark.__root__
                if generic_config.spark
                else {},
                columns=generic_config.columns,
                partitions=(
                    generic_config.temporal_partition(materialization.node_revision)
                    + generic_config.categorical_partitions(
                        materialization.node_revision,
                    )
                ),
            ),
            request_headers=request_headers,
        )
        return result
