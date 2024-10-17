"""
Cube materialization jobs
"""
from typing import Dict, Optional

from datajunction_server.database.materialization import Materialization
from datajunction_server.database.node import NodeRevision
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.materialization.jobs.materialization_job import (
    MaterializationJob,
)
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import (
    DruidMaterializationInput,
    DruidMeasuresCubeConfig,
    DruidMetricsCubeConfig,
    MaterializationInfo,
    MaterializationStrategy,
)
from datajunction_server.naming import amenable_name
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse


class DefaultCubeMaterialization(
    MaterializationJob,
):  # pylint: disable=too-few-public-methods
    """
    Dummy job that is not meant to be executed but contains all the
    settings needed for to materialize a generic cube.
    """

    def schedule(
        self,
        materialization: Materialization,
        query_service_client: QueryServiceClient,
    ):
        """
        Since this is a settings-only dummy job, we do nothing in this stage.
        """
        return  # pragma: no cover


class DruidMaterializationJob(MaterializationJob):
    """
    Generic Druid materialization job, irrespective of measures or aggregation load.
    """

    config_class = None

    def schedule(
        self,
        materialization: Materialization,
        query_service_client: QueryServiceClient,
        request_headers: Optional[Dict[str, str]] = None,
    ) -> MaterializationInfo:
        """
        Use the query service to kick off the materialization setup.
        """
        if not self.config_class:
            raise DJInvalidInputException(  # pragma: no cover
                "The materialization job config class must be defined!",
            )
        cube_config = self.config_class.parse_obj(materialization.config)
        druid_spec = cube_config.build_druid_spec(
            materialization.node_revision,
        )
        temporal_partition = cube_config.temporal_partition(
            materialization.node_revision,
        )
        categorical_partitions = cube_config.categorical_partitions(
            materialization.node_revision,
        )
        final_query = build_materialization_query(
            cube_config.query,
            materialization,
            materialization.node_revision,
        )
        return query_service_client.materialize(
            DruidMaterializationInput(
                name=materialization.name,
                node_name=materialization.node_revision.name,
                node_version=materialization.node_revision.version,
                node_type=materialization.node_revision.type,
                schedule=materialization.schedule,
                query=str(final_query),
                spark_conf=cube_config.spark.__root__ if cube_config.spark else {},
                druid_spec=druid_spec,
                upstream_tables=cube_config.upstream_tables or [],
                columns=cube_config.columns,
                partitions=temporal_partition + categorical_partitions,
                job=materialization.job,
                strategy=materialization.strategy,
                lookback_window=cube_config.lookback_window,
            ),
            request_headers=request_headers,
        )


class DruidMetricsCubeMaterializationJob(DruidMaterializationJob, MaterializationJob):
    """
    Druid materialization (aggregations aka metrics) for a cube node.
    """

    config_class = DruidMetricsCubeConfig  # type: ignore


class DruidMeasuresCubeMaterializationJob(DruidMaterializationJob, MaterializationJob):
    """
    Druid materialization (measures) for a cube node.
    """

    dialect = Dialect.DRUID
    config_class = DruidMeasuresCubeConfig  # type: ignore


def build_materialization_query(
    base_cube_query: str,
    materialization: Materialization,
    node_revision: NodeRevision,
) -> ast.Query:
    """
    Build materialization query (based on configured temporal partitions).
    """
    cube_materialization_query_ast = parse(
        base_cube_query.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()"),
    )
    final_query = ast.Query(
        select=ast.Select(
            projection=[
                ast.Column(name=ast.Name(col.alias_or_name.name))  # type: ignore
                for col in cube_materialization_query_ast.select.projection
            ],
        ),
        ctes=cube_materialization_query_ast.ctes,
    )

    if materialization.strategy == MaterializationStrategy.INCREMENTAL_TIME:
        temporal_partitions = node_revision.temporal_partition_columns()
        temporal_partition_col = [
            col
            for col in cube_materialization_query_ast.select.projection
            if col.alias_or_name.name == amenable_name(temporal_partitions[0].name)  # type: ignore
        ]
        temporal_op = (
            ast.BinaryOp(
                left=ast.Column(
                    name=ast.Name(temporal_partition_col[0].alias_or_name.name),  # type: ignore
                ),
                right=temporal_partitions[0].partition.temporal_expression(),
                op=ast.BinaryOpKind.Eq,
            )
            if not materialization.config["lookback_window"]
            else ast.Between(
                expr=ast.Column(
                    name=ast.Name(
                        temporal_partition_col[0].alias_or_name.name,  # type: ignore
                    ),
                ),
                low=temporal_partitions[0].partition.temporal_expression(
                    interval=materialization.config["lookback_window"],
                ),
                high=temporal_partitions[0].partition.temporal_expression(),
            )
        )

        categorical_partitions = node_revision.categorical_partition_columns()
        if categorical_partitions:
            categorical_partition_col = [
                col
                for col in cube_materialization_query_ast.select.projection
                if col.alias_or_name.name  # type: ignore
                == amenable_name(categorical_partitions[0].name)  # type: ignore
            ]
            categorical_op = ast.BinaryOp(
                left=ast.Column(
                    name=ast.Name(categorical_partition_col[0].alias_or_name.name),  # type: ignore
                ),
                right=categorical_partitions[0].partition.categorical_expression(),
                op=ast.BinaryOpKind.Eq,
            )
            final_query.select.where = ast.BinaryOp(
                left=temporal_op,
                right=categorical_op,
                op=ast.BinaryOpKind.And,
            )
        else:
            final_query.select.where = temporal_op

    combiner_cte = ast.Query(select=cube_materialization_query_ast.select).set_alias(
        ast.Name("combiner_query"),
    )
    combiner_cte.parenthesized = True
    combiner_cte.as_ = True
    combiner_cte.parent = final_query
    combiner_cte.parent_key = "ctes"
    final_query.ctes += [combiner_cte]
    final_query.select.from_ = ast.From(
        relations=[ast.Relation(primary=ast.Table(name=ast.Name("combiner_query")))],
    )
    return final_query
