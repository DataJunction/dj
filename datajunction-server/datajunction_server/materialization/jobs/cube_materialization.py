"""
Cube materialization jobs
"""
from datajunction_server.materialization.jobs.materialization_job import (
    MaterializationJob,
)
from datajunction_server.models import NodeRevision
from datajunction_server.models.engine import Dialect
from datajunction_server.models.materialization import (
    DruidCubeConfig,
    DruidMaterializationInput,
    Materialization,
    MaterializationInfo,
)
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.parsing import ast
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.utils import amenable_name


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


class DruidCubeMaterializationJob(MaterializationJob):
    """
    Druid materialization for a cube node.
    """

    dialect = Dialect.DRUID

    def schedule(
        self,
        materialization: Materialization,
        query_service_client: QueryServiceClient,
    ) -> MaterializationInfo:
        """
        Use the query service to kick off the materialization setup.
        """
        cube_config = DruidCubeConfig.parse_obj(materialization.config)
        druid_spec = cube_config.build_druid_spec(
            materialization.node_revision,
        )
        measures_temporal_partition = cube_config.temporal_partition(
            materialization.node_revision,
        )
        categorical_partitions = cube_config.categorical_partitions(
            materialization.node_revision,
        )
        final_query = build_materialization_query(
            cube_config.query,
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
                # Cube materialization involves creating an intermediate dataset,
                # which will have measures columns for all metrics in the cube
                columns=cube_config.columns,
                partitions=measures_temporal_partition + categorical_partitions,
                job=materialization.job,
                strategy=materialization.strategy,
            ),
        )


def build_materialization_query(
    base_cube_query: str,
    node_revision: NodeRevision,
) -> ast.Query:
    """
    Build materialization query (based on configured temporal partitions).
    """
    cube_materialization_query_ast = parse(base_cube_query)
    temporal_partitions = node_revision.temporal_partition_columns()
    temporal_partition_col = [
        col
        for col in cube_materialization_query_ast.select.projection
        if col.alias_or_name.name == amenable_name(temporal_partitions[0].name)  # type: ignore
    ]

    final_query = ast.Query(
        select=ast.Select(
            projection=[
                ast.Column(name=ast.Name(col.alias_or_name.name))  # type: ignore
                for col in cube_materialization_query_ast.select.projection
            ],
            where=ast.BinaryOp(
                left=ast.Column(
                    name=ast.Name(temporal_partition_col[0].alias_or_name.name),  # type: ignore
                ),
                right=temporal_partitions[0].partition.temporal_expression(),
                op=ast.BinaryOpKind.Eq,
            ),
        ),
        ctes=cube_materialization_query_ast.ctes,
    )
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
