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


"""
Funnel: if you only materialize at cube level, pick full_snapshot.
Then choose filters as test=222
Start/end timestamp ranges from 2023-01-01 to 2023-05-01
This is your "filterset" on the cube.
Then when you materialize, DJ will create a table like:
hash(test_id_22222__start_dateint_20230101__end_dateint_20230501)
dj.{node_name}__{node_version}__{hash}

Alt: materialize at transform. This will have strategy of "incremental_time"
lookback_window=ms to lookback
Each run will write to a range of time partitions
from DJ_LOGICAL_TIMESTAMP() to DJ_LOGICAL_TIMESTAMP() - lookback_window
Still need to materialize at cube level, but cheaper?

Cube materialization:
    - Strategies: FULL, FULL_SNAPSHOT, INCREMENTAL_TIME, VIEW
    * Druid (outputs to a datasource in Druid)
        - FULL:
            - Can partition by time and one additional partition
            - Overwrite entire datasource each time
        - FULL_SNAPSHOT:
            - Can partition by time and one additional partition
            - Snapshots captured by filterset + timestamp
            - New datasource every time
        - INCREMENTAL_TIME:
            Can partition by temporal partitions
            For each materialization run, will write to a time partition in a single table.
            Time partition increments based on the processing timestamp
    * Spark (outputs to a table in data warehouse)
        - Partition by both categorical and temporal partitions
        - Snapshots captured by filterset + timestamp

Snapshot:
- name
- timestamp
- filters (if any, only for Druid)
- full table name (catalog, schema, table)
- vtts

Transform materialization:
    * Spark (outputs to a table in data warehouse)
        - Strategies: FULL, FULL_SNAPSHOT, INCREMENTAL_TIME, VIEW
        (single table)
        - FULL:
            Can partition by both categorical and temporal partitions.
            For each materialization run, will fully overwrite partitions in a single table.
        - INCREMENTAL_TIME:
            Can partition by temporal partitions
            For each materialization run, will write to a time partition in a single table.
            Time partition increments based on the processing timestamp

        (snapshot tables)
        - FULL_SNAPSHOT:
            If you have parameters other than DJ_LOGICAL_TIMESTAMP in your query, must choose this option.
            Can partition by both categorical and temporal partitions.
            For each materialization run, will write to a snapshot table, where the table name
            is determined by the processing timestamp

        (not materialized)
        - VIEW:
            Creates the view with a node-versioned name. Will not change the view unless the node
            version increments.
"""


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
                strategy=materialization.strategy,
                schedule=materialization.schedule,
                query=str(final_query),
                spark_conf=cube_config.spark.__root__ if cube_config.spark else {},
                druid_spec=druid_spec,
                upstream_tables=cube_config.upstream_tables or [],
                # Cube materialization involves creating an intermediate dataset,
                # which will have measures columns for all metrics in the cube
                columns=cube_config.columns,
                partitions=measures_temporal_partition + categorical_partitions,
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
