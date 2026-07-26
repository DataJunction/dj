"""
Internal pre-aggregation logic shared by the API layer and the deployment
orchestrator.

The reusable core lives here (rather than in ``api/preaggregations.py``) so that
``internal`` code -- notably the deployment orchestrator's reconcile step -- can
call it without importing from the ``api`` layer, keeping the dependency
direction api -> internal.
"""

from typing import Optional, cast

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from datajunction_server.api.helpers import get_catalog_by_name
from datajunction_server.construction.build_v3.builder import build_measures_sql
from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.preaggregation import (
    PreAggregation,
    compute_expression_hash,
    compute_grain_group_hash,
    compute_preagg_hash,
)
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.models.decompose import PreAggMeasure
from datajunction_server.models.dialect import Dialect
from datajunction_server.models.materialization import MaterializationStrategy
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.preaggregation import ExternalPreAggTable
from datajunction_server.models.query import V3ColumnMetadata
from datajunction_server.service_clients import QueryServiceClient
from datajunction_server.sql.decompose import MetricComponentExtractor


def assert_column_type_compatible(
    *,
    subject: str,
    physical_column: str,
    expected_type: Optional[str],
    actual_type: object,
    table: ExternalPreAggTable,
) -> None:
    """
    Reject a mapping whose physical column type is incompatible with the element
    it backs (e.g. a SUM measure, or numeric dimension, bound to a string
    column). subject names the element in the error message.

    Conservative: both types are re-parsed to concrete ColumnType subclasses (the
    query service returns a bare type string is_compatible can't reason about),
    and the check is skipped on unknown/unparseable types, so int-vs-bigint never
    false-fails.
    """
    from datajunction_server.sql.parsing.backends.antlr4 import parse_rule
    from datajunction_server.sql.parsing.types import ColumnType

    if expected_type is None:  # pragma: no cover - defensive
        return
    try:
        expected = cast(ColumnType, parse_rule(str(expected_type), "dataType"))
        actual = cast(ColumnType, parse_rule(str(actual_type), "dataType"))
    except Exception:  # pragma: no cover - defensive
        return
    if not expected.is_compatible(actual):
        raise DJInvalidInputException(
            message=(
                f"Column '{physical_column}' (type {actual}) in table "
                f"{table.catalog}.{table.schema_}.{table.table} is not "
                f"type-compatible with {subject} (expected {expected})."
            ),
        )


async def register_external_preaggregations(
    session: AsyncSession,
    query_service_client: QueryServiceClient,
    request_headers: dict[str, str],
    *,
    name: Optional[str],
    metrics: list[str],
    dimensions: list[str],
    table: ExternalPreAggTable,
    measure_columns: dict[str, str],
    dimension_columns: dict[str, str] | None = None,
) -> list[PreAggregation]:
    """
    Core logic for adopting an externally-built pre-aggregation table.

    Decomposes ``metrics`` into component measures, binds each to a physical
    column via ``measure_columns``, validates them against ``table``, and upserts
    the pre-aggregation(s) marked ``EXTERNAL``. Flushes but does NOT commit — the
    caller owns the transaction (the endpoint commits; the deploy orchestrator
    commits its whole plan). Callers must ensure ``query_service_client`` is
    configured. Returns the created/updated pre-aggregations.
    """
    # 1. Validate each mapped metric is a measure, and map its single component's
    #    expression hash to the declared physical column.
    measure_hash_to_column: dict[str, str] = {}
    for metric_name, physical_column in measure_columns.items():
        node = await Node.get_by_name(
            session,
            metric_name,
            options=[
                selectinload(Node.current).options(
                    *NodeRevision.default_load_options(),
                ),
            ],
            raise_if_not_exists=False,
        )
        if not node or node.type != NodeType.METRIC:
            raise DJInvalidInputException(
                message=f"'{metric_name}' in measure_columns is not a metric node.",
            )
        if not node.current.is_measure:
            raise DJInvalidInputException(
                message=(
                    f"Metric '{metric_name}' is not a measure: its query is not a "
                    f"single aggregation that maps 1:1 to a column. Composite "
                    f"metrics (e.g. AVG, ratios) must be modelled as derived "
                    f"metrics over their base measures."
                ),
            )
        components, _ = await MetricComponentExtractor(node.current.id).extract(session)
        # is_measure guarantees exactly one component.
        measure_hash_to_column[compute_expression_hash(components[0].expression)] = (
            physical_column
        )

    # 2. Decompose the requested metrics into grain groups (no SQL is executed).
    measures_result = await build_measures_sql(
        session=session,
        metrics=metrics,
        dimensions=dimensions,
        dialect=Dialect.SPARK,
        use_materialized=False,
    )

    # 3. Introspect the external table and confirm the declared columns exist.
    catalog = await get_catalog_by_name(session=session, name=table.catalog)
    table_columns = await query_service_client.get_columns_for_table(
        catalog.name,
        table.schema_,
        table.table,
        request_headers,
        catalog.engines[0] if catalog.engines else None,
    )
    dimension_columns = dimension_columns or {}
    unknown_dims = sorted(set(dimension_columns) - set(dimensions))
    if unknown_dims:
        raise DJInvalidInputException(
            message=(
                f"dimension_columns references {unknown_dims}, which are not in "
                f"the pre-aggregation's dimensions {sorted(dimensions)}."
            ),
        )
    table_columns_by_name = {col.name: col.type for col in table_columns}
    missing_columns = sorted(
        column
        for column in (*measure_columns.values(), *dimension_columns.values())
        if column not in table_columns_by_name
    )
    if missing_columns:
        raise DJInvalidInputException(
            message=(
                f"Columns {missing_columns} declared in measure_columns/"
                f"dimension_columns were not found in table "
                f"{table.catalog}.{table.schema_}.{table.table}."
            ),
        )

    # 4. For each grain group: verify measure coverage, bind source columns,
    #    and upsert the pre-aggregation.
    created_preaggs: list[PreAggregation] = []
    for grain_group in measures_result.grain_groups:
        parent_node = measures_result.ctx.nodes.get(grain_group.parent_name)
        if not parent_node or not parent_node.current:  # pragma: no cover
            continue
        node_revision_id = parent_node.current.id
        grain_columns = list(measures_result.requested_dimensions)

        # Expected measure types, keyed by measure name, for the physical-column
        # type-compatibility check below.
        measure_types = {
            col.name: col.type
            for col in grain_group.columns
            if col.semantic_type in ("metric_component", "measure", "metric")
        }

        grain_measures: list[PreAggMeasure] = []
        for component in grain_group.components:
            expr_hash = compute_expression_hash(component.expression)
            if expr_hash not in measure_hash_to_column:
                raise DJInvalidInputException(
                    message=(
                        f"Measure '{component.name}' required by the requested "
                        f"metrics is not covered by measure_columns. Add the "
                        f"is_measure metric it corresponds to."
                    ),
                )
            physical_column = measure_hash_to_column[expr_hash]
            measure_name = grain_group.component_aliases.get(
                component.name,
                component.name,
            )
            assert_column_type_compatible(
                subject=f"measure '{component.name}'",
                physical_column=physical_column,
                expected_type=measure_types.get(measure_name)
                or measure_types.get(component.name),
                actual_type=table_columns_by_name[physical_column],
                table=table,
            )
            grain_measures.append(
                PreAggMeasure(
                    **{
                        **component.model_dump(),
                        "name": measure_name,
                    },
                    expr_hash=expr_hash,
                    source_column=physical_column,
                ),
            )

        # Bind dimension columns to physical columns via dimension_columns, keyed
        # by dimension reference. Measures carry their binding on PreAggMeasure.
        columns = []
        for col in grain_group.columns:
            source_column = None
            if col.semantic_type == "dimension":
                source_column = dimension_columns.get(col.semantic_name)
                if source_column is not None:
                    assert_column_type_compatible(
                        subject=f"dimension '{col.semantic_name}'",
                        physical_column=source_column,
                        expected_type=col.type,
                        actual_type=table_columns_by_name[source_column],
                        table=table,
                    )
            columns.append(
                V3ColumnMetadata(
                    name=col.name,
                    type=col.type,
                    semantic_type=col.semantic_type,
                    semantic_name=col.semantic_name,
                    source_column=source_column,
                ),
            )
        grain_group_hash = compute_grain_group_hash(node_revision_id, grain_columns)
        existing = await PreAggregation.find_matching(
            session=session,
            node_revision_id=node_revision_id,
            grain_columns=grain_columns,
            measure_expr_hashes={m.expr_hash for m in grain_measures if m.expr_hash},
        )
        if existing:
            existing.measures = grain_measures
            existing.columns = columns
            existing.sql = grain_group.sql
            existing.strategy = MaterializationStrategy.EXTERNAL
            existing.name = name
            preagg = existing
        else:
            preagg = PreAggregation(
                node_revision_id=node_revision_id,
                grain_columns=grain_columns,
                measures=grain_measures,
                columns=columns,
                sql=grain_group.sql,
                grain_group_hash=grain_group_hash,
                preagg_hash=compute_preagg_hash(
                    node_revision_id,
                    grain_columns,
                    grain_measures,
                ),
                strategy=MaterializationStrategy.EXTERNAL,
                name=name,
            )
            session.add(preagg)
        created_preaggs.append(preagg)

        # 5. Set availability immediately when freshness is provided; otherwise
        #    leave it pending for the availability callback to report.
        if table.valid_through_ts is not None:
            availability = AvailabilityState(
                catalog=table.catalog,
                schema_=table.schema_,
                table=table.table,
                valid_through_ts=table.valid_through_ts,
            )
            session.add(availability)
            await session.flush()
            preagg.availability_id = availability.id

    await session.flush()
    return created_preaggs
