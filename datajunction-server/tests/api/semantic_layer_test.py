"""Unit + integration tests for the semantic-layer REST API.

The unit tests cover the pure logic — filter translation and value quoting —
without a database. The DB-backed endpoint tests (list / view / sql against a
seeded cube) use the standard ``client`` fixture over the testcontainers
Postgres harness.
"""

import re
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.responses import JSONResponse

from datajunction_server.api import semantic_layer as semantic_layer_api
from datajunction_server.api.semantic_layer import (
    MAX_ROW_LIMIT,
    FilterPayload,
    GroupLimitPayload,
    QueryPayload,
    ViewSummary,
    _arrow_type_name,
    _active_git_namespaces,
    _cube_summaries,
    _column_metadata,
    _dimensions_payload,
    _discovery_filter,
    _filter_to_sql,
    _git_location,
    _generated_column_arrow_type_name,
    _list_semantic_branches,
    _limit_to_ranked_groups,
    _build_ranked_groups_sql,
    _metrics_payload,
    _path_filter,
    _quote_value,
    _view_scopes,
    _view_payload,
)
from datajunction_server.construction.build_v3.group_others import (
    GroupOthers,
    _bucket_grain_group,
    apply_group_others,
    bucket_dimension_rows,
)
from datajunction_server.construction.build_v3.metrics import (
    _build_pre_agg_wrapper_cte,
    process_base_metrics,
)
from datajunction_server.construction.build_v3.types import (
    ColumnMetadata as V3ColumnMetadata,
    DecomposedMetricInfo,
    GeneratedSQL as V3GeneratedSQL,
    GrainGroupSQL,
)
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.errors import DJException
from datajunction_server.internal.access.authorization import AccessChecker
from datajunction_server.internal.namespaces import get_git_info_for_namespace
from datajunction_server.internal.sql import build_row_count_sql
from datajunction_server.models.access import (
    AccessDecision,
    ResourceType,
)
from datajunction_server.models.dialect import Dialect, DialectRegistry
from datajunction_server.models.decompose import (
    Aggregability,
    AggregationRule,
    MetricComponent,
)
from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregationFunction,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse
from datajunction_server.transpilation import SQLTranspilationPlugin
from datajunction_server.models.node_type import NodeType


class TestFilterToSql:
    def test_equality_quotes_string_value(self):
        flt = FilterPayload(column="ns.dim", operator="=", value="North")
        assert _filter_to_sql(flt) == "ns.dim = 'North'"

    def test_is_null_omits_value(self):
        flt = FilterPayload(column="ns.dim", operator="IS NULL")
        assert _filter_to_sql(flt) == "ns.dim IS NULL"

    def test_comparison_operator_passes_through(self):
        flt = FilterPayload(column="ns.metric", operator=">=", value=10)
        assert _filter_to_sql(flt) == "ns.metric >= 10"

    def test_unsupported_operator_rejected_400(self):
        flt = FilterPayload(column="ns.dim", operator="LIKE", value="x%")
        with pytest.raises(DJException) as exc:
            _filter_to_sql(flt)
        assert exc.value.http_status_code == 400

    def test_missing_column_rejected_400(self):
        flt = FilterPayload(column=None, operator="=", value="x")
        with pytest.raises(DJException) as exc:
            _filter_to_sql(flt)
        assert exc.value.http_status_code == 400

    def test_in_quotes_each_value(self):
        flt = FilterPayload(
            column="ns.dim",
            operator="IN",
            value=["North", "O'Brien"],
        )
        assert _filter_to_sql(flt) == "ns.dim IN ('North', 'O\\'Brien')"

    @pytest.mark.parametrize("value", [[], "North"])
    def test_collection_operator_requires_non_empty_collection(self, value):
        flt = FilterPayload(column="ns.dim", operator="IN", value=value)

        with pytest.raises(DJException) as exc:
            _filter_to_sql(flt)

        assert exc.value.http_status_code == 400
        assert "requires a non-empty list" in exc.value.message

    def test_between_requires_two_values(self):
        flt = FilterPayload(column="ns.dim", operator="between", value=[1, 10])
        assert _filter_to_sql(flt) == "ns.dim BETWEEN 1 AND 10"

        with pytest.raises(DJException) as exc:
            _filter_to_sql(
                FilterPayload(column="ns.dim", operator="between", value=[1]),
            )
        assert exc.value.http_status_code == 400


class TestQuoteValue:
    def test_none_is_null(self):
        assert _quote_value(None) == "NULL"

    def test_bool_renders_sql_literal(self):
        assert _quote_value(True) == "TRUE"
        assert _quote_value(False) == "FALSE"

    def test_number_is_bare(self):
        assert _quote_value(42) == "42"

    def test_string_escapes_single_quotes(self):
        # DJ's parser uses Spark's grammar (BACKSLASH escaping); a doubled ''
        # would mis-lex into two adjacent strings. So the correct literal is
        # backslash-escaped.
        assert _quote_value("O'Brien") == "'O\\'Brien'"


class TestSemanticViewPayloadTypes:
    def test_arrow_type_name_uses_standard_name_only(self):
        assert _arrow_type_name("bigint") == "int"
        assert _arrow_type_name("double") == "floating"
        assert _arrow_type_name("varchar(255)") == "utf8"
        assert _arrow_type_name("array<string>") == "list"
        assert _arrow_type_name("timestamp") == "timestamp"
        assert _arrow_type_name(None) is None

    @pytest.mark.parametrize("display_name", [None, ""])
    def test_view_payloads_use_null_for_missing_display_name(self, display_name):
        cube = SimpleNamespace(
            name="sem.sales_cube",
            display_name=display_name,
            columns=[],
            cube_node_metrics=[],
            cube_node_dimensions=[],
        )

        summary = ViewSummary(
            name=cube.name,
            display_name=display_name or None,
            uid=cube.name,
            features=[],
        )
        detail = _view_payload(cube)  # type: ignore[arg-type]

        assert summary.model_dump()["display_name"] is None
        assert detail.model_dump()["display_name"] is None

    def test_metric_and_dimension_payloads_use_cube_column_types(self):
        cube = SimpleNamespace(
            columns=[
                SimpleNamespace(
                    cube_element_name="sem.total_amount",
                    type="decimal(18,2)",
                    display_name="Total amount",
                    description="Gross sales amount",
                    unit={"kind": "currency", "code": "USD"},
                    attribute_names=lambda: ["certified"],
                ),
                SimpleNamespace(
                    cube_element_name="sem.region.region_id",
                    type="bigint",
                    display_name="Region ID",
                    description=None,
                    unit=None,
                    attribute_names=lambda: ["primary_key"],
                ),
                SimpleNamespace(
                    cube_element_name="sem.region.region_name[home]",
                    type="varchar(255)",
                    display_name="Home region name",
                    description=None,
                    unit=None,
                    attribute_names=lambda: [],
                ),
            ],
            cube_node_metrics=["sem.total_amount"],
            cube_node_dimensions=[
                "sem.region.region_id",
                "sem.region.region_name[home]",
            ],
        )

        metrics = _metrics_payload(cube)  # type: ignore[arg-type]
        dimensions = _dimensions_payload(cube)  # type: ignore[arg-type]

        assert [metric.model_dump() for metric in metrics] == [
            {
                "id": "sem.total_amount",
                "name": "total_amount",
                "type": "decimal",
                "definition": "sem.total_amount",
                "description": "Gross sales amount",
                "aggregation": "OTHER",
                "metadata": {
                    "display_name": "Total amount",
                    "semantic_type": "currency",
                    "unit": {"kind": "currency", "code": "USD"},
                    "attributes": ["certified"],
                    "format": {
                        "preset": "currency",
                        "precision": None,
                        "scale": None,
                    },
                    "filter": None,
                    "extensions": {
                        "superset": {"d3format": "$,.2f"},
                        "google_sheets": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "$#,##0.00",
                            },
                        },
                    },
                },
            },
        ]

        assert [dim.model_dump() for dim in dimensions] == [
            {
                "id": "sem.region.region_id",
                "name": "region_id",
                "type": "int",
                "definition": "sem.region.region_id",
                "description": None,
                "grain": None,
                "metadata": {
                    "display_name": "Region ID",
                    "semantic_type": "identifier",
                    "unit": None,
                    "attributes": ["primary_key"],
                    "format": None,
                    "filter": None,
                    "extensions": None,
                },
            },
            {
                "id": "sem.region.region_name[home]",
                "name": "region_name[home]",
                "type": "utf8",
                "definition": "sem.region.region_name[home]",
                "description": None,
                "grain": None,
                "metadata": {
                    "display_name": "Home region name",
                    "semantic_type": "category",
                    "unit": None,
                    "attributes": None,
                    "format": None,
                    "filter": None,
                    "extensions": None,
                },
            },
        ]

    def test_metric_and_dimension_payloads_fallback_when_type_is_unknown(self):
        cube = SimpleNamespace(
            columns=[
                SimpleNamespace(
                    cube_element_name="sem.total_amount",
                    type=None,
                    display_name="Total amount",
                ),
                SimpleNamespace(
                    cube_element_name="sem.region.region_name",
                    type="unknown_type",
                    display_name="Region name",
                ),
            ],
            cube_node_metrics=["sem.total_amount"],
            cube_node_dimensions=["sem.region.region_name"],
        )

        metrics = _metrics_payload(cube)  # type: ignore[arg-type]
        dimensions = _dimensions_payload(cube)  # type: ignore[arg-type]

        assert metrics[0].type == "floating"
        assert dimensions[0].type == "utf8"

    def test_metric_metadata_reads_portable_custom_metadata(self):
        metric_revision = SimpleNamespace(
            name="sem.total_amount",
            type=NodeType.METRIC,
            custom_metadata={
                "semantic_layer": {
                    "display_name": "Net revenue",
                    "semantic_type": "currency",
                    "unit": {"kind": "currency", "code": "EUR"},
                    "attributes": ["certified", "certified"],
                    "format": {"preset": "currency", "precision": 2},
                    "filter": {
                        "kind": "range",
                        "operators": [">", ">="],
                        "default_operator": ">=",
                    },
                    "extensions": {
                        "superset": {"d3format": "€,.2f"},
                        "google_sheets": {
                            "numberFormat": {
                                "type": "CURRENCY",
                                "pattern": "€#,##0.00",
                            },
                        },
                    },
                    "owner": "finance",
                },
            },
        )
        cube = SimpleNamespace(
            columns=[
                SimpleNamespace(
                    cube_element_name="sem.total_amount",
                    type="double",
                    display_name="Total amount",
                    description=None,
                    unit=None,
                    attribute_names=lambda: [],
                ),
            ],
            cube_elements=[SimpleNamespace(node_revision=metric_revision)],
            cube_node_metrics=["sem.total_amount"],
        )

        metadata = _metrics_payload(cube)[0].metadata

        assert metadata is not None
        assert metadata.model_dump(exclude_none=True) == {
            "display_name": "Net revenue",
            "semantic_type": "currency",
            "unit": {"kind": "currency", "code": "EUR"},
            "attributes": ["certified"],
            "format": {"preset": "currency", "precision": 2},
            "filter": {
                "kind": "range",
                "operators": [">", ">="],
                "default_operator": ">=",
            },
            "extensions": {
                "superset": {"d3format": "€,.2f"},
                "google_sheets": {
                    "numberFormat": {
                        "type": "CURRENCY",
                        "pattern": "€#,##0.00",
                    },
                },
                "datajunction": {"owner": "finance"},
            },
        }

    @pytest.mark.parametrize(
        ("format_metadata", "expected_extensions"),
        [
            (
                {"preset": "number", "precision": 0},
                {
                    "superset": {"d3format": ",.0f"},
                    "google_sheets": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0"},
                    },
                },
            ),
            (
                {"preset": "percentage"},
                {
                    "superset": {"d3format": ".2%"},
                    "google_sheets": {
                        "numberFormat": {"type": "PERCENT", "pattern": "0.00%"},
                    },
                },
            ),
            (
                {"preset": "smart_number"},
                {"superset": {"d3format": "SMART_NUMBER"}},
            ),
        ],
    )
    def test_explicit_portable_formats_generate_client_fallbacks(
        self,
        format_metadata,
        expected_extensions,
    ):
        column = SimpleNamespace(
            type="double",
            display_name="Value",
            unit=None,
            attribute_names=lambda: [],
        )

        metadata = _column_metadata(
            column,
            is_metric=True,
            raw_metadata={"format": format_metadata},
        )

        assert metadata is not None
        assert metadata.extensions == expected_extensions

    def test_explicit_client_formats_take_precedence_over_generated_values(self):
        column = SimpleNamespace(
            type="double",
            display_name="Revenue",
            unit={"kind": "currency", "code": "USD"},
            attribute_names=lambda: [],
        )

        metadata = _column_metadata(
            column,
            is_metric=True,
            raw_metadata={
                "format": {"preset": "currency", "precision": 0},
                "extensions": {
                    "superset": {"d3format": "EXPLICIT", "other": True},
                    "google_sheets": {
                        "numberFormat": {"type": "TEXT"},
                        "other": True,
                    },
                },
            },
        )

        assert metadata is not None
        assert metadata.extensions == {
            "superset": {"d3format": "EXPLICIT", "other": True},
            "google_sheets": {
                "numberFormat": {"type": "TEXT"},
                "other": True,
            },
        }

    def test_explicit_usd_unit_generates_default_currency_formats(self):
        column = SimpleNamespace(
            type="double",
            display_name="Revenue",
            unit=None,
            attribute_names=lambda: [],
        )

        metadata = _column_metadata(
            column,
            is_metric=True,
            raw_metadata={"unit": {"kind": "currency", "code": "USD"}},
        )

        assert metadata is not None
        assert metadata.format is not None
        assert metadata.format.preset == "currency"
        assert metadata.extensions == {
            "superset": {"d3format": "$,.2f"},
            "google_sheets": {
                "numberFormat": {
                    "type": "CURRENCY",
                    "pattern": "$#,##0.00",
                },
            },
        }

    def test_ambiguous_native_units_do_not_generate_client_formats(self):
        percentage = SimpleNamespace(
            type="double",
            display_name="Percentage",
            unit={"kind": "percentage"},
            attribute_names=lambda: [],
        )
        euros = SimpleNamespace(
            type="double",
            display_name="Revenue",
            unit={"kind": "currency", "code": "EUR"},
            attribute_names=lambda: [],
        )

        percentage_metadata = _column_metadata(percentage, is_metric=True)
        euro_metadata = _column_metadata(euros, is_metric=True)

        assert percentage_metadata is not None
        assert percentage_metadata.format is None
        assert percentage_metadata.extensions is None
        assert euro_metadata is not None
        assert euro_metadata.format is not None
        assert euro_metadata.format.preset == "currency"
        assert euro_metadata.extensions is None

    def test_metric_and_dimension_payloads_fallback_when_column_is_missing(self):
        cube = SimpleNamespace(
            columns=[],
            cube_node_metrics=["sem.total_amount"],
            cube_node_dimensions=["sem.region.region_name"],
        )

        metrics = _metrics_payload(cube)  # type: ignore[arg-type]
        dimensions = _dimensions_payload(cube)  # type: ignore[arg-type]

        assert metrics[0].type == "floating"
        assert dimensions[0].type == "utf8"

    def test_generated_sql_column_types_use_standard_names(self):
        column = SimpleNamespace(type="varchar(255)", semantic_type="dimension")

        assert _generated_column_arrow_type_name(column) == "utf8"

    def test_generated_sql_dimension_column_type_fallback(self):
        column = SimpleNamespace(type="unknown_type", semantic_type="dimension")

        assert _generated_column_arrow_type_name(column) == "utf8"

    def test_generated_sql_metric_column_type_fallback(self):
        column = SimpleNamespace(type=None, semantic_type="metric")

        assert _generated_column_arrow_type_name(column) == "floating"


def test_git_location_uses_root_and_actual_branch() -> None:
    root = NodeNamespace(
        namespace="demo.metrics",
        github_repo_path="team/metrics",
        default_branch="main",
    )
    branch = NodeNamespace(
        namespace="demo.metrics.feature",
        parent_namespace="demo.metrics",
        git_branch="feature/foo",
    )
    namespaces = {root.namespace: root, branch.namespace: branch}

    assert _git_location("demo.metrics", namespaces) == (
        "demo.metrics",
        "main",
        None,
    )
    assert _git_location("demo.metrics.feature.cubes", namespaces) == (
        "demo.metrics",
        "feature/foo",
        "demo.metrics.feature",
    )
    assert _git_location("plain.main", namespaces) is None

    # A configured repository without an effective branch has no view location.
    incomplete_root = NodeNamespace(
        namespace="demo.incomplete",
        github_repo_path="team/incomplete",
    )
    assert (
        _git_location(
            incomplete_root.namespace,
            {incomplete_root.namespace: incomplete_root},
        )
        is None
    )


def test_view_scopes_exclude_a_nested_independent_git_root() -> None:
    root = NodeNamespace(
        namespace="demo.metrics",
        github_repo_path="team/metrics",
        default_branch="main",
    )
    other_root = NodeNamespace(
        namespace="demo.metrics.other",
        github_repo_path="team/other",
        default_branch="main",
    )
    main_branch = NodeNamespace(
        namespace="demo.metrics.main",
        parent_namespace="demo.metrics",
        git_branch="main",
    )
    release_branch = NodeNamespace(
        namespace="demo.metrics.release",
        parent_namespace="demo.metrics",
        git_branch="release",
    )
    main_child = NodeNamespace(namespace="demo.metrics.main.cubes")
    namespaces = {
        row.namespace: row
        for row in (root, other_root, main_branch, main_child, release_branch)
    }

    assert _view_scopes("demo.metrics", "main", namespaces) == {
        "demo.metrics",
        "demo.metrics.main",
    }
    assert _view_scopes("demo.metrics", "release", namespaces) == {
        "demo.metrics.release",
    }


@pytest.mark.asyncio
async def test_git_info_ignores_deactivated_linked_parent(
    client: AsyncClient,
    session: AsyncSession,
) -> None:
    for namespace in ("orphan", "orphan.feature"):
        await _expect(await client.post(f"/namespaces/{namespace}"), 200, 201, 409)
    root = await NodeNamespace.get(session, "orphan")
    branch = await NodeNamespace.get(session, "orphan.feature")
    assert root and branch
    root.github_repo_path = "team/orphan"
    root.default_branch = "main"
    root.deactivated_at = datetime.now(UTC)
    branch.parent_namespace = root.namespace
    branch.git_branch = "feature"
    await session.commit()

    assert (
        await get_git_info_for_namespace(session, branch.namespace, active_only=True)
        is None
    )


@pytest.mark.parametrize(
    "configuration",
    [
        {"namespace": ""},
        {"branch": ""},
        {"namespace": 123},
    ],
)
def test_discovery_filter_rejects_invalid_selector(configuration) -> None:
    with pytest.raises(DJException, match="must be a non-empty string"):
        _discovery_filter(configuration)


@pytest.mark.parametrize("path", [[""], ["demo.metrics", ""], ["a", "b", "c"]])
def test_path_filter_rejects_invalid_selection(path) -> None:
    with pytest.raises(DJException, match="Invalid view discovery path"):
        _path_filter(path)


@pytest.mark.asyncio
async def test_empty_discovery_scopes_do_not_query() -> None:
    session = AsyncMock(spec=AsyncSession)
    assert await _cube_summaries(session, set()) == []
    assert await _cube_summaries(session, None, exact_namespaces=set()) == []
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    ("dialect", "quoted_alias"),
    [
        (Dialect.SPARK, "`COUNT`"),
        (Dialect.TRINO, '"COUNT"'),
        (Dialect.DRUID, '"COUNT"'),
        (Dialect.POSTGRES, '"COUNT"'),
        (Dialect.DUCKDB, '"COUNT"'),
    ],
)
def test_row_count_uses_native_ast_and_quotes_alias(
    dialect,
    quoted_alias,
    monkeypatch,
):
    monkeypatch.setitem(
        DialectRegistry._registry,
        dialect.value,
        SQLTranspilationPlugin,
    )
    generated = V3GeneratedSQL(
        query=parse("SELECT region_name FROM sales"),
        columns=[],
        dialect=dialect,
        cube_name="sem.sales_cube",
    )

    result = build_row_count_sql(generated)

    assert f"COUNT(*) AS {quoted_alias}" in result.sql
    assert ") AS semantic_query" in result.sql
    assert [(column.name, column.type) for column in result.columns] == [
        ("COUNT", "bigint"),
    ]
    assert result.cube_name == "sem.sales_cube"


@pytest.mark.asyncio
@pytest.mark.parametrize("override", [None, True])
async def test_group_limit_builds_metrics_used_by_ranking_filters(override):
    """Inherited and explicit HAVING filters can name a second metric."""
    having = FilterPayload(
        type="HAVING",
        column="sem.average_amount",
        operator=">",
        value=10,
    )
    group_limit = GroupLimitPayload(
        dimensions=["sem.region"],
        top=5,
        metric="sem.total_amount",
        filters=[having] if override else None,
    )
    payload = QueryPayload(
        metrics=["sem.total_amount"],
        dimensions=["sem.region"],
        filters=[] if override else [having],
        group_limit=group_limit,
    )
    cube = SimpleNamespace(
        cube_node_metrics=["sem.total_amount", "sem.average_amount"],
    )
    with patch(
        "datajunction_server.api.semantic_layer.generate_metrics_sql",
        new_callable=AsyncMock,
    ) as generate:
        generate.return_value = V3GeneratedSQL(
            query=parse("SELECT region FROM ranking"),
            columns=[
                V3ColumnMetadata("region", "sem.region", "string", "dimension"),
            ],
            dialect=Dialect.DUCKDB,
        )
        await _build_ranked_groups_sql(
            SimpleNamespace(),
            cube,
            payload,
            group_limit,
            Dialect.DUCKDB,
        )

    assert generate.await_args.kwargs["metrics"] == [
        "sem.total_amount",
        "sem.average_amount",
    ]
    assert generate.await_args.kwargs["filters"] == ["sem.average_amount > 10"]
    assert generate.await_args.kwargs["orderby"][0] == "sem.total_amount DESC"


@pytest.mark.asyncio
@pytest.mark.parametrize("group_others", [False, True])
@pytest.mark.parametrize("ranked_dimensions", [[], ["sem.country", "sem.day"]])
async def test_group_limit_rejects_ranking_at_a_different_grain(
    ranked_dimensions,
    group_others,
):
    """The ranked side must contain exactly one row per requested tuple."""
    group_limit = GroupLimitPayload(
        dimensions=["sem.country"],
        top=2,
        metric="sem.cumulative_views",
        group_others=group_others,
    )
    payload = QueryPayload(
        metrics=["sem.views"],
        dimensions=["sem.country"],
        group_limit=group_limit,
    )
    dimensions = [
        V3ColumnMetadata(
            name=dimension.rsplit(".", 1)[-1],
            semantic_name=dimension,
            type="string",
            semantic_type="dimension",
        )
        for dimension in ranked_dimensions
    ]
    ranked = V3GeneratedSQL(
        query=parse("SELECT country FROM ranking"),
        columns=dimensions,
        dialect=Dialect.DUCKDB,
    )
    with patch(
        "datajunction_server.api.semantic_layer.generate_metrics_sql",
        new_callable=AsyncMock,
        return_value=ranked,
    ):
        with pytest.raises(DJException, match="one row per requested dimension") as exc:
            await _build_ranked_groups_sql(
                SimpleNamespace(cube_node_metrics=["sem.cumulative_views"]),
                SimpleNamespace(cube_node_metrics=["sem.cumulative_views"]),
                payload,
                group_limit,
                Dialect.DUCKDB,
            )
    assert exc.value.http_status_code == 400


def test_group_limit_keeps_ranked_tuples_including_nulls():
    """Ranked tuples filter main rows without losing NULL keys or mixing keys."""
    import duckdb

    def metadata(name, semantic_name, semantic_type):
        return V3ColumnMetadata(
            name=name,
            semantic_name=semantic_name,
            type="string" if semantic_type == "dimension" else "double",
            semantic_type=semantic_type,
        )

    main = V3GeneratedSQL(
        query=parse(
            "WITH sales_data AS (SELECT * FROM sales) "
            "SELECT category, region, day, SUM(amount) AS amount "
            "FROM sales_data GROUP BY category, region, day",
        ),
        columns=[
            metadata("category", "sem.category", "dimension"),
            metadata("region", "sem.region", "dimension"),
            metadata("day", "sem.day", "dimension"),
            metadata("amount", "sem.amount", "metric"),
        ],
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse(
            "WITH ranking_data AS (SELECT * FROM ranking) "
            "SELECT category, region, SUM(score) AS score FROM ranking_data "
            "GROUP BY category, region ORDER BY score DESC, category, region LIMIT 2",
        ),
        columns=[
            metadata("category", "sem.category", "dimension"),
            metadata("region", "sem.region", "dimension"),
            metadata("score", "sem.score", "metric"),
        ],
        dialect=Dialect.DUCKDB,
    )
    result = _limit_to_ranked_groups(
        main,
        ranked,
        ["sem.category", "sem.region"],
        ["sem.amount DESC"],
        10,
    )
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE sales(category VARCHAR, region VARCHAR, day VARCHAR, amount INT)",
    )
    connection.execute(
        "CREATE TABLE ranking(category VARCHAR, region VARCHAR, score INT)",
    )
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?, ?, ?)",
        [
            ("A", "East", "Mon", 100),
            ("A", "West", "Mon", 80),
            ("B", "East", "Mon", 7),
            ("B", "East", "Tue", 3),
            ("C", None, "Mon", 5),
        ],
    )
    connection.executemany(
        "INSERT INTO ranking VALUES (?, ?, ?)",
        [("A", "West", 1), ("B", "East", 20), ("C", None, 10)],
    )

    assert connection.execute(result.sql).fetchall() == [
        ("B", "East", "Mon", 7),
        ("C", None, "Mon", 5),
        ("B", "East", "Tue", 3),
    ]
    assert connection.execute(build_row_count_sql(result).sql).fetchone() == (3,)


def test_group_others_reaggregates_daily_metric_components():
    """Remaining countries form one daily series with a correctly merged ratio."""
    import duckdb

    group = GrainGroupSQL(
        query=parse(
            "WITH source_rows AS (SELECT * FROM sales) "
            "SELECT source_rows.country, source_rows.day, "
            "SUM(source_rows.views) AS views_sum, "
            "SUM(source_rows.sessions) AS sessions_sum "
            "FROM source_rows GROUP BY source_rows.country, source_rows.day",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("day", "sem.day", "string", "dimension"),
            V3ColumnMetadata(
                "views_sum",
                "sem.views:sum",
                "bigint",
                "metric_component",
            ),
            V3ColumnMetadata(
                "sessions_sum",
                "sem.sessions:sum",
                "bigint",
                "metric_component",
            ),
        ],
        grain=["country", "day"],
        aggregability=Aggregability.FULL,
        metrics=["sem.views", "sem.sessions"],
        parent_name="sem.sales",
        dimension_group_by_positions={"sem.country": 0, "sem.day": 1},
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse(
            "WITH ranking_data AS (SELECT * FROM ranking) "
            "SELECT country, SUM(score) AS score FROM ranking_data "
            "GROUP BY country ORDER BY score DESC, country LIMIT 1",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("score", "sem.views", "bigint", "metric"),
        ],
        dialect=Dialect.DUCKDB,
    )
    filters = ["sem.country != 'US'", "sem.day = 'Mon'"]
    context = SimpleNamespace(dimension_filters=filters)
    # An auxiliary weekly/window group can omit the daily country dimension.
    window_group = SimpleNamespace(is_window_grain_group=True)
    measures = SimpleNamespace(
        grain_groups=[group, window_group],
        ctx=context,
        decomposed_metrics={},
    )
    output_filters = apply_group_others(
        measures,
        GroupOthers(dimensions=("sem.country",), ranked=ranked),
    )
    assert output_filters == ["sem.day = 'Mon'"]
    assert context.dimension_filters == filters

    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE sales(day VARCHAR, country VARCHAR, views INT, sessions INT)",
    )
    connection.execute("CREATE TABLE ranking(country VARCHAR, score INT)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?, ?, ?)",
        [
            ("Mon", "US", 100, 10),
            ("Mon", "Canada", 40, 4),
            ("Mon", "Mexico", 20, 10),
            ("Tue", "US", 80, 8),
            ("Tue", "Canada", 30, 6),
            ("Tue", "Mexico", 10, 2),
        ],
    )
    connection.executemany(
        "INSERT INTO ranking VALUES (?, ?)",
        [("US", 180), ("Canada", 70), ("Mexico", 30)],
    )
    result = connection.execute(
        "SELECT day, country, SUM(views_sum) AS views, "
        "SUM(views_sum) / SUM(sessions_sum) AS views_per_session "
        f"FROM ({group.sql}) AS grouped "
        "GROUP BY day, country ORDER BY day, country",
    ).fetchall()

    assert result == [
        ("Mon", "Others", 60, pytest.approx(60 / 14)),
        ("Mon", "US", 100, 10),
        ("Tue", "Others", 40, 5),
        ("Tue", "US", 80, 10),
    ]


def test_group_others_output_filters_preserve_dimension_roles():
    """Only the bucketed role's predicate is removed after source filtering."""
    filters = [
        "sem.region.region_name[home] IN ('North', 'South')",
        "sem.region.region_name[work] = 'East'",
    ]
    measures = SimpleNamespace(
        grain_groups=[],
        ctx=SimpleNamespace(dimension_filters=filters),
        decomposed_metrics={},
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT 1"),
        columns=[],
        dialect=Dialect.DUCKDB,
    )

    output_filters = apply_group_others(
        measures,
        GroupOthers(dimensions=("sem.region.region_name[home]",), ranked=ranked),
    )

    assert output_filters == ["sem.region.region_name[work] = 'East'"]
    assert measures.ctx.dimension_filters == filters


def test_group_others_rejects_bucketed_reaggregation_dimension():
    """A protected date must not become a string bucket before LAST_VALUE."""
    component = MetricComponent(
        name="balance_sum",
        expression="balance",
        aggregation="SUM",
        merge="SUM",
        rule=AggregationRule(
            type=Aggregability.FULL,
            reaggregate=DimensionReaggregateRule(
                dimension="sem.date.date_id[order]",
                fn=ReaggregationFunction.LAST_VALUE,
            ),
        ),
    )
    metric = SimpleNamespace(components=[component])
    measures = SimpleNamespace(
        decomposed_metrics={"sem.balance": metric},
        grain_groups=[],
        ctx=SimpleNamespace(dimension_filters=[]),
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT 1"),
        columns=[],
        dialect=Dialect.DUCKDB,
    )
    with pytest.raises(DJException, match="protected dimension") as exc:
        apply_group_others(
            measures,
            GroupOthers(("sem.date.date_id[order]",), ranked),
        )
    assert exc.value.http_status_code == 400
    assert "sem.balance" in exc.value.message

    assert (
        apply_group_others(
            measures,
            GroupOthers(("sem.country",), ranked),
        )
        == []
    )


def test_group_others_matches_null_tuple_and_casts_numeric_dimension():
    """A ranked NULL key stays separate from the remaining composite groups."""
    import duckdb

    group = GrainGroupSQL(
        query=parse(
            "SELECT sales.country, sales.channel, SUM(sales.views) AS views_sum "
            "FROM sales GROUP BY sales.country, sales.channel",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("channel", "sem.channel", "bigint", "dimension"),
            V3ColumnMetadata(
                "views_sum",
                "sem.views:sum",
                "bigint",
                "metric_component",
            ),
        ],
        grain=["country", "channel"],
        aggregability=Aggregability.FULL,
        metrics=["sem.views"],
        parent_name="sem.sales",
        dimension_group_by_positions={"sem.country": 0, "sem.channel": 1},
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse(
            "SELECT country, channel, SUM(score) AS score FROM ranking "
            "GROUP BY country, channel ORDER BY score DESC LIMIT 1",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("channel", "sem.channel", "bigint", "dimension"),
            V3ColumnMetadata("score", "sem.views", "bigint", "metric"),
        ],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(group, ranked, ("sem.country", "sem.channel"), set())

    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(country VARCHAR, channel INT, views INT)")
    connection.execute("CREATE TABLE ranking(country VARCHAR, channel INT, score INT)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?, ?)",
        [(None, 1, 5), ("US", 1, 10), ("US", 2, 20)],
    )
    connection.executemany(
        "INSERT INTO ranking VALUES (?, ?, ?)",
        [(None, 1, 50), ("US", 1, 20), ("US", 2, 10)],
    )

    assert connection.execute(
        f"SELECT country, channel, SUM(views_sum) FROM ({group.sql}) AS grouped "
        "GROUP BY country, channel ORDER BY country NULLS FIRST",
    ).fetchall() == [(None, "1", 5), ("Others", "Others", 30)]
    assert group.columns[1].type == "string"
    assert _generated_column_arrow_type_name(group.columns[1]) == "utf8"


def test_group_others_uses_dimension_position_for_duplicate_group_expressions():
    """Identical SQL expressions retain their distinct semantic grain positions."""
    import duckdb

    group = GrainGroupSQL(
        query=parse(
            "SELECT sales.country AS country, sales.country AS country_copy, "
            "SUM(sales.views) AS views_sum FROM sales "
            "GROUP BY sales.country, sales.country",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("country_copy", "sem.country_copy", "string", "dimension"),
            V3ColumnMetadata(
                "views_sum",
                "sem.views:sum",
                "bigint",
                "metric_component",
            ),
        ],
        grain=["country", "country_copy"],
        aggregability=Aggregability.FULL,
        metrics=["sem.views"],
        parent_name="sem.sales",
        dimension_group_by_positions={"sem.country": 0, "sem.country_copy": 1},
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT country FROM ranking LIMIT 1"),
        columns=[V3ColumnMetadata("country", "sem.country", "string", "dimension")],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(group, ranked, ("sem.country",), set())

    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(country VARCHAR, views INT)")
    connection.execute("CREATE TABLE ranking(country VARCHAR)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?)",
        [("US", 10), ("Canada", 4), ("Mexico", 2)],
    )
    connection.execute("INSERT INTO ranking VALUES ('US')")
    assert connection.execute(
        f"SELECT country, country_copy, views_sum FROM ({group.sql}) AS grouped "
        "ORDER BY country_copy",
    ).fetchall() == [
        ("Others", "Canada", 4),
        ("Others", "Mexico", 2),
        ("US", "US", 10),
    ]


def test_group_others_finds_aliased_expression_after_unnamed_projection():
    """A dimension expression can follow an unnamed projection in a grain query."""
    import duckdb

    group = GrainGroupSQL(
        query=parse(
            "SELECT 1, CAST(sales.country AS STRING) AS country, "
            "SUM(sales.views) AS views_sum FROM sales "
            "GROUP BY CAST(sales.country AS STRING)",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata(
                "views_sum",
                "sem.views:sum",
                "bigint",
                "metric_component",
            ),
        ],
        grain=["country"],
        aggregability=Aggregability.FULL,
        metrics=["sem.views"],
        parent_name="sem.sales",
        dimension_group_by_positions={"sem.country": 0},
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT country FROM ranking LIMIT 1"),
        columns=[V3ColumnMetadata("country", "sem.country", "string", "dimension")],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(group, ranked, ("sem.country",), set())

    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(country VARCHAR, views INT)")
    connection.execute("CREATE TABLE ranking(country VARCHAR)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?)",
        [("US", 10), ("Canada", 4), ("Mexico", 2)],
    )
    connection.execute("INSERT INTO ranking VALUES ('US')")
    assert connection.execute(
        f"SELECT country, SUM(views_sum) FROM ({group.sql}) AS grouped "
        "GROUP BY country ORDER BY country",
    ).fetchall() == [("Others", 6), ("US", 10)]


def test_group_others_preserves_non_decomposable_metric_input():
    """MAX_BY reads the original numeric ID after its display is bucketed."""
    import duckdb

    decomposed = DecomposedMetricInfo(
        metric_node=SimpleNamespace(name="sem.top_product"),
        components=[],
        aggregability=Aggregability.NONE,
        combiner="MAX_BY(product_id, line_total)",
        derived_ast=parse("SELECT MAX_BY(product_id, line_total) FROM sales"),
    )
    group = GrainGroupSQL(
        query=parse("SELECT sales.product_id, sales.line_total FROM sales"),
        columns=[
            V3ColumnMetadata("product_id", "sem.product_id", "bigint", "dimension"),
            V3ColumnMetadata("line_total", "sem.line_total", "bigint", "metric_input"),
        ],
        grain=["product_id"],
        aggregability=Aggregability.NONE,
        metrics=["sem.top_product"],
        parent_name="sem.sales",
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT product_id FROM ranking LIMIT 1"),
        columns=[
            V3ColumnMetadata("product_id", "sem.product_id", "bigint", "dimension"),
        ],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(
        group,
        ranked,
        ("sem.product_id",),
        {"product_id"},
    )
    raw_alias = group.raw_metric_input_aliases["product_id"]
    assert next(col for col in group.columns if col.name == raw_alias).type == "bigint"

    metric = (
        process_base_metrics(
            [group],
            ["sales_0"],
            {"sem.top_product": decomposed},
            {},
        )
        .metric_exprs["sem.top_product"]
        .expr_ast
    )
    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(product_id INT, line_total INT)")
    connection.execute("CREATE TABLE ranking(product_id INT)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?)",
        [(1, 10), (2, 30), (3, 20)],
    )
    connection.execute("INSERT INTO ranking VALUES (1)")
    assert connection.execute(
        f"SELECT product_id, {metric} AS top_product "
        f"FROM ({group.sql}) AS sales_0 "
        "GROUP BY product_id ORDER BY product_id",
    ).fetchall() == [("1", 1), ("Others", 2)]


def test_group_others_preserves_count_distinct_dimension_key():
    """Bucketing an ID must not collapse distinct raw IDs into one key."""
    import duckdb

    component = MetricComponent(
        name="distinct_orders",
        expression="order_id",
        aggregation=None,
        rule=AggregationRule(type=Aggregability.LIMITED, level=["order_id"]),
    )
    group = GrainGroupSQL(
        query=parse(
            "SELECT sales.order_id, COUNT(*) AS order_rows "
            "FROM sales GROUP BY sales.order_id",
        ),
        columns=[
            V3ColumnMetadata("order_id", "sem.order_id", "bigint", "dimension"),
            V3ColumnMetadata("order_rows", "sem.rows", "bigint", "metric_component"),
        ],
        grain=["order_id"],
        aggregability=Aggregability.LIMITED,
        metrics=["sem.distinct_orders"],
        parent_name="sem.sales",
        dimension_group_by_positions={"sem.order_id": 0},
        component_aliases={"distinct_orders": "order_id"},
        components=[component],
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT order_id FROM ranking LIMIT 1"),
        columns=[V3ColumnMetadata("order_id", "sem.order_id", "bigint", "dimension")],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(group, ranked, ("sem.order_id",), set())
    raw_key = group.component_aliases[component.name]

    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(order_id INT)")
    connection.execute("CREATE TABLE ranking(order_id INT)")
    connection.executemany("INSERT INTO sales VALUES (?)", [(1,), (2,), (3,)])
    connection.execute("INSERT INTO ranking VALUES (1)")
    assert connection.execute(
        f"SELECT order_id, COUNT(DISTINCT {raw_key}) FROM ({group.sql}) AS grouped "
        "GROUP BY order_id ORDER BY order_id",
    ).fetchall() == [("1", 1), ("Others", 2)]

    wrapper, _ = _build_pre_agg_wrapper_cte("sales_0", group)
    assert "COUNT(" in str(wrapper)
    assert raw_key in str(wrapper)
    assert raw_key not in [str(expr) for expr in wrapper.select.group_by]
    assert connection.execute(
        f"WITH sales_0 AS ({group.sql}) {wrapper.select} ORDER BY order_id",
    ).fetchall() == [("1", 1), ("Others", 2)]


def test_group_others_preserves_raw_distinct_key_without_source_group_by():
    """Raw grain rows keep their distinct keys and avoid private name collisions."""
    import duckdb

    component = MetricComponent(
        name="distinct_orders",
        expression="order_id",
        aggregation=None,
        rule=AggregationRule(type=Aggregability.LIMITED, level=["order_id"]),
    )
    group = GrainGroupSQL(
        query=parse(
            "SELECT sales.order_id, sales.extra AS __dj_raw_group_key_0 FROM sales",
        ),
        columns=[
            V3ColumnMetadata("order_id", "sem.order_id", "bigint", "dimension"),
            V3ColumnMetadata(
                "__dj_raw_group_key_0",
                "sem.extra",
                "bigint",
                "metric_input",
            ),
        ],
        grain=["order_id"],
        aggregability=Aggregability.LIMITED,
        metrics=["sem.distinct_orders"],
        parent_name="sem.sales",
        component_aliases={"distinct_orders": "order_id"},
        components=[component],
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse("SELECT order_id FROM ranking LIMIT 1"),
        columns=[V3ColumnMetadata("order_id", "sem.order_id", "bigint", "dimension")],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(group, ranked, ("sem.order_id",), set())
    raw_key = group.component_aliases[component.name]
    assert raw_key == "__dj_raw_group_key_0_1"

    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(order_id INT, extra INT)")
    connection.execute("CREATE TABLE ranking(order_id INT)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?)",
        [(1, 10), (2, 20), (3, 30)],
    )
    connection.execute("INSERT INTO ranking VALUES (1)")
    assert connection.execute(
        f"SELECT order_id, COUNT(DISTINCT {raw_key}) FROM ({group.sql}) AS grouped "
        "GROUP BY order_id ORDER BY order_id",
    ).fetchall() == [("1", 1), ("Others", 2)]


def test_group_others_does_not_shadow_unqualified_source_columns():
    """Ranked dimensions and metrics must not make source references ambiguous."""
    import duckdb

    group = GrainGroupSQL(
        query=parse(
            "SELECT country, SUM(__dj_ranked_key_0) AS views_sum "
            "FROM sales GROUP BY country",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata(
                "views_sum",
                "sem.views:sum",
                "bigint",
                "metric_component",
            ),
        ],
        grain=["country"],
        aggregability=Aggregability.FULL,
        metrics=["sem.views"],
        parent_name="sem.sales",
        dimension_group_by_positions={"sem.country": 0},
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse(
            "SELECT country, SUM(views_sum) AS views_sum FROM ranking "
            "GROUP BY country ORDER BY views_sum DESC LIMIT 1",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("views_sum", "sem.views", "bigint", "metric"),
        ],
        dialect=Dialect.DUCKDB,
    )
    _bucket_grain_group(group, ranked, ("sem.country",), set())
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE sales(country VARCHAR, __dj_ranked_key_0 INT)",
    )
    connection.execute("CREATE TABLE ranking(country VARCHAR, views_sum INT)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?)",
        [("US", 10), ("Canada", 4), ("Mexico", 2)],
    )
    connection.executemany(
        "INSERT INTO ranking VALUES (?, ?)",
        [("US", 10), ("Canada", 4), ("Mexico", 2)],
    )
    assert connection.execute(
        f"SELECT country, SUM(views_sum) FROM ({group.sql}) AS grouped "
        "GROUP BY country ORDER BY country",
    ).fetchall() == [("Others", 6), ("US", 10)]
    assert "__dj_ranked_key_0_1" in group.sql


def test_group_others_reports_incomplete_grain_or_ranking_metadata():
    """Internal SQL shape mismatches fail with a useful input error."""

    def group(sql="SELECT sales.country FROM sales GROUP BY sales.country"):
        return GrainGroupSQL(
            query=parse(sql),
            columns=[V3ColumnMetadata("country", "sem.country", "string", "dimension")],
            grain=["country"],
            aggregability=Aggregability.FULL,
            metrics=[],
            parent_name="sem.sales",
            dimension_group_by_positions={"sem.country": 0},
            dialect=Dialect.DUCKDB,
        )

    def ranked(columns=None):
        return V3GeneratedSQL(
            query=parse("SELECT country FROM ranking"),
            columns=[V3ColumnMetadata("country", "sem.country", "string", "dimension")]
            if columns is None
            else columns,
            dialect=Dialect.DUCKDB,
        )

    with pytest.raises(DJException, match="requires a grouped metrics query"):
        _bucket_grain_group(group("SELECT 1"), ranked(), ("sem.country",), set())

    missing_dimension = group()
    missing_dimension.columns = []
    with pytest.raises(DJException, match="cannot resolve dimension"):
        _bucket_grain_group(missing_dimension, ranked(), ("sem.country",), set())

    with pytest.raises(DJException, match="cannot find the output"):
        _bucket_grain_group(
            group("SELECT sales.region FROM sales GROUP BY sales.region"),
            ranked(),
            ("sem.country",),
            set(),
        )

    with pytest.raises(DJException, match="cannot resolve ranking dimensions"):
        _bucket_grain_group(group(), ranked([]), ("sem.country",), set())

    missing_position = group()
    missing_position.dimension_group_by_positions = {}
    with pytest.raises(DJException, match="cannot group dimension"):
        _bucket_grain_group(missing_position, ranked(), ("sem.country",), set())

    main = V3GeneratedSQL(
        query=parse("SELECT country FROM sales"),
        columns=[],
        dialect=Dialect.DUCKDB,
    )
    with pytest.raises(DJException, match="cannot resolve ranking dimensions"):
        bucket_dimension_rows(main, ranked(), ("sem.country",))


def test_group_others_dimension_only_query_deduplicates_bucketed_rows():
    """Dimension-only requests can rank via an explicit metric."""
    import duckdb

    main = V3GeneratedSQL(
        query=parse("SELECT DISTINCT country, day FROM sales"),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("day", "sem.day", "string", "dimension"),
        ],
        dialect=Dialect.DUCKDB,
    )
    ranked = V3GeneratedSQL(
        query=parse(
            "SELECT country, SUM(score) AS score FROM ranking "
            "GROUP BY country ORDER BY score DESC LIMIT 1",
        ),
        columns=[
            V3ColumnMetadata("country", "sem.country", "string", "dimension"),
            V3ColumnMetadata("score", "sem.views", "bigint", "metric"),
        ],
        dialect=Dialect.DUCKDB,
    )
    result = bucket_dimension_rows(main, ranked, ("sem.country",))
    connection = duckdb.connect()
    connection.execute("CREATE TABLE sales(country VARCHAR, day VARCHAR)")
    connection.execute("CREATE TABLE ranking(country VARCHAR, score INT)")
    connection.executemany(
        "INSERT INTO sales VALUES (?, ?)",
        [("US", "Mon"), ("Canada", "Mon"), ("Mexico", "Mon")],
    )
    connection.executemany(
        "INSERT INTO ranking VALUES (?, ?)",
        [("US", 100), ("Canada", 40), ("Mexico", 20)],
    )
    assert connection.execute(
        f"SELECT * FROM ({result.sql}) AS bucketed ORDER BY country",
    ).fetchall() == [("Others", "Mon"), ("US", "Mon")]


# ---------------------------------------------------------------------------
# DB-backed integration tests (require the testcontainers Postgres harness)
# ---------------------------------------------------------------------------


async def _expect(resp, *codes):
    assert resp.status_code in codes, f"{resp.status_code}: {resp.text}"
    return resp


async def _setup_cube(client: AsyncClient) -> str:
    """Build a minimal materialization-free cube and return its node name.

    sem.sales (fact) --link region_id--> sem.region (dim over sem.regions);
    metric sem.total_amount = SUM(amount); cube sem.sales_cube over
    [sem.total_amount] x [sem.region.region_name]. Catalog ``semcat`` has only a
    trino engine, so the cube resolves to the trino dialect deterministically.
    """
    await _expect(
        await client.post("/catalogs/", json={"name": "semcat"}),
        200,
        201,
        409,
    )
    await _expect(
        await client.post(
            "/engines/",
            json={"name": "trino", "version": "451", "dialect": "trino"},
        ),
        200,
        201,
        409,
    )
    await _expect(
        await client.post(
            "/catalogs/semcat/engines/",
            json=[{"name": "trino", "version": "451"}],
        ),
        200,
        201,
    )
    await _expect(await client.post("/namespaces/sem"), 200, 201, 409)
    await _expect(
        await client.post(
            "/nodes/source/",
            json={
                "name": "sem.sales",
                "display_name": "Sales",
                "description": "",
                "mode": "published",
                "catalog": "semcat",
                "schema_": "sem",
                "table": "sales",
                "columns": [
                    {"name": "id", "type": "bigint"},
                    {"name": "region_id", "type": "bigint"},
                    {"name": "amount", "type": "double"},
                ],
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/source/",
            json={
                "name": "sem.regions",
                "display_name": "Regions",
                "description": "",
                "mode": "published",
                "catalog": "semcat",
                "schema_": "sem",
                "table": "regions",
                "columns": [
                    {"name": "region_id", "type": "bigint"},
                    {"name": "region_name", "type": "string"},
                ],
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/dimension/",
            json={
                "name": "sem.region",
                "display_name": "Region",
                "description": "",
                "mode": "published",
                "primary_key": ["region_id"],
                "query": "SELECT region_id, region_name FROM sem.regions",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/sem.sales/link/",
            json={
                "dimension_node": "sem.region",
                "join_type": "inner",
                "join_on": "sem.sales.region_id = sem.region.region_id",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/metric/",
            json={
                "name": "sem.total_amount",
                "display_name": "Total Amount",
                "description": "",
                "mode": "published",
                "query": "SELECT SUM(amount) FROM sem.sales",
                "custom_metadata": {
                    "semantic_layer": {
                        "semantic_type": "currency",
                        "unit": {"kind": "currency", "code": "USD"},
                        "format": {"preset": "currency", "precision": 2},
                        "extensions": {
                            "superset": {"d3format": "$,.2f"},
                            "google_sheets": {
                                "numberFormat": {
                                    "type": "CURRENCY",
                                    "pattern": "$#,##0.00",
                                },
                            },
                        },
                    },
                },
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/cube/",
            json={
                "name": "sem.sales_cube",
                "display_name": "Sales Cube",
                "description": "Sales by region",
                "mode": "published",
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        ),
        200,
        201,
    )
    return "sem.sales_cube"


@pytest.mark.asyncio
async def test_semantic_endpoints_end_to_end(client: AsyncClient):
    view = await _setup_cube(client)
    query = {
        "additional_configuration": {},
        "query": {
            "metrics": ["sem.total_amount"],
            "dimensions": ["sem.region.region_name"],
        },
    }

    # /views/list includes the cube.
    resp = await _expect(
        await client.post("/semantic/views/list", json={"runtime_configuration": {}}),
        200,
    )
    summary = next(v for v in resp.json() if v["name"] == view)
    assert summary["display_name"] == "Sales Cube"
    assert summary["features"] == ["GROUP_LIMIT", "GROUP_OTHERS"]

    # /views/{view} returns the cube's metrics and dimensions in spec shape.
    resp = await _expect(
        await client.post(
            f"/semantic/views/{view}",
            json={"additional_configuration": {}},
        ),
        200,
    )
    detail = resp.json()
    assert detail["display_name"] == "Sales Cube"
    assert detail["features"] == ["GROUP_LIMIT", "GROUP_OTHERS"]
    assert {m["id"] for m in detail["metrics"]} == {"sem.total_amount"}
    assert any(d["id"] == "sem.region.region_name" for d in detail["dimensions"])
    assert detail["metrics"][0]["metadata"] == {
        "display_name": "Total Amount",
        "semantic_type": "currency",
        "unit": {"kind": "currency", "code": "USD"},
        "format": {"preset": "currency", "precision": 2},
        "extensions": {
            "superset": {"d3format": "$,.2f"},
            "google_sheets": {
                "numberFormat": {
                    "type": "CURRENCY",
                    "pattern": "$#,##0.00",
                },
            },
        },
    }
    dimension = next(
        dim for dim in detail["dimensions"] if dim["id"] == "sem.region.region_name"
    )
    assert dimension["metadata"] == {
        "display_name": "Region Name",
        "semantic_type": "category",
    }

    # /sql generates physical SQL, pinned to this cube, in the trino dialect.
    resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json=query),
        200,
    )
    sql_body = resp.json()
    assert sql_body["dialect"] == "trino"
    assert "SELECT" in sql_body["sql"].upper()
    assert sql_body["columns"]

    # /row-count wraps the same semantic query in COUNT(*).
    resp = await _expect(
        await client.post(f"/semantic/views/{view}/row-count", json=query),
        200,
    )
    count_body = resp.json()
    assert 'COUNT(*) AS "COUNT"' in count_body["sql"]
    assert count_body["columns"] == [{"name": "COUNT", "type": "int"}]
    assert count_body["dialect"] == "trino"

    # /values returns a filtered distinct-dimension query.
    resp = await _expect(
        await client.post(
            f"/semantic/views/{view}/values",
            json={
                "additional_configuration": {},
                "dimension": "sem.region.region_name",
                "filters": [
                    {
                        "column": "sem.region.region_name",
                        "operator": "IN",
                        "value": ["North", "South"],
                    },
                ],
            },
        ),
        200,
    )
    values_body = resp.json()
    assert "DISTINCT" in values_body["sql"]
    assert "total_amount" in values_body["sql"]
    assert " IN ('North', 'South')" in values_body["sql"]
    assert values_body["columns"] == [{"name": "region_name", "type": "utf8"}]
    assert values_body["dialect"] == "trino"

    # Unknown metric/dimension ids are rejected (400).
    resp = await client.post(
        f"/semantic/views/{view}/sql",
        json={"query": {"metrics": ["sem.not_a_metric"], "dimensions": []}},
    )
    assert resp.status_code == 400, resp.text

    # Dimension-only queries return the dimension's distinct values.
    resp = await _expect(
        await client.post(
            f"/semantic/views/{view}/sql",
            json={
                "query": {
                    "metrics": [],
                    "dimensions": ["sem.region.region_name"],
                },
            },
        ),
        200,
    )
    assert "DISTINCT" in resp.json()["sql"]

    # Unknown view -> 404.
    resp = await client.post(
        "/semantic/views/sem.does_not_exist/sql",
        json={"query": {"metrics": ["sem.total_amount"], "dimensions": []}},
    )
    assert resp.status_code == 404, resp.text


@pytest.mark.asyncio
async def test_semantic_git_discovery_and_view_filtering(
    client: AsyncClient,
    session: AsyncSession,
    monkeypatch,
    capture_queries: list[str],
):
    await _setup_cube(client)
    for namespace in (
        "demo",
        "demo.metrics",
        "demo.metrics.main",
        "demo.metrics.literal",
        "demo.metrics.other",
        "demo.metrics.other.release",
        "legacy",
        "legacy.feature",
        "legacy.feature.child",
        "flat",
        "flat.release",
        "plain",
        "plain.metrics",
        "plain.metrics.deep",
        "empty.metrics",
        "branch_only",
        "branch_only.feature",
    ):
        await _expect(await client.post(f"/namespaces/{namespace}"), 200, 201, 409)

    async def create_cube(name: str) -> None:
        await _expect(
            await client.post(
                "/nodes/cube/",
                json={
                    "name": name,
                    "display_name": name.rsplit(".", 1)[-1],
                    "description": "",
                    "mode": "published",
                    "metrics": ["sem.total_amount"],
                    "dimensions": ["sem.region.region_name"],
                },
            ),
            200,
            201,
        )

    main_view = "demo.metrics.main.download_session"
    feature_view = "legacy.feature.thumb_report"
    flat_view = "flat.release.report"
    literal_view = "demo.metrics.literal.report"
    other_view = "demo.metrics.other.report"
    plain_view = "plain.metrics.report"
    plain_child_view = "plain.metrics.deep.report"
    for name in (
        main_view,
        feature_view,
        flat_view,
        literal_view,
        other_view,
        plain_view,
        plain_child_view,
    ):
        await create_cube(name)

    root = await NodeNamespace.get(session, "demo.metrics")
    main = await NodeNamespace.get(session, "demo.metrics.main")
    literal = await NodeNamespace.get(session, "demo.metrics.literal")
    feature = await NodeNamespace.get(session, "legacy.feature")
    flat = await NodeNamespace.get(session, "flat.release")
    other = await NodeNamespace.get(session, "demo.metrics.other")
    other_release = await NodeNamespace.get(session, "demo.metrics.other.release")
    branch_only = await NodeNamespace.get(session, "branch_only")
    branch_only_feature = await NodeNamespace.get(session, "branch_only.feature")
    assert root and main and literal and feature and flat and other and other_release
    assert branch_only and branch_only_feature
    root.github_repo_path = "team/metrics"
    root.default_branch = "main"
    main.parent_namespace = root.namespace
    main.git_branch = "main"
    literal.parent_namespace = root.namespace
    literal.git_branch = "unversioned"
    feature.parent_namespace = root.namespace  # legacy Git link outside root prefix
    feature.git_branch = "feature/foo"
    feature.github_repo_path = "team/stale-feature"
    flat.github_repo_path = "team/flat"
    flat.git_branch = "release"
    other.github_repo_path = "team/other"
    other.default_branch = "main"
    other_release.parent_namespace = other.namespace
    other_release.git_branch = "release"
    branch_only.github_repo_path = "team/branch_only"
    branch_only_feature.parent_namespace = branch_only.namespace
    branch_only_feature.git_branch = "feature"
    await session.commit()

    # A branch linked outside the root's name prefix can have descendants of
    # its own. Loading a single cube namespace must also follow its Git parent.
    scoped_namespaces = await _active_git_namespaces(session, "demo.metrics")
    assert "legacy.feature.child" in scoped_namespaces
    assert "flat.release" not in scoped_namespaces
    linked_namespaces = await _active_git_namespaces(session, "legacy.feature.child")
    assert _git_location("legacy.feature.child", linked_namespaces) == (
        "demo.metrics",
        "feature/foo",
        "legacy.feature",
    )
    assert "flat.release" not in linked_namespaces
    checker = AsyncMock(spec=AccessChecker)
    checker.approved_resource_keys.return_value = {
        (ResourceType.NAMESPACE, namespace)
        for namespace in (
            "demo.metrics",
            "demo.metrics.main",
            "demo.metrics.literal",
            "legacy.feature",
            "demo.metrics.other.release",
        )
    }
    assert await _list_semantic_branches("demo.metrics", session, checker) == [
        "feature/foo",
        "main",
        "unversioned",
    ]
    branch_is_not_a_root = await _list_semantic_branches(
        "demo.metrics.main",
        session,
        AsyncMock(),
    )
    assert isinstance(branch_is_not_a_root, JSONResponse)
    assert branch_is_not_a_root.status_code == 404

    capture_queries.clear()
    first_level = await client.post("/semantic/views", json={"path": []})
    assert first_level.status_code == 200, first_level.text
    assert not any(re.search(r"\bFROM node\s", query) for query in capture_queries)
    assert first_level.json()["hierarchy"]["next"] == {
        "id": "namespace",
        "display_name": "Namespace",
    }
    assert "demo.metrics" in first_level.json()["hierarchy"]["values"]
    assert "demo.metrics.main" not in first_level.json()["hierarchy"]["values"]
    assert "legacy.feature" not in first_level.json()["hierarchy"]["values"]
    linked_branch = await client.post(
        "/semantic/views",
        json={"path": ["legacy.feature"]},
    )
    assert linked_branch.status_code == 404, linked_branch.text
    assert "flat.release" in first_level.json()["hierarchy"]["values"]
    assert "demo.metrics.other" in first_level.json()["hierarchy"]["values"]
    assert "plain.metrics" in first_level.json()["hierarchy"]["values"]
    assert "plain.metrics.deep" in first_level.json()["hierarchy"]["values"]
    assert "empty.metrics" in first_level.json()["hierarchy"]["values"]
    assert "current" not in first_level.json()["hierarchy"]
    assert first_level.json()["views"] == []
    second_level = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics"]},
    )
    assert second_level.json() == {
        "hierarchy": {
            "next": {"id": "branch", "display_name": "Branch"},
            "values": ["feature/foo", "main", "unversioned"],
        },
        "views": [],
    }
    other_branches = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics.other"]},
    )
    assert other_branches.json()["hierarchy"]["values"] == ["main", "release"]
    plain_level = await client.post(
        "/semantic/views",
        json={"path": ["plain.metrics"]},
    )
    assert plain_level.json()["hierarchy"]["values"] == [None]
    assert [view["name"] for view in plain_level.json()["views"]] == [plain_view]
    empty_level = await client.post(
        "/semantic/views",
        json={"path": ["empty.metrics"]},
    )
    assert empty_level.json()["hierarchy"]["values"] == [None]
    branch_only_level = await client.post(
        "/semantic/views",
        json={"path": ["branch_only"]},
    )
    assert branch_only_level.json()["hierarchy"]["values"] == ["feature"]
    branch_only_views = await client.post(
        "/semantic/views",
        json={"path": ["branch_only", "feature"]},
    )
    assert branch_only_views.json()["hierarchy"] == {"next": None}
    assert branch_only_views.status_code == 200, branch_only_views.text
    assert branch_only_views.json()["views"] == []
    for selector in (
        {"path": ["empty.metrics", None]},
        {"runtime_configuration": {"namespace": "empty.metrics"}},
    ):
        empty_views = await client.post("/semantic/views", json=selector)
        assert empty_views.status_code == 200, empty_views.text
        assert (
            empty_views.json()["views"] if "path" in selector else empty_views.json()
        ) == []
    assert (
        await client.post(
            "/semantic/views",
            json={"path": ["demo.metrics", "main", "extra"]},
        )
    ).status_code == 400
    assert (
        await client.post(
            "/semantic/views",
            json={"path": [], "runtime_configuration": {"namespace": "demo.metrics"}},
        )
    ).status_code == 400
    assert (
        await client.post(
            "/semantic/views",
            json={"path": ["demo.metrics.main"]},
        )
    ).status_code == 404

    payload = {
        "runtime_configuration": {"namespace": "demo.metrics", "branch": "main"},
    }
    for path in ("/semantic/views", "/semantic/views/list"):
        response = await client.post(path, json=payload)
        assert response.status_code == 200, response.text
        assert response.json() == [
            {
                "name": main_view,
                "display_name": "download_session",
                "uid": main_view,
                "features": ["GROUP_LIMIT", "GROUP_OTHERS"],
                "namespace": "demo.metrics",
                "branch": "main",
            },
        ]

    fetched_cubes = []
    load_cubes = semantic_layer_api._cube_summaries

    async def record_loaded_cubes(*args, **kwargs):
        cubes = await load_cubes(*args, **kwargs)
        fetched_cubes.extend(name for name, _, _ in cubes)
        return cubes

    with monkeypatch.context() as patcher:
        patcher.setattr(semantic_layer_api, "_cube_summaries", record_loaded_cubes)
        filtered = await client.post(
            "/semantic/views",
            json={"path": ["demo.metrics", "main"]},
        )
    assert filtered.status_code == 200, filtered.text
    assert [view["name"] for view in filtered.json()["views"]] == [main_view]
    assert fetched_cubes == [main_view]
    assert (
        await client.post(
            "/semantic/views",
            json={
                "path": ["demo.metrics"],
                "runtime_configuration": {"branch": "main"},
            },
        )
    ).status_code == 400
    assert (
        await client.post(
            "/semantic/views",
            json={"path": ["demo.metrics.main"]},
        )
    ).status_code == 404
    invalid_git_null = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics", None]},
    )
    assert invalid_git_null.status_code == 404
    plain_filtered = await client.post(
        "/semantic/views",
        json={"path": ["plain.metrics", None]},
    )
    assert plain_filtered.status_code == 200, plain_filtered.text
    assert [view["name"] for view in plain_filtered.json()["views"]] == [plain_view]
    assert "namespace" not in plain_filtered.json()["views"][0]
    assert "branch" not in plain_filtered.json()["views"][0]
    plain_runtime = await client.post(
        "/semantic/views",
        json={"runtime_configuration": {"namespace": "plain.metrics"}},
    )
    assert plain_runtime.status_code == 200, plain_runtime.text
    assert plain_runtime.json() == plain_filtered.json()["views"]
    plain_with_branch = await client.post(
        "/semantic/views",
        json={
            "runtime_configuration": {
                "namespace": "plain.metrics",
                "branch": "unversioned",
            },
        },
    )
    assert plain_with_branch.status_code == 404
    plain_partial = await client.post(
        "/semantic/views",
        json={"path": ["plain.metrics"]},
    )
    assert [view["name"] for view in plain_partial.json()["views"]] == [plain_view]
    literal_filtered = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics", "unversioned"]},
    )
    assert [view["name"] for view in literal_filtered.json()["views"]] == [literal_view]
    assert (
        await client.post(
            "/semantic/views",
            json={"path": ["plain.metrics", "unversioned"]},
        )
    ).status_code == 404
    partial = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics"]},
    )
    assert partial.json()["views"] == []
    assert partial.json()["hierarchy"]["values"] == [
        "feature/foo",
        "main",
        "unversioned",
    ]
    all_views = await client.post("/semantic/views", json={"path": []})
    assert all_views.status_code == 200, all_views.text
    assert all_views.json()["views"] == []
    assert "demo.metrics.other" in all_views.json()["hierarchy"]["values"]
    invalid_path = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics", "missing"]},
    )
    assert invalid_path.status_code == 404
    for selector in (
        {"runtime_configuration": {"namespace": "missing"}},
        {"runtime_configuration": {"namespace": "demo.metrics", "branch": "missing"}},
    ):
        invalid = await client.post("/semantic/views", json=selector)
        assert invalid.status_code == 404, invalid.text

    response = await client.post("/semantic/views/list")
    assert response.status_code == 200, response.text
    assert main_view in {view["name"] for view in response.json()}

    feature_payload = {
        "runtime_configuration": {
            "namespace": "demo.metrics",
            "branch": "feature/foo",
        },
    }
    response = await client.post("/semantic/views", json=feature_payload)
    assert response.status_code == 200, response.text
    assert [view["name"] for view in response.json()] == [feature_view]
    assert response.json()[0]["namespace"] == "demo.metrics"
    assert response.json()[0]["branch"] == "feature/foo"

    namespace_only = await client.post(
        "/semantic/views",
        json={"runtime_configuration": {"namespace": "demo.metrics"}},
    )
    assert {view["name"] for view in namespace_only.json()} == {
        main_view,
        feature_view,
        literal_view,
    }

    flat_response = await client.post(
        "/semantic/views",
        json={
            "runtime_configuration": {
                "namespace": "flat.release",
                "branch": "release",
            },
        },
    )
    assert flat_response.status_code == 200, flat_response.text
    assert [view["name"] for view in flat_response.json()] == [flat_view]
    flat_branches = await client.post(
        "/semantic/views",
        json={"path": ["flat.release"]},
    )
    assert flat_branches.json()["hierarchy"]["values"] == ["release"]

    response = await client.post(
        "/semantic/views",
        json={"runtime_configuration": {"branch": "main"}},
    )
    assert response.status_code == 400, response.text
    assert "requires namespace" in response.json()["detail"]

    missing = await client.post(
        "/semantic/views",
        json={"path": ["missing"]},
    )
    assert missing.status_code == 404, missing.text

    # A cube and an approved branch namespace can have the same name. The
    # namespace grant must not authorize the cube when its READ is denied.
    # The regular cube endpoint rejects writes while the namespace is Git-managed.
    root.github_repo_path = None
    await session.commit()
    await create_cube("demo.metrics.main")
    root.github_repo_path = "team/metrics"
    await session.commit()

    class SelectiveAuthorizationService:
        calls = []

        def authorize(self, _context, requests):
            self.calls.append(requests)
            allowed_namespaces = {"demo.metrics", "demo.metrics.main"}
            allowed_nodes = {main_view}
            return [
                AccessDecision(
                    request=request,
                    approved=(
                        request.access_object.name
                        in (
                            allowed_namespaces
                            if request.access_object.resource_type
                            == ResourceType.NAMESPACE
                            else allowed_nodes
                        )
                    ),
                )
                for request in requests
            ]

    monkeypatch.setattr(
        "datajunction_server.internal.access.authorization.validator.get_authorization_service",
        SelectiveAuthorizationService,
    )
    discovered = await client.post("/semantic/views", json={"path": []})
    assert discovered.json()["hierarchy"]["values"] == ["demo.metrics"]
    assert all(
        request.access_object.resource_type == ResourceType.NAMESPACE
        for call in SelectiveAuthorizationService.calls
        for request in call
    )
    discovered = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics"]},
    )
    assert discovered.json()["hierarchy"]["values"] == ["main"]
    denied_branches = await client.post(
        "/semantic/views",
        json={"path": ["flat.release"]},
    )
    assert denied_branches.status_code == 404
    denied_plain_discovery = await client.post(
        "/semantic/views",
        json={"path": ["plain.metrics"]},
    )
    assert denied_plain_discovery.status_code == 404
    views = await client.post("/semantic/views/list")
    assert [view["name"] for view in views.json()] == [main_view]
    SelectiveAuthorizationService.calls.clear()
    namespace_loader = AsyncMock(wraps=_active_git_namespaces)
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer._active_git_namespaces",
        namespace_loader,
    )
    branch_views = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics", "main"]},
    )
    assert [view["name"] for view in branch_views.json()["views"]] == [main_view]
    assert len(SelectiveAuthorizationService.calls) == 1
    assert namespace_loader.await_count == 1
    feature_views = await client.post("/semantic/views", json=feature_payload)
    assert feature_views.status_code == 404
    denied_runtime = await client.post(
        "/semantic/views",
        json={"runtime_configuration": {"namespace": "flat.release"}},
    )
    assert denied_runtime.status_code == 404
    denied_plain = await client.post(
        "/semantic/views",
        json={"path": ["plain.metrics", None]},
    )
    assert denied_plain.status_code == 404


@pytest.mark.asyncio
async def test_scoped_discovery_uses_ancestor_branch(
    client: AsyncClient,
    session: AsyncSession,
):
    await _setup_cube(client)
    for namespace in ("demo", "demo.metrics"):
        await _expect(await client.post(f"/namespaces/{namespace}"), 200, 201, 409)
    cube_name = "demo.metrics.report"
    await _expect(
        await client.post(
            "/nodes/cube/",
            json={
                "name": cube_name,
                "display_name": "Report",
                "description": "",
                "mode": "published",
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        ),
        200,
        201,
    )
    ancestor = await NodeNamespace.get(session, "demo")
    root = await NodeNamespace.get(session, "demo.metrics")
    assert ancestor and root
    ancestor.git_branch = "release"
    root.github_repo_path = "team/metrics"
    root.default_branch = "main"
    await session.commit()

    unfiltered = await client.post("/semantic/views", json={})
    assert unfiltered.status_code == 200, unfiltered.text
    view = next(view for view in unfiltered.json() if view["name"] == cube_name)
    assert (view["namespace"], view["branch"]) == ("demo.metrics", "release")

    discovered = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics"]},
    )
    assert discovered.status_code == 200, discovered.text
    assert discovered.json()["hierarchy"]["values"] == ["release"]

    filtered = await client.post(
        "/semantic/views",
        json={"path": ["demo.metrics", "release"]},
    )
    assert filtered.status_code == 200, filtered.text
    assert filtered.json()["views"] == [view]


@pytest.mark.asyncio
async def test_incomplete_git_root_is_not_discoverable(
    client: AsyncClient,
    session: AsyncSession,
):
    await _setup_cube(client)
    await _expect(await client.post("/namespaces/incomplete"), 200, 201, 409)
    cube_name = "incomplete.report"
    await _expect(
        await client.post(
            "/nodes/cube/",
            json={
                "name": cube_name,
                "display_name": "Report",
                "description": "",
                "mode": "published",
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        ),
        200,
        201,
    )
    root = await NodeNamespace.get(session, "incomplete")
    assert root
    root.github_repo_path = "team/incomplete"
    await session.commit()

    unfiltered = await client.post("/semantic/views", json={})
    assert unfiltered.status_code == 200, unfiltered.text
    assert cube_name in {view["name"] for view in unfiltered.json()}

    discovered = await client.post("/semantic/views", json={"path": []})
    assert discovered.status_code == 200, discovered.text
    assert "incomplete" not in discovered.json()["hierarchy"]["values"]
    selected = await client.post("/semantic/views", json={"path": ["incomplete"]})
    assert selected.status_code == 404, selected.text
    invalid_branch = await client.post(
        "/semantic/views",
        json={"path": ["incomplete", "main"]},
    )
    assert invalid_branch.status_code == 404


@pytest.mark.asyncio
async def test_deactivated_git_ancestor_does_not_hide_active_child(
    client: AsyncClient,
    session: AsyncSession,
):
    for namespace in ("repo", "legacy", "legacy.feature", "legacy.feature.child"):
        await _expect(await client.post(f"/namespaces/{namespace}"), 200, 201, 409)
    root = await NodeNamespace.get(session, "repo")
    branch = await NodeNamespace.get(session, "legacy.feature")
    assert root and branch
    root.github_repo_path = "team/repo"
    root.default_branch = "main"
    branch.parent_namespace = root.namespace
    branch.git_branch = "feature"
    await session.commit()
    await _expect(await client.delete("/namespaces/legacy.feature/"), 200)

    discovered = await client.post("/semantic/views", json={"path": []})
    assert discovered.status_code == 200, discovered.text
    assert "legacy.feature.child" in discovered.json()["hierarchy"]["values"]

    branches = await client.post(
        "/semantic/views",
        json={"path": ["legacy.feature.child"]},
    )
    assert branches.status_code == 200, branches.text
    assert branches.json()["hierarchy"]["values"] == [None]

    views = await client.post(
        "/semantic/views",
        json={"path": ["legacy.feature.child", None]},
    )
    assert views.status_code == 200, views.text
    assert views.json()["views"] == []


@pytest.mark.asyncio
async def test_group_limit_endpoint_builds_independent_ranked_query(
    client: AsyncClient,
):
    view = await _setup_cube(client)
    query: dict[str, Any] = {
        "metrics": ["sem.total_amount"],
        "dimensions": ["sem.region.region_name"],
        "filters": [
            {"column": "sem.region.region_name", "operator": "=", "value": "North"},
        ],
        "order": [{"by": "sem.total_amount", "direction": "DESC"}],
        "group_limit": {
            "dimensions": ["sem.region.region_name"],
            "top": 1,
            "metric": "sem.total_amount",
            "direction": "DESC",
            "filters": [],
        },
    }
    resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json={"query": query}),
        200,
    )
    body = resp.json()
    assert body["dialect"] == "trino"
    assert "INNER JOIN" in body["sql"]
    assert "ranked_groups" in body["sql"]
    assert "'North'" in body["sql"]
    assert "LIMIT 1" in body["sql"]

    count_resp = await _expect(
        await client.post(f"/semantic/views/{view}/row-count", json={"query": query}),
        200,
    )
    assert "COUNT(*)" in count_resp.json()["sql"]
    assert "ranked_groups" in count_resp.json()["sql"]

    inherited = {**query, "group_limit": {**query["group_limit"], "filters": None}}
    inherited_resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json={"query": inherited}),
        200,
    )
    assert inherited_resp.json()["sql"].count("'North'") > body["sql"].count(
        "'North'",
    )
    omitted = {
        **query,
        "group_limit": {
            key: value
            for key, value in query["group_limit"].items()
            if key != "filters"
        },
    }
    omitted_resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json={"query": omitted}),
        200,
    )
    assert omitted_resp.json()["sql"] == inherited_resp.json()["sql"]

    ascending = {
        **query,
        "group_limit": {**query["group_limit"], "direction": "ASC"},
    }
    ascending_resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json={"query": ascending}),
        200,
    )
    assert "total_amount ASC" in ascending_resp.json()["sql"]

    having = {
        **query,
        "group_limit": {
            **query["group_limit"],
            "filters": [
                {
                    "type": "HAVING",
                    "column": "sem.total_amount",
                    "operator": ">",
                    "value": 10,
                },
            ],
        },
    }
    having_resp = await _expect(
        await client.post(f"/semantic/views/{view}/sql", json={"query": having}),
        200,
    )
    assert "HAVING" in having_resp.json()["sql"]
    assert "> 10" in having_resp.json()["sql"]

    with_others = {
        **query,
        "group_limit": {**query["group_limit"], "group_others": True},
    }
    others_resp = await _expect(
        await client.post(
            f"/semantic/views/{view}/sql",
            json={"query": with_others},
        ),
        200,
    )
    assert "'Others'" in others_resp.json()["sql"]
    assert "LEFT OUTER JOIN" in others_resp.json()["sql"]
    others_count = await _expect(
        await client.post(
            f"/semantic/views/{view}/row-count",
            json={"query": with_others},
        ),
        200,
    )
    assert "COUNT(*)" in others_count.json()["sql"]
    assert "'Others'" in others_count.json()["sql"]

    dimension_only = {
        **with_others,
        "metrics": [],
        "order": [{"by": "sem.region.region_name", "direction": "ASC"}],
    }
    dimension_only_resp = await _expect(
        await client.post(
            f"/semantic/views/{view}/sql",
            json={"query": dimension_only},
        ),
        200,
    )
    assert "DISTINCT" in dimension_only_resp.json()["sql"]
    assert "'Others'" in dimension_only_resp.json()["sql"]
    assert dimension_only_resp.json()["columns"] == [
        {"name": "region_name", "type": "utf8"},
    ]

    invalid_filter = {
        **query,
        "group_limit": {
            **query["group_limit"],
            "filters": [{"type": "WHERE", "column": "sem.total_amount"}],
        },
    }
    invalid_resp = await client.post(
        f"/semantic/views/{view}/sql",
        json={"query": invalid_filter},
    )
    assert invalid_resp.status_code == 400

    invalid_group_limits = [
        ({**query["group_limit"], "dimensions": []}, "dimensions"),
        (
            {
                **query["group_limit"],
                "dimensions": ["sem.region.region_name", "sem.region.region_name"],
            },
            "dimensions",
        ),
        ({**query["group_limit"], "metric": "sem.unknown"}, "metric"),
        ({**query["group_limit"], "top": 0}, "top"),
        ({**query["group_limit"], "top": MAX_ROW_LIMIT + 1}, "top"),
        ({**query["group_limit"], "direction": "SIDEWAYS"}, "direction"),
        (
            {
                **query["group_limit"],
                "filters": [{"type": "HAVING", "column": "sem.region.region_name"}],
            },
            "HAVING columns",
        ),
        (
            {
                **query["group_limit"],
                "filters": [{"type": "WHERE", "column": "sem.unknown"}],
            },
            "filter columns",
        ),
        (
            {
                **query["group_limit"],
                "filters": [
                    {"type": "JOIN", "column": "sem.region.region_name"},
                ],
            },
            "filters.type",
        ),
    ]
    for group_limit, expected_detail in invalid_group_limits:
        invalid_resp = await client.post(
            f"/semantic/views/{view}/sql",
            json={"query": {**query, "group_limit": group_limit}},
        )
        assert invalid_resp.status_code == 400
        assert expected_detail in invalid_resp.json()["detail"]

    no_ranking_metric = {
        **query,
        "metrics": [],
        "group_limit": {**query["group_limit"], "metric": None},
    }
    no_ranking_metric_resp = await client.post(
        f"/semantic/views/{view}/sql",
        json={"query": no_ranking_metric},
    )
    assert no_ranking_metric_resp.status_code == 400
    assert "ranking metric" in no_ranking_metric_resp.json()["detail"]

    nonselected_resp = await client.post(
        f"/semantic/views/{view}/sql",
        json={"query": {**query, "dimensions": []}},
    )
    assert nonselected_resp.status_code == 400
    assert "selected dimensions" in nonselected_resp.json()["detail"]

    dimension_only = {
        **query,
        "metrics": [],
        "order": [],
    }
    dimension_resp = await _expect(
        await client.post(
            f"/semantic/views/{view}/sql",
            json={"query": dimension_only},
        ),
        200,
    )
    assert dimension_resp.json()["columns"] == [
        {"name": "region_name", "type": "utf8"},
    ]

    await _expect(
        await client.post(
            "/nodes/metric/",
            json={
                "name": "sem.average_amount",
                "description": "Average sale amount",
                "mode": "published",
                "query": "SELECT AVG(amount) FROM sem.sales",
            },
        ),
        200,
        201,
    )
    await _expect(
        await client.post(
            "/nodes/cube/",
            json={
                "name": "sem.sales_cube_avg",
                "description": "Sales totals and averages by region",
                "mode": "published",
                "metrics": ["sem.total_amount", "sem.average_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        ),
        200,
        201,
    )
    average_query = {
        **with_others,
        "metrics": ["sem.total_amount", "sem.average_amount"],
        "filters": [],
    }
    average_resp = await _expect(
        await client.post(
            "/semantic/views/sem.sales_cube_avg/sql",
            json={"query": average_query},
        ),
        200,
    )
    assert "'Others'" in average_resp.json()["sql"]
    assert {column["name"] for column in average_resp.json()["columns"]} == {
        "region_name",
        "total_amount",
        "average_amount",
    }


@pytest.mark.asyncio
async def test_list_views_query_count(
    client: AsyncClient,
    capture_queries: list[str],
):
    """
    Listing views must not hydrate every cube as a full ORM ``Node``.
    """
    await _setup_cube(client)

    capture_queries.clear()
    resp = await _expect(
        await client.post("/semantic/views/list", json={"runtime_configuration": {}}),
        200,
    )
    assert resp.json()

    node_queries = [
        query for query in capture_queries if "FROM node JOIN noderevision" in query
    ]
    assert len(node_queries) == 1, "\n\n".join(node_queries)
    namespace_queries = [
        query for query in capture_queries if "FROM nodenamespace " in query
    ]
    assert len(namespace_queries) == 1, "\n\n".join(namespace_queries)

    # The remaining queries build the per-request RBAC context.
    assert len(capture_queries) <= 7, "\n\n".join(capture_queries)


@pytest.mark.asyncio
async def test_view_endpoints_use_null_for_missing_display_name(
    client: AsyncClient,
    monkeypatch,
):
    cube_revision = SimpleNamespace(
        name="sem.unnamed_cube",
        display_name="",
        columns=[],
        cube_node_metrics=["sem.total_amount"],
        cube_node_dimensions=["sem.region.region_name"],
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer._cube_summaries",
        AsyncMock(return_value=[(cube_revision.name, None, "sem")]),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(return_value=SimpleNamespace(current=cube_revision)),
    )

    list_resp = await client.post("/semantic/views/list", json={})
    detail_resp = await client.post(f"/semantic/views/{cube_revision.name}", json={})

    assert list_resp.status_code == 200, list_resp.text
    assert detail_resp.status_code == 200, detail_resp.text
    assert list_resp.json()[0]["display_name"] is None
    detail = detail_resp.json()
    assert detail["display_name"] is None
    assert detail["metrics"][0]["description"] is None
    assert detail["dimensions"][0]["description"] is None
    assert detail["dimensions"][0]["grain"] is None


# ---------------------------------------------------------------------------
# generate_query_sql request-validation rejections
#
# These fire before any cube lookup, so no seeded cube is required — the
# view-name path param can be arbitrary.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generate_sql_rejects_offset(client: AsyncClient):
    """`offset` present -> 400 (covers the ``if payload.offset`` branch)."""
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {"metrics": ["m"], "offset": 5}},
    )
    assert resp.status_code == 400, resp.text
    assert "offset" in resp.json()["detail"].lower()


@pytest.mark.asyncio
async def test_generate_sql_rejects_negative_limit(client: AsyncClient):
    """Negative `limit` -> 400 (covers the ``limit < 0`` branch)."""
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {"metrics": ["m"], "limit": -1}},
    )
    assert resp.status_code == 400, resp.text
    assert "non-negative" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_generate_sql_rejects_limit_over_max(client: AsyncClient):
    """`limit` > MAX_ROW_LIMIT -> 400 (covers the ``limit > MAX_ROW_LIMIT`` branch)."""
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {"metrics": ["m"], "limit": MAX_ROW_LIMIT + 1}},
    )
    assert resp.status_code == 400, resp.text
    assert str(MAX_ROW_LIMIT) in resp.json()["detail"]


@pytest.mark.asyncio
async def test_generate_sql_rejects_empty_query(client: AsyncClient):
    resp = await client.post(
        "/semantic/views/any_view/sql",
        json={"query": {}},
    )
    assert resp.status_code == 400, resp.text
    assert "at least one metric or dimension" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# get_view unknown-view 404 (the ``cube_node is None`` branch)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_view_unknown_returns_404(client: AsyncClient):
    """An unknown view name on ``/views/{view}`` -> 404."""
    resp = await client.post(
        "/semantic/views/sem.no_such_view",
        json={"additional_configuration": {}},
    )
    assert resp.status_code == 404, resp.text
    assert "does not exist" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_view_named_discover_uses_detail_route(client: AsyncClient):
    """The combined listing leaves this view name available."""
    assert (await client.post("/semantic/discover", json={})).status_code == 404
    resp = await client.post("/semantic/views/discover", json={})
    assert resp.status_code == 404, resp.text
    assert "View `discover` does not exist" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_values_rejects_unknown_dimension(client: AsyncClient):
    view = await _setup_cube(client)
    resp = await client.post(
        f"/semantic/views/{view}/values",
        json={"dimension": "sem.region.unknown", "filters": []},
    )
    assert resp.status_code == 400, resp.text
    assert "does not contain dimension" in resp.json()["detail"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cube", [None, SimpleNamespace(current=None)])
async def test_values_unknown_view_returns_404(
    client: AsyncClient,
    monkeypatch,
    cube,
):
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(return_value=cube),
    )

    resp = await client.post(
        "/semantic/views/sem.no_such_view/values",
        json={"dimension": "sem.region.region_name"},
    )

    assert resp.status_code == 404, resp.text
    assert "does not exist" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_values_rejects_filter_outside_view(
    client: AsyncClient,
    monkeypatch,
):
    fake_cube = SimpleNamespace(
        current=SimpleNamespace(
            cube_node_dimensions=["sem.region.region_name"],
        ),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(return_value=fake_cube),
    )

    resp = await client.post(
        "/semantic/views/sem.sales_cube/values",
        json={
            "dimension": "sem.region.region_name",
            "filters": [
                {
                    "column": "sem.customer.country",
                    "operator": "=",
                    "value": "US",
                },
            ],
        },
    )

    assert resp.status_code == 400, resp.text
    assert "does not contain filter dimensions" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# DJException handlers (the ``except DJException`` branches) — forced via
# monkeypatch so the underlying call raises a DJException with a specific
# status code, and we assert the ``{status_code, detail}`` problem shape.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_views_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    """A failed cube summary lookup returns a problem response."""
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer._cube_summaries",
        AsyncMock(
            side_effect=DJException(message="find blew up", http_status_code=418),
        ),
    )
    resp = await client.post("/semantic/views/list", json={"runtime_configuration": {}})
    assert resp.status_code == 418, resp.text
    assert resp.json() == {"status_code": 418, "detail": "find blew up"}


@pytest.mark.asyncio
async def test_get_view_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    """``Node.get_cube_by_name`` raising DJException -> problem in get_view."""
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(
            side_effect=DJException(message="cube blew up", http_status_code=422),
        ),
    )
    resp = await client.post(
        "/semantic/views/some_view",
        json={"additional_configuration": {}},
    )
    assert resp.status_code == 422, resp.text
    assert resp.json() == {"status_code": 422, "detail": "cube blew up"}


@pytest.mark.asyncio
async def test_values_cube_lookup_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(
            side_effect=DJException(message="cube blew up", http_status_code=418),
        ),
    )

    resp = await client.post(
        "/semantic/views/some_view/values",
        json={"dimension": "sem.region.region_name"},
    )

    assert resp.status_code == 418, resp.text
    assert resp.json() == {"status_code": 418, "detail": "cube blew up"}


@pytest.mark.asyncio
async def test_generate_sql_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    """``generate_metrics_sql`` raising DJException -> problem in generate_query_sql.

    The request passes validation and cube-membership (a fake cube exposing the
    requested metric/dimension is returned) so execution reaches ``_generate_sql``,
    where the patched ``generate_metrics_sql`` raises.
    """
    fake_cube = SimpleNamespace(
        current=SimpleNamespace(
            cube_node_metrics=["sem.total_amount"],
            cube_node_dimensions=["sem.region.region_name"],
        ),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(return_value=fake_cube),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.generate_metrics_sql",
        AsyncMock(
            side_effect=DJException(message="sql gen blew up", http_status_code=400),
        ),
    )
    resp = await client.post(
        "/semantic/views/sem.sales_cube/sql",
        json={
            "query": {
                "metrics": ["sem.total_amount"],
                "dimensions": ["sem.region.region_name"],
            },
        },
    )
    assert resp.status_code == 400, resp.text
    assert resp.json() == {"status_code": 400, "detail": "sql gen blew up"}


@pytest.mark.asyncio
async def test_values_sql_generation_djexception_returns_problem(
    client: AsyncClient,
    monkeypatch,
):
    fake_cube = SimpleNamespace(
        current=SimpleNamespace(
            cube_node_dimensions=["sem.region.region_name"],
        ),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer.Node.get_cube_by_name",
        AsyncMock(return_value=fake_cube),
    )
    monkeypatch.setattr(
        "datajunction_server.api.semantic_layer._generate_sql",
        AsyncMock(
            side_effect=DJException(message="values SQL blew up", http_status_code=422),
        ),
    )

    resp = await client.post(
        "/semantic/views/sem.sales_cube/values",
        json={"dimension": "sem.region.region_name"},
    )

    assert resp.status_code == 422, resp.text
    assert resp.json() == {"status_code": 422, "detail": "values SQL blew up"}
