"""Deploy and export round trip for metric filter requirements."""

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    DeploymentSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)

from tests.api.deployments_test import deploy_and_wait
from tests.api.required_filters_validation_test import _upstream_link_setup


def _nodes() -> list:
    return [
        SourceSpec(
            name="items",
            catalog="default",
            schema="pricing",
            table="items",
            columns=[
                ColumnSpec(name="item_id", type="int"),
                ColumnSpec(name="price", type="int"),
            ],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}version",
                    join_on="${prefix}items.item_id = ${prefix}version.item_id",
                ),
            ],
        ),
        DimensionSpec(
            name="version",
            query="SELECT item_id, price > 0 AS is_current FROM ${prefix}items",
            primary_key=["item_id"],
            owners=["dj"],
        ),
        MetricSpec(
            name="total_price",
            query="SELECT SUM(price) FROM ${prefix}items",
            owners=["dj"],
            required_filters=[
                {
                    "dimension": "${prefix}version.is_current",
                    "default": "${prefix}version.is_current = true",
                },
            ],
            recommended_filters=[
                {"dimension": ["${prefix}version.item_id"]},
            ],
        ),
    ]


@pytest.mark.asyncio
async def test_filter_requirements_round_trip(client):
    namespace = "filter_reqs"
    spec = DeploymentSpec(namespace=namespace, nodes=_nodes())
    data = await deploy_and_wait(client, spec)
    assert data["status"] == "success", data["results"]

    node = (await client.get(f"/nodes/{namespace}.total_price/")).json()
    assert node["status"] == "valid"
    assert node["required_filters"] == [
        {
            "dimension": [f"{namespace}.version.is_current"],
            "default": f"{namespace}.version.is_current = true",
        },
    ]
    assert node["recommended_filters"] == [
        {"dimension": [f"{namespace}.version.item_id"], "default": None},
    ]
    version = node["version"]

    data = await deploy_and_wait(client, spec)
    assert data["status"] == "success", data["results"]
    assert {r["name"]: r["operation"] for r in data["results"]} == {
        f"{namespace}.items": "noop",
        f"{namespace}.version": "noop",
        f"{namespace}.total_price": "noop",
    }
    node = (await client.get(f"/nodes/{namespace}.total_price/")).json()
    assert node["version"] == version

    exported = (await client.get(f"/namespaces/{namespace}/export/spec")).json()
    metric = next(n for n in exported["nodes"] if n["name"].endswith("total_price"))
    assert metric["required_filters"] == [
        {
            "dimension": ["${prefix}version.is_current"],
            "default": "${prefix}version.is_current = true",
        },
    ]
    assert metric["recommended_filters"] == [
        {"dimension": ["${prefix}version.item_id"]},
    ]


def _roled_metric(attribute: str) -> MetricSpec:
    return MetricSpec(
        name="roled_total",
        query="SELECT SUM(line_total) FROM v3.order_details",
        owners=["dj"],
        required_filters=[
            {
                "dimension": "v3.date.date_id[order]",
                "default": f"v3.date.date_id[order] = 20250101 AND {attribute} = 1",
            },
        ],
    )


@pytest.mark.asyncio
async def test_role_checked_against_saved_links(client_with_build_v3):
    client = client_with_build_v3
    spec = DeploymentSpec(
        namespace="rf_roles_ok",
        nodes=[_roled_metric("v3.date.date_id[order]")],
    )
    data = await deploy_and_wait(client, spec)
    assert [(r["name"], r["status"]) for r in data["results"]] == [
        ("rf_roles_ok.roled_total", "success"),
    ]

    spec = DeploymentSpec(
        namespace="rf_roles_bad",
        nodes=[_roled_metric("v3.date.date_id[nonexistent]")],
    )
    data = await deploy_and_wait(client, spec)
    assert data["status"] == "success", data["results"]
    assert data["results"] == [
        {
            "name": "rf_roles_bad.roled_total",
            "deploy_type": "node",
            "status": "invalid",
            "operation": "create",
            "message": "Created metric (v1.0)\n[invalid] Node definition "
            "declares filter requirements that name attributes that could not "
            "be resolved.",
            "changed_fields": [],
        },
    ]


def _invalid_result(name: str) -> dict:
    return {
        "name": name,
        "deploy_type": "node",
        "status": "invalid",
        "operation": "create",
        "message": "Created metric (v1.0)\n[invalid] Node definition "
        "declares filter requirements that name attributes that could not "
        "be resolved.",
        "changed_fields": [],
    }


@pytest.mark.asyncio
async def test_multi_hop_role_checked_against_saved_links(client_with_build_v3):
    client = client_with_build_v3
    spec = DeploymentSpec(
        namespace="rf_chain_ok",
        nodes=[
            MetricSpec(
                name="roled_total",
                query="SELECT SUM(line_total) FROM v3.order_details",
                owners=["dj"],
                required_filters=[
                    {
                        "dimension": "v3.location.country[customer->home]",
                        "default": "v3.location.country[customer->home] = 'US'",
                    },
                ],
            ),
        ],
    )
    data = await deploy_and_wait(client, spec)
    assert [(r["name"], r["status"]) for r in data["results"]] == [
        ("rf_chain_ok.roled_total", "success"),
    ]

    spec = DeploymentSpec(
        namespace="rf_chain_bad",
        nodes=[
            MetricSpec(
                name="roled_total",
                query="SELECT SUM(line_total) FROM v3.order_details",
                owners=["dj"],
                required_filters=[
                    {"dimension": "v3.location.country[customer->nonexistent]"},
                ],
            ),
        ],
    )
    data = await deploy_and_wait(client, spec)
    assert data["status"] == "success", data["results"]
    assert data["results"] == [_invalid_result("rf_chain_bad.roled_total")]


def _chain_nodes(attribute: str) -> list:
    return [
        SourceSpec(
            name="events",
            catalog="default",
            schema="chain",
            table="events",
            columns=[
                ColumnSpec(name="user_id", type="int"),
                ColumnSpec(name="amount", type="int"),
            ],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}user",
                    join_on="${prefix}events.user_id = ${prefix}user.user_id",
                    role="buyer",
                ),
            ],
        ),
        SourceSpec(
            name="users",
            catalog="default",
            schema="chain",
            table="users",
            columns=[
                ColumnSpec(name="user_id", type="int"),
                ColumnSpec(name="city_id", type="int"),
            ],
        ),
        SourceSpec(
            name="cities",
            catalog="default",
            schema="chain",
            table="cities",
            columns=[
                ColumnSpec(name="city_id", type="int"),
                ColumnSpec(name="name", type="string"),
            ],
        ),
        DimensionSpec(
            name="user",
            query="SELECT user_id, city_id FROM ${prefix}users",
            primary_key=["user_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}city",
                    join_on="${prefix}user.city_id = ${prefix}city.city_id",
                    role="home",
                ),
            ],
        ),
        DimensionSpec(
            name="city",
            query="SELECT city_id, name FROM ${prefix}cities",
            primary_key=["city_id"],
            owners=["dj"],
        ),
        MetricSpec(
            name="total_amount",
            query="SELECT SUM(amount) FROM ${prefix}events",
            owners=["dj"],
            required_filters=[{"dimension": attribute}],
        ),
    ]


@pytest.mark.asyncio
async def test_multi_hop_role_checked_against_declared_links(client):
    spec = DeploymentSpec(
        namespace="rf_declared_ok",
        nodes=_chain_nodes("${prefix}city.name[buyer->home]"),
    )
    data = await deploy_and_wait(client, spec)
    assert data["status"] == "success", data["results"]
    metric = (await client.get("/nodes/rf_declared_ok.total_amount/")).json()
    assert metric["status"] == "valid", data["results"]
    assert metric["required_filters"] == [
        {"dimension": ["rf_declared_ok.city.name[buyer->home]"], "default": None},
    ]

    spec = DeploymentSpec(
        namespace="rf_declared_bad",
        nodes=_chain_nodes("${prefix}city.name[buyer->away]"),
    )
    data = await deploy_and_wait(client, spec)
    metric = next(
        r for r in data["results"] if r["name"] == "rf_declared_bad.total_amount"
    )
    assert metric == _invalid_result("rf_declared_bad.total_amount")


def _self_link_nodes() -> list:
    return [
        SourceSpec(
            name="sales",
            catalog="default",
            schema="chain",
            table="sales",
            columns=[
                ColumnSpec(name="staff_id", type="int"),
                ColumnSpec(name="amount", type="int"),
            ],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}employee",
                    join_on="${prefix}sales.staff_id = ${prefix}employee.employee_id",
                    role="staff",
                ),
            ],
        ),
        SourceSpec(
            name="employees",
            catalog="default",
            schema="chain",
            table="employees",
            columns=[
                ColumnSpec(name="employee_id", type="int"),
                ColumnSpec(name="country", type="string"),
                ColumnSpec(name="manager_id", type="int"),
            ],
        ),
        DimensionSpec(
            name="employee",
            query="SELECT employee_id, country, manager_id FROM ${prefix}employees",
            primary_key=["employee_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}employee",
                    join_on=(
                        "${prefix}employee.manager_id = ${prefix}employee.employee_id"
                    ),
                    role="manager",
                ),
            ],
        ),
        MetricSpec(
            name="total_sales",
            query="SELECT SUM(amount) FROM ${prefix}sales",
            owners=["dj"],
            required_filters=[
                {"dimension": "${prefix}employee.country[staff->manager]"},
            ],
        ),
    ]


@pytest.mark.asyncio
async def test_chain_that_revisits_a_node_rejected(client):
    spec = DeploymentSpec(namespace="rf_self_link", nodes=_self_link_nodes())
    data = await deploy_and_wait(client, spec)
    metric = next(r for r in data["results"] if r["name"] == "rf_self_link.total_sales")
    assert metric == _invalid_result("rf_self_link.total_sales")


def _quoted_default_nodes() -> list:
    return [
        SourceSpec(
            name="events",
            catalog="default",
            schema="quoted",
            table="events",
            columns=[
                ColumnSpec(name="event_id", type="int"),
                ColumnSpec(name="name", type="string"),
            ],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}event",
                    join_on="${prefix}events.event_id = ${prefix}event.event_id",
                ),
            ],
        ),
        DimensionSpec(
            name="event",
            query="SELECT event_id, name FROM ${prefix}events",
            primary_key=["event_id"],
            owners=["dj"],
        ),
        MetricSpec(
            name="event_count",
            query="SELECT COUNT(event_id) FROM ${prefix}events",
            owners=["dj"],
            required_filters=[
                {
                    "dimension": "${prefix}event.name",
                    "default": "${prefix}event.name = 'rf_quoted.created'",
                },
            ],
        ),
    ]


@pytest.mark.asyncio
async def test_export_keeps_namespace_text_in_default_literals(client):
    spec = DeploymentSpec(namespace="rf_quoted", nodes=_quoted_default_nodes())
    data = await deploy_and_wait(client, spec)
    assert data["status"] == "success", data["results"]

    node = (await client.get("/nodes/rf_quoted.event_count/")).json()
    assert node["status"] == "valid"

    exported = (await client.get("/namespaces/rf_quoted/export/spec")).json()
    metric = next(n for n in exported["nodes"] if n["name"].endswith("event_count"))
    assert metric["required_filters"] == [
        {
            "dimension": ["${prefix}event.name"],
            "default": "${prefix}event.name = 'rf_quoted.created'",
        },
    ]

    copy = DeploymentSpec(namespace="rf_quoted_copy", nodes=exported["nodes"])
    data = await deploy_and_wait(client, copy)
    assert data["status"] == "success", data["results"]
    node = (await client.get("/nodes/rf_quoted_copy.event_count/")).json()
    assert node["status"] == "valid"
    assert node["required_filters"] == [
        {
            "dimension": ["rf_quoted_copy.event.name"],
            "default": "rf_quoted_copy.event.name = 'rf_quoted.created'",
        },
    ]


@pytest.mark.parametrize(
    "entry,status",
    [
        ({"dimension": "v3.rf_plan.plan_label"}, "success"),
        (
            {
                "dimension": ["v3.date.date_id[order]", "v3.rf_plan.plan_label"],
                "default": "v3.rf_plan.plan_label = 'checkout'",
            },
            "success",
        ),
        # SQL builds read only the referenced column.
        ({"dimension": "v3.rf_plan.region"}, "invalid"),
        ({"dimension": "v3.page_views_enriched.customer_id"}, "invalid"),
    ],
    ids=["referenced", "referenced-default", "other-column", "unlinked"],
)
@pytest.mark.asyncio
async def test_saved_reference_link_counts(client_with_build_v3, entry, status):
    client = client_with_build_v3
    response = await client.post(
        "/nodes/dimension/",
        json={
            "name": "v3.rf_plan",
            "description": "d",
            "mode": "published",
            "primary_key": ["plan_label"],
            "query": "SELECT city AS plan_label, region FROM v3.src_locations",
        },
    )
    assert response.status_code == 201, response.text
    response = await client.post(
        "/nodes/v3.order_details/columns/status/link",
        params={"dimension_node": "v3.rf_plan", "dimension_column": "plan_label"},
    )
    assert response.status_code == 201, response.text

    spec = DeploymentSpec(
        namespace="rf_ref",
        nodes=[
            MetricSpec(
                name="total",
                query="SELECT SUM(line_total) FROM v3.order_details",
                owners=["dj"],
                required_filters=[entry],
            ),
        ],
    )
    data = await deploy_and_wait(client, spec)
    if status == "success":
        assert [(r["name"], r["status"]) for r in data["results"]] == [
            ("rf_ref.total", "success"),
        ]
    else:
        assert data["results"] == [_invalid_result("rf_ref.total")]


@pytest.mark.asyncio
async def test_declared_reference_link_counts(client):
    nodes = [
        SourceSpec(
            name="visits",
            catalog="default",
            schema="ref",
            table="visits",
            columns=[
                ColumnSpec(name="plan", type="string"),
                ColumnSpec(name="amount", type="int"),
            ],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    node_column="plan",
                    dimension="${prefix}plan.plan_label",
                ),
            ],
        ),
        SourceSpec(
            name="plans",
            catalog="default",
            schema="ref",
            table="plans",
            columns=[ColumnSpec(name="plan_label", type="string")],
        ),
        DimensionSpec(
            name="plan",
            query="SELECT plan_label FROM ${prefix}plans",
            primary_key=["plan_label"],
            owners=["dj"],
        ),
        MetricSpec(
            name="total_amount",
            query="SELECT SUM(amount) FROM ${prefix}visits",
            owners=["dj"],
            required_filters=[{"dimension": "${prefix}plan.plan_label"}],
        ),
    ]
    data = await deploy_and_wait(
        client,
        DeploymentSpec(namespace="rf_declared_ref", nodes=nodes),
    )
    assert data["status"] == "success", data["results"]
    metric = (await client.get("/nodes/rf_declared_ref.total_amount/")).json()
    assert metric["status"] == "valid", data["results"]


@pytest.mark.parametrize(
    "entry,status",
    [
        ({"dimension": "v3.rf_audit_date.dateint"}, "success"),
        (
            {
                "dimension": "v3.rf_audit_date.dateint",
                "default": "v3.rf_audit_date.dateint >= 20260101",
            },
            "success",
        ),
        ({"dimension": "v3.date.date_id[order]"}, "invalid"),
    ],
    ids=["entry", "default", "not-in-lineage"],
)
@pytest.mark.asyncio
async def test_saved_upstream_link_counts(client_with_build_v3, entry, status):
    client = client_with_build_v3
    await _upstream_link_setup(client)
    spec = DeploymentSpec(
        namespace="rf_upstream",
        nodes=[
            MetricSpec(
                name="event_total",
                query="SELECT SUM(event_count) FROM v3.rf_account_events",
                owners=["dj"],
                required_filters=[entry],
            ),
        ],
    )
    data = await deploy_and_wait(client, spec)
    if status == "success":
        assert [(r["name"], r["status"]) for r in data["results"]] == [
            ("rf_upstream.event_total", "success"),
        ]
    else:
        assert data["results"] == [_invalid_result("rf_upstream.event_total")]


def _declared_upstream_nodes(attribute: str, link_role: str | None) -> list:
    return [
        SourceSpec(
            name="audit_dates",
            catalog="default",
            schema="up",
            table="audit_dates",
            columns=[ColumnSpec(name="dateint", type="int")],
        ),
        DimensionSpec(
            name="audit_date",
            query="SELECT dateint FROM ${prefix}audit_dates",
            primary_key=["dateint"],
            owners=["dj"],
        ),
        SourceSpec(
            name="audit_log",
            catalog="default",
            schema="up",
            table="audit_log",
            columns=[
                ColumnSpec(name="audit_date", type="int"),
                ColumnSpec(name="account_id", type="int"),
            ],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}audit_date",
                    join_on=(
                        "${prefix}audit_log.audit_date = ${prefix}audit_date.dateint"
                    ),
                    role=link_role,
                ),
            ],
        ),
        TransformSpec(
            name="account_events",
            query=(
                "SELECT account_id, COUNT(*) AS event_count "
                "FROM ${prefix}audit_log GROUP BY account_id"
            ),
            owners=["dj"],
        ),
        MetricSpec(
            name="event_total",
            query="SELECT SUM(event_count) FROM ${prefix}account_events",
            owners=["dj"],
            required_filters=[{"dimension": attribute}],
        ),
    ]


@pytest.mark.parametrize(
    "namespace,attribute,link_role,valid",
    [
        ("rf_up_entry", "${prefix}audit_date.dateint", None, True),
        # No role is the null role, which a named upstream link doesn't satisfy.
        ("rf_up_named_link", "${prefix}audit_date.dateint", "audit", False),
        # A pushed down role breaks the SQL.
        ("rf_up_role", "${prefix}audit_date.dateint[audit]", "audit", False),
        ("rf_up_bad_role", "${prefix}audit_date.dateint[nope]", "audit", False),
    ],
    ids=["entry", "named-link", "upstream-role", "unknown-role"],
)
@pytest.mark.asyncio
async def test_declared_upstream_link_counts(
    client,
    namespace,
    attribute,
    link_role,
    valid,
):
    data = await deploy_and_wait(
        client,
        DeploymentSpec(
            namespace=namespace,
            nodes=_declared_upstream_nodes(attribute, link_role),
        ),
    )
    metric = next(r for r in data["results"] if r["name"].endswith("event_total"))
    if valid:
        assert metric["status"] == "success", data["results"]
    else:
        assert metric == _invalid_result(f"{namespace}.event_total")


@pytest.mark.asyncio
async def test_removed_link_no_longer_counts(client):
    namespace = "rf_removed_link"
    data = await deploy_and_wait(
        client,
        DeploymentSpec(namespace=namespace, nodes=_nodes()),
    )
    assert data["status"] == "success", data["results"]
    metric = (await client.get(f"/nodes/{namespace}.total_price/")).json()
    assert metric["status"] == "valid"

    nodes = _nodes()
    nodes[0].dimension_links = []
    nodes[2].description = "Changed so the metric is checked again"
    data = await deploy_and_wait(
        client,
        DeploymentSpec(namespace=namespace, nodes=nodes),
    )
    result = next(r for r in data["results"] if r["name"].endswith("total_price"))
    assert result == {
        "name": f"{namespace}.total_price",
        "deploy_type": "node",
        "status": "invalid",
        "operation": "update",
        "message": "Updated metric (v1.1)\n└─ Updated description\n"
        "[invalid] Node definition declares filter requirements that name "
        "attributes that could not be resolved.",
        "changed_fields": ["description"],
    }
