"""
Save-time checks of filter requirement attributes must match what SQL builds
can serve.

Each scenario names an attribute. The test saves a metric with a required
filter on it, then builds SQL for the same metric without requirements and a
caller filter on the attribute. A saved attribute must build, and a rejected
one must not. A role-less attribute only matches a role-free link, so on a
dimension reached only through roles it is rejected and the build fails too.
When the build falls back to another role, the attribute is rejected and the
build is checked to give the fallback role's SQL.
"""

from dataclasses import dataclass

import pytest
import pytest_asyncio

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


@dataclass(frozen=True)
class Scenario:
    """One attribute and whether a metric can filter on it."""

    id: str
    metric: str
    attribute: str
    can_filter: bool
    # Attribute whose SQL the build gives instead.
    fallback: str | None = None
    # Pushed down key column, role left on.
    key_with_role: str | None = None


# Metrics without filter requirements, by kind.
PLAIN_METRICS = {
    "base": "ag_total",
    "derived": "ag_total_x2",
    "two_facts": "ag_sales_per_visit",
}

DIMENSIONS = {
    "base": [],
    "derived": [],
    "two_facts": ["v3.ag_employee.employee_id[staff]"],
}

SCENARIOS = [
    Scenario("own-column", "base", "ag_sales.region_code", True),
    Scenario("join", "base", "ag_employee.country", False),
    Scenario("join-role", "base", "ag_employee.country[staff]", True),
    Scenario("reference", "base", "ag_plan.plan_code", True),
    Scenario("reference-any-role", "base", "ag_plan.plan_code[any]", True),
    Scenario("reference-beside-join-role", "base", "ag_plan.plan_code[billing]", True),
    Scenario("reference-other-column", "base", "ag_plan.tier", False),
    Scenario("join-then-reference", "base", "ag_country.name", False),
    Scenario("upstream-key", "base", "ag_date.dateint", True),
    Scenario(
        "upstream-key-role",
        "base",
        "ag_date.dateint[sold]",
        False,
        fallback="ag_date.dateint",
        key_with_role="s.sale_date",
    ),
    Scenario("upstream-other-column", "base", "ag_date.month", False),
    Scenario("upstream-own-column", "base", "ag_src_sales.account_id", False),
    Scenario(
        "upstream-unknown-role",
        "base",
        "ag_date.dateint[nope]",
        False,
        fallback="ag_date.dateint",
        key_with_role="s.sale_date",
    ),
    Scenario("no-link", "base", "ag_island.month", False),
    Scenario("chain", "base", "ag_office.city[staff->work]", True),
    Scenario("chain-no-role", "base", "ag_office.city", False),
    Scenario(
        "chain-revisits",
        "base",
        "ag_employee.country[staff->manager]",
        False,
        fallback="ag_employee.country[staff]",
    ),
    Scenario(
        "unknown-role",
        "base",
        "ag_employee.country[nope]",
        False,
        fallback="ag_employee.country[staff]",
    ),
    Scenario(
        "chain-unknown-segment",
        "base",
        "ag_office.city[staff->nope]",
        False,
        fallback="ag_office.city[staff->work]",
    ),
    Scenario("derived-join", "derived", "ag_employee.country", False),
    Scenario("derived-upstream-key", "derived", "ag_date.dateint", True),
    Scenario("derived-reference-other-column", "derived", "ag_plan.tier", False),
    Scenario("two-facts-shared", "two_facts", "ag_employee.country", False),
    Scenario("two-facts-one-side", "two_facts", "ag_plan.plan_code", False),
]


def _source(name: str, table: str, columns: dict[str, str]) -> dict:
    return {
        "name": name,
        "description": "d",
        "catalog": "default",
        "schema_": "v3",
        "table": table,
        "mode": "published",
        "columns": [{"name": col, "type": typ} for col, typ in columns.items()],
    }


def _dimension(name: str, query: str, key: str) -> dict:
    return {
        "name": name,
        "description": "d",
        "query": query,
        "mode": "published",
        "primary_key": [key],
    }


def _join(dimension: str, join_on: str, role: str | None = None) -> dict:
    return {
        "dimension_node": dimension,
        "join_type": "left",
        "join_on": join_on,
        **({"role": role} if role else {}),
    }


def graph_calls(ns: str) -> list[tuple[str, dict | None, dict | None]]:
    """
    API calls that build the scenario graph in namespace `ns`.

    The metric reads the transform ag_sales. Its upstream source links to
    ag_date. ag_sales links to ag_employee as `staff`, ag_employee links to
    itself as `manager` and to ag_office as `work`. ag_sales also links to
    ag_plan as `billing`, and ag_sales.plan_code references ag_plan.plan_code.
    ag_employee.country references ag_country.name. Nothing links to ag_island. A second fact, the source
    ag_src_visits, links to ag_employee as `staff`.
    """
    calls: list[tuple[str, dict | None, dict | None]] = []

    def post(path: str, body: dict | None = None, params: dict | None = None):
        calls.append((path, body, params))

    post(
        "/nodes/source/",
        _source(
            f"{ns}.ag_src_sales",
            f"{ns}_sales",
            {
                "sale_id": "int",
                "amount": "int",
                "account_id": "int",
                "staff_id": "int",
                "plan_code": "string",
                "sale_date": "int",
                "region_code": "string",
            },
        ),
    )
    post(
        "/nodes/source/",
        _source(
            f"{ns}.ag_src_employees",
            f"{ns}_employees",
            {
                "employee_id": "int",
                "country": "string",
                "manager_id": "int",
                "office_id": "int",
            },
        ),
    )
    post(
        "/nodes/source/",
        _source(
            f"{ns}.ag_src_other",
            f"{ns}_other",
            {"code": "string", "label": "string", "dateint": "int", "month": "int"},
        ),
    )
    post(
        "/nodes/source/",
        _source(
            f"{ns}.ag_src_visits",
            f"{ns}_visits",
            {"visit_id": "int", "staff_id": "int"},
        ),
    )
    for name, query, key in [
        (
            "ag_employee",
            f"SELECT employee_id, country, manager_id, office_id "
            f"FROM {ns}.ag_src_employees",
            "employee_id",
        ),
        (
            "ag_office",
            f"SELECT office_id, country AS city FROM {ns}.ag_src_employees",
            "office_id",
        ),
        (
            "ag_plan",
            f"SELECT code AS plan_code, label AS tier FROM {ns}.ag_src_other",
            "plan_code",
        ),
        (
            "ag_country",
            f"SELECT code AS name, label FROM {ns}.ag_src_other",
            "name",
        ),
        ("ag_date", f"SELECT dateint, month FROM {ns}.ag_src_other", "dateint"),
        ("ag_island", f"SELECT dateint, month FROM {ns}.ag_src_other", "dateint"),
    ]:
        post("/nodes/dimension/", _dimension(f"{ns}.{name}", query, key))
    post(
        "/nodes/transform/",
        {
            "name": f"{ns}.ag_sales",
            "description": "d",
            "query": (
                "SELECT sale_id, amount, staff_id, plan_code, region_code "
                f"FROM {ns}.ag_src_sales s"
            ),
            "mode": "published",
            "primary_key": ["sale_id"],
        },
    )
    for node, dimension, join_on, role in [
        (
            "ag_src_sales",
            "ag_date",
            f"{ns}.ag_src_sales.sale_date = {ns}.ag_date.dateint",
            None,
        ),
        (
            "ag_src_sales",
            "ag_date",
            f"{ns}.ag_src_sales.sale_date = {ns}.ag_date.dateint",
            "sold",
        ),
        (
            "ag_sales",
            "ag_employee",
            f"{ns}.ag_sales.staff_id = {ns}.ag_employee.employee_id",
            "staff",
        ),
        (
            "ag_sales",
            "ag_plan",
            f"{ns}.ag_sales.plan_code = {ns}.ag_plan.plan_code",
            "billing",
        ),
        (
            "ag_employee",
            "ag_employee",
            f"{ns}.ag_employee.manager_id = {ns}.ag_employee.employee_id",
            "manager",
        ),
        (
            "ag_employee",
            "ag_office",
            f"{ns}.ag_employee.office_id = {ns}.ag_office.office_id",
            "work",
        ),
        (
            "ag_src_visits",
            "ag_employee",
            f"{ns}.ag_src_visits.staff_id = {ns}.ag_employee.employee_id",
            "staff",
        ),
    ]:
        post(f"/nodes/{ns}.{node}/link", _join(f"{ns}.{dimension}", join_on, role))
    for column, dimension, dimension_column in [
        ("ag_sales/columns/plan_code", "ag_plan", "plan_code"),
        ("ag_employee/columns/country", "ag_country", "name"),
    ]:
        post(
            f"/nodes/{ns}.{column}/link",
            None,
            {
                "dimension_node": f"{ns}.{dimension}",
                "dimension_column": dimension_column,
            },
        )
    for name, query in [
        ("ag_total", f"SELECT SUM(amount) FROM {ns}.ag_sales"),
        ("ag_visit_count", f"SELECT COUNT(visit_id) FROM {ns}.ag_src_visits"),
        ("ag_total_x2", f"SELECT {ns}.ag_total * 2"),
        ("ag_sales_per_visit", f"SELECT {ns}.ag_total / {ns}.ag_visit_count"),
    ]:
        post(
            "/nodes/metric/",
            {"name": f"{ns}.{name}", "description": "d", "query": query},
        )
    return calls


@pytest_asyncio.fixture(scope="module")
async def agreement_client(module__client_with_build_v3):
    """The v3 examples plus the scenario graph in the v3 namespace."""
    client = module__client_with_build_v3
    for path, body, params in graph_calls("v3"):
        response = await client.post(path, json=body, params=params)
        assert response.status_code in (200, 201), response.text
    return client


def _requirement(attribute: str) -> list[dict]:
    return [{"dimension": attribute, "default": f"{attribute} IS NOT NULL"}]


async def _metric_sql(client, kind: str, attribute: str):
    return await client.get(
        "/sql/metrics/v3/",
        params={
            "metrics": [f"v3.{PLAIN_METRICS[kind]}"],
            # Two-fact metrics need a shared dimension.
            "dimensions": DIMENSIONS[kind],
            "filters": [f"{attribute} IS NOT NULL"],
        },
    )


def _requirement_query(prefix: str, kind: str) -> str:
    if kind == "base":
        return f"SELECT SUM(amount) FROM {prefix}ag_sales"
    return f"SELECT {prefix}{PLAIN_METRICS[kind]}"


@pytest.mark.parametrize("scenario", SCENARIOS, ids=[s.id for s in SCENARIOS])
@pytest.mark.asyncio
async def test_save_agrees_with_build(agreement_client, scenario: Scenario):
    """A metric saves with a filter on the attribute iff SQL builds serve it."""
    client = agreement_client
    attribute = f"v3.{scenario.attribute}"
    name = f"v3.ag_req_{scenario.id.replace('-', '_')}"
    saved = await client.post(
        "/nodes/metric/",
        json={
            "name": name,
            "description": "d",
            "query": _requirement_query("v3.", scenario.metric),
            "required_filters": _requirement(attribute),
        },
    )
    built = await _metric_sql(client, scenario.metric, attribute)
    if scenario.can_filter:
        assert saved.status_code == 201, saved.text
        assert built.status_code == 200, built.text
        return
    assert saved.status_code == 422, saved.text
    error = saved.json()["errors"][0]
    assert {"message": error["message"], "debug": error["debug"]} == {
        "message": (
            f"Metric `{name}` has filter requirements that name attributes "
            "that could not be resolved."
        ),
        "debug": {"invalid_filter_requirement_attributes": [attribute]},
    }
    if scenario.fallback is None:
        assert built.status_code == 422, built.text
        return
    # The build uses another role, or breaks.
    fallback = await _metric_sql(client, scenario.metric, f"v3.{scenario.fallback}")
    expected = fallback.json()["sql"]
    if scenario.key_with_role:
        role = attribute.split("[")[1].rstrip("]")
        expected = expected.replace(
            scenario.key_with_role,
            f"{scenario.key_with_role}[{role}]",
        )
    assert built.status_code == 200, built.text
    assert built.json()["sql"] == expected


P = "${prefix}"


def _source_spec(name: str, columns: dict[str, str], links=()) -> SourceSpec:
    return SourceSpec(
        name=name,
        catalog="default",
        schema="agree",
        table=name,
        columns=[ColumnSpec(name=col, type=typ) for col, typ in columns.items()],
        dimension_links=list(links),
    )


def _join_spec(dimension: str, join_on: str, role: str | None = None):
    return DimensionJoinLinkSpec(
        dimension_node=f"{P}{dimension}",
        join_on=join_on,
        role=role,
    )


def graph_specs() -> list:
    """The scenario graph of graph_calls, as deployment specs."""
    sale_date = f"{P}ag_src_sales.sale_date = {P}ag_date.dateint"
    return [
        _source_spec(
            "ag_src_sales",
            {
                "sale_id": "int",
                "amount": "int",
                "account_id": "int",
                "staff_id": "int",
                "plan_code": "string",
                "sale_date": "int",
                "region_code": "string",
            },
            [
                _join_spec("ag_date", sale_date),
                _join_spec("ag_date", sale_date, "sold"),
            ],
        ),
        _source_spec(
            "ag_src_employees",
            {
                "employee_id": "int",
                "country": "string",
                "manager_id": "int",
                "office_id": "int",
            },
        ),
        _source_spec(
            "ag_src_other",
            {"code": "string", "label": "string", "dateint": "int", "month": "int"},
        ),
        _source_spec(
            "ag_src_visits",
            {"visit_id": "int", "staff_id": "int"},
            [
                _join_spec(
                    "ag_employee",
                    f"{P}ag_src_visits.staff_id = {P}ag_employee.employee_id",
                    "staff",
                ),
            ],
        ),
        DimensionSpec(
            name="ag_employee",
            query=(
                "SELECT employee_id, country, manager_id, office_id "
                f"FROM {P}ag_src_employees"
            ),
            primary_key=["employee_id"],
            dimension_links=[
                _join_spec(
                    "ag_employee",
                    f"{P}ag_employee.manager_id = {P}ag_employee.employee_id",
                    "manager",
                ),
                _join_spec(
                    "ag_office",
                    f"{P}ag_employee.office_id = {P}ag_office.office_id",
                    "work",
                ),
                DimensionReferenceLinkSpec(
                    node_column="country",
                    dimension=f"{P}ag_country.name",
                ),
            ],
        ),
        DimensionSpec(
            name="ag_office",
            query=f"SELECT office_id, country AS city FROM {P}ag_src_employees",
            primary_key=["office_id"],
        ),
        DimensionSpec(
            name="ag_plan",
            query=f"SELECT code AS plan_code, label AS tier FROM {P}ag_src_other",
            primary_key=["plan_code"],
        ),
        DimensionSpec(
            name="ag_country",
            query=f"SELECT code AS name, label FROM {P}ag_src_other",
            primary_key=["name"],
        ),
        DimensionSpec(
            name="ag_date",
            query=f"SELECT dateint, month FROM {P}ag_src_other",
            primary_key=["dateint"],
        ),
        DimensionSpec(
            name="ag_island",
            query=f"SELECT dateint, month FROM {P}ag_src_other",
            primary_key=["dateint"],
        ),
        TransformSpec(
            name="ag_sales",
            query=(
                "SELECT sale_id, amount, staff_id, plan_code, region_code "
                f"FROM {P}ag_src_sales s"
            ),
            primary_key=["sale_id"],
            dimension_links=[
                _join_spec(
                    "ag_employee",
                    f"{P}ag_sales.staff_id = {P}ag_employee.employee_id",
                    "staff",
                ),
                DimensionReferenceLinkSpec(
                    node_column="plan_code",
                    dimension=f"{P}ag_plan.plan_code",
                ),
            ],
        ),
        MetricSpec(name="ag_total", query=f"SELECT SUM(amount) FROM {P}ag_sales"),
        MetricSpec(
            name="ag_visit_count",
            query=f"SELECT COUNT(visit_id) FROM {P}ag_src_visits",
        ),
        MetricSpec(name="ag_total_x2", query=f"SELECT {P}ag_total * 2"),
        MetricSpec(
            name="ag_sales_per_visit",
            query=f"SELECT {P}ag_total / {P}ag_visit_count",
        ),
    ]


def requirement_specs(prefix: str) -> list[MetricSpec]:
    """One metric per scenario, on nodes whose names start with `prefix`."""
    return [
        MetricSpec(
            name=f"req_{scenario.id.replace('-', '_')}",
            query=_requirement_query(prefix, scenario.metric),
            required_filters=_requirement(f"{prefix}{scenario.attribute}"),
        )
        for scenario in SCENARIOS
    ]


def _metric_statuses(data: dict, namespace: str) -> dict[str, str]:
    prefix = f"{namespace}.req_"
    return {
        result["name"][len(prefix) :]: result["status"]
        for result in data["results"]
        if result["name"].startswith(prefix)
    }


EXPECTED_STATUSES = {
    scenario.id.replace("-", "_"): "success" if scenario.can_filter else "invalid"
    for scenario in SCENARIOS
}


@pytest.mark.asyncio
async def test_deploy_agrees_with_saved_links(agreement_client):
    """Deployed metrics on saved nodes get the same outcomes as the API."""
    namespace = "ag_on_saved"
    data = await deploy_and_wait(
        agreement_client,
        DeploymentSpec(namespace=namespace, nodes=requirement_specs("v3.")),
    )
    assert _metric_statuses(data, namespace) == EXPECTED_STATUSES


@pytest.mark.asyncio
async def test_deploy_agrees_with_declared_links(agreement_client):
    """Links declared in the same deployment get the same outcomes too."""
    namespace = "ag_declared"
    data = await deploy_and_wait(
        agreement_client,
        DeploymentSpec(
            namespace=namespace,
            nodes=graph_specs() + requirement_specs(P),
        ),
    )
    assert _metric_statuses(data, namespace) == EXPECTED_STATUSES
