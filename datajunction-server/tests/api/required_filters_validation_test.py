"""Validation of filter requirements at write time."""

import pytest


async def _post(client, **fields):
    query = (await client.get("/nodes/v3.total_revenue/")).json()["query"]
    return await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.rf_invalid",
            "description": "d",
            "query": query,
            "mode": "published",
            **fields,
        },
    )


@pytest.mark.asyncio
async def test_attribute_in_both_lists_rejected(client_with_build_v3):
    response = await _post(
        client_with_build_v3,
        required_filters=[{"dimension": "v3.date.date_id[order]"}],
        recommended_filters=[{"dimension": "v3.date.date_id[order]"}],
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.rf_invalid` lists v3.date.date_id[order] in both required_filters and recommended_filters."
    )


@pytest.mark.asyncio
async def test_unparseable_default_rejected(client_with_build_v3):
    response = await _post(
        client_with_build_v3,
        required_filters=[
            {"dimension": "v3.date.date_id[order]", "default": "date_id = ="},
        ],
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.rf_invalid` has a default filter that does not parse: date_id = ="
    )


@pytest.mark.asyncio
async def test_unknown_dimension_rejected(client_with_build_v3):
    response = await _post(
        client_with_build_v3,
        required_filters=[{"dimension": "v3.nope.missing"}],
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.rf_invalid` has filter requirements that name attributes that could not be resolved."
    )
    assert response.json()["errors"][0]["debug"] == {
        "invalid_filter_requirement_attributes": ["v3.nope.missing"],
    }


@pytest.mark.asyncio
async def test_default_on_unreachable_attribute_rejected(client_with_build_v3):
    response = await _post(
        client_with_build_v3,
        required_filters=[
            {
                "dimension": "v3.date.date_id[order]",
                "default": "v3.date.date_id[order] = 1 AND v3.nope.missing = 1",
            },
        ],
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.rf_invalid` has filter requirements that name attributes that could not be resolved."
    )
    assert response.json()["errors"][0]["debug"] == {
        "invalid_filter_requirement_attributes": ["v3.nope.missing"],
    }


@pytest.mark.asyncio
async def test_default_with_bare_column_rejected(client_with_build_v3):
    response = await _post(
        client_with_build_v3,
        required_filters=[
            {"dimension": "v3.date.date_id[order]", "default": "date_id = 20250101"},
        ],
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.rf_invalid` has a default filter with columns not in "
        "node.column form (date_id): date_id = 20250101"
    )


@pytest.mark.asyncio
async def test_bare_dimension_rejected(client_with_build_v3):
    response = await _post(
        client_with_build_v3,
        required_filters=[{"dimension": "order_date"}],
    )
    assert response.status_code == 422
    assert response.json()["message"] == (
        "Metric `v3.rf_invalid` has a filter requirement on order_date, "
        "which is not in node.column form."
    )


UNRESOLVED = (
    "Metric `v3.rf_invalid` has filter requirements that name attributes "
    "that could not be resolved."
)
UNKNOWN_ROLE = {
    "invalid_filter_requirement_attributes": ["v3.date.date_id[nonexistent]"],
}
ROLE_ENTRIES = [
    pytest.param({"dimension": "v3.date.date_id[nonexistent]"}, id="dimension"),
    pytest.param(
        {
            "dimension": "v3.date.date_id[order]",
            "default": (
                "v3.date.date_id[order] = 1 AND v3.date.date_id[nonexistent] = 1"
            ),
        },
        id="default",
    ),
]


@pytest.mark.parametrize("entry", ROLE_ENTRIES)
@pytest.mark.asyncio
async def test_unknown_role_rejected(client_with_build_v3, entry):
    response = await _post(client_with_build_v3, required_filters=[entry])
    assert response.status_code == 422
    assert response.json()["message"] == UNRESOLVED
    assert response.json()["errors"][0]["debug"] == UNKNOWN_ROLE


@pytest.mark.parametrize("entry", ROLE_ENTRIES)
@pytest.mark.asyncio
async def test_unknown_role_rejected_on_update(client_with_build_v3, entry):
    created = await _post(client_with_build_v3)
    assert created.status_code == 201, created.text
    response = await client_with_build_v3.patch(
        "/nodes/v3.rf_invalid/",
        json={"recommended_filters": [entry]},
    )
    assert response.status_code == 422
    assert response.json()["message"] == UNRESOLVED
    assert response.json()["errors"][0]["debug"] == UNKNOWN_ROLE


@pytest.mark.parametrize(
    "entry",
    [
        {
            "dimension": "v3.date.date_id[order]",
            "default": "v3.date.date_id[order] = 20250101",
        },
        # No role is the null role, which a role-free link satisfies.
        {
            "dimension": "v3.product.category",
            "default": "v3.product.category = 'Books'",
        },
    ],
    ids=["known-role", "no-role-free-link"],
)
@pytest.mark.asyncio
async def test_known_role_or_no_role_accepted(client_with_build_v3, entry):
    response = await _post(client_with_build_v3, required_filters=[entry])
    assert response.status_code == 201, response.text
    assert response.json()["required_filters"] == [
        {**entry, "dimension": [entry["dimension"]]},
    ]


@pytest.mark.parametrize(
    "entry",
    [
        {"dimension": "v3.date.date_id"},
        {"dimension": "v3.date.date_id", "default": "v3.date.date_id = 20250101"},
    ],
    ids=["dimension", "default"],
)
@pytest.mark.asyncio
async def test_no_role_rejected_when_only_roles_reach(client_with_build_v3, entry):
    """v3.date is linked from order_details only through roles."""
    response = await _post(client_with_build_v3, required_filters=[entry])
    assert response.status_code == 422
    assert response.json()["message"] == UNRESOLVED
    assert response.json()["errors"][0]["debug"] == {
        "invalid_filter_requirement_attributes": ["v3.date.date_id"],
    }


MULTI_HOP_ENTRIES = [
    pytest.param(
        {"dimension": "v3.location.country[customer->home]"},
        id="dimension",
    ),
    pytest.param(
        {
            "dimension": "v3.location.country[customer->home]",
            "default": "v3.location.country[customer->home] = 'US'",
        },
        id="default",
    ),
]


@pytest.mark.parametrize("entry", MULTI_HOP_ENTRIES)
@pytest.mark.asyncio
async def test_multi_hop_role_accepted(client_with_build_v3, entry):
    response = await _post(client_with_build_v3, required_filters=[entry])
    assert response.status_code == 201, response.text
    assert response.json()["required_filters"] == [
        {"default": None, **entry, "dimension": [entry["dimension"]]},
    ]


@pytest.mark.parametrize("entry", MULTI_HOP_ENTRIES)
@pytest.mark.asyncio
async def test_multi_hop_role_accepted_on_update(client_with_build_v3, entry):
    created = await _post(client_with_build_v3)
    assert created.status_code == 201, created.text
    response = await client_with_build_v3.patch(
        "/nodes/v3.rf_invalid/",
        json={"recommended_filters": [entry]},
    )
    assert response.status_code == 200, response.text
    assert response.json()["recommended_filters"] == [
        {"default": None, **entry, "dimension": [entry["dimension"]]},
    ]


@pytest.mark.parametrize(
    "attribute",
    [
        "v3.location.country[customer->nonexistent]",
        "v3.location.country[nonexistent->home]",
        # `customer->home` lands on v3.location, not v3.date.
        "v3.date.date_id[customer->home]",
        "v3.location.country[nonexistent]",
    ],
    ids=["unknown-last", "unknown-first", "wrong-node", "unknown-single"],
)
@pytest.mark.asyncio
async def test_bad_role_chain_rejected(client_with_build_v3, attribute):
    response = await _post(
        client_with_build_v3,
        required_filters=[{"dimension": attribute}],
    )
    assert response.status_code == 422
    assert response.json()["message"] == UNRESOLVED
    assert response.json()["errors"][0]["debug"] == {
        "invalid_filter_requirement_attributes": [attribute],
    }


async def _self_link_setup(client):
    """Sales link to employee as `staff`; employee links to itself as `manager`."""
    for path, body in [
        (
            "/nodes/source/",
            {
                "name": "default.employees_table",
                "catalog": "default",
                "schema_": "public",
                "table": "employees",
                "columns": [
                    {"name": "employee_id", "type": "int"},
                    {"name": "country", "type": "string"},
                    {"name": "manager_id", "type": "int"},
                ],
            },
        ),
        (
            "/nodes/source/",
            {
                "name": "default.sales_table",
                "catalog": "default",
                "schema_": "public",
                "table": "sales",
                "columns": [
                    {"name": "staff_id", "type": "int"},
                    {"name": "amount", "type": "int"},
                ],
            },
        ),
        (
            "/nodes/dimension/",
            {
                "name": "default.employee",
                "mode": "published",
                "primary_key": ["employee_id"],
                "query": (
                    "SELECT employee_id, country, manager_id "
                    "FROM default.employees_table"
                ),
            },
        ),
        (
            "/nodes/default.employee/link",
            {
                "dimension_node": "default.employee",
                "join_type": "left",
                "join_on": (
                    "default.employee.manager_id = default.employee.employee_id"
                ),
                "role": "manager",
            },
        ),
        (
            "/nodes/default.sales_table/link",
            {
                "dimension_node": "default.employee",
                "join_type": "left",
                "join_on": (
                    "default.sales_table.staff_id = default.employee.employee_id"
                ),
                "role": "staff",
            },
        ),
    ]:
        response = await client.post(path, json=body)
        assert response.status_code in (200, 201), response.text


@pytest.mark.parametrize(
    "attribute,status_code",
    [
        # SQL builds skip a hop back to employee, so this chain is refused.
        ("default.employee.country[staff->manager]", 422),
        ("default.employee.country[staff]", 201),
    ],
    ids=["revisits-node", "one-hop"],
)
@pytest.mark.asyncio
async def test_self_link_chain(client_with_service_setup, attribute, status_code):
    client = client_with_service_setup
    await _self_link_setup(client)
    response = await client.post(
        "/nodes/metric/",
        json={
            "name": "default.total_sales",
            "description": "d",
            "query": "SELECT SUM(amount) FROM default.sales_table",
            "mode": "published",
            "required_filters": [{"dimension": attribute}],
        },
    )
    assert response.status_code == status_code, response.text
    if status_code == 422:
        assert response.json()["message"] == (
            "Metric `default.total_sales` has filter requirements that name "
            "attributes that could not be resolved."
        )
        assert response.json()["errors"][0]["debug"] == {
            "invalid_filter_requirement_attributes": [attribute],
        }
    else:
        assert response.json()["required_filters"] == [
            {"dimension": [attribute], "default": None},
        ]


UNCOVERED_DEFAULTS = [
    pytest.param(
        {
            "dimension": "v3.date.date_id[order]",
            "default": "v3.product.category = 'Books'",
        },
        "v3.date.date_id[order]",
        id="other-attribute",
    ),
    pytest.param(
        {
            "dimension": "v3.date.date_id[order]",
            "default": "v3.date.date_id[customer->registration] = 1",
        },
        "v3.date.date_id[order]",
        id="other-role",
    ),
]


def _uncovered_errors(entry: dict, attributes: str) -> list[dict]:
    message = (
        "Metric `v3.rf_invalid` has a default filter that does not filter on "
        f"{attributes}: {entry['default']}"
    )
    return [
        {"code": "INVALID_COLUMN", "message": message, "debug": None, "context": ""},
    ]


@pytest.mark.parametrize("entry,attributes", UNCOVERED_DEFAULTS)
@pytest.mark.asyncio
async def test_default_on_other_attribute_rejected(
    client_with_build_v3,
    entry,
    attributes,
):
    response = await _post(client_with_build_v3, required_filters=[entry])
    assert response.status_code == 422
    errors = _uncovered_errors(entry, attributes)
    assert response.json()["message"] == errors[0]["message"]
    assert response.json()["errors"] == errors


@pytest.mark.parametrize("entry,attributes", UNCOVERED_DEFAULTS)
@pytest.mark.asyncio
async def test_default_on_other_attribute_rejected_on_update(
    client_with_build_v3,
    entry,
    attributes,
):
    created = await _post(client_with_build_v3)
    assert created.status_code == 201, created.text
    response = await client_with_build_v3.patch(
        "/nodes/v3.rf_invalid/",
        json={"recommended_filters": [entry]},
    )
    assert response.status_code == 422
    errors = _uncovered_errors(entry, attributes)
    assert response.json()["message"] == errors[0]["message"]
    assert response.json()["errors"] == errors


async def _reach_setup(client):
    """Add a dimension two hops away and one that nothing links to."""
    for path, body in [
        (
            "/nodes/dimension/",
            {
                "name": "v3.rf_country",
                "description": "d",
                "mode": "published",
                "primary_key": ["country"],
                "query": "SELECT country, region FROM v3.src_locations",
            },
        ),
        (
            "/nodes/v3.location/link",
            {
                "dimension_node": "v3.rf_country",
                "join_type": "left",
                "join_on": "v3.location.country = v3.rf_country.country",
            },
        ),
        (
            "/nodes/dimension/",
            {
                "name": "v3.rf_island",
                "description": "d",
                "mode": "published",
                "primary_key": ["location_id"],
                "query": "SELECT location_id, city FROM v3.src_locations",
            },
        ),
    ]:
        response = await client.post(path, json=body)
        assert response.status_code in (200, 201), response.text


REACH_ENTRIES = [
    pytest.param(
        {"dimension": "v3.rf_island.city"},
        ["v3.rf_island.city"],
        id="unlinked",
    ),
    pytest.param(
        {
            "dimension": "v3.date.date_id[order]",
            "default": "v3.date.date_id[order] = 1 OR v3.rf_island.city = 'x'",
        },
        ["v3.rf_island.city"],
        id="unlinked-default",
    ),
    pytest.param({"dimension": "v3.product.category"}, [], id="one-hop"),
    pytest.param({"dimension": "v3.rf_country.region[from]"}, [], id="two-hops"),
    pytest.param({"dimension": "v3.order_details.status"}, [], id="own-table"),
]


def _check_reach(response, entry: dict, invalid: list[str], field: str) -> None:
    if invalid:
        assert response.status_code == 422, response.text
        assert response.json()["message"] == UNRESOLVED
        assert response.json()["errors"][0]["debug"] == {
            "invalid_filter_requirement_attributes": invalid,
        }
    else:
        assert response.status_code in (200, 201), response.text
        assert response.json()[field] == [
            {"default": None, **entry, "dimension": [entry["dimension"]]},
        ]


@pytest.mark.parametrize("entry,invalid", REACH_ENTRIES)
@pytest.mark.asyncio
async def test_attribute_must_be_reachable(client_with_build_v3, entry, invalid):
    await _reach_setup(client_with_build_v3)
    response = await _post(client_with_build_v3, required_filters=[entry])
    _check_reach(response, entry, invalid, "required_filters")


@pytest.mark.parametrize("entry,invalid", REACH_ENTRIES)
@pytest.mark.asyncio
async def test_attribute_must_be_reachable_on_update(
    client_with_build_v3,
    entry,
    invalid,
):
    await _reach_setup(client_with_build_v3)
    created = await _post(client_with_build_v3)
    assert created.status_code == 201, created.text
    response = await client_with_build_v3.patch(
        "/nodes/v3.rf_invalid/",
        json={"recommended_filters": [entry]},
    )
    _check_reach(response, entry, invalid, "recommended_filters")


async def _reference_setup(client):
    """
    v3.order_details.status references v3.rf_plan.plan_label, and
    v3.customer.name references v3.rf_tier.tier_label.
    """
    for name, label in [("v3.rf_plan", "plan_label"), ("v3.rf_tier", "tier_label")]:
        response = await client.post(
            "/nodes/dimension/",
            json={
                "name": name,
                "description": "d",
                "mode": "published",
                "primary_key": [label],
                "query": f"SELECT city AS {label}, region FROM v3.src_locations",
            },
        )
        assert response.status_code == 201, response.text
    for column, dimension, label in [
        ("v3.order_details/columns/status", "v3.rf_plan", "plan_label"),
        ("v3.customer/columns/name", "v3.rf_tier", "tier_label"),
    ]:
        response = await client.post(
            f"/nodes/{column}/link",
            params={"dimension_node": dimension, "dimension_column": label},
        )
        assert response.status_code == 201, response.text


REFERENCE_ENTRIES = [
    pytest.param({"dimension": ["v3.rf_plan.plan_label"]}, [], id="referenced"),
    pytest.param(
        {
            "dimension": ["v3.date.date_id[order]", "v3.rf_plan.plan_label"],
            "default": "v3.rf_plan.plan_label = 'checkout'",
        },
        [],
        id="referenced-default",
    ),
    # SQL builds read the referenced column under any role.
    pytest.param({"dimension": ["v3.rf_plan.plan_label[any]"]}, [], id="any-role"),
    # SQL builds read only the referenced column.
    pytest.param(
        {"dimension": ["v3.rf_plan.region"]},
        ["v3.rf_plan.region"],
        id="other-column",
    ),
    # SQL builds do not follow a reference after a join.
    pytest.param(
        {"dimension": ["v3.rf_tier.tier_label"]},
        ["v3.rf_tier.tier_label"],
        id="join-then-reference",
    ),
]


def _check_reference(response, entry: dict, invalid: list[str], field: str) -> None:
    if invalid:
        assert response.status_code == 422, response.text
        assert response.json()["message"] == UNRESOLVED
        assert response.json()["errors"][0]["debug"] == {
            "invalid_filter_requirement_attributes": invalid,
        }
    else:
        assert response.status_code in (200, 201), response.text
        assert response.json()[field] == [{"default": None, **entry}]


@pytest.mark.parametrize("entry,invalid", REFERENCE_ENTRIES)
@pytest.mark.asyncio
async def test_reference_link_counts(client_with_build_v3, entry, invalid):
    await _reference_setup(client_with_build_v3)
    response = await _post(client_with_build_v3, required_filters=[entry])
    _check_reference(response, entry, invalid, "required_filters")


@pytest.mark.parametrize("entry,invalid", REFERENCE_ENTRIES)
@pytest.mark.asyncio
async def test_reference_link_counts_on_update(client_with_build_v3, entry, invalid):
    await _reference_setup(client_with_build_v3)
    created = await _post(client_with_build_v3)
    assert created.status_code == 201, created.text
    response = await client_with_build_v3.patch(
        "/nodes/v3.rf_invalid/",
        json={"recommended_filters": [entry]},
    )
    _check_reference(response, entry, invalid, "recommended_filters")


@pytest.mark.asyncio
async def test_reference_link_counts_for_derived_metric(client_with_build_v3):
    await _reference_setup(client_with_build_v3)
    entry = {"dimension": ["v3.rf_plan.plan_label"], "default": None}
    response = await client_with_build_v3.post(
        "/nodes/metric/",
        json={
            "name": "v3.rf_derived",
            "description": "d",
            "query": "SELECT v3.total_revenue * 2",
            "mode": "published",
            "required_filters": [entry],
        },
    )
    assert response.status_code == 201, response.text
    assert response.json()["required_filters"] == [entry]


async def _upstream_link_setup(client):
    """
    v3.rf_event_total reads the transform v3.rf_account_events, whose
    upstream source v3.rf_src_audit links to v3.rf_audit_date twice: without
    a role and as `audit`. Nothing links v3.rf_event_total's parent directly.
    """
    calls = [
        (
            "/nodes/source/",
            {
                "name": "v3.rf_src_audit_dates",
                "description": "d",
                "columns": [{"name": "dateint", "type": "int"}],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "rf_audit_dates",
            },
        ),
        (
            "/nodes/dimension/",
            {
                "name": "v3.rf_audit_date",
                "description": "d",
                "query": "SELECT dateint FROM v3.rf_src_audit_dates",
                "mode": "published",
                "primary_key": ["dateint"],
            },
        ),
        (
            "/nodes/source/",
            {
                "name": "v3.rf_src_audit",
                "description": "d",
                "columns": [
                    {"name": "audit_date", "type": "int"},
                    {"name": "account_id", "type": "int"},
                ],
                "mode": "published",
                "catalog": "default",
                "schema_": "v3",
                "table": "rf_audit_log",
            },
        ),
        (
            "/nodes/v3.rf_src_audit/link",
            {
                "dimension_node": "v3.rf_audit_date",
                "join_type": "inner",
                "join_on": "v3.rf_src_audit.audit_date = v3.rf_audit_date.dateint",
            },
        ),
        (
            "/nodes/v3.rf_src_audit/link",
            {
                "dimension_node": "v3.rf_audit_date",
                "join_type": "inner",
                "join_on": "v3.rf_src_audit.audit_date = v3.rf_audit_date.dateint",
                "role": "audit",
            },
        ),
        (
            "/nodes/transform/",
            {
                "name": "v3.rf_account_events",
                "description": "d",
                "query": (
                    "SELECT account_id, COUNT(*) AS event_count "
                    "FROM v3.rf_src_audit GROUP BY account_id"
                ),
                "mode": "published",
                "primary_key": ["account_id"],
            },
        ),
    ]
    for path, body in calls:
        response = await client.post(path, json=body)
        assert response.status_code in (200, 201), response.text


async def _post_event_total(client, **fields):
    return await client.post(
        "/nodes/metric/",
        json={
            "name": "v3.rf_event_total",
            "description": "d",
            "query": "SELECT SUM(event_count) FROM v3.rf_account_events",
            "mode": "published",
            **fields,
        },
    )


UPSTREAM_ENTRIES = [
    pytest.param({"dimension": ["v3.rf_audit_date.dateint"]}, [], id="entry"),
    pytest.param(
        {
            "dimension": ["v3.rf_audit_date.dateint"],
            "default": "v3.rf_audit_date.dateint >= 20260101",
        },
        [],
        id="default",
    ),
    # A pushed down role breaks the SQL.
    pytest.param(
        {"dimension": ["v3.rf_audit_date.dateint[audit]"]},
        ["v3.rf_audit_date.dateint[audit]"],
        id="upstream-role",
    ),
    pytest.param(
        {"dimension": ["v3.rf_audit_date.dateint[nope]"]},
        ["v3.rf_audit_date.dateint[nope]"],
        id="unknown-role",
    ),
    pytest.param(
        {"dimension": ["v3.date.date_id"]},
        ["v3.date.date_id"],
        id="not-in-lineage",
    ),
]


def _upstream_errors(invalid: list[str]) -> dict:
    return {
        "message": (
            "Metric `v3.rf_event_total` has filter requirements that name "
            "attributes that could not be resolved."
        ),
        "debug": {"invalid_filter_requirement_attributes": invalid},
    }


def _check_upstream(response, entry: dict, invalid: list[str], field: str) -> None:
    if invalid:
        assert response.status_code == 422, response.text
        error = response.json()["errors"][0]
        assert {"message": error["message"], "debug": error["debug"]} == (
            _upstream_errors(invalid)
        )
    else:
        assert response.status_code in (200, 201), response.text
        assert response.json()[field] == [{"default": None, **entry}]


@pytest.mark.parametrize("entry,invalid", UPSTREAM_ENTRIES)
@pytest.mark.asyncio
async def test_upstream_link_counts(client_with_build_v3, entry, invalid):
    await _upstream_link_setup(client_with_build_v3)
    response = await _post_event_total(client_with_build_v3, required_filters=[entry])
    _check_upstream(response, entry, invalid, "required_filters")


@pytest.mark.parametrize("entry,invalid", UPSTREAM_ENTRIES)
@pytest.mark.asyncio
async def test_upstream_link_counts_on_update(client_with_build_v3, entry, invalid):
    await _upstream_link_setup(client_with_build_v3)
    created = await _post_event_total(client_with_build_v3)
    assert created.status_code == 201, created.text
    response = await client_with_build_v3.patch(
        "/nodes/v3.rf_event_total/",
        json={"recommended_filters": [entry]},
    )
    _check_upstream(response, entry, invalid, "recommended_filters")
