"""Tests for SQL generation with self-join dimension links."""

import pytest
from httpx import AsyncClient

from tests.construction.build_v3 import assert_sql_equal


@pytest.mark.asyncio
async def test_direct_self_join_employee_manager(
    client_with_service_setup: AsyncClient,
) -> None:
    """
    Test basic self-join SQL generation for employee -> manager relationship.
    """
    # Create source table for employees
    response = await client_with_service_setup.post(
        "/nodes/source/",
        json={
            "name": "default.employees_table",
            "display_name": "Employees Table",
            "catalog": "default",
            "schema_": "public",
            "table": "employees",
            "columns": [
                {"name": "employee_id", "type": "int"},
                {"name": "employee_name", "type": "string"},
                {"name": "manager_employee_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create employee dimension with self-join
    response = await client_with_service_setup.post(
        "/nodes/dimension/",
        json={
            "name": "default.employee",
            "display_name": "Employee",
            "mode": "published",
            "primary_key": ["employee_id"],
            "query": "SELECT employee_id, employee_name, manager_employee_id FROM default.employees_table",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create self-join dimension link
    response = await client_with_service_setup.post(
        "/nodes/default.employee/link",
        json={
            "dimension_node": "default.employee",
            "join_type": "left",
            "join_on": "default.employee.manager_employee_id = default.employee.employee_id",
            "role": "manager",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create a metric based on employee count
    response = await client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "name": "default.employee_count",
            "display_name": "Employee Count",
            "mode": "published",
            "query": "SELECT COUNT(DISTINCT employee_id) FROM default.employee",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Request SQL with the metric and self-join dimension
    response = await client_with_service_setup.get(
        "/sql/measures/v3",
        params={
            "metrics": ["default.employee_count"],
            "dimensions": [
                "default.employee.employee_name[manager]",
            ],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert_sql_equal(
        data["grain_groups"][0]["sql"],
        """
        WITH default_employee AS (
          SELECT
            employee_id,
            employee_name,
            manager_employee_id
          FROM default.public.employees
        )
        SELECT
          t2.employee_name employee_name_manager,
          t1.employee_id
        FROM default_employee t1
        LEFT OUTER JOIN default_employee t2 ON t1.manager_employee_id = t2.employee_id
        GROUP BY
          t2.employee_name, t1.employee_id
        """,
    )

    # Request SQL with the metric and self-join dimension
    response = await client_with_service_setup.get(
        "/sql/metrics/v3",
        params={
            "metrics": ["default.employee_count"],
            "dimensions": [
                "default.employee.employee_name[manager]",
            ],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert_sql_equal(
        data["sql"],
        """
        WITH default_employee AS (
          SELECT
            employee_id,
            employee_name,
            manager_employee_id
          FROM default.public.employees
        ),
        employee_0 AS (
          SELECT
            t2.employee_name employee_name_manager,
            t1.employee_id
          FROM default_employee t1
          LEFT OUTER JOIN default_employee t2 ON t1.manager_employee_id = t2.employee_id
          GROUP BY
            t2.employee_name, t1.employee_id
        )
        SELECT
          employee_0.employee_name_manager AS employee_name_manager,
          COUNT(DISTINCT employee_0.employee_id) AS employee_count
        FROM employee_0
        GROUP BY
          employee_0.employee_name_manager
        """,
    )


@pytest.mark.asyncio
async def test_indirect_self_join_employee_manager(
    client_with_service_setup: AsyncClient,
) -> None:
    """
    Test that a metric using a self-join dimension generates correct SQL.
    """
    # Create source
    response = await client_with_service_setup.post(
        "/nodes/source/",
        json={
            "name": "default.sales_table",
            "catalog": "default",
            "schema_": "public",
            "table": "sales",
            "columns": [
                {"name": "sale_id", "type": "int"},
                {"name": "amount", "type": "double"},
                {"name": "employee_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text

    response = await client_with_service_setup.post(
        "/nodes/source/",
        json={
            "name": "default.emp_table",
            "catalog": "default",
            "schema_": "public",
            "table": "employees",
            "columns": [
                {"name": "employee_id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "manager_id", "type": "int"},
            ],
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create employee dimension
    response = await client_with_service_setup.post(
        "/nodes/dimension/",
        json={
            "name": "default.emp",
            "mode": "published",
            "primary_key": ["employee_id"],
            "query": "SELECT employee_id, name, manager_id FROM default.emp_table",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create self-join link
    response = await client_with_service_setup.post(
        "/nodes/default.emp/link",
        json={
            "dimension_node": "default.emp",
            "join_type": "left",
            "join_on": "default.emp.manager_id = default.emp.employee_id",
            "role": "manager",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create transform that joins sales to employee
    response = await client_with_service_setup.post(
        "/nodes/transform/",
        json={
            "name": "default.sales_with_emp",
            "mode": "published",
            "query": """
                SELECT
                    s.sale_id,
                    s.amount,
                    s.employee_id,
                    e.name,
                    e.manager_id
                FROM default.sales_table s
                LEFT JOIN default.emp_table e ON s.employee_id = e.employee_id
            """,
        },
    )
    assert response.status_code in (200, 201), response.text

    # Link transform to employee dimension
    response = await client_with_service_setup.post(
        "/nodes/default.sales_with_emp/link",
        json={
            "dimension_node": "default.emp",
            "join_type": "left",
            "join_on": "default.sales_with_emp.employee_id = default.emp.employee_id",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Create metric
    response = await client_with_service_setup.post(
        "/nodes/metric/",
        json={
            "name": "default.total_sales",
            "mode": "published",
            "query": "SELECT SUM(amount) FROM default.sales_with_emp",
        },
    )
    assert response.status_code in (200, 201), response.text

    # Test /sql/measures/v3 - validates self-join through indirect dimension link
    response = await client_with_service_setup.get(
        "/sql/measures/v3",
        params={
            "metrics": ["default.total_sales"],
            "dimensions": [
                "default.emp.name[manager]",
            ],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert_sql_equal(
        data["grain_groups"][0]["sql"],
        """
        WITH default_emp AS (
          SELECT
            employee_id,
            name
          FROM default.public.employees
        ),
        default_sales_with_emp AS (
          SELECT
            s.amount,
            s.employee_id
          FROM default.public.sales s
          LEFT JOIN default.public.employees e ON s.employee_id = e.employee_id
        )
        SELECT
          t2.name name_manager,
          SUM(t1.amount) amount_sum_b1a226f0
        FROM default_sales_with_emp t1
        LEFT OUTER JOIN default_emp t2 ON t1.employee_id = t2.employee_id
        GROUP BY
          t2.name
        """,
    )

    # Test /sql/metrics/v3 - validates full aggregation with self-join
    response = await client_with_service_setup.get(
        "/sql/metrics/v3",
        params={
            "metrics": ["default.total_sales"],
            "dimensions": [
                "default.emp.name[manager]",
            ],
        },
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert_sql_equal(
        data["sql"],
        """
        WITH default_emp AS (
          SELECT
            employee_id,
            name
          FROM default.public.employees
        ),
        default_sales_with_emp AS (
          SELECT
            s.amount,
            s.employee_id
          FROM default.public.sales s
          LEFT JOIN default.public.employees e ON s.employee_id = e.employee_id
        ),
        sales_with_emp_0 AS (
          SELECT
            t2.name name_manager,
            SUM(t1.amount) amount_sum_b1a226f0
          FROM default_sales_with_emp t1 LEFT OUTER JOIN default_emp t2 ON t1.employee_id = t2.employee_id
          GROUP BY  t2.name
        )
        SELECT
          sales_with_emp_0.name_manager AS name_manager,
          SUM(sales_with_emp_0.amount_sum_b1a226f0) AS total_sales
        FROM sales_with_emp_0
        GROUP BY
          sales_with_emp_0.name_manager
        """,
    )


@pytest.mark.asyncio
async def test_self_join_validation_in_deployment(
    client_with_service_setup: AsyncClient,
) -> None:
    """
    Test self-join dimension links work in deployment specs.
    """
    deployment_spec = {
        "namespace": "test_selfjoin",
        "nodes": [
            {
                "node_type": "source",
                "name": "employee_src",
                "catalog": "default",
                "schema_": "public",
                "table": "employees",
                "columns": [
                    {"name": "emp_id", "type": "int"},
                    {"name": "emp_name", "type": "string"},
                    {"name": "mgr_id", "type": "int"},
                ],
            },
            {
                "node_type": "dimension",
                "name": "employee_dim",
                "mode": "published",
                "primary_key": ["emp_id"],
                "query": "SELECT emp_id, emp_name, mgr_id FROM ${prefix}employee_src",
                "dimension_links": [
                    {
                        "type": "join",
                        "dimension_node": "${prefix}employee_dim",
                        "join_type": "left",
                        "join_on": "${prefix}employee_dim.mgr_id = ${prefix}employee_dim.emp_id",
                        "role": "manager",
                    },
                ],
            },
        ],
    }

    response = await client_with_service_setup.post(
        "/deployments/",
        json=deployment_spec,
    )
    assert response.status_code in (200, 201), response.text
    data = response.json()

    # Wait for deployment to complete
    import asyncio

    deployment_id = data["uuid"]
    for _ in range(30):
        status_response = await client_with_service_setup.get(
            f"/deployments/{deployment_id}",
        )
        status_data = status_response.json()
        if status_data["status"] in ("success", "failed"):
            break
        await asyncio.sleep(0.1)

    # Verify deployment succeeded
    assert status_data["status"] == "success", f"Deployment failed: {status_data}"
    assert "results" in status_data
    results = status_data["results"]

    # Find the dimension link result
    link_results = [r for r in results if r["deploy_type"] == "link"]
    assert len(link_results) == 1, f"Expected 1 link result, got {len(link_results)}"
    assert link_results[0]["status"] == "success"

    # Verify the dimension node
    response = await client_with_service_setup.get("/nodes/test_selfjoin.employee_dim")
    assert response.status_code == 200
    node_data = response.json()

    links = node_data["dimension_links"]
    assert len(links) == 1
    assert links[0]["role"] == "manager"
    assert links[0]["dimension"]["name"] == "test_selfjoin.employee_dim"
