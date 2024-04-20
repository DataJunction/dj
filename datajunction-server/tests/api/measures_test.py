"""
Tests for the namespaces API.
"""
from typing import Dict

import pytest
from httpx import AsyncClient


@pytest.fixture
def completed_repairs_measure() -> Dict:
    """
    Test ``GET /measures/``.
    """
    return {
        "name": "completed_repairs",
        "description": "Number of completed repairs",
        "columns": [
            {
                "node": "default.regional_level_agg",
                "column": "completed_repairs",
            },
        ],
    }


@pytest.fixture
def failed_measure() -> Dict:
    """
    Measure that will fail due to one of the columns not existing
    """
    return {
        "name": "completed_repairs2",
        "description": "Number of completed repairs",
        "columns": [
            {
                "node": "default.regional_level_agg",
                "column": "completed_repairs",
            },
            {
                "node": "default.national_level_agg",
                "column": "completed_repairs",
            },
        ],
    }


@pytest.mark.asyncio
async def test_list_all_measures(
    client_with_roads: AsyncClient,
    completed_repairs_measure: Dict,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Test ``GET /measures/``.
    """
    response = await client_with_roads.get("/measures/")
    assert response.status_code in (200, 201)
    assert response.json() == []

    await client_with_roads.post("/measures/", json=completed_repairs_measure)

    response = await client_with_roads.get("/measures/")
    assert response.status_code in (200, 201)
    assert response.json() == ["completed_repairs"]

    response = await client_with_roads.get("/measures/?prefix=comp")
    assert response.status_code in (200, 201)
    assert response.json() == ["completed_repairs"]

    response = await client_with_roads.get("/measures/?prefix=xyz")
    assert response.status_code in (200, 201)
    assert response.json() == []

    response = await client_with_roads.get("/measures/completed_repairs")
    assert response.status_code in (200, 201)
    assert response.json() == {
        "additive": "non-additive",
        "columns": [
            {
                "name": "completed_repairs",
                "node": "default.regional_level_agg",
                "type": "bigint",
            },
        ],
        "description": "Number of completed repairs",
        "display_name": "Completed Repairs",
        "name": "completed_repairs",
    }

    response = await client_with_roads.get("/measures/random_measure")
    assert response.status_code >= 400
    assert (
        response.json()["message"]
        == "Measure with name `random_measure` does not exist"
    )


@pytest.mark.asyncio
async def test_create_measure(
    client_with_roads: AsyncClient,
    completed_repairs_measure: Dict,  # pylint: disable=redefined-outer-name
    failed_measure: Dict,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Test ``POST /measures/``.
    """
    # Successful measure creation
    response = await client_with_roads.post(
        "/measures/",
        json=completed_repairs_measure,
    )
    assert response.status_code in (200, 201)
    assert response.json() == {
        "additive": "non-additive",
        "columns": [
            {
                "name": "completed_repairs",
                "node": "default.regional_level_agg",
                "type": "bigint",
            },
        ],
        "description": "Number of completed repairs",
        "display_name": "Completed Repairs",
        "name": "completed_repairs",
    }

    # Creating the same measure again will fail
    response = await client_with_roads.post(
        "/measures/",
        json=completed_repairs_measure,
    )
    assert response.status_code >= 400
    assert response.json()["message"] == "Measure `completed_repairs` already exists!"

    # Failed measure creation
    response = await client_with_roads.post("/measures/", json=failed_measure)
    assert response.status_code >= 400
    assert response.json()["message"] == (
        "Column `completed_repairs` does not exist on node `default.national_level_agg`"
    )


@pytest.mark.asyncio
async def test_edit_measure(
    client_with_roads: AsyncClient,
    completed_repairs_measure: Dict,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Test ``PATCH /measures/{name}``.
    """
    await client_with_roads.post("/measures", json=completed_repairs_measure)

    # Successfully edit measure
    response = await client_with_roads.patch(
        "/measures/completed_repairs",
        json={
            "additive": "additive",
            "display_name": "blah",
            "description": "random description",
        },
    )
    assert response.status_code in (200, 201)
    assert response.json() == {
        "additive": "additive",
        "columns": [
            {
                "name": "completed_repairs",
                "node": "default.regional_level_agg",
                "type": "bigint",
            },
        ],
        "description": "random description",
        "display_name": "blah",
        "name": "completed_repairs",
    }

    response = await client_with_roads.patch(
        "/measures/completed_repairs",
        json={
            "additive": "non-additive",
            "columns": [],
        },
    )
    assert response.status_code in (200, 201)
    assert response.json() == {
        "additive": "non-additive",
        "columns": [],
        "description": "random description",
        "display_name": "blah",
        "name": "completed_repairs",
    }

    response = await client_with_roads.patch(
        "/measures/completed_repairs",
        json={
            "columns": [
                {
                    "node": "default.regional_level_agg",
                    "column": "completed_repairs",
                },
                {
                    "node": "default.national_level_agg",
                    "column": "total_amount_nationwide",
                },
            ],
        },
    )
    assert response.status_code in (200, 201)
    assert response.json() == {
        "additive": "non-additive",
        "columns": [
            {
                "name": "completed_repairs",
                "node": "default.regional_level_agg",
                "type": "bigint",
            },
            {
                "name": "total_amount_nationwide",
                "node": "default.national_level_agg",
                "type": "double",
            },
        ],
        "description": "random description",
        "display_name": "blah",
        "name": "completed_repairs",
    }

    # Failed edit
    response = await client_with_roads.patch(
        "/measures/completed_repairs",
        json={
            "columns": [
                {
                    "node": "default.regional_level_agg",
                    "column": "completed_repairs",
                },
                {
                    "node": "default.national_level_agg",
                    "column": "non_existent_column",
                },
            ],
        },
    )
    assert response.status_code >= 400
    assert response.json()["message"] == (
        "Column `non_existent_column` does not exist on node `default.national_level_agg`"
    )
