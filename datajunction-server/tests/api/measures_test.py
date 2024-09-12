"""
Tests for the namespaces API.
"""
from typing import Dict

import pytest
from httpx import AsyncClient


def completed_repairs_measure(measure_name: str = "completed_repairs") -> Dict:
    """
    Test ``GET /measures/``.
    """
    return {
        "name": f"{measure_name}",
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
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test ``GET /measures/``.
    """
    response = await module__client_with_roads.get("/measures/")
    assert response.status_code in (200, 201)
    assert response.json() == []

    await module__client_with_roads.post(
        "/measures/",
        json=completed_repairs_measure(measure_name="completed_repairs_1"),
    )

    response = await module__client_with_roads.get("/measures/")
    assert response.status_code in (200, 201)
    assert response.json() == ["completed_repairs_1"]

    response = await module__client_with_roads.get("/measures/?prefix=comp")
    assert response.status_code in (200, 201)
    assert response.json() == ["completed_repairs_1"]

    response = await module__client_with_roads.get("/measures/?prefix=xyz")
    assert response.status_code in (200, 201)
    assert response.json() == []

    response = await module__client_with_roads.get("/measures/completed_repairs_1")
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
        "display_name": "Completed Repairs 1",
        "name": "completed_repairs_1",
    }

    response = await module__client_with_roads.get("/measures/random_measure")
    assert response.status_code >= 400
    assert (
        response.json()["message"]
        == "Measure with name `random_measure` does not exist"
    )


@pytest.mark.asyncio
async def test_create_measure(
    module__client_with_roads: AsyncClient,
    failed_measure: Dict,  # pylint: disable=redefined-outer-name
) -> None:
    """
    Test ``POST /measures/``.
    """
    # Successful measure creation
    response = await module__client_with_roads.post(
        "/measures/",
        json=completed_repairs_measure(measure_name="completed_repairs_2"),
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
        "display_name": "Completed Repairs 2",
        "name": "completed_repairs_2",
    }

    # Creating the same measure again will fail
    response = await module__client_with_roads.post(
        "/measures/",
        json=completed_repairs_measure(measure_name="completed_repairs_2"),
    )
    assert response.status_code >= 400
    assert response.json()["message"] == "Measure `completed_repairs_2` already exists!"

    # Failed measure creation
    response = await module__client_with_roads.post("/measures/", json=failed_measure)
    assert response.status_code >= 400
    assert response.json()["message"] == (
        "Column `completed_repairs` does not exist on node `default.national_level_agg`"
    )


@pytest.mark.asyncio
async def test_edit_measure(
    module__client_with_roads: AsyncClient,
) -> None:
    """
    Test ``PATCH /measures/{name}``.
    """
    await module__client_with_roads.post(
        "/measures",
        json=completed_repairs_measure(measure_name="completed_repairs_3"),
    )

    # Successfully edit measure
    response = await module__client_with_roads.patch(
        "/measures/completed_repairs_3",
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
        "name": "completed_repairs_3",
    }

    response = await module__client_with_roads.patch(
        "/measures/completed_repairs_3",
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
        "name": "completed_repairs_3",
    }

    response = await module__client_with_roads.patch(
        "/measures/completed_repairs_3",
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
                "name": "total_amount_nationwide",
                "node": "default.national_level_agg",
                "type": "double",
            },
            {
                "name": "completed_repairs",
                "node": "default.regional_level_agg",
                "type": "bigint",
            },
        ],
        "description": "random description",
        "display_name": "blah",
        "name": "completed_repairs_3",
    }

    # Failed edit
    response = await module__client_with_roads.patch(
        "/measures/completed_repairs_3",
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
