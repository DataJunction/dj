#!/usr/bin/env python
"""
Create the pre-aggregations that ``tests/api/preaggregations_test.py`` needs.

The target database is already a clone of the main template, so the BUILD_V3
nodes these preaggs sit on top of are present. This only runs the ten
``/preaggs/plan`` calls and prints the resulting ids, so the caller can turn the
database into a template that every test clones instead of re-planning.

Run as a subprocess to avoid event loop conflicts with pytest-asyncio, the same
way ``populate_template.py`` is.

Usage: python populate_preaggs_template.py <database_url>
"""

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.template_app import (
    configure_database_env,
    mark_template_populated,
    template_app_client,
)

db_url = sys.argv[1]
reader_db_url = configure_database_env(db_url)

# Same order as the fixture's preagg1..preagg10, so the printed ids line up
# positionally with the names the tests use.
PREAGG_SPECS: list[dict] = [
    {
        "metrics": ["v3.total_revenue", "v3.total_quantity"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.total_revenue", "v3.avg_unit_price"],
        "dimensions": ["v3.order_details.status", "v3.product.category"],
        "strategy": "full",
        "schedule": "0 * * * *",
    },
    {
        "metrics": ["v3.max_unit_price"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
    },
    {
        "metrics": ["v3.total_revenue"],
        "dimensions": ["v3.product.category"],
    },
    {
        "metrics": ["v3.order_count"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.min_unit_price"],
        "dimensions": ["v3.order_details.status"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.total_revenue"],
        "dimensions": ["v3.customer.customer_id"],
    },
    {
        "metrics": ["v3.page_view_count"],
        "dimensions": ["v3.product.category"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.session_count"],
        "dimensions": ["v3.product.category"],
        "strategy": "full",
        "schedule": "0 0 * * *",
    },
    {
        "metrics": ["v3.visitor_count"],
        "dimensions": ["v3.product.category"],
    },
]


async def main() -> None:
    async with template_app_client(db_url, reader_db_url) as (session, client):
        preagg_ids: list[int] = []
        for index, spec in enumerate(PREAGG_SPECS, start=1):
            response = await client.post("/preaggs/plan", json=spec)
            if response.status_code != 201:
                raise RuntimeError(
                    f"preagg {index} failed ({response.status_code}): {response.text}",
                )
            preagg_ids.append(response.json()["preaggs"][0]["id"])
        await session.commit()

    mark_template_populated(db_url)
    print("PREAGG_IDS " + json.dumps(preagg_ids))


if __name__ == "__main__":
    asyncio.run(main())
