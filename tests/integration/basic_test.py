"""
Basic integration tests.
"""

import pytest
from sqlalchemy.engine import create_engine


@pytest.mark.integration_test
def test_query() -> None:
    """
    Test a simple query.
    """
    engine = create_engine("dj://localhost:8000/0")
    connection = engine.connect()

    sql = """
SELECT "core.users.gender", "core.num_comments"
FROM metrics
GROUP BY "core.users.gender"
    """
    results = list(connection.execute(sql))
    assert results == [("female", 5), ("non-binary", 10), ("male", 7)]
