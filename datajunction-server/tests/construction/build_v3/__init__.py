import re

from datajunction_server.sql.parsing.backends.antlr4 import parse as parse_sql


def assert_sql_equal(
    actual_sql: str,
    expected_sql: str,
    normalize_aliases: bool = False,
):
    """
    Assert that two SQL strings are semantically equal.

    Uses the DJ SQL parser to normalize both strings before comparison.
    This handles whitespace differences, keyword casing, etc.

    Args:
        actual_sql: The actual SQL generated
        expected_sql: The expected SQL
        normalize_aliases: If True, normalizes component hash suffixes (e.g., sum_x_abc123 -> sum_x_*)
    """
    if normalize_aliases:
        # Normalize hash-based component names: sum_foo_abc123 -> sum_foo_HASH
        hash_pattern = r"(_[a-f0-9]{8})(?=[\s,)]|$)"
        actual_sql = re.sub(hash_pattern, "_HASH", actual_sql)
        expected_sql = re.sub(hash_pattern, "_HASH", expected_sql)

    actual_sql = actual_sql.replace("${dj_logical_timestamp}", "DJ_LOGICAL_TIMESTAMP()")
    actual_parsed = str(parse_sql(actual_sql))
    expected_parsed = str(parse_sql(expected_sql))

    assert actual_parsed == expected_parsed, (
        f"\n\nActual SQL:\n{actual_parsed}\n\nExpected SQL:\n{expected_parsed}"
    )


def get_first_grain_group(response_data: dict) -> dict:
    """
    Extract the first grain group from a measures SQL response.

    The new V3 measures SQL returns multiple grain groups (one per aggregability level).
    Most tests only have FULL aggregability metrics, so this helper extracts the
    first (and usually only) grain group for simpler test assertions.

    Returns a dict with 'sql' and 'columns' keys for backward compatibility with
    existing test assertions.
    """
    assert "grain_groups" in response_data, "Response should have 'grain_groups'"
    assert len(response_data["grain_groups"]) > 0, (
        "Should have at least one grain group"
    )

    grain_group = response_data["grain_groups"][0]
    return {
        "sql": grain_group["sql"],
        "columns": grain_group["columns"],
        "grain": grain_group.get("grain", []),
        "aggregability": grain_group.get("aggregability"),
        "metrics": grain_group.get("metrics", []),
    }
