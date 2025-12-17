"""
Tests for the Spark -> target dialect function translation layer.
"""

from datajunction_server.models.engine import Dialect
from datajunction_server.sql.translation import (
    get_translation,
    translate_function_name,
    translate_sql,
)


class TestSparkToDialectTranslation:
    """Tests for translating Spark function names to other dialects."""

    # -------------------------------------------------------------------------
    # hll_sketch_agg (Spark's HLL accumulate)
    # -------------------------------------------------------------------------

    def test_hll_sketch_agg_to_spark(self):
        """Spark -> Spark is identity."""
        assert (
            translate_function_name("hll_sketch_agg", Dialect.SPARK) == "hll_sketch_agg"
        )

    def test_hll_sketch_agg_to_druid(self):
        """Spark hll_sketch_agg -> Druid DS_HLL."""
        assert translate_function_name("hll_sketch_agg", Dialect.DRUID) == "DS_HLL"

    def test_hll_sketch_agg_to_trino(self):
        """Spark hll_sketch_agg -> Trino approx_set."""
        assert translate_function_name("hll_sketch_agg", Dialect.TRINO) == "approx_set"

    # -------------------------------------------------------------------------
    # hll_union (Spark's HLL merge)
    # -------------------------------------------------------------------------

    def test_hll_union_to_spark(self):
        """Spark -> Spark is identity."""
        assert translate_function_name("hll_union", Dialect.SPARK) == "hll_union"

    def test_hll_union_to_druid(self):
        """Spark hll_union -> Druid DS_HLL."""
        assert translate_function_name("hll_union", Dialect.DRUID) == "DS_HLL"

    def test_hll_union_to_trino(self):
        """Spark hll_union -> Trino merge."""
        assert translate_function_name("hll_union", Dialect.TRINO) == "merge"

    # -------------------------------------------------------------------------
    # hll_sketch_estimate (Spark's HLL finalize)
    # -------------------------------------------------------------------------

    def test_hll_sketch_estimate_to_spark(self):
        """Spark -> Spark is identity."""
        assert (
            translate_function_name("hll_sketch_estimate", Dialect.SPARK)
            == "hll_sketch_estimate"
        )

    def test_hll_sketch_estimate_to_druid(self):
        """Spark hll_sketch_estimate -> Druid APPROX_COUNT_DISTINCT_DS_HLL."""
        assert (
            translate_function_name("hll_sketch_estimate", Dialect.DRUID)
            == "APPROX_COUNT_DISTINCT_DS_HLL"
        )

    def test_hll_sketch_estimate_to_trino(self):
        """Spark hll_sketch_estimate -> Trino cardinality."""
        assert (
            translate_function_name("hll_sketch_estimate", Dialect.TRINO)
            == "cardinality"
        )

    # -------------------------------------------------------------------------
    # Edge cases
    # -------------------------------------------------------------------------

    def test_unknown_function_unchanged(self):
        """Unknown functions should be returned unchanged."""
        assert translate_function_name("unknown_func", Dialect.DRUID) == "unknown_func"

    def test_case_insensitive_lookup(self):
        """Function name lookup should be case-insensitive."""
        assert translate_function_name("HLL_SKETCH_AGG", Dialect.DRUID) == "DS_HLL"
        assert translate_function_name("Hll_Union", Dialect.TRINO) == "merge"


class TestSqlTranslation:
    """Tests for full SQL string translation."""

    def test_spark_to_spark_identity(self):
        """Spark -> Spark should return SQL unchanged."""
        sql = "SELECT hll_sketch_estimate(hll_union(user_hll)) FROM table"
        assert translate_sql(sql, Dialect.SPARK) == sql

    def test_spark_to_druid(self):
        """Test translating Spark SQL to Druid."""
        sql = "SELECT hll_sketch_estimate(hll_union(user_hll)) FROM measures_table"
        expected = (
            "SELECT APPROX_COUNT_DISTINCT_DS_HLL(DS_HLL(user_hll)) FROM measures_table"
        )
        assert translate_sql(sql, Dialect.DRUID) == expected

    def test_spark_to_trino(self):
        """Test translating Spark SQL to Trino."""
        sql = "SELECT hll_sketch_estimate(hll_union(user_hll)) FROM measures_table"
        expected = "SELECT cardinality(merge(user_hll)) FROM measures_table"
        assert translate_sql(sql, Dialect.TRINO) == expected

    def test_translate_hll_sketch_agg(self):
        """Test translating hll_sketch_agg."""
        sql = "SELECT hll_sketch_agg(user_id) FROM events"
        assert translate_sql(sql, Dialect.DRUID) == "SELECT DS_HLL(user_id) FROM events"

    def test_translate_preserves_non_hll_functions(self):
        """Non-HLL functions should be preserved."""
        sql = "SELECT SUM(amount), hll_sketch_estimate(user_hll) FROM table"
        translated = translate_sql(sql, Dialect.DRUID)
        assert "SUM(amount)" in translated
        assert "APPROX_COUNT_DISTINCT_DS_HLL(user_hll)" in translated

    def test_translate_case_insensitive(self):
        """Translation should be case-insensitive for function names."""
        sql = "SELECT HLL_SKETCH_ESTIMATE(HLL_UNION(user_hll)) FROM table"
        translated = translate_sql(sql, Dialect.TRINO)
        assert "cardinality" in translated
        assert "merge" in translated

    def test_translate_with_complex_expressions(self):
        """Test translation with complex expressions."""
        sql = """
        SELECT
            company_name,
            hll_sketch_estimate(hll_union(user_id_hll_abc123)) AS unique_users,
            SUM(click_count) AS total_clicks
        FROM measures_cube
        GROUP BY company_name
        """
        translated = translate_sql(sql, Dialect.DRUID)
        assert "APPROX_COUNT_DISTINCT_DS_HLL" in translated
        assert "DS_HLL" in translated
        assert "SUM(click_count)" in translated
        assert "company_name" in translated

    def test_translate_preserves_column_names_with_hll(self):
        """Column names containing 'hll' should not be translated."""
        sql = "SELECT hll_column, user_hll_sketch FROM table"
        translated = translate_sql(sql, Dialect.DRUID)
        # Column names should be preserved (they don't have parentheses)
        assert "hll_column" in translated
        assert "user_hll_sketch" in translated


class TestGetTranslation:
    """Tests for the get_translation function."""

    def test_get_translation_returns_translation_object(self):
        """get_translation should return a FunctionTranslation object."""
        translation = get_translation("hll_sketch_agg", Dialect.DRUID)
        assert translation is not None
        assert translation.target_name == "DS_HLL"

    def test_get_translation_returns_none_for_unknown(self):
        """get_translation should return None for unknown functions."""
        translation = get_translation("unknown_func", Dialect.DRUID)
        assert translation is None

    def test_get_translation_returns_none_for_spark(self):
        """get_translation should return None for Spark (no translation needed)."""
        translation = get_translation("hll_sketch_agg", Dialect.SPARK)
        assert translation is None
