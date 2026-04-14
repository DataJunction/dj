"""
Complex test suites for validate_node_query.

Part 1: SQL Complexity — gnarly standalone queries against a small set of source tables.
Part 2: DAG Depth — topological resolution chain simulating deployment propagation.
"""

import pytest

from datajunction_server.internal.deployment.type_inference import (
    validate_node_query,
    columns_signature_changed,
    TypeResolutionError,
)
from datajunction_server.sql.parsing.types import (
    BigIntType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    TimestampType,
    UnknownType,
)


# =====================================================================
# Shared source tables for SQL complexity tests
# =====================================================================

def _col_map(*tables):
    return {
        name: {col_name: col_type for col_name, col_type in cols}
        for name, cols in tables
    }


EVENTS = (
    "default.events",
    [
        ("event_id", IntegerType()),
        ("user_id", IntegerType()),
        ("amount", DoubleType()),
        ("discount", FloatType()),
        ("tags", StringType()),  # array<string> stored as string
        ("created_at", TimestampType()),
        ("category", StringType()),
    ],
)

USERS = (
    "default.users",
    [
        ("user_id", IntegerType()),
        ("username", StringType()),
        ("country", StringType()),
        ("signup_date", DateType()),
        ("is_active", BooleanType()),
    ],
)

DATE_DIM = (
    "default.date_dim",
    [
        ("date_id", DateType()),
        ("year", IntegerType()),
        ("month", IntegerType()),
        ("week", StringType()),
        ("quarter", IntegerType()),
    ],
)

PRODUCTS = (
    "default.products",
    [
        ("product_id", IntegerType()),
        ("product_name", StringType()),
        ("category", StringType()),
        ("price", DoubleType()),
    ],
)

ORDER_ITEMS = (
    "default.order_items",
    [
        ("item_id", IntegerType()),
        ("event_id", IntegerType()),
        ("product_id", IntegerType()),
        ("quantity", IntegerType()),
        ("unit_price", DoubleType()),
    ],
)

ALL_SOURCES = _col_map(EVENTS, USERS, DATE_DIM, PRODUCTS, ORDER_ITEMS)


# #####################################################################
#
#  PART 1: SQL COMPLEXITY
#
# #####################################################################


class TestComplexCTEs:
    """Multi-CTE queries with inter-CTE references and window functions."""

    def test_multi_cte_with_window_functions(self):
        """
        WITH
          daily AS (
            SELECT user_id, CAST(created_at AS DATE) AS dt,
                   SUM(amount) AS daily_total,
                   ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY CAST(created_at AS DATE) DESC) AS rn
            FROM default.events
            GROUP BY user_id, CAST(created_at AS DATE)
          ),
          weekly AS (
            SELECT user_id, COUNT(*) AS active_days, AVG(daily_total) AS avg_daily
            FROM daily WHERE rn <= 7
            GROUP BY user_id
          )
        SELECT w.user_id, w.active_days, w.avg_daily,
               CASE WHEN w.avg_daily > 100 THEN 'high'
                    WHEN w.avg_daily > 10 THEN 'medium'
                    ELSE 'low' END AS tier
        FROM weekly w
        """
        result = validate_node_query(
            "WITH "
            "daily AS ("
            "  SELECT user_id, CAST(created_at AS DATE) AS dt, "
            "         SUM(amount) AS daily_total, "
            "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY CAST(created_at AS DATE) DESC) AS rn "
            "  FROM default.events "
            "  GROUP BY user_id, CAST(created_at AS DATE)"
            "), "
            "weekly AS ("
            "  SELECT user_id, COUNT(*) AS active_days, AVG(daily_total) AS avg_daily "
            "  FROM daily WHERE rn <= 7 "
            "  GROUP BY user_id"
            ") "
            "SELECT w.user_id, w.active_days, w.avg_daily, "
            "       CASE WHEN w.avg_daily > 100 THEN 'high' "
            "            WHEN w.avg_daily > 10 THEN 'medium' "
            "            ELSE 'low' END AS tier "
            "FROM weekly w",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 4
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1][0] == "active_days"
        assert isinstance(result.output_columns[1][1], BigIntType)
        assert result.output_columns[2][0] == "avg_daily"
        assert result.output_columns[3] == ("tier", StringType())

    def test_cte_referencing_earlier_cte(self):
        """
        WITH
          base AS (SELECT user_id, amount FROM default.events),
          enriched AS (SELECT b.user_id, b.amount, u.country FROM base b JOIN default.users u ON b.user_id = u.user_id),
          summary AS (SELECT country, SUM(amount) AS total, COUNT(*) AS cnt FROM enriched GROUP BY country)
        SELECT country, total, cnt, total / cnt AS avg_per_event FROM summary
        """
        result = validate_node_query(
            "WITH "
            "base AS (SELECT user_id, amount FROM default.events), "
            "enriched AS ("
            "  SELECT b.user_id, b.amount, u.country "
            "  FROM base b JOIN default.users u ON b.user_id = u.user_id"
            "), "
            "summary AS ("
            "  SELECT country, SUM(amount) AS total, COUNT(*) AS cnt "
            "  FROM enriched GROUP BY country"
            ") "
            "SELECT country, total, cnt, total / cnt AS avg_per_event FROM summary",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 4
        assert result.output_columns[0] == ("country", StringType())
        assert result.output_columns[1][0] == "total"
        assert isinstance(result.output_columns[1][1], DoubleType)
        assert result.output_columns[2][0] == "cnt"
        assert result.output_columns[3][0] == "avg_per_event"


class TestComplexJoins:
    """Multi-join queries with mixed aggregation and expressions."""

    def test_multi_join_with_mixed_agg(self):
        """
        SELECT
          u.country,
          COUNT(DISTINCT e.user_id) AS unique_users,
          SUM(CASE WHEN e.amount > 0 THEN e.amount ELSE 0 END) AS positive_revenue,
          MAX(e.created_at) AS last_activity
        FROM default.events e
        JOIN default.users u ON e.user_id = u.user_id
        GROUP BY u.country
        """
        result = validate_node_query(
            "SELECT "
            "  u.country, "
            "  COUNT(DISTINCT e.user_id) AS unique_users, "
            "  SUM(CASE WHEN e.amount > 0 THEN e.amount ELSE 0 END) AS positive_revenue, "
            "  MAX(e.created_at) AS last_activity "
            "FROM default.events e "
            "JOIN default.users u ON e.user_id = u.user_id "
            "GROUP BY u.country",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 4
        assert result.output_columns[0] == ("country", StringType())
        assert result.output_columns[1][0] == "unique_users"
        assert isinstance(result.output_columns[1][1], BigIntType)
        assert result.output_columns[2][0] == "positive_revenue"
        assert isinstance(result.output_columns[2][1], DoubleType)
        assert result.output_columns[3][0] == "last_activity"

    def test_three_way_join(self):
        """
        SELECT
          u.username,
          p.product_name,
          SUM(oi.quantity * oi.unit_price) AS total_spent
        FROM default.order_items oi
        JOIN default.events e ON oi.event_id = e.event_id
        JOIN default.users u ON e.user_id = u.user_id
        JOIN default.products p ON oi.product_id = p.product_id
        GROUP BY u.username, p.product_name
        """
        result = validate_node_query(
            "SELECT "
            "  u.username, "
            "  p.product_name, "
            "  SUM(oi.quantity * oi.unit_price) AS total_spent "
            "FROM default.order_items oi "
            "JOIN default.events e ON oi.event_id = e.event_id "
            "JOIN default.users u ON e.user_id = u.user_id "
            "JOIN default.products p ON oi.product_id = p.product_id "
            "GROUP BY u.username, p.product_name",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 3
        assert result.output_columns[0] == ("username", StringType())
        assert result.output_columns[1] == ("product_name", StringType())
        assert result.output_columns[2][0] == "total_spent"

    def test_left_join_with_coalesce(self):
        """
        SELECT
          e.event_id,
          COALESCE(u.username, 'anonymous') AS display_name,
          COALESCE(u.country, 'unknown') AS country
        FROM default.events e
        LEFT JOIN default.users u ON e.user_id = u.user_id
        """
        result = validate_node_query(
            "SELECT "
            "  e.event_id, "
            "  COALESCE(u.username, 'anonymous') AS display_name, "
            "  COALESCE(u.country, 'unknown') AS country "
            "FROM default.events e "
            "LEFT JOIN default.users u ON e.user_id = u.user_id",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 3
        assert result.output_columns[0] == ("event_id", IntegerType())
        assert result.output_columns[1] == ("display_name", StringType())
        assert result.output_columns[2] == ("country", StringType())


class TestComplexSubqueries:
    """Nested subqueries including subqueries in WHERE and FROM."""

    def test_subquery_in_from_with_aggregation(self):
        """
        SELECT country, avg_amount
        FROM (
          SELECT u.country, AVG(e.amount) AS avg_amount
          FROM default.events e
          JOIN default.users u ON e.user_id = u.user_id
          GROUP BY u.country
        ) country_stats
        WHERE avg_amount > 50
        """
        result = validate_node_query(
            "SELECT country, avg_amount "
            "FROM ("
            "  SELECT u.country, AVG(e.amount) AS avg_amount "
            "  FROM default.events e "
            "  JOIN default.users u ON e.user_id = u.user_id "
            "  GROUP BY u.country"
            ") country_stats "
            "WHERE avg_amount > 50",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 2
        assert result.output_columns[0] == ("country", StringType())
        assert result.output_columns[1][0] == "avg_amount"

    def test_subquery_in_where_with_in(self):
        """
        SELECT user_id, username
        FROM default.users
        WHERE user_id IN (
          SELECT user_id FROM default.events
          GROUP BY user_id
          HAVING SUM(amount) > 1000
        )
        """
        result = validate_node_query(
            "SELECT user_id, username "
            "FROM default.users "
            "WHERE user_id IN ("
            "  SELECT user_id FROM default.events "
            "  GROUP BY user_id "
            "  HAVING SUM(amount) > 1000"
            ")",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 2
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("username", StringType())

    def test_deeply_nested_subqueries(self):
        """
        Three levels deep:
        SELECT * FROM (
          SELECT country, total FROM (
            SELECT u.country, SUM(e.amount) AS total
            FROM default.events e
            JOIN default.users u ON e.user_id = u.user_id
            GROUP BY u.country
          ) inner_agg
          WHERE total > 100
        ) outer_filter
        """
        result = validate_node_query(
            "SELECT country, total FROM ("
            "  SELECT country, total FROM ("
            "    SELECT u.country, SUM(e.amount) AS total "
            "    FROM default.events e "
            "    JOIN default.users u ON e.user_id = u.user_id "
            "    GROUP BY u.country"
            "  ) inner_agg "
            "  WHERE total > 100"
            ") outer_filter",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 2
        assert result.output_columns[0] == ("country", StringType())
        assert result.output_columns[1][0] == "total"
        assert isinstance(result.output_columns[1][1], DoubleType)

    def test_correlated_subquery_in_select(self):
        """
        SELECT
          u.user_id,
          u.username,
          (SELECT COUNT(*) FROM default.events e WHERE e.user_id = u.user_id) AS event_count
        FROM default.users u
        """
        result = validate_node_query(
            "SELECT "
            "  u.user_id, "
            "  u.username, "
            "  (SELECT COUNT(*) FROM default.events e WHERE e.user_id = u.user_id) AS event_count "
            "FROM default.users u",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 3
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("username", StringType())
        assert result.output_columns[2][0] == "event_count"


class TestComplexExpressions:
    """Deeply nested expressions: CASE inside CASE, chained functions, etc."""

    def test_nested_case_inside_case(self):
        """
        SELECT
          CASE
            WHEN category = 'premium' THEN
              CASE WHEN amount > 500 THEN 'whale' ELSE 'premium' END
            WHEN category = 'standard' THEN 'standard'
            ELSE 'basic'
          END AS user_tier
        FROM default.events
        """
        result = validate_node_query(
            "SELECT "
            "  CASE "
            "    WHEN category = 'premium' THEN "
            "      CASE WHEN amount > 500 THEN 'whale' ELSE 'premium' END "
            "    WHEN category = 'standard' THEN 'standard' "
            "    ELSE 'basic' "
            "  END AS user_tier "
            "FROM default.events",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 1
        assert result.output_columns[0] == ("user_tier", StringType())

    def test_complex_arithmetic_chain(self):
        """
        SELECT
          user_id,
          (amount * (1 - discount)) / NULLIF(quantity, 0) AS effective_price
        FROM default.events e
        JOIN default.order_items oi ON e.event_id = oi.event_id
        """
        result = validate_node_query(
            "SELECT "
            "  e.user_id, "
            "  (e.amount * (1 - e.discount)) AS effective_amount "
            "FROM default.events e",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 2
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1][0] == "effective_amount"

    def test_multiple_window_functions(self):
        """
        SELECT
          user_id,
          amount,
          SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at) AS running_total,
          LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY created_at) AS prev_amount,
          RANK() OVER (ORDER BY amount DESC) AS amount_rank
        FROM default.events
        """
        result = validate_node_query(
            "SELECT "
            "  user_id, "
            "  amount, "
            "  SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at) AS running_total, "
            "  LAG(amount, 1) OVER (PARTITION BY user_id ORDER BY created_at) AS prev_amount, "
            "  RANK() OVER (ORDER BY amount DESC) AS amount_rank "
            "FROM default.events",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 5
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("amount", DoubleType())
        assert result.output_columns[2][0] == "running_total"
        assert isinstance(result.output_columns[2][1], DoubleType)
        assert result.output_columns[3][0] == "prev_amount"
        assert isinstance(result.output_columns[3][1], DoubleType)  # LAG returns arg's type
        assert result.output_columns[4][0] == "amount_rank"
        assert isinstance(result.output_columns[4][1], IntegerType)  # RANK returns IntegerType

    def test_max_case_greatest_coalesce(self):
        """The gnarliest nested expression: MAX(CASE WHEN GREATEST(COALESCE(...), ...) > 0 THEN ...)"""
        result = validate_node_query(
            "SELECT MAX(CASE "
            "  WHEN GREATEST(COALESCE(amount, 0), 0) > 0 "
            "  THEN amount "
            "  ELSE 0 "
            "END) AS max_positive "
            "FROM default.events",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 1
        assert result.output_columns[0][0] == "max_positive"
        assert isinstance(result.output_columns[0][1], DoubleType)

    def test_sum_of_case_with_multiple_conditions(self):
        """
        SELECT
          SUM(CASE WHEN category = 'A' THEN amount ELSE 0 END) AS a_total,
          SUM(CASE WHEN category = 'B' THEN amount ELSE 0 END) AS b_total,
          COUNT(CASE WHEN amount > 100 THEN 1 END) AS high_value_count
        FROM default.events
        """
        result = validate_node_query(
            "SELECT "
            "  SUM(CASE WHEN category = 'A' THEN amount ELSE 0 END) AS a_total, "
            "  SUM(CASE WHEN category = 'B' THEN amount ELSE 0 END) AS b_total, "
            "  COUNT(CASE WHEN amount > 100 THEN 1 END) AS high_value_count "
            "FROM default.events",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 3
        assert result.output_columns[0][0] == "a_total"
        assert isinstance(result.output_columns[0][1], DoubleType)
        assert result.output_columns[1][0] == "b_total"
        assert isinstance(result.output_columns[1][1], DoubleType)
        assert result.output_columns[2][0] == "high_value_count"
        assert isinstance(result.output_columns[2][1], BigIntType)


class TestComplexSetOperations:
    """UNION, UNION ALL, INTERSECT with different complexities."""

    def test_union_all_with_aggregation(self):
        """
        SELECT user_id, 'event' AS source, COUNT(*) AS cnt
        FROM default.events GROUP BY user_id
        UNION ALL
        SELECT user_id, 'order' AS source, COUNT(*) AS cnt
        FROM default.order_items
        JOIN default.events ON order_items.event_id = events.event_id
        GROUP BY user_id
        """
        # Simplified — UNION output takes types from the first SELECT
        result = validate_node_query(
            "SELECT user_id, 'event' AS source, COUNT(*) AS cnt "
            "FROM default.events GROUP BY user_id "
            "UNION ALL "
            "SELECT user_id, 'order' AS source, COUNT(*) AS cnt "
            "FROM default.events GROUP BY user_id",
            ALL_SOURCES,
        )
        assert len(result.output_columns) == 3
        assert result.output_columns[0] == ("user_id", IntegerType())
        assert result.output_columns[1] == ("source", StringType())
        assert result.output_columns[2][0] == "cnt"
        assert isinstance(result.output_columns[2][1], BigIntType)


# #####################################################################
#
#  PART 2: DAG DEPTH — topological resolution chain
#
# #####################################################################


class TestTopologicalResolution:
    """
    Simulate deployment propagation: resolve nodes in topological order,
    feeding each node's resolved output columns into the parent map for
    the next level.
    """

    # ---- Source tables (level 0 — given, not resolved) ----
    SOURCES = _col_map(
        ("ns.raw_events", [
            ("event_id", IntegerType()),
            ("user_id", IntegerType()),
            ("amount", DoubleType()),
            ("tags", StringType()),
            ("created_at", TimestampType()),
        ]),
        ("ns.raw_users", [
            ("user_id", IntegerType()),
            ("username", StringType()),
            ("country", StringType()),
            ("signup_date", DateType()),
        ]),
        ("ns.date_dim", [
            ("date_id", DateType()),
            ("year", IntegerType()),
            ("month", IntegerType()),
            ("week", StringType()),
        ]),
    )

    # ---- Level 1: transforms that reference sources ----
    LEVEL_1_NODES = {
        "ns.events_enriched": (
            "SELECT e.event_id, e.user_id, e.amount, e.created_at, "
            "u.username, u.country "
            "FROM ns.raw_events e "
            "JOIN ns.raw_users u ON e.user_id = u.user_id"
        ),
        "ns.events_exploded": (
            "SELECT event_id, user_id, tag "
            "FROM ns.raw_events "
            "LATERAL VIEW EXPLODE(tags) t AS tag"
        ),
    }

    # ---- Level 2: transforms that reference level 1 ----
    LEVEL_2_NODES = {
        "ns.daily_summary": (
            "SELECT user_id, CAST(created_at AS DATE) AS dt, "
            "SUM(amount) AS daily_total, COUNT(*) AS event_count "
            "FROM ns.events_enriched "
            "GROUP BY user_id, CAST(created_at AS DATE)"
        ),
        "ns.country_stats": (
            "SELECT country, COUNT(DISTINCT user_id) AS unique_users, "
            "SUM(amount) AS total_revenue "
            "FROM ns.events_enriched "
            "GROUP BY country"
        ),
    }

    # ---- Level 3: transforms that reference level 2 ----
    LEVEL_3_NODES = {
        "ns.user_weekly_stats": (
            "WITH ranked AS ("
            "  SELECT user_id, dt, daily_total, "
            "  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) AS rn "
            "  FROM ns.daily_summary"
            ") "
            "SELECT user_id, AVG(daily_total) AS avg_daily, "
            "COUNT(*) AS active_days "
            "FROM ranked WHERE rn <= 7 "
            "GROUP BY user_id"
        ),
    }

    # ---- Metrics (reference various levels) ----
    METRIC_NODES = {
        "ns.total_revenue": (
            "SELECT SUM(amount) AS ns_DOT_total_revenue "
            "FROM ns.events_enriched"
        ),
        "ns.avg_daily_revenue": (
            "SELECT AVG(daily_total) AS ns_DOT_avg_daily_revenue "
            "FROM ns.daily_summary"
        ),
    }

    # ---- Derived metric (references other metrics) ----
    DERIVED_METRIC_NODES = {
        "ns.revenue_health": (
            "SELECT ns.total_revenue / ns.avg_daily_revenue AS ns_DOT_revenue_health"
        ),
    }

    def test_full_topological_resolution(self):
        """
        Resolve the entire DAG in topological order:
        sources → level 1 → level 2 → level 3 → metrics → derived metrics.

        At each level, resolved output columns feed into the parent map
        for the next level — exactly what propagate_impact Phase 4 would do.
        """
        # Start with source columns
        parent_map = dict(self.SOURCES)

        # Level 1
        for name, query in self.LEVEL_1_NODES.items():
            result = validate_node_query(query, parent_map)
            parent_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        # Verify level 1: events_enriched
        assert isinstance(parent_map["ns.events_enriched"]["event_id"], IntegerType)
        assert isinstance(parent_map["ns.events_enriched"]["user_id"], IntegerType)
        assert isinstance(parent_map["ns.events_enriched"]["amount"], DoubleType)
        assert isinstance(parent_map["ns.events_enriched"]["created_at"], TimestampType)
        assert isinstance(parent_map["ns.events_enriched"]["username"], StringType)
        assert isinstance(parent_map["ns.events_enriched"]["country"], StringType)
        # events_exploded: tag is UnknownType from LATERAL VIEW
        assert isinstance(parent_map["ns.events_exploded"]["event_id"], IntegerType)
        assert isinstance(parent_map["ns.events_exploded"]["user_id"], IntegerType)

        # Level 2
        for name, query in self.LEVEL_2_NODES.items():
            result = validate_node_query(query, parent_map)
            parent_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        # Verify level 2: daily_summary
        assert isinstance(parent_map["ns.daily_summary"]["user_id"], IntegerType)
        assert isinstance(parent_map["ns.daily_summary"]["daily_total"], DoubleType)
        assert isinstance(parent_map["ns.daily_summary"]["event_count"], BigIntType)
        # country_stats
        assert isinstance(parent_map["ns.country_stats"]["country"], StringType)
        assert isinstance(parent_map["ns.country_stats"]["unique_users"], BigIntType)
        assert isinstance(parent_map["ns.country_stats"]["total_revenue"], DoubleType)

        # Level 3
        for name, query in self.LEVEL_3_NODES.items():
            result = validate_node_query(query, parent_map)
            parent_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        # Verify level 3: user_weekly_stats
        assert isinstance(parent_map["ns.user_weekly_stats"]["user_id"], IntegerType)
        assert isinstance(parent_map["ns.user_weekly_stats"]["avg_daily"], DoubleType)
        assert isinstance(parent_map["ns.user_weekly_stats"]["active_days"], BigIntType)

        # Metrics
        for name, query in self.METRIC_NODES.items():
            result = validate_node_query(query, parent_map)
            parent_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        # Metrics: single output column each
        assert isinstance(
            list(parent_map["ns.total_revenue"].values())[0],
            DoubleType,
        )
        assert isinstance(
            list(parent_map["ns.avg_daily_revenue"].values())[0],
            DoubleType,
        )

        # Derived metrics
        for name, query in self.DERIVED_METRIC_NODES.items():
            result = validate_node_query(query, parent_map)
            parent_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        assert len(parent_map["ns.revenue_health"]) == 1
        # revenue / avg_daily → DoubleType / DoubleType → DoubleType
        assert isinstance(
            list(parent_map["ns.revenue_health"].values())[0],
            DoubleType,
        )

    def test_column_type_change_propagates(self):
        """
        Simulate a parent column type change and verify it propagates.

        1. Resolve level 1 with original source columns
        2. Change raw_events.amount from DoubleType to FloatType
        3. Re-resolve level 1 and verify the change propagates
        4. Use columns_signature_changed to detect the difference
        """
        # Original resolution
        original_map = dict(self.SOURCES)
        for name, query in self.LEVEL_1_NODES.items():
            result = validate_node_query(query, original_map)
            original_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        original_enriched = [
            (name, typ) for name, typ in original_map["ns.events_enriched"].items()
        ]

        # Change source: amount DoubleType → FloatType
        changed_sources = dict(self.SOURCES)
        changed_sources["ns.raw_events"] = {
            "event_id": IntegerType(),
            "user_id": IntegerType(),
            "amount": FloatType(),  # CHANGED
            "tags": StringType(),
            "created_at": TimestampType(),
        }

        # Re-resolve level 1
        changed_map = dict(changed_sources)
        for name, query in self.LEVEL_1_NODES.items():
            result = validate_node_query(query, changed_map)
            changed_map[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        changed_enriched = [
            (name, typ) for name, typ in changed_map["ns.events_enriched"].items()
        ]

        # The column signature should have changed (amount: double → float)
        assert columns_signature_changed(original_enriched, changed_enriched) is True

        # Specifically, the amount column type should differ
        assert isinstance(original_map["ns.events_enriched"]["amount"], DoubleType)
        assert isinstance(changed_map["ns.events_enriched"]["amount"], FloatType)

    def test_column_rename_detected(self):
        """
        Simulate renaming a source column and verify downstream breakage is detected.
        """
        # Change source: rename 'amount' → 'total_amount'
        changed_sources = dict(self.SOURCES)
        changed_sources["ns.raw_events"] = {
            "event_id": IntegerType(),
            "user_id": IntegerType(),
            "total_amount": DoubleType(),  # RENAMED from 'amount'
            "tags": StringType(),
            "created_at": TimestampType(),
        }

        # events_enriched references 'e.amount' which no longer exists
        result = validate_node_query(
            self.LEVEL_1_NODES["ns.events_enriched"],
            changed_sources,
        )
        assert any("amount" in e for e in result.errors)

    def test_column_addition_doesnt_break_downstream(self):
        """
        Adding a new column to a source should NOT break existing downstream nodes.
        """
        # Add a new column to raw_events
        expanded_sources = dict(self.SOURCES)
        expanded_sources["ns.raw_events"] = {
            **self.SOURCES["ns.raw_events"],
            "new_column": StringType(),  # ADDED
        }

        # Level 1 should still resolve fine
        for name, query in self.LEVEL_1_NODES.items():
            result = validate_node_query(query, expanded_sources)
            assert len(result.output_columns) > 0  # Should succeed

    def test_unchanged_source_no_propagation_needed(self):
        """
        If source columns don't change, downstream nodes with fully-resolved
        types should have identical column signatures.

        Note: nodes with UnknownType columns (e.g., from LATERAL VIEW EXPLODE)
        will always show as "changed" since UnknownType can't confirm equality.
        We test only the fully-resolved node (events_enriched).
        """
        # Resolve twice with the same sources
        map1 = dict(self.SOURCES)
        for name, query in self.LEVEL_1_NODES.items():
            result = validate_node_query(query, map1)
            map1[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        map2 = dict(self.SOURCES)
        for name, query in self.LEVEL_1_NODES.items():
            result = validate_node_query(query, map2)
            map2[name] = {col_name: col_type for col_name, col_type in result.output_columns}

        # events_enriched has no UnknownType — signatures should be identical
        sig1 = list(map1["ns.events_enriched"].items())
        sig2 = list(map2["ns.events_enriched"].items())
        assert columns_signature_changed(sig1, sig2) is False

        # events_exploded has UnknownType (from LATERAL VIEW) — always "changed"
        sig1 = list(map1["ns.events_exploded"].items())
        sig2 = list(map2["ns.events_exploded"].items())
        assert columns_signature_changed(sig1, sig2) is True  # Expected: UnknownType triggers this
