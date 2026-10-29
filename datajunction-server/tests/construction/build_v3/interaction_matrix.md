# SQL generation interaction coverage

The matrix in `interaction_matrix_test.py` complements the existing feature
tests. It exercises the real v3 SQL builder through the SQL API, with the query
service mocked by the existing fixtures. It does not execute metric queries or
claim full semantic equivalence from parsing alone.

The suite contains 100 SQL interaction cases and two assertion-helper checks,
including passing regressions for the three bugs documented below. Rejected combinations are passing
tests with explicit error assertions, not skips.

## Bounded core

Twenty explicit requests cover all **71 pairs of settings** across these axes:

| Axis | Settings |
| --- | --- |
| Aggregation | SUM, decomposed AVG, exact COUNT DISTINCT, raw MAX_BY, conditional SUM |
| Composition | Base, derived ratio, nested derived expression |
| Facts | Same fact, cross-fact |
| Filter | None, grouped dimension, filter-only dimension, metric/HAVING |

The full product would contain 120 requests. `test_core_matrix_covers_every_pair`
checks the declared pair coverage and rejects duplicate rows. This guarantee is
limited to these four axes; it is not a claim of pairwise coverage across every
DJ feature, aggregation function, dialect, or possible graph shape.

Derived requests also select their parent metrics. Their expression trees must
use the same operands as the selected parents, including operator nesting. The
suite checks the aggregates separately and verifies that cross-fact aggregate
CTEs are computed before the final FULL JOIN. Raw MAX_BY inputs are an explicit
exception: they remain at native grain. A derived MAX_BY metric spanning facts
is currently rejected, and its case asserts that rejection.

## Targeted higher-order interactions

| Test | Interaction and SQL contract |
| --- | --- |
| `test_grain_derived_filter_interactions` | Global/partitioned fixed grain or each of four snapshot collapse functions, derived ratio, filters, and date retained/omitted. Only fixed-grain operands broadcast; snapshot operands collapse together before division. |
| `test_window_over_grain_modified_parent` | Semi-additive parent, LAG, role-qualified week, and dimension filter. Collapse precedes LAG; category partitions remain separate. Fixed-grain parents currently reject LAG. |
| `test_grain_derived_preagg_interactions` | Grain modifier/AVG/distinct, derived ratio, filter-only/HAVING predicates, and live versus materialized routing. Fine pre-aggregations retain dates and filter columns. A distinct group falls back when its pre-aggregation grain is not an exact match. |
| `test_cross_fact_derived_filter_with_partial_preagg` | Cross-fact ratio with dimension and metric filters, where only one fact has a matching pre-aggregation. The other fact remains live. |
| `test_grain_modifiers_fail_closed` | Partitioned fixed grain or a collapsed snapshot, additional additive metric, and filter-only dimension. Unsupported fanout/partition binding is rejected, including a same-fact additive/snapshot combination. |
| `test_rolling_window_filter_interactions` | Rolling window, optional derived ratio, category partition, filter-only subcategory, and optional narrow date bounds. History is retained for the window and output dates are restricted afterward. |

Snapshot ratios use two semi-additive operands with matching protected grain.
Combining a collapsed snapshot with an additive metric can create separate
grain groups even on the same fact and is subject to the builder's fanout guard.
That restriction must not be confused with the supported snapshot/snapshot case.

Successful cases check CTE projection closure with the existing
`unprojected_references` helper. SQLGlot parses the emitted Spark SQL and
translates it for DuckDB. DuckDB binds `EXPLAIN` and `DESCRIBE` against empty
tables whose schemas come from the source model, checking grouping, column
resolution, and the emitted output columns independently of DJ's parser. The
pre-aggregation materialization SQL is bound too, before its declared physical
columns are used to bind the serving query.

Binding does not verify numeric answers, fanout effects on values, or actual
Spark/Trino/Druid execution. The existing property tests supply independent
result comparisons for their supported feature subset. Ratio assertions focus
on operands and operator structure; engine-specific division-by-zero behavior
remains in the existing dedicated tests.

## Bugs fixed after the matrix exposed them

These cases now assert successful SQL generation and independent binding.

| Failure | Regression |
| --- | --- |
| Nested cross-fact distinct metric | A nested ratio can apply COUNT DISTINCT to a CTE column that already contains distinct counts, while the separately selected base metric correctly reads that column with MAX. |
| Fixed-grain derived metrics plus HAVING | Output metadata resolution fails for `base_metrics.total_quantity AS total_quantity`, reporting that the column has no parent. Both global and partitioned grains are affected. |
| Derived arithmetic over a rolling metric | The generated SQL leaves `v3.trailing_7d_revenue` unresolved in the derived expression. It is a semantic metric name, not a table/column reference that an engine can bind. Both unfiltered and narrow date-range cases reproduce it. |

Derived formulas now resolve nested definitions using the expressions chosen
for their base metrics, preserving distinct-count passthroughs and coarser-grain
window CTE references. Projection aliases and metric predicates use independent
AST copies. Filters use WHERE once aggregation has moved to `base_metrics`;
the window stage exposes its computed metric columns to an outer WHERE,
so even base-metric predicates cannot remove rows from a rolling frame.

Additional regressions vary parent selection and request order, cover nested
rolling formulas and weekly window values consumed at daily grain, and combine
base/derived metric filters with window lookback and output date restrictions.

Dependency planning includes unselected ancestors. Only definitions that own
LAG/LEAD windows receive window grain groups; scalar descendants consume their
results at the requested grain. Planned expressions are authoritative, including
when the requested metric itself already maps to a coarser window CTE column.

This changes existing SQL for the cross-fact `weekly_active_rate` regression:
`trailing_7d_active_users` previously ran inside `base_metrics`, before date
densification, with an unresolved semantic ORDER BY reference. It now runs in
`__windowed` as `SUM(base_metrics.daily_active_users) OVER (...)`, after
densification. The updated expectation records this intentional stage change.
Metric predicates reference the projected metric columns directly in an outer
WHERE. There is no hidden predicate column or duplicated window expression.

Expression construction uses one dependency resolver for base-stage derived
formulas, window formulas, and scalar descendants of windows. Its inputs are
the metric values available at that SQL stage. Crossing a CTE boundary replaces
those expressions with column references; unresolved base metrics fail rather
than being reconstructed from source expressions. SQL-stage transformations
(partition injection, dimension binding, and division guards) happen after
resolution. Projections receive independent AST copies before aliasing.
Division guards apply at every stage, including scalar formulas over window
outputs and ratios with fixed-grain operands. Regressions assert the guards
directly and evaluate a generated scalar formula with a zero denominator to
check that it returns NULL.
`metric_expressions_test.py` checks the resolver's stage boundaries, request-order
independence, and ownership of shared expression trees.

## Existing coverage and remaining expansion

These files already assert the following interactions; extend them or add a
named matrix scenario when a specific gap is identified rather than duplicating
their full SQL expectations here.

| Area | Existing coverage | Remaining matrix expansion |
| --- | --- | --- |
| Dimension roles, self joins, multi-hop paths | `measures_sql_test.py`, `metrics_sql_test.py`, `roled_filter_pushdown_test.py`, `measure_filter_with_roled_join_test.py`, `test_self_join_anchor_filter_pushdown.py` | Combine multiple roles and shared-transform pushdown with grain modifiers. |
| Required dimensions | `metrics_sql_test.py` (`TestBareRequiredDimension`), `having_clause_test.py` | Add role-qualified required dimensions to grain/window cases. Requirements can add grouping columns automatically. |
| Cube routing | `cube_matcher_test.py`, `fixed_grain_test.py`, `reaggregate_coverage_test.py` | Bind cube-serving combinations alongside these live/pre-aggregation cases. |
| GROUP_LIMIT / GROUP_OTHERS | `tests/api/semantic_layer_test.py`, `metrics_sql_test.py` (`TestNonDecomposableMetrics`) | Add a full-build ranked-groups × derived-metric × window scenario. Existing tests cover component regrouping, raw/distinct inputs, role filters, and protected-dimension rejection. |
| Sketches and dialects | `sketch_family_e2e_test.py`, `serialize_target_test.py`, `decomposition_test.py` | Add named sketch/grain/routing combinations per supported engine. Do not treat DuckDB binding as a sketch execution oracle. |
| Transform SQL shapes | `cte_test.py`, `cte_transitive_consumer_test.py`, `set_operations_test.py`, `transform_query_shapes_test.py` | Add shared transforms to targeted triples involving roles and filters. |
| Cardinality, composite keys, defaults | `fanout_guard_test.py`, `preagg_substitution_test.py`, `dimension_refs_test.py` | Combine fanout warnings and null/default semantics with grain modifiers; use result tests for actual duplication. |
| Statistical aggregates, nulls, empty inputs | `decomposition_test.py`, `tests/property/metric_decomposition_test.py`, `tests/property/known_issues.py` | Extend independent numeric oracles; SQL binding alone cannot establish these properties. |

No blanket skip excludes unsupported feature combinations. Known restrictions
have rejection assertions; fixed bugs have passing regressions; uncovered interactions
are recorded above. Adding another core-axis setting requires adding scenarios
until its pairs are covered, together with a SQL contract for that setting.

## Running

From `datajunction-server`, with the project test dependencies installed and
the standard Postgres test fixtures available:

```sh
python -m pytest tests/construction/build_v3/interaction_matrix_test.py -q
```
