# Conservative configured context for narrow v2

`await compute_configured_v2_fingerprints(snapshot)` adds configured metric and
cube context to the output/row analyzer. Production callers continue to use v1.

The metric formula is evaluated independently of required dimensions, filters,
and grain declarations. Derived metrics recursively retain each input formula
and contract under the requested context. Context components retain constraints
and captured links that connect provider ancestry to a requested dimension with
the requested role sequence. Formula dimension references accept any role,
matching the planner. Context includes join predicates, cardinality, typed
defaults, join input values/types, and dimension contracts. Evaluation uses
no planner adapter, selected-path tracing, or synthetic ORM objects.

This policy accepts extra invalidation. Adding an alternative link with the same
role can change a configured consumer. Unrelated scalar outputs can remain
independent because context does not automatically include the fact provider's
whole-node hash. Unsupported or incomplete evidence retains a whole dependency
or returns unknown, and must never be interpreted as a saved fingerprint.

Required filters, effective recommended defaults, fixed grain, reaggregation,
cube filters/dimensions, and cube partition declarations remain semantic.
Metric and cube definitions may leave required filter values for query time;
their contracts and required dimension routes remain hashed. Conflicting defaults,
invalid filters, and incomplete context remain unknown.
Presentation metadata, Spark hints, and warning-only recommendations are
excluded. Caller filters can make defaults irrelevant at runtime; the coarse
context may still retain those defaults and report an extra change.

The configured graph is conservative. It provides fewer independence claims
than the previous selected-path candidate. Complex SQL uses the intrinsic
fallback closure. Validation and query planning are unchanged.
