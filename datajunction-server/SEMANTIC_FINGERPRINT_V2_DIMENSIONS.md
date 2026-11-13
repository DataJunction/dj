# Conservative configured context for narrow v2

`await compute_configured_v2_fingerprints(snapshot)` adds configured metric and
cube context to the output/row analyzer. Production callers continue to use v1.

The metric formula is evaluated independently of required dimensions, filters,
and grain declarations. Context components retain those constraints and all
captured candidate links that can connect the provider's ancestry to a requested
dimension. They include join predicates, roles, cardinality, typed defaults,
join input values/types, and dimension contracts. They use no planner adapter,
selected-path tracing, or synthetic ORM objects.

This policy deliberately accepts extra invalidation. Adding an unused alternative
link can change a configured consumer. Unrelated scalar outputs can remain
independent because context does not automatically include the fact provider's
whole-node hash. Unsupported or incomplete evidence retains a whole dependency
or returns unknown, and must never be interpreted as a saved fingerprint.

Required filters, effective recommended defaults, fixed grain, reaggregation,
cube filters/dimensions, and cube partition declarations remain semantic.
Presentation metadata, Spark hints, and warning-only recommendations are
excluded. Caller filters can make defaults irrelevant at runtime; the coarse
context may still retain those defaults and report an extra change.

The configured graph is conservative. It provides fewer independence claims
than the previous selected-path candidate. Complex SQL uses the intrinsic
fallback closure. Validation and query planning are unchanged.
