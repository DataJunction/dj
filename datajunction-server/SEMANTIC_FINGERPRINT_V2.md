# Narrow offline semantic fingerprint candidate

Version 2 is explicitly invoked for local evaluation. Deployment responses,
validation, and the default fingerprint engine continue to use version 1.

`SemanticGraphSnapshot(specs, resolved_columns)` copies captured definitions and
source schemas. `await compute_v2_fingerprints(snapshot)` returns per-output,
row, and full-node components with their dependency bindings. Evaluation makes
no database calls and does not reconstruct compiler or ORM objects.

## Precision boundary

The SQL analyzer reuses DJ's parsed AST over captured schemas. Its precise scope
is flat, single-source projections, scalar expressions, WHERE predicates, and common
aggregates. Output components track their referenced provider values. Row
components track input rows and row conditions. Aggregate values also depend on
the rows they aggregate. An unrelated projection can change a node's full hash
while preserving existing output components and their consumers.

Flat projections through one explicitly aliased inner join also bind used columns,
including captured struct fields. Join predicates must be ANDs of cross-input
column equalities. Joined rows track both input row contracts, ON, and WHERE
dependencies.

Other joins, joined aggregates, CTEs, subqueries, set operations, windows, HAVING,
DISTINCT, limits, and unsupported expressions retain conservative whole-node dependencies
or return unknown when the captured evidence is incomplete. Standalone metric
constraints use conservative context. The configured entry point separates that
context from the metric formula. A separate calculation component includes SQL,
rows, values, and grain while excluding optional dimension links. Conservative
SQL fallbacks depend on their parents' calculation components, so adding an
unused link does not invalidate the calculation. SQL that implicitly reads a
dimension attribute retains the relevant link contracts.

Configured derived metrics recursively include their referenced metrics under
the requested dimensions and filters, along with each intermediate formula and
metric contract. Missing references and recursive metric cycles remain unknown.
Joined ancestry can use conservative whole-input dependencies. Configured join
routes must match the requested role sequence, including the empty role for an
ordinary dimension request. All candidate routes with that role remain included;
the candidate does not reproduce the planner's choice among those routes.
Formula expressions that read dimension attributes retain all eligible roles,
matching the planner's any-role lookup. Configured evaluation builds the formula
graph once, then attaches the metric and cube demands.

Descriptions, display labels, owners, tags, custom metadata, column presentation
attributes, and Spark hints are excluded from precise and conservative hashes.
Recommended filters with no effective default (`None` or `""`) are warning-only
and contribute no dependency. Required filters and nonempty defaults remain
semantic. Metadata remains available on captured specifications.

Cached ASTs are read without mutation, including when v1 has already processed
the specifications. A versioned structural representation removes aliases and
source qualifiers after binding. Numeric literal text is retained from DJ's
cached parse tree because the AST can lose decimal precision. Forms with
omitted AST semantics use conservative token fallback when the complete parse
tree establishes their relation and dimension dependencies. This includes
TRY_CAST, sampling, aggregate FILTER/null treatment, custom TRIM characters,
and lateral views. Missing or ambiguous dependency evidence remains unknown.
Fixed digests, alias/comment equivalence, and value/row change controls cover
the supported boundary.

Conservative fallback uses the authored SQL token stream from DJ's parse cache.
It ignores comments and whitespace while retaining literal text and clauses.
Equivalent alias or keyword-case changes can still invalidate this fallback.

## Reading the results

A component carries a versioned digest or `"unknown"`, a precision classification,
and reasons. `compare_components()` returns `indeterminate` when either digest
is unavailable. Unknown-to-unknown equality never establishes unchanged values.
Conservative results must be reported separately from precise results.

Labelled component cycles use a deterministic SCC digest. Missing used types or
unresolved dependencies propagate unknown. The candidate does not detect source
content changes without a data-version input, or describe unrecorded runtime
consumers. Comparisons must use the same captured external context on both sides.
