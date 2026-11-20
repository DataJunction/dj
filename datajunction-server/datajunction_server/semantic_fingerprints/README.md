# Semantic fingerprints

This package describes how a node's definition can affect query results. V2 aims
to track changes to consumer values or population while ignoring presentation
metadata and unused inputs. Materialization reuse also checks a separate
planner-derived storage contract. Evaluation compares definitions and captured
schemas; it does not execute queries or detect new data arriving in a source table.

This document describes the implemented behavior, including conservative cases
that can still cause extra invalidation.

- [Strategy and propagation](#strategy-and-propagation)
- [V2 input inventory](#v2-input-inventory)
- [Excluded inputs](#excluded-inputs)
- [Optimizations](#optimizations)
- [SQL coverage and remaining extra propagation](#sql-coverage-and-remaining-extra-propagation)
- [Determinism and unknown results](#determinism-and-unknown-results)
- [Materialization compatibility](#materialization-compatibility)
- [V1 compatibility and current integration](#v1-compatibility-and-current-integration)
- [Code and tests](#code-and-tests)

## Strategy and propagation

V1 hashes a node together with its parents' whole fingerprints. That makes an
unused column change travel through every dependent node. V2 splits a node into
components and connects consumers to the components they use:

| Component | What it represents |
| --- | --- |
| `output` | One output expression, its column contract, and the input values it reads. |
| `rows` | Which rows exist and how often they occur, including input populations, joins, and predicates. |
| `full` | The node contract, rows, and ordered output layout. This intentionally includes every output. |
| `calculation` | Rows and outputs, omitting the node's optional link list. Links used by SQL still affect rows/outputs. Conservative SQL consumers use this instead of the provider's `full` component. |
| `opaque` | A conservative SQL recipe when narrower binding is unavailable. |
| Configured components | A metric's values and rows under a particular dimension/filter demand, combined into a metric or cube result. |

For example, a transform exposes `revenue` and `debug_label`, and a metric reads
`SUM(revenue)`:

| Edit | Expected propagation with precise binding |
| --- | --- |
| Change a description | No V2 fingerprint changes. |
| Change only `debug_label`'s expression | The transform's output and full fingerprint change. The revenue metric stays unchanged. |
| Change `revenue`'s expression | Its output changes and the metric changes. |
| Change the transform's `WHERE` | Its rows change and the metric changes, even if the revenue expression stays identical. |

Value and row dependencies must travel together. A source column's output
component alone does not identify the physical table or its population.
Aggregates retain the rows they aggregate, and configured metric values retain
both formula and context rows.

### What one component hashes

Each ordinary component is SHA-256 over canonical JSON containing:

1. The domain `datajunction/component-semantic` and fingerprint version `2`.
2. Its identity: fully qualified node name, component kind, and output or demand
   identifier where applicable.
3. Its local semantic payload, detailed below.
4. Its sorted dependencies: binding label, provider component identity, and the
   provider's versioned fingerprint.

Node and output names are structural inputs. Renaming them can change hashes
even when the resulting numbers would be identical. Precision labels and
diagnostic reasons are reported alongside hashes; they are not direct inputs.

## V2 input inventory

Inputs below are included where the component needs them. A full-node hash can
change while an unaffected output and its downstream consumers stay unchanged.

### Sources and column contracts

| Input | Meaning and scope |
| --- | --- |
| Node type | Distinguishes source, transform, dimension, metric, and cube contracts. |
| Source `catalog`, `schema_`, `table` | Physical source identity, included in source rows and full/calculation contracts. Switching tables can change every result. |
| `primary_key` | Declared grain, sorted before hashing. Included in source rows and linkable-node contracts, and in configured grains where relevant. |
| Column `name` | The output's identity and binding. |
| Column `type` | Normalized declared/resolved type. Type changes can change comparisons, casts, aggregation, or stored values. |
| Column `partition.type` | The partitioning declaration. |
| Column `partition.granularity` | Partition time granularity. |
| Column `partition.format` | Partition value format. |
| Ordered output layout | Retained in full/calculation contracts. Reordering exposed outputs can affect consumers of a layout. The column `order` annotation itself is excluded. |

Captured resolved source columns take precedence over authored source columns.
A source's full/calculation contract includes all its columns. A precisely bound
consumer reads only the needed column components plus source rows, so an unused
source column can change without propagating to that consumer.

Partition declarations remain compatibility inputs even when the numerical
result is unchanged. They affect storage layout and materialization behavior.

### Transform, dimension, and metric SQL

| Input | Why it is retained |
| --- | --- |
| Bound provider identity | Identifies which node supplies each relation and value. |
| Output expression | Operators, functions, arguments, casts, literals, conditionals, and referenced column paths can change a value. |
| Numeric literal text | Preserves distinctions that could be lost by converting a decimal through a floating-point AST value. |
| Used input outputs | Retains the values and types read by each expression. |
| Every input's rows | Retains population and multiplicity changes even when projected values are unchanged. |
| `WHERE` and referenced outputs | Predicate changes, and changes to values used by predicates, can change the population. |
| Ordered joins | Retains the introduced providers, join types, complete `ON` expressions, and outputs/types read by those expressions. |
| Aggregate versus scalar row shape | Distinguishes a global aggregate from a projection over source rows. Aggregate outputs depend on their input rows. |
| Output column contract | Retains output name, available type, and partition declaration. |
| Implicit dimension references | Retains the dimension attribute and the link/context needed to read it. |
| Conservative SQL tokens | For unsupported syntax, retains the authored semantic token stream and broader parent calculation dependencies. |

The serializer removes bound relation aliases and redundant expression
alias/parenthesis wrappers where supported. Output names remain inputs. This
normalization does not establish that arbitrary SQL rewrites are equivalent.

### Dimension links

The following fields form a link contract. A linkable node's full contract
retains its declared links. Calculation components omit optional links;
configured consumers retain the relevant candidate paths.

| Field | Join link | Reference link | Meaning |
| --- | --- | --- | --- |
| `type` | Yes | Yes | Distinguishes join and reference behavior. |
| `role` | Yes | Yes | Selects the intended relationship to a dimension. |
| Rendered `dimension_node` | Yes | Yes | Target node identity. |
| `join_type` | Yes | No | Determines matching and unmatched-row behavior. |
| Rendered `join_on` | Yes | No | The matching predicate. |
| `node_column` | Yes | Yes | Source-side binding. |
| `dimension_attribute` | No | Yes | Target-side attribute binding. |
| `default_value` | Yes | No | Value supplied by the link. Both its type name and value are retained, so `false` and `0` stay distinct. |

`join_cardinality` and `spark_hints` are excluded. The cardinality annotation is
not used as a substitute for row or join-key dependencies.

### Configured metrics and dimension context

A configured demand evaluates a metric under requested dimensions and filters.
The metric's formula is analyzed separately, then combined with this context:

| Input | Meaning and scope |
| --- | --- |
| Formula outputs and rows | Retains the calculation and its population. |
| `required_dimensions` | Canonical dimension requirements can change grouping and necessary joins. |
| `fixed_grain` | Retains declared accumulation grain. `None` and `[]` differ: inferred query grain versus explicit global grain. |
| `reaggregate.fn` | Selects reaggregation or sketch behavior. |
| `reaggregate.rules` | Retains each rule's `dimension` and `fn`. |
| `reaggregate.params` | Retains parameters that can change aggregate state or precision. |
| Effective filter defaults | Nonempty defaults from both `required_filters` and `recommended_filters`, including their dimension scopes. |
| Effective filters | Caller/cube predicates combined with applicable defaults using the shared filter-requirement logic. Caller filters can suppress defaults. |
| Requested dimension outputs and roles | Retains the attributes used for grouping or filtering and the requested relationship. |
| Candidate link contracts | Retains all compatible paths for the requested role sequence. An ordinary unroled demand uses the empty role; formula dimension references allow any eligible role. |
| Join-key outputs and types | Retains both endpoints of the joins needed by the context. |
| Context rows and grains | Retains rows of dimensions/intermediate link sources, context primary keys, and root transform/dimension grains. |
| Derived metric formulas | Recursively retains each intermediate formula, its contract, and the input demands under the inherited context. |

A required or recommended filter with `default=None` or `default=""` contributes
no effective predicate. Changing only required versus recommended classification
also leaves the data fingerprint unchanged when the effective defaults stay the
same. Required-filter validation still happens in the query API. Conflicting or
unresolvable effective filters produce an unknown result.

### Cubes

| Input | Meaning and scope |
| --- | --- |
| `metrics` | Metric membership and the formulas/contracts of those metrics. |
| `dimensions` | Dimension attributes and roles used to group the cube. |
| `filters` | Cube predicates and the outputs/context they read. |
| Column partitions | Authored partition type, granularity, and format on matched cube columns. Other cube column annotations are excluded. |
| Configured metric demands | Each metric evaluated under cube dimensions/filters, including effective defaults and recursive derived formulas. |
| Combined rows and values | Retains the cube's blended result dependencies. Row composition is conservative and can depend on metric values. |

Metric/dimension/filter collections are normalized for membership and ordering.
This does not remove ordering from SQL expressions, join chains, output layouts,
or arbitrary nested declarations.

## Excluded inputs

These user-facing fields do not enter V2 semantic payloads, including conservative
fallback payloads:

| Excluded input | Explanation |
| --- | --- |
| Node and column descriptions | Documentation. |
| Display names and labels | Presentation. Structural node/output names remain included. |
| Owners and tags | Ownership and organization. |
| `custom_metadata` and arbitrary annotations | No generic metadata dump is hashed. Payloads explicitly select fields. |
| Column `attributes` | Includes hidden/groupability/dimension flags and the column-level primary-key annotation. The node's explicit `primary_key` remains included. |
| Column units and `order` annotation | Presentation/internal annotations. |
| Metric unit and direction | Presentation of the metric. |
| Filter requirements without effective defaults | Eligibility/validation alone adds no result predicate. |
| Required/recommended classification alone | The effective predicate determines the data dependency. |
| Join cardinality annotations and Spark hints | Planning/safety annotations outside the value/row fingerprint contract. Cardinality can still affect fanout safety warnings. |
| Revision IDs, revision timestamps, and author information | Operational identity/history. |

SQL comments and whitespace are ignored by the query AST or semantic-token
representation. Effective metric defaults, cube filters, and rendered link
predicates are retained as text; harmless formatting edits to those strings can
still change a hash.

`semantic_diff()` reports a broader set of edits for review and attribution,
including cube column descriptions. A field appearing in that diff does not
mean it is an input to a V2 fingerprint.

## Optimizations

### Reducing unnecessary propagation

| Optimization | Why it helps |
| --- | --- |
| Explicit semantic field projection | Descriptions, labels, tags, and other presentation edits leave hashes unchanged, including in SQL fallback cases. |
| Separate rows and outputs | An unused projection can change without invalidating consumers of other outputs. Row changes still propagate. |
| Used-column binding | Changes to unused source/dimension columns stop before consumers that do not read them. |
| Calculation components without optional links | Adding an unused dimension link does not invalidate conservative SQL consumers of the provider's calculation. |
| Demand-specific metric/cube context | Consumers retain dimensions, roles, filters, join keys, and context rows relevant to their demand. |
| Narrow dimension dependencies | Supported context uses join keys and requested outputs instead of a dimension's whole fingerprint. |
| Recursive derived metric demands | Dimension and filter precision survives through intermediate metric formulas. |
| Effective-default filtering | Requirements without defaults and required/recommended labels add no data dependency. Caller filters can make a default irrelevant. |
| Scalar allowlist, join-chain binding, and struct binding | More queries retain output-level dependencies instead of falling back to whole calculations. |
| Structural SQL normalization | Supported alias, comment, whitespace, and redundant-wrapper edits avoid propagation. |
| Canonical collections and types | Equivalent collection orderings and recognized type spellings do not introduce accidental differences. |

### Keeping evaluation fast

- Reuse DJ's cached ANTLR AST and parse tree. Fingerprint evaluation adds no
  SQLGlot parsing pass. Parsing is still needed for uncached SQL and filters.
- Build the formula graph once per configured evaluation and reuse nonmetric
  components when attaching demands.
- Cache schemas, ancestry, link indexes, candidate paths, filter references,
  join-predicate columns, type parsing, and duplicate demands.
- Evaluate a captured in-memory graph without database calls. Database loading
  and materialization capture live outside the pure evaluator.

The snapshot copies definitions and resolved schemas, protects caller-owned ASTs
from mutation, and drops a cached AST if its recorded SQL no longer matches the
rendered query. Evaluation caches do not authorize reusing a digest after its
snapshot inputs change.

## SQL coverage and remaining extra propagation

Precise binding covers flat single-source projections, supported scalar
expressions, `WHERE`, and common global aggregates. It also covers flat,
explicitly aliased `INNER`, `LEFT`, and `CROSS` table join chains. `INNER`/`LEFT`
require complete scalar `ON` predicates; `CROSS` has no `ON` criteria. Each
predicate binds against the tables introduced so far, preventing a forward
reference from being accepted as a valid binding.
Supported functions are explicitly listed in
[`v2_dependencies.py`](v2_dependencies.py), including `NF_DATEADD` and `NF_DATEDIFF`.

Struct binding supports validated alias-qualified paths and the bounded
single-source, one-level, unquoted `root.field` form. The dependency retains the
whole source root type, so changing an unused sibling field's type can propagate.

The following retain conservative dependencies or become unknown when evidence
is incomplete:

- CTEs, subqueries/derived tables, self joins, and unsupported join forms such as
  `RIGHT`, `FULL`, `NATURAL`, `USING`, and `LATERAL`.
- Joined aggregates, `GROUP BY`, `HAVING`, `DISTINCT`, windows, set operations,
  limits/offsets, and ordering clauses.
- Unsupported functions, generators, ambiguous bindings, and missing used types.
- Syntax whose meaning is incompletely represented by the AST, including
  `TRY_CAST`, sampling, aggregate `FILTER`/null treatment, custom `TRIM`
  characters, and lateral constructs. The fallback retains the semantic tokens.

Unknown-function projections also remain row dependencies because the function
might aggregate or generate rows. Ignoring their population effect would be unsafe.

Remaining sources of extra propagation include candidate dimension paths
(the evaluator does not trace the planner-selected path), opaque SQL depending
on whole parent calculations, whole struct types, structural renames, and
syntactically different equivalent expressions. Token fallback can distinguish
harmless alias or keyword-case changes. A changed hash means the retained
contract changed; it does not establish that query execution would return
different values on today's data or that every downstream owner needs review.

## Determinism and unknown results

Canonical JSON sorts mapping keys, uses compact encoding, and rejects nonfinite
values. Dependency edges and unordered contracts are sorted explicitly. Bound
expressions retain literal meaning and provider identity. Component cycles use a
deterministic strongly connected component hash containing member payloads,
internal labelled edges, and external fingerprints.

Each component reports one of:

| Precision | Interpretation |
| --- | --- |
| `precise` | The supported binding rules identified its dependency boundary. |
| `conservative` | A known digest with broader dependencies that may overinvalidate. |
| `unknown` | Insufficient evidence to produce a usable digest. |

Missing providers/schemas, invalid filters, ambiguous bindings, and unresolved
contexts can produce unknown and propagate it. Recursive derived metric demands
are unknown even though the component evaluator supports deterministic cycles.
`compare_components()` returns `indeterminate` if either digest is missing.
Two `"unknown"` results never prove equality.

Determinism applies to the same definitions, captured external schemas, and
implementation version. It does not make nondeterministic SQL functions or
changing source contents stable. Freshness requires separate evidence. Changes
to function implementations or runtime settings outside the captured inputs
need separate versioning or invalidation.

## Materialization compatibility

Reusing saved cube data needs both the V2 semantic fingerprint and a separate
planner-derived storage contract. Equal result semantics alone cannot establish
that stored aggregate states or output bindings are readable by the new plan.

[`materialization.py`](materialization.py) hashes these storage inputs:

| Input | Included fields |
| --- | --- |
| Contract identity | Contract version, planner epoch, and target (default `druid`). |
| Stage identity | Measure, combiner, or final cube role. |
| Emitted columns | `name`, `semantic_entity`, `semantic_type`, normalized physical `type`. The semantic fields here identify planner output bindings. |
| Stage layout | Grain and dimensions. |
| Aggregate component | `name`, `aggregation`, `merge`, `params`, `merge_args`. |
| Serialization | `serialize`, `serialize_targets`, `serialize_type`. |
| Aggregability | Rule type, level, fixed grain, and the reaggregation declaration. |
| Time handling | Timestamp column, timestamp format, and stage granularity. |
| V3 component bindings | Final combined columns/grain, measure components, and component alias map. |
| V3 Druid schema | Generated `dataSchema`, including parser/parse specification, metrics, and granularity settings, excluding `dataSource` and `granularitySpec.intervals`. |

The current storage contract version and planner epoch are both `1`.

The `DruidCubeConfig` adapter covers measure and combiner stages. Custom Druid
overrides make its contract unknown. The `DruidCubeV3Config` adapter covers the
final cube artifact; preaggregation-table reuse needs its own contract.

Storage projection omits revision IDs, generated table/datasource names, generated
query SQL text, coverage intervals, and scheduling/worker/tuning settings outside
the retained schema. Aggregate and merge expressions above remain included.
The generated V3 `dataSchema` is retained with the two explicit exclusions above;
it is not filtered through the user-metadata exclusion table.

The reuse decision helper requires known current manifests, equal semantic and
storage digests, availability, freshness, and execution compatibility. Unknown,
legacy, malformed, or unsupported contracts block reuse. Known conservative
semantic digests are accepted; precision diagnostics are not in the saved manifest.

Capture also checks that the planner's node revisions still match current
revisions. An availability callback that sends a V2 receipt must provide an
explicit built `node_version` and echo the `fingerprint_v2` manifest for the
planned artifact and physical table. Legacy callbacks without a V2 receipt
remain accepted. Desired configuration alone cannot establish what produced
existing data.

**Integration boundary:** manifests are captured and availability receipts are
validated, but the new reuse decision helper currently has no production caller.
Automatic old-table adoption still uses the existing cube-change logic. The
presence of these contracts does not enable V2-based adoption by itself.

## V1 compatibility and current integration

V1 remains frozen for compatibility. The latest/default fingerprint version is
still `1`; supported versions are `1` and `2`. Deployment's default review-impact
path uses V1. Materialization manifest capture explicitly requests V2.

V1 hashes node type, the following normalized local fields, and sorted distinct
whole-parent digests:

| Node type | V1 local fields |
| --- | --- |
| Source | `columns`, `dimension_links`, `primary_key`, `catalog`, `schema_`, `table`. |
| Transform/dimension | `columns`, `dimension_links`, `primary_key`, `query`. |
| Metric | `query`, `required_dimensions`, `reaggregate`, `fixed_grain`, `required_filters`, `recommended_filters`. |
| Cube | `metrics`, `dimensions`, `filters`, column partitions. |

V1's source/transform/dimension column normalization retains descriptions,
display names, units, and non-primary-key attributes. It compares source column
types and clears transform/dimension column types. Top-level descriptions,
owners, and tags are outside V1's selected fields. V2 addresses both column
metadata noise and propagation through whole parents.

Use `compute_configured_v2_fingerprints(SemanticGraphSnapshot(...))` for V2
metric/cube evaluation, or explicitly pass `version=2` to the deployment graph
builder. `compute_v2_fingerprints()` exposes the base rows/outputs/full graph;
standalone cubes need the configured entry point. The legacy single-node
`engine.py` builder registry only implements V1.

## Code and tests

| File | Responsibility |
| --- | --- |
| [`v2.py`](v2.py) | Snapshot, component results, comparison, deterministic graph evaluation. |
| [`v2_dependencies.py`](v2_dependencies.py) | Semantic field projection, SQL binding, fallback dependencies. |
| [`v2_configured.py`](v2_configured.py) | Metric/cube demands, role-aware dimension context, derived formulas. |
| [`materialization.py`](materialization.py) | Storage contracts, manifest validation, reuse decisions. |
| [`v1.py`](v1.py), [`engine.py`](engine.py) | Frozen V1 payload and legacy dispatch. |
| [`normalization.py`](normalization.py), [`merkle.py`](merkle.py) | Shared canonicalization/diffs and graph helpers. |
| [`internal/deployment/fingerprints.py`](../internal/deployment/fingerprints.py) | Database loading and versioned deployment graph orchestration. |
| [`internal/materializations.py`](../internal/materializations.py) | Plan capture, persisted manifests, availability validation. |

Focused regression coverage lives in
[`tests/internal/deployment`](../../tests/internal/deployment) (`fingerprints_v2_*`
and V1 compatibility), [`tests/semantic_fingerprints`](../../tests/semantic_fingerprints)
(metadata, storage, planner contracts), and
[`tests/internal/materialization_fingerprints_test.py`](../../tests/internal/materialization_fingerprints_test.py)
(capture and receipt checks).
