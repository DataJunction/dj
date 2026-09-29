---
name: datajunction-review
description: Review a DataJunction code change, branch, or PR for semantic correctness, security, compatibility, and operational regressions. Use for requested code reviews, not ordinary implementation or questions about using DJ.
---

# DataJunction code review

Review as a maintainer of a semantic layer. Prioritize wrong metric results,
unsafe metadata or data changes, authorization gaps, and failures across
services. Be thorough about the behavior a change affects, while keeping
findings specific and evidenced. A bug in unchanged code is in scope when the
change exposes or worsens it, or when the user requests a wider audit; explain
that connection.

## Establish the contract

- For a PR, read its title, description, linked issues, full diff, changed
  files, relevant discussion, and available CI results. Check the PR template's
  Summary, Test Plan, and Deployment Plan against the actual change: a claimed
  test or rollout plan is evidence only if it covers the behavior being changed.
  For a branch or diff range, identify its intended base and read the
  corresponding commits. For local changes, include staged, unstaged, and
  relevant untracked files.
- Infer the promised behavior from the request, tests, API/docs changes, and
  code. State the key invariant in plain language before judging whether the
  implementation preserves it.
- Read surrounding code and follow relevant callers, callees, and consumers.
  For high-risk query construction, authorization, deployment, or persistence
  changes, inspect the whole affected function or file and the corresponding
  tests, not just diff context.
- Check whether the behavior also appears through another entrypoint or
  representation: REST, GraphQL, MCP, Python/JavaScript clients, UI, repo-backed
  YAML, async jobs, or direct SQL. Follow only paths that can affect the same
  contract.
- If reviewing after a new commit or author reply, reread the current code and
  prior findings. Verify claimed fixes, avoid duplicate findings, and treat an
  explained tradeoff as a decision to evaluate rather than an invitation to
  repeat the same point.
- Do not choose a verdict until the promised contract, affected entrypoints and
  representations, failure/retry paths, and proof for material claims have each
  been addressed. If one cannot be validated, name the blind spot and the
  evidence that would close it.

## Trace DJ-specific risks when triggered

### Metric and SQL semantics

For changes under `datajunction-server/datajunction_server/sql/` or
`datajunction-server/datajunction_server/construction/`, trace the request from
node definitions and revisions through dependency loading, AST/parser,
dimension resolution, measure decomposition, query construction, and SQL
rendering. The repository's
`docs/content/0.1.0/docs/developers/how-metric-requests-are-converted-to-sql.md`
maps the main phases. Check the relevant invariants:

- Grain and cardinality: joins must not multiply measures; dimensions and role
  aliases must resolve to the intended path; combining facts or grain groups
  must retain rows and use the right keys.
- Filters and parameters: predicates must apply at the correct stage and scope
  (including before/after aggregation), with correct null, type, and temporal
  behavior. Inspect equivalent SQL/AST forms when a rule matches a construct by
  name or shape.
- Name- or shape-based rules: check aliases, alternate syntax, and any expansion
  or rewrite that can introduce the same semantics after the guard runs. Match
  the rule at the point where those forms have been normalized, or verify a
  later recheck.
- Aggregation: decomposed and derived metrics, distinct/limited aggregations,
  window metrics, and aliases must preserve the requested metric at the
  requested grain.
- Dialects: a generated query should be valid and semantically equivalent for
  the engines the changed path supports. Reject or document unsupported cases
  instead of silently generating different results.

Use a tiny data example to trace a suspected wrong-result path. A SQL snapshot
can show query shape; it does not by itself prove cardinality or result
correctness.

### Cubes, pre-aggregations, and materialization

When routing to a cube or pre-aggregation changes, compare its result with the
ordinary query path. Verify eligibility covers requested metrics, dimensions,
filters, grain, engine, availability/freshness, and any join-back or temporal
partition requirements. Follow both a match and a fallback, including stale or
partially materialized state. For materialization lifecycle changes, trace
creation, refresh/backfill, failure, retry, teardown, and what users can query
during each transition.

### Metadata, deployment, and access

For node or namespace changes, follow current versus historical revisions,
dependency links, status/mode, branch and namespace boundaries, downstream
invalidation, and cache keys. Check creation, update, rename, delete, rollback,
and repeated deployment where applicable. Partial failures must not leave
database state, generated metadata, query-service resources, and caches
disagreeing silently.

For asynchronous work, check cancellation, retries, shutdown, and callbacks
that can still write after ownership or status changes. A guard at job start
does not prove later writes are safe.

For a new option, flag, or semantic parameter, follow every relevant carrier
and consumer (API schema, stored model, cache identity, builder/context, SQL
renderer, job payload, and client as applicable). Each supported path must
preserve it, intentionally ignore it, or reject it; a single correctly wired
call path is not enough. For distributed or materialized state, distinguish
shared state from local state after restart or failover.

For a risky new query strategy, materialization mode, or persisted format,
check the default/feature gate, mixed-version behavior, and rollback path. Do
not require an experimental gate for a low-risk additive change merely by
analogy to another project.

For an access-control change, enumerate every relevant way to reach or derive
the protected object before evaluating the check's granularity. Include
metadata, generated SQL, error responses, REST/GraphQL/MCP endpoints, and
background operations as applicable; if a subclass adds the guard, inspect
inherited operations it does not override. Check the permission before
protected information is fetched or exposed. Test both an allowed and a denied
caller at the boundary that matters.

For model or persistence changes, compare the SQLAlchemy model, Alembic
migration, serialized API/client representations, and existing stored rows.
Verify upgrade behavior, meaningful downgrade behavior, defaults/backfills,
and SQLite/PostgreSQL behavior where the changed migration supports both.
`CONTRIBUTING.rst` documents the repository's migration expectations.

### API, clients, and UI

When a public request or response changes, trace the server route/schema through
generated OpenAPI where relevant, clients, and UI consumers. Check omitted
versus null values, errors/status codes, pagination, version compatibility, and
authorization. For UI changes, test the actual user state transition (loading,
failure, retry, stale response) rather than only a rendered snapshot.

## Evidence and judgment

- After the first serious failure of an invariant, make one focused pass through
  sibling paths and lifecycle transitions that share it. Look for separate
  user-impacting gaps, then stop expanding when the connection becomes
  speculative.
- A finding about a guard, permission, cache key, or routing decision does not
  establish that the mechanism is reached everywhere. Recheck completeness and
  ordering across entrypoints before closing that topic.
- Use concrete inputs and step-by-step execution traces for suspicious logic.
  Distinguish a demonstrated defect from a serious risk needing verification;
  state what evidence would settle the latter.
- Important behavior needs a focused regression test or a clear reason a test
  is impractical. Performance claims need a representative measurement. Prefer
  tests that would fail if the implementation were removed or wired to the
  wrong path. Use the relevant component's `Makefile` or
  `datajunction-ui/package.json` for test commands.
- Compare tests with the claimed contract: a snapshot or happy-path assertion
  that would pass with the wrong result is not sufficient proof. Missing
  evidence for a material correctness, compatibility, or performance claim
  affects the verdict even when the implementation looks plausible.
- Do not duplicate failures already reported clearly by build or lint CI. Skip
  style preferences and optional refactors unless they materially affect
  correctness or maintenance.
- Calibrate severity by impact and confidence: incorrect metric results, data
  loss, authorization bypass, and unsafe migrations are blocking concerns;
  realistic uncovered edge cases and operational failures are major concerns.
  Do not inflate uncertain claims into facts.

## Report

Use `Summary` and `Final Verdict` for every review. The summary says what the
change does and the high-level judgment, not just what files were inspected. In
the verdict, choose **✅ Approve**, **⚠️ Request changes**, or **❌ Block**; if
not approving, state the minimum required actions. Do not approve with
unresolved wrong-result, data-loss, access, or compatibility defects, serious
plausible risks, or missing proof for a material claim.

Add only relevant optional sections: `Findings` for actionable issues, `Tests`
for missing focused proof, `Missing context / blind spots` for material
unknowns, and `Performance & safety` or `User impact` when they add information
not already in a finding. Group findings by severity. Each finding needs a
precise `file:line`, broken contract, concrete trigger, impact, and surgical
repair direction. Use ❌ for blocking defects, ⚠️ for major risks or evidence
gaps, and 💡 only for a minor issue that materially matters. Omit empty sections
and routine checks that passed. If no issue is found, say what scope was
reviewed and what could not be verified.

Reviewing does not itself authorize posting comments or changing PR state.
Prepare the review locally; publish it only when the user explicitly asks.

For an unattended review that requests machine-readable findings, read
[references/automated-review.md](references/automated-review.md). Keep the
review judgment above; the reference defines the bot handoff, not permission to
publish.
