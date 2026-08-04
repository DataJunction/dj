---
weight: 60
title: "DAG Impact Analysis"
---

A DJ server holds a single shared graph. The transform you are about to edit may be the parent of a
dimension somebody else built, which in turn feeds metrics and cubes in namespaces you have never
looked at. Because [node queries reference other nodes by name](../node-dependencies/), a change that
looks local — renaming a column, tightening a cast, dropping a field from a projection, deleting a
node that seems unused — can invalidate nodes owned by other people. Impact analysis is how DJ
answers the question that follows from that: *if I change this node, what breaks?*

DJ answers it by actually applying the change and then revalidating everything downstream of it,
rather than by guessing from the shape of the diff. That means the answer you get is the same answer
the deployment itself would produce.

---

## What DJ computes

Impact analysis runs as part of a deployment. Given the set of nodes a deployment creates, updates,
or deletes — plus any nodes whose [dimension links](../../data-modeling/dimension-links/) changed —
DJ walks the parent/child relationships breadth-first to collect every downstream node, then
revalidates each one against the post-change state of its parents. Each affected node comes back as a
`DownstreamImpact` record with these fields:

| Field | Meaning |
| --- | --- |
| `name` | The affected downstream node |
| `node_type` | Its type: source, transform, dimension, metric, or cube |
| `current_status` | Its status before the change, `valid` or `invalid` |
| `predicted_status` | Its status after the change |
| `impact_type` | The kind of impact — see below |
| `impact_reason` | A human-readable explanation, e.g. the revalidation errors that were raised |
| `depth` | How many hops downstream it is; direct children are at depth 1 |
| `caused_by` | The names of the changed nodes that this impact traces back to |
| `is_external` | Whether the node lives outside the namespace being deployed |

`caused_by` is worth dwelling on, because in a bulk deployment many nodes change at once. Causality is
propagated along with the traversal, so a metric five hops down is attributed to the specific changed
node (or nodes) that reach it, not to the deployment as a whole. Together with `depth`, that is enough
to reconstruct the chain from an edit to the thing it broke.

Revalidation is done level by level, and a node that fails is withheld from its own children's
validation. A transform whose columns no longer resolve therefore invalidates the metrics beneath it
naturally, the same way a genuinely missing parent would, instead of every level being judged against
a state that no longer exists.

---

## Impact types

`impact_type` classifies what the revalidation found:

* **`will_invalidate`** — the node's query no longer validates against its new parents, so a `valid`
  node becomes `invalid`. `impact_reason` carries the validation errors. Cubes are a special case: a
  cube has no query of its own to revalidate, so after a dimension link change DJ separately checks
  whether the cube's dimensions are still reachable from its metrics' parents, and reports the
  dimensions that are not.
* **`will_recover`** — the node was `invalid` and the change fixes it. This is what you see when a
  deployment repairs a broken upstream node and the nodes beneath it come back to life.
* **`may_affect`** — the node is downstream of something that changed and still validates. Column
  types that shifted underneath it fall here: the node is not broken, but its output schema moved, so
  it is worth a look.

---

## Where impacts are surfaced

Impact analysis is reported by deployments, not by a standalone endpoint on a node. A deployment
records two things: `results`, the per-node outcome of what was created, updated, deleted, or skipped,
and `downstream_impacts`, the list of `DownstreamImpact` records above. Both are returned when you
create a deployment and when you poll it afterwards, and both are persisted with the deployment
record, so the blast radius of a past deployment remains inspectable after the fact.

The same analysis is available without committing anything. A dry run executes the whole deployment —
setup, planning, node validation, deployment, impact propagation — inside a database savepoint, and
then rolls the savepoint back. Nothing is persisted, but the impact list computed inside it is
returned to you. Because the dry run and the real deployment go through the identical code path, the
predicted statuses you see in a preview are the statuses the real deployment would produce.

From the CLI, that is the `--dryrun` flag on a push:

```sh
dj push ./my-nodes --dryrun
```

which prints the downstream nodes that would end up `invalid`, grouped under the changed node that
caused them. `--format json` gives you the raw records if you want to gate a CI job on them. See
[Using the CLI](../../getting-started/using-the-cli/) for the full set of flags and
[YAML Projects](../../data-modeling/yaml/) for how a deployable project is laid out.

---

## Your namespace versus external consumers

Every impact is flagged with `is_external`, which is true when the affected node's name falls outside
the namespace being deployed. This is the distinction that matters for governance. Impacts confined to
your own namespace are yours to fix; you are breaking your own nodes and you will presumably repair
them in the same change. Impacts on nodes outside it are somebody else's nodes breaking because of
your edit, and the owner of those nodes is not in the room when you run the deploy.

DJ computes and reports that flag; it does not by itself refuse a deployment because external
consumers would break. What it gives you is the information — before you commit, if you dry run — to
decide whether a change is safe to make unilaterally or whether it needs to be coordinated with the
people downstream of you.

---

## Relationship to validity propagation

Impact analysis and status propagation are the same mechanism seen from two angles. As described in
[Node Dependencies](../node-dependencies/), a node has a server-defined status of `valid` or
`invalid`, and a status change propagates a revalidation to everything downstream. Impact analysis is
that propagation made visible: it performs the revalidation, writes the resulting statuses, and
returns the before-and-after pair as `current_status` and `predicted_status`. On a real deployment the
new statuses are committed along with the rest of the change. On a dry run the savepoint is rolled
back, so the statuses are discarded and only the prediction survives.

The practical consequence is that a `valid` → `invalid` transition in a downstream node is never
silent. It arrives as part of the deployment's own output, attributed to the node that caused it, at a
known distance from it.
