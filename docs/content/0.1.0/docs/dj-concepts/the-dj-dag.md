---
weight: 20
title: The DJ DAG
---

Everything a DJ server knows about your data model lives in a single directed acyclic graph. The nodes of that graph
are the [nodes](../nodes/) you define—sources, transforms, dimensions, metrics, and cubes—and the edges are the
dependencies between them. When you ask DJ for the SQL behind a metric, when DJ tells you which dimensions a metric
can be grouped by, or when a schema change marks a set of nodes invalid, what is happening underneath is a traversal
of this graph. It's worth understanding how it gets built.

---

## Nodes and Edges

A node is the unit you author and version. Each node has a stable identity—its name, type, and namespace—and a
history of revisions, one of which is current at any moment. Edges are recorded between a parent *node* and a child
*node revision*. This asymmetry matters: the set of upstreams belongs to a specific revision of the child, because a
revision is exactly the thing whose query determined those upstreams, while the parent is referenced by name and so
resolves to whatever revision of the parent is current.

From the child's side these edges are its `parents`; from the parent's side they are its `children`. A node's
**upstreams** are the nodes it depends on and its **downstreams** are the nodes that depend on it, and both directions
are walked breadth-first over the same edge table.

DJ also tracks edges that don't resolve yet. If a node's query references a name that no node currently has, that
reference is recorded as a *missing parent* rather than being dropped, and it is reconsidered when new nodes appear.
This is what makes it possible to build a DAG out of order, and it's covered in more detail on
[Node Dependencies](../node-dependencies/).

---

## Edges Are Derived, Not Declared

You never write down a node's parents. DJ discovers them by parsing the node's query.

When a transform, dimension, or metric node is created or updated, DJ parses its SQL into an AST and collects the
table references in it, excluding names bound locally by CTEs. Those names are then resolved against the nodes the
server already knows about. Names that resolve become real edges; names that don't become missing parents. Consider
this transform:

```sql
WITH recent AS (
  SELECT * FROM default.repair_orders WHERE order_date >= '2024-01-01'
)
SELECT r.repair_order_id, r.order_date, d.company_name
FROM recent AS r
LEFT JOIN default.dispatchers AS d
ON r.dispatcher_id = d.dispatcher_id
```

Two edges come out of this: one from `default.repair_orders` and one from `default.dispatchers`. The name `recent` is
a CTE, not a node, so it contributes nothing. Nothing about the parent relationship was configured—rewriting the
query to read from a different node moves the node in the DAG, automatically.

Two node types are handled specially. **Source** nodes have no query that DJ parses for upstreams, so they have no
derived edges at all; they are the roots of the graph, and their only real dependency is the external table they
point at. **Cube** nodes have
no query of their own either—their upstreams are read directly from their declared contents: each metric they list, plus the
dimension node behind each dimension attribute they list (`default.hard_hat.state` contributes an edge from
`default.hard_hat`).

Metrics that are defined in terms of other metrics are a third case. Such a query has no `FROM` clause, so there are
no table references to collect. DJ instead treats namespaced column identifiers in the expression as candidate parent
names, resolves each against the database, and keeps only the ones that turn out to be metric nodes—the rest are
dimension attribute references, which are not SQL parents.

---

## Node Types and Their Place in the Graph

The five node types occupy characteristic positions in the DAG.

**Source** nodes are the roots. Rather than a query over other nodes, they carry a reference to a table or view in a
catalog, along with the column names and types that table is expected to have. Because they are the only point at which the graph touches
physical storage, they are also where external schema changes enter it—see
[Table Reflection](../table-reflection/).

**Transform** nodes are the interior of the graph. They read from sources and from other transforms, and because they
can be chained arbitrarily deep they are how complicated logic is decomposed into reusable steps.

**Dimension** nodes look like transforms structurally—a named node with a query over other nodes—but they carry a
primary key and are eligible to be linked into other nodes. They participate in the query-parent graph like any other
queried node, *and* they are the targets of the dimension-link edges described below.

**Metric** nodes are the leaves of the query graph. A metric's query is a single aggregation expression over one
upstream node, so a metric normally has exactly one parent, and requests for SQL or data always start from one or
more metrics.

**Cube** nodes sit downstream of metrics. A cube is a declared bundle of metrics and dimension attributes, which makes
it a child of every metric it names and of every dimension node it draws an attribute from. Cubes are the usual
handle for [materialization](../materialization/).

---

## Dimension Links Are a Second Kind of Edge

Query-parent edges say "this node reads from that node." They are not sufficient to describe a star schema, because
the relationship between a fact table and a dimension table isn't a read—it's a join that hasn't happened yet, and
may never happen unless somebody asks for it. So DJ records those separately, as **dimension links**.

A dimension link attaches a source, transform, or dimension node to a dimension node and stores the join itself: the
join SQL, the join type, the join cardinality, and optionally a Spark join strategy hint and a default value to
substitute when a `LEFT JOIN` produces no match. A link may also carry a **role**, which is what allows the same
dimension node to be linked in more than once for different meanings—a `users` node with both a birth date and a
registration date can link twice to a `date` dimension, once per role, and stay unambiguous. Alongside these join
links, DJ supports *reference* links, which alias a column on a node directly to a column on a dimension node
without a join definition.

Links live on the node revision, so they are versioned with the node like its query is, but they are stored and
traversed independently of the parent graph. Traversal is what makes them useful. To answer "what can I group this
metric by," DJ starts from the metric's non-metric parents, follows their dimension links, then follows the links on
the dimensions it reaches, and so on—a dimension two links away is just as available as one link away, and the
recorded join SQL along the path is exactly what DJ needs to emit the `JOIN` clauses for you. For a metric, the
answer is the *intersection* of what each of its underlying non-metric parents can reach, since a grouping is only
meaningful if every input supports it.

Dimension links are the subject of [Dimension Discovery](../dimension-discovery/), and the authoring syntax is
documented under [Dimension Links](../../data-modeling/dimension-links/).

---

## Why the DAG Matters

Three things follow from having the model in graph form.

**SQL generation walks it.** DJ doesn't store a compiled query per node. When you request SQL for a set of metrics
and dimensions, DJ walks upward from those metrics through their parents, assembling each upstream node's query as it
goes, and walks the dimension links to add the joins needed to reach the requested dimensions. This is why changing
an upstream transform changes downstream results with no downstream edit: the downstream SQL was never a copy in the
first place.

**Validity propagates along it.** Nodes have a server-assigned status of `valid` or `invalid`. Because a node's
validity depends on its upstreams—the names must exist, the columns must exist, the types must work—a status change
can't be a local fact. When a node becomes valid, DJ collects its downstreams, sorts them topologically so that each
is revalidated only after its own upstreams have been, and revalidates them in that order, repeating outward from any
that newly became valid. Invalidation moves the same way.

**Changes ripple downstream.** Updating a node also means finding everything that reads from it, which is a
downstream traversal of the parent edges. Being acyclic is what makes any of this terminate; deployments sort the
graph they are given into topological levels and reject it outright if it contains a cycle.

The practical consequence is that the shape of your DAG is a design decision, not an accident of how you happened to
write your queries. Where you choose to draw node boundaries determines what can be reused, what can be materialized
once and shared, and how far a breaking change travels.
