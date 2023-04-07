---
weight: 40
---

# Node Dependencies

Relationships between nodes are tracked by a DJ server. A node's position in the DJ DAG is determined by the node's definition,
particularly the query. Node queries reference other DJ nodes and this is what defines upstream and downstream dependencies
for any given node. In other words--given a node, the other nodes it queries are its **upstream** dependencies and nodes that
query it are its **downstream** dependencies.

---

## Source Node Dependency Validation

Source nodes make up the foundational layer that other nodes are built upon. The only dependency for a source nodes is the external
table that it represents. Therefore, dependency validation for a source node is unique relative to other node types. DJ allows
you to create a source node with any set of columns and if you are running an optional reflection service, the column and column types
will be set automatically to match the external table.

---

## Transform, Metric, and Dimension Node Dependency Validation

Since transform, metric, and dimension nodes contain queries that reference other nodes, the process of validating their dependencies
is different than validating dependencies for source nodes. Here is a summary of the validations that are performed for these kinds of
nodes.

* All node names used in the query must exist in the DJ DAG and have a status of `valid`
* All columns used in projections must exist in the referenced node
* All columns used in functions or operations must match the type requirements

---

## Node Status

Nodes have a **server defined** status of `valid` or `invalid`. When a node meets all of the dependency requirements, the server sets
the node's status to `valid`. Upstream changes, such as deleting nodes or dropping columns, can cause a `valid` node to become
`invalid`--consequently, all downstream nodes will also have their status updated to `invalid`. In fact, all status changes of a node
propagates a revalidation of all of its downstream nodes.

---

## Node Mode

Nodes have a **user defined** mode of `published` or `draft`. When a user is creating or updating a node in `published` mode,
the server enforces a requirement that the node definition has a `valid` status. In `draft` mode, however, a node can be created with
broken references to missing upstream nodes or columns. This allows clients to quickly and interactively develop portions of a DJ DAG in any order,
delaying the dependency validation until the moment the nodes in the DAG are switchd from `draft` mode to `published` mode.

---

## Missing Upstream Nodes

As mentioned, one of the reasons a node may have a status of `invalid` is that it contains references to upstream nodes that do not exist.
These broken references are tracked by a DJ server. Each time a new node is created, these missing links to upstream nodes are resolved and converted
to links between the two nodes. The `invalid` node is also revalidated to determine if the node's status has changed.
