---
weight: 10
---

# Nodes

In DJ, nodes play a central role. Understanding the relationships between nodes is key to understanding how DJ works.
All node types are similar in many ways. Let’s start by covering their similarities.

---

## Similarities Between Node Types

A summary of things that are true of all nodes:

* All nodes have a name and a description
* All nodes have a schema defined as named columns, each with a specific type
* All nodes have a system-defined state of either valid or invalid
* All nodes have a user-defined mode of either draft or published
* All nodes track the parent nodes they depend on

In addition to these universal statements about nodes, there are things that are common to a subset of node types.

* **Metrics**, **dimensions**, and **transforms** always have a query.
* **Source** nodes do not have a query but have a reference to an external table.
* Queries can reference any node by name, including **source** nodes

Some node types have unique attributes, behaviors, and restrictions. For example, a **metric** node’s query can only include
a single field in the select statement and that field must be used in an aggregation expression. Also, **source** nodes
never have a query at all and instead must include a reference to an external table.

---

## Source Nodes

Real tables in a database or data warehouse are represented in DJ as source nodes. Each source node has a name and that
name can be used by transform and dimension nodes as a “virtual” reference to the real table. In fact, you can change
the table name in a source node’s definition to use a different real table and as long as the table schema is the same,
you can be certain that all valid downstream nodes will remain valid.

If a DJ deployment uses a reflection service, the main responsibility of that reflection service is to keep the schema
defined in the source node in sync with the schema of the real table. This ensures that breaking changes to real tables
are immediately identified and the impact to downstream nodes are communicated. When a source node’s schema is
automatically updated to incorporate a change to the real table’s schema, this can break downstream nodes that relied on
particular aspects of the table's schema that no longer exist. These breaking changes are communicated by labeling
the impacted nodes as “invalid”.

---

## Transform Nodes

A lot of the heavy lifting in DJ is done by transform nodes. These nodes contain the queries that join, filter, and
group data from various source nodes as well as other transform nodes. Although less common, transform nodes can even
perform manipulations of dimension nodes.

Since transform nodes can apply SQL manipulations of other transform nodes, they allow you to break up very complicated
logic into smaller incremental processing steps. This is a very powerful feature and proper design of transform nodes
can help optimize the speed and efficiency of a DJ server. Let’s take an example of two very similar queries that
produce different tables.

*Query #1 - Recent Transaction Amounts by Customers with Active Accounts*

```sql
SELECT
t.amount
,t.purchase_date
,c.id as customer_id
,c.first_name as customer_first_name
,c.last_name as customer_last_name
FROM transaction AS t
LEFT JOIN customer AS c
ON t.customer_id = c.id
WHERE c.status = 'active'
AND t.purchase_date >= (3 months ago)
```

*Query #2 - Recent Transaction Amounts by Customers with Non-Trial Active Accounts*

```sql
SELECT
t.amount
,t.purchase_date
,c.id as customer_id
,c.first_name as customer_first_name
,c.last_name as customer_last_name
FROM transaction AS t
LEFT JOIN customer AS c
ON t.customer_id = c.id
WHERE c.status = 'active'
AND t.purchase_date >= (3 months ago)
AND c.account_type <> 'trial'
```

If you look closely, you can see similarities between both queries. Both queries are joining the transaction table to
the customer table and filtering out transactions by customers who have since deleted their account as well as
filtering to only include transactions that have happened in the past three months. However, they differ in that query
#2 also filters out customers who are using a free trial account. If these queries are materialized daily, the join and
two out of the three filters are performed twice a day.

A better design would be building query #2 as an additional transformation of query #1. Let’s say you created a DJ
transform node for query #1.

*recent_transactions_active_customers*

```sql
SELECT
t.amount
,t.purchase_date
,c.id as customer_id
,c.first_name as customer_first_name
,c.last_name as customer_last_name
FROM transaction AS t
LEFT JOIN customer AS c
ON t.customer_id = c.id
WHERE c.status = 'active'
AND t.purchase_date >= (3 months ago)
```

You can then create the equivalent of query #2 by defining a transform node that queries that transform node already
defined.

*recent_transactions_non_trial_active_customers*

```sql
SELECT
amount
,purchase_date
,customer_id
,customer_first_name
,customer_last_name
FROM recent_transactions_active_customers
WHERE account_type <> 'trial'
```

With this design, materializing *recent_transactions_active_customers* is enough to no longer require performing a join
to get the data for both nodes. If the filter to non-trial accounts is fast, you may choose not to materialize the
second node at all!

---

## Dimension Nodes

One of the benefits of DJ is that it can easily find all of the available dimensions that you can use to group metrics
as well as all of the metrics that can be grouped by a set of dimensions. Defining a dimension node includes a query to
generate the dimension dataset as well as a label of the dimension’s primary key(s).

If another node includes a foreign key for an existing dimension node, you can include a reference to the dimension
node’s primary key in the other node’s definition. Furthermore, a dimension itself can include a foreign key that
includes a reference to another dimension node’s primary key, meaning that dimension is also available as a second-join
dimension. This metadata is what allows DJ to understand the relationships between metrics and dimensions and allows
abstracting away the `JOIN` and `GROUP BY` clauses required to bring metrics and dimensions together!

---

## Metric Nodes

The primary component of a request for SQL or data from a DJ server is always one or more metrics. A metric node is
defined as a single column from another existing node as well as an aggregation expression. If the existing node has
other columns that are connected to dimension nodes, those dimensions will be revealed by DJ as available dimensions
with which the metric can be grouped by. Additional dimensions will also be available if first-join dimensions contain
foreign key(s) to other dimensions.

---

## Cube Nodes

In data analytics, a cube is a multi-dimensional dataset of one or more metrics. As more nodes are defined in DJ, a
single metric can have a wide selection of dimension sets with which it can be grouped by. Also, many metrics will
share common dimensions making it possible to create cubes of multiple metrics and multiple dimensions.
Although materializing upstream transform nodes can serve as a huge performance optimization when creating these cubes,
the final `JOIN` and `GROUP BY` operations happen at the moment a particular cube is requested.

Since it’s not practical to schedule the materialization of all possible combinations of metrics and dimensions,
cube nodes allow you to define specific sets of metrics and dimensions that should be materialized. This is useful
when you want to to pre-compute a dataset that’s used by an analytics product such as a dashboard or report.
