---
weight: 80
---

# Materialization

## Cube Nodes

When we attach a materialization config to a cube node (instructions 
[here](../../getting-started/creating-nodes/cubes#adding-materialization-config)), we are requesting DJ to prepare 
for the materialization of the cube's underlying data into an OLAP database (such as Druid). This enables 
low-latency metric queries across all defined dimensions in the cube. 

However, many such databases are only configured to work with simple aggregations, so DJ will break down each
metric expression into its constituent simple aggregation measures prior to materialization. These measures are 
ingested into the OLAP database as separate columns and they're combined back together into the original metrics 
when users request metric data.

A few examples include:

<table>
<tr><th>Metric Query</th><th colspan="3">Measures Ingested</th></tr>
<tr>
<td rowspan="3">

```sql
SELECT
  AVG(price) 
    AS avg_repair_price 
FROM repair_order_details
```
</td>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`price_count`
</td>
<td>

`count`
</td>
<td>


```sql
count(price)
```
</td>
</tr>
<tr>
<td>

`price_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price)
```
</td>
</tr>

<tr>
<td rowspan="3">

```sql
SELECT
  SUM(price) 
    AS total_repair_price 
FROM repair_order_details
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`price_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price)
```
</td>
</tr>





<tr>
<td rowspan="4">

```sql
SELECT
  CAST(
    SUM(
      IF(discount > 0.0,
          1, 0)
    ) AS DOUBLE
  ) / COUNT(*)
    AS discounted_orders_rate
FROM repair_order_details
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`discount_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(
  if(
    discount > 0.0, 
    1, 0
  )
)
```
</td>
</tr>

<tr>
<td>

`count_star`
</td>
<td>

`count`
</td>
<td>

```sql
count(*)
```
</td>
</tr>




<tr>
<td rowspan="4">

```sql
SELECT
  SUM(price1) + 
    SUM(price2) 
  AS total_cost
FROM costs
```
</td>
<tr>
<th>Name</th><th>Agg</th><th>Expr</th>
</tr>
<tr>
<td>

`price1_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price1)
```
</td>
</tr>

<tr>
<td>

`price2_sum`
</td>
<td>

`sum`
</td>
<td>

```sql
sum(price2)
```
</td>
</tr>
</table>