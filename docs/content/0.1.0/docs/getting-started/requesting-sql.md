---
weight: 50
title: "Requesting SQL"
---

## Metrics SQL

DJ can generate SQL for one or more metrics with a set of compatible filters and group by dimensions.

{{< tabs "retrieving sql multiple" >}}
{{< tab "python" >}}
```py
dj.sql(
    metrics=[
      "default.avg_repair_price",
      "default.num_repair_orders",
    ],
    dimensions=[
      "default.hard_hat.city",
      "default.hard_hat.state",
    ],
    filters=[
      "default.hard_hat.state = 'NY'"
    ],
)
```
{{< /tab >}}
{{< tab "curl" >}}
```sh
curl -X 'GET' \
  'http://localhost:8000/sql/?metrics=default.num_repair_orders&&metrics=default.avg_repair_price&dimensions=default.hard_hat.city&dimensions=default.hard_hat.state&filters=default.hard_hat.state%20%3D%20%27NY%27' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.sql.get(
  metrics=[
    "default.num_repair_orders",
    "default.avg_repair_price",
  ],
  dimensions=[
    "default.hard_hat.city",
    "default.hard_hat.state",
  ],
  filters=["default.hard_hat.state = 'NY'"]
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

{{< alert icon="👉" >}}
You can optionally provide an `engine_name` and `engine_version`. A typical DataJunction query service will include a default engine.
{{< /alert >}}

## Node SQL
DJ can generate SQL for a node with a compatible filters and dimensions.

{{< tabs "retrieving node sql" >}}
{{< tab "python" >}}
```py
dj.sql(
    node_name=[
      "default.repair_orders_fact",
    ],
    dimensions=[
      "default.hard_hat.city",
      "default.hard_hat.state",
    ],
    filters=[
      "default.hard_hat.state = 'NY'"
    ],
)
```
{{< /tab >}}
{{< tab "curl" >}}
```sh
curl -X 'GET' \
  'http://localhost:8000/sql/default.repair_orders_fact?dimensions=default.hard_hat.city&dimensions=default.hard_hat.state&filters=default.hard_hat.state%20%3D%20%27NY%27' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< /tabs >}}

## Measures SQL

DJ can also return the measures SQL for a set of metrics with group by dimensions and filters. This SQL can be used to produce an intermediate table with all the measures and dimensions needed for an analytics database (e.g., Druid).

{{< tabs "retrieving measures sql" >}}
{{< tab "python" >}}
```py
dj.sql(
    metrics=[
      "default.avg_repair_price",
      "default.num_repair_orders",
    ],
    dimensions=[
      "default.hard_hat.city",
      "default.hard_hat.state",
    ],
    filters=[
      "default.hard_hat.state = 'NY'"
    ],
    measures=True,
)
```
{{< /tab >}}
{{< tab "curl" >}}
```sh
curl -X 'GET' \
  'http://localhost:8000/sql/measures?metrics=default.num_repair_orders&&metrics=default.avg_repair_price&dimensions=default.hard_hat.city&dimensions=default.hard_hat.state&filters=default.hard_hat.state%20%3D%20%27NY%27' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< /tabs >}}
