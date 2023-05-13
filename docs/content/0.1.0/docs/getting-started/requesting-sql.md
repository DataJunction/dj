---
weight: 50
---

# Requesting SQL

DJ can generate SQL for one or more metrics with a set of compatible 
filters and dimensions.

{{< tabs "retrieving sql multiple" >}}
{{< tab "curl" >}}
```sh
curl -X 'GET' \
  'http://localhost:8000/sql/?metrics=default.num_repair_orders&dimensions=default.all_dispatchers.company_name&filters=default.all_dispatchers.company_name%20IS%20NOT%20NULL' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< tab "python" >}}
```py
dj.sql(
    metrics=[
      "default.num_repair_orders",
    ],
    dimensions=[
      "default.all_dispatchers.company_name",
    ],
    filters=[
      "default.all_dispatchers.company_name IS NOT NULL"
    ],
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.sql.get(
  metrics=["default.num_repair_orders"],
  dimensions=["default.all_dispatchers.company_name"],
  filters=["default.all_dispatchers.company_name IS NOT NULL"]
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
You can optionally provide an `engine_name` and `engine_version`. A typical DataJunction query service will include a default engine.
{{< /hint >}}