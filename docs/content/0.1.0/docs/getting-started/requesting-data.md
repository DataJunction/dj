---
weight: 60
title: "Requesting Data"
---

DJ can generate SQL for one or more metrics with a set of compatible filters and dimensions.

{{< tabs "retrieving sql" >}}
{{< tab "curl" >}}
```sh
curl -X 'GET' \
  'http://localhost:8000/data/?metrics=default.num_repair_orders&dimensions=default.all_dispatchers.company_name&filters=default.all_dispatchers.company_name%20IS%20NOT%20NULL' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< tab "python" >}}
```py
dj.data(
    metrics=[
      "num_repair_orders",
      "avg_repair_price"
    ],
    dimensions=[
      "hard_hat.city",
      "hard_hat.state",
      "dispatcher.company_name"
    ],
    filters=[
      "hard_hat.state = 'AZ'"
    ],
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.data.get(
  metrics=["default.num_repair_orders"],
  dimensions=["default.all_dispatchers.company_name"],
  filters=["default.all_dispatchers.company_name IS NOT NULL"]
).then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

{{< alert icon="👉" >}}
You can optionally provide an `engine_name` and `engine_version`. A typical DataJunction query service will include a default engine.
{{< /alert >}}
{{< alert icon="👉" >}}
When using the python client, retrieving data requires that you have [pandas](https://pandas.pydata.org/) installed locally.
You can install pandas by running `pip install pandas`.
{{< /alert >}}
