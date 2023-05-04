---
weight: 60
---

# Requesting Data

DJ can generate SQL for one or more metrics with a set of compatible filters and dimensions.

{{< tabs "retrieving sql" >}}
{{< tab "curl" >}}
```sh
curl -X GET "http://localhost:8000/data/?metrics=num_repair_orders&metrics=avg_repair_price&dimensions=hard_hat.city&dimensions=hard_hat.state&dimensions=dispatcher.company_name&filters=hard_hat.state%3D%27AZ%27"
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
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
{{< /tabs >}}

{{< hint info >}}
You can optionally provide an `engine_name` and `engine_version`. A typical DataJunction query service will include a default engine.
{{< /hint >}}
{{< hint info >}}
When using the python client, retrieving data requires that you have [pandas](https://pandas.pydata.org/) installed locally.
You can install pandas by running `pip install pandas`.
{{< /hint >}}