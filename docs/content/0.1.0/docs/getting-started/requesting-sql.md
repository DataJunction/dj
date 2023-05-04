---
weight: 50
---

# Requesting SQL

DJ can generate SQL for one or more metrics with a set of compatible 
filters and dimensions.

{{< tabs "retrieving sql multiple" >}}
{{< tab "curl" >}}
```sh
curl -X GET "http://localhost:8000/sql/?metrics=num_repair_orders&metrics=avg_repair_price&dimensions=hard_hat.city&dimensions=hard_hat.state&dimensions=dispatcher.company_name&filters=hard_hat.state%3D%27AZ%27"
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
dj.sql(
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