---
weight: 50
---

# Requesting SQL

DJ can generate SQL for one or more metrics with a set of compatible 
filters and dimensions.

## SQL for a Single Metric

{{< tabs "retrieving sql" >}}
{{< tab "curl" >}}
```sh
curl -X GET http://localhost:8000/sql/num_repair_orders/ \
-H 'Content-Type: application/json' \
-d '{
    "dimensions": [
      "hard_hat.city",
      "hard_hat.state",
      "dispatcher.company_name"
    ],
    "filters": [
      "hard_hat.state = ''AZ''"
    ],
    "engine_name": "SPARKSQL",
    "engine_version": "3.1.1"
}'
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")

# Assumes that the metric has been created
metric = dj.metric("num_repair_orders")
metric.sql(
    dimensions=[
      "hard_hat.city",
      "hard_hat.state",
      "dispatcher.company_name"
    ],
    filters=[
      "hard_hat.state = 'AZ'"
    ],
    engine_name="SPARKSQL",
    engine_version="3.1.1",
)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
The `engine_name` and `engine_version` fields are optional. A typical DataJunction query service will include a default engine.
{{< /hint >}}

## SQL for Multiple Metrics

{{< tabs "retrieving sql multiple" >}}
{{< tab "curl" >}}
```sh
curl -X GET "http://localhost:8000/sql/?metrics=num_repair_orders,avg_repair_price&dimensions=hard_hat.city,hard_hat.state,dispatcher.company_name&filters=hard_hat.state='AZ'" \
-H 'Content-Type: application/json' \
-d '{
    "metrics": [
      "num_repair_orders",
      "avg_repair_price"
    ],
    "dimensions": [
      "hard_hat.city",
      "hard_hat.state",
      "dispatcher.company_name"
    ],
    "filters": [
      "hard_hat.state = ''AZ''"
    ],
    "engine_name": "SPARKSQL",
    "engine_version": "3.1.1"
}'
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
    engine_name="SPARKSQL",
    engine_version="3.1.1",
)
```
{{< /tab >}}
{{< /tabs >}}
