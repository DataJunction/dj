---
weight: 30
---

# Listing Metrics

One of the most imporant entities in DataJunction are metrics. Exploring DataJunction usually starts with
exploring available metrics.    

{{< tabs "listing metrics" >}}
{{< tab "curl" >}}
```sh
curl -X GET http://localhost:8000/metrics/
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
metrics = dj.metrics()
```
{{< /tab >}}
{{< /tabs >}}

You can also narrow the list of metrics to a specific namespace. Here's an example of only listing metrics in a `roads.demo`
namespace.

{{< tabs "listing metrics in a namespace" >}}
{{< tab "curl" >}}
```sh
curl -X GET http://localhost:8000/namespaces/roads.demo/?type_=metric
```
{{< /tab >}}
{{< tab "python" >}}
```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
namespace = dj.namespace("roads.demo")
namespace.metrics()
```
{{< /tab >}}
{{< /tabs >}}

# Metric Details

After selecting a metric, you can retrieve details for the given metric. Here's an example of retrieving
details for a metric named `num_repair_orders`.

{{< tabs "listing metric details" >}}
{{< tab "curl" >}}
```sh
curl -X GET http://localhost:8000/metrics/num_repair_orders/
```
{{< /tab >}}
{{< tab "python" >}}
```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
metric = dj.metric("num_repair_orders")
```
{{< /tab >}}
{{< /tabs >}}

Metric details include the available dimensions that are discoverable through the DataJunction DAG. See the pages on
[Requesting Data](../requesting-data) and [Requesting SQL](../requesting-sql) to learn how combinations of metrics
and dimensions can be used.