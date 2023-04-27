---
weight: 15
---

# Namespaces

All nodes in DataJunction exist within a namespace which is represented as the prefix elements of a node's name. Elements in
node names are separated by dots (`.`). Nodes that do not include any dots in the name are automatically defined in the `default`
namespace.

| Node Name           | Namespace    |
|---------------------|--------------|
| roads.demo.repairs  | roads.demo   |
| finance.revenue     | finance      |
| hr.people.employees | hr.people    |
| customer            | default      |

Since namespaces are inferred directly from the node name, creating a node in a particular namespace simply requires prefixing
the node name with the namespace. In other words, creating a node named `financ` will automatically create the node in the
`finance` namespace.

# Creating Namespaces

Before creating nodes in a namespace, the namespace must already exist. Here is an example of creating a `roads.demo` namespace.

{{< tabs "creating namespaces" >}}
{{< tab "curl" >}}
```sh
curl -X POST http://localhost:8000/namespaces/roads.demo/
```
{{< /tab >}}
{{< tab "python" >}}

```py
from datajunction import DJClient

dj = DJClient("http://localhost:8000/")
namespace = dj.new_namespace("roads.demo")
```
{{< /tab >}}
{{< /tabs >}}
