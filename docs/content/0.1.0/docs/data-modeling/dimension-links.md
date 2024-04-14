---
weight: 6
title: "Dimension Links"
mermaid: true
---

## Dimension Links

Linking dimensions helps build out the DJ's dimensional metadata graph, a key component of the DJ DAG.
There are two types of dimension links: [join links](#join-links) and [alias/reference links](#reference-link).

### Join Links

You can configure a join link between a dimension node and any table/view-like DJ nodes (sources, transforms, 
dimensions). Configuring this join link will make it so that all dimension attributes on the dimension node are 
accessible by the original node. 

Let's look at an example of this in action. Here is a simple dimension join link configured between the 
`events` source node and the `user` dimension node:

{{< mermaid class="bg-light text-center" >}}
erDiagram
    "events" {
        user_id long 
        country_id int
        event_secs long
        event_ts long
    }
   "user" {
        int id PK
        str username
        str name
        str registration_country
    }
   "events" ||--o{ "Dimension Join Link" : "linked via"
   "user" ||--o{ "Dimension Join Link" : "linked via"

   "Dimension Join Link" {
        str join_on "events.user_id = user.id"
        enum join_type "LEFT"
        str role "event_user"
    }
{{< /mermaid >}}

This join link was configured using `events`'s `user_id` column joined to the `user` dimension's `id` column. This
tells DJ how to perform a join, should we ever need the `country` dimension for any of the fact's downstream metrics.

{{< alert icon="👉" >}}
In most cases, the join link's `join_on` clause will just be equality comparisons between the primary key and foreign 
key columns of the original node and the dimension node. However, more complex join clauses can be configured if 
desired, including the ability to specify `RIGHT`, `LEFT` or `INNER` join links.
{{< /alert >}}

Let's also assume that this metric `total_event_duration` was created using the `fact` transform node:

{{< mermaid class="bg-light text-center" >}}
classDiagram
    direction LR
    class events {
      user_id -> long
      country_id -> int
      event_secs -> long
      event_ts -> long
    }
    class total_event_duration {
      query -> sum(event_secs)
    }
    events <.. total_event_duration
{{< /mermaid >}}

After the dimension link, when someone asks DJ for the `total_event_duration` metric grouped by the `user`'s 
`registration_country`, DJ will use the configured join link to build a join between the `user` dimension and the 
`events` source node. 

All dimension attributes on the `user` dimension node (`id`, `username`, `name` etc) 
will be available to the `total_event_duration` metric to optionally group or filter by.

#### Additional Links

Some dimensions themselves may include join links to other dimensions, adding to the available join paths and 
allowing DJ to discover more dimensions that can be used by downstream metrics. Let's walk through an example of such
a case.

Extending from the example above, let's add on a `country` dimension node:

{{< mermaid class="bg-light text-center" >}}
erDiagram
    "events" {
        user_id long 
        country_id int
        event_secs long
        event_ts long
    }
   "user" {
        int id PK
        str username
        str name
        str registration_country
    }
   "country" {
        int id PK
        str name
        long population
    }
   "events" ||--o{ "Dimension Join Link" : "linked via"
   "user" ||--o{ "Dimension Join Link" : "linked via"

   "Dimension Join Link" {
        str join_on "events.user_id = user.id"
        enum join_type "LEFT"
        str role "event_user"
    }
{{< /mermaid >}}

This can be linked to the `user` dimension node using the node's `registration_country` column:

{{< mermaid class="bg-light text-center" >}}
erDiagram
    "events" {
        user_id long 
        country_id int
        event_secs long
        event_ts long
    }
   "user" {
        int id PK
        str username
        str name
        str registration_country
    }
   "country" {
        int id PK
        str name
        long population
    }
   "events" ||--o{ "Dimension Join Link" : "linked via"
   "user" ||--o{ "Dimension Join Link" : "linked via"

   "user" ||--o{ "Dimension Join Link 2" : "linked via"
   "country" ||--o{ "Dimension Join Link 2" : "linked via"

   "Dimension Join Link" {
        str join_on "fact.user_id = user.id"
        enum join_type "LEFT"
        str role "event_user"
    }

   "Dimension Join Link 2" {
        str join_on "user.registration_country = country.id"
        enum join_type "LEFT"
        str role "registration_country"
    }
{{< /mermaid >}}

Now `events` and its downstream metric `total_event_duration` will have additional dimensions available:
`country.id`, `country.name`, `country.population`, in additional to the dimensions from the `user` dimension
node.

#### Dimension Roles

{{< alert icon="👉" >}}
Note the `role` attribute on the dimension link from above.
{{< /alert >}}

The dimension link's `role` attribute is used to distinguish between dimensions that play different roles 
for a given node. In this case, the `country` dimension represents a user's `registration_country`, but it can
also play a different role, like representing the country an event was recorded in.

Let's look at an example:

{{< mermaid class="bg-light text-center" >}}
erDiagram
    "events" {
        user_id long 
        country_id int
        event_secs long
        event_ts long
    }
   "user" {
        int id PK
        str username
        str name
        str registration_country
    }
   "country" {
        int id PK
        str name
        long population
    }

   "user" ||--o{ "Dimension Join Link 2" : "linked via"
   "country" ||--o{ "Dimension Join Link 2" : "linked via"

   "events" ||--o{ "👉Dimension Join Link 3" : "linked via"
   "country" ||--o{ "👉Dimension Join Link 3" : "linked via"

   "events" ||--o{ "Dimension Join Link" : "linked via"
   "user" ||--o{ "Dimension Join Link" : "linked via"

   "Dimension Join Link" {
        str join_on "events.user_id = user.id"
        enum join_type "LEFT"
        str role "event_user"
    }

   "Dimension Join Link 2" {
        str join_on "user.registration_country = country.id"
        enum join_type "LEFT"
        str role "registration_country"
    }

   "👉Dimension Join Link 3" {
        str join_on "events.country_id = country.id"
        enum join_type "LEFT"
        str role "👉event_country"
    }
{{< /mermaid >}}

In this case, the `country` dimension was linked to both `user` (as the role `registration_country`) and to `event` (as
the role `event_country`). After this link is in place, request the `country.name` dimension for the `fact` node will 
be ambiguous without choosing a role. 

DJ will distinguish between the two dimension roles with the following syntax:
* `country.name[registration_country]`
* `country.name[event_country]`

Or more generally, `<dimension>[<role>]`. The `[role]` part can be safely omitted if there is only a single role 
defined for that dimension.

### Reference Link

You can configure a dimension alias/reference between a particular column on a table/view-like node
(source, transform, dimension) and a column on a dimension node. An example of the alias/reference link:
{{< mermaid class="bg-light text-center" >}}
erDiagram
    "events" {
        long user_id 
        string country_name
        long event_secs
        long event_ts
    }
   "country" {
        int id PK
        str name
        long population
    }
   "events" ||--o{ "Dimension Alias/Ref": ""
   "country" ||--o{ "Dimension Alias/Ref" : ""

   "Dimension Alias/Ref" {
        str column "events.country_name"
        str dimension_column "country.name"
        str role "event_country"
    }
{{< /mermaid >}}

In this case, configuring a reference between `events.country_name` and `country.name` will indicate that the 
semantic meaning behind the `events.country_name` column refers to the `default.country` dimension's `name` field.

If someone requests `events` with the dimension `default.country.name` (or the metric `total_event_duration` with the 
same dimension), DJ will know **not** to perform a join to the country dimension, since the `name` attribute is 
already available directly on the original node.

## Connecting Node Columns to Dimensions

Any column on any node can be identified as a join key to a dimension node column by making
a `POST` request to the columns. For example, let's assume you have a `hard_hats` node that contains
employee information. The state in which the employee works is stored in a separate lookup table
that includes a mapping of `hard_hat_id` to `state_id`.


{{< mermaid class="bg-light text-center" >}}
classDiagram
    direction LR
    
    class hard_hat_state {
      hard_hat_id -> int
      state_id -> int
    }

    hard_hats <-- hard_hat_state : hard_hat_id

    hard_hats : hard_hat_id -> int
    hard_hats : first_name -> str
    hard_hats : last_name -> str
    hard_hats : title -> str
    hard_hats : birth_date -> date
{{< /mermaid >}}

This connection in DJ can be added using the following request.
{{< tabs "connecting dimension" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/nodes/default.repair_orders/columns/dispatcher_id/?dimension=default.all_dispatchers&dimension_column=dispatcher_id' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< tab "python" >}}

```py
dimension = dj.dimension("default.repair_orders")
dimension.link_dimension(
    column="dispatcher_id",
    dimension="default.dispatcher",
    dimension_column="dispatcher_id",
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.dimensions.link("default.repair_orders", "dispatcher_id", "default.all_dispatchers", "dispatcher_id").then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}
