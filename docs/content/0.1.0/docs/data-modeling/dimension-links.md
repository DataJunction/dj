---
weight: 6
title: "Dimension Links"
mermaid: true
---

## Dimension Links

Dimension links help build out DJ's dimensional metadata graph, a key component of the DJ DAG.
There are two types of dimension links: [join links](#join-links) and [alias/reference links](#reference-link).

### Join Links

You can configure a join link between a dimension node and any source, transform, or dimension nodes. Configuring this
join link will make it so that all dimension attributes on the dimension node are accessible by the original node. 

Let's look at an example to understand what "accessible" means in this context.

Here is a simple dimension join link configured between the `events` source node and the `user` dimension node:
<!--

  background-color: #ffefd0 !important;
  color: #a96621;
  background-color: #cf7d2950 !important;
  color: #cf7d29;

  background-color: #7eb46150 !important;
  color: #7eb461;

-->

{{< mermaid class="bg-light text-center" >}}
erDiagram
    %%{init: {"theme": "default", "themeCSS": [
        "[id*=events] .er.entityBox { fill: #7eb46150; stroke: #7eb46150; } [id*=events] .er.attributeBoxEven { fill: #fff; stroke: #7eb46150; } [id*=events] .er.attributeBoxOdd { fill: #fff; stroke: #7eb46150; }",
        "[id*=user] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=user] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=user] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }"
    ]}}%%
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
tells DJ how to perform a join, should we ever need the `country` dimension for any of the `events` node's 
downstream metrics.

{{< alert icon="👉" >}}
In most cases, the join link's `join_on` clause will just be equality comparisons between the primary key and foreign 
key columns of the original node and the dimension node. However, more complex join clauses can be configured if 
desired, including the ability to specify `RIGHT`, `LEFT` or `INNER` join links.
{{< /alert >}}

Let's also assume that this metric `total_event_duration` was created using the `events` source node:

{{< mermaid class="bg-light text-center" >}}
erDiagram
    %%{init: {"theme": "default", "themeCSS": [
        "[id*=TotalEventDuration] .er.entityBox { fill: #fad7dd; stroke: #a27e8650; } [id*=TotalEventDuration] .er.attributeBoxEven { fill: #fff; stroke: #a27e8650; } [id*=TotalEventDuration] .er.attributeBoxOdd { fill: #fff; stroke: #a27e8650; } ",
        "[id*=events] .er.entityBox { fill: #7eb46150; stroke: #7eb46150; } [id*=events] .er.attributeBoxEven { fill: #fff; stroke: #7eb46150; } [id*=events] .er.attributeBoxOdd { fill: #fff; stroke: #7eb46150; } "
    ]}}%%
    "events" {
        user_id long 
        country_id int
        event_secs long
        event_ts long
    }
   "TotalEventDuration"["total_event_duration"] {
        query sum[event_secs]
    }
   "events" ||--o| "TotalEventDuration" : "queries from"
{{< /mermaid >}}

After the dimension link, all dimension attributes on the `user` dimension node (`id`, `username`, `name` etc) 
will be available to the `total_event_duration` metric to optionally group or filter by.  When someone asks DJ for
the `total_event_duration` metric grouped by the `user`'s `registration_country`, DJ will use the configured join link
to build a join between the `user` dimension and the `events` source node.

#### Configuring Join Links

Dimension join links can be configured in DJ using the following:

{{< tabs "connecting dimension" >}}
{{< tab "curl" >}}
```sh
curl -X 'POST' \
  'http://localhost:8000/nodes/default.events/columns/user_id/?dimension=default.user&dimension_column=id' \
  -H 'accept: application/json'
```
{{< /tab >}}
{{< tab "python" >}}

```py
dimension = dj.dimension("default.events")
dimension.link_dimension(
    column="user_id",
    dimension="default.user",
    dimension_column="id",
)
```
{{< /tab >}}
{{< tab "javascript" >}}
```js
dj.dimensions.link("default.events", "user_id", "default.user", "id").then(data => console.log(data))
```
{{< /tab >}}
{{< /tabs >}}

#### Additional Links

Some dimensions themselves may include join links to other dimensions, adding to the available join paths and 
allowing DJ to discover more dimensions that can be used by downstream metrics. Let's walk through an example of such
a case.

Extending from the example above, let's add on a `country` dimension node:

{{< mermaid class="bg-light text-center" >}}
%%{init: {"theme": "default", "themeCSS": [
    "[id*=events] .er.entityBox { fill: #7eb46150; stroke: #7eb46150; } [id*=events] .er.attributeBoxEven { fill: #fff; stroke: #7eb46150; } [id*=events] .er.attributeBoxOdd { fill: #fff; stroke: #7eb46150; }",
    "[id*=user] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=user] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=user] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }",
    "[id*=country] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=country] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=country] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }"
]}}%%
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
%%{init: {"theme": "default", "themeCSS": [
    "[id*=events] .er.entityBox { fill: #7eb46150; stroke: #7eb46150; } [id*=events] .er.attributeBoxEven { fill: #fff; stroke: #7eb46150; } [id*=events] .er.attributeBoxOdd { fill: #fff; stroke: #7eb46150; }",
    "[id*=user] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=user] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=user] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }",
    "[id*=country] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=country] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=country] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }"
]}}%%
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
        str join_on "events.user_id = user.id"
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
%%{init: {"theme": "default", "themeCSS": [
    "[id*=events] .er.entityBox { fill: #7eb46150; stroke: #7eb46150; } [id*=events] .er.attributeBoxEven { fill: #fff; stroke: #7eb46150; } [id*=events] .er.attributeBoxOdd { fill: #fff; stroke: #7eb46150; }",
    "[id*=user] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=user] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=user] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }",
    "[id*=country] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=country] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=country] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }"
]}}%%
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

In this case, the `country` dimension was linked to both `user` (as the role `registration_country`) and to `events` (as
the role `event_country`). After this link is in place, request the `country.name` dimension for the `events` node will 
be ambiguous without choosing a role. 

DJ will distinguish between the two dimension roles with the following syntax:
* `country.name[registration_country]`
* `country.name[event_country]`

Or more generally, `<dimension>[<role>]`. The `[role]` part can be safely omitted if there is only a single role
defined for that dimension.

#### Default Values for NULL Handling

When using `LEFT` or `RIGHT` join types for dimension links, unmatched rows will produce NULL values for dimension
columns. You can configure a `default_value` on the dimension link to provide a fallback value in these cases.

When `default_value` is set, DJ will wrap dimension columns in a `COALESCE` function in the generated SQL:

```sql
-- Without default_value
SELECT user.name AS user_name
FROM events
LEFT JOIN user ON events.user_id = user.id

-- With default_value = "Unknown"
SELECT COALESCE(user.name, 'Unknown') AS user_name
FROM events
LEFT JOIN user ON events.user_id = user.id
```

This is useful when:
- You want to ensure dimension columns never return NULL in query results
- Downstream consumers expect non-null values for grouping or display
- You want to provide meaningful labels like "Unknown", "N/A", or "Other" for unmatched rows

{{< alert icon="👉" >}}
The `default_value` option is only applicable when using `LEFT` or `RIGHT` join types, as these are the join types
that can produce NULL values from unmatched rows.
{{< /alert >}}

### Reference Link

You can configure a dimension alias/reference between a particular column on a table/view-like node
(source, transform, dimension) and a column on a dimension node. An example of the alias/reference link:
{{< mermaid class="bg-light text-center" >}}
%%{init: {"theme": "default", "themeCSS": [
    "[id*=events] .er.entityBox { fill: #7eb46150; stroke: #7eb46150; } [id*=events] .er.attributeBoxEven { fill: #fff; stroke: #7eb46150; } [id*=events] .er.attributeBoxOdd { fill: #fff; stroke: #7eb46150; }",
    "[id*=country] .er.entityBox { fill: #ffefd0; stroke: #a9662150; } [id*=country] .er.attributeBoxEven { fill: #fff; stroke: #a9662150; } [id*=country] .er.attributeBoxOdd { fill: #fff; stroke: #a9662150; }"
]}}%%
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
   "events" ||--o{ "Dimension Alias": ""
   "country" ||--o{ "Dimension Alias" : ""

   "Dimension Alias" {
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
