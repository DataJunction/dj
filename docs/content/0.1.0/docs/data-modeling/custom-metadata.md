---
weight: 9
title: "Custom Metadata"
---

You can optionally set a `custom_metadata` field on any node. It is a free-form object, so you can put whatever you want in it. DJ stores it, returns it, and by default does not interpret it at all.

```yaml
name: default.repair_orders
node_type: source
custom_metadata:
  team: logistics
  cost_center: "4400"
```

## Registering a schema

You can register a JSON Schema for a key. Once a schema exists, DJ validates every write against it and rejects values that do not match. You declare schemas in the deployment manifest:

```yaml
namespace: analytics.sales

custom_metadata_schemas:
  - key: sla
    description: Freshness commitment for scheduled outputs.
    json_schema:
      type: object
      properties:
        max_staleness_hours:
          type: integer
          minimum: 1
        pager_rotation:
          type: string
      required: [max_staleness_hours]

  - key: review_status
    node_type: metric
    description: Where a metric definition sits in review.
    json_schema:
      type: string
      enum: [draft, in_review, approved]
```

With that deployed, a node in `analytics.sales` setting `custom_metadata.review_status: approved` is accepted, and one setting `Approved` is rejected with the allowed values in the error.

You can also register a schema through the API, which is useful for one-offs and for namespaces that are not managed by a repo:

```sh
curl -X POST $DJ_SERVER/metadata-schemas/ \
  -H 'Content-Type: application/json' \
  -d '{
    "key": "review_status",
    "namespace": "analytics.sales",
    "node_type": "metric",
    "json_schema": {"type": "string", "enum": ["draft", "in_review", "approved"]}
  }'
```

If a repo manages the namespace, this is refused. A deployment reconciles that namespace to exactly what its manifest declares, so anything you register here would be undone on the next push. The error tells you to declare it in the repo instead.

## What validation does and does not do

Validation is **lax about keys it does not know**. A key with no registered schema passes untouched. That means registering your first schema breaks nothing that already exists, and the feature is inert until you opt into it.

Validation is **strict about keys it does know**. A registered key is checked on every write and a failure rejects the deploy. There is no warning mode, so a schema is a hard gate from the moment it registers.

Keep that distinction in mind when you decide what to put in a schema. Constraining the *shape* of a value, meaning its type and its allowed values, is safe, because a wrong value is always a mistake. Requiring a key to be *present* is a different thing: adding `required` to a schema will fail every existing node that lacks it, on its next deploy. If you want to phase presence in, leave it out of the schema and check for it another way until the values are backfilled.

To find nodes that would fail a schema you are about to tighten, ask for its violations:

```sh
curl $DJ_SERVER/metadata-schemas/12/violations
```

That returns a count and a sample of offending nodes without changing anything.

## Scoping

You can apply a schema to everything, or narrow it to a namespace, a node type, or both:

| Registered with | Applies to |
|---|---|
| neither | every node, which makes it a global schema |
| `namespace` | that namespace and everything beneath it |
| `node_type` | nodes of that type, in any namespace |
| both | that node type, in that namespace and below |

When more than one schema exists for the same key, the most specific one wins. Namespace counts for more than node type, so a schema registered on `analytics.sales` beats one registered for all metrics.

Two more rules apply to global schemas. Registering one takes an administrator, since a schema with no namespace governs every node on the server. An administrator can also mark a global schema **reserved**, which stops any namespace from shadowing it. A reserved key always resolves to the global schema no matter what else is registered, so a platform team can guarantee that a key means one thing everywhere.

A manifest can only scope a schema to its own namespace or one beneath it. Declaring `analytics.sales.customer` from a deployment of `analytics.sales` is fine, and it lets you roll a set of values out to part of a graph before it applies to all of it. Anything outside is rejected, so one repo cannot register schemas that govern another repo's nodes.

## Reconciliation

A deployment manages the complete set of schemas for its scope, the same way it manages nodes. Declared keys are created, updated, or revived if they were previously retired, and keys in scope that the manifest no longer names are retired.

Because of that, an absent section and an empty one mean different things:

- **Omitting `custom_metadata_schemas`** leaves existing schemas alone. This is what most manifests do.
- **Declaring it as an empty list** says this manifest manages schemas and declares none, which retires them.

## Filtering

Registered or not, you can search for nodes by what is in their metadata. Dots address nested values, and a backslash escapes a dot that is part of a key name:

```graphql
{
  findNodesPaginated(
    customMetadataFilters: [
      {key: "sla.max_staleness_hours", op: LTE, value: 24}
      {key: "review_status", op: EQ, value: "approved"}
    ]
    limit: 100
  ) {
    edges { node { name } }
    totalCount
  }
}
```

Available operators are `EQ`, `NE`, `EXISTS`, `GT`, `GTE`, `LT`, `LTE`, and `CONTAINS` for arrays and objects. Multiple filters are combined with AND.

An index serves equality filters, so they stay fast on a large graph. Numeric comparisons are indexed too, but only for keys whose registered schema declares a numeric type, since DJ builds that index when you register the schema. You can still compare a key with no schema, or a non-numeric one, but the query will scan.

## Choosing between a schema and a tag

Both let you attach a controlled vocabulary to nodes, and the difference is what it costs to add a value.

A **registered schema** suits a closed set that changes rarely, where adding a value should go through review. A lifecycle state, a review status, or a service tier all fit. The set lives in one place and DJ enforces it.

A **tag** suits an open set that grows continuously, where anyone should be able to add a value without editing a schema. A domain, a project, or a team label all fit.

If you find yourself editing a schema every week to add another allowed value, it probably wanted to be a tag.
