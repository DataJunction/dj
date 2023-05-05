---
weight: 11
---

# The DataJunction API Specification

License: <a href="https://mit-license.org/">MIT License</a>

## List Catalogs

<a id="opIdlist_catalogs_catalogs__get"></a>

`GET /catalogs/`

List all available catalogs

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "engines": []
  }
]
```

<h3 id="list-catalogs-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-catalogs-responseschema">Response Schema</h3>

Status Code **200**

*Response List Catalogs Catalogs  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Catalogs Catalogs  Get|[[CatalogInfo](#schemacataloginfo)]|false|none|[Class for catalog creation]|
|» CatalogInfo|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»» name|string|true|none|none|
|»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»» name|string|true|none|none|
|»»»» version|string|true|none|none|
|»»»» uri|string|false|none|none|
|»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|

#### Enumerated Values

|Property|Value|
|---|---|
|dialect|spark|
|dialect|trino|
|dialect|druid|

<aside class="success">
This operation does not require authentication
</aside>

## Add A Catalog

<a id="opIdadd_a_catalog_catalogs__post"></a>

`POST /catalogs/`

Add a Catalog

> Body parameter

```json
{
  "name": "string",
  "engines": []
}
```

<h3 id="add-a-catalog-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CatalogInfo](#schemacataloginfo)|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "engines": []
}
```

<h3 id="add-a-catalog-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[CatalogInfo](#schemacataloginfo)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Get A Catalog

<a id="opIdget_a_catalog_catalogs__name___get"></a>

`GET /catalogs/{name}/`

Return a catalog by name

<h3 id="get-a-catalog-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "name": "string",
  "engines": []
}
```

<h3 id="get-a-catalog-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[CatalogInfo](#schemacataloginfo)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Add Engines To A Catalog

<a id="opIdadd_engines_to_a_catalog_catalogs__name__engines__post"></a>

`POST /catalogs/{name}/engines/`

Attach one or more engines to a catalog

> Body parameter

```json
[
  {
    "name": "string",
    "version": "string",
    "uri": "string",
    "dialect": "spark"
  }
]
```

<h3 id="add-engines-to-a-catalog-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "engines": []
}
```

<h3 id="add-engines-to-a-catalog-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[CatalogInfo](#schemacataloginfo)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## List Engines

<a id="opIdlist_engines_engines__get"></a>

`GET /engines/`

List all available engines

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "version": "string",
    "uri": "string",
    "dialect": "spark"
  }
]
```

<h3 id="list-engines-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-engines-responseschema">Response Schema</h3>

Status Code **200**

*Response List Engines Engines  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Engines Engines  Get|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»» name|string|true|none|none|
|»» version|string|true|none|none|
|»» uri|string|false|none|none|
|»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|

#### Enumerated Values

|Property|Value|
|---|---|
|dialect|spark|
|dialect|trino|
|dialect|druid|

<aside class="success">
This operation does not require authentication
</aside>

## Add An Engine

<a id="opIdadd_an_engine_engines__post"></a>

`POST /engines/`

Add an Engine

> Body parameter

```json
{
  "name": "string",
  "version": "string",
  "uri": "string",
  "dialect": "spark"
}
```

<h3 id="add-an-engine-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[EngineInfo](#schemaengineinfo)|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "version": "string",
  "uri": "string",
  "dialect": "spark"
}
```

<h3 id="add-an-engine-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[EngineInfo](#schemaengineinfo)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Get An Engine

<a id="opIdget_an_engine_engines__name___version___get"></a>

`GET /engines/{name}/{version}/`

Return an engine by name and version

<h3 id="get-an-engine-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|version|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "name": "string",
  "version": "string",
  "uri": "string",
  "dialect": "spark"
}
```

<h3 id="get-an-engine-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[EngineInfo](#schemaengineinfo)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## List Metrics

<a id="opIdlist_metrics_metrics__get"></a>

`GET /metrics/`

List all available metrics.

> Example responses

> 200 Response

```json
[
  "string"
]
```

<h3 id="list-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-metrics-responseschema">Response Schema</h3>

Status Code **200**

*Response List Metrics Metrics  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Metrics Metrics  Get|[string]|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Get A Metric

<a id="opIdget_a_metric_metrics__name___get"></a>

`GET /metrics/{name}/`

Return a metric by name.

<h3 id="get-a-metric-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "id": 0,
  "name": "string",
  "display_name": "string",
  "current_version": "string",
  "description": "",
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z",
  "query": "string",
  "dimensions": [
    "string"
  ]
}
```

<h3 id="get-a-metric-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[Metric](#schemametric)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Get Common Dimensions

<a id="opIdget_common_dimensions_metrics_common_dimensions__get"></a>

`GET /metrics/common/dimensions/`

Return common dimensions for a set of metrics.

<h3 id="get-common-dimensions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metric|query|array[string]|false|none|

> Example responses

> 200 Response

```json
[
  "string"
]
```

<h3 id="get-common-dimensions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="get-common-dimensions-responseschema">Response Schema</h3>

Status Code **200**

*Response Get Common Dimensions Metrics Common Dimensions  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Get Common Dimensions Metrics Common Dimensions  Get|[string]|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Build A Dj Query

<a id="opIdbuild_a_dj_query_query__sql__get"></a>

`GET /query/{sql}`

Return SQL for a DJ Query.

A database can be optionally specified. If no database is specified the optimal one
will be used.

<h3 id="build-a-dj-query-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|sql|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "sql": "string",
  "columns": [
    {
      "name": "string",
      "type": "string"
    }
  ],
  "dialect": "spark"
}
```

<h3 id="build-a-dj-query-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Validate A Node

<a id="opIdvalidate_a_node_nodes_validate__post"></a>

`POST /nodes/validate/`

Validate a node.

> Body parameter

```json
{
  "name": "string",
  "display_name": "string",
  "type": "source",
  "description": "",
  "query": "string",
  "mode": "published"
}
```

> Example responses

> 200 Response

```json
{
  "message": "string",
  "status": "valid",
  "node_revision": {
    "name": "string",
    "display_name": "string",
    "type": "source",
    "description": "",
    "query": "string",
    "mode": "published",
    "id": 0,
    "version": "v0.1",
    "node_id": 0,
    "catalog_id": 0,
    "schema_": "string",
    "table": "string",
    "status": "invalid",
    "updated_at": "2019-08-24T14:15:22Z"
  },
  "dependencies": [
    {
      "node_revision_id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "id": 0,
        "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
        "name": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z",
        "extra_params": {}
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "max_partition": [
          "string"
        ],
        "min_partition": [
          "string"
        ],
        "id": 0,
        "updated_at": "2019-08-24T14:15:22Z"
      },
      "columns": [
        {
          "name": "string",
          "type": {},
          "attributes": [
            {
              "attribute_type": {
                "namespace": "string",
                "name": "string"
              }
            }
          ],
          "dimension": {
            "name": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materialization_configs": [
        {
          "engine": {
            "name": "string",
            "version": "string",
            "uri": "string",
            "dialect": "spark"
          },
          "config": {},
          "schedule": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ]
    }
  ],
  "columns": [
    {
      "id": 0,
      "name": "string",
      "type": {},
      "dimension_id": 0,
      "dimension_column": "string"
    }
  ]
}
```

<h3 id="validate-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeValidation](#schemanodevalidation)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Set Column Attributes

<a id="opIdset_column_attributes_nodes__node_name__attributes__post"></a>

`POST /nodes/{node_name}/attributes/`

Set column attributes for the node.

> Body parameter

```json
[
  {
    "attribute_type_namespace": "system",
    "attribute_type_name": "string",
    "column_name": "string"
  }
]
```

<h3 id="set-column-attributes-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|

> Example responses

> 201 Response

```json
[
  {
    "name": "string",
    "type": {},
    "attributes": [
      {
        "attribute_type": {
          "namespace": "string",
          "name": "string"
        }
      }
    ],
    "dimension": {
      "name": "string"
    }
  }
]
```

<h3 id="set-column-attributes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="set-column-attributes-responseschema">Response Schema</h3>

Status Code **201**

*Response Set Column Attributes Nodes  Node Name  Attributes  Post*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Set Column Attributes Nodes  Node Name  Attributes  Post|[[ColumnOutput](#schemacolumnoutput)]|false|none|[A simplified column schema, without ID or dimensions.]|
|» ColumnOutput|[ColumnOutput](#schemacolumnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»» name|string|true|none|none|
|»» type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»» namespace|string|true|none|none|
|»»»»» name|string|true|none|none|
|»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»» name|string|true|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## List Nodes

<a id="opIdlist_nodes_nodes__get"></a>

`GET /nodes/`

List the available nodes.

> Example responses

> 200 Response

```json
[
  {
    "namespace": "string",
    "current": {
      "node_revision_id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "id": 0,
        "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
        "name": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z",
        "extra_params": {}
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "max_partition": [
          "string"
        ],
        "min_partition": [
          "string"
        ],
        "id": 0,
        "updated_at": "2019-08-24T14:15:22Z"
      },
      "columns": [
        {
          "name": "string",
          "type": {},
          "attributes": [
            {
              "attribute_type": {
                "namespace": "string",
                "name": "string"
              }
            }
          ],
          "dimension": {
            "name": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materialization_configs": [
        {
          "engine": {
            "name": "string",
            "version": "string",
            "uri": "string",
            "dialect": "spark"
          },
          "config": {},
          "schedule": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ]
    },
    "created_at": "2019-08-24T14:15:22Z",
    "tags": []
  }
]
```

<h3 id="list-nodes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-nodes-responseschema">Response Schema</h3>

Status Code **200**

*Response List Nodes Nodes  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Nodes Nodes  Get|[[NodeOutput](#schemanodeoutput)]|false|none|[Output for a node that shows the current revision.]|
|» NodeOutput|[NodeOutput](#schemanodeoutput)|false|none|Output for a node that shows the current revision.|
|»» namespace|string|true|none|none|
|»» current|[NodeRevisionOutput](#schemanoderevisionoutput)|true|none|Output for a node revision with information about columns and if it is a metric.|
|»»» node_revision_id|integer|true|none|none|
|»»» node_id|integer|true|none|none|
|»»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»»» name|string|true|none|none|
|»»» display_name|string|true|none|none|
|»»» version|string|true|none|none|
|»»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»»» catalog|[Catalog](#schemacatalog)|false|none|A catalog.|
|»»»» id|integer|false|none|none|
|»»»» uuid|string(uuid)|false|none|none|
|»»»» name|string|true|none|none|
|»»»» created_at|string(date-time)|false|none|none|
|»»»» updated_at|string(date-time)|false|none|none|
|»»»» extra_params|object|false|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|false|none|none|
|»»» description|string|false|none|none|
|»»» query|string|false|none|none|
|»»» availability|[AvailabilityState](#schemaavailabilitystate)|false|none|The availability of materialized data for a node|
|»»»» catalog|string|true|none|none|
|»»»» schema_|string|false|none|none|
|»»»» table|string|true|none|none|
|»»»» valid_through_ts|integer|true|none|none|
|»»»» max_partition|[string]|true|none|none|
|»»»» min_partition|[string]|true|none|none|
|»»»» id|integer|false|none|none|
|»»»» updated_at|string(date-time)|false|none|none|
|»»» columns|[[ColumnOutput](#schemacolumnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»»» ColumnOutput|[ColumnOutput](#schemacolumnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»»» name|string|true|none|none|
|»»»»» type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|»»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»»» namespace|string|true|none|none|
|»»»»»»»» name|string|true|none|none|
|»»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»»» name|string|true|none|none|
|»»» updated_at|string(date-time)|true|none|none|
|»»» materialization_configs|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»»» engine|[EngineInfo](#schemaengineinfo)|true|none|Class for engine creation|
|»»»»»» name|string|true|none|none|
|»»»»»» version|string|true|none|none|
|»»»»»» uri|string|false|none|none|
|»»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»»»»» config|object|true|none|none|
|»»»»» schedule|string|true|none|none|
|»»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» created_at|string(date-time)|true|none|none|
|»» tags|[[Tag](#schematag)]|false|none|[A tag.]|
|»»» Tag|[Tag](#schematag)|false|none|A tag.|
|»»»» description|string|true|none|none|
|»»»» tag_metadata|object|false|none|none|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» tag_type|string|true|none|none|
|»»»» id|integer|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|source|
|type|transform|
|type|metric|
|type|dimension|
|type|cube|
|status|valid|
|status|invalid|
|mode|published|
|mode|draft|
|dialect|spark|
|dialect|trino|
|dialect|druid|

<aside class="success">
This operation does not require authentication
</aside>

## Get A Node

<a id="opIdget_a_node_nodes__name___get"></a>

`GET /nodes/{name}/`

Show the active version of the specified node.

<h3 id="get-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "namespace": "string",
  "current": {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "tags": []
}
```

<h3 id="get-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Delete A Node

<a id="opIddelete_a_node_nodes__name___delete"></a>

`DELETE /nodes/{name}/`

Delete the specified node.

<h3 id="delete-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 422 Response

```json
{
  "detail": [
    {
      "loc": [
        "string"
      ],
      "msg": "string",
      "type": "string"
    }
  ]
}
```

<h3 id="delete-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Successful Response|None|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Update A Node

<a id="opIdupdate_a_node_nodes__name___patch"></a>

`PATCH /nodes/{name}/`

Update a node.

> Body parameter

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "columns": [
    {
      "name": "string",
      "type": {},
      "attributes": [
        {
          "attribute_type": {
            "namespace": "string",
            "name": "string"
          }
        }
      ],
      "dimension": "string"
    }
  ],
  "display_name": "string",
  "description": "string",
  "mode": "published",
  "primary_key": [
    "string"
  ],
  "query": "string"
}
```

<h3 id="update-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|body|body|[UpdateNode](#schemaupdatenode)|true|none|

> Example responses

> 200 Response

```json
{
  "namespace": "string",
  "current": {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "tags": []
}
```

<h3 id="update-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Upsert A Materialization Config

<a id="opIdupsert_a_materialization_config_nodes__name__materialization__post"></a>

`POST /nodes/{name}/materialization/`

Update materialization config of the specified node.

> Body parameter

```json
{
  "engine_name": "string",
  "engine_version": "string",
  "config": {},
  "schedule": "string"
}
```

<h3 id="upsert-a-materialization-config-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|body|body|[UpsertMaterializationConfig](#schemaupsertmaterializationconfig)|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="upsert-a-materialization-config-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="upsert-a-materialization-config-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## List Node Revisions

<a id="opIdlist_node_revisions_nodes__name__revisions__get"></a>

`GET /nodes/{name}/revisions/`

List all revisions for the node.

<h3 id="list-node-revisions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
[
  {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  }
]
```

<h3 id="list-node-revisions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-node-revisions-responseschema">Response Schema</h3>

Status Code **200**

*Response List Node Revisions Nodes  Name  Revisions  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Node Revisions Nodes  Name  Revisions  Get|[[NodeRevisionOutput](#schemanoderevisionoutput)]|false|none|[Output for a node revision with information about columns and if it is a metric.]|
|» NodeRevisionOutput|[NodeRevisionOutput](#schemanoderevisionoutput)|false|none|Output for a node revision with information about columns and if it is a metric.|
|»» node_revision_id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[Catalog](#schemacatalog)|false|none|A catalog.|
|»»» id|integer|false|none|none|
|»»» uuid|string(uuid)|false|none|none|
|»»» name|string|true|none|none|
|»»» created_at|string(date-time)|false|none|none|
|»»» updated_at|string(date-time)|false|none|none|
|»»» extra_params|object|false|none|none|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» query|string|false|none|none|
|»» availability|[AvailabilityState](#schemaavailabilitystate)|false|none|The availability of materialized data for a node|
|»»» catalog|string|true|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|true|none|none|
|»»» valid_through_ts|integer|true|none|none|
|»»» max_partition|[string]|true|none|none|
|»»» min_partition|[string]|true|none|none|
|»»» id|integer|false|none|none|
|»»» updated_at|string(date-time)|false|none|none|
|»» columns|[[ColumnOutput](#schemacolumnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[ColumnOutput](#schemacolumnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» materialization_configs|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»» engine|[EngineInfo](#schemaengineinfo)|true|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»»»» config|object|true|none|none|
|»»»» schedule|string|true|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|

#### Enumerated Values

|Property|Value|
|---|---|
|type|source|
|type|transform|
|type|metric|
|type|dimension|
|type|cube|
|status|valid|
|status|invalid|
|mode|published|
|mode|draft|
|dialect|spark|
|dialect|trino|
|dialect|druid|

<aside class="success">
This operation does not require authentication
</aside>

## Create A Source

<a id="opIdcreate_a_source_nodes_source__post"></a>

`POST /nodes/source/`

Create a source node. If columns are not provided, the source node's schema
will be inferred using the configured query service.

> Body parameter

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "columns": [],
  "display_name": "string",
  "description": "string",
  "mode": "published",
  "primary_key": [
    "string"
  ],
  "name": "string",
  "namespace": "default"
}
```

<h3 id="create-a-source-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateSourceNode](#schemacreatesourcenode)|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "current": {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "tags": []
}
```

<h3 id="create-a-source-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Create A Node

<a id="opIdcreate_a_node_nodes_transform__post"></a>

`POST /nodes/transform/`

Create a node.

> Body parameter

```json
{
  "query": "string",
  "display_name": "string",
  "description": "string",
  "mode": "published",
  "primary_key": [
    "string"
  ],
  "name": "string",
  "namespace": "default"
}
```

<h3 id="create-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateNode](#schemacreatenode)|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "current": {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "tags": []
}
```

<h3 id="create-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Create A Cube

<a id="opIdcreate_a_cube_nodes_cube__post"></a>

`POST /nodes/cube/`

Create a cube node.

> Body parameter

```json
{
  "display_name": "string",
  "metrics": [
    "string"
  ],
  "dimensions": [
    "string"
  ],
  "filters": [
    "string"
  ],
  "description": "string",
  "mode": "published",
  "name": "string",
  "namespace": "default"
}
```

<h3 id="create-a-cube-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateCubeNode](#schemacreatecubenode)|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "current": {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "tags": []
}
```

<h3 id="create-a-cube-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Link A Dimension

<a id="opIdlink_a_dimension_nodes__name__columns__column___post"></a>

`POST /nodes/{name}/columns/{column}/`

Add information to a node column

<h3 id="link-a-dimension-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|column|path|string|true|none|
|dimension|query|string|false|none|
|dimension_column|query|string|false|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="link-a-dimension-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="link-a-dimension-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Tag A Node

<a id="opIdtag_a_node_nodes__name__tag__post"></a>

`POST /nodes/{name}/tag/`

Add a tag to a node

<h3 id="tag-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|tag_name|query|string|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="tag-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="tag-a-node-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Calculate Node Similarity

<a id="opIdcalculate_node_similarity_nodes_similarity__node1_name___node2_name__get"></a>

`GET /nodes/similarity/{node1_name}/{node2_name}`

Compare two nodes by how similar their queries are

<h3 id="calculate-node-similarity-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node1_name|path|string|true|none|
|node2_name|path|string|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="calculate-node-similarity-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="calculate-node-similarity-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## List Downstream Nodes

<a id="opIdlist_downstream_nodes_nodes__name__downstream__get"></a>

`GET /nodes/{name}/downstream/`

List all nodes that are downstream from the given node, filterable by type.

<h3 id="list-downstream-nodes-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|node_type|query|[NodeType](#schemanodetype)|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|node_type|source|
|node_type|transform|
|node_type|metric|
|node_type|dimension|
|node_type|cube|

> Example responses

> 200 Response

```json
[
  {
    "namespace": "string",
    "current": {
      "node_revision_id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "id": 0,
        "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
        "name": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z",
        "extra_params": {}
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "max_partition": [
          "string"
        ],
        "min_partition": [
          "string"
        ],
        "id": 0,
        "updated_at": "2019-08-24T14:15:22Z"
      },
      "columns": [
        {
          "name": "string",
          "type": {},
          "attributes": [
            {
              "attribute_type": {
                "namespace": "string",
                "name": "string"
              }
            }
          ],
          "dimension": {
            "name": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materialization_configs": [
        {
          "engine": {
            "name": "string",
            "version": "string",
            "uri": "string",
            "dialect": "spark"
          },
          "config": {},
          "schedule": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ]
    },
    "created_at": "2019-08-24T14:15:22Z",
    "tags": []
  }
]
```

<h3 id="list-downstream-nodes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-downstream-nodes-responseschema">Response Schema</h3>

Status Code **200**

*Response List Downstream Nodes Nodes  Name  Downstream  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Downstream Nodes Nodes  Name  Downstream  Get|[[NodeOutput](#schemanodeoutput)]|false|none|[Output for a node that shows the current revision.]|
|» NodeOutput|[NodeOutput](#schemanodeoutput)|false|none|Output for a node that shows the current revision.|
|»» namespace|string|true|none|none|
|»» current|[NodeRevisionOutput](#schemanoderevisionoutput)|true|none|Output for a node revision with information about columns and if it is a metric.|
|»»» node_revision_id|integer|true|none|none|
|»»» node_id|integer|true|none|none|
|»»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»»» name|string|true|none|none|
|»»» display_name|string|true|none|none|
|»»» version|string|true|none|none|
|»»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»»» catalog|[Catalog](#schemacatalog)|false|none|A catalog.|
|»»»» id|integer|false|none|none|
|»»»» uuid|string(uuid)|false|none|none|
|»»»» name|string|true|none|none|
|»»»» created_at|string(date-time)|false|none|none|
|»»»» updated_at|string(date-time)|false|none|none|
|»»»» extra_params|object|false|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|false|none|none|
|»»» description|string|false|none|none|
|»»» query|string|false|none|none|
|»»» availability|[AvailabilityState](#schemaavailabilitystate)|false|none|The availability of materialized data for a node|
|»»»» catalog|string|true|none|none|
|»»»» schema_|string|false|none|none|
|»»»» table|string|true|none|none|
|»»»» valid_through_ts|integer|true|none|none|
|»»»» max_partition|[string]|true|none|none|
|»»»» min_partition|[string]|true|none|none|
|»»»» id|integer|false|none|none|
|»»»» updated_at|string(date-time)|false|none|none|
|»»» columns|[[ColumnOutput](#schemacolumnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»»» ColumnOutput|[ColumnOutput](#schemacolumnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»»» name|string|true|none|none|
|»»»»» type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|»»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»»» namespace|string|true|none|none|
|»»»»»»»» name|string|true|none|none|
|»»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»»» name|string|true|none|none|
|»»» updated_at|string(date-time)|true|none|none|
|»»» materialization_configs|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»»» engine|[EngineInfo](#schemaengineinfo)|true|none|Class for engine creation|
|»»»»»» name|string|true|none|none|
|»»»»»» version|string|true|none|none|
|»»»»»» uri|string|false|none|none|
|»»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»»»»» config|object|true|none|none|
|»»»»» schedule|string|true|none|none|
|»»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» created_at|string(date-time)|true|none|none|
|»» tags|[[Tag](#schematag)]|false|none|[A tag.]|
|»»» Tag|[Tag](#schematag)|false|none|A tag.|
|»»»» description|string|true|none|none|
|»»»» tag_metadata|object|false|none|none|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» tag_type|string|true|none|none|
|»»»» id|integer|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|source|
|type|transform|
|type|metric|
|type|dimension|
|type|cube|
|status|valid|
|status|invalid|
|mode|published|
|mode|draft|
|dialect|spark|
|dialect|trino|
|dialect|druid|

<aside class="success">
This operation does not require authentication
</aside>

## List Upstream Nodes

<a id="opIdlist_upstream_nodes_nodes__name__upstream__get"></a>

`GET /nodes/{name}/upstream/`

List all nodes that are upstream from the given node, filterable by type.

<h3 id="list-upstream-nodes-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|node_type|query|[NodeType](#schemanodetype)|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|node_type|source|
|node_type|transform|
|node_type|metric|
|node_type|dimension|
|node_type|cube|

> Example responses

> 200 Response

```json
[
  {
    "namespace": "string",
    "current": {
      "node_revision_id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "id": 0,
        "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
        "name": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z",
        "extra_params": {}
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "max_partition": [
          "string"
        ],
        "min_partition": [
          "string"
        ],
        "id": 0,
        "updated_at": "2019-08-24T14:15:22Z"
      },
      "columns": [
        {
          "name": "string",
          "type": {},
          "attributes": [
            {
              "attribute_type": {
                "namespace": "string",
                "name": "string"
              }
            }
          ],
          "dimension": {
            "name": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materialization_configs": [
        {
          "engine": {
            "name": "string",
            "version": "string",
            "uri": "string",
            "dialect": "spark"
          },
          "config": {},
          "schedule": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ]
    },
    "created_at": "2019-08-24T14:15:22Z",
    "tags": []
  }
]
```

<h3 id="list-upstream-nodes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-upstream-nodes-responseschema">Response Schema</h3>

Status Code **200**

*Response List Upstream Nodes Nodes  Name  Upstream  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Upstream Nodes Nodes  Name  Upstream  Get|[[NodeOutput](#schemanodeoutput)]|false|none|[Output for a node that shows the current revision.]|
|» NodeOutput|[NodeOutput](#schemanodeoutput)|false|none|Output for a node that shows the current revision.|
|»» namespace|string|true|none|none|
|»» current|[NodeRevisionOutput](#schemanoderevisionoutput)|true|none|Output for a node revision with information about columns and if it is a metric.|
|»»» node_revision_id|integer|true|none|none|
|»»» node_id|integer|true|none|none|
|»»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»»» name|string|true|none|none|
|»»» display_name|string|true|none|none|
|»»» version|string|true|none|none|
|»»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»»» catalog|[Catalog](#schemacatalog)|false|none|A catalog.|
|»»»» id|integer|false|none|none|
|»»»» uuid|string(uuid)|false|none|none|
|»»»» name|string|true|none|none|
|»»»» created_at|string(date-time)|false|none|none|
|»»»» updated_at|string(date-time)|false|none|none|
|»»»» extra_params|object|false|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|false|none|none|
|»»» description|string|false|none|none|
|»»» query|string|false|none|none|
|»»» availability|[AvailabilityState](#schemaavailabilitystate)|false|none|The availability of materialized data for a node|
|»»»» catalog|string|true|none|none|
|»»»» schema_|string|false|none|none|
|»»»» table|string|true|none|none|
|»»»» valid_through_ts|integer|true|none|none|
|»»»» max_partition|[string]|true|none|none|
|»»»» min_partition|[string]|true|none|none|
|»»»» id|integer|false|none|none|
|»»»» updated_at|string(date-time)|false|none|none|
|»»» columns|[[ColumnOutput](#schemacolumnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»»» ColumnOutput|[ColumnOutput](#schemacolumnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»»» name|string|true|none|none|
|»»»»» type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|»»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»»» namespace|string|true|none|none|
|»»»»»»»» name|string|true|none|none|
|»»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»»» name|string|true|none|none|
|»»» updated_at|string(date-time)|true|none|none|
|»»» materialization_configs|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»»» engine|[EngineInfo](#schemaengineinfo)|true|none|Class for engine creation|
|»»»»»» name|string|true|none|none|
|»»»»»» version|string|true|none|none|
|»»»»»» uri|string|false|none|none|
|»»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»»»»» config|object|true|none|none|
|»»»»» schedule|string|true|none|none|
|»»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» created_at|string(date-time)|true|none|none|
|»» tags|[[Tag](#schematag)]|false|none|[A tag.]|
|»»» Tag|[Tag](#schematag)|false|none|A tag.|
|»»»» description|string|true|none|none|
|»»»» tag_metadata|object|false|none|none|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» tag_type|string|true|none|none|
|»»»» id|integer|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|source|
|type|transform|
|type|metric|
|type|dimension|
|type|cube|
|status|valid|
|status|invalid|
|mode|published|
|mode|draft|
|dialect|spark|
|dialect|trino|
|dialect|druid|

<aside class="success">
This operation does not require authentication
</aside>

## List Nodes In Namespace

<a id="opIdlist_nodes_in_namespace_namespaces__namespace___get"></a>

`GET /namespaces/{namespace}/`

List node names in namespace, filterable to a given type if desired.

<h3 id="list-nodes-in-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|type_|query|[NodeType](#schemanodetype)|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|type_|source|
|type_|transform|
|type_|metric|
|type_|dimension|
|type_|cube|

> Example responses

> 200 Response

```json
[
  "string"
]
```

<h3 id="list-nodes-in-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeNameList](#schemanodenamelist)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Create A Node Namespace

<a id="opIdcreate_a_node_namespace_namespaces__namespace___post"></a>

`POST /namespaces/{namespace}/`

Create a node namespace

<h3 id="create-a-node-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="create-a-node-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="create-a-node-namespace-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## List Node Namespaces

<a id="opIdlist_node_namespaces_namespaces__get"></a>

`GET /namespaces/`

List node namespaces

> Example responses

> 200 Response

```json
[
  {
    "namespace": "string"
  }
]
```

<h3 id="list-node-namespaces-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-node-namespaces-responseschema">Response Schema</h3>

Status Code **200**

*Response List Node Namespaces Namespaces  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Node Namespaces Namespaces  Get|[[NodeNamespace](#schemanodenamespace)]|false|none|[A node namespace]|
|» NodeNamespace|[NodeNamespace](#schemanodenamespace)|false|none|A node namespace|
|»» namespace|string|true|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Add An Availability State

<a id="opIdadd_an_availability_state_data__node_name__availability__post"></a>

`POST /data/{node_name}/availability/`

Add an availability state to a node

> Body parameter

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "valid_through_ts": 0,
  "max_partition": [
    "string"
  ],
  "min_partition": [
    "string"
  ]
}
```

<h3 id="add-an-availability-state-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|body|body|[AvailabilityStateBase](#schemaavailabilitystatebase)|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="add-an-availability-state-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="add-an-availability-state-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Get Data

<a id="opIdget_data_data__node_name___get"></a>

`GET /data/{node_name}/`

Gets data for a node

<h3 id="get-data-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|async_|query|boolean|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="get-data-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="get-data-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Get Data For Metrics

<a id="opIdget_data_for_metrics_data__get"></a>

`GET /data/`

Return data for a set of metrics with dimensions and filters

<h3 id="get-data-for-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metrics|query|array[string]|false|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|async_|query|boolean|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "engine_name": "string",
  "engine_version": "string",
  "submitted_query": "string",
  "executed_query": "string",
  "scheduled": "2019-08-24T14:15:22Z",
  "started": "2019-08-24T14:15:22Z",
  "finished": "2019-08-24T14:15:22Z",
  "state": "UNKNOWN",
  "progress": 0,
  "output_table": {
    "catalog": "string",
    "schema": "string",
    "table": "string"
  },
  "results": [
    {
      "sql": "string",
      "columns": [
        {
          "name": "string",
          "type": "string"
        }
      ],
      "rows": [
        [
          null
        ]
      ],
      "row_count": 0
    }
  ],
  "next": "http://example.com",
  "previous": "http://example.com",
  "errors": [
    "string"
  ]
}
```

<h3 id="get-data-for-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Health Check

<a id="opIdhealth_check_health__get"></a>

`GET /health/`

Healthcheck for services.

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "status": "ok"
  }
]
```

<h3 id="health-check-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="health-check-responseschema">Response Schema</h3>

Status Code **200**

*Response Health Check Health  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Health Check Health  Get|[[HealthCheck](#schemahealthcheck)]|false|none|[A healthcheck response.]|
|» HealthCheck|[HealthCheck](#schemahealthcheck)|false|none|A healthcheck response.|
|»» name|string|true|none|none|
|»» status|[HealthcheckStatus](#schemahealthcheckstatus)|true|none|Possible health statuses.|

#### Enumerated Values

|Property|Value|
|---|---|
|status|ok|
|status|failed|

<aside class="success">
This operation does not require authentication
</aside>

## Get A Cube

<a id="opIdget_a_cube_cubes__name___get"></a>

`GET /cubes/{name}/`

Get information on a cube

<h3 id="get-a-cube-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "description": "",
  "availability": {
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "max_partition": [
      "string"
    ],
    "min_partition": [
      "string"
    ],
    "id": 0,
    "updated_at": "2019-08-24T14:15:22Z"
  },
  "cube_elements": [
    {
      "name": "string",
      "node_name": "string",
      "type": "string"
    }
  ],
  "query": "string",
  "columns": [
    {
      "name": "string",
      "type": {},
      "attributes": [
        {
          "attribute_type": {
            "namespace": "string",
            "name": "string"
          }
        }
      ],
      "dimension": {
        "name": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materialization_configs": [
    {
      "node_revision_id": 0,
      "engine_id": 0,
      "schedule": "string",
      "config": {}
    }
  ]
}
```

<h3 id="get-a-cube-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[CubeRevisionMetadata](#schemacuberevisionmetadata)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## List Tags

<a id="opIdlist_tags_tags__get"></a>

`GET /tags/`

List all available tags.

<h3 id="list-tags-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|tag_type|query|string|false|none|

> Example responses

> 200 Response

```json
[
  {
    "description": "string",
    "tag_metadata": {},
    "name": "string",
    "display_name": "string",
    "tag_type": "string"
  }
]
```

<h3 id="list-tags-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-tags-responseschema">Response Schema</h3>

Status Code **200**

*Response List Tags Tags  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Tags Tags  Get|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|» TagOutput|[TagOutput](#schematagoutput)|false|none|Output tag model.|
|»» description|string|true|none|none|
|»» tag_metadata|object|false|none|none|
|»» name|string|true|none|none|
|»» display_name|string|false|none|none|
|»» tag_type|string|true|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Create A Tag

<a id="opIdcreate_a_tag_tags__post"></a>

`POST /tags/`

Create a tag.

> Body parameter

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string"
}
```

<h3 id="create-a-tag-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateTag](#schemacreatetag)|true|none|

> Example responses

> 201 Response

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string",
  "id": 0
}
```

<h3 id="create-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[Tag](#schematag)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Get A Tag

<a id="opIdget_a_tag_tags__name___get"></a>

`GET /tags/{name}/`

Return a tag by name.

<h3 id="get-a-tag-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string",
  "id": 0
}
```

<h3 id="get-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[Tag](#schematag)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Update A Tag

<a id="opIdupdate_a_tag_tags__name___patch"></a>

`PATCH /tags/{name}/`

Update a tag.

> Body parameter

```json
{
  "description": "string",
  "tag_metadata": {}
}
```

<h3 id="update-a-tag-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|body|body|[UpdateTag](#schemaupdatetag)|true|none|

> Example responses

> 200 Response

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string",
  "id": 0
}
```

<h3 id="update-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[Tag](#schematag)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## List Nodes For A Tag

<a id="opIdlist_nodes_for_a_tag_tags__name__nodes__get"></a>

`GET /tags/{name}/nodes/`

Find nodes tagged with the tag, filterable by node type.

<h3 id="list-nodes-for-a-tag-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|node_type|query|[NodeType](#schemanodetype)|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|node_type|source|
|node_type|transform|
|node_type|metric|
|node_type|dimension|
|node_type|cube|

> Example responses

> 200 Response

```json
[
  "string"
]
```

<h3 id="list-nodes-for-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-nodes-for-a-tag-responseschema">Response Schema</h3>

Status Code **200**

*Response List Nodes For A Tag Tags  Name  Nodes  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Nodes For A Tag Tags  Name  Nodes  Get|[string]|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## List Attributes

<a id="opIdlist_attributes_attributes__get"></a>

`GET /attributes/`

List all available attribute types.

> Example responses

> 200 Response

```json
[
  {
    "uniqueness_scope": [],
    "namespace": "string",
    "name": "string",
    "description": "string",
    "allowed_node_types": [
      "source"
    ],
    "id": 0
  }
]
```

<h3 id="list-attributes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-attributes-responseschema">Response Schema</h3>

Status Code **200**

*Response List Attributes Attributes  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Attributes Attributes  Get|[[AttributeType](#schemaattributetype)]|false|none|[Available attribute types for column metadata.]|
|» AttributeType|[AttributeType](#schemaattributetype)|false|none|Available attribute types for column metadata.|
|»» uniqueness_scope|[[UniquenessScope](#schemauniquenessscope)]|false|none|[The scope at which this attribute needs to be unique.]|
|»»» UniquenessScope|[UniquenessScope](#schemauniquenessscope)|false|none|The scope at which this attribute needs to be unique.|
|»» namespace|string|true|none|none|
|»» name|string|true|none|none|
|»» description|string|true|none|none|
|»» allowed_node_types|[[NodeType](#schemanodetype)]|true|none|[Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.]|
|»»» NodeType|[NodeType](#schemanodetype)|false|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» id|integer|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|UniquenessScope|node|
|UniquenessScope|column_type|
|NodeType|source|
|NodeType|transform|
|NodeType|metric|
|NodeType|dimension|
|NodeType|cube|

<aside class="success">
This operation does not require authentication
</aside>

## Add An Attribute Type

<a id="opIdadd_an_attribute_type_attributes__post"></a>

`POST /attributes/`

Add a new attribute type

> Body parameter

```json
{
  "namespace": "string",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ]
}
```

<h3 id="add-an-attribute-type-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[MutableAttributeTypeFields](#schemamutableattributetypefields)|true|none|

> Example responses

> 201 Response

```json
{
  "uniqueness_scope": [],
  "namespace": "string",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ],
  "id": 0
}
```

<h3 id="add-an-attribute-type-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[AttributeType](#schemaattributetype)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Get Sql

<a id="opIdget_sql_sql__node_name___get"></a>

`GET /sql/{node_name}/`

Return SQL for a node.

<h3 id="get-sql-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "sql": "string",
  "columns": [
    {
      "name": "string",
      "type": "string"
    }
  ],
  "dialect": "spark"
}
```

<h3 id="get-sql-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

## Get Sql For Metrics

<a id="opIdget_sql_for_metrics_sql__get"></a>

`GET /sql/`

Return SQL for a set of metrics with dimensions and filters

<h3 id="get-sql-for-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metrics|query|array[string]|false|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "sql": "string",
  "columns": [
    {
      "name": "string",
      "type": "string"
    }
  ],
  "dialect": "spark"
}
```

<h3 id="get-sql-for-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_AttributeOutput">AttributeOutput</h2>
<!-- backwards compatibility -->
<a id="schemaattributeoutput"></a>
<a id="schema_AttributeOutput"></a>
<a id="tocSattributeoutput"></a>
<a id="tocsattributeoutput"></a>

```json
{
  "attribute_type": {
    "namespace": "string",
    "name": "string"
  }
}

```

AttributeOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|

<h2 id="tocS_AttributeType">AttributeType</h2>
<!-- backwards compatibility -->
<a id="schemaattributetype"></a>
<a id="schema_AttributeType"></a>
<a id="tocSattributetype"></a>
<a id="tocsattributetype"></a>

```json
{
  "uniqueness_scope": [],
  "namespace": "string",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ],
  "id": 0
}

```

AttributeType

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|uniqueness_scope|[[UniquenessScope](#schemauniquenessscope)]|false|none|[The scope at which this attribute needs to be unique.]|
|namespace|string|true|none|none|
|name|string|true|none|none|
|description|string|true|none|none|
|allowed_node_types|[[NodeType](#schemanodetype)]|true|none|[Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.]|
|id|integer|false|none|none|

<h2 id="tocS_AttributeTypeName">AttributeTypeName</h2>
<!-- backwards compatibility -->
<a id="schemaattributetypename"></a>
<a id="schema_AttributeTypeName"></a>
<a id="tocSattributetypename"></a>
<a id="tocsattributetypename"></a>

```json
{
  "namespace": "string",
  "name": "string"
}

```

AttributeTypeName

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|
|name|string|true|none|none|

<h2 id="tocS_AvailabilityState">AvailabilityState</h2>
<!-- backwards compatibility -->
<a id="schemaavailabilitystate"></a>
<a id="schema_AvailabilityState"></a>
<a id="tocSavailabilitystate"></a>
<a id="tocsavailabilitystate"></a>

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "valid_through_ts": 0,
  "max_partition": [
    "string"
  ],
  "min_partition": [
    "string"
  ],
  "id": 0,
  "updated_at": "2019-08-24T14:15:22Z"
}

```

AvailabilityState

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|catalog|string|true|none|none|
|schema_|string|false|none|none|
|table|string|true|none|none|
|valid_through_ts|integer|true|none|none|
|max_partition|[string]|true|none|none|
|min_partition|[string]|true|none|none|
|id|integer|false|none|none|
|updated_at|string(date-time)|false|none|none|

<h2 id="tocS_AvailabilityStateBase">AvailabilityStateBase</h2>
<!-- backwards compatibility -->
<a id="schemaavailabilitystatebase"></a>
<a id="schema_AvailabilityStateBase"></a>
<a id="tocSavailabilitystatebase"></a>
<a id="tocsavailabilitystatebase"></a>

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "valid_through_ts": 0,
  "max_partition": [
    "string"
  ],
  "min_partition": [
    "string"
  ]
}

```

AvailabilityStateBase

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|catalog|string|true|none|none|
|schema_|string|false|none|none|
|table|string|true|none|none|
|valid_through_ts|integer|true|none|none|
|max_partition|[string]|true|none|none|
|min_partition|[string]|true|none|none|

<h2 id="tocS_Catalog">Catalog</h2>
<!-- backwards compatibility -->
<a id="schemacatalog"></a>
<a id="schema_Catalog"></a>
<a id="tocScatalog"></a>
<a id="tocscatalog"></a>

```json
{
  "id": 0,
  "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
  "name": "string",
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z",
  "extra_params": {}
}

```

Catalog

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|false|none|none|
|uuid|string(uuid)|false|none|none|
|name|string|true|none|none|
|created_at|string(date-time)|false|none|none|
|updated_at|string(date-time)|false|none|none|
|extra_params|object|false|none|none|

<h2 id="tocS_CatalogInfo">CatalogInfo</h2>
<!-- backwards compatibility -->
<a id="schemacataloginfo"></a>
<a id="schema_CatalogInfo"></a>
<a id="tocScataloginfo"></a>
<a id="tocscataloginfo"></a>

```json
{
  "name": "string",
  "engines": []
}

```

CatalogInfo

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|

<h2 id="tocS_Column">Column</h2>
<!-- backwards compatibility -->
<a id="schemacolumn"></a>
<a id="schema_Column"></a>
<a id="tocScolumn"></a>
<a id="tocscolumn"></a>

```json
{
  "id": 0,
  "name": "string",
  "type": {},
  "dimension_id": 0,
  "dimension_column": "string"
}

```

Column

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|false|none|none|
|name|string|true|none|none|
|type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|dimension_id|integer|false|none|none|
|dimension_column|string|false|none|none|

<h2 id="tocS_ColumnAttributeInput">ColumnAttributeInput</h2>
<!-- backwards compatibility -->
<a id="schemacolumnattributeinput"></a>
<a id="schema_ColumnAttributeInput"></a>
<a id="tocScolumnattributeinput"></a>
<a id="tocscolumnattributeinput"></a>

```json
{
  "attribute_type_namespace": "system",
  "attribute_type_name": "string",
  "column_name": "string"
}

```

ColumnAttributeInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|attribute_type_namespace|string|false|none|none|
|attribute_type_name|string|true|none|none|
|column_name|string|true|none|none|

<h2 id="tocS_ColumnMetadata">ColumnMetadata</h2>
<!-- backwards compatibility -->
<a id="schemacolumnmetadata"></a>
<a id="schema_ColumnMetadata"></a>
<a id="tocScolumnmetadata"></a>
<a id="tocscolumnmetadata"></a>

```json
{
  "name": "string",
  "type": "string"
}

```

ColumnMetadata

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|type|string|true|none|none|

<h2 id="tocS_ColumnOutput">ColumnOutput</h2>
<!-- backwards compatibility -->
<a id="schemacolumnoutput"></a>
<a id="schema_ColumnOutput"></a>
<a id="tocScolumnoutput"></a>
<a id="tocscolumnoutput"></a>

```json
{
  "name": "string",
  "type": {},
  "attributes": [
    {
      "attribute_type": {
        "namespace": "string",
        "name": "string"
      }
    }
  ],
  "dimension": {
    "name": "string"
  }
}

```

ColumnOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|

<h2 id="tocS_ColumnType">ColumnType</h2>
<!-- backwards compatibility -->
<a id="schemacolumntype"></a>
<a id="schema_ColumnType"></a>
<a id="tocScolumntype"></a>
<a id="tocscolumntype"></a>

```json
{}

```

ColumnType

### Properties

*None*

<h2 id="tocS_CreateCubeNode">CreateCubeNode</h2>
<!-- backwards compatibility -->
<a id="schemacreatecubenode"></a>
<a id="schema_CreateCubeNode"></a>
<a id="tocScreatecubenode"></a>
<a id="tocscreatecubenode"></a>

```json
{
  "display_name": "string",
  "metrics": [
    "string"
  ],
  "dimensions": [
    "string"
  ],
  "filters": [
    "string"
  ],
  "description": "string",
  "mode": "published",
  "name": "string",
  "namespace": "default"
}

```

CreateCubeNode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|display_name|string|false|none|none|
|metrics|[string]|true|none|none|
|dimensions|[string]|true|none|none|
|filters|[string]|false|none|none|
|description|string|true|none|none|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|name|string|true|none|none|
|namespace|string|false|none|none|

<h2 id="tocS_CreateNode">CreateNode</h2>
<!-- backwards compatibility -->
<a id="schemacreatenode"></a>
<a id="schema_CreateNode"></a>
<a id="tocScreatenode"></a>
<a id="tocscreatenode"></a>

```json
{
  "query": "string",
  "display_name": "string",
  "description": "string",
  "mode": "published",
  "primary_key": [
    "string"
  ],
  "name": "string",
  "namespace": "default"
}

```

CreateNode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|query|string|true|none|none|
|display_name|string|false|none|none|
|description|string|true|none|none|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|primary_key|[string]|false|none|none|
|name|string|true|none|none|
|namespace|string|false|none|none|

<h2 id="tocS_CreateSourceNode">CreateSourceNode</h2>
<!-- backwards compatibility -->
<a id="schemacreatesourcenode"></a>
<a id="schema_CreateSourceNode"></a>
<a id="tocScreatesourcenode"></a>
<a id="tocscreatesourcenode"></a>

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "columns": [],
  "display_name": "string",
  "description": "string",
  "mode": "published",
  "primary_key": [
    "string"
  ],
  "name": "string",
  "namespace": "default"
}

```

CreateSourceNode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|catalog|string|true|none|none|
|schema_|string|true|none|none|
|table|string|true|none|none|
|columns|[[SourceColumnOutput](#schemasourcecolumnoutput)]|false|none|[A column used in creation of a source node]|
|display_name|string|false|none|none|
|description|string|true|none|none|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|primary_key|[string]|false|none|none|
|name|string|true|none|none|
|namespace|string|false|none|none|

<h2 id="tocS_CreateTag">CreateTag</h2>
<!-- backwards compatibility -->
<a id="schemacreatetag"></a>
<a id="schema_CreateTag"></a>
<a id="tocScreatetag"></a>
<a id="tocscreatetag"></a>

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string"
}

```

CreateTag

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|true|none|none|
|tag_metadata|object|false|none|none|
|name|string|true|none|none|
|display_name|string|false|none|none|
|tag_type|string|true|none|none|

<h2 id="tocS_CubeElementMetadata">CubeElementMetadata</h2>
<!-- backwards compatibility -->
<a id="schemacubeelementmetadata"></a>
<a id="schema_CubeElementMetadata"></a>
<a id="tocScubeelementmetadata"></a>
<a id="tocscubeelementmetadata"></a>

```json
{
  "name": "string",
  "node_name": "string",
  "type": "string"
}

```

CubeElementMetadata

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|node_name|string|true|none|none|
|type|string|true|none|none|

<h2 id="tocS_CubeRevisionMetadata">CubeRevisionMetadata</h2>
<!-- backwards compatibility -->
<a id="schemacuberevisionmetadata"></a>
<a id="schema_CubeRevisionMetadata"></a>
<a id="tocScuberevisionmetadata"></a>
<a id="tocscuberevisionmetadata"></a>

```json
{
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "description": "",
  "availability": {
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "max_partition": [
      "string"
    ],
    "min_partition": [
      "string"
    ],
    "id": 0,
    "updated_at": "2019-08-24T14:15:22Z"
  },
  "cube_elements": [
    {
      "name": "string",
      "node_name": "string",
      "type": "string"
    }
  ],
  "query": "string",
  "columns": [
    {
      "name": "string",
      "type": {},
      "attributes": [
        {
          "attribute_type": {
            "namespace": "string",
            "name": "string"
          }
        }
      ],
      "dimension": {
        "name": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materialization_configs": [
    {
      "node_revision_id": 0,
      "engine_id": 0,
      "schedule": "string",
      "config": {}
    }
  ]
}

```

CubeRevisionMetadata

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|node_revision_id|integer|true|none|none|
|node_id|integer|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|name|string|true|none|none|
|display_name|string|true|none|none|
|version|string|true|none|none|
|description|string|false|none|none|
|availability|[AvailabilityState](#schemaavailabilitystate)|false|none|The availability of materialized data for a node|
|cube_elements|[[CubeElementMetadata](#schemacubeelementmetadata)]|true|none|[Metadata for an element in a cube]|
|query|string|true|none|none|
|columns|[[ColumnOutput](#schemacolumnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|updated_at|string(date-time)|true|none|none|
|materialization_configs|[[MaterializationConfig](#schemamaterializationconfig)]|true|none|[Materialization configuration for a node and specific engines.]|

<h2 id="tocS_Dialect">Dialect</h2>
<!-- backwards compatibility -->
<a id="schemadialect"></a>
<a id="schema_Dialect"></a>
<a id="tocSdialect"></a>
<a id="tocsdialect"></a>

```json
"spark"

```

Dialect

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Dialect|string|false|none|SQL dialect|

#### Enumerated Values

|Property|Value|
|---|---|
|Dialect|spark|
|Dialect|trino|
|Dialect|druid|

<h2 id="tocS_EngineInfo">EngineInfo</h2>
<!-- backwards compatibility -->
<a id="schemaengineinfo"></a>
<a id="schema_EngineInfo"></a>
<a id="tocSengineinfo"></a>
<a id="tocsengineinfo"></a>

```json
{
  "name": "string",
  "version": "string",
  "uri": "string",
  "dialect": "spark"
}

```

EngineInfo

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|version|string|true|none|none|
|uri|string|false|none|none|
|dialect|[Dialect](#schemadialect)|false|none|SQL dialect|

<h2 id="tocS_HTTPValidationError">HTTPValidationError</h2>
<!-- backwards compatibility -->
<a id="schemahttpvalidationerror"></a>
<a id="schema_HTTPValidationError"></a>
<a id="tocShttpvalidationerror"></a>
<a id="tocshttpvalidationerror"></a>

```json
{
  "detail": [
    {
      "loc": [
        "string"
      ],
      "msg": "string",
      "type": "string"
    }
  ]
}

```

HTTPValidationError

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|detail|[[ValidationError](#schemavalidationerror)]|false|none|none|

<h2 id="tocS_HealthCheck">HealthCheck</h2>
<!-- backwards compatibility -->
<a id="schemahealthcheck"></a>
<a id="schema_HealthCheck"></a>
<a id="tocShealthcheck"></a>
<a id="tocshealthcheck"></a>

```json
{
  "name": "string",
  "status": "ok"
}

```

HealthCheck

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|status|[HealthcheckStatus](#schemahealthcheckstatus)|true|none|Possible health statuses.|

<h2 id="tocS_HealthcheckStatus">HealthcheckStatus</h2>
<!-- backwards compatibility -->
<a id="schemahealthcheckstatus"></a>
<a id="schema_HealthcheckStatus"></a>
<a id="tocShealthcheckstatus"></a>
<a id="tocshealthcheckstatus"></a>

```json
"ok"

```

HealthcheckStatus

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|HealthcheckStatus|string|false|none|Possible health statuses.|

#### Enumerated Values

|Property|Value|
|---|---|
|HealthcheckStatus|ok|
|HealthcheckStatus|failed|

<h2 id="tocS_MaterializationConfig">MaterializationConfig</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationconfig"></a>
<a id="schema_MaterializationConfig"></a>
<a id="tocSmaterializationconfig"></a>
<a id="tocsmaterializationconfig"></a>

```json
{
  "node_revision_id": 0,
  "engine_id": 0,
  "schedule": "string",
  "config": {}
}

```

MaterializationConfig

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|node_revision_id|integer|true|none|none|
|engine_id|integer|true|none|none|
|schedule|string|true|none|none|
|config|object|false|none|none|

<h2 id="tocS_MaterializationConfigOutput">MaterializationConfigOutput</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationconfigoutput"></a>
<a id="schema_MaterializationConfigOutput"></a>
<a id="tocSmaterializationconfigoutput"></a>
<a id="tocsmaterializationconfigoutput"></a>

```json
{
  "engine": {
    "name": "string",
    "version": "string",
    "uri": "string",
    "dialect": "spark"
  },
  "config": {},
  "schedule": "string"
}

```

MaterializationConfigOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|engine|[EngineInfo](#schemaengineinfo)|true|none|Class for engine creation|
|config|object|true|none|none|
|schedule|string|true|none|none|

<h2 id="tocS_Metric">Metric</h2>
<!-- backwards compatibility -->
<a id="schemametric"></a>
<a id="schema_Metric"></a>
<a id="tocSmetric"></a>
<a id="tocsmetric"></a>

```json
{
  "id": 0,
  "name": "string",
  "display_name": "string",
  "current_version": "string",
  "description": "",
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z",
  "query": "string",
  "dimensions": [
    "string"
  ]
}

```

Metric

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|true|none|none|
|name|string|true|none|none|
|display_name|string|true|none|none|
|current_version|string|true|none|none|
|description|string|false|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|
|query|string|true|none|none|
|dimensions|[string]|true|none|none|

<h2 id="tocS_MutableAttributeTypeFields">MutableAttributeTypeFields</h2>
<!-- backwards compatibility -->
<a id="schemamutableattributetypefields"></a>
<a id="schema_MutableAttributeTypeFields"></a>
<a id="tocSmutableattributetypefields"></a>
<a id="tocsmutableattributetypefields"></a>

```json
{
  "namespace": "string",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ]
}

```

MutableAttributeTypeFields

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|
|name|string|true|none|none|
|description|string|true|none|none|
|allowed_node_types|[[NodeType](#schemanodetype)]|true|none|[Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.]|

<h2 id="tocS_NodeMode">NodeMode</h2>
<!-- backwards compatibility -->
<a id="schemanodemode"></a>
<a id="schema_NodeMode"></a>
<a id="tocSnodemode"></a>
<a id="tocsnodemode"></a>

```json
"published"

```

NodeMode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|NodeMode|string|false|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|

#### Enumerated Values

|Property|Value|
|---|---|
|NodeMode|published|
|NodeMode|draft|

<h2 id="tocS_NodeNameList">NodeNameList</h2>
<!-- backwards compatibility -->
<a id="schemanodenamelist"></a>
<a id="schema_NodeNameList"></a>
<a id="tocSnodenamelist"></a>
<a id="tocsnodenamelist"></a>

```json
[
  "string"
]

```

NodeNameList

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|NodeNameList|[string]|false|none|List of node names|

<h2 id="tocS_NodeNameOutput">NodeNameOutput</h2>
<!-- backwards compatibility -->
<a id="schemanodenameoutput"></a>
<a id="schema_NodeNameOutput"></a>
<a id="tocSnodenameoutput"></a>
<a id="tocsnodenameoutput"></a>

```json
{
  "name": "string"
}

```

NodeNameOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|

<h2 id="tocS_NodeNamespace">NodeNamespace</h2>
<!-- backwards compatibility -->
<a id="schemanodenamespace"></a>
<a id="schema_NodeNamespace"></a>
<a id="tocSnodenamespace"></a>
<a id="tocsnodenamespace"></a>

```json
{
  "namespace": "string"
}

```

NodeNamespace

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|

<h2 id="tocS_NodeOutput">NodeOutput</h2>
<!-- backwards compatibility -->
<a id="schemanodeoutput"></a>
<a id="schema_NodeOutput"></a>
<a id="tocSnodeoutput"></a>
<a id="tocsnodeoutput"></a>

```json
{
  "namespace": "string",
  "current": {
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "id": 0,
      "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
      "name": "string",
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z",
      "extra_params": {}
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "max_partition": [
        "string"
      ],
      "min_partition": [
        "string"
      ],
      "id": 0,
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "columns": [
      {
        "name": "string",
        "type": {},
        "attributes": [
          {
            "attribute_type": {
              "namespace": "string",
              "name": "string"
            }
          }
        ],
        "dimension": {
          "name": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materialization_configs": [
      {
        "engine": {
          "name": "string",
          "version": "string",
          "uri": "string",
          "dialect": "spark"
        },
        "config": {},
        "schedule": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "tags": []
}

```

NodeOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|
|current|[NodeRevisionOutput](#schemanoderevisionoutput)|true|none|Output for a node revision with information about columns and if it is a metric.|
|created_at|string(date-time)|true|none|none|
|tags|[[Tag](#schematag)]|false|none|[A tag.]|

<h2 id="tocS_NodeRevision">NodeRevision</h2>
<!-- backwards compatibility -->
<a id="schemanoderevision"></a>
<a id="schema_NodeRevision"></a>
<a id="tocSnoderevision"></a>
<a id="tocsnoderevision"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "type": "source",
  "description": "",
  "query": "string",
  "mode": "published",
  "id": 0,
  "version": "v0.1",
  "node_id": 0,
  "catalog_id": 0,
  "schema_": "string",
  "table": "string",
  "status": "invalid",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

NodeRevision

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|false|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|description|string|false|none|none|
|query|string|false|none|none|
|mode|[NodeMode](#schemanodemode)|false|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|id|integer|false|none|none|
|version|string|false|none|none|
|node_id|integer|false|none|none|
|catalog_id|integer|false|none|none|
|schema_|string|false|none|none|
|table|string|false|none|none|
|status|[NodeStatus](#schemanodestatus)|false|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|updated_at|string(date-time)|false|none|none|

<h2 id="tocS_NodeRevisionBase">NodeRevisionBase</h2>
<!-- backwards compatibility -->
<a id="schemanoderevisionbase"></a>
<a id="schema_NodeRevisionBase"></a>
<a id="tocSnoderevisionbase"></a>
<a id="tocsnoderevisionbase"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "type": "source",
  "description": "",
  "query": "string",
  "mode": "published"
}

```

NodeRevisionBase

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|false|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|description|string|false|none|none|
|query|string|false|none|none|
|mode|[NodeMode](#schemanodemode)|false|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|

<h2 id="tocS_NodeRevisionOutput">NodeRevisionOutput</h2>
<!-- backwards compatibility -->
<a id="schemanoderevisionoutput"></a>
<a id="schema_NodeRevisionOutput"></a>
<a id="tocSnoderevisionoutput"></a>
<a id="tocsnoderevisionoutput"></a>

```json
{
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "id": 0,
    "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
    "name": "string",
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z",
    "extra_params": {}
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "max_partition": [
      "string"
    ],
    "min_partition": [
      "string"
    ],
    "id": 0,
    "updated_at": "2019-08-24T14:15:22Z"
  },
  "columns": [
    {
      "name": "string",
      "type": {},
      "attributes": [
        {
          "attribute_type": {
            "namespace": "string",
            "name": "string"
          }
        }
      ],
      "dimension": {
        "name": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materialization_configs": [
    {
      "engine": {
        "name": "string",
        "version": "string",
        "uri": "string",
        "dialect": "spark"
      },
      "config": {},
      "schedule": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ]
}

```

NodeRevisionOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|node_revision_id|integer|true|none|none|
|node_id|integer|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|name|string|true|none|none|
|display_name|string|true|none|none|
|version|string|true|none|none|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|catalog|[Catalog](#schemacatalog)|false|none|A catalog.|
|schema_|string|false|none|none|
|table|string|false|none|none|
|description|string|false|none|none|
|query|string|false|none|none|
|availability|[AvailabilityState](#schemaavailabilitystate)|false|none|The availability of materialized data for a node|
|columns|[[ColumnOutput](#schemacolumnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|updated_at|string(date-time)|true|none|none|
|materialization_configs|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|

<h2 id="tocS_NodeStatus">NodeStatus</h2>
<!-- backwards compatibility -->
<a id="schemanodestatus"></a>
<a id="schema_NodeStatus"></a>
<a id="tocSnodestatus"></a>
<a id="tocsnodestatus"></a>

```json
"valid"

```

NodeStatus

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|NodeStatus|string|false|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|

#### Enumerated Values

|Property|Value|
|---|---|
|NodeStatus|valid|
|NodeStatus|invalid|

<h2 id="tocS_NodeType">NodeType</h2>
<!-- backwards compatibility -->
<a id="schemanodetype"></a>
<a id="schema_NodeType"></a>
<a id="tocSnodetype"></a>
<a id="tocsnodetype"></a>

```json
"source"

```

NodeType

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|NodeType|string|false|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|

#### Enumerated Values

|Property|Value|
|---|---|
|NodeType|source|
|NodeType|transform|
|NodeType|metric|
|NodeType|dimension|
|NodeType|cube|

<h2 id="tocS_NodeValidation">NodeValidation</h2>
<!-- backwards compatibility -->
<a id="schemanodevalidation"></a>
<a id="schema_NodeValidation"></a>
<a id="tocSnodevalidation"></a>
<a id="tocsnodevalidation"></a>

```json
{
  "message": "string",
  "status": "valid",
  "node_revision": {
    "name": "string",
    "display_name": "string",
    "type": "source",
    "description": "",
    "query": "string",
    "mode": "published",
    "id": 0,
    "version": "v0.1",
    "node_id": 0,
    "catalog_id": 0,
    "schema_": "string",
    "table": "string",
    "status": "invalid",
    "updated_at": "2019-08-24T14:15:22Z"
  },
  "dependencies": [
    {
      "node_revision_id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "id": 0,
        "uuid": "095be615-a8ad-4c33-8e9c-c7612fbf6c9f",
        "name": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z",
        "extra_params": {}
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "max_partition": [
          "string"
        ],
        "min_partition": [
          "string"
        ],
        "id": 0,
        "updated_at": "2019-08-24T14:15:22Z"
      },
      "columns": [
        {
          "name": "string",
          "type": {},
          "attributes": [
            {
              "attribute_type": {
                "namespace": "string",
                "name": "string"
              }
            }
          ],
          "dimension": {
            "name": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materialization_configs": [
        {
          "engine": {
            "name": "string",
            "version": "string",
            "uri": "string",
            "dialect": "spark"
          },
          "config": {},
          "schedule": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ]
    }
  ],
  "columns": [
    {
      "id": 0,
      "name": "string",
      "type": {},
      "dimension_id": 0,
      "dimension_column": "string"
    }
  ]
}

```

NodeValidation

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|message|string|true|none|none|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|node_revision|[NodeRevision](#schemanoderevision)|true|none|A node revision.|
|dependencies|[[NodeRevisionOutput](#schemanoderevisionoutput)]|true|none|[Output for a node revision with information about columns and if it is a metric.]|
|columns|[[Column](#schemacolumn)]|true|none|[A column.<br><br>Columns can be physical (associated with ``Table`` objects) or abstract (associated<br>with ``Node`` objects).]|

<h2 id="tocS_QueryResults">QueryResults</h2>
<!-- backwards compatibility -->
<a id="schemaqueryresults"></a>
<a id="schema_QueryResults"></a>
<a id="tocSqueryresults"></a>
<a id="tocsqueryresults"></a>

```json
[
  {
    "sql": "string",
    "columns": [
      {
        "name": "string",
        "type": "string"
      }
    ],
    "rows": [
      [
        null
      ]
    ],
    "row_count": 0
  }
]

```

QueryResults

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|QueryResults|[[StatementResults](#schemastatementresults)]|false|none|Results for a given query.|

<h2 id="tocS_QueryState">QueryState</h2>
<!-- backwards compatibility -->
<a id="schemaquerystate"></a>
<a id="schema_QueryState"></a>
<a id="tocSquerystate"></a>
<a id="tocsquerystate"></a>

```json
"UNKNOWN"

```

QueryState

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|QueryState|string|false|none|Different states of a query.|

#### Enumerated Values

|Property|Value|
|---|---|
|QueryState|UNKNOWN|
|QueryState|ACCEPTED|
|QueryState|SCHEDULED|
|QueryState|RUNNING|
|QueryState|FINISHED|
|QueryState|CANCELED|
|QueryState|FAILED|

<h2 id="tocS_QueryWithResults">QueryWithResults</h2>
<!-- backwards compatibility -->
<a id="schemaquerywithresults"></a>
<a id="schema_QueryWithResults"></a>
<a id="tocSquerywithresults"></a>
<a id="tocsquerywithresults"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "engine_name": "string",
  "engine_version": "string",
  "submitted_query": "string",
  "executed_query": "string",
  "scheduled": "2019-08-24T14:15:22Z",
  "started": "2019-08-24T14:15:22Z",
  "finished": "2019-08-24T14:15:22Z",
  "state": "UNKNOWN",
  "progress": 0,
  "output_table": {
    "catalog": "string",
    "schema": "string",
    "table": "string"
  },
  "results": [
    {
      "sql": "string",
      "columns": [
        {
          "name": "string",
          "type": "string"
        }
      ],
      "rows": [
        [
          null
        ]
      ],
      "row_count": 0
    }
  ],
  "next": "http://example.com",
  "previous": "http://example.com",
  "errors": [
    "string"
  ]
}

```

QueryWithResults

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|engine_name|string|false|none|none|
|engine_version|string|false|none|none|
|submitted_query|string|true|none|none|
|executed_query|string|false|none|none|
|scheduled|string(date-time)|false|none|none|
|started|string(date-time)|false|none|none|
|finished|string(date-time)|false|none|none|
|state|[QueryState](#schemaquerystate)|false|none|Different states of a query.|
|progress|number|false|none|none|
|output_table|[TableRef](#schematableref)|false|none|Table reference|
|results|[QueryResults](#schemaqueryresults)|true|none|Results for a given query.|
|next|string(uri)|false|none|none|
|previous|string(uri)|false|none|none|
|errors|[string]|true|none|none|

<h2 id="tocS_SourceColumnOutput">SourceColumnOutput</h2>
<!-- backwards compatibility -->
<a id="schemasourcecolumnoutput"></a>
<a id="schema_SourceColumnOutput"></a>
<a id="tocSsourcecolumnoutput"></a>
<a id="tocssourcecolumnoutput"></a>

```json
{
  "name": "string",
  "type": {},
  "attributes": [
    {
      "attribute_type": {
        "namespace": "string",
        "name": "string"
      }
    }
  ],
  "dimension": "string"
}

```

SourceColumnOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|type|[ColumnType](#schemacolumntype)|true|none|Base type for all Column Types|
|attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|dimension|string|false|none|none|

<h2 id="tocS_StatementResults">StatementResults</h2>
<!-- backwards compatibility -->
<a id="schemastatementresults"></a>
<a id="schema_StatementResults"></a>
<a id="tocSstatementresults"></a>
<a id="tocsstatementresults"></a>

```json
{
  "sql": "string",
  "columns": [
    {
      "name": "string",
      "type": "string"
    }
  ],
  "rows": [
    [
      null
    ]
  ],
  "row_count": 0
}

```

StatementResults

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|sql|string|true|none|none|
|columns|[[ColumnMetadata](#schemacolumnmetadata)]|true|none|[A simple model for column metadata.]|
|rows|[array]|true|none|none|
|row_count|integer|false|none|none|

<h2 id="tocS_TableRef">TableRef</h2>
<!-- backwards compatibility -->
<a id="schematableref"></a>
<a id="schema_TableRef"></a>
<a id="tocStableref"></a>
<a id="tocstableref"></a>

```json
{
  "catalog": "string",
  "schema": "string",
  "table": "string"
}

```

TableRef

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|catalog|string|true|none|none|
|schema|string|true|none|none|
|table|string|true|none|none|

<h2 id="tocS_Tag">Tag</h2>
<!-- backwards compatibility -->
<a id="schematag"></a>
<a id="schema_Tag"></a>
<a id="tocStag"></a>
<a id="tocstag"></a>

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string",
  "id": 0
}

```

Tag

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|true|none|none|
|tag_metadata|object|false|none|none|
|name|string|true|none|none|
|display_name|string|false|none|none|
|tag_type|string|true|none|none|
|id|integer|false|none|none|

<h2 id="tocS_TagOutput">TagOutput</h2>
<!-- backwards compatibility -->
<a id="schematagoutput"></a>
<a id="schema_TagOutput"></a>
<a id="tocStagoutput"></a>
<a id="tocstagoutput"></a>

```json
{
  "description": "string",
  "tag_metadata": {},
  "name": "string",
  "display_name": "string",
  "tag_type": "string"
}

```

TagOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|true|none|none|
|tag_metadata|object|false|none|none|
|name|string|true|none|none|
|display_name|string|false|none|none|
|tag_type|string|true|none|none|

<h2 id="tocS_TranslatedSQL">TranslatedSQL</h2>
<!-- backwards compatibility -->
<a id="schematranslatedsql"></a>
<a id="schema_TranslatedSQL"></a>
<a id="tocStranslatedsql"></a>
<a id="tocstranslatedsql"></a>

```json
{
  "sql": "string",
  "columns": [
    {
      "name": "string",
      "type": "string"
    }
  ],
  "dialect": "spark"
}

```

TranslatedSQL

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|sql|string|true|none|none|
|columns|[[ColumnMetadata](#schemacolumnmetadata)]|false|none|[A simple model for column metadata.]|
|dialect|[Dialect](#schemadialect)|false|none|SQL dialect|

<h2 id="tocS_UniquenessScope">UniquenessScope</h2>
<!-- backwards compatibility -->
<a id="schemauniquenessscope"></a>
<a id="schema_UniquenessScope"></a>
<a id="tocSuniquenessscope"></a>
<a id="tocsuniquenessscope"></a>

```json
"node"

```

UniquenessScope

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|UniquenessScope|string|false|none|The scope at which this attribute needs to be unique.|

#### Enumerated Values

|Property|Value|
|---|---|
|UniquenessScope|node|
|UniquenessScope|column_type|

<h2 id="tocS_UpdateNode">UpdateNode</h2>
<!-- backwards compatibility -->
<a id="schemaupdatenode"></a>
<a id="schema_UpdateNode"></a>
<a id="tocSupdatenode"></a>
<a id="tocsupdatenode"></a>

```json
{
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "columns": [
    {
      "name": "string",
      "type": {},
      "attributes": [
        {
          "attribute_type": {
            "namespace": "string",
            "name": "string"
          }
        }
      ],
      "dimension": "string"
    }
  ],
  "display_name": "string",
  "description": "string",
  "mode": "published",
  "primary_key": [
    "string"
  ],
  "query": "string"
}

```

UpdateNode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|catalog|string|false|none|none|
|schema_|string|false|none|none|
|table|string|false|none|none|
|columns|[[SourceColumnOutput](#schemasourcecolumnoutput)]|false|none|[A column used in creation of a source node]|
|display_name|string|false|none|none|
|description|string|false|none|none|
|mode|[NodeMode](#schemanodemode)|false|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|primary_key|[string]|false|none|none|
|query|string|false|none|none|

<h2 id="tocS_UpdateTag">UpdateTag</h2>
<!-- backwards compatibility -->
<a id="schemaupdatetag"></a>
<a id="schema_UpdateTag"></a>
<a id="tocSupdatetag"></a>
<a id="tocsupdatetag"></a>

```json
{
  "description": "string",
  "tag_metadata": {}
}

```

UpdateTag

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|false|none|none|
|tag_metadata|object|false|none|none|

<h2 id="tocS_UpsertMaterializationConfig">UpsertMaterializationConfig</h2>
<!-- backwards compatibility -->
<a id="schemaupsertmaterializationconfig"></a>
<a id="schema_UpsertMaterializationConfig"></a>
<a id="tocSupsertmaterializationconfig"></a>
<a id="tocsupsertmaterializationconfig"></a>

```json
{
  "engine_name": "string",
  "engine_version": "string",
  "config": {},
  "schedule": "string"
}

```

UpsertMaterializationConfig

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|engine_name|string|true|none|none|
|engine_version|string|true|none|none|
|config|object|true|none|none|
|schedule|string|true|none|none|

<h2 id="tocS_ValidationError">ValidationError</h2>
<!-- backwards compatibility -->
<a id="schemavalidationerror"></a>
<a id="schema_ValidationError"></a>
<a id="tocSvalidationerror"></a>
<a id="tocsvalidationerror"></a>

```json
{
  "loc": [
    "string"
  ],
  "msg": "string",
  "type": "string"
}

```

ValidationError

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|loc|[anyOf]|true|none|none|

anyOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|string|false|none|none|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|integer|false|none|none|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|msg|string|true|none|none|
|type|string|true|none|none|

