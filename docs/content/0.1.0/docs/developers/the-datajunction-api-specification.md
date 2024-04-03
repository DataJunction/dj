---
title: DJ server v0.0.1a44
language_tabs:
  - shell: Shell
  - http: HTTP
  - javascript: JavaScript
  - ruby: Ruby
  - python: Python
  - php: PHP
  - java: Java
  - go: Go
toc_footers: []
includes: []
search: true
highlight_theme: darkula
headingLevel: 2

---

<!-- Generator: Widdershins v4.0.1 -->

<h1 id="dj-server">DJ server v0.0.1a44</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

A DataJunction metrics layer

License: <a href="https://mit-license.org/">MIT License</a>

# Authentication

- HTTP Authentication, scheme: bearer 

<h1 id="dj-server-default">Default</h1>

## Handle Http Get

<a id="opIdhandle_http_get_graphql_get"></a>

`GET /graphql`

> Example responses

> 200 Response

```json
null
```

<h3 id="handle-http-get-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|The GraphiQL integrated development environment.|Inline|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Not found if GraphiQL or query via GET are not enabled.|None|

<h3 id="handle-http-get-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Handle Http Post

<a id="opIdhandle_http_post_graphql_post"></a>

`POST /graphql`

> Example responses

> 200 Response

```json
null
```

<h3 id="handle-http-post-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="handle-http-post-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

<h1 id="dj-server-catalogs">catalogs</h1>

## List Catalogs

<a id="opIdlist_catalogs_catalogs_get"></a>

`GET /catalogs`

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

*Response List Catalogs Catalogs Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Catalogs Catalogs Get|[[CatalogInfo](#schemacataloginfo)]|false|none|[Class for catalog creation]|
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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Add A Catalog

<a id="opIdAdd_A_Catalog_catalogs_post"></a>

`POST /catalogs`

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get A Catalog

<a id="opIdGet_a_Catalog_catalogs__name__get"></a>

`GET /catalogs/{name}`

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Add Engines To A Catalog

<a id="opIdAdd_Engines_to_a_Catalog_catalogs__name__engines_post"></a>

`POST /catalogs/{name}/engines`

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-engines">engines</h1>

## List Engines

<a id="opIdlist_engines_engines_get"></a>

`GET /engines`

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

*Response List Engines Engines Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Engines Engines Get|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Add An Engine

<a id="opIdAdd_An_Engine_engines_post"></a>

`POST /engines`

Add a new engine

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get An Engine

<a id="opIdget_an_engine_engines__name___version__get"></a>

`GET /engines/{name}/{version}`

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-metrics">metrics</h1>

## List Metrics

<a id="opIdlist_metrics_metrics_get"></a>

`GET /metrics`

List all available metrics.

<h3 id="list-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|prefix|query|string|false|none|

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
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-metrics-responseschema">Response Schema</h3>

Status Code **200**

*Response List Metrics Metrics Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Metrics Metrics Get|[string]|false|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Metric Metadata

<a id="opIdlist_metric_metadata_metrics_metadata_get"></a>

`GET /metrics/metadata`

Return available metric metadata attributes

> Example responses

> 200 Response

```json
{
  "directions": [
    "higher_is_better"
  ],
  "units": [
    {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  ]
}
```

<h3 id="list-metric-metadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[MetricMetadataOptions](#schemametricmetadataoptions)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get A Metric

<a id="opIdget_a_metric_metrics__name__get"></a>

`GET /metrics/{name}`

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
  "upstream_node": "string",
  "expression": "string",
  "dimensions": [
    {
      "name": "string",
      "node_name": "string",
      "node_display_name": "string",
      "is_primary_key": true,
      "type": "string",
      "path": [
        "string"
      ]
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "required_dimensions": [
    "string"
  ]
}
```

<h3 id="get-a-metric-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[Metric](#schemametric)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Common Dimensions

<a id="opIdget_common_dimensions_metrics_common_dimensions_get"></a>

`GET /metrics/common/dimensions`

Return common dimensions for a set of metrics.

<h3 id="get-common-dimensions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metric|query|array[string]|false|none|

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "node_name": "string",
    "node_display_name": "string",
    "is_primary_key": true,
    "type": "string",
    "path": [
      "string"
    ]
  }
]
```

<h3 id="get-common-dimensions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="get-common-dimensions-responseschema">Response Schema</h3>

Status Code **200**

*Response Get Common Dimensions Metrics Common Dimensions Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Get Common Dimensions Metrics Common Dimensions Get|[[DimensionAttributeOutput](#schemadimensionattributeoutput)]|false|none|[Dimension attribute output should include the name and type]|
|» DimensionAttributeOutput|[DimensionAttributeOutput](#schemadimensionattributeoutput)|false|none|Dimension attribute output should include the name and type|
|»» name|string|true|none|none|
|»» node_name|string|false|none|none|
|»» node_display_name|string|false|none|none|
|»» is_primary_key|boolean|true|none|none|
|»» type|string|true|none|none|
|»» path|[string]|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-djsql">DJSQL</h1>

## Get Data For Djsql

<a id="opIdget_data_for_djsql_djsql_data_get"></a>

`GET /djsql/data`

Return data for a DJ SQL query

<h3 id="get-data-for-djsql-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|query|query|string|true|none|
|async_|query|boolean|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}
```

<h3 id="get-data-for-djsql-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Data Stream For Djsql

<a id="opIdget_data_stream_for_djsql_djsql_stream_get"></a>

`GET /djsql/stream`

Return data for a DJ SQL query using server side events

<h3 id="get-data-stream-for-djsql-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|query|query|string|true|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}
```

<h3 id="get-data-stream-for-djsql-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-nodes">nodes</h1>

## Validate Node

<a id="opIdvalidate_node_nodes_validate_post"></a>

`POST /nodes/validate`

Determines whether the provided node is valid and returns metadata from node validation.

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

<h3 id="validate-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[NodeRevisionBase](#schemanoderevisionbase)|true|none|

> Example responses

> 200 Response

```json
{
  "message": "string",
  "status": "valid",
  "dependencies": [
    {
      "id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "name": "string",
        "engines": []
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "min_temporal_partition": [],
        "max_temporal_partition": [],
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "url": "string",
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": []
      },
      "columns": [
        {
          "name": "string",
          "display_name": "string",
          "type": "string",
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
          },
          "partition": {
            "type_": "temporal",
            "format": "string",
            "granularity": "string",
            "expression": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materializations": [
        {
          "name": "string",
          "config": {},
          "schedule": "string",
          "job": "string",
          "backfills": [
            {
              "spec": {
                "column_name": "string",
                "values": [
                  null
                ],
                "range": [
                  null
                ]
              },
              "urls": [
                "string"
              ]
            }
          ],
          "strategy": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ],
      "metric_metadata": {
        "direction": "higher_is_better",
        "unit": {
          "name": "string",
          "label": "string",
          "category": "string",
          "abbreviation": "string",
          "description": "string"
        }
      },
      "dimension_links": [
        {
          "dimension": {
            "name": "string"
          },
          "join_type": "left",
          "join_sql": "string",
          "join_cardinality": "one_to_one",
          "role": "string",
          "foreign_keys": {
            "property1": "string",
            "property2": "string"
          }
        }
      ]
    }
  ],
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "errors": [
    {
      "code": 0,
      "message": "string",
      "debug": {},
      "context": ""
    }
  ],
  "missing_parents": [
    "string"
  ]
}
```

<h3 id="validate-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeValidation](#schemanodevalidation)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Revalidate

<a id="opIdrevalidate_nodes__name__validate_post"></a>

`POST /nodes/{name}/validate`

Revalidate a single existing node and update its status appropriately

<h3 id="revalidate-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "message": "string",
  "status": "valid",
  "dependencies": [
    {
      "id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "name": "string",
        "engines": []
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "min_temporal_partition": [],
        "max_temporal_partition": [],
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "url": "string",
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": []
      },
      "columns": [
        {
          "name": "string",
          "display_name": "string",
          "type": "string",
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
          },
          "partition": {
            "type_": "temporal",
            "format": "string",
            "granularity": "string",
            "expression": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materializations": [
        {
          "name": "string",
          "config": {},
          "schedule": "string",
          "job": "string",
          "backfills": [
            {
              "spec": {
                "column_name": "string",
                "values": [
                  null
                ],
                "range": [
                  null
                ]
              },
              "urls": [
                "string"
              ]
            }
          ],
          "strategy": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ],
      "metric_metadata": {
        "direction": "higher_is_better",
        "unit": {
          "name": "string",
          "label": "string",
          "category": "string",
          "abbreviation": "string",
          "description": "string"
        }
      },
      "dimension_links": [
        {
          "dimension": {
            "name": "string"
          },
          "join_type": "left",
          "join_sql": "string",
          "join_cardinality": "one_to_one",
          "role": "string",
          "foreign_keys": {
            "property1": "string",
            "property2": "string"
          }
        }
      ]
    }
  ],
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "errors": [
    {
      "code": 0,
      "message": "string",
      "debug": {},
      "context": ""
    }
  ],
  "missing_parents": [
    "string"
  ]
}
```

<h3 id="revalidate-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeValidation](#schemanodevalidation)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Set Column Attributes

<a id="opIdset_column_attributes_nodes__node_name__columns__column_name__attributes_post"></a>

`POST /nodes/{node_name}/columns/{column_name}/attributes`

Set column attributes for the node.

> Body parameter

```json
[
  {
    "namespace": "system",
    "name": "string"
  }
]
```

<h3 id="set-column-attributes-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|column_name|path|string|true|none|

> Example responses

> 201 Response

```json
[
  {
    "name": "string",
    "display_name": "string",
    "type": "string",
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
    },
    "partition": {
      "type_": "temporal",
      "format": "string",
      "granularity": "string",
      "expression": "string"
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

*Response Set Column Attributes Nodes  Node Name  Columns  Column Name  Attributes Post*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Set Column Attributes Nodes  Node Name  Columns  Column Name  Attributes Post|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|false|none|[A simplified column schema, without ID or dimensions.]|
|» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»» name|string|true|none|none|
|»» display_name|string|false|none|none|
|»» type|string|true|none|none|
|»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»» namespace|string|true|none|none|
|»»»»» name|string|true|none|none|
|»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»» name|string|true|none|none|
|»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»» format|string|false|none|none|
|»»» granularity|string|false|none|none|
|»»» expression|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type_|temporal|
|type_|categorical|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Nodes

<a id="opIdlist_nodes_nodes_get"></a>

`GET /nodes`

List the available nodes.

<h3 id="list-nodes-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_type|query|[NodeType](#schemanodetype)|false|none|
|prefix|query|string|false|none|

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

<h3 id="list-nodes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-nodes-responseschema">Response Schema</h3>

Status Code **200**

*Response List Nodes Nodes Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Nodes Nodes Get|[string]|false|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List All Nodes With Details

<a id="opIdlist_all_nodes_with_details_nodes_details_get"></a>

`GET /nodes/details`

List the available nodes.

<h3 id="list-all-nodes-with-details-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
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
    "name": "string",
    "display_name": "string",
    "description": "string",
    "type": "source"
  }
]
```

<h3 id="list-all-nodes-with-details-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-all-nodes-with-details-responseschema">Response Schema</h3>

Status Code **200**

*Response List All Nodes With Details Nodes Details Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List All Nodes With Details Nodes Details Get|[[NodeIndexItem](#schemanodeindexitem)]|false|none|[Node details used for indexing purposes]|
|» NodeIndexItem|[NodeIndexItem](#schemanodeindexitem)|false|none|Node details used for indexing purposes|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» description|string|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|

#### Enumerated Values

|Property|Value|
|---|---|
|type|source|
|type|transform|
|type|metric|
|type|dimension|
|type|cube|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Node

<a id="opIdget_node_nodes__name__get"></a>

`GET /nodes/{name}`

Show the active version of the specified node.

<h3 id="get-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="get-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Delete Node

<a id="opIddelete_node_nodes__name__delete"></a>

`DELETE /nodes/{name}`

Delete (aka deactivate) the specified node.

<h3 id="delete-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="delete-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="delete-node-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Update Node

<a id="opIdupdate_node_nodes__name__patch"></a>

`PATCH /nodes/{name}`

Update a node.

> Body parameter

```json
{
  "metrics": [
    "string"
  ],
  "dimensions": [
    "string"
  ],
  "filters": [
    "string"
  ],
  "orderby": [
    "string"
  ],
  "limit": 0,
  "description": "string",
  "mode": "published",
  "required_dimensions": [
    "string"
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "string"
  },
  "query": "string",
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
  "missing_table": true,
  "display_name": "string",
  "primary_key": [
    "string"
  ]
}
```

<h3 id="update-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|body|body|[UpdateNode](#schemaupdatenode)|true|none|

> Example responses

> 200 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="update-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Hard Delete A Dj Node

<a id="opIdHard_Delete_a_DJ_Node_nodes__name__hard_delete"></a>

`DELETE /nodes/{name}/hard`

Hard delete a node, destroying all links and invalidating all downstream nodes.
This should be used with caution, deactivating a node is preferred.

<h3 id="hard-delete-a-dj-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="hard-delete-a-dj-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="hard-delete-a-dj-node-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Restore Node

<a id="opIdrestore_node_nodes__name__restore_post"></a>

`POST /nodes/{name}/restore`

Restore (aka re-activate) the specified node.

<h3 id="restore-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="restore-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="restore-node-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Node Revisions

<a id="opIdlist_node_revisions_nodes__name__revisions_get"></a>

`GET /nodes/{name}/revisions`

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
    "id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "name": "string",
      "engines": []
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "min_temporal_partition": [],
      "max_temporal_partition": [],
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "url": "string",
      "categorical_partitions": [],
      "temporal_partitions": [],
      "partitions": []
    },
    "columns": [
      {
        "name": "string",
        "display_name": "string",
        "type": "string",
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
        },
        "partition": {
          "type_": "temporal",
          "format": "string",
          "granularity": "string",
          "expression": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materializations": [
      {
        "name": "string",
        "config": {},
        "schedule": "string",
        "job": "string",
        "backfills": [
          {
            "spec": {
              "column_name": "string",
              "values": [
                null
              ],
              "range": [
                null
              ]
            },
            "urls": [
              "string"
            ]
          }
        ],
        "strategy": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ],
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": {
        "name": "string",
        "label": "string",
        "category": "string",
        "abbreviation": "string",
        "description": "string"
      }
    },
    "dimension_links": [
      {
        "dimension": {
          "name": "string"
        },
        "join_type": "left",
        "join_sql": "string",
        "join_cardinality": "one_to_one",
        "role": "string",
        "foreign_keys": {
          "property1": "string",
          "property2": "string"
        }
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

*Response List Node Revisions Nodes  Name  Revisions Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Node Revisions Nodes  Name  Revisions Get|[[NodeRevisionOutput](#schemanoderevisionoutput)]|false|none|[Output for a node revision with information about columns and if it is a metric.]|
|» NodeRevisionOutput|[NodeRevisionOutput](#schemanoderevisionoutput)|false|none|Output for a node revision with information about columns and if it is a metric.|
|»» id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»»» name|string|true|none|none|
|»»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» query|string|false|none|none|
|»» availability|[AvailabilityStateBase](#schemaavailabilitystatebase)|false|none|An availability state base|
|»»» min_temporal_partition|[string]|false|none|none|
|»»» max_temporal_partition|[string]|false|none|none|
|»»» catalog|string|true|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|true|none|none|
|»»» valid_through_ts|integer|true|none|none|
|»»» url|string|false|none|none|
|»»» categorical_partitions|[string]|false|none|none|
|»»» temporal_partitions|[string]|false|none|none|
|»»» partitions|[[PartitionAvailability](#schemapartitionavailability)]|false|none|[Partition-level availability]|
|»»»» PartitionAvailability|[PartitionAvailability](#schemapartitionavailability)|false|none|Partition-level availability|
|»»»»» min_temporal_partition|[string]|false|none|none|
|»»»»» max_temporal_partition|[string]|false|none|none|
|»»»»» value|[string]|true|none|none|
|»»»»» valid_through_ts|integer|false|none|none|
|»» columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» type|string|true|none|none|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»»»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»»»» format|string|false|none|none|
|»»»»» granularity|string|false|none|none|
|»»»»» expression|string|false|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» materializations|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»» name|string|false|none|none|
|»»»» config|object|true|none|none|
|»»»» schedule|string|true|none|none|
|»»»» job|string|false|none|none|
|»»»» backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|»»»»» BackfillOutput|[BackfillOutput](#schemabackfilloutput)|false|none|Output model for backfills|
|»»»»»» spec|[PartitionBackfill](#schemapartitionbackfill)|false|none|Used for setting backfilled values|
|»»»»»»» column_name|string|true|none|none|
|»»»»»»» values|[any]|false|none|none|
|»»»»»»» range|[any]|false|none|none|
|»»»»»» urls|[string]|false|none|none|
|»»»» strategy|string|false|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» metric_metadata|[MetricMetadataOutput](#schemametricmetadataoutput)|false|none|Metric metadata output|
|»»» direction|[MetricDirection](#schemametricdirection)|false|none|The direction of the metric that's considered good, i.e., higher is better|
|»»» unit|[Unit](#schemaunit)|false|none|Metric unit|
|»»»» name|string|true|none|none|
|»»»» label|string|false|none|none|
|»»»» category|string|false|none|none|
|»»»» abbreviation|string|false|none|none|
|»»»» description|string|false|none|none|
|»» dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|false|none|[Input for linking a dimension to a node]|
|»»» LinkDimensionOutput|[LinkDimensionOutput](#schemalinkdimensionoutput)|false|none|Input for linking a dimension to a node|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|»»»» join_type|[JoinType](#schemajointype)|true|none|Join type|
|»»»» join_sql|string|true|none|none|
|»»»» join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|»»»» role|string|false|none|none|
|»»»» foreign_keys|object|true|none|none|
|»»»»» **additionalProperties**|string|false|none|none|

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
|type_|temporal|
|type_|categorical|
|direction|higher_is_better|
|direction|lower_is_better|
|direction|neutral|
|join_type|left|
|join_type|right|
|join_type|inner|
|join_type|full|
|join_type|cross|
|join_cardinality|one_to_one|
|join_cardinality|one_to_many|
|join_cardinality|many_to_one|
|join_cardinality|many_to_many|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create A Source Node

<a id="opIdCreate_A_Source_Node_nodes_source_post"></a>

`POST /nodes/source`

Create a source node. If columns are not provided, the source node's schema
will be inferred using the configured query service.

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
  "missing_table": false,
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

<h3 id="create-a-source-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateSourceNode](#schemacreatesourcenode)|true|none|

> Example responses

> 200 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="create-a-source-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create A Metric Node

<a id="opIdCreate_A_Metric_Node_nodes_metric_post"></a>

`POST /nodes/metric`

Create a node.

> Body parameter

```json
{
  "required_dimensions": [
    "string"
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "string"
  },
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

<h3 id="create-a-metric-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateNode](#schemacreatenode)|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="create-a-metric-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create A Dimension Node

<a id="opIdCreate_A_Dimension_Node_nodes_dimension_post"></a>

`POST /nodes/dimension`

Create a node.

> Body parameter

```json
{
  "required_dimensions": [
    "string"
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "string"
  },
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

<h3 id="create-a-dimension-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateNode](#schemacreatenode)|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="create-a-dimension-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create A Transform Node

<a id="opIdCreate_A_Transform_Node_nodes_transform_post"></a>

`POST /nodes/transform`

Create a node.

> Body parameter

```json
{
  "required_dimensions": [
    "string"
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "string"
  },
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

<h3 id="create-a-transform-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateNode](#schemacreatenode)|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="create-a-transform-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create A Cube

<a id="opIdCreate_A_Cube_nodes_cube_post"></a>

`POST /nodes/cube`

Create a cube node.

> Body parameter

```json
{
  "metrics": [
    "string"
  ],
  "dimensions": [
    "string"
  ],
  "filters": [
    "string"
  ],
  "orderby": [
    "string"
  ],
  "limit": 0,
  "description": "string",
  "mode": "published",
  "display_name": "string",
  "primary_key": [
    "string"
  ],
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
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="create-a-cube-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Register Table

<a id="opIdregister_table_register_table__catalog___schema____table__post"></a>

`POST /register/table/{catalog}/{schema_}/{table}`

Register a table. This creates a source node in the SOURCE_NODE_NAMESPACE and
the source node's schema will be inferred using the configured query service.

<h3 id="register-table-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|catalog|path|string|true|none|
|schema_|path|string|true|none|
|table|path|string|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="register-table-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Link Dimension

<a id="opIdlink_dimension_nodes__name__columns__column__post"></a>

`POST /nodes/{name}/columns/{column}`

Add information to a node column

<h3 id="link-dimension-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|column|path|string|true|none|
|dimension|query|string|true|none|
|dimension_column|query|string|false|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="link-dimension-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="link-dimension-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Delete Dimension Link

<a id="opIddelete_dimension_link_nodes__name__columns__column__delete"></a>

`DELETE /nodes/{name}/columns/{column}`

Remove the link between a node column and a dimension node

<h3 id="delete-dimension-link-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|column|path|string|true|none|
|dimension|query|string|true|none|
|dimension_column|query|string|false|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="delete-dimension-link-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="delete-dimension-link-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Add Complex Dimension Link

<a id="opIdadd_complex_dimension_link_nodes__node_name__link_post"></a>

`POST /nodes/{node_name}/link`

Links a source, dimension, or transform node to a dimension with a custom join query.
If a link already exists, updates the link definition.

> Body parameter

```json
{
  "dimension_node": "string",
  "join_type": "left",
  "join_on": "string",
  "join_cardinality": "many_to_one",
  "role": "string"
}
```

<h3 id="add-complex-dimension-link-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|body|body|[LinkDimensionInput](#schemalinkdimensioninput)|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="add-complex-dimension-link-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="add-complex-dimension-link-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Remove Complex Dimension Link

<a id="opIdremove_complex_dimension_link_nodes__node_name__link_delete"></a>

`DELETE /nodes/{node_name}/link`

Removes a complex dimension link based on the dimension node and its role (if any).

> Body parameter

```json
{
  "dimension_node": "string",
  "role": "string"
}
```

<h3 id="remove-complex-dimension-link-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|body|body|[LinkDimensionIdentifier](#schemalinkdimensionidentifier)|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="remove-complex-dimension-link-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="remove-complex-dimension-link-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Migrate Dimension Link

<a id="opIdmigrate_dimension_link_nodes__name__migrate_dim_link_post"></a>

`POST /nodes/{name}/migrate_dim_link`

Migrate dimension link from column-level to node-level

<h3 id="migrate-dimension-link-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="migrate-dimension-link-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="migrate-dimension-link-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Update Tags On Node

<a id="opIdUpdate_Tags_on_Node_nodes__name__tags_post"></a>

`POST /nodes/{name}/tags`

Add a tag to a node

<h3 id="update-tags-on-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|tag_names|query|array[string]|false|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="update-tags-on-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="update-tags-on-node-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Refresh Source Node

<a id="opIdrefresh_source_node_nodes__name__refresh_post"></a>

`POST /nodes/{name}/refresh`

Refresh a source node with the latest columns from the query service.

<h3 id="refresh-source-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 201 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}
```

<h3 id="refresh-source-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[NodeOutput](#schemanodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Downstream Nodes For A Node

<a id="opIdList_Downstream_Nodes_For_A_Node_nodes__name__downstream_get"></a>

`GET /nodes/{name}/downstream`

List all nodes that are downstream from the given node, filterable by type.

<h3 id="list-downstream-nodes-for-a-node-parameters">Parameters</h3>

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
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "name": "string",
      "engines": []
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "columns": [
      {
        "name": "string",
        "display_name": "string",
        "type": "string",
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
        },
        "partition": {
          "type_": "temporal",
          "format": "string",
          "granularity": "string",
          "expression": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "parents": [
      {
        "name": "string"
      }
    ],
    "dimension_links": [
      {
        "dimension": {
          "name": "string"
        },
        "join_type": "left",
        "join_sql": "string",
        "join_cardinality": "one_to_one",
        "role": "string",
        "foreign_keys": {
          "property1": "string",
          "property2": "string"
        }
      }
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "tags": [],
    "current_version": "string"
  }
]
```

<h3 id="list-downstream-nodes-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-downstream-nodes-for-a-node-responseschema">Response Schema</h3>

Status Code **200**

*Response List Downstream Nodes For A Node Nodes  Name  Downstream Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Downstream Nodes For A Node Nodes  Name  Downstream Get|[[DAGNodeOutput](#schemadagnodeoutput)]|false|none|[Output for a node in another node's DAG]|
|» DAGNodeOutput|[DAGNodeOutput](#schemadagnodeoutput)|false|none|Output for a node in another node's DAG|
|»» namespace|string|true|none|none|
|»» node_revision_id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»»» name|string|true|none|none|
|»»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» type|string|true|none|none|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»»»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»»»» format|string|false|none|none|
|»»»»» granularity|string|false|none|none|
|»»»»» expression|string|false|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|true|none|[Input for linking a dimension to a node]|
|»»» LinkDimensionOutput|[LinkDimensionOutput](#schemalinkdimensionoutput)|false|none|Input for linking a dimension to a node|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|»»»» join_type|[JoinType](#schemajointype)|true|none|Join type|
|»»»» join_sql|string|true|none|none|
|»»»» join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|»»»» role|string|false|none|none|
|»»»» foreign_keys|object|true|none|none|
|»»»»» **additionalProperties**|string|false|none|none|
|»» created_at|string(date-time)|true|none|none|
|»» tags|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|»»» TagOutput|[TagOutput](#schematagoutput)|false|none|Output tag model.|
|»»»» description|string|false|none|none|
|»»»» display_name|string|false|none|none|
|»»»» tag_metadata|object|false|none|none|
|»»»» name|string|true|none|none|
|»»»» tag_type|string|true|none|none|
|»» current_version|string|true|none|none|

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
|type_|temporal|
|type_|categorical|
|join_type|left|
|join_type|right|
|join_type|inner|
|join_type|full|
|join_type|cross|
|join_cardinality|one_to_one|
|join_cardinality|one_to_many|
|join_cardinality|many_to_one|
|join_cardinality|many_to_many|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Upstream Nodes For A Node

<a id="opIdList_Upstream_Nodes_For_A_Node_nodes__name__upstream_get"></a>

`GET /nodes/{name}/upstream`

List all nodes that are upstream from the given node, filterable by type.

<h3 id="list-upstream-nodes-for-a-node-parameters">Parameters</h3>

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
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "name": "string",
      "engines": []
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "columns": [
      {
        "name": "string",
        "display_name": "string",
        "type": "string",
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
        },
        "partition": {
          "type_": "temporal",
          "format": "string",
          "granularity": "string",
          "expression": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "parents": [
      {
        "name": "string"
      }
    ],
    "dimension_links": [
      {
        "dimension": {
          "name": "string"
        },
        "join_type": "left",
        "join_sql": "string",
        "join_cardinality": "one_to_one",
        "role": "string",
        "foreign_keys": {
          "property1": "string",
          "property2": "string"
        }
      }
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "tags": [],
    "current_version": "string"
  }
]
```

<h3 id="list-upstream-nodes-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-upstream-nodes-for-a-node-responseschema">Response Schema</h3>

Status Code **200**

*Response List Upstream Nodes For A Node Nodes  Name  Upstream Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Upstream Nodes For A Node Nodes  Name  Upstream Get|[[DAGNodeOutput](#schemadagnodeoutput)]|false|none|[Output for a node in another node's DAG]|
|» DAGNodeOutput|[DAGNodeOutput](#schemadagnodeoutput)|false|none|Output for a node in another node's DAG|
|»» namespace|string|true|none|none|
|»» node_revision_id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»»» name|string|true|none|none|
|»»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» type|string|true|none|none|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»»»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»»»» format|string|false|none|none|
|»»»»» granularity|string|false|none|none|
|»»»»» expression|string|false|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|true|none|[Input for linking a dimension to a node]|
|»»» LinkDimensionOutput|[LinkDimensionOutput](#schemalinkdimensionoutput)|false|none|Input for linking a dimension to a node|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|»»»» join_type|[JoinType](#schemajointype)|true|none|Join type|
|»»»» join_sql|string|true|none|none|
|»»»» join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|»»»» role|string|false|none|none|
|»»»» foreign_keys|object|true|none|none|
|»»»»» **additionalProperties**|string|false|none|none|
|»» created_at|string(date-time)|true|none|none|
|»» tags|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|»»» TagOutput|[TagOutput](#schematagoutput)|false|none|Output tag model.|
|»»»» description|string|false|none|none|
|»»»» display_name|string|false|none|none|
|»»»» tag_metadata|object|false|none|none|
|»»»» name|string|true|none|none|
|»»»» tag_type|string|true|none|none|
|»» current_version|string|true|none|none|

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
|type_|temporal|
|type_|categorical|
|join_type|left|
|join_type|right|
|join_type|inner|
|join_type|full|
|join_type|cross|
|join_cardinality|one_to_one|
|join_cardinality|one_to_many|
|join_cardinality|many_to_one|
|join_cardinality|many_to_many|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List All Connected Nodes (Upstreams + Downstreams)

<a id="opIdList_All_Connected_Nodes__Upstreams___Downstreams__nodes__name__dag_get"></a>

`GET /nodes/{name}/dag`

List all nodes that are part of the DAG of the given node. This means getting all upstreams,
downstreams, and linked dimension nodes.

<h3 id="list-all-connected-nodes-(upstreams-+-downstreams)-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
[
  {
    "namespace": "string",
    "node_revision_id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "name": "string",
      "engines": []
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "columns": [
      {
        "name": "string",
        "display_name": "string",
        "type": "string",
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
        },
        "partition": {
          "type_": "temporal",
          "format": "string",
          "granularity": "string",
          "expression": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "parents": [
      {
        "name": "string"
      }
    ],
    "dimension_links": [
      {
        "dimension": {
          "name": "string"
        },
        "join_type": "left",
        "join_sql": "string",
        "join_cardinality": "one_to_one",
        "role": "string",
        "foreign_keys": {
          "property1": "string",
          "property2": "string"
        }
      }
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "tags": [],
    "current_version": "string"
  }
]
```

<h3 id="list-all-connected-nodes-(upstreams-+-downstreams)-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-all-connected-nodes-(upstreams-+-downstreams)-responseschema">Response Schema</h3>

Status Code **200**

*Response List All Connected Nodes  Upstreams   Downstreams  Nodes  Name  Dag Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List All Connected Nodes  Upstreams   Downstreams  Nodes  Name  Dag Get|[[DAGNodeOutput](#schemadagnodeoutput)]|false|none|[Output for a node in another node's DAG]|
|» DAGNodeOutput|[DAGNodeOutput](#schemadagnodeoutput)|false|none|Output for a node in another node's DAG|
|»» namespace|string|true|none|none|
|»» node_revision_id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»»» name|string|true|none|none|
|»»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» type|string|true|none|none|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»»»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»»»» format|string|false|none|none|
|»»»»» granularity|string|false|none|none|
|»»»»» expression|string|false|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|true|none|[Input for linking a dimension to a node]|
|»»» LinkDimensionOutput|[LinkDimensionOutput](#schemalinkdimensionoutput)|false|none|Input for linking a dimension to a node|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|»»»» join_type|[JoinType](#schemajointype)|true|none|Join type|
|»»»» join_sql|string|true|none|none|
|»»»» join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|»»»» role|string|false|none|none|
|»»»» foreign_keys|object|true|none|none|
|»»»»» **additionalProperties**|string|false|none|none|
|»» created_at|string(date-time)|true|none|none|
|»» tags|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|»»» TagOutput|[TagOutput](#schematagoutput)|false|none|Output tag model.|
|»»»» description|string|false|none|none|
|»»»» display_name|string|false|none|none|
|»»»» tag_metadata|object|false|none|none|
|»»»» name|string|true|none|none|
|»»»» tag_type|string|true|none|none|
|»» current_version|string|true|none|none|

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
|type_|temporal|
|type_|categorical|
|join_type|left|
|join_type|right|
|join_type|inner|
|join_type|full|
|join_type|cross|
|join_cardinality|one_to_one|
|join_cardinality|one_to_many|
|join_cardinality|many_to_one|
|join_cardinality|many_to_many|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List All Dimension Attributes

<a id="opIdList_All_Dimension_Attributes_nodes__name__dimensions_get"></a>

`GET /nodes/{name}/dimensions`

List all available dimension attributes for the given node.

<h3 id="list-all-dimension-attributes-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "node_name": "string",
    "node_display_name": "string",
    "is_primary_key": true,
    "type": "string",
    "path": [
      "string"
    ]
  }
]
```

<h3 id="list-all-dimension-attributes-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-all-dimension-attributes-responseschema">Response Schema</h3>

Status Code **200**

*Response List All Dimension Attributes Nodes  Name  Dimensions Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List All Dimension Attributes Nodes  Name  Dimensions Get|[[DimensionAttributeOutput](#schemadimensionattributeoutput)]|false|none|[Dimension attribute output should include the name and type]|
|» DimensionAttributeOutput|[DimensionAttributeOutput](#schemadimensionattributeoutput)|false|none|Dimension attribute output should include the name and type|
|»» name|string|true|none|none|
|»» node_name|string|false|none|none|
|»» node_display_name|string|false|none|none|
|»» is_primary_key|boolean|true|none|none|
|»» type|string|true|none|none|
|»» path|[string]|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Column Level Lineage Of Node

<a id="opIdList_column_level_lineage_of_node_nodes__name__lineage_get"></a>

`GET /nodes/{name}/lineage`

List column-level lineage of a node in a graph

<h3 id="list-column-level-lineage-of-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|

> Example responses

> 200 Response

```json
[
  {
    "column_name": "string",
    "node_name": "string",
    "node_type": "string",
    "display_name": "string",
    "lineage": [
      {}
    ]
  }
]
```

<h3 id="list-column-level-lineage-of-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-column-level-lineage-of-node-responseschema">Response Schema</h3>

Status Code **200**

*Response List Column Level Lineage Of Node Nodes  Name  Lineage Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Column Level Lineage Of Node Nodes  Name  Lineage Get|[[LineageColumn](#schemalineagecolumn)]|false|none|[Column in lineage graph]|
|» LineageColumn|[LineageColumn](#schemalineagecolumn)|false|none|Column in lineage graph|
|»» column_name|string|true|none|none|
|»» node_name|string|false|none|none|
|»» node_type|string|false|none|none|
|»» display_name|string|false|none|none|
|»» lineage|[[LineageColumn](#schemalineagecolumn)]|false|none|Column in lineage graph|
|»»» LineageColumn|[LineageColumn](#schemalineagecolumn)|false|none|Column in lineage graph|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Set Column Display Name

<a id="opIdset_column_display_name_nodes__node_name__columns__column_name__patch"></a>

`PATCH /nodes/{node_name}/columns/{column_name}`

Set column name for the node

<h3 id="set-column-display-name-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|column_name|path|string|true|none|
|display_name|query|string|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "display_name": "string",
  "type": "string",
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
  },
  "partition": {
    "type_": "temporal",
    "format": "string",
    "granularity": "string",
    "expression": "string"
  }
}
```

<h3 id="set-column-display-name-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Set Node Column As Partition

<a id="opIdSet_Node_Column_as_Partition_nodes__node_name__columns__column_name__partition_post"></a>

`POST /nodes/{node_name}/columns/{column_name}/partition`

Add or update partition columns for the specified node.

> Body parameter

```json
{
  "type_": "temporal",
  "granularity": "second",
  "format": "string"
}
```

<h3 id="set-node-column-as-partition-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|column_name|path|string|true|none|
|body|body|[PartitionInput](#schemapartitioninput)|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "display_name": "string",
  "type": "string",
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
  },
  "partition": {
    "type_": "temporal",
    "format": "string",
    "granularity": "string",
    "expression": "string"
  }
}
```

<h3 id="set-node-column-as-partition-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Copy A Node

<a id="opIdCopy_A_Node_nodes__node_name__copy_post"></a>

`POST /nodes/{node_name}/copy`

Copy this node to a new name.

<h3 id="copy-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|new_name|query|string|true|none|

> Example responses

> 200 Response

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "parents": [
    {
      "name": "string"
    }
  ],
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string"
}
```

<h3 id="copy-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[DAGNodeOutput](#schemadagnodeoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-namespaces">namespaces</h1>

## List Nodes In Namespace

<a id="opIdlist_nodes_in_namespace_namespaces__namespace__get"></a>

`GET /namespaces/{namespace}`

List node names in namespace, filterable to a given type if desired.

<h3 id="list-nodes-in-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|type_|query|any|false|Filter the list of nodes to this type|

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "display_name": "string",
    "description": "string",
    "version": "string",
    "type": "source",
    "status": "valid",
    "mode": "published",
    "updated_at": "2019-08-24T14:15:22Z"
  }
]
```

<h3 id="list-nodes-in-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-nodes-in-namespace-responseschema">Response Schema</h3>

Status Code **200**

*Response List Nodes In Namespace Namespaces  Namespace  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Nodes In Namespace Namespaces  Namespace  Get|[[NodeMinimumDetail](#schemanodeminimumdetail)]|false|none|[List of high level node details]|
|» NodeMinimumDetail|[NodeMinimumDetail](#schemanodeminimumdetail)|false|none|List of high level node details|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» description|string|true|none|none|
|»» version|string|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» updated_at|string(date-time)|true|none|none|

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create Node Namespace

<a id="opIdcreate_node_namespace_namespaces__namespace__post"></a>

`POST /namespaces/{namespace}`

Create a node namespace

<h3 id="create-node-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|include_parents|query|boolean|false|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="create-node-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="create-node-namespace-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Deactivate A Namespace

<a id="opIddeactivate_a_namespace_namespaces__namespace__delete"></a>

`DELETE /namespaces/{namespace}`

Deactivates a node namespace

<h3 id="deactivate-a-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|cascade|query|boolean|false|Cascade the deletion down to the nodes in the namespace|

> Example responses

> 200 Response

```json
null
```

<h3 id="deactivate-a-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="deactivate-a-namespace-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Namespaces

<a id="opIdlist_namespaces_namespaces_get"></a>

`GET /namespaces`

List namespaces with the number of nodes contained in them

> Example responses

> 200 Response

```json
[
  {
    "namespace": "string",
    "num_nodes": 0
  }
]
```

<h3 id="list-namespaces-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="list-namespaces-responseschema">Response Schema</h3>

Status Code **200**

*Response List Namespaces Namespaces Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Namespaces Namespaces Get|[[NamespaceOutput](#schemanamespaceoutput)]|false|none|[Output for a namespace that includes the number of nodes]|
|» NamespaceOutput|[NamespaceOutput](#schemanamespaceoutput)|false|none|Output for a namespace that includes the number of nodes|
|»» namespace|string|true|none|none|
|»» num_nodes|integer|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Restore A Namespace

<a id="opIdrestore_a_namespace_namespaces__namespace__restore_post"></a>

`POST /namespaces/{namespace}/restore`

Restores a node namespace

<h3 id="restore-a-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|cascade|query|boolean|false|Cascade the restore down to the nodes in the namespace|

> Example responses

> 201 Response

```json
null
```

<h3 id="restore-a-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="restore-a-namespace-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Hard Delete A Dj Namespace

<a id="opIdHard_Delete_a_DJ_Namespace_namespaces__namespace__hard_delete"></a>

`DELETE /namespaces/{namespace}/hard`

Hard delete a namespace, which will completely remove the namespace. Additionally,
if any nodes are saved under this namespace, we'll hard delete the nodes if cascade
is set to true. If cascade is set to false, we'll raise an error. This should be used
with caution, as the impact may be large.

<h3 id="hard-delete-a-dj-namespace-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|
|cascade|query|boolean|false|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="hard-delete-a-dj-namespace-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="hard-delete-a-dj-namespace-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Export A Namespace As A Single Project'S Metadata

<a id="opIdExport_a_namespace_as_a_single_project_s_metadata_namespaces__namespace__export_get"></a>

`GET /namespaces/{namespace}/export`

Generates a zip of YAML files for the contents of the given namespace
as well as a project definition file.

<h3 id="export-a-namespace-as-a-single-project's-metadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|namespace|path|string|true|none|

> Example responses

> 200 Response

```json
[
  {}
]
```

<h3 id="export-a-namespace-as-a-single-project's-metadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="export-a-namespace-as-a-single-project's-metadata-responseschema">Response Schema</h3>

Status Code **200**

*Response Export A Namespace As A Single Project S Metadata Namespaces  Namespace  Export Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Export A Namespace As A Single Project S Metadata Namespaces  Namespace  Export Get|[object]|false|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-materializations">materializations</h1>

## Materialization Jobs Info

<a id="opIdMaterialization_Jobs_Info_materialization_info_get"></a>

`GET /materialization/info`

Materialization job types and strategies

> Example responses

> 200 Response

```json
null
```

<h3 id="materialization-jobs-info-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="materialization-jobs-info-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Insert Or Update A Materialization For A Node

<a id="opIdInsert_or_Update_a_Materialization_for_a_Node_nodes__node_name__materialization_post"></a>

`POST /nodes/{node_name}/materialization`

Add or update a materialization of the specified node. If a node_name is specified
for the materialization config, it will always update that named config.

> Body parameter

```json
{
  "name": "string",
  "job": {
    "name": "spark_sql",
    "label": "Spark SQL",
    "description": "Spark SQL materialization job",
    "allowed_node_types": [
      "transform",
      "dimension",
      "cube"
    ],
    "job_class": "SparkSqlMaterializationJob"
  },
  "config": {
    "spark": {},
    "lookback_window": "string",
    "dimensions": [
      "string"
    ],
    "measures": {
      "property1": {
        "metric": "string",
        "measures": [
          {
            "name": "string",
            "field_name": "string",
            "agg": "string",
            "type": "string"
          }
        ],
        "combiner": "string"
      },
      "property2": {
        "metric": "string",
        "measures": [
          {
            "name": "string",
            "field_name": "string",
            "agg": "string",
            "type": "string"
          }
        ],
        "combiner": "string"
      }
    },
    "metrics": [
      {
        "name": "string",
        "type": "string",
        "column": "string",
        "node": "string",
        "semantic_entity": "string",
        "semantic_type": "string"
      }
    ],
    "prefix": "",
    "suffix": "",
    "druid": {
      "granularity": "string",
      "intervals": [
        "string"
      ],
      "timestamp_column": "string",
      "timestamp_format": "string",
      "parse_spec_format": "string"
    }
  },
  "schedule": "string",
  "strategy": "full"
}
```

<h3 id="insert-or-update-a-materialization-for-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|body|body|[UpsertMaterialization](#schemaupsertmaterialization)|true|none|

> Example responses

> 201 Response

```json
null
```

<h3 id="insert-or-update-a-materialization-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="insert-or-update-a-materialization-for-a-node-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Materializations For A Node

<a id="opIdList_Materializations_for_a_Node_nodes__node_name__materializations_get"></a>

`GET /nodes/{node_name}/materializations`

Show all materializations configured for the node, with any associated metadata
like urls from the materialization service, if available.

<h3 id="list-materializations-for-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|show_deleted|query|boolean|false|none|

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "config": {},
    "schedule": "string",
    "job": "string",
    "backfills": [
      {
        "spec": {
          "column_name": "string",
          "values": [
            null
          ],
          "range": [
            null
          ]
        },
        "urls": [
          "string"
        ]
      }
    ],
    "strategy": "string",
    "output_tables": [
      "string"
    ],
    "urls": [
      "http://example.com"
    ]
  }
]
```

<h3 id="list-materializations-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-materializations-for-a-node-responseschema">Response Schema</h3>

Status Code **200**

*Response List Materializations For A Node Nodes  Node Name  Materializations Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Materializations For A Node Nodes  Node Name  Materializations Get|[[MaterializationConfigInfoUnified](#schemamaterializationconfiginfounified)]|false|none|[Materialization config + info]|
|» MaterializationConfigInfoUnified|[MaterializationConfigInfoUnified](#schemamaterializationconfiginfounified)|false|none|Materialization config + info|
|»» name|string|false|none|none|
|»» config|object|true|none|none|
|»» schedule|string|true|none|none|
|»» job|string|false|none|none|
|»» backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|»»» BackfillOutput|[BackfillOutput](#schemabackfilloutput)|false|none|Output model for backfills|
|»»»» spec|[PartitionBackfill](#schemapartitionbackfill)|false|none|Used for setting backfilled values|
|»»»»» column_name|string|true|none|none|
|»»»»» values|[any]|false|none|none|
|»»»»» range|[any]|false|none|none|
|»»»» urls|[string]|false|none|none|
|»» strategy|string|false|none|none|
|»» output_tables|[string]|true|none|none|
|»» urls|[string]|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Deactivate A Materialization For A Node

<a id="opIdDeactivate_a_Materialization_for_a_Node_nodes__node_name__materializations_delete"></a>

`DELETE /nodes/{node_name}/materializations`

Deactivate the node materialization with the provided name.
Also calls the query service to deactivate the associated scheduled jobs.

<h3 id="deactivate-a-materialization-for-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|materialization_name|query|string|true|none|

> Example responses

> 200 Response

```json
[
  {
    "name": "string",
    "config": {},
    "schedule": "string",
    "job": "string",
    "backfills": [
      {
        "spec": {
          "column_name": "string",
          "values": [
            null
          ],
          "range": [
            null
          ]
        },
        "urls": [
          "string"
        ]
      }
    ],
    "strategy": "string",
    "output_tables": [
      "string"
    ],
    "urls": [
      "http://example.com"
    ]
  }
]
```

<h3 id="deactivate-a-materialization-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="deactivate-a-materialization-for-a-node-responseschema">Response Schema</h3>

Status Code **200**

*Response Deactivate A Materialization For A Node Nodes  Node Name  Materializations Delete*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Deactivate A Materialization For A Node Nodes  Node Name  Materializations Delete|[[MaterializationConfigInfoUnified](#schemamaterializationconfiginfounified)]|false|none|[Materialization config + info]|
|» MaterializationConfigInfoUnified|[MaterializationConfigInfoUnified](#schemamaterializationconfiginfounified)|false|none|Materialization config + info|
|»» name|string|false|none|none|
|»» config|object|true|none|none|
|»» schedule|string|true|none|none|
|»» job|string|false|none|none|
|»» backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|»»» BackfillOutput|[BackfillOutput](#schemabackfilloutput)|false|none|Output model for backfills|
|»»»» spec|[PartitionBackfill](#schemapartitionbackfill)|false|none|Used for setting backfilled values|
|»»»»» column_name|string|true|none|none|
|»»»»» values|[any]|false|none|none|
|»»»»» range|[any]|false|none|none|
|»»»» urls|[string]|false|none|none|
|»» strategy|string|false|none|none|
|»» output_tables|[string]|true|none|none|
|»» urls|[string]|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Kick Off A Backfill Run For A Configured Materialization

<a id="opIdKick_off_a_backfill_run_for_a_configured_materialization_nodes__node_name__materializations__materialization_name__backfill_post"></a>

`POST /nodes/{node_name}/materializations/{materialization_name}/backfill`

Start a backfill for a configured materialization.

> Body parameter

```json
{
  "column_name": "string",
  "values": [
    null
  ],
  "range": [
    null
  ]
}
```

<h3 id="kick-off-a-backfill-run-for-a-configured-materialization-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|materialization_name|path|string|true|none|
|body|body|[PartitionBackfill](#schemapartitionbackfill)|true|none|

> Example responses

> 201 Response

```json
{
  "output_tables": [
    "string"
  ],
  "urls": [
    "http://example.com"
  ]
}
```

<h3 id="kick-off-a-backfill-run-for-a-configured-materialization-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[MaterializationInfo](#schemamaterializationinfo)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-measures">measures</h1>

## List Measures

<a id="opIdlist_measures_measures_get"></a>

`GET /measures`

List all measures.

<h3 id="list-measures-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|prefix|query|string|false|none|

> Example responses

> 200 Response

```json
[
  "string"
]
```

<h3 id="list-measures-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-measures-responseschema">Response Schema</h3>

Status Code **200**

*Response List Measures Measures Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Measures Measures Get|[string]|false|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Add A Measure

<a id="opIdAdd_a_Measure_measures_post"></a>

`POST /measures`

Add a measure

> Body parameter

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "node": "string",
      "column": "string"
    }
  ],
  "additive": "non-additive"
}
```

<h3 id="add-a-measure-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[CreateMeasure](#schemacreatemeasure)|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "name": "string",
      "type": "string",
      "node": "string"
    }
  ],
  "additive": "additive"
}
```

<h3 id="add-a-measure-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[MeasureOutput](#schemameasureoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Measure

<a id="opIdget_measure_measures__measure_name__get"></a>

`GET /measures/{measure_name}`

Get info on a measure.

<h3 id="get-measure-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|measure_name|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "name": "string",
      "type": "string",
      "node": "string"
    }
  ],
  "additive": "additive"
}
```

<h3 id="get-measure-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[MeasureOutput](#schemameasureoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Edit A Measure

<a id="opIdEdit_a_Measure_measures__measure_name__patch"></a>

`PATCH /measures/{measure_name}`

Edit a measure

> Body parameter

```json
{
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "node": "string",
      "column": "string"
    }
  ],
  "additive": "additive"
}
```

<h3 id="edit-a-measure-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|measure_name|path|string|true|none|
|body|body|[EditMeasure](#schemaeditmeasure)|true|none|

> Example responses

> 201 Response

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "name": "string",
      "type": "string",
      "node": "string"
    }
  ],
  "additive": "additive"
}
```

<h3 id="edit-a-measure-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[MeasureOutput](#schemameasureoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-data">data</h1>

## Add Availability State To Node

<a id="opIdAdd_Availability_State_to_Node_data__node_name__availability_post"></a>

`POST /data/{node_name}/availability`

Add an availability state to a node.

> Body parameter

```json
{
  "min_temporal_partition": [],
  "max_temporal_partition": [],
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "valid_through_ts": 0,
  "url": "string",
  "categorical_partitions": [],
  "temporal_partitions": [],
  "partitions": []
}
```

<h3 id="add-availability-state-to-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|body|body|[AvailabilityStateBase](#schemaavailabilitystatebase)|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="add-availability-state-to-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="add-availability-state-to-node-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Data For A Node

<a id="opIdGet_Data_for_a_Node_data__node_name__get"></a>

`GET /data/{node_name}`

Gets data for a node

<h3 id="get-data-for-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|dimensions|query|array[string]|false|Dimensional attributes to group by|
|filters|query|array[string]|false|Filters on dimensional attributes|
|orderby|query|array[string]|false|Expression to order by|
|limit|query|integer|false|Number of rows to limit the data retrieved to|
|async_|query|boolean|false|Whether to run the query async or wait for results from the query engine|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}
```

<h3 id="get-data-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Data For Query Id

<a id="opIdGet_Data_For_Query_ID_data_query__query_id__get"></a>

`GET /data/query/{query_id}`

Return data for a specific query ID.

<h3 id="get-data-for-query-id-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|query_id|path|string|true|none|

> Example responses

> 200 Response

```json
{
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}
```

<h3 id="get-data-for-query-id-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Data For Metrics

<a id="opIdGet_Data_For_Metrics_data_get"></a>

`GET /data`

Return data for a set of metrics with dimensions and filters

<h3 id="get-data-for-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metrics|query|array[string]|false|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|orderby|query|array[string]|false|none|
|limit|query|integer|false|none|
|async_|query|boolean|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}
```

<h3 id="get-data-for-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Data Stream For Metrics

<a id="opIdget_data_stream_for_metrics_stream_get"></a>

`GET /stream`

Return data for a set of metrics with dimensions and filters using server side events

<h3 id="get-data-stream-for-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metrics|query|array[string]|false|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|orderby|query|array[string]|false|none|
|limit|query|integer|false|none|
|engine_name|query|string|false|none|
|engine_version|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}
```

<h3 id="get-data-stream-for-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[QueryWithResults](#schemaquerywithresults)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-health">health</h1>

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

<h1 id="dj-server-history">history</h1>

## List History

<a id="opIdlist_history_history__entity_type___entity_name__get"></a>

`GET /history/{entity_type}/{entity_name}`

List history for an entity type (i.e. Node) and entity name

<h3 id="list-history-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|entity_type|path|[EntityType](#schemaentitytype)|true|none|
|entity_name|path|string|true|none|
|offset|query|integer|false|none|
|limit|query|integer|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|entity_type|attribute|
|entity_type|availability|
|entity_type|backfill|
|entity_type|catalog|
|entity_type|column_attribute|
|entity_type|dependency|
|entity_type|engine|
|entity_type|link|
|entity_type|materialization|
|entity_type|namespace|
|entity_type|node|
|entity_type|partition|
|entity_type|query|
|entity_type|tag|

> Example responses

> 200 Response

```json
[
  {
    "id": 0,
    "entity_type": "attribute",
    "entity_name": "string",
    "node": "string",
    "activity_type": "create",
    "user": "string",
    "pre": {},
    "post": {},
    "details": {},
    "created_at": "2019-08-24T14:15:22Z"
  }
]
```

<h3 id="list-history-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-history-responseschema">Response Schema</h3>

Status Code **200**

*Response List History History  Entity Type   Entity Name  Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List History History  Entity Type   Entity Name  Get|[[HistoryOutput](#schemahistoryoutput)]|false|none|[Output history event]|
|» HistoryOutput|[HistoryOutput](#schemahistoryoutput)|false|none|Output history event|
|»» id|integer|true|none|none|
|»» entity_type|[EntityType](#schemaentitytype)|false|none|An entity type for which activity can occur|
|»» entity_name|string|false|none|none|
|»» node|string|false|none|none|
|»» activity_type|[ActivityType](#schemaactivitytype)|false|none|An activity type|
|»» user|string|false|none|none|
|»» pre|object|true|none|none|
|»» post|object|true|none|none|
|»» details|object|true|none|none|
|»» created_at|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|entity_type|attribute|
|entity_type|availability|
|entity_type|backfill|
|entity_type|catalog|
|entity_type|column_attribute|
|entity_type|dependency|
|entity_type|engine|
|entity_type|link|
|entity_type|materialization|
|entity_type|namespace|
|entity_type|node|
|entity_type|partition|
|entity_type|query|
|entity_type|tag|
|activity_type|create|
|activity_type|delete|
|activity_type|restore|
|activity_type|update|
|activity_type|refresh|
|activity_type|tag|
|activity_type|set_attribute|
|activity_type|status_change|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List History By Node Context

<a id="opIdlist_history_by_node_context_history_get"></a>

`GET /history`

List all activity history for a node context

<h3 id="list-history-by-node-context-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node|query|string|true|none|
|offset|query|integer|false|none|
|limit|query|integer|false|none|

> Example responses

> 200 Response

```json
[
  {
    "id": 0,
    "entity_type": "attribute",
    "entity_name": "string",
    "node": "string",
    "activity_type": "create",
    "user": "string",
    "pre": {},
    "post": {},
    "details": {},
    "created_at": "2019-08-24T14:15:22Z"
  }
]
```

<h3 id="list-history-by-node-context-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-history-by-node-context-responseschema">Response Schema</h3>

Status Code **200**

*Response List History By Node Context History Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List History By Node Context History Get|[[HistoryOutput](#schemahistoryoutput)]|false|none|[Output history event]|
|» HistoryOutput|[HistoryOutput](#schemahistoryoutput)|false|none|Output history event|
|»» id|integer|true|none|none|
|»» entity_type|[EntityType](#schemaentitytype)|false|none|An entity type for which activity can occur|
|»» entity_name|string|false|none|none|
|»» node|string|false|none|none|
|»» activity_type|[ActivityType](#schemaactivitytype)|false|none|An activity type|
|»» user|string|false|none|none|
|»» pre|object|true|none|none|
|»» post|object|true|none|none|
|»» details|object|true|none|none|
|»» created_at|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|entity_type|attribute|
|entity_type|availability|
|entity_type|backfill|
|entity_type|catalog|
|entity_type|column_attribute|
|entity_type|dependency|
|entity_type|engine|
|entity_type|link|
|entity_type|materialization|
|entity_type|namespace|
|entity_type|node|
|entity_type|partition|
|entity_type|query|
|entity_type|tag|
|activity_type|create|
|activity_type|delete|
|activity_type|restore|
|activity_type|update|
|activity_type|refresh|
|activity_type|tag|
|activity_type|set_attribute|
|activity_type|status_change|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-cubes">cubes</h1>

## Get A Cube

<a id="opIdGet_a_Cube_cubes__name__get"></a>

`GET /cubes/{name}`

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
  "status": "valid",
  "mode": "published",
  "description": "",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "cube_elements": [
    {
      "name": "string",
      "display_name": "string",
      "node_name": "string",
      "type": "string",
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "cube_node_metrics": [
    "string"
  ],
  "cube_node_dimensions": [
    "string"
  ],
  "query": "string",
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "tags": [
    {
      "description": "string",
      "display_name": "string",
      "tag_metadata": {},
      "name": "string",
      "tag_type": "string"
    }
  ]
}
```

<h3 id="get-a-cube-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[CubeRevisionMetadata](#schemacuberevisionmetadata)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Dimensions Sql For Cube

<a id="opIdDimensions_SQL_for_Cube_cubes__name__dimensions_sql_get"></a>

`GET /cubes/{name}/dimensions/sql`

Generates SQL to retrieve all unique values of a dimension for the cube

<h3 id="dimensions-sql-for-cube-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|dimensions|query|array[string]|false|Dimensions to get values for|
|filters|query|string|false|Filters on dimensional attributes|
|limit|query|integer|false|Number of rows to limit the data retrieved to|
|include_counts|query|boolean|false|none|

> Example responses

> 200 Response

```json
{
  "sql": "string",
  "columns": [
    {
      "name": "string",
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ],
  "dialect": "spark",
  "upstream_tables": [
    "string"
  ]
}
```

<h3 id="dimensions-sql-for-cube-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Dimensions Values For Cube

<a id="opIdDimensions_Values_for_Cube_cubes__name__dimensions_data_get"></a>

`GET /cubes/{name}/dimensions/data`

All unique values of a dimension from the cube

<h3 id="dimensions-values-for-cube-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|dimensions|query|array[string]|false|Dimensions to get values for|
|filters|query|string|false|Filters on dimensional attributes|
|limit|query|integer|false|Number of rows to limit the data retrieved to|
|include_counts|query|boolean|false|none|
|async_|query|boolean|false|none|

> Example responses

> 200 Response

```json
{
  "dimensions": [
    "string"
  ],
  "values": [
    {
      "value": [
        "string"
      ],
      "count": 0
    }
  ],
  "cardinality": 0
}
```

<h3 id="dimensions-values-for-cube-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[DimensionValues](#schemadimensionvalues)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-tags">tags</h1>

## List Tags

<a id="opIdlist_tags_tags_get"></a>

`GET /tags`

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
    "display_name": "string",
    "tag_metadata": {},
    "name": "string",
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

*Response List Tags Tags Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Tags Tags Get|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|» TagOutput|[TagOutput](#schematagoutput)|false|none|Output tag model.|
|»» description|string|false|none|none|
|»» display_name|string|false|none|none|
|»» tag_metadata|object|false|none|none|
|»» name|string|true|none|none|
|»» tag_type|string|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Create A Tag

<a id="opIdcreate_a_tag_tags_post"></a>

`POST /tags`

Create a tag.

> Body parameter

```json
{
  "description": "string",
  "display_name": "string",
  "tag_metadata": {},
  "name": "string",
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
  "display_name": "string",
  "tag_metadata": {},
  "name": "string",
  "tag_type": "string"
}
```

<h3 id="create-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[TagOutput](#schematagoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get A Tag

<a id="opIdget_a_tag_tags__name__get"></a>

`GET /tags/{name}`

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
  "display_name": "string",
  "tag_metadata": {},
  "name": "string",
  "tag_type": "string"
}
```

<h3 id="get-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TagOutput](#schematagoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Update A Tag

<a id="opIdupdate_a_tag_tags__name__patch"></a>

`PATCH /tags/{name}`

Update a tag.

> Body parameter

```json
{
  "description": "string",
  "display_name": "string",
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
  "display_name": "string",
  "tag_metadata": {},
  "name": "string",
  "tag_type": "string"
}
```

<h3 id="update-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TagOutput](#schematagoutput)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## List Nodes For A Tag

<a id="opIdlist_nodes_for_a_tag_tags__name__nodes_get"></a>

`GET /tags/{name}/nodes`

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
  {
    "name": "string",
    "display_name": "string",
    "description": "string",
    "version": "string",
    "type": "source",
    "status": "valid",
    "mode": "published",
    "updated_at": "2019-08-24T14:15:22Z"
  }
]
```

<h3 id="list-nodes-for-a-tag-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-nodes-for-a-tag-responseschema">Response Schema</h3>

Status Code **200**

*Response List Nodes For A Tag Tags  Name  Nodes Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Nodes For A Tag Tags  Name  Nodes Get|[[NodeMinimumDetail](#schemanodeminimumdetail)]|false|none|[List of high level node details]|
|» NodeMinimumDetail|[NodeMinimumDetail](#schemanodeminimumdetail)|false|none|List of high level node details|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» description|string|true|none|none|
|»» version|string|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» updated_at|string(date-time)|true|none|none|

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

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-attributes">attributes</h1>

## List Attributes

<a id="opIdlist_attributes_attributes_get"></a>

`GET /attributes`

List all available attribute types.

> Example responses

> 200 Response

```json
[
  {
    "namespace": "system",
    "name": "string",
    "description": "string",
    "allowed_node_types": [
      "source"
    ],
    "uniqueness_scope": [
      "node"
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

*Response List Attributes Attributes Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Attributes Attributes Get|[[AttributeTypeBase](#schemaattributetypebase)]|false|none|[Base attribute type.]|
|» AttributeTypeBase|[AttributeTypeBase](#schemaattributetypebase)|false|none|Base attribute type.|
|»» namespace|string|false|none|none|
|»» name|string|true|none|none|
|»» description|string|true|none|none|
|»» allowed_node_types|[[NodeType](#schemanodetype)]|true|none|[Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.]|
|»»» NodeType|[NodeType](#schemanodetype)|false|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» uniqueness_scope|[[UniquenessScope](#schemauniquenessscope)]|false|none|[The scope at which this attribute needs to be unique.]|
|»»» UniquenessScope|[UniquenessScope](#schemauniquenessscope)|false|none|The scope at which this attribute needs to be unique.|
|»» id|integer|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|NodeType|source|
|NodeType|transform|
|NodeType|metric|
|NodeType|dimension|
|NodeType|cube|
|UniquenessScope|node|
|UniquenessScope|column_type|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Add An Attribute Type

<a id="opIdAdd_an_Attribute_Type_attributes_post"></a>

`POST /attributes`

Add a new attribute type

> Body parameter

```json
{
  "namespace": "system",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ],
  "uniqueness_scope": [
    "node"
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
  "namespace": "system",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ],
  "uniqueness_scope": [
    "node"
  ],
  "id": 0
}
```

<h3 id="add-an-attribute-type-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Successful Response|[AttributeTypeBase](#schemaattributetypebase)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-sql">sql</h1>

## Get Measures Sql

<a id="opIdGet_Measures_SQL_sql_measures_get"></a>

`GET /sql/measures`

Return the measures SQL for a set of metrics with dimensions and filters.
This SQL can be used to produce an intermediate table with all the measures
and dimensions needed for an analytics database (e.g., Druid).

<h3 id="get-measures-sql-parameters">Parameters</h3>

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
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ],
  "dialect": "spark",
  "upstream_tables": [
    "string"
  ]
}
```

<h3 id="get-measures-sql-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Sql For A Node

<a id="opIdGet_SQL_For_A_Node_sql__node_name__get"></a>

`GET /sql/{node_name}`

Return SQL for a node.

<h3 id="get-sql-for-a-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|orderby|query|array[string]|false|none|
|limit|query|integer|false|none|
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
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ],
  "dialect": "spark",
  "upstream_tables": [
    "string"
  ]
}
```

<h3 id="get-sql-for-a-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Sql For Metrics

<a id="opIdGet_SQL_For_Metrics_sql_get"></a>

`GET /sql`

Return SQL for a set of metrics with dimensions and filters

<h3 id="get-sql-for-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|metrics|query|array[string]|false|none|
|dimensions|query|array[string]|false|none|
|filters|query|array[string]|false|none|
|orderby|query|array[string]|false|none|
|limit|query|integer|false|none|
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
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ],
  "dialect": "spark",
  "upstream_tables": [
    "string"
  ]
}
```

<h3 id="get-sql-for-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[TranslatedSQL](#schematranslatedsql)|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-client">client</h1>

## Client Code For Creating Node

<a id="opIdclient_code_for_creating_node_datajunction_clients_python_new_node__node_name__get"></a>

`GET /datajunction-clients/python/new_node/{node_name}`

Generate the Python client code used for creating this node

<h3 id="client-code-for-creating-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|

> Example responses

> 200 Response

```json
"string"
```

<h3 id="client-code-for-creating-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|string|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Client Code For Adding Materialization

<a id="opIdclient_code_for_adding_materialization_datajunction_clients_python_add_materialization__node_name___materialization_name__get"></a>

`GET /datajunction-clients/python/add_materialization/{node_name}/{materialization_name}`

Generate the Python client code used for adding this materialization

<h3 id="client-code-for-adding-materialization-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|materialization_name|path|string|true|none|

> Example responses

> 200 Response

```json
"string"
```

<h3 id="client-code-for-adding-materialization-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|string|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Client Code For Linking Dimension To Node

<a id="opIdclient_code_for_linking_dimension_to_node_datajunction_clients_python_link_dimension__node_name___column___dimension__get"></a>

`GET /datajunction-clients/python/link_dimension/{node_name}/{column}/{dimension}`

Generate the Python client code used for linking this node's column to a dimension

<h3 id="client-code-for-linking-dimension-to-node-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|node_name|path|string|true|none|
|column|path|string|true|none|
|dimension|path|string|true|none|

> Example responses

> 200 Response

```json
"string"
```

<h3 id="client-code-for-linking-dimension-to-node-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|string|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-dimensions">dimensions</h1>

## List Dimensions

<a id="opIdlist_dimensions_dimensions_get"></a>

`GET /dimensions`

List all available dimensions.

<h3 id="list-dimensions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|prefix|query|string|false|none|

> Example responses

> 200 Response

```json
[
  "string"
]
```

<h3 id="list-dimensions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="list-dimensions-responseschema">Response Schema</h3>

Status Code **200**

*Response List Dimensions Dimensions Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response List Dimensions Dimensions Get|[string]|false|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Find Nodes With Dimension

<a id="opIdfind_nodes_with_dimension_dimensions__name__nodes_get"></a>

`GET /dimensions/{name}/nodes`

List all nodes that have the specified dimension

<h3 id="find-nodes-with-dimension-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|name|path|string|true|none|
|node_type|query|array[string]|false|none|

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
    "id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "name": "string",
      "engines": []
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "min_temporal_partition": [],
      "max_temporal_partition": [],
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "url": "string",
      "categorical_partitions": [],
      "temporal_partitions": [],
      "partitions": []
    },
    "columns": [
      {
        "name": "string",
        "display_name": "string",
        "type": "string",
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
        },
        "partition": {
          "type_": "temporal",
          "format": "string",
          "granularity": "string",
          "expression": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materializations": [
      {
        "name": "string",
        "config": {},
        "schedule": "string",
        "job": "string",
        "backfills": [
          {
            "spec": {
              "column_name": "string",
              "values": [
                null
              ],
              "range": [
                null
              ]
            },
            "urls": [
              "string"
            ]
          }
        ],
        "strategy": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ],
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": {
        "name": "string",
        "label": "string",
        "category": "string",
        "abbreviation": "string",
        "description": "string"
      }
    },
    "dimension_links": [
      {
        "dimension": {
          "name": "string"
        },
        "join_type": "left",
        "join_sql": "string",
        "join_cardinality": "one_to_one",
        "role": "string",
        "foreign_keys": {
          "property1": "string",
          "property2": "string"
        }
      }
    ]
  }
]
```

<h3 id="find-nodes-with-dimension-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="find-nodes-with-dimension-responseschema">Response Schema</h3>

Status Code **200**

*Response Find Nodes With Dimension Dimensions  Name  Nodes Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Find Nodes With Dimension Dimensions  Name  Nodes Get|[[NodeRevisionOutput](#schemanoderevisionoutput)]|false|none|[Output for a node revision with information about columns and if it is a metric.]|
|» NodeRevisionOutput|[NodeRevisionOutput](#schemanoderevisionoutput)|false|none|Output for a node revision with information about columns and if it is a metric.|
|»» id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»»» name|string|true|none|none|
|»»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» query|string|false|none|none|
|»» availability|[AvailabilityStateBase](#schemaavailabilitystatebase)|false|none|An availability state base|
|»»» min_temporal_partition|[string]|false|none|none|
|»»» max_temporal_partition|[string]|false|none|none|
|»»» catalog|string|true|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|true|none|none|
|»»» valid_through_ts|integer|true|none|none|
|»»» url|string|false|none|none|
|»»» categorical_partitions|[string]|false|none|none|
|»»» temporal_partitions|[string]|false|none|none|
|»»» partitions|[[PartitionAvailability](#schemapartitionavailability)]|false|none|[Partition-level availability]|
|»»»» PartitionAvailability|[PartitionAvailability](#schemapartitionavailability)|false|none|Partition-level availability|
|»»»»» min_temporal_partition|[string]|false|none|none|
|»»»»» max_temporal_partition|[string]|false|none|none|
|»»»»» value|[string]|true|none|none|
|»»»»» valid_through_ts|integer|false|none|none|
|»» columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» type|string|true|none|none|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»»»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»»»» format|string|false|none|none|
|»»»»» granularity|string|false|none|none|
|»»»»» expression|string|false|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» materializations|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»» name|string|false|none|none|
|»»»» config|object|true|none|none|
|»»»» schedule|string|true|none|none|
|»»»» job|string|false|none|none|
|»»»» backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|»»»»» BackfillOutput|[BackfillOutput](#schemabackfilloutput)|false|none|Output model for backfills|
|»»»»»» spec|[PartitionBackfill](#schemapartitionbackfill)|false|none|Used for setting backfilled values|
|»»»»»»» column_name|string|true|none|none|
|»»»»»»» values|[any]|false|none|none|
|»»»»»»» range|[any]|false|none|none|
|»»»»»» urls|[string]|false|none|none|
|»»»» strategy|string|false|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» metric_metadata|[MetricMetadataOutput](#schemametricmetadataoutput)|false|none|Metric metadata output|
|»»» direction|[MetricDirection](#schemametricdirection)|false|none|The direction of the metric that's considered good, i.e., higher is better|
|»»» unit|[Unit](#schemaunit)|false|none|Metric unit|
|»»»» name|string|true|none|none|
|»»»» label|string|false|none|none|
|»»»» category|string|false|none|none|
|»»»» abbreviation|string|false|none|none|
|»»»» description|string|false|none|none|
|»» dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|false|none|[Input for linking a dimension to a node]|
|»»» LinkDimensionOutput|[LinkDimensionOutput](#schemalinkdimensionoutput)|false|none|Input for linking a dimension to a node|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|»»»» join_type|[JoinType](#schemajointype)|true|none|Join type|
|»»»» join_sql|string|true|none|none|
|»»»» join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|»»»» role|string|false|none|none|
|»»»» foreign_keys|object|true|none|none|
|»»»»» **additionalProperties**|string|false|none|none|

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
|type_|temporal|
|type_|categorical|
|direction|higher_is_better|
|direction|lower_is_better|
|direction|neutral|
|join_type|left|
|join_type|right|
|join_type|inner|
|join_type|full|
|join_type|cross|
|join_cardinality|one_to_one|
|join_cardinality|one_to_many|
|join_cardinality|many_to_one|
|join_cardinality|many_to_many|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Find Nodes With Common Dimensions

<a id="opIdfind_nodes_with_common_dimensions_dimensions_common_get"></a>

`GET /dimensions/common`

Find all nodes that have the list of common dimensions

<h3 id="find-nodes-with-common-dimensions-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|dimension|query|array[string]|false|none|
|node_type|query|array[string]|false|none|

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
    "id": 0,
    "node_id": 0,
    "type": "source",
    "name": "string",
    "display_name": "string",
    "version": "string",
    "status": "valid",
    "mode": "published",
    "catalog": {
      "name": "string",
      "engines": []
    },
    "schema_": "string",
    "table": "string",
    "description": "",
    "query": "string",
    "availability": {
      "min_temporal_partition": [],
      "max_temporal_partition": [],
      "catalog": "string",
      "schema_": "string",
      "table": "string",
      "valid_through_ts": 0,
      "url": "string",
      "categorical_partitions": [],
      "temporal_partitions": [],
      "partitions": []
    },
    "columns": [
      {
        "name": "string",
        "display_name": "string",
        "type": "string",
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
        },
        "partition": {
          "type_": "temporal",
          "format": "string",
          "granularity": "string",
          "expression": "string"
        }
      }
    ],
    "updated_at": "2019-08-24T14:15:22Z",
    "materializations": [
      {
        "name": "string",
        "config": {},
        "schedule": "string",
        "job": "string",
        "backfills": [
          {
            "spec": {
              "column_name": "string",
              "values": [
                null
              ],
              "range": [
                null
              ]
            },
            "urls": [
              "string"
            ]
          }
        ],
        "strategy": "string"
      }
    ],
    "parents": [
      {
        "name": "string"
      }
    ],
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": {
        "name": "string",
        "label": "string",
        "category": "string",
        "abbreviation": "string",
        "description": "string"
      }
    },
    "dimension_links": [
      {
        "dimension": {
          "name": "string"
        },
        "join_type": "left",
        "join_sql": "string",
        "join_cardinality": "one_to_one",
        "role": "string",
        "foreign_keys": {
          "property1": "string",
          "property2": "string"
        }
      }
    ]
  }
]
```

<h3 id="find-nodes-with-common-dimensions-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="find-nodes-with-common-dimensions-responseschema">Response Schema</h3>

Status Code **200**

*Response Find Nodes With Common Dimensions Dimensions Common Get*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Response Find Nodes With Common Dimensions Dimensions Common Get|[[NodeRevisionOutput](#schemanoderevisionoutput)]|false|none|[Output for a node revision with information about columns and if it is a metric.]|
|» NodeRevisionOutput|[NodeRevisionOutput](#schemanoderevisionoutput)|false|none|Output for a node revision with information about columns and if it is a metric.|
|»» id|integer|true|none|none|
|»» node_id|integer|true|none|none|
|»» type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|»» name|string|true|none|none|
|»» display_name|string|true|none|none|
|»» version|string|true|none|none|
|»» status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|»» mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|»» catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|»»» name|string|true|none|none|
|»»» engines|[[EngineInfo](#schemaengineinfo)]|false|none|[Class for engine creation]|
|»»»» EngineInfo|[EngineInfo](#schemaengineinfo)|false|none|Class for engine creation|
|»»»»» name|string|true|none|none|
|»»»»» version|string|true|none|none|
|»»»»» uri|string|false|none|none|
|»»»»» dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|»» schema_|string|false|none|none|
|»» table|string|false|none|none|
|»» description|string|false|none|none|
|»» query|string|false|none|none|
|»» availability|[AvailabilityStateBase](#schemaavailabilitystatebase)|false|none|An availability state base|
|»»» min_temporal_partition|[string]|false|none|none|
|»»» max_temporal_partition|[string]|false|none|none|
|»»» catalog|string|true|none|none|
|»»» schema_|string|false|none|none|
|»»» table|string|true|none|none|
|»»» valid_through_ts|integer|true|none|none|
|»»» url|string|false|none|none|
|»»» categorical_partitions|[string]|false|none|none|
|»»» temporal_partitions|[string]|false|none|none|
|»»» partitions|[[PartitionAvailability](#schemapartitionavailability)]|false|none|[Partition-level availability]|
|»»»» PartitionAvailability|[PartitionAvailability](#schemapartitionavailability)|false|none|Partition-level availability|
|»»»»» min_temporal_partition|[string]|false|none|none|
|»»»»» max_temporal_partition|[string]|false|none|none|
|»»»»» value|[string]|true|none|none|
|»»»»» valid_through_ts|integer|false|none|none|
|»» columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|»»» ColumnOutput|[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)|false|none|A simplified column schema, without ID or dimensions.|
|»»»» name|string|true|none|none|
|»»»» display_name|string|false|none|none|
|»»»» type|string|true|none|none|
|»»»» attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|»»»»» AttributeOutput|[AttributeOutput](#schemaattributeoutput)|false|none|Column attribute output.|
|»»»»»» attribute_type|[AttributeTypeName](#schemaattributetypename)|true|none|Attribute type name.|
|»»»»»»» namespace|string|true|none|none|
|»»»»»»» name|string|true|none|none|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»»»»» name|string|true|none|none|
|»»»» partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|
|»»»»» type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|»»»»» format|string|false|none|none|
|»»»»» granularity|string|false|none|none|
|»»»»» expression|string|false|none|none|
|»» updated_at|string(date-time)|true|none|none|
|»» materializations|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|»»» MaterializationConfigOutput|[MaterializationConfigOutput](#schemamaterializationconfigoutput)|false|none|Output for materialization config.|
|»»»» name|string|false|none|none|
|»»»» config|object|true|none|none|
|»»»» schedule|string|true|none|none|
|»»»» job|string|false|none|none|
|»»»» backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|»»»»» BackfillOutput|[BackfillOutput](#schemabackfilloutput)|false|none|Output model for backfills|
|»»»»»» spec|[PartitionBackfill](#schemapartitionbackfill)|false|none|Used for setting backfilled values|
|»»»»»»» column_name|string|true|none|none|
|»»»»»»» values|[any]|false|none|none|
|»»»»»»» range|[any]|false|none|none|
|»»»»»» urls|[string]|false|none|none|
|»»»» strategy|string|false|none|none|
|»» parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|»»» NodeNameOutput|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|»» metric_metadata|[MetricMetadataOutput](#schemametricmetadataoutput)|false|none|Metric metadata output|
|»»» direction|[MetricDirection](#schemametricdirection)|false|none|The direction of the metric that's considered good, i.e., higher is better|
|»»» unit|[Unit](#schemaunit)|false|none|Metric unit|
|»»»» name|string|true|none|none|
|»»»» label|string|false|none|none|
|»»»» category|string|false|none|none|
|»»»» abbreviation|string|false|none|none|
|»»»» description|string|false|none|none|
|»» dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|false|none|[Input for linking a dimension to a node]|
|»»» LinkDimensionOutput|[LinkDimensionOutput](#schemalinkdimensionoutput)|false|none|Input for linking a dimension to a node|
|»»»» dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|»»»» join_type|[JoinType](#schemajointype)|true|none|Join type|
|»»»» join_sql|string|true|none|none|
|»»»» join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|»»»» role|string|false|none|none|
|»»»» foreign_keys|object|true|none|none|
|»»»»» **additionalProperties**|string|false|none|none|

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
|type_|temporal|
|type_|categorical|
|direction|higher_is_better|
|direction|lower_is_better|
|direction|neutral|
|join_type|left|
|join_type|right|
|join_type|inner|
|join_type|full|
|join_type|cross|
|join_cardinality|one_to_one|
|join_cardinality|one_to_many|
|join_cardinality|many_to_one|
|join_cardinality|many_to_many|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-who-am-i-">Who am I?</h1>

## Get User

<a id="opIdget_user_whoami_get"></a>

`GET /whoami`

Returns the current authenticated user

> Example responses

> 200 Response

```json
{
  "id": 0,
  "username": "string",
  "email": "string",
  "name": "string",
  "oauth_provider": "basic",
  "is_admin": false
}
```

<h3 id="get-user-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|[UserOutput](#schemauseroutput)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

## Get Short Lived Token

<a id="opIdget_short_lived_token_token_get"></a>

`GET /token`

Returns a token that expires in 24 hours

> Example responses

> 200 Response

```json
null
```

<h3 id="get-short-lived-token-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="get-short-lived-token-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
DJHTTPBearer
</aside>

<h1 id="dj-server-basic-oauth2">Basic OAuth2</h1>

## Create A User

<a id="opIdcreate_a_user_basic_user__post"></a>

`POST /basic/user/`

Create a new user

> Body parameter

```yaml
email: string
username: string
password: string

```

<h3 id="create-a-user-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[Body_create_a_user_basic_user__post](#schemabody_create_a_user_basic_user__post)|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="create-a-user-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="create-a-user-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Login

<a id="opIdlogin_basic_login__post"></a>

`POST /basic/login/`

Get a JWT token and set it as an HTTP only cookie

> Body parameter

```yaml
grant_type: string
username: string
password: string
scope: ""
client_id: string
client_secret: string

```

<h3 id="login-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[Body_login_basic_login__post](#schemabody_login_basic_login__post)|true|none|

> Example responses

> 200 Response

```json
null
```

<h3 id="login-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|
|422|[Unprocessable Entity](https://tools.ietf.org/html/rfc2518#section-10.3)|Validation Error|[HTTPValidationError](#schemahttpvalidationerror)|

<h3 id="login-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

## Logout

<a id="opIdlogout_logout__post"></a>

`POST /logout/`

Logout a user by deleting the auth cookie

> Example responses

> 200 Response

```json
null
```

<h3 id="logout-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Successful Response|Inline|

<h3 id="logout-responseschema">Response Schema</h3>

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_ActivityType">ActivityType</h2>
<!-- backwards compatibility -->
<a id="schemaactivitytype"></a>
<a id="schema_ActivityType"></a>
<a id="tocSactivitytype"></a>
<a id="tocsactivitytype"></a>

```json
"create"

```

ActivityType

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ActivityType|string|false|none|An activity type|

#### Enumerated Values

|Property|Value|
|---|---|
|ActivityType|create|
|ActivityType|delete|
|ActivityType|restore|
|ActivityType|update|
|ActivityType|refresh|
|ActivityType|tag|
|ActivityType|set_attribute|
|ActivityType|status_change|

<h2 id="tocS_AggregationRule">AggregationRule</h2>
<!-- backwards compatibility -->
<a id="schemaaggregationrule"></a>
<a id="schema_AggregationRule"></a>
<a id="tocSaggregationrule"></a>
<a id="tocsaggregationrule"></a>

```json
"additive"

```

AggregationRule

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|AggregationRule|string|false|none|Type of allowed aggregation for a given measure.|

#### Enumerated Values

|Property|Value|
|---|---|
|AggregationRule|additive|
|AggregationRule|non-additive|
|AggregationRule|semi-additive|

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

<h2 id="tocS_AttributeTypeBase">AttributeTypeBase</h2>
<!-- backwards compatibility -->
<a id="schemaattributetypebase"></a>
<a id="schema_AttributeTypeBase"></a>
<a id="tocSattributetypebase"></a>
<a id="tocsattributetypebase"></a>

```json
{
  "namespace": "system",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ],
  "uniqueness_scope": [
    "node"
  ],
  "id": 0
}

```

AttributeTypeBase

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|false|none|none|
|name|string|true|none|none|
|description|string|true|none|none|
|allowed_node_types|[[NodeType](#schemanodetype)]|true|none|[Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.]|
|uniqueness_scope|[[UniquenessScope](#schemauniquenessscope)]|false|none|[The scope at which this attribute needs to be unique.]|
|id|integer|true|none|none|

<h2 id="tocS_AttributeTypeIdentifier">AttributeTypeIdentifier</h2>
<!-- backwards compatibility -->
<a id="schemaattributetypeidentifier"></a>
<a id="schema_AttributeTypeIdentifier"></a>
<a id="tocSattributetypeidentifier"></a>
<a id="tocsattributetypeidentifier"></a>

```json
{
  "namespace": "system",
  "name": "string"
}

```

AttributeTypeIdentifier

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|false|none|none|
|name|string|true|none|none|

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

<h2 id="tocS_AvailabilityStateBase">AvailabilityStateBase</h2>
<!-- backwards compatibility -->
<a id="schemaavailabilitystatebase"></a>
<a id="schema_AvailabilityStateBase"></a>
<a id="tocSavailabilitystatebase"></a>
<a id="tocsavailabilitystatebase"></a>

```json
{
  "min_temporal_partition": [],
  "max_temporal_partition": [],
  "catalog": "string",
  "schema_": "string",
  "table": "string",
  "valid_through_ts": 0,
  "url": "string",
  "categorical_partitions": [],
  "temporal_partitions": [],
  "partitions": []
}

```

AvailabilityStateBase

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|min_temporal_partition|[string]|false|none|none|
|max_temporal_partition|[string]|false|none|none|
|catalog|string|true|none|none|
|schema_|string|false|none|none|
|table|string|true|none|none|
|valid_through_ts|integer|true|none|none|
|url|string|false|none|none|
|categorical_partitions|[string]|false|none|none|
|temporal_partitions|[string]|false|none|none|
|partitions|[[PartitionAvailability](#schemapartitionavailability)]|false|none|[Partition-level availability]|

<h2 id="tocS_BackfillOutput">BackfillOutput</h2>
<!-- backwards compatibility -->
<a id="schemabackfilloutput"></a>
<a id="schema_BackfillOutput"></a>
<a id="tocSbackfilloutput"></a>
<a id="tocsbackfilloutput"></a>

```json
{
  "spec": {
    "column_name": "string",
    "values": [
      null
    ],
    "range": [
      null
    ]
  },
  "urls": [
    "string"
  ]
}

```

BackfillOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|spec|[PartitionBackfill](#schemapartitionbackfill)|false|none|Used for setting backfilled values|
|urls|[string]|false|none|none|

<h2 id="tocS_Body_create_a_user_basic_user__post">Body_create_a_user_basic_user__post</h2>
<!-- backwards compatibility -->
<a id="schemabody_create_a_user_basic_user__post"></a>
<a id="schema_Body_create_a_user_basic_user__post"></a>
<a id="tocSbody_create_a_user_basic_user__post"></a>
<a id="tocsbody_create_a_user_basic_user__post"></a>

```json
{
  "email": "string",
  "username": "string",
  "password": "string"
}

```

Body_create_a_user_basic_user__post

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|email|string|true|none|none|
|username|string|true|none|none|
|password|string|true|none|none|

<h2 id="tocS_Body_login_basic_login__post">Body_login_basic_login__post</h2>
<!-- backwards compatibility -->
<a id="schemabody_login_basic_login__post"></a>
<a id="schema_Body_login_basic_login__post"></a>
<a id="tocSbody_login_basic_login__post"></a>
<a id="tocsbody_login_basic_login__post"></a>

```json
{
  "grant_type": "string",
  "username": "string",
  "password": "string",
  "scope": "",
  "client_id": "string",
  "client_secret": "string"
}

```

Body_login_basic_login__post

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|grant_type|string|false|none|none|
|username|string|true|none|none|
|password|string|true|none|none|
|scope|string|false|none|none|
|client_id|string|false|none|none|
|client_secret|string|false|none|none|

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

<h2 id="tocS_ColumnMetadata">ColumnMetadata</h2>
<!-- backwards compatibility -->
<a id="schemacolumnmetadata"></a>
<a id="schema_ColumnMetadata"></a>
<a id="tocScolumnmetadata"></a>
<a id="tocscolumnmetadata"></a>

```json
{
  "name": "string",
  "type": "string",
  "column": "string",
  "node": "string",
  "semantic_entity": "string",
  "semantic_type": "string"
}

```

ColumnMetadata

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|type|string|true|none|none|
|column|string|false|none|none|
|node|string|false|none|none|
|semantic_entity|string|false|none|none|
|semantic_type|string|false|none|none|

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
  "metrics": [
    "string"
  ],
  "dimensions": [
    "string"
  ],
  "filters": [
    "string"
  ],
  "orderby": [
    "string"
  ],
  "limit": 0,
  "description": "string",
  "mode": "published",
  "display_name": "string",
  "primary_key": [
    "string"
  ],
  "name": "string",
  "namespace": "default"
}

```

CreateCubeNode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|metrics|[string]|true|none|none|
|dimensions|[string]|true|none|none|
|filters|[string]|false|none|none|
|orderby|[string]|false|none|none|
|limit|integer|false|none|none|
|description|string|true|none|none|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|display_name|string|false|none|none|
|primary_key|[string]|false|none|none|
|name|string|true|none|none|
|namespace|string|false|none|none|

<h2 id="tocS_CreateMeasure">CreateMeasure</h2>
<!-- backwards compatibility -->
<a id="schemacreatemeasure"></a>
<a id="schema_CreateMeasure"></a>
<a id="tocScreatemeasure"></a>
<a id="tocscreatemeasure"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "node": "string",
      "column": "string"
    }
  ],
  "additive": "non-additive"
}

```

CreateMeasure

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|false|none|none|
|description|string|false|none|none|
|columns|[[NodeColumn](#schemanodecolumn)]|true|none|[Defines a column on a node]|
|additive|[AggregationRule](#schemaaggregationrule)|false|none|Type of allowed aggregation for a given measure.|

<h2 id="tocS_CreateNode">CreateNode</h2>
<!-- backwards compatibility -->
<a id="schemacreatenode"></a>
<a id="schema_CreateNode"></a>
<a id="tocScreatenode"></a>
<a id="tocscreatenode"></a>

```json
{
  "required_dimensions": [
    "string"
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "string"
  },
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
|required_dimensions|[string]|false|none|none|
|metric_metadata|[MetricMetadataInput](#schemametricmetadatainput)|false|none|Metric metadata output|
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
  "missing_table": false,
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
|columns|[[SourceColumnOutput](#schemasourcecolumnoutput)]|true|none|[A column used in creation of a source node]|
|missing_table|boolean|false|none|none|
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
  "display_name": "string",
  "tag_metadata": {},
  "name": "string",
  "tag_type": "string"
}

```

CreateTag

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|false|none|none|
|display_name|string|false|none|none|
|tag_metadata|object|false|none|none|
|name|string|true|none|none|
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
  "display_name": "string",
  "node_name": "string",
  "type": "string",
  "partition": {
    "type_": "temporal",
    "format": "string",
    "granularity": "string",
    "expression": "string"
  }
}

```

CubeElementMetadata

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|true|none|none|
|node_name|string|true|none|none|
|type|string|true|none|none|
|partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|

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
  "status": "valid",
  "mode": "published",
  "description": "",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "cube_elements": [
    {
      "name": "string",
      "display_name": "string",
      "node_name": "string",
      "type": "string",
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "cube_node_metrics": [
    "string"
  ],
  "cube_node_dimensions": [
    "string"
  ],
  "query": "string",
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "tags": [
    {
      "description": "string",
      "display_name": "string",
      "tag_metadata": {},
      "name": "string",
      "tag_type": "string"
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
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|description|string|false|none|none|
|availability|[AvailabilityStateBase](#schemaavailabilitystatebase)|false|none|An availability state base|
|cube_elements|[[CubeElementMetadata](#schemacubeelementmetadata)]|true|none|[Metadata for an element in a cube]|
|cube_node_metrics|[string]|true|none|none|
|cube_node_dimensions|[string]|true|none|none|
|query|string|false|none|none|
|columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|updated_at|string(date-time)|true|none|none|
|materializations|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|tags|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|

<h2 id="tocS_DAGNodeOutput">DAGNodeOutput</h2>
<!-- backwards compatibility -->
<a id="schemadagnodeoutput"></a>
<a id="schema_DAGNodeOutput"></a>
<a id="tocSdagnodeoutput"></a>
<a id="tocsdagnodeoutput"></a>

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "parents": [
    {
      "name": "string"
    }
  ],
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string"
}

```

DAGNodeOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|
|node_revision_id|integer|true|none|none|
|node_id|integer|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|name|string|true|none|none|
|display_name|string|true|none|none|
|version|string|true|none|none|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|schema_|string|false|none|none|
|table|string|false|none|none|
|description|string|false|none|none|
|columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|updated_at|string(date-time)|true|none|none|
|parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|true|none|[Input for linking a dimension to a node]|
|created_at|string(date-time)|true|none|none|
|tags|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|current_version|string|true|none|none|

<h2 id="tocS_DJError">DJError</h2>
<!-- backwards compatibility -->
<a id="schemadjerror"></a>
<a id="schema_DJError"></a>
<a id="tocSdjerror"></a>
<a id="tocsdjerror"></a>

```json
{
  "code": 0,
  "message": "string",
  "debug": {},
  "context": ""
}

```

DJError

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|code|[ErrorCode](#schemaerrorcode)|true|none|Error codes.|
|message|string|true|none|none|
|debug|object|false|none|none|
|context|string|false|none|none|

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

<h2 id="tocS_DimensionAttributeOutput">DimensionAttributeOutput</h2>
<!-- backwards compatibility -->
<a id="schemadimensionattributeoutput"></a>
<a id="schema_DimensionAttributeOutput"></a>
<a id="tocSdimensionattributeoutput"></a>
<a id="tocsdimensionattributeoutput"></a>

```json
{
  "name": "string",
  "node_name": "string",
  "node_display_name": "string",
  "is_primary_key": true,
  "type": "string",
  "path": [
    "string"
  ]
}

```

DimensionAttributeOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|node_name|string|false|none|none|
|node_display_name|string|false|none|none|
|is_primary_key|boolean|true|none|none|
|type|string|true|none|none|
|path|[string]|true|none|none|

<h2 id="tocS_DimensionValue">DimensionValue</h2>
<!-- backwards compatibility -->
<a id="schemadimensionvalue"></a>
<a id="schema_DimensionValue"></a>
<a id="tocSdimensionvalue"></a>
<a id="tocsdimensionvalue"></a>

```json
{
  "value": [
    "string"
  ],
  "count": 0
}

```

DimensionValue

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|value|[string]|true|none|none|
|count|integer|false|none|none|

<h2 id="tocS_DimensionValues">DimensionValues</h2>
<!-- backwards compatibility -->
<a id="schemadimensionvalues"></a>
<a id="schema_DimensionValues"></a>
<a id="tocSdimensionvalues"></a>
<a id="tocsdimensionvalues"></a>

```json
{
  "dimensions": [
    "string"
  ],
  "values": [
    {
      "value": [
        "string"
      ],
      "count": 0
    }
  ],
  "cardinality": 0
}

```

DimensionValues

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|dimensions|[string]|true|none|none|
|values|[[DimensionValue](#schemadimensionvalue)]|true|none|[Dimension value and count]|
|cardinality|integer|true|none|none|

<h2 id="tocS_DruidConf">DruidConf</h2>
<!-- backwards compatibility -->
<a id="schemadruidconf"></a>
<a id="schema_DruidConf"></a>
<a id="tocSdruidconf"></a>
<a id="tocsdruidconf"></a>

```json
{
  "granularity": "string",
  "intervals": [
    "string"
  ],
  "timestamp_column": "string",
  "timestamp_format": "string",
  "parse_spec_format": "string"
}

```

DruidConf

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|granularity|string|false|none|none|
|intervals|[string]|false|none|none|
|timestamp_column|string|false|none|none|
|timestamp_format|string|false|none|none|
|parse_spec_format|string|false|none|none|

<h2 id="tocS_DruidCubeConfigInput">DruidCubeConfigInput</h2>
<!-- backwards compatibility -->
<a id="schemadruidcubeconfiginput"></a>
<a id="schema_DruidCubeConfigInput"></a>
<a id="tocSdruidcubeconfiginput"></a>
<a id="tocsdruidcubeconfiginput"></a>

```json
{
  "spark": {},
  "lookback_window": "string",
  "dimensions": [
    "string"
  ],
  "measures": {
    "property1": {
      "metric": "string",
      "measures": [
        {
          "name": "string",
          "field_name": "string",
          "agg": "string",
          "type": "string"
        }
      ],
      "combiner": "string"
    },
    "property2": {
      "metric": "string",
      "measures": [
        {
          "name": "string",
          "field_name": "string",
          "agg": "string",
          "type": "string"
        }
      ],
      "combiner": "string"
    }
  },
  "metrics": [
    {
      "name": "string",
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ],
  "prefix": "",
  "suffix": "",
  "druid": {
    "granularity": "string",
    "intervals": [
      "string"
    ],
    "timestamp_column": "string",
    "timestamp_format": "string",
    "parse_spec_format": "string"
  }
}

```

DruidCubeConfigInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|spark|[SparkConf](#schemasparkconf)|false|none|Spark configuration|
|lookback_window|string|false|none|none|
|dimensions|[string]|false|none|none|
|measures|object|false|none|none|
|» **additionalProperties**|[MetricMeasures](#schemametricmeasures)|false|none|Represent a metric as a set of measures, along with the expression for<br>combining the measures to make the metric.|
|metrics|[[ColumnMetadata](#schemacolumnmetadata)]|false|none|[A simple model for column metadata.]|
|prefix|string|false|none|none|
|suffix|string|false|none|none|
|druid|[DruidConf](#schemadruidconf)|false|none|Druid configuration|

<h2 id="tocS_EditMeasure">EditMeasure</h2>
<!-- backwards compatibility -->
<a id="schemaeditmeasure"></a>
<a id="schema_EditMeasure"></a>
<a id="tocSeditmeasure"></a>
<a id="tocseditmeasure"></a>

```json
{
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "node": "string",
      "column": "string"
    }
  ],
  "additive": "additive"
}

```

EditMeasure

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|display_name|string|false|none|none|
|description|string|false|none|none|
|columns|[[NodeColumn](#schemanodecolumn)]|false|none|[Defines a column on a node]|
|additive|[AggregationRule](#schemaaggregationrule)|false|none|Type of allowed aggregation for a given measure.|

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

<h2 id="tocS_EntityType">EntityType</h2>
<!-- backwards compatibility -->
<a id="schemaentitytype"></a>
<a id="schema_EntityType"></a>
<a id="tocSentitytype"></a>
<a id="tocsentitytype"></a>

```json
"attribute"

```

EntityType

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|EntityType|string|false|none|An entity type for which activity can occur|

#### Enumerated Values

|Property|Value|
|---|---|
|EntityType|attribute|
|EntityType|availability|
|EntityType|backfill|
|EntityType|catalog|
|EntityType|column_attribute|
|EntityType|dependency|
|EntityType|engine|
|EntityType|link|
|EntityType|materialization|
|EntityType|namespace|
|EntityType|node|
|EntityType|partition|
|EntityType|query|
|EntityType|tag|

<h2 id="tocS_ErrorCode">ErrorCode</h2>
<!-- backwards compatibility -->
<a id="schemaerrorcode"></a>
<a id="schema_ErrorCode"></a>
<a id="tocSerrorcode"></a>
<a id="tocserrorcode"></a>

```json
0

```

ErrorCode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ErrorCode|integer|false|none|Error codes.|

#### Enumerated Values

|Property|Value|
|---|---|
|ErrorCode|0|
|ErrorCode|1|
|ErrorCode|2|
|ErrorCode|100|
|ErrorCode|101|
|ErrorCode|102|
|ErrorCode|200|
|ErrorCode|201|
|ErrorCode|202|
|ErrorCode|203|
|ErrorCode|204|
|ErrorCode|205|
|ErrorCode|206|
|ErrorCode|300|
|ErrorCode|301|
|ErrorCode|302|
|ErrorCode|400|
|ErrorCode|401|
|ErrorCode|402|
|ErrorCode|403|
|ErrorCode|500|
|ErrorCode|501|

<h2 id="tocS_GenericCubeConfigInput">GenericCubeConfigInput</h2>
<!-- backwards compatibility -->
<a id="schemagenericcubeconfiginput"></a>
<a id="schema_GenericCubeConfigInput"></a>
<a id="tocSgenericcubeconfiginput"></a>
<a id="tocsgenericcubeconfiginput"></a>

```json
{
  "spark": {},
  "lookback_window": "string",
  "dimensions": [
    "string"
  ],
  "measures": {
    "property1": {
      "metric": "string",
      "measures": [
        {
          "name": "string",
          "field_name": "string",
          "agg": "string",
          "type": "string"
        }
      ],
      "combiner": "string"
    },
    "property2": {
      "metric": "string",
      "measures": [
        {
          "name": "string",
          "field_name": "string",
          "agg": "string",
          "type": "string"
        }
      ],
      "combiner": "string"
    }
  },
  "metrics": [
    {
      "name": "string",
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ]
}

```

GenericCubeConfigInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|spark|[SparkConf](#schemasparkconf)|false|none|Spark configuration|
|lookback_window|string|false|none|none|
|dimensions|[string]|false|none|none|
|measures|object|false|none|none|
|» **additionalProperties**|[MetricMeasures](#schemametricmeasures)|false|none|Represent a metric as a set of measures, along with the expression for<br>combining the measures to make the metric.|
|metrics|[[ColumnMetadata](#schemacolumnmetadata)]|false|none|[A simple model for column metadata.]|

<h2 id="tocS_GenericMaterializationConfigInput">GenericMaterializationConfigInput</h2>
<!-- backwards compatibility -->
<a id="schemagenericmaterializationconfiginput"></a>
<a id="schema_GenericMaterializationConfigInput"></a>
<a id="tocSgenericmaterializationconfiginput"></a>
<a id="tocsgenericmaterializationconfiginput"></a>

```json
{
  "spark": {},
  "lookback_window": "string"
}

```

GenericMaterializationConfigInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|spark|[SparkConf](#schemasparkconf)|false|none|Spark configuration|
|lookback_window|string|false|none|none|

<h2 id="tocS_Granularity">Granularity</h2>
<!-- backwards compatibility -->
<a id="schemagranularity"></a>
<a id="schema_Granularity"></a>
<a id="tocSgranularity"></a>
<a id="tocsgranularity"></a>

```json
"second"

```

Granularity

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|Granularity|string|false|none|Time dimension granularity.|

#### Enumerated Values

|Property|Value|
|---|---|
|Granularity|second|
|Granularity|minute|
|Granularity|hour|
|Granularity|day|
|Granularity|week|
|Granularity|month|
|Granularity|quarter|
|Granularity|year|

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

<h2 id="tocS_HistoryOutput">HistoryOutput</h2>
<!-- backwards compatibility -->
<a id="schemahistoryoutput"></a>
<a id="schema_HistoryOutput"></a>
<a id="tocShistoryoutput"></a>
<a id="tocshistoryoutput"></a>

```json
{
  "id": 0,
  "entity_type": "attribute",
  "entity_name": "string",
  "node": "string",
  "activity_type": "create",
  "user": "string",
  "pre": {},
  "post": {},
  "details": {},
  "created_at": "2019-08-24T14:15:22Z"
}

```

HistoryOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|true|none|none|
|entity_type|[EntityType](#schemaentitytype)|false|none|An entity type for which activity can occur|
|entity_name|string|false|none|none|
|node|string|false|none|none|
|activity_type|[ActivityType](#schemaactivitytype)|false|none|An activity type|
|user|string|false|none|none|
|pre|object|true|none|none|
|post|object|true|none|none|
|details|object|true|none|none|
|created_at|string(date-time)|true|none|none|

<h2 id="tocS_JoinCardinality">JoinCardinality</h2>
<!-- backwards compatibility -->
<a id="schemajoincardinality"></a>
<a id="schema_JoinCardinality"></a>
<a id="tocSjoincardinality"></a>
<a id="tocsjoincardinality"></a>

```json
"one_to_one"

```

JoinCardinality

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|JoinCardinality|string|false|none|The version upgrade type|

#### Enumerated Values

|Property|Value|
|---|---|
|JoinCardinality|one_to_one|
|JoinCardinality|one_to_many|
|JoinCardinality|many_to_one|
|JoinCardinality|many_to_many|

<h2 id="tocS_JoinType">JoinType</h2>
<!-- backwards compatibility -->
<a id="schemajointype"></a>
<a id="schema_JoinType"></a>
<a id="tocSjointype"></a>
<a id="tocsjointype"></a>

```json
"left"

```

JoinType

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|JoinType|string|false|none|Join type|

#### Enumerated Values

|Property|Value|
|---|---|
|JoinType|left|
|JoinType|right|
|JoinType|inner|
|JoinType|full|
|JoinType|cross|

<h2 id="tocS_LineageColumn">LineageColumn</h2>
<!-- backwards compatibility -->
<a id="schemalineagecolumn"></a>
<a id="schema_LineageColumn"></a>
<a id="tocSlineagecolumn"></a>
<a id="tocslineagecolumn"></a>

```json
{
  "column_name": "string",
  "node_name": "string",
  "node_type": "string",
  "display_name": "string",
  "lineage": [
    {
      "column_name": "string",
      "node_name": "string",
      "node_type": "string",
      "display_name": "string",
      "lineage": []
    }
  ]
}

```

LineageColumn

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|column_name|string|true|none|none|
|node_name|string|false|none|none|
|node_type|string|false|none|none|
|display_name|string|false|none|none|
|lineage|[[LineageColumn](#schemalineagecolumn)]|false|none|[Column in lineage graph]|

<h2 id="tocS_LinkDimensionIdentifier">LinkDimensionIdentifier</h2>
<!-- backwards compatibility -->
<a id="schemalinkdimensionidentifier"></a>
<a id="schema_LinkDimensionIdentifier"></a>
<a id="tocSlinkdimensionidentifier"></a>
<a id="tocslinkdimensionidentifier"></a>

```json
{
  "dimension_node": "string",
  "role": "string"
}

```

LinkDimensionIdentifier

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|dimension_node|string|true|none|none|
|role|string|false|none|none|

<h2 id="tocS_LinkDimensionInput">LinkDimensionInput</h2>
<!-- backwards compatibility -->
<a id="schemalinkdimensioninput"></a>
<a id="schema_LinkDimensionInput"></a>
<a id="tocSlinkdimensioninput"></a>
<a id="tocslinkdimensioninput"></a>

```json
{
  "dimension_node": "string",
  "join_type": "left",
  "join_on": "string",
  "join_cardinality": "many_to_one",
  "role": "string"
}

```

LinkDimensionInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|dimension_node|string|true|none|none|
|join_type|[JoinType](#schemajointype)|false|none|Join type|
|join_on|string|true|none|none|
|join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|role|string|false|none|none|

<h2 id="tocS_LinkDimensionOutput">LinkDimensionOutput</h2>
<!-- backwards compatibility -->
<a id="schemalinkdimensionoutput"></a>
<a id="schema_LinkDimensionOutput"></a>
<a id="tocSlinkdimensionoutput"></a>
<a id="tocslinkdimensionoutput"></a>

```json
{
  "dimension": {
    "name": "string"
  },
  "join_type": "left",
  "join_sql": "string",
  "join_cardinality": "one_to_one",
  "role": "string",
  "foreign_keys": {
    "property1": "string",
    "property2": "string"
  }
}

```

LinkDimensionOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|dimension|[NodeNameOutput](#schemanodenameoutput)|true|none|Node name only|
|join_type|[JoinType](#schemajointype)|true|none|Join type|
|join_sql|string|true|none|none|
|join_cardinality|[JoinCardinality](#schemajoincardinality)|false|none|The version upgrade type|
|role|string|false|none|none|
|foreign_keys|object|true|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_MaterializationConfigInfoUnified">MaterializationConfigInfoUnified</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationconfiginfounified"></a>
<a id="schema_MaterializationConfigInfoUnified"></a>
<a id="tocSmaterializationconfiginfounified"></a>
<a id="tocsmaterializationconfiginfounified"></a>

```json
{
  "name": "string",
  "config": {},
  "schedule": "string",
  "job": "string",
  "backfills": [
    {
      "spec": {
        "column_name": "string",
        "values": [
          null
        ],
        "range": [
          null
        ]
      },
      "urls": [
        "string"
      ]
    }
  ],
  "strategy": "string",
  "output_tables": [
    "string"
  ],
  "urls": [
    "http://example.com"
  ]
}

```

MaterializationConfigInfoUnified

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|config|object|true|none|none|
|schedule|string|true|none|none|
|job|string|false|none|none|
|backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|strategy|string|false|none|none|
|output_tables|[string]|true|none|none|
|urls|[string]|true|none|none|

<h2 id="tocS_MaterializationConfigOutput">MaterializationConfigOutput</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationconfigoutput"></a>
<a id="schema_MaterializationConfigOutput"></a>
<a id="tocSmaterializationconfigoutput"></a>
<a id="tocsmaterializationconfigoutput"></a>

```json
{
  "name": "string",
  "config": {},
  "schedule": "string",
  "job": "string",
  "backfills": [
    {
      "spec": {
        "column_name": "string",
        "values": [
          null
        ],
        "range": [
          null
        ]
      },
      "urls": [
        "string"
      ]
    }
  ],
  "strategy": "string"
}

```

MaterializationConfigOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|config|object|true|none|none|
|schedule|string|true|none|none|
|job|string|false|none|none|
|backfills|[[BackfillOutput](#schemabackfilloutput)]|true|none|[Output model for backfills]|
|strategy|string|false|none|none|

<h2 id="tocS_MaterializationInfo">MaterializationInfo</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationinfo"></a>
<a id="schema_MaterializationInfo"></a>
<a id="tocSmaterializationinfo"></a>
<a id="tocsmaterializationinfo"></a>

```json
{
  "output_tables": [
    "string"
  ],
  "urls": [
    "http://example.com"
  ]
}

```

MaterializationInfo

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|output_tables|[string]|true|none|none|
|urls|[string]|true|none|none|

<h2 id="tocS_MaterializationJobTypeEnum">MaterializationJobTypeEnum</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationjobtypeenum"></a>
<a id="schema_MaterializationJobTypeEnum"></a>
<a id="tocSmaterializationjobtypeenum"></a>
<a id="tocsmaterializationjobtypeenum"></a>

```json
{
  "name": "spark_sql",
  "label": "Spark SQL",
  "description": "Spark SQL materialization job",
  "allowed_node_types": [
    "transform",
    "dimension",
    "cube"
  ],
  "job_class": "SparkSqlMaterializationJob"
}

```

MaterializationJobTypeEnum

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|MaterializationJobTypeEnum|any|false|none|Available materialization job types|

#### Enumerated Values

|Property|Value|
|---|---|
|MaterializationJobTypeEnum|{"name":"spark_sql","label":"Spark SQL","description":"Spark SQL materialization job","allowed_node_types":["transform","dimension","cube"],"job_class":"SparkSqlMaterializationJob"}|
|MaterializationJobTypeEnum|{"name":"druid_measures_cube","label":"Druid Measures Cube (Pre-Agg Cube)","description":"Used to materialize a cube's measures to Druid for low-latency access to a set of metrics and dimensions. While the logical cube definition is at the level of metrics and dimensions, this materialized Druid cube will contain measures and dimensions, with rollup configured on the measures where appropriate.","allowed_node_types":["cube"],"job_class":"DruidMeasuresCubeMaterializationJob"}|
|MaterializationJobTypeEnum|{"name":"druid_metrics_cube","label":"Druid Metrics Cube (Post-Agg Cube)","description":"Used to materialize a cube of metrics and dimensions to Druid for low-latency access. The materialized cube is at the metric level, meaning that all metrics will be aggregated to the level of the cube's dimensions.","allowed_node_types":["cube"],"job_class":"DruidMetricsCubeMaterializationJob"}|

<h2 id="tocS_MaterializationStrategy">MaterializationStrategy</h2>
<!-- backwards compatibility -->
<a id="schemamaterializationstrategy"></a>
<a id="schema_MaterializationStrategy"></a>
<a id="tocSmaterializationstrategy"></a>
<a id="tocsmaterializationstrategy"></a>

```json
"full"

```

MaterializationStrategy

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|MaterializationStrategy|string|false|none|Materialization strategies|

#### Enumerated Values

|Property|Value|
|---|---|
|MaterializationStrategy|full|
|MaterializationStrategy|snapshot|
|MaterializationStrategy|snapshot_partition|
|MaterializationStrategy|incremental_time|
|MaterializationStrategy|view|

<h2 id="tocS_Measure">Measure</h2>
<!-- backwards compatibility -->
<a id="schemameasure"></a>
<a id="schema_Measure"></a>
<a id="tocSmeasure"></a>
<a id="tocsmeasure"></a>

```json
{
  "name": "string",
  "field_name": "string",
  "agg": "string",
  "type": "string"
}

```

Measure

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|field_name|string|true|none|none|
|agg|string|true|none|none|
|type|string|true|none|none|

<h2 id="tocS_MeasureOutput">MeasureOutput</h2>
<!-- backwards compatibility -->
<a id="schemameasureoutput"></a>
<a id="schema_MeasureOutput"></a>
<a id="tocSmeasureoutput"></a>
<a id="tocsmeasureoutput"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "columns": [
    {
      "name": "string",
      "type": "string",
      "node": "string"
    }
  ],
  "additive": "additive"
}

```

MeasureOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|false|none|none|
|description|string|false|none|none|
|columns|[[datajunction_server__models__measure__ColumnOutput](#schemadatajunction_server__models__measure__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|additive|[AggregationRule](#schemaaggregationrule)|true|none|Type of allowed aggregation for a given measure.|

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
  "upstream_node": "string",
  "expression": "string",
  "dimensions": [
    {
      "name": "string",
      "node_name": "string",
      "node_display_name": "string",
      "is_primary_key": true,
      "type": "string",
      "path": [
        "string"
      ]
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "required_dimensions": [
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
|upstream_node|string|true|none|none|
|expression|string|true|none|none|
|dimensions|[[DimensionAttributeOutput](#schemadimensionattributeoutput)]|true|none|[Dimension attribute output should include the name and type]|
|metric_metadata|[MetricMetadataOutput](#schemametricmetadataoutput)|false|none|Metric metadata output|
|required_dimensions|[string]|true|none|none|

<h2 id="tocS_MetricDirection">MetricDirection</h2>
<!-- backwards compatibility -->
<a id="schemametricdirection"></a>
<a id="schema_MetricDirection"></a>
<a id="tocSmetricdirection"></a>
<a id="tocsmetricdirection"></a>

```json
"higher_is_better"

```

MetricDirection

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|MetricDirection|string|false|none|The direction of the metric that's considered good, i.e., higher is better|

#### Enumerated Values

|Property|Value|
|---|---|
|MetricDirection|higher_is_better|
|MetricDirection|lower_is_better|
|MetricDirection|neutral|

<h2 id="tocS_MetricMeasures">MetricMeasures</h2>
<!-- backwards compatibility -->
<a id="schemametricmeasures"></a>
<a id="schema_MetricMeasures"></a>
<a id="tocSmetricmeasures"></a>
<a id="tocsmetricmeasures"></a>

```json
{
  "metric": "string",
  "measures": [
    {
      "name": "string",
      "field_name": "string",
      "agg": "string",
      "type": "string"
    }
  ],
  "combiner": "string"
}

```

MetricMeasures

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|metric|string|true|none|none|
|measures|[[Measure](#schemameasure)]|true|none|[A measure with a simple aggregation]|
|combiner|string|true|none|none|

<h2 id="tocS_MetricMetadataInput">MetricMetadataInput</h2>
<!-- backwards compatibility -->
<a id="schemametricmetadatainput"></a>
<a id="schema_MetricMetadataInput"></a>
<a id="tocSmetricmetadatainput"></a>
<a id="tocsmetricmetadatainput"></a>

```json
{
  "direction": "higher_is_better",
  "unit": "string"
}

```

MetricMetadataInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|direction|[MetricDirection](#schemametricdirection)|false|none|The direction of the metric that's considered good, i.e., higher is better|
|unit|string|false|none|none|

<h2 id="tocS_MetricMetadataOptions">MetricMetadataOptions</h2>
<!-- backwards compatibility -->
<a id="schemametricmetadataoptions"></a>
<a id="schema_MetricMetadataOptions"></a>
<a id="tocSmetricmetadataoptions"></a>
<a id="tocsmetricmetadataoptions"></a>

```json
{
  "directions": [
    "higher_is_better"
  ],
  "units": [
    {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  ]
}

```

MetricMetadataOptions

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|directions|[[MetricDirection](#schemametricdirection)]|true|none|[The direction of the metric that's considered good, i.e., higher is better]|
|units|[[Unit](#schemaunit)]|true|none|[Metric unit]|

<h2 id="tocS_MetricMetadataOutput">MetricMetadataOutput</h2>
<!-- backwards compatibility -->
<a id="schemametricmetadataoutput"></a>
<a id="schema_MetricMetadataOutput"></a>
<a id="tocSmetricmetadataoutput"></a>
<a id="tocsmetricmetadataoutput"></a>

```json
{
  "direction": "higher_is_better",
  "unit": {
    "name": "string",
    "label": "string",
    "category": "string",
    "abbreviation": "string",
    "description": "string"
  }
}

```

MetricMetadataOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|direction|[MetricDirection](#schemametricdirection)|false|none|The direction of the metric that's considered good, i.e., higher is better|
|unit|[Unit](#schemaunit)|false|none|Metric unit|

<h2 id="tocS_MutableAttributeTypeFields">MutableAttributeTypeFields</h2>
<!-- backwards compatibility -->
<a id="schemamutableattributetypefields"></a>
<a id="schema_MutableAttributeTypeFields"></a>
<a id="tocSmutableattributetypefields"></a>
<a id="tocsmutableattributetypefields"></a>

```json
{
  "namespace": "system",
  "name": "string",
  "description": "string",
  "allowed_node_types": [
    "source"
  ],
  "uniqueness_scope": [
    "node"
  ]
}

```

MutableAttributeTypeFields

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|false|none|none|
|name|string|true|none|none|
|description|string|true|none|none|
|allowed_node_types|[[NodeType](#schemanodetype)]|true|none|[Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.]|
|uniqueness_scope|[[UniquenessScope](#schemauniquenessscope)]|false|none|[The scope at which this attribute needs to be unique.]|

<h2 id="tocS_NamespaceOutput">NamespaceOutput</h2>
<!-- backwards compatibility -->
<a id="schemanamespaceoutput"></a>
<a id="schema_NamespaceOutput"></a>
<a id="tocSnamespaceoutput"></a>
<a id="tocsnamespaceoutput"></a>

```json
{
  "namespace": "string",
  "num_nodes": 0
}

```

NamespaceOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|
|num_nodes|integer|true|none|none|

<h2 id="tocS_NodeColumn">NodeColumn</h2>
<!-- backwards compatibility -->
<a id="schemanodecolumn"></a>
<a id="schema_NodeColumn"></a>
<a id="tocSnodecolumn"></a>
<a id="tocsnodecolumn"></a>

```json
{
  "node": "string",
  "column": "string"
}

```

NodeColumn

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|node|string|true|none|none|
|column|string|true|none|none|

<h2 id="tocS_NodeIndexItem">NodeIndexItem</h2>
<!-- backwards compatibility -->
<a id="schemanodeindexitem"></a>
<a id="schema_NodeIndexItem"></a>
<a id="tocSnodeindexitem"></a>
<a id="tocsnodeindexitem"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "type": "source"
}

```

NodeIndexItem

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|true|none|none|
|description|string|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|

<h2 id="tocS_NodeMinimumDetail">NodeMinimumDetail</h2>
<!-- backwards compatibility -->
<a id="schemanodeminimumdetail"></a>
<a id="schema_NodeMinimumDetail"></a>
<a id="tocSnodeminimumdetail"></a>
<a id="tocsnodeminimumdetail"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "description": "string",
  "version": "string",
  "type": "source",
  "status": "valid",
  "mode": "published",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

NodeMinimumDetail

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|true|none|none|
|description|string|true|none|none|
|version|string|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|updated_at|string(date-time)|true|none|none|

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

<h2 id="tocS_NodeOutput">NodeOutput</h2>
<!-- backwards compatibility -->
<a id="schemanodeoutput"></a>
<a id="schema_NodeOutput"></a>
<a id="tocSnodeoutput"></a>
<a id="tocsnodeoutput"></a>

```json
{
  "namespace": "string",
  "node_revision_id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "tags": [],
  "current_version": "string",
  "missing_table": false
}

```

NodeOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|namespace|string|true|none|none|
|node_revision_id|integer|true|none|none|
|node_id|integer|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|name|string|true|none|none|
|display_name|string|true|none|none|
|version|string|true|none|none|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|schema_|string|false|none|none|
|table|string|false|none|none|
|description|string|false|none|none|
|query|string|false|none|none|
|availability|[AvailabilityStateBase](#schemaavailabilitystatebase)|false|none|An availability state base|
|columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|updated_at|string(date-time)|true|none|none|
|materializations|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|metric_metadata|[MetricMetadataOutput](#schemametricmetadataoutput)|false|none|Metric metadata output|
|dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|false|none|[Input for linking a dimension to a node]|
|created_at|string(date-time)|true|none|none|
|tags|[[TagOutput](#schematagoutput)]|false|none|[Output tag model.]|
|current_version|string|true|none|none|
|missing_table|boolean|false|none|none|

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
  "id": 0,
  "node_id": 0,
  "type": "source",
  "name": "string",
  "display_name": "string",
  "version": "string",
  "status": "valid",
  "mode": "published",
  "catalog": {
    "name": "string",
    "engines": []
  },
  "schema_": "string",
  "table": "string",
  "description": "",
  "query": "string",
  "availability": {
    "min_temporal_partition": [],
    "max_temporal_partition": [],
    "catalog": "string",
    "schema_": "string",
    "table": "string",
    "valid_through_ts": 0,
    "url": "string",
    "categorical_partitions": [],
    "temporal_partitions": [],
    "partitions": []
  },
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "updated_at": "2019-08-24T14:15:22Z",
  "materializations": [
    {
      "name": "string",
      "config": {},
      "schedule": "string",
      "job": "string",
      "backfills": [
        {
          "spec": {
            "column_name": "string",
            "values": [
              null
            ],
            "range": [
              null
            ]
          },
          "urls": [
            "string"
          ]
        }
      ],
      "strategy": "string"
    }
  ],
  "parents": [
    {
      "name": "string"
    }
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": {
      "name": "string",
      "label": "string",
      "category": "string",
      "abbreviation": "string",
      "description": "string"
    }
  },
  "dimension_links": [
    {
      "dimension": {
        "name": "string"
      },
      "join_type": "left",
      "join_sql": "string",
      "join_cardinality": "one_to_one",
      "role": "string",
      "foreign_keys": {
        "property1": "string",
        "property2": "string"
      }
    }
  ]
}

```

NodeRevisionOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|true|none|none|
|node_id|integer|true|none|none|
|type|[NodeType](#schemanodetype)|true|none|Node type.<br><br>A node can have 4 types, currently:<br><br>1. SOURCE nodes are root nodes in the DAG, and point to tables or views in a DB.<br>2. TRANSFORM nodes are SQL transformations, reading from SOURCE/TRANSFORM nodes.<br>3. METRIC nodes are leaves in the DAG, and have a single aggregation query.<br>4. DIMENSION nodes are special SOURCE nodes that can be auto-joined with METRICS.<br>5. CUBE nodes contain a reference to a set of METRICS and a set of DIMENSIONS.|
|name|string|true|none|none|
|display_name|string|true|none|none|
|version|string|true|none|none|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|mode|[NodeMode](#schemanodemode)|true|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|catalog|[CatalogInfo](#schemacataloginfo)|false|none|Class for catalog creation|
|schema_|string|false|none|none|
|table|string|false|none|none|
|description|string|false|none|none|
|query|string|false|none|none|
|availability|[AvailabilityStateBase](#schemaavailabilitystatebase)|false|none|An availability state base|
|columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|updated_at|string(date-time)|true|none|none|
|materializations|[[MaterializationConfigOutput](#schemamaterializationconfigoutput)]|true|none|[Output for materialization config.]|
|parents|[[NodeNameOutput](#schemanodenameoutput)]|true|none|[Node name only]|
|metric_metadata|[MetricMetadataOutput](#schemametricmetadataoutput)|false|none|Metric metadata output|
|dimension_links|[[LinkDimensionOutput](#schemalinkdimensionoutput)]|false|none|[Input for linking a dimension to a node]|

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
  "dependencies": [
    {
      "id": 0,
      "node_id": 0,
      "type": "source",
      "name": "string",
      "display_name": "string",
      "version": "string",
      "status": "valid",
      "mode": "published",
      "catalog": {
        "name": "string",
        "engines": []
      },
      "schema_": "string",
      "table": "string",
      "description": "",
      "query": "string",
      "availability": {
        "min_temporal_partition": [],
        "max_temporal_partition": [],
        "catalog": "string",
        "schema_": "string",
        "table": "string",
        "valid_through_ts": 0,
        "url": "string",
        "categorical_partitions": [],
        "temporal_partitions": [],
        "partitions": []
      },
      "columns": [
        {
          "name": "string",
          "display_name": "string",
          "type": "string",
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
          },
          "partition": {
            "type_": "temporal",
            "format": "string",
            "granularity": "string",
            "expression": "string"
          }
        }
      ],
      "updated_at": "2019-08-24T14:15:22Z",
      "materializations": [
        {
          "name": "string",
          "config": {},
          "schedule": "string",
          "job": "string",
          "backfills": [
            {
              "spec": {
                "column_name": "string",
                "values": [
                  null
                ],
                "range": [
                  null
                ]
              },
              "urls": [
                "string"
              ]
            }
          ],
          "strategy": "string"
        }
      ],
      "parents": [
        {
          "name": "string"
        }
      ],
      "metric_metadata": {
        "direction": "higher_is_better",
        "unit": {
          "name": "string",
          "label": "string",
          "category": "string",
          "abbreviation": "string",
          "description": "string"
        }
      },
      "dimension_links": [
        {
          "dimension": {
            "name": "string"
          },
          "join_type": "left",
          "join_sql": "string",
          "join_cardinality": "one_to_one",
          "role": "string",
          "foreign_keys": {
            "property1": "string",
            "property2": "string"
          }
        }
      ]
    }
  ],
  "columns": [
    {
      "name": "string",
      "display_name": "string",
      "type": "string",
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
      },
      "partition": {
        "type_": "temporal",
        "format": "string",
        "granularity": "string",
        "expression": "string"
      }
    }
  ],
  "errors": [
    {
      "code": 0,
      "message": "string",
      "debug": {},
      "context": ""
    }
  ],
  "missing_parents": [
    "string"
  ]
}

```

NodeValidation

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|message|string|true|none|none|
|status|[NodeStatus](#schemanodestatus)|true|none|Node status.<br><br>A node can have one of the following statuses:<br><br>1. VALID - All references to other nodes and node columns are valid<br>2. INVALID - One or more parent nodes are incompatible or do not exist|
|dependencies|[[NodeRevisionOutput](#schemanoderevisionoutput)]|true|none|[Output for a node revision with information about columns and if it is a metric.]|
|columns|[[datajunction_server__models__node__ColumnOutput](#schemadatajunction_server__models__node__columnoutput)]|true|none|[A simplified column schema, without ID or dimensions.]|
|errors|[[DJError](#schemadjerror)]|true|none|[An error.]|
|missing_parents|[string]|true|none|none|

<h2 id="tocS_OAuthProvider">OAuthProvider</h2>
<!-- backwards compatibility -->
<a id="schemaoauthprovider"></a>
<a id="schema_OAuthProvider"></a>
<a id="tocSoauthprovider"></a>
<a id="tocsoauthprovider"></a>

```json
"basic"

```

OAuthProvider

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|OAuthProvider|string|false|none|Support oauth providers|

#### Enumerated Values

|Property|Value|
|---|---|
|OAuthProvider|basic|
|OAuthProvider|github|
|OAuthProvider|google|

<h2 id="tocS_PartitionAvailability">PartitionAvailability</h2>
<!-- backwards compatibility -->
<a id="schemapartitionavailability"></a>
<a id="schema_PartitionAvailability"></a>
<a id="tocSpartitionavailability"></a>
<a id="tocspartitionavailability"></a>

```json
{
  "min_temporal_partition": [
    "string"
  ],
  "max_temporal_partition": [
    "string"
  ],
  "value": [
    "string"
  ],
  "valid_through_ts": 0
}

```

PartitionAvailability

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|min_temporal_partition|[string]|false|none|none|
|max_temporal_partition|[string]|false|none|none|
|value|[string]|true|none|none|
|valid_through_ts|integer|false|none|none|

<h2 id="tocS_PartitionBackfill">PartitionBackfill</h2>
<!-- backwards compatibility -->
<a id="schemapartitionbackfill"></a>
<a id="schema_PartitionBackfill"></a>
<a id="tocSpartitionbackfill"></a>
<a id="tocspartitionbackfill"></a>

```json
{
  "column_name": "string",
  "values": [
    null
  ],
  "range": [
    null
  ]
}

```

PartitionBackfill

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|column_name|string|true|none|none|
|values|[any]|false|none|none|
|range|[any]|false|none|none|

<h2 id="tocS_PartitionInput">PartitionInput</h2>
<!-- backwards compatibility -->
<a id="schemapartitioninput"></a>
<a id="schema_PartitionInput"></a>
<a id="tocSpartitioninput"></a>
<a id="tocspartitioninput"></a>

```json
{
  "type_": "temporal",
  "granularity": "second",
  "format": "string"
}

```

PartitionInput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|granularity|[Granularity](#schemagranularity)|false|none|Time dimension granularity.|
|format|string|false|none|none|

<h2 id="tocS_PartitionOutput">PartitionOutput</h2>
<!-- backwards compatibility -->
<a id="schemapartitionoutput"></a>
<a id="schema_PartitionOutput"></a>
<a id="tocSpartitionoutput"></a>
<a id="tocspartitionoutput"></a>

```json
{
  "type_": "temporal",
  "format": "string",
  "granularity": "string",
  "expression": "string"
}

```

PartitionOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|type_|[PartitionType](#schemapartitiontype)|true|none|Partition type.<br><br>A partition can be temporal or categorical|
|format|string|false|none|none|
|granularity|string|false|none|none|
|expression|string|false|none|none|

<h2 id="tocS_PartitionType">PartitionType</h2>
<!-- backwards compatibility -->
<a id="schemapartitiontype"></a>
<a id="schema_PartitionType"></a>
<a id="tocSpartitiontype"></a>
<a id="tocspartitiontype"></a>

```json
"temporal"

```

PartitionType

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|PartitionType|string|false|none|Partition type.<br><br>A partition can be temporal or categorical|

#### Enumerated Values

|Property|Value|
|---|---|
|PartitionType|temporal|
|PartitionType|categorical|

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
        "type": "string",
        "column": "string",
        "node": "string",
        "semantic_entity": "string",
        "semantic_type": "string"
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
  "id": "string",
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
          "type": "string",
          "column": "string",
          "node": "string",
          "semantic_entity": "string",
          "semantic_type": "string"
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
  ],
  "links": [
    "http://example.com"
  ]
}

```

QueryWithResults

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string|true|none|none|
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
|links|[string]|false|none|none|

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

<h2 id="tocS_SparkConf">SparkConf</h2>
<!-- backwards compatibility -->
<a id="schemasparkconf"></a>
<a id="schema_SparkConf"></a>
<a id="tocSsparkconf"></a>
<a id="tocssparkconf"></a>

```json
{}

```

SparkConf

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|string|false|none|none|

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
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
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

<h2 id="tocS_TagOutput">TagOutput</h2>
<!-- backwards compatibility -->
<a id="schematagoutput"></a>
<a id="schema_TagOutput"></a>
<a id="tocStagoutput"></a>
<a id="tocstagoutput"></a>

```json
{
  "description": "string",
  "display_name": "string",
  "tag_metadata": {},
  "name": "string",
  "tag_type": "string"
}

```

TagOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|false|none|none|
|display_name|string|false|none|none|
|tag_metadata|object|false|none|none|
|name|string|true|none|none|
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
      "type": "string",
      "column": "string",
      "node": "string",
      "semantic_entity": "string",
      "semantic_type": "string"
    }
  ],
  "dialect": "spark",
  "upstream_tables": [
    "string"
  ]
}

```

TranslatedSQL

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|sql|string|true|none|none|
|columns|[[ColumnMetadata](#schemacolumnmetadata)]|false|none|[A simple model for column metadata.]|
|dialect|[Dialect](#schemadialect)|false|none|SQL dialect|
|upstream_tables|[string]|false|none|none|

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

<h2 id="tocS_Unit">Unit</h2>
<!-- backwards compatibility -->
<a id="schemaunit"></a>
<a id="schema_Unit"></a>
<a id="tocSunit"></a>
<a id="tocsunit"></a>

```json
{
  "name": "string",
  "label": "string",
  "category": "string",
  "abbreviation": "string",
  "description": "string"
}

```

Unit

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|label|string|false|none|none|
|category|string|false|none|none|
|abbreviation|string|false|none|none|
|description|string|false|none|none|

<h2 id="tocS_UpdateNode">UpdateNode</h2>
<!-- backwards compatibility -->
<a id="schemaupdatenode"></a>
<a id="schema_UpdateNode"></a>
<a id="tocSupdatenode"></a>
<a id="tocsupdatenode"></a>

```json
{
  "metrics": [
    "string"
  ],
  "dimensions": [
    "string"
  ],
  "filters": [
    "string"
  ],
  "orderby": [
    "string"
  ],
  "limit": 0,
  "description": "string",
  "mode": "published",
  "required_dimensions": [
    "string"
  ],
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "string"
  },
  "query": "string",
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
  "missing_table": true,
  "display_name": "string",
  "primary_key": [
    "string"
  ]
}

```

UpdateNode

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|metrics|[string]|false|none|none|
|dimensions|[string]|false|none|none|
|filters|[string]|false|none|none|
|orderby|[string]|false|none|none|
|limit|integer|false|none|none|
|description|string|false|none|none|
|mode|[NodeMode](#schemanodemode)|false|none|Node mode.<br><br>A node can be in one of the following modes:<br><br>1. PUBLISHED - Must be valid and not cause any child nodes to be invalid<br>2. DRAFT - Can be invalid, have invalid parents, and include dangling references|
|required_dimensions|[string]|false|none|none|
|metric_metadata|[MetricMetadataInput](#schemametricmetadatainput)|false|none|Metric metadata output|
|query|string|false|none|none|
|catalog|string|false|none|none|
|schema_|string|false|none|none|
|table|string|false|none|none|
|columns|[[SourceColumnOutput](#schemasourcecolumnoutput)]|false|none|[A column used in creation of a source node]|
|missing_table|boolean|false|none|none|
|display_name|string|false|none|none|
|primary_key|[string]|false|none|none|

<h2 id="tocS_UpdateTag">UpdateTag</h2>
<!-- backwards compatibility -->
<a id="schemaupdatetag"></a>
<a id="schema_UpdateTag"></a>
<a id="tocSupdatetag"></a>
<a id="tocsupdatetag"></a>

```json
{
  "description": "string",
  "display_name": "string",
  "tag_metadata": {}
}

```

UpdateTag

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|false|none|none|
|display_name|string|false|none|none|
|tag_metadata|object|false|none|none|

<h2 id="tocS_UpsertMaterialization">UpsertMaterialization</h2>
<!-- backwards compatibility -->
<a id="schemaupsertmaterialization"></a>
<a id="schema_UpsertMaterialization"></a>
<a id="tocSupsertmaterialization"></a>
<a id="tocsupsertmaterialization"></a>

```json
{
  "name": "string",
  "job": {
    "name": "spark_sql",
    "label": "Spark SQL",
    "description": "Spark SQL materialization job",
    "allowed_node_types": [
      "transform",
      "dimension",
      "cube"
    ],
    "job_class": "SparkSqlMaterializationJob"
  },
  "config": {
    "spark": {},
    "lookback_window": "string",
    "dimensions": [
      "string"
    ],
    "measures": {
      "property1": {
        "metric": "string",
        "measures": [
          {
            "name": "string",
            "field_name": "string",
            "agg": "string",
            "type": "string"
          }
        ],
        "combiner": "string"
      },
      "property2": {
        "metric": "string",
        "measures": [
          {
            "name": "string",
            "field_name": "string",
            "agg": "string",
            "type": "string"
          }
        ],
        "combiner": "string"
      }
    },
    "metrics": [
      {
        "name": "string",
        "type": "string",
        "column": "string",
        "node": "string",
        "semantic_entity": "string",
        "semantic_type": "string"
      }
    ],
    "prefix": "",
    "suffix": "",
    "druid": {
      "granularity": "string",
      "intervals": [
        "string"
      ],
      "timestamp_column": "string",
      "timestamp_format": "string",
      "parse_spec_format": "string"
    }
  },
  "schedule": "string",
  "strategy": "full"
}

```

UpsertMaterialization

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|job|[MaterializationJobTypeEnum](#schemamaterializationjobtypeenum)|true|none|Available materialization job types|
|config|any|true|none|none|

anyOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[DruidCubeConfigInput](#schemadruidcubeconfiginput)|false|none|Specific Druid cube materialization fields that require user input|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[GenericCubeConfigInput](#schemagenericcubeconfiginput)|false|none|Generic cube materialization config fields that require user input|

or

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[GenericMaterializationConfigInput](#schemagenericmaterializationconfiginput)|false|none|User-input portions of the materialization config|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|schedule|string|true|none|none|
|strategy|[MaterializationStrategy](#schemamaterializationstrategy)|true|none|Materialization strategies|

<h2 id="tocS_UserOutput">UserOutput</h2>
<!-- backwards compatibility -->
<a id="schemauseroutput"></a>
<a id="schema_UserOutput"></a>
<a id="tocSuseroutput"></a>
<a id="tocsuseroutput"></a>

```json
{
  "id": 0,
  "username": "string",
  "email": "string",
  "name": "string",
  "oauth_provider": "basic",
  "is_admin": false
}

```

UserOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer|true|none|none|
|username|string|true|none|none|
|email|string|false|none|none|
|name|string|false|none|none|
|oauth_provider|[OAuthProvider](#schemaoauthprovider)|true|none|Support oauth providers|
|is_admin|boolean|false|none|none|

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

<h2 id="tocS_datajunction_server__models__measure__ColumnOutput">datajunction_server__models__measure__ColumnOutput</h2>
<!-- backwards compatibility -->
<a id="schemadatajunction_server__models__measure__columnoutput"></a>
<a id="schema_datajunction_server__models__measure__ColumnOutput"></a>
<a id="tocSdatajunction_server__models__measure__columnoutput"></a>
<a id="tocsdatajunction_server__models__measure__columnoutput"></a>

```json
{
  "name": "string",
  "type": "string",
  "node": "string"
}

```

ColumnOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|type|string|true|none|none|
|node|string|true|none|none|

<h2 id="tocS_datajunction_server__models__node__ColumnOutput">datajunction_server__models__node__ColumnOutput</h2>
<!-- backwards compatibility -->
<a id="schemadatajunction_server__models__node__columnoutput"></a>
<a id="schema_datajunction_server__models__node__ColumnOutput"></a>
<a id="tocSdatajunction_server__models__node__columnoutput"></a>
<a id="tocsdatajunction_server__models__node__columnoutput"></a>

```json
{
  "name": "string",
  "display_name": "string",
  "type": "string",
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
  },
  "partition": {
    "type_": "temporal",
    "format": "string",
    "granularity": "string",
    "expression": "string"
  }
}

```

ColumnOutput

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|display_name|string|false|none|none|
|type|string|true|none|none|
|attributes|[[AttributeOutput](#schemaattributeoutput)]|false|none|[Column attribute output.]|
|dimension|[NodeNameOutput](#schemanodenameoutput)|false|none|Node name only|
|partition|[PartitionOutput](#schemapartitionoutput)|false|none|Output for partition|

