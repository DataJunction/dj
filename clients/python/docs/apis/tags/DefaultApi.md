<a name="__pageTop"></a>
# djclient.apis.tags.default_api.DefaultApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_availability_data_availability_node_name_post**](#add_availability_data_availability_node_name_post) | **post** /data/availability/{node_name}/ | Add Availability
[**add_catalog_catalogs_post**](#add_catalog_catalogs_post) | **post** /catalogs/ | Add Catalog
[**add_dimension_to_node_nodes_name_columns_column_post**](#add_dimension_to_node_nodes_name_columns_column_post) | **post** /nodes/{name}/columns/{column}/ | Add Dimension To Node
[**add_engine_engines_post**](#add_engine_engines_post) | **post** /engines/ | Add Engine
[**add_engines_to_catalog_catalogs_name_engines_post**](#add_engines_to_catalog_catalogs_name_engines_post) | **post** /catalogs/{name}/engines/ | Add Engines To Catalog
[**add_table_to_node_nodes_name_table_post**](#add_table_to_node_nodes_name_table_post) | **post** /nodes/{name}/table/ | Add Table To Node
[**add_tag_to_node_nodes_name_tag_post**](#add_tag_to_node_nodes_name_tag_post) | **post** /nodes/{name}/tag/ | Add Tag To Node
[**create_node_nodes_post**](#create_node_nodes_post) | **post** /nodes/ | Create Node
[**create_tag_tags_post**](#create_tag_tags_post) | **post** /tags/ | Create Tag
[**downstream_nodes_nodes_name_downstream_get**](#downstream_nodes_nodes_name_downstream_get) | **get** /nodes/{name}/downstream/ | Downstream Nodes
[**find_nodes_by_tag_tags_name_nodes_get**](#find_nodes_by_tag_tags_name_nodes_get) | **get** /tags/{name}/nodes/ | Find Nodes By Tag
[**handle_http_get_graphql_get**](#handle_http_get_graphql_get) | **get** /graphql | Handle Http Get
[**handle_http_post_graphql_post**](#handle_http_post_graphql_post) | **post** /graphql | Handle Http Post
[**health_health_get**](#health_health_get) | **get** /health/ | Health
[**list_catalogs_catalogs_get**](#list_catalogs_catalogs_get) | **get** /catalogs/ | List Catalogs
[**list_engine_engines_name_version_get**](#list_engine_engines_name_version_get) | **get** /engines/{name}/{version}/ | List Engine
[**list_engines_engines_get**](#list_engines_engines_get) | **get** /engines/ | List Engines
[**list_node_revisions_nodes_name_revisions_get**](#list_node_revisions_nodes_name_revisions_get) | **get** /nodes/{name}/revisions/ | List Node Revisions
[**list_tags_tags_get**](#list_tags_tags_get) | **get** /tags/ | List Tags
[**node_similarity_nodes_similarity_node1_name_node2_name_get**](#node_similarity_nodes_similarity_node1_name_node2_name_get) | **get** /nodes/similarity/{node1_name}/{node2_name} | Node Similarity
[**read_catalog_catalogs_name_get**](#read_catalog_catalogs_name_get) | **get** /catalogs/{name}/ | Read Catalog
[**read_cube_cubes_name_get**](#read_cube_cubes_name_get) | **get** /cubes/{name}/ | Read Cube
[**read_databases_databases_get**](#read_databases_databases_get) | **get** /databases/ | Read Databases
[**read_metric_metrics_name_get**](#read_metric_metrics_name_get) | **get** /metrics/{name}/ | Read Metric
[**read_metrics_metrics_get**](#read_metrics_metrics_get) | **get** /metrics/ | Read Metrics
[**read_metrics_sql_metrics_name_sql_get**](#read_metrics_sql_metrics_name_sql_get) | **get** /metrics/{name}/sql/ | Read Metrics Sql
[**read_metrics_sql_query_validate_post**](#read_metrics_sql_query_validate_post) | **post** /query/validate | Read Metrics Sql
[**read_node_nodes_name_get**](#read_node_nodes_name_get) | **get** /nodes/{name}/ | Read Node
[**read_nodes_nodes_get**](#read_nodes_nodes_get) | **get** /nodes/ | Read Nodes
[**read_tag_tags_name_get**](#read_tag_tags_name_get) | **get** /tags/{name}/ | Read Tag
[**update_node_nodes_name_patch**](#update_node_nodes_name_patch) | **patch** /nodes/{name}/ | Update Node
[**update_tag_tags_name_patch**](#update_tag_tags_name_patch) | **patch** /tags/{name}/ | Update Tag
[**upsert_node_materialization_config_nodes_name_materialization_post**](#upsert_node_materialization_config_nodes_name_materialization_post) | **post** /nodes/{name}/materialization/ | Upsert Node Materialization Config
[**validate_node_nodes_validate_post**](#validate_node_nodes_validate_post) | **post** /nodes/validate/ | Validate Node

# **add_availability_data_availability_node_name_post**
<a name="add_availability_data_availability_node_name_post"></a>
> bool, date, datetime, dict, float, int, list, str, none_type add_availability_data_availability_node_name_post(node_nameavailability_state_base)

Add Availability

Add an availability state to a node

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.availability_state_base import AvailabilityStateBase
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'node_name': "node_name_example",
    }
    body = AvailabilityStateBase(
        catalog="catalog_example",
        schema_="schema__example",
        table="table_example",
        valid_through_ts=1,
        max_partition=[
            "max_partition_example"
        ],
        min_partition=[
            "min_partition_example"
        ],
    )
    try:
        # Add Availability
        api_response = api_instance.add_availability_data_availability_node_name_post(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_availability_data_availability_node_name_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**AvailabilityStateBase**](../../models/AvailabilityStateBase.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
node_name | NodeNameSchema | | 

# NodeNameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_availability_data_availability_node_name_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_availability_data_availability_node_name_post.ApiResponseFor422) | Validation Error

#### add_availability_data_availability_node_name_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### add_availability_data_availability_node_name_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **add_catalog_catalogs_post**
<a name="add_catalog_catalogs_post"></a>
> CatalogInfo add_catalog_catalogs_post(catalog_info)

Add Catalog

Add a Catalog

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.catalog_info import CatalogInfo
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    body = CatalogInfo(
        name="name_example",
        engines=[],
    )
    try:
        # Add Catalog
        api_response = api_instance.add_catalog_catalogs_post(
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_catalog_catalogs_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CatalogInfo**](../../models/CatalogInfo.md) |  | 


### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_catalog_catalogs_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_catalog_catalogs_post.ApiResponseFor422) | Validation Error

#### add_catalog_catalogs_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CatalogInfo**](../../models/CatalogInfo.md) |  | 


#### add_catalog_catalogs_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **add_dimension_to_node_nodes_name_columns_column_post**
<a name="add_dimension_to_node_nodes_name_columns_column_post"></a>
> bool, date, datetime, dict, float, int, list, str, none_type add_dimension_to_node_nodes_name_columns_column_post(namecolumn)

Add Dimension To Node

Add information to a node column

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
        'column': "column_example",
    }
    query_params = {
    }
    try:
        # Add Dimension To Node
        api_response = api_instance.add_dimension_to_node_nodes_name_columns_column_post(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_dimension_to_node_nodes_name_columns_column_post: %s\n" % e)

    # example passing only optional values
    path_params = {
        'name': "name_example",
        'column': "column_example",
    }
    query_params = {
        'dimension': "dimension_example",
        'dimension_column': "dimension_column_example",
    }
    try:
        # Add Dimension To Node
        api_response = api_instance.add_dimension_to_node_nodes_name_columns_column_post(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_dimension_to_node_nodes_name_columns_column_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
dimension | DimensionSchema | | optional
dimension_column | DimensionColumnSchema | | optional


# DimensionSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# DimensionColumnSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 
column | ColumnSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# ColumnSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_dimension_to_node_nodes_name_columns_column_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_dimension_to_node_nodes_name_columns_column_post.ApiResponseFor422) | Validation Error

#### add_dimension_to_node_nodes_name_columns_column_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### add_dimension_to_node_nodes_name_columns_column_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **add_engine_engines_post**
<a name="add_engine_engines_post"></a>
> EngineInfo add_engine_engines_post(engine_info)

Add Engine

Add an Engine

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.engine_info import EngineInfo
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    body = EngineInfo(
        name="name_example",
        version="version_example",
        uri="uri_example",
    )
    try:
        # Add Engine
        api_response = api_instance.add_engine_engines_post(
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_engine_engines_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**EngineInfo**](../../models/EngineInfo.md) |  | 


### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_engine_engines_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_engine_engines_post.ApiResponseFor422) | Validation Error

#### add_engine_engines_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**EngineInfo**](../../models/EngineInfo.md) |  | 


#### add_engine_engines_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **add_engines_to_catalog_catalogs_name_engines_post**
<a name="add_engines_to_catalog_catalogs_name_engines_post"></a>
> CatalogInfo add_engines_to_catalog_catalogs_name_engines_post(nameengine_info)

Add Engines To Catalog

Attach one or more engines to a catalog

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.catalog_info import CatalogInfo
from djclient.model.engine_info import EngineInfo
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    body = [
        EngineInfo(
            name="name_example",
            version="version_example",
            uri="uri_example",
        )
    ]
    try:
        # Add Engines To Catalog
        api_response = api_instance.add_engines_to_catalog_catalogs_name_engines_post(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_engines_to_catalog_catalogs_name_engines_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**EngineInfo**]({{complexTypePrefix}}EngineInfo.md) | [**EngineInfo**]({{complexTypePrefix}}EngineInfo.md) | [**EngineInfo**]({{complexTypePrefix}}EngineInfo.md) |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_engines_to_catalog_catalogs_name_engines_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_engines_to_catalog_catalogs_name_engines_post.ApiResponseFor422) | Validation Error

#### add_engines_to_catalog_catalogs_name_engines_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CatalogInfo**](../../models/CatalogInfo.md) |  | 


#### add_engines_to_catalog_catalogs_name_engines_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **add_table_to_node_nodes_name_table_post**
<a name="add_table_to_node_nodes_name_table_post"></a>
> bool, date, datetime, dict, float, int, list, str, none_type add_table_to_node_nodes_name_table_post(namecreate_table)

Add Table To Node

Add a table to a node

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.create_table import CreateTable
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    body = CreateTable(
        schema="schema_example",
        table="table_example",
        cost=1.0,
        database_name="database_name_example",
        catalog_name="catalog_name_example",
        columns=[
            CreateColumn(
                name="name_example",
                type="type_example",
            )
        ],
    )
    try:
        # Add Table To Node
        api_response = api_instance.add_table_to_node_nodes_name_table_post(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_table_to_node_nodes_name_table_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CreateTable**](../../models/CreateTable.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_table_to_node_nodes_name_table_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_table_to_node_nodes_name_table_post.ApiResponseFor422) | Validation Error

#### add_table_to_node_nodes_name_table_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### add_table_to_node_nodes_name_table_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **add_tag_to_node_nodes_name_tag_post**
<a name="add_tag_to_node_nodes_name_tag_post"></a>
> bool, date, datetime, dict, float, int, list, str, none_type add_tag_to_node_nodes_name_tag_post(nametag_name)

Add Tag To Node

Add a tag to a node

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    query_params = {
        'tag_name': "tag_name_example",
    }
    try:
        # Add Tag To Node
        api_response = api_instance.add_tag_to_node_nodes_name_tag_post(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->add_tag_to_node_nodes_name_tag_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
tag_name | TagNameSchema | | 


# TagNameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#add_tag_to_node_nodes_name_tag_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#add_tag_to_node_nodes_name_tag_post.ApiResponseFor422) | Validation Error

#### add_tag_to_node_nodes_name_tag_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### add_tag_to_node_nodes_name_tag_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **create_node_nodes_post**
<a name="create_node_nodes_post"></a>
> NodeOutput create_node_nodes_post(any_type)

Create Node

Create a node.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.create_cube_node import CreateCubeNode
from djclient.model.node_output import NodeOutput
from djclient.model.create_source_node import CreateSourceNode
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.create_node import CreateNode
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    body = None
    try:
        # Create Node
        api_response = api_instance.create_node_nodes_post(
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->create_node_nodes_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

### Composed Schemas (allOf/anyOf/oneOf/not)
#### anyOf
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[CreateSourceNode]({{complexTypePrefix}}CreateSourceNode.md) | [**CreateSourceNode**]({{complexTypePrefix}}CreateSourceNode.md) | [**CreateSourceNode**]({{complexTypePrefix}}CreateSourceNode.md) |  | 
[CreateCubeNode]({{complexTypePrefix}}CreateCubeNode.md) | [**CreateCubeNode**]({{complexTypePrefix}}CreateCubeNode.md) | [**CreateCubeNode**]({{complexTypePrefix}}CreateCubeNode.md) |  | 
[CreateNode]({{complexTypePrefix}}CreateNode.md) | [**CreateNode**]({{complexTypePrefix}}CreateNode.md) | [**CreateNode**]({{complexTypePrefix}}CreateNode.md) |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#create_node_nodes_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#create_node_nodes_post.ApiResponseFor422) | Validation Error

#### create_node_nodes_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**NodeOutput**](../../models/NodeOutput.md) |  | 


#### create_node_nodes_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **create_tag_tags_post**
<a name="create_tag_tags_post"></a>
> Tag create_tag_tags_post(create_tag)

Create Tag

Create a tag.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.create_tag import CreateTag
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.tag import Tag
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    body = CreateTag(
        description="description_example",
        tag_metadata=dict(),
        name="name_example",
        display_name="display_name_example",
        tag_type="tag_type_example",
    )
    try:
        # Create Tag
        api_response = api_instance.create_tag_tags_post(
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->create_tag_tags_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CreateTag**](../../models/CreateTag.md) |  | 


### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
201 | [ApiResponseFor201](#create_tag_tags_post.ApiResponseFor201) | Successful Response
422 | [ApiResponseFor422](#create_tag_tags_post.ApiResponseFor422) | Validation Error

#### create_tag_tags_post.ApiResponseFor201
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor201ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor201ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Tag**](../../models/Tag.md) |  | 


#### create_tag_tags_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **downstream_nodes_nodes_name_downstream_get**
<a name="downstream_nodes_nodes_name_downstream_get"></a>
> [NodeOutput] downstream_nodes_nodes_name_downstream_get(name)

Downstream Nodes

List all nodes that are downstream from the given node, filterable by type.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.node_output import NodeOutput
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.node_type import NodeType
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    query_params = {
    }
    try:
        # Downstream Nodes
        api_response = api_instance.downstream_nodes_nodes_name_downstream_get(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->downstream_nodes_nodes_name_downstream_get: %s\n" % e)

    # example passing only optional values
    path_params = {
        'name': "name_example",
    }
    query_params = {
        'node_type': NodeType("source"),
    }
    try:
        # Downstream Nodes
        api_response = api_instance.downstream_nodes_nodes_name_downstream_get(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->downstream_nodes_nodes_name_downstream_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
node_type | NodeTypeSchema | | optional


# NodeTypeSchema
Type | Description  | Notes
------------- | ------------- | -------------
[**NodeType**](../../models/NodeType.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#downstream_nodes_nodes_name_downstream_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#downstream_nodes_nodes_name_downstream_get.ApiResponseFor422) | Validation Error

#### downstream_nodes_nodes_name_downstream_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**NodeOutput**]({{complexTypePrefix}}NodeOutput.md) | [**NodeOutput**]({{complexTypePrefix}}NodeOutput.md) | [**NodeOutput**]({{complexTypePrefix}}NodeOutput.md) |  | 

#### downstream_nodes_nodes_name_downstream_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **find_nodes_by_tag_tags_name_nodes_get**
<a name="find_nodes_by_tag_tags_name_nodes_get"></a>
> [str] find_nodes_by_tag_tags_name_nodes_get(name)

Find Nodes By Tag

Find nodes tagged with the tag, filterable by node type.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.node_type import NodeType
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    query_params = {
    }
    try:
        # Find Nodes By Tag
        api_response = api_instance.find_nodes_by_tag_tags_name_nodes_get(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->find_nodes_by_tag_tags_name_nodes_get: %s\n" % e)

    # example passing only optional values
    path_params = {
        'name': "name_example",
    }
    query_params = {
        'node_type': NodeType("source"),
    }
    try:
        # Find Nodes By Tag
        api_response = api_instance.find_nodes_by_tag_tags_name_nodes_get(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->find_nodes_by_tag_tags_name_nodes_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
node_type | NodeTypeSchema | | optional


# NodeTypeSchema
Type | Description  | Notes
------------- | ------------- | -------------
[**NodeType**](../../models/NodeType.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#find_nodes_by_tag_tags_name_nodes_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#find_nodes_by_tag_tags_name_nodes_get.ApiResponseFor422) | Validation Error

#### find_nodes_by_tag_tags_name_nodes_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
items | str,  | str,  |  | 

#### find_nodes_by_tag_tags_name_nodes_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **handle_http_get_graphql_get**
<a name="handle_http_get_graphql_get"></a>
> bool, date, datetime, dict, float, int, list, str, none_type handle_http_get_graphql_get()

Handle Http Get

### Example

```python
import djclient
from djclient.apis.tags import default_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Handle Http Get
        api_response = api_instance.handle_http_get_graphql_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->handle_http_get_graphql_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#handle_http_get_graphql_get.ApiResponseFor200) | The GraphiQL integrated development environment.
404 | [ApiResponseFor404](#handle_http_get_graphql_get.ApiResponseFor404) | Not found if GraphiQL is not enabled.

#### handle_http_get_graphql_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### handle_http_get_graphql_get.ApiResponseFor404
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | Unset | headers were not defined |

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **handle_http_post_graphql_post**
<a name="handle_http_post_graphql_post"></a>
> bool, date, datetime, dict, float, int, list, str, none_type handle_http_post_graphql_post()

Handle Http Post

### Example

```python
import djclient
from djclient.apis.tags import default_api
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Handle Http Post
        api_response = api_instance.handle_http_post_graphql_post()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->handle_http_post_graphql_post: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#handle_http_post_graphql_post.ApiResponseFor200) | Successful Response

#### handle_http_post_graphql_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **health_health_get**
<a name="health_health_get"></a>
> [HealthCheck] health_health_get()

Health

Healthcheck for services.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.health_check import HealthCheck
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Health
        api_response = api_instance.health_health_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->health_health_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#health_health_get.ApiResponseFor200) | Successful Response

#### health_health_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**HealthCheck**]({{complexTypePrefix}}HealthCheck.md) | [**HealthCheck**]({{complexTypePrefix}}HealthCheck.md) | [**HealthCheck**]({{complexTypePrefix}}HealthCheck.md) |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **list_catalogs_catalogs_get**
<a name="list_catalogs_catalogs_get"></a>
> [CatalogInfo] list_catalogs_catalogs_get()

List Catalogs

List all available catalogs

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.catalog_info import CatalogInfo
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # List Catalogs
        api_response = api_instance.list_catalogs_catalogs_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->list_catalogs_catalogs_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_catalogs_catalogs_get.ApiResponseFor200) | Successful Response

#### list_catalogs_catalogs_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**CatalogInfo**]({{complexTypePrefix}}CatalogInfo.md) | [**CatalogInfo**]({{complexTypePrefix}}CatalogInfo.md) | [**CatalogInfo**]({{complexTypePrefix}}CatalogInfo.md) |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **list_engine_engines_name_version_get**
<a name="list_engine_engines_name_version_get"></a>
> EngineInfo list_engine_engines_name_version_get(nameversion)

List Engine

Return an engine by name and version

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.engine_info import EngineInfo
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
        'version': "version_example",
    }
    try:
        # List Engine
        api_response = api_instance.list_engine_engines_name_version_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->list_engine_engines_name_version_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 
version | VersionSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# VersionSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_engine_engines_name_version_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#list_engine_engines_name_version_get.ApiResponseFor422) | Validation Error

#### list_engine_engines_name_version_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**EngineInfo**](../../models/EngineInfo.md) |  | 


#### list_engine_engines_name_version_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **list_engines_engines_get**
<a name="list_engines_engines_get"></a>
> [EngineInfo] list_engines_engines_get()

List Engines

List all available engines

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.engine_info import EngineInfo
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # List Engines
        api_response = api_instance.list_engines_engines_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->list_engines_engines_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_engines_engines_get.ApiResponseFor200) | Successful Response

#### list_engines_engines_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**EngineInfo**]({{complexTypePrefix}}EngineInfo.md) | [**EngineInfo**]({{complexTypePrefix}}EngineInfo.md) | [**EngineInfo**]({{complexTypePrefix}}EngineInfo.md) |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **list_node_revisions_nodes_name_revisions_get**
<a name="list_node_revisions_nodes_name_revisions_get"></a>
> [NodeRevisionOutput] list_node_revisions_nodes_name_revisions_get(name)

List Node Revisions

List all revisions for the node.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.node_revision_output import NodeRevisionOutput
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    try:
        # List Node Revisions
        api_response = api_instance.list_node_revisions_nodes_name_revisions_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->list_node_revisions_nodes_name_revisions_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_node_revisions_nodes_name_revisions_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#list_node_revisions_nodes_name_revisions_get.ApiResponseFor422) | Validation Error

#### list_node_revisions_nodes_name_revisions_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**NodeRevisionOutput**]({{complexTypePrefix}}NodeRevisionOutput.md) | [**NodeRevisionOutput**]({{complexTypePrefix}}NodeRevisionOutput.md) | [**NodeRevisionOutput**]({{complexTypePrefix}}NodeRevisionOutput.md) |  | 

#### list_node_revisions_nodes_name_revisions_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **list_tags_tags_get**
<a name="list_tags_tags_get"></a>
> [TagOutput] list_tags_tags_get()

List Tags

List all available tags.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.tag_output import TagOutput
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only optional values
    query_params = {
        'tag_type': "tag_type_example",
    }
    try:
        # List Tags
        api_response = api_instance.list_tags_tags_get(
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->list_tags_tags_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
tag_type | TagTypeSchema | | optional


# TagTypeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_tags_tags_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#list_tags_tags_get.ApiResponseFor422) | Validation Error

#### list_tags_tags_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**TagOutput**]({{complexTypePrefix}}TagOutput.md) | [**TagOutput**]({{complexTypePrefix}}TagOutput.md) | [**TagOutput**]({{complexTypePrefix}}TagOutput.md) |  | 

#### list_tags_tags_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **node_similarity_nodes_similarity_node1_name_node2_name_get**
<a name="node_similarity_nodes_similarity_node1_name_node2_name_get"></a>
> bool, date, datetime, dict, float, int, list, str, none_type node_similarity_nodes_similarity_node1_name_node2_name_get(node1_namenode2_name)

Node Similarity

Compare two nodes by how similar their queries are

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'node1_name': "node1_name_example",
        'node2_name': "node2_name_example",
    }
    try:
        # Node Similarity
        api_response = api_instance.node_similarity_nodes_similarity_node1_name_node2_name_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->node_similarity_nodes_similarity_node1_name_node2_name_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
node1_name | Node1NameSchema | | 
node2_name | Node2NameSchema | | 

# Node1NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# Node2NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#node_similarity_nodes_similarity_node1_name_node2_name_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#node_similarity_nodes_similarity_node1_name_node2_name_get.ApiResponseFor422) | Validation Error

#### node_similarity_nodes_similarity_node1_name_node2_name_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### node_similarity_nodes_similarity_node1_name_node2_name_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_catalog_catalogs_name_get**
<a name="read_catalog_catalogs_name_get"></a>
> CatalogInfo read_catalog_catalogs_name_get(name)

Read Catalog

Return a catalog by name

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.catalog_info import CatalogInfo
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    try:
        # Read Catalog
        api_response = api_instance.read_catalog_catalogs_name_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_catalog_catalogs_name_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_catalog_catalogs_name_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_catalog_catalogs_name_get.ApiResponseFor422) | Validation Error

#### read_catalog_catalogs_name_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CatalogInfo**](../../models/CatalogInfo.md) |  | 


#### read_catalog_catalogs_name_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_cube_cubes_name_get**
<a name="read_cube_cubes_name_get"></a>
> CubeRevisionMetadata read_cube_cubes_name_get(name)

Read Cube

Get information on a cube

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.cube_revision_metadata import CubeRevisionMetadata
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    try:
        # Read Cube
        api_response = api_instance.read_cube_cubes_name_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_cube_cubes_name_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_cube_cubes_name_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_cube_cubes_name_get.ApiResponseFor422) | Validation Error

#### read_cube_cubes_name_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**CubeRevisionMetadata**](../../models/CubeRevisionMetadata.md) |  | 


#### read_cube_cubes_name_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_databases_databases_get**
<a name="read_databases_databases_get"></a>
> [Database] read_databases_databases_get()

Read Databases

List the available databases.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.database import Database
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Read Databases
        api_response = api_instance.read_databases_databases_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_databases_databases_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_databases_databases_get.ApiResponseFor200) | Successful Response

#### read_databases_databases_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**Database**]({{complexTypePrefix}}Database.md) | [**Database**]({{complexTypePrefix}}Database.md) | [**Database**]({{complexTypePrefix}}Database.md) |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_metric_metrics_name_get**
<a name="read_metric_metrics_name_get"></a>
> Metric read_metric_metrics_name_get(name)

Read Metric

Return a metric by name.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.metric import Metric
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    try:
        # Read Metric
        api_response = api_instance.read_metric_metrics_name_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_metric_metrics_name_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_metric_metrics_name_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_metric_metrics_name_get.ApiResponseFor422) | Validation Error

#### read_metric_metrics_name_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Metric**](../../models/Metric.md) |  | 


#### read_metric_metrics_name_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_metrics_metrics_get**
<a name="read_metrics_metrics_get"></a>
> [Metric] read_metrics_metrics_get()

Read Metrics

List all available metrics.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.metric import Metric
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Read Metrics
        api_response = api_instance.read_metrics_metrics_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_metrics_metrics_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_metrics_metrics_get.ApiResponseFor200) | Successful Response

#### read_metrics_metrics_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**Metric**]({{complexTypePrefix}}Metric.md) | [**Metric**]({{complexTypePrefix}}Metric.md) | [**Metric**]({{complexTypePrefix}}Metric.md) |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_metrics_sql_metrics_name_sql_get**
<a name="read_metrics_sql_metrics_name_sql_get"></a>
> TranslatedSQL read_metrics_sql_metrics_name_sql_get(name)

Read Metrics Sql

Return SQL for a metric.  A database can be optionally specified. If no database is specified the optimal one will be used.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.translated_sql import TranslatedSQL
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    query_params = {
    }
    try:
        # Read Metrics Sql
        api_response = api_instance.read_metrics_sql_metrics_name_sql_get(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_metrics_sql_metrics_name_sql_get: %s\n" % e)

    # example passing only optional values
    path_params = {
        'name': "name_example",
    }
    query_params = {
        'dimensions': [],
        'filters': [],
        'database_name': "database_name_example",
        'check_database_online': True,
    }
    try:
        # Read Metrics Sql
        api_response = api_instance.read_metrics_sql_metrics_name_sql_get(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_metrics_sql_metrics_name_sql_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
dimensions | DimensionsSchema | | optional
filters | FiltersSchema | | optional
database_name | DatabaseNameSchema | | optional
check_database_online | CheckDatabaseOnlineSchema | | optional


# DimensionsSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
items | str,  | str,  |  | 

# FiltersSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
items | str,  | str,  |  | 

# DatabaseNameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# CheckDatabaseOnlineSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bool,  | BoolClass,  |  | if omitted the server will use the default value of True

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_metrics_sql_metrics_name_sql_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_metrics_sql_metrics_name_sql_get.ApiResponseFor422) | Validation Error

#### read_metrics_sql_metrics_name_sql_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TranslatedSQL**](../../models/TranslatedSQL.md) |  | 


#### read_metrics_sql_metrics_name_sql_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_metrics_sql_query_validate_post**
<a name="read_metrics_sql_query_validate_post"></a>
> TranslatedSQL read_metrics_sql_query_validate_post(djsql)

Read Metrics Sql

Return SQL for a DJ Query.  A database can be optionally specified. If no database is specified the optimal one will be used.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.djsql import DJSQL
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.translated_sql import TranslatedSQL
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    body = DJSQL(
        database_name="database_name_example",
        sql="sql_example",
    )
    try:
        # Read Metrics Sql
        api_response = api_instance.read_metrics_sql_query_validate_post(
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_metrics_sql_query_validate_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**DJSQL**](../../models/DJSQL.md) |  | 


### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_metrics_sql_query_validate_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_metrics_sql_query_validate_post.ApiResponseFor422) | Validation Error

#### read_metrics_sql_query_validate_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TranslatedSQL**](../../models/TranslatedSQL.md) |  | 


#### read_metrics_sql_query_validate_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_node_nodes_name_get**
<a name="read_node_nodes_name_get"></a>
> NodeOutput read_node_nodes_name_get(name)

Read Node

Show the active version of the specified node.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.node_output import NodeOutput
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    try:
        # Read Node
        api_response = api_instance.read_node_nodes_name_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_node_nodes_name_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_node_nodes_name_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_node_nodes_name_get.ApiResponseFor422) | Validation Error

#### read_node_nodes_name_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**NodeOutput**](../../models/NodeOutput.md) |  | 


#### read_node_nodes_name_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_nodes_nodes_get**
<a name="read_nodes_nodes_get"></a>
> [NodeOutput] read_nodes_nodes_get()

Read Nodes

List the available nodes.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.node_output import NodeOutput
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example, this endpoint has no required or optional parameters
    try:
        # Read Nodes
        api_response = api_instance.read_nodes_nodes_get()
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_nodes_nodes_get: %s\n" % e)
```
### Parameters
This endpoint does not need any parameter.

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_nodes_nodes_get.ApiResponseFor200) | Successful Response

#### read_nodes_nodes_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**NodeOutput**]({{complexTypePrefix}}NodeOutput.md) | [**NodeOutput**]({{complexTypePrefix}}NodeOutput.md) | [**NodeOutput**]({{complexTypePrefix}}NodeOutput.md) |  | 

### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **read_tag_tags_name_get**
<a name="read_tag_tags_name_get"></a>
> Tag read_tag_tags_name_get(name)

Read Tag

Return a tag by name.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.tag import Tag
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    try:
        # Read Tag
        api_response = api_instance.read_tag_tags_name_get(
            path_params=path_params,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->read_tag_tags_name_get: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#read_tag_tags_name_get.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#read_tag_tags_name_get.ApiResponseFor422) | Validation Error

#### read_tag_tags_name_get.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Tag**](../../models/Tag.md) |  | 


#### read_tag_tags_name_get.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **update_node_nodes_name_patch**
<a name="update_node_nodes_name_patch"></a>
> NodeOutput update_node_nodes_name_patch(nameupdate_node)

Update Node

Update a node.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.node_output import NodeOutput
from djclient.model.update_node import UpdateNode
from djclient.model.http_validation_error import HTTPValidationError
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    body = UpdateNode(
        columns=dict(
            "key": SourceNodeColumnType(
                type="type_example",
                dimension="dimension_example",
            ),
        ),
        display_name="display_name_example",
        description="description_example",
        query="query_example",
        mode=NodeMode("published"),
    )
    try:
        # Update Node
        api_response = api_instance.update_node_nodes_name_patch(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->update_node_nodes_name_patch: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**UpdateNode**](../../models/UpdateNode.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#update_node_nodes_name_patch.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#update_node_nodes_name_patch.ApiResponseFor422) | Validation Error

#### update_node_nodes_name_patch.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**NodeOutput**](../../models/NodeOutput.md) |  | 


#### update_node_nodes_name_patch.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **update_tag_tags_name_patch**
<a name="update_tag_tags_name_patch"></a>
> Tag update_tag_tags_name_patch(nameupdate_tag)

Update Tag

Update a tag.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.update_tag import UpdateTag
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.tag import Tag
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    body = UpdateTag(
        description="description_example",
        tag_metadata=dict(),
    )
    try:
        # Update Tag
        api_response = api_instance.update_tag_tags_name_patch(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->update_tag_tags_name_patch: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**UpdateTag**](../../models/UpdateTag.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#update_tag_tags_name_patch.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#update_tag_tags_name_patch.ApiResponseFor422) | Validation Error

#### update_tag_tags_name_patch.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Tag**](../../models/Tag.md) |  | 


#### update_tag_tags_name_patch.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **upsert_node_materialization_config_nodes_name_materialization_post**
<a name="upsert_node_materialization_config_nodes_name_materialization_post"></a>
> bool, date, datetime, dict, float, int, list, str, none_type upsert_node_materialization_config_nodes_name_materialization_post(nameupsert_materialization_config)

Upsert Node Materialization Config

Update materialization config of the specified node.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.upsert_materialization_config import UpsertMaterializationConfig
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'name': "name_example",
    }
    body = UpsertMaterializationConfig(
        engine_name="engine_name_example",
        engine_version="engine_version_example",
        config="config_example",
    )
    try:
        # Upsert Node Materialization Config
        api_response = api_instance.upsert_node_materialization_config_nodes_name_materialization_post(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->upsert_node_materialization_config_nodes_name_materialization_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**UpsertMaterializationConfig**](../../models/UpsertMaterializationConfig.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
name | NameSchema | | 

# NameSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#upsert_node_materialization_config_nodes_name_materialization_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#upsert_node_materialization_config_nodes_name_materialization_post.ApiResponseFor422) | Validation Error

#### upsert_node_materialization_config_nodes_name_materialization_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

#### upsert_node_materialization_config_nodes_name_materialization_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **validate_node_nodes_validate_post**
<a name="validate_node_nodes_validate_post"></a>
> NodeValidation validate_node_nodes_validate_post(any_type)

Validate Node

Validate a node.

### Example

```python
import djclient
from djclient.apis.tags import default_api
from djclient.model.node_revision import NodeRevision
from djclient.model.node_revision_base import NodeRevisionBase
from djclient.model.http_validation_error import HTTPValidationError
from djclient.model.node_validation import NodeValidation
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = djclient.Configuration(
    host = "http://localhost"
)

# Enter a context with an instance of the API client
with djclient.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = default_api.DefaultApi(api_client)

    # example passing only required values which don't have defaults set
    body = None
    try:
        # Validate Node
        api_response = api_instance.validate_node_nodes_validate_post(
            body=body,
        )
        pprint(api_response)
    except djclient.ApiException as e:
        print("Exception when calling DefaultApi->validate_node_nodes_validate_post: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict, str, date, datetime, uuid.UUID, int, float, decimal.Decimal, bool, None, list, tuple, bytes, io.FileIO, io.BufferedReader,  | frozendict.frozendict, str, decimal.Decimal, BoolClass, NoneClass, tuple, bytes, FileIO |  | 

### Composed Schemas (allOf/anyOf/oneOf/not)
#### anyOf
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[NodeRevisionBase]({{complexTypePrefix}}NodeRevisionBase.md) | [**NodeRevisionBase**]({{complexTypePrefix}}NodeRevisionBase.md) | [**NodeRevisionBase**]({{complexTypePrefix}}NodeRevisionBase.md) |  | 
[NodeRevision]({{complexTypePrefix}}NodeRevision.md) | [**NodeRevision**]({{complexTypePrefix}}NodeRevision.md) | [**NodeRevision**]({{complexTypePrefix}}NodeRevision.md) |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#validate_node_nodes_validate_post.ApiResponseFor200) | Successful Response
422 | [ApiResponseFor422](#validate_node_nodes_validate_post.ApiResponseFor422) | Validation Error

#### validate_node_nodes_validate_post.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**NodeValidation**](../../models/NodeValidation.md) |  | 


#### validate_node_nodes_validate_post.ApiResponseFor422
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor422ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor422ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**HTTPValidationError**](../../models/HTTPValidationError.md) |  | 


### Authorization

No authorization required

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

