# djopenapi.model.node_revision_output.NodeRevisionOutput

Output for a node revision with information about columns and if it is a metric.

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | Output for a node revision with information about columns and if it is a metric. | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**mode** | [**NodeMode**](NodeMode.md) | [**NodeMode**](NodeMode.md) |  | 
**node_revision_id** | decimal.Decimal, int,  | decimal.Decimal,  |  | 
**updated_at** | str, datetime,  | str,  |  | value must conform to RFC-3339 date-time
**[columns](#columns)** | list, tuple,  | tuple,  |  | 
**name** | str,  | str,  |  | 
**display_name** | str,  | str,  |  | 
**type** | [**NodeType**](NodeType.md) | [**NodeType**](NodeType.md) |  | 
**version** | str,  | str,  |  | 
**[materialization_configs](#materialization_configs)** | list, tuple,  | tuple,  |  | 
**node_id** | decimal.Decimal, int,  | decimal.Decimal,  |  | 
**status** | [**NodeStatus**](NodeStatus.md) | [**NodeStatus**](NodeStatus.md) |  | 
**catalog** | [**Catalog**](Catalog.md) | [**Catalog**](Catalog.md) |  | [optional] 
**schema_** | str,  | str,  |  | [optional] 
**table** | str,  | str,  |  | [optional] 
**description** | str,  | str,  |  | [optional] if omitted the server will use the default value of ""
**query** | str,  | str,  |  | [optional] 
**availability** | [**AvailabilityState**](AvailabilityState.md) | [**AvailabilityState**](AvailabilityState.md) |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# columns

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**ColumnOutput**](ColumnOutput.md) | [**ColumnOutput**](ColumnOutput.md) | [**ColumnOutput**](ColumnOutput.md) |  | 

# materialization_configs

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**MaterializationConfigOutput**](MaterializationConfigOutput.md) | [**MaterializationConfigOutput**](MaterializationConfigOutput.md) | [**MaterializationConfigOutput**](MaterializationConfigOutput.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

