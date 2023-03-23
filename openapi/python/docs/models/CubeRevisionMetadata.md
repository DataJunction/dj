# djopenapi.model.cube_revision_metadata.CubeRevisionMetadata

Metadata for a cube node

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | Metadata for a cube node | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**node_revision_id** | decimal.Decimal, int,  | decimal.Decimal,  |  | 
**updated_at** | str, datetime,  | str,  |  | value must conform to RFC-3339 date-time
**name** | str,  | str,  |  | 
**[cube_elements](#cube_elements)** | list, tuple,  | tuple,  |  | 
**display_name** | str,  | str,  |  | 
**type** | [**NodeType**](NodeType.md) | [**NodeType**](NodeType.md) |  | 
**version** | str,  | str,  |  | 
**node_id** | decimal.Decimal, int,  | decimal.Decimal,  |  | 
**description** | str,  | str,  |  | [optional] if omitted the server will use the default value of ""
**availability** | [**AvailabilityState**](AvailabilityState.md) | [**AvailabilityState**](AvailabilityState.md) |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# cube_elements

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**CubeElementMetadata**](CubeElementMetadata.md) | [**CubeElementMetadata**](CubeElementMetadata.md) | [**CubeElementMetadata**](CubeElementMetadata.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

