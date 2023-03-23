# djclient.model.attribute_type.AttributeType

Available attribute types for column metadata.

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | Available attribute types for column metadata. | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**[allowed_node_types](#allowed_node_types)** | list, tuple,  | tuple,  |  | 
**name** | str,  | str,  |  | 
**namespace** | str,  | str,  |  | 
**description** | str,  | str,  |  | 
**[uniqueness_scope](#uniqueness_scope)** | list, tuple,  | tuple,  |  | [optional] 
**id** | decimal.Decimal, int,  | decimal.Decimal,  |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# allowed_node_types

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**NodeType**](NodeType.md) | [**NodeType**](NodeType.md) | [**NodeType**](NodeType.md) |  | 

# uniqueness_scope

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**UniquenessScope**](UniquenessScope.md) | [**UniquenessScope**](UniquenessScope.md) | [**UniquenessScope**](UniquenessScope.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

