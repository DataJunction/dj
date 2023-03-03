# djclient.model.node_validation.NodeValidation

A validation of a provided node definition

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | A validation of a provided node definition | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**node_revision** | [**NodeRevision**](NodeRevision.md) | [**NodeRevision**](NodeRevision.md) |  | 
**[columns](#columns)** | list, tuple,  | tuple,  |  | 
**message** | str,  | str,  |  | 
**[dependencies](#dependencies)** | list, tuple,  | tuple,  |  | 
**status** | [**NodeStatus**](NodeStatus.md) | [**NodeStatus**](NodeStatus.md) |  | 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# dependencies

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**NodeRevisionOutput**](NodeRevisionOutput.md) | [**NodeRevisionOutput**](NodeRevisionOutput.md) | [**NodeRevisionOutput**](NodeRevisionOutput.md) |  | 

# columns

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**Column**](Column.md) | [**Column**](Column.md) | [**Column**](Column.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

