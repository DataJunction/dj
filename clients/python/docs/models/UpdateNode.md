# djclient.model.update_node.UpdateNode

Update node object where all fields are optional

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | Update node object where all fields are optional | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**catalog** | str,  | str,  |  | [optional] 
**schema_** | str,  | str,  |  | [optional] 
**table** | str,  | str,  |  | [optional] 
**[columns](#columns)** | dict, frozendict.frozendict,  | frozendict.frozendict,  |  | [optional] 
**display_name** | str,  | str,  |  | [optional] 
**description** | str,  | str,  |  | [optional] 
**query** | str,  | str,  |  | [optional] 
**mode** | [**NodeMode**](NodeMode.md) | [**NodeMode**](NodeMode.md) |  | [optional] 

# columns

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**any_string_name** | [**SourceNodeColumnType**](SourceNodeColumnType.md) | [**SourceNodeColumnType**](SourceNodeColumnType.md) | any string name can be used but the value must be the correct type | [optional] 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

