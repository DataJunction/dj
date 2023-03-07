# djclient.model.node_revision_base.NodeRevisionBase

A base node revision.

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | A base node revision. | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**name** | str,  | str,  |  | 
**type** | [**NodeType**](NodeType.md) | [**NodeType**](NodeType.md) |  | 
**display_name** | str,  | str,  |  | [optional] 
**description** | str,  | str,  |  | [optional] if omitted the server will use the default value of ""
**query** | str,  | str,  |  | [optional] 
**mode** | [**NodeMode**](NodeMode.md) | [**NodeMode**](NodeMode.md) |  | [optional] 
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

