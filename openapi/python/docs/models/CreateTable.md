# djclient.model.create_table.CreateTable

Create table input

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | Create table input | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**catalog_name** | str,  | str,  |  | 
**database_name** | str,  | str,  |  | 
**[columns](#columns)** | list, tuple,  | tuple,  |  | 
**table** | str,  | str,  |  | 
**schema** | str,  | str,  |  | [optional] 
**cost** | decimal.Decimal, int, float,  | decimal.Decimal,  |  | [optional] if omitted the server will use the default value of 1.0
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# columns

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
list, tuple,  | tuple,  |  | 

### Tuple Items
Class Name | Input Type | Accessed Type | Description | Notes
------------- | ------------- | ------------- | ------------- | -------------
[**CreateColumn**](CreateColumn.md) | [**CreateColumn**](CreateColumn.md) | [**CreateColumn**](CreateColumn.md) |  | 

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

