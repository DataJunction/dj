# djclient.model.database.Database

A database.  A simple example:      name: druid     description: An Apache Druid database     URI: druid://localhost:8082/druid/v2/sql/     read-only: true     async_: false     cost: 1.0

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  | A database.  A simple example:      name: druid     description: An Apache Druid database     URI: druid://localhost:8082/druid/v2/sql/     read-only: true     async_: false     cost: 1.0 | 

### Dictionary Keys
Key | Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | ------------- | -------------
**name** | str,  | str,  |  | 
**URI** | str,  | str,  |  | 
**id** | decimal.Decimal, int,  | decimal.Decimal,  |  | [optional] 
**uuid** | str, uuid.UUID,  | str,  |  | [optional] value must be a uuid
**description** | str,  | str,  |  | [optional] if omitted the server will use the default value of ""
**[extra_params](#extra_params)** | dict, frozendict.frozendict,  | frozendict.frozendict,  |  | [optional] if omitted the server will use the default value of {}
**read_only** | bool,  | BoolClass,  |  | [optional] if omitted the server will use the default value of True
**async_** | bool,  | BoolClass,  |  | [optional] if omitted the server will use the default value of False
**cost** | decimal.Decimal, int, float,  | decimal.Decimal,  |  | [optional] if omitted the server will use the default value of 1.0
**created_at** | str, datetime,  | str,  |  | [optional] value must conform to RFC-3339 date-time
**updated_at** | str, datetime,  | str,  |  | [optional] value must conform to RFC-3339 date-time
**any_string_name** | dict, frozendict.frozendict, str, date, datetime, int, float, bool, decimal.Decimal, None, list, tuple, bytes, io.FileIO, io.BufferedReader | frozendict.frozendict, str, BoolClass, decimal.Decimal, NoneClass, tuple, bytes, FileIO | any string name can be used but the value must be the correct type | [optional]

# extra_params

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | if omitted the server will use the default value of {}

[[Back to Model list]](../../README.md#documentation-for-models) [[Back to API list]](../../README.md#documentation-for-api-endpoints) [[Back to README]](../../README.md)

