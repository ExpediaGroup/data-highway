## Definitions
### BasicRoadModel
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|contactEmail|Team's contact email.|true|string||
|description|A concise description of type and source of data available on the road.|true|string||
|enabled|Indicates if the road is enabled.|false|boolean||
|metadata|A map where additional information about the road that does not fit into any other fields can be stored.|false|object||
|name|Road name|true|string||
|partitionPath|The path within a JSON message for partitioning data on the road.|false|string||
|teamName|Team that pushes data onto this road.|true|string||


### Entry«string,JsonNode»
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|key||false|string||
|value||false|JsonNode||


### Field
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|jsonProps||false|object||
|props||false|object||


### HiveDestinationModel
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|enabled|Specifies if the destination is enabled.|false|boolean||
|landingInterval|Specifies how often data is landed to Hive, defaults to "PT1H". The format is an ISO 8601 Duration, see https://en.wikipedia.org/wiki/ISO_8601#Durations. The value must fall between PT5M and P1D|false|string||


### JsonNode
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|array||false|boolean||
|bigDecimal||false|boolean||
|bigInteger||false|boolean||
|bigIntegerValue||false|integer||
|binary||false|boolean||
|binaryValue||false|string (byte) array||
|boolean||false|boolean||
|booleanValue||false|boolean||
|containerNode||false|boolean||
|decimalValue||false|number||
|double||false|boolean||
|doubleValue||false|number (double)||
|elements||false|Iterator«JsonNode»||
|fieldNames||false|Iterator«string»||
|fields||false|Iterator«Entry«string,JsonNode»»||
|floatingPointNumber||false|boolean||
|int||false|boolean||
|intValue||false|integer (int32)||
|integralNumber||false|boolean||
|long||false|boolean||
|longValue||false|integer (int64)||
|missingNode||false|boolean||
|null||false|boolean||
|number||false|boolean||
|numberType||false|enum (INT, LONG, BIG_INTEGER, FLOAT, DOUBLE, BIG_DECIMAL)||
|numberValue||false|Number||
|object||false|boolean||
|pojo||false|boolean||
|textValue||false|string||
|textual||false|boolean||
|valueAsBoolean||false|boolean||
|valueAsDouble||false|number (double)||
|valueAsInt||false|integer (int32)||
|valueAsLong||false|integer (int64)||
|valueAsText||false|string||
|valueNode||false|boolean||


### PatchOperation
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|op||true|enum (add, remove, replace)||
|path||true|string||
|value||true|object||


### RoadModel
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|agentMessages|A map of agents' status or error messages affecting the road.|false|object||
|compatibilityMode|Specifies which compatibility mode is used on the road|false|string||
|contactEmail|Team's contact email.|true|string||
|description|A concise description of type and source of data available on the road.|true|string||
|enabled|Indicates if the road is enabled.|false|boolean||
|metadata|A map where additional information about the road that does not fit into any other fields can be stored.|false|object||
|name|Road name|true|string||
|partitionPath|The path within a JSON message for partitioning data on the road.|false|string||
|roadIntact|Indicates if the road is ready to accept messages.|false|boolean||
|teamName|Team that pushes data onto this road.|true|string||


### Schema
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|aliases||false|string array||
|doc||false|string||
|elementType||false|Schema||
|enumSymbols||false|string array||
|error||false|boolean||
|fields||false|Field array||
|fixedSize||false|integer (int32)||
|fullName||false|string||
|jsonProps||false|object||
|name||false|string||
|namespace||false|string||
|props||false|object||
|type||false|enum (RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, NULL)||
|types||false|Schema array||
|valueType||false|Schema||


### StandardResponse
|Name|Description|Required|Schema|Default|
|----|----|----|----|----|
|message|Contains message that was sent with the response.|false|string||
|success|Indicates if the response was successfull or not.|false|boolean||
|timestamp|Timestamp in milliseconds indicating when the response was sent.|false|integer (int64)||


