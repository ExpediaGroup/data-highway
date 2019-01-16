# SNS Notifications

SNS notifications is a component of Data Highway responsible for sending Data Highway notifications via Amazon Simple Notification Service (SNS).

## JSON Messages

Currently 5 types of notifications are supported.

### Road Created Notification

#### Fields

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version)|
|`type`|String Enum Value|Always|ROAD_CREATED|
|`roadName`|String|Always|Name of the road|
|`description`|String|Always|Description of the road|
|`teamName`|String|Always|Name of the team responsible for the road|
|`contactEmail`|String|Always|Contact email of the team responsible for the road|

#### Example message

```
{
  "protocolVersion" : "1.0",
  "type" : "ROAD_CREATED",
  "roadName" : "test_road",
  "description" : "This is a test road.",
  "teamName" : "TEAM",
  "contactEmail" : "team@company.com"
}
```

### Schema Version Added Notification

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version)|
|`type`|String Enum Value|Always|SCHEMA_VERSION_ADDED|
|`roadName`|String|Always|Name of the road|
|`schemaVersion`|String|Always|The Avro schema version|

#### Example message

```
{
  "protocolVersion" : "1.0",
  "type" : "SCHEMA_VERSION_ADDED",
  "roadName" : "test_road",
  "schemaVersion" : 5
}
```

### Schema Version Deleted Notification

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version)|
|`type`|String Enum Value|Always|SCHEMA_VERSION_DELETED|
|`roadName`|String|Always|Name of the road|
|`schemaVersion`|String|Always|The Avro schema version|

#### Example message

```
{
  "protocolVersion" : "1.0",
  "type" : "SCHEMA_VERSION_DELETED",
  "roadName" : "test_road",
  "schemaVersion" : 5
}
```

### Hive Table Created Notification

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version)|
|`type`|String Enum Value|Always|HIVE_TABLE_CREATED|
|`roadName`|String|Always|Name of the road|
|`databaseName`|String|Always|Name of the Hive database|
|`tableName`|String|Always|Name of the Hive table that was created|
|`partitionColumnName`|String|Always|The name of the Hive partition column|
|`metastoreUris`|String|Always|The Hive metastore URIs of the Hive Thrift metastore where the table was created|
|`baseLocation`|String|Always|The base location of the Hive table|

#### Example message

```
{
  "protocolVersion" : "1.0",
  "type" : "HIVE_TABLE_CREATED",
  "roadName" : "test_road",
  "databaseName" : "test_database",
  "tableName" : "test_table",
  "partitionColumnName" : "acquisition_instant",
  "metastoreUris" : "thrift://hive-metastore-host:9083",
  "baseLocation" : "s3://bucket/roads/test_road"
}
```

### Hive Partition Created Notification

|Field Name|Type|When present|Description|
|----|----|----|----|
|`protocolVersion`|String|Always|The [semantic version number](https://semver.org/) of the message in MAJOR.MINOR format (i.e. omitting the PATCH version)|
|`type`|String Enum Value|Always|HIVE_PARTITION_CREATED|
|`roadName`|String|Always|Name of the road|
|`databaseName`|String|Always|The Hive database|
|`tableName`|String|Always|Name of the Hive table that the partition was added to|
|`partitionSpec`|String|Always|Hive partition specification|
|`metastoreUris`|String|Always|The Hive metastore URIs of the Hive Thrift metastore where the partition was added|
|`locationUri`|String|Always|The partition location|
|`recordCount`|Long|Always|Number of records in the partition|

#### Example message

```
{
  "protocolVersion" : "1.0",
  "type" : "HIVE_PARTITION_CREATED",
  "roadName" : "test_road",
  "databaseName" : "test_database",
  "tableName" : "test_table",
  "partitionColumnName" : "acquisition_instant",
  "metastoreUris" : "thrift://hive-metastore-host:9083",
  "locationUri" : "s3://bucket/roads/test_road/acquisition_instant=20180226T082805Z",
  "recordCount" : 25189150
}
```
