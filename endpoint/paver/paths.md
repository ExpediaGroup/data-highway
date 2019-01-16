## Paths
### Lists all road names
```
GET /roads
```

#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|List of all road names|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* road

### Creates a new road
```
POST /roads
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|BodyParameter|road|road|true|BasicRoadModel||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Request to create a road received.|StandardResponse|
|400|Invalid request or road name.|StandardResponse|
|409|Road already exists.|StandardResponse|
|415|Unsupported media type. Only 'application/json' content type is supported.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* road

### Returns the details of a road
```
GET /roads/{name}
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Details of a road returned successfully.|StandardResponse|
|400|Invalid request or road name.|StandardResponse|
|404|Road not found.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* road

### Applies JSON Patch modifications to a road
```
PUT /roads/{name}
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||
|BodyParameter|patchSet|patchSet|true|PatchOperation array||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Patch applied successfully|StandardResponse|
|400|Invalid patch|StandardResponse|


#### Consumes

* application/json-patch+json

#### Produces

* */*

#### Tags

* road

### Applies JSON Patch modifications to a road
```
PATCH /roads/{name}
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||
|BodyParameter|patchSet|patchSet|true|PatchOperation array||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Patch applied successfully|StandardResponse|
|400|Invalid patch|StandardResponse|


#### Consumes

* application/json-patch+json

#### Produces

* */*

#### Tags

* road

### Returns details of a Hive destination
```
GET /roads/{name}/destinations/hive
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|name|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Details of a Hive destination.|StandardResponse|
|404|Road or Hive destination not found.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* hive

### Creates a new Hive destination
```
POST /roads/{name}/destinations/hive
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|name|true|string||
|BodyParameter|hiveDestinationModel|hiveDestinationModel|true|HiveDestinationModel||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Creation of a new Hive destination requested.|StandardResponse|
|404|Road not found.|StandardResponse|
|409|Hive destination already exists.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* hive

### Updates an existing Hive destination
```
PUT /roads/{name}/destinations/hive
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|name|true|string||
|BodyParameter|hiveDestinationModel|hiveDestinationModel|true|HiveDestinationModel||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Update of existing Hive destination requested.|StandardResponse|
|404|Road or Hive destination not found.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* hive

### Returns all schemas keyed by version number
```
GET /roads/{name}/schemas
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|name|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|List of schemas returned successfully.|StandardResponse|
|404|Road not found.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* schema

### Adds a new schema to a road
```
POST /roads/{name}/schemas
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||
|BodyParameter|schema|schema|true|Schema||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Request to add a new schema received.|StandardResponse|
|404|Road not found.|StandardResponse|
|409|Schema is not compatible with previous schemas.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* schema

### Returns a specific version of a schema
```
GET /roads/{name}/schemas/{version}
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||
|PathParameter|version|version|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Requested schema returned successfully.|StandardResponse|
|404|Road or schema not found.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* schema

### Adds a new schema with a specific version to a road
```
POST /roads/{name}/schemas/{version}
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||
|PathParameter|version|version|true|integer (int32)||
|BodyParameter|schema|schema|true|Schema||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Request to add a new schema received.|StandardResponse|
|400|Invalid Schema Version. The requested version must be greater than the largest registered version.|StandardResponse|
|404|Road not found.|StandardResponse|
|409|Schema is not compatible with previous schemas.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* schema

### Deletes the latest version of a schema
```
DELETE /roads/{name}/schemas/{version}
```

#### Parameters
|Type|Name|Description|Required|Schema|Default|
|----|----|----|----|----|----|
|PathParameter|name|road name|true|string||
|PathParameter|version|version|true|string||


#### Responses
|HTTP Code|Description|Schema|
|----|----|----|
|200|Request to delete a schema received.|StandardResponse|
|400|Invalid Schema Version. Only the latest schema can be deleted.|StandardResponse|
|404|Road or Schema not found.|StandardResponse|


#### Consumes

* application/json

#### Produces

* */*

#### Tags

* schema

