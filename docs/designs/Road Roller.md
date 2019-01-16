# Design Proposal - Road Roller (Compacted Topics)

## Overview

In order to support use cases requiring a way to track change events for a
distributed key value store we will expose a new road type which will use Kafka
Compact Topics. Events sent to this road will differ from a normal road by
having a key as well as the current value. Each Key and Value pair sent to
Onramp will be treated as an update. All updates will be kept for a period of
time and then old values for a particular key will be removed. The latest value
for each unique key will be kept indefinitely.

As an example, three updates with the following values sent to Onramp:

```
{"key": "a", "payload": "one"}
{"key": "b", "payload": "two"}
{"key": "a", "payload": "three"}
```

In the immediate aftermath of those messages a connection to Offramp will
return all three events. After the base retention period for the road has
passed only the following will be returned.

```
{"key": "b", "payload": "two"}
{"key": "a", "payload": "three"}
```

## Interface Design

The model of a road will be extended to include a `type` parameter which will
be a string matching an enum value. Traditional roads will be labelled `STREAM`
road-roller (compacted) roads will be labelled `COMPACTED` and future values
may be added later.

Roads that are labelled with `COMPACTED` will have a different format for
messages on both Onramp and Offramp.

When using Onramp road-roller clients will send an array of Json objects just
as they do now but the format of those objects will not directly map to the
schema. Instead the object will contain two values `key` which will be a string
and will be used as the key of the record and `payload` which can be `null` if
the record is to be deleted or contain a Json object that will be checked
against the schema for the road.

The message from Offramp will be the same as for a `STREAM` road but will also
include a new field `key`. `key` will contain the same value as `key` sent to
Onramp and the existing `payload` field will contain the value passed as
`payload`.

Data written through the "Hive" destination will be written with a modified
schema to wrap the user provided schema inside the `payload` field.

## Implementation Design

Paver, Traffic-Control, Onramp, Offramp, and Loading-Bay will have to be
altered in order to implement this feature.

### Paver

The road model will have to have an enum added as a `type` field with the
values described in the section above. This value cannot be changed once the
road has been created. `partitionPath` also has no effect for `COMPACTED`
roads.

### Traffic Control

When creating Kafka topics Traffic Control will have to set
`log.cleanup.policy=compact` on the topic. We will also need to set a higher
number of partitions since it will be hard/impossible to change this later.

### Onramp

Onramp currently uses the `partitionPath` to extract a value which it then
hashes down to 4 bytes. For `COMPACTED` roads this will have to change to use
the `key` value and replace the value part of the `ProducerRecord` with the
`payload` value from the incoming request.

### Offramp

For Offramp the biggest change should be to add the `key` value to the outgoing
message when the road type is `COMPACTED`. We may also need to add a seek
function so that clients can reset to the beginning of the stream in order to
reload the data.

### Loading Bay

Loading Bay will be modified to write out a modified schema to the Hive
Metastore that includes a nullable version of the road schema as the `payload`
field next to a text field called `key`.

## Use Cases

### Read Only Copy of Data Store

If you have a table on an existing OLTP system and you would like an OLAP
replica then you can capture the change events for the master store and shop
them to a road-roller road. If you then configure a Key/Value store streaming
destination then the Key/Value store becomes your OLAP replica.

### Daily Table Snapshots

There are two ways of getting a snapshot of the key values held on a
road-roller road.

For the first you would land the stream onto Hive/S3 using the Hive Destination
and then trigger a job to roll the last landing into the previous snapshot and
write out as the new snapshot.

For the second you would land into DynamoDB and dump copies of the data from
there at regular intervals.

### Scalable Search Cluster

If you had a collection of documents for which you provide a search service
that needs to scale dynamically with load then you can store the documents on
a road-roller road. When each instance of the search service starts it will
connect to Data Highway and pull the current state of the road, indexing the
documents as it does so. It can then stay up-to-date by remaining subscribed to
the road through Offramp.

Care should be taken in this case to stagger the start times of large numbers
of service instances so as not to become throttled or banned by Offramp.
