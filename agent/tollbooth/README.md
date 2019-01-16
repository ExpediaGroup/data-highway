# Road Tollbooth

Tollbooth is a Spring Boot application that applies updates to road models.

Road models are stored as JSON documents on a [compacted](https://kafka.apache.org/documentation/#compaction) Kafka
topic. Updates come in the form of [JSON Patch](http://jsonpatch.com/) documents which are read from another Kafka
topic. These patch updates are applied to the complete model JSON documents and persisted back to the compacted model
Kafka topic.

Updates come from [Paver](../../endpoint/paver) and Agents such as [Traffic Control](../traffic-control) and
[Loading Bay](../loading-bay) agents.

On startup, Tollbooth will create the necessary topics, if required. If the road model topic already exists, it is
checked for validity. It must have only a single partition and have the `compact` retention policy. If the topic does
not have either of these properties, Tollbooth will fail to start.

## Configuration Options

All arguments without a default are mandatory.

| Argument                 | Default | Description
|---                       |---      |---
| `kafka.zookeeper`        | -       | Connection string for Zookeeper to allow creation of Kafka topics. In Kubernetes, a typical value would be `zookeeper:2181`. It should be the same value as used to configure the Kafka brokers.
| `kafka.bootstrapServers` | -       | Connection string for Kafka to allow reading and writing of models and reading modifications. In Kubernetes, a typical value would be `kafka:9092`.
| `kafka.store.topic`      | -       | The topic name of the compacted topic that stores road model data.
| `kafka.store.replicas`   | 3       | The replication factor for the road model topic.
| `kafka.patch.topic`      | -       | The topic name of the topic that stores road model updates.
| `kafka.patch.groupId`    | -       | The consumer group id for the patch topic.
| `kafka.patch.partitions` | 1       | The number of Kafka partitions for the patch topic.
| `kafka.patch.replicas`   | 3       | The replication factor for the patch topic.
