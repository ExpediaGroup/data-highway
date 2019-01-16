# Traffic Control

`road-kafka-agent` is a Data Highway Agent that uses the `road-traffic-cop` framework. It responds to changes in and
periodically inspects road models to determine if any action needs to be taken.

Specifically, it ensures that a [Kafka](https://kafka.apache.org/) topic exists for all roads. It then updates the
model with the current status of whether the `topicExists`, the number of `partitions` and the `replicationFactor`.

The agent reads from `/topic` and reads from and writes to `/status` in the model. Below is an example of the model
subset that the agent uses with example values:

```
{
  "topicName": "road.roadName",
  "status": {
    "topicCreated": true,
    "partitions": 6,
    "replicationFactor": 3,
    "message": "Error creating Kafka topic \"exceptionMessage\""
  }
}
```

## Configuration Options

All arguments without a default are mandatory.

| Argument                          | Default              | Description
|---                                |---                   |---
| `kafka.zookeeper`                 | -                    | Connection string for Zookeeper to allow creation of Kafka topics. In Kubernetes, a typical value would be `zookeeper:2181`. It should be the same value as used to configure the Kafka brokers.
| `kafka.sessionTimeout`            | 60000 (milliseconds) | Zookeeper connection option.
| `kafka.connectionTimeout`         | 60000 (milliseconds) | Zookeeper connection option.
| `kafka.zkSecurityEnabled`         | false                | Zookeeper connection option.
| `kafka.bootstrapServers`          | -                    | Connection string for Kafka to allow reading of models and writing modifications. In Kubernetes, a typical value would be `kafka:9092`.
| `kafka.store.topic`               | -                    | Kafka topic for reading models.
| `kafka.modification.topic`        | -	                   | Kafka topic for sending modifications.
| `kafka.default.topicConfig`       | -                    | Additional properties to assign to Kakfa topics on creation.
| `kafka.default.partitions`        | 6                    | The number of partitions to create Kafka topics with.
| `kafka.default.replicationFactor` | 3                    | The replication factor to create Kafka topics with.
| `graphite.endpoint`               | disabled             | Graphite host and port for sending metrics. Disabled by default.

## Rest Endpoint

The Agent includes a REST endpoint that returns the models for all roads.

```
GET /
{ ... }
```
