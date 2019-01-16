# Road Truck Park

Truck Park is an application that is instructed to consume a batch of a road's data from Kafka up to a given set of
partition offsets and stream it to S3 in Avro format.

## Configuration Options

Arguments without a default are mandatory.

| Argument                          | Default           | Description
|---                                |---                |---
| `kafka.bootstrapServers`          |                   | Kafka bootstrap servers for data to consume.
| `kafka.pollTimeout`               | 100 (ms)          | The time, in milliseconds, spent waiting in poll if data is not available in the buffer.
| `road.name`                       |                   | The road name.
| `road.topic`                      |                   | Kafka topic where road message data is stored.
| `road.offsets`                    |                   | key/value delimited string of kafka partitions and end offset to process to.
| `road.model.topic`                |                   | Kafka topic where road model data is stored. Used for schema lookup.
| `writer.flushBytesThreshold`      | 134217728 (128Mi) | Byte threshold at which a file is closed and a new file started.
| `avroCodec.name`                  | deflate           | Avro compression codec name.
| `avroCodec.level`                 | 3                 | Avro compression level, if applicable.
| `s3.bucket`                       |                   | S3 bucket to upload data to.
| `s3.prefix`                       |                   | S3 key prefix (or 'directory') for data being uploaded.
| `s3.partSize`                     | 5242880 (5Mi)     | Size of individual parts in S3 multipart upload.
| `s3.retry.maxAttempts`            | 3                 | Maximum number of retries to attempt individual part uploads.
| `s3.retry.sleepSeconds`           | 1                 | Number of seconds to sleep between retry attempts.
| `s3.async.poolSize`               | 3                 | Fixed number of threads available for concurrent uploads.
| `s3.async.queueSize`              | 3                 | Fixed queue size of part uploads waiting to execute.
| `s3.endpoint.url`                 | -                 | Location of S3 endpoint for data landing.
| `s3.endpoint.signingRegion`       | -                 | Signing region of S3 endpoint for data landing.
| `metrics.graphiteEndpoint`        | disabled          | Graphite instance to send metrics to.

## Example YAML

```
kafka.bootstrapServers: kafka:9092
road:
  name: my_road
  topic: _roads.my_road
  offsets: 0:123,234;1:124,235
  model.topic: _roads
s3:
  bucket: my-bucket
  prefix: data
metrics.graphiteEndpoint: graphite:2003
```
