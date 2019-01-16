# Loading Bay

`road-loading-bay` is a Data Highway Agent that uses the `road-traffic-cop` framework. It responds to changes in and
periodically inspects road models to determine if any action needs to be taken.

Specifically, if a Hive destination is configured the agent will ensure that the table is created with the correct
schema in the target Hive metastore. It will also create and maintain a set of [Truck Park](../truck-park) instances
in Kubernetes which will sink the road's data to S3 and create Hive partitions.

The agent reads various attributes from the main road model and reads from and writes to `/destinations/hive/status` in
the model.

## Configuration Options

All arguments without a default are mandatory.

| Argument                               | Default   | Description
|---                                     |---        |---
| `configuration.cache.expiry.duration`  | 30        | Duration for which to cache a road's Hive status before re-checking reality.
| `configuration.cache.expiry.time.unit` | SECONDS   | Duration time unit.
| `kafka.connect.host.and.port`          | -         | Location of kafka-connect service endpoint for management of data connectors.
| `graphite.endpoint`                    | -         | Location of graphite endpoint for metrics reporting.
| `hive.metastore.uris`                  | -         | Location of the Hive metastore.
| `hive.database`                        | -         | The Hive database in which to create tables for landed data.
| `hive.table.partition.column`          | -         | The table column on which to partition landed data.
| `hive.table.location.bucket`           | -         | The S3 bucket in which landed data is stored.
| `hive.table.location.prefix`           | -         | The S3 key prefix for landed table data.
| `hive.table.schema.bucket`             | -         | The S3 bucket in which the Avro schema is stored.
| `hive.table.schema.prefix`             | -         | The S3 key prefix for Avro schemas.
| `sns.topic.arn.format`                 | -         | Java message format used to derive the SNS topic ARN. Provides one optional placeholder, representing the road name.
| `s3.endpoint.url`                      | -         | Location of S3 endpoint for data landing.
| `s3.endpoint.signingRegion`            | -         | Signing region of S3 endpoint for data landing.
| `sns.endpoint.url`                     | -         | Location of SNS endpoint for landing notifications.
| `sns.region`                           | us-west-2 | Region in which SNS is operating.
