# Road Towtruck

`road-towtruck` is a simple backup utility for road model data. It consumes all data on the road model Kafka topic,
compresses it and uploads to s3. It is intended to be run as a job in Kubernetes.

## Configuration Options

All options are mandatory.

| Argument                          | Description
|---                                |---
| `kafka.bootstrapServers`          | Kafka bootstrap servers for data to consume.
| `road.model.topic`                | Kafka topic where road model data is stored.
| `s3.bucket`                       | S3 bucket to upload data to.
| `s3.keyPrefix`                    | S3 key prefix for data being uploaded.
| `s3.endpoint.url`                 | Location of S3 endpoint for model archive.
| `s3.endpoint.signingRegion`       | Signing region of S3 endpoint for model archive.
