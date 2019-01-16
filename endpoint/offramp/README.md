# Offramp v2

Offramp provides a service and Java client for consuming data from Data Highway roads. V2 has been built using web
sockets and conforms to the [Reactive Streams](http://www.reactive-streams.org/) standard (now part of Java 9's
[java.util.concurrent.Flow](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/Flow.html)).

The service also supports automatic rebalancing, so each concurrent consumer of a stream is automatically assigned some
partitions to consume from. Whenever a consumer leaves or joins the stream, the partitions get rebalanced between all
the active consumers.

For details on the client, please see the [README](../../client/offramp-v2/README.md).

## Service

A standard web socket connection can be acquired with the usual upgrade headers at:

```
GET /offramp/v2/roads/{roadName}/streams/{streamName}/messages?defaultOffset={defaultOffset}
```

| Argument             | Default   | Description 
|---                   |---        |---
| `roadName`           | N/A       | The road name
| `streamName`         | N/A       | A unique (per-road) name that all consumers for the stream share
| `defaultOffset`      | `LATEST`  | Defines whether to start streaming at the `EARLIEST` or `LATEST` offset on each partition if no commit exists.

### Client -> Server Events

#### Request

By default, the server will not send any `Messages` until the client requests some. The client can do this by sending a
`Request` containing the count of messages it would like to receive. A client can dynamically switch from a pull
strategy to push by requesting unbounded (`Long.MAX_VALUE`). To revert to a pull strategy, one would have to cancel the
subscription and then resubscribe.

```
{
  "type": "REQUEST",
  "count": long
}
```

#### Cancel

Upon receiving a `Cancel` event the server will complete sending the `Message` it is in the process of sending, if any.
It will then set the remaining number of requested messages to zero and stop, but the connection will remain open,
waiting for further requests.

```
{
  "type": "CANCEL"
}
```

#### Commit

The current position in the stream can be saved by sending a `Commit` containing a `Map` of the partition/offsets to be
committed. Note that the offset to be committed is the next offset that the user wishes to receive, not the last
consumed offset. For example, if offset 10 has been consumed, then offset 11 should be committed. The user can also
assign a unique `correlationId` to the commit. The server will send an asynchronous `CommitResponse` for each commit
which also contains the `correlationId`. This allows the user to correlate the status to the original commit.

```
{
  "type": "COMMIT",
  "correlationId": "string",
  "offsets": {
    "int": long,
    ...
  }
}
```

### Server -> Client Events

#### Connection

Upon establishing a connection, the service will immediately send the client a `Connection` event. This currently
contains only the `agentName` - the name of the offramp instance the client is connection to. The only purpose this serves
is informative to aid investigations.

```
{
  "type": "CONNECTION",
  "agentName": "string"
}
```

#### Message

After a `Request` has been made, the server will start sending up to that count of `Messages`. Each message contains
the `partition` and `offset` of each message that can be used for committing. They also contain the `schema` version.
The schema itself can be retrieved from Paver at `GET /paver/v1/road/{roadName}/schemas/{schema}`. The `payload` will
contain the actual message body.

```
{
  "type": "MESSAGE",
  "partition": int,
  "offset": long,
  "schema": int,
  "payload": {...}
}
```

#### CommitResponse

The server will send an asychronous `CommitResponse` with the status of each `Commit`. The client can correlate them by
matching up the `correlationIds`.

```
{
  "type": "COMMIT_RESPONSE",
  "correlationId": "string",
  "success": boolean
}
```

#### Rebalance

Every time a consumer joins or leaves the stream, a partition rebalance will occur and all active consumers will be
assigned a new set of partitions. Whenever this occurs each consumer will be sent a `Rebalance` which includes the new
`Set` of partitions that have been assigned to it.

```
{
  "type": "REBALANCE",
  "assignment": [int]
}
```
