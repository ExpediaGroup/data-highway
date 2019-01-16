# DRAFT

# ROAD Client

A library providing a client that can be used to send messages onto a road via
Onramp.

Two interfaces and implementations are provided `RoadClient` and
`AsyncRoadClient`.

## `RoadClient`

Create a client.

```java
SimpleRoadClient<SimpleModel> client = new SimpleRoadClient<>(host, roadName, threads);
```

Send some messages. The call to `sendMessages` will block until the messages are
sent. The `responses` object returned will tell for each message what the result
was. If `StandardResponse.isSuccess()` is `true` then the message was sent. If
`false` then the message was not sent and `StandardResponse.getMessage()`
returns a String describing the cause of the failure.

```java
List<SimpleModel> messages = getMessages();
List<StandardResponse> responses = client.sendMessages(messages);
```

Finally close the client. `SimpleRoadClient` implements `AutoClosable` so, if
created as a Spring Bean, Spring will take care of closing the client for you.

```java
client.close();
```

It is safe to call `sendMessage` and `sendMessages` from multiple threads.
Although if used across threads the order of messages cannot be guaranteed.

## `AsyncRoadClient`

Asynchronous clients are created using the `PartitioningRoadClientBuilder`
class.

```java
AsyncRoadClient<String> client = new PartitioningRoadClientBuilder(host, roadName)
    .withThreads(threads)
    .withBufferSize(bufferSize)
    .withMaxBatchSize(maxBatchSize)
    .withObjectMapper(objectMapper)
    .build();
```

By default the client will always return immediately, possibly with an
exceptionally completed Future. You can change this behaviour to have
`sendMessage()` and `sendMessages()` block when the buffer is full by calling
the `blockOnFullQueue()` method on the builder.
