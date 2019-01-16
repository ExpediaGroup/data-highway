# Offramp v2

Offramp provides a service and Java client for consuming data from Data Highway roads. V2 has been built using web
sockets and conforms to the [Reactive Streams](http://www.reactive-streams.org/) standard (now part of Java 9's
[java.util.concurrent.Flow](https://docs.oracle.com/javase/9/docs/api/java/util/concurrent/Flow.html)).

The service also supports automatic rebalancing, so each concurrent consumer of a stream is automatically assigned some
partitions to consume from. Whenever a consumer leaves or joins the stream, the partitions get rebalanced between all
the active consumers.

For details on the service, please see the [README](../../endpoint/offramp-v2/README.md).

## Client

The client interface exposes the various event types as
[Publishers](http://www.reactive-streams.org/reactive-streams-1.0.1-javadoc/org/reactivestreams/Publisher.html).
Internally, the implementation uses [reactor-core](https://github.com/reactor/reactor-core).


```
public interface OfframpClient<T> extends AutoCloseable {

  Publisher<Message<T>> messages();
  
  Mono<Boolean> commit(Map<Integer, Long> offsets);

  Publisher<Set<Integer>> rebalances();

}
```

### Configuration Options

| Option              | Requred | Default | Description
|---                  |---      |---      |---
| username            | Y       |         | The username used to authenticate with Data Highway.
| password            | Y       |         | The password used to authenticate with Data Highway.
| host                | Y       |         | The Data Highway host.
| roadName            | Y       |         | The road name.
| streamName          | Y       |         | The stream name. All active consumers of the same stream will have the partitions dynamically allocated between them.
| defaultOffset       | N       | LATEST  | The `DefaultOffset` to start streaming at on each partition if no commit exists. Valid options are `EARLIEST` or `LATEST`.
| grants              | N       | Empty   | The `Set` of data `Sensitivity` types that should be consumed unobfuscated. `PUBLIC` data is always served unobfuscated.
| payloadClass        | Y       |         | The payload type.
| payloadTypeFactory  | N       |         | Allows Jackson to be configured correctly with the payload type. Will only be needed if the payload type itself is a generic type.
| payloadDeserialiser | N       |         | May be needed if any specific code is required for deserialisation to the payload type.
| retry               | Y       | true    | Specifies whether the client should reacquire a connection to offramp and retry when errors occur. Retries are infinite with a backoff of 1 second.

#### Create an `OfframpClient` with `OfframpOptions`

```
    OfframpOptions<JsonNode> options = OfframpOptions
        .builder(JsonNode.class)
        .username(username)
        .password(password)
        .host(host)
        .roadName(roadName)
        .streamName(streamName)
        .defaultOffset(DefaultOffset.LATEST)
        .grants(singleton(PII))
        .payloadTypeFactory(payloadTypeFactory)
        .payloadDeserialiser(payloadDeserialiser)
        .retry(retry)	
        .build();

    OfframpClient<JsonNode> client = OfframpClient.create(options);
```


### Messages

Subscribe to `client.messages()` to receive messages for the given road.

### Commits

Publish to `client.commit(Map<Integer, Long>)` to save the current position in the stream. Complete the `Mono<Boolean>` return value to receive the response.

### Rebalances

Subscribe to `client.rebalances()` to be notified about when partition rebalances occur and what partitions have been
assigned.

## Committer

`Committer` provides a simple mechanism for consumers to notify the service that a given `Message` has been consumed
and and should be marked for committing. When used in conjunction with `OfframpOptions.commitBatchInterval` it provides
an automatic commit strategy whereby all messages can be marked for commit, then every interval those messages will be
batched up - picking the latest offset for each partition, and submitted to the offramp service. `CommitResponses`
coming back are subscribed to so that all `Commits` are verified.

## Consumption Examples

## Examples

### Using Project Reactor
```
    class Example1 {
      static <T> Disposable consume(OfframpClient<T> client, Committer committer) {
        return Flux
            .from(client.messages())
            .doOnNext(m -> {
              // process message
            })
            .doOnNext(committer::commit)
            .subscribe();
      }
    }
```

### Basic Subscriber Impl - Pull messages by message

```
    @RequiredArgsConstructor
    class Example2<T> implements Subscriber<Message<T>> {
      private final Committer committer;
      private Subscription subscription;

      static <T> Example2<T> create(OfframpClient<T> client, Committer committer) {
        Example2<T> example2 = new Example2<>(committer);
        client.messages().subscribe(example2);
        return example2;
      }

      @Override
      public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1L);
      }

      @Override
      public void onNext(Message<T> message) {
        // process message
        committer.commit(message);
        subscription.request(1L);
      }

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    }
```

### Basic Subscriber Impl - Push

```
    @RequiredArgsConstructor
    class Example3<T> implements Subscriber<Message<T>> {
      private final Committer committer;
      private Subscription subscription;

      static <T> Example3<T> create(OfframpClient<T> client, Committer committer) {
        Example3<T> example3 = new Example3<>(committer);
        client.messages().subscribe(example3);
        return example3;
      }

      @Override
      public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      public void onNext(Message<T> message) {
        // process message
        committer.commit(message);
      }

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onComplete() {}
    }
```
