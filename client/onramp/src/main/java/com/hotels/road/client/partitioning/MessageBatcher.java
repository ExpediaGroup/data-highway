/**
 * Copyright (C) 2016-2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.road.client.partitioning;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;
import java.util.function.Function;

import lombok.extern.slf4j.Slf4j;

/**
 * A Function that given a delegate that takes collections of messages and returns a collection of response will take
 * individual messages and asynchronously return responses as {@link CompletableFuture}s. Passed messages will be passed
 * to the delegate in batches containing all the messages that arrived whilst the last batch was being executed. If the
 * processing time per message is on aggregate faster in larger batches then this function will improve efficiency.
 * There is a maximum batch size to prevent the back pressure causing the batch size to grow uncontrollably. When the
 * number of messages in the next batch exceeds the maximum batch size this function will block until execution on the
 * currently waiting batch starts. If the number of waiting messages is below the maximum threshold then this function
 * returns immediately.
 *
 * @param <MESSAGE> The Function's from type
 * @param <RESPONSE> The Function's to type
 */
@Slf4j
class MessageBatcher<MESSAGE, RESPONSE> implements CloseableFunction<MESSAGE, CompletableFuture<RESPONSE>> {
  private final BlockingQueue<MessageWithCallback<MESSAGE, RESPONSE>> queue;
  private final int maxBatchSize;
  private final Function<List<MESSAGE>, List<RESPONSE>> batchHandler;
  private final EnqueueBehaviour enqueueBehaviour;

  private boolean shutdownFlag = false;
  private final Thread thread;

  public MessageBatcher(
      int bufferSize,
      int maxBatchSize,
      EnqueueBehaviour enqueueBehaviour,
      Function<List<MESSAGE>, List<RESPONSE>> batchHandler) {
    this(bufferSize, maxBatchSize, enqueueBehaviour, batchHandler,
        r -> new Thread(r, "batcher-" + batchHandler.toString()));
  }

  MessageBatcher(
      int bufferSize,
      int maxBatchSize,
      EnqueueBehaviour enqueueBehaviour,
      Function<List<MESSAGE>, List<RESPONSE>> batchHandler,
      ThreadFactory threadFactory) {
    if (bufferSize < maxBatchSize) {
      throw new IllegalArgumentException("maxBatchSize must be less than or equal to bufferSize");
    }
    this.queue = new LinkedBlockingQueue<>(bufferSize);
    this.maxBatchSize = maxBatchSize;
    this.batchHandler = batchHandler;
    this.enqueueBehaviour = enqueueBehaviour;

    thread = threadFactory.newThread(this::processMessages);
    thread.start();
  }

  @Override
  public CompletableFuture<RESPONSE> apply(MESSAGE t) {
    try {
      return enqueueBehaviour.enqueueMessage(queue, t);
    } catch (Exception e) {
      CompletableFuture<RESPONSE> callback = new CompletableFuture<>();
      callback.completeExceptionally(e);
      return callback;
    }
  }

  @Override
  public void close() throws InterruptedException {
    shutdownFlag = true;
    thread.join();
  }

  private void processMessages() {
    while (!shutdownFlag) {
      try {
        List<MessageWithCallback<MESSAGE, RESPONSE>> buffer = new ArrayList<>();
        MessageWithCallback<MESSAGE, RESPONSE> message = queue.poll(100, MILLISECONDS);
        if (message != null) {
          buffer.add(message);
          queue.drainTo(buffer, maxBatchSize - 1);
          handleBatch(buffer);
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        log.warn("Unhandled exception in message sending thread. Thread shutting down", e);
        throw e;
      }
    }
  }

  private void handleBatch(List<MessageWithCallback<MESSAGE, RESPONSE>> batch) {
    try {
      List<MESSAGE> messages = batch.stream().map(MessageWithCallback::getMessage).collect(toList());
      List<RESPONSE> responses = batchHandler.apply(messages);
      zipConsume(batch, responses, (message, response) -> message.getCallback().complete(response));
    } catch (Exception e) {
      failBatch(batch, e);
    }
  }

  private <A, B> void zipConsume(Iterable<A> a, Iterable<B> b, BiConsumer<A, B> consumer) {
    Iterator<A> iterA = a.iterator();
    Iterator<B> iterB = b.iterator();
    while (iterA.hasNext() && iterB.hasNext()) {
      consumer.accept(iterA.next(), iterB.next());
    }
  }

  private void failBatch(List<MessageWithCallback<MESSAGE, RESPONSE>> batch, Exception e) {
    for (MessageWithCallback<MESSAGE, RESPONSE> message : batch) {
      message.getCallback().completeExceptionally(e);
    }
  }
}
