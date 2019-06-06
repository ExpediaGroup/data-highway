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
package com.hotels.road.highwaypatrol;

import static java.util.concurrent.TimeUnit.SECONDS;

import static com.google.common.base.Predicates.not;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;

import com.hotels.road.offramp.client.Commits;
import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.model.Message;

@Slf4j
@Component
public class Receiver implements Subscriber<Message<TestMessage>>, AutoCloseable {
  private final @NonNull TestMessageContextManager contextManager;
  private final @NonNull String origin;
  private final @NonNull OfframpClient<TestMessage> offramp;
  private final @NonNull MessagesMetricSet metrics;
  private Subscription subscription;

  private final DirectProcessor<Message<?>> commitProcessor = DirectProcessor.create();

  public Receiver(
      @NonNull MessagesMetricSet metrics,
      @NonNull TestMessageContextManager contextManager,
      @NonNull @Value("${originName}") String origin,
      @NonNull OfframpClient<TestMessage> offramp) throws IOException {
    this.metrics = metrics;
    this.contextManager = contextManager;
    this.origin = origin;
    this.offramp = offramp;
  }

  public void start() throws InterruptedException {
    log.debug("Starting receiver");

    CountDownLatch latch = new CountDownLatch(1);

    Flux.from(offramp.rebalances()).any(not(Set::isEmpty)).subscribe(b -> latch.countDown());

    offramp.messages().subscribe(this);

    Commits.fromMessages(commitProcessor, Duration.ofSeconds(1)).flatMap(offramp::commit).subscribe();

    if (!latch.await(30, SECONDS)) {
      throw new RuntimeException("Timed out waiting for a rebalance event to start the receiver");
    }

    log.info("Receiver started");
  }

  @Override
  public void close() throws Exception {
    log.debug("Shutting down receiver");
    subscription.cancel();
    log.info("Receiver shutdown");
  }

  @Override
  public void onSubscribe(Subscription s) {
    subscription = s;
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Message<TestMessage> message) {
    TestMessage payload = message.getPayload();
    if (!origin.equals(payload.getOrigin())) {
      return;
    }
    TestMessageContext context = contextManager.getContext(payload);
    if (context == null) {
      log.debug("Received a message without a context: {}", payload);
      return;
    }
    context.messageReceived(payload);
    commitProcessor.onNext(message);
  }

  @Override
  public void onError(Throwable t) {
    log.warn("Exception getting message", t);
    metrics.markReceiverError();
  }

  @Override
  public void onComplete() {
    log.warn("Should never complete!");
  }
}
