/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.road.offramp.client;

import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.offramp.model.Cancel;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.offramp.model.Request;

@Slf4j
@RequiredArgsConstructor
class MessageProcessor<T> implements Processor<Message<T>, Message<T>>, Subscription {
  private final EventSender eventSender;
  private Subscriber<? super Message<T>> subscriber;

  @Override
  public void subscribe(Subscriber<? super Message<T>> subscriber) {
    this.subscriber = new FirstErrorOnlySubscriber<>(subscriber);
    this.subscriber.onSubscribe(this);
  }

  @Override
  public void request(long n) {
    try {
      eventSender.send(new Request(n));
    } catch (Exception e) {
      log.error("Erroring requesting messages", e);
      onError(e);
    }
  }

  @Override
  public void cancel() {
    try {
      eventSender.send(new Cancel());
    } catch (Exception e) {
      log.error("Erroring cancelling", e);
      onError(e);
    }
  }

  @Override
  public void onNext(Message<T> t) {
    if (subscriber != null) {
      subscriber.onNext(t);
    }
  }

  @Override
  public void onError(Throwable t) {
    if (subscriber != null) {
      subscriber.onError(t);
    }
  }

  @Override
  public void onSubscribe(Subscription s) {}

  @Override
  public void onComplete() {}

  @RequiredArgsConstructor
  static class FirstErrorOnlySubscriber<T> implements Subscriber<T> {
    private final Subscriber<T> subscriber;
    private final AtomicBoolean hasError = new AtomicBoolean(false);

    @Override
    public void onSubscribe(Subscription s) {
      subscriber.onSubscribe(s);
    }

    @Override
    public void onNext(T t) {
      subscriber.onNext(t);
    }

    @Override
    public void onError(Throwable t) {
      if (hasError.compareAndSet(false, true)) {
        subscriber.onError(t);
      }
    }

    @Override
    public void onComplete() {
      subscriber.onComplete();
    }
  }
}
