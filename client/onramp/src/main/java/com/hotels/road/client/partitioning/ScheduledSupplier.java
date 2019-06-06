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

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;

import static lombok.AccessLevel.PACKAGE;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Caches a non-null value from a delegate {@link Supplier} and updates this cached value on a repeating interval.
 * Scheduling only begins on the first call to {@link #get()}, which also blocks the calling thread.
 * <p>
 * Should the delegate return a {@code null} this {@link Supplier} will throw an exception. If the delegate throws an
 * exception this {@link Supplier} will throw a {@link RuntimeException} giving the upstream exception as the cause.
 */
@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
class ScheduledSupplier<T> implements Supplier<T>, AutoCloseable {
  private final @NonNull Supplier<T> delegate;
  private final @NonNull ScheduledExecutorService scheduler;
  private final long interval;
  private final @NonNull TimeUnit timeUnit;

  private volatile T value;
  private volatile boolean scheduled;
  private volatile Exception error;

  ScheduledSupplier(@NonNull Supplier<T> delegate, long interval, @NonNull TimeUnit timeUnit) {
    this(delegate, newSingleThreadScheduledExecutor(new DaemonThreadFactory()), interval, timeUnit);
  }

  @Override
  public T get() {
    if (!scheduled) {
      synchronized (this) {
        if (!scheduled) {
          Runnable updater = () -> {
            try {
              T t = delegate.get();
              value = t;
              log.debug("Updated supplier value: {}", value);
            } catch (Exception e) {
              error = e;
              log.warn("Failure when calling supplier: {}.", delegate, e);
            }
          };
          updater.run();
          scheduler.scheduleWithFixedDelay(updater, interval, interval, timeUnit);
          scheduled = true;
        }
      }
    }
    if (value == null) {
      if (error == null) {
        throw new RuntimeException("No value found from delegate");
      } else {
        throw new RuntimeException(error);
      }
    }
    return value;
  }

  @Override
  public void close() throws Exception {
    try {
      scheduler.shutdownNow();
      scheduler.awaitTermination(10, SECONDS);
    } catch (Exception e) {
      log.warn("Exceptional shutdown of scheduler.", e);
      throw e;
    }

    if (delegate instanceof AutoCloseable) {
      ((AutoCloseable) delegate).close();
    }
  }

  private static final class DaemonThreadFactory implements ThreadFactory {
    private final AtomicInteger threadNumber = new AtomicInteger(1);

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r, "scheduled-supplier-" + threadNumber.getAndIncrement());
      thread.setDaemon(true);
      return thread;
    }
  }
}
