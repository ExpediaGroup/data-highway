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
package com.hotels.road.s3.io;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An {@link Executor} that blocks on execute when the thread pool and queue are full.
 */
@lombok.RequiredArgsConstructor
class BlockingExecutor extends AbstractExecutorService implements ExecutorService {
  private final ExecutorService delegate;
  private final Semaphore semaphore;

  BlockingExecutor(int poolSize, int queueSize) {
    this(new ThreadPoolExecutor(poolSize, poolSize, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()),
        new Semaphore(poolSize + queueSize));
  }

  @Override
  public synchronized void execute(Runnable command) {
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while acquiring permit.", e);
    }
    AtomicBoolean released = new AtomicBoolean(false);
    Runnable release = () -> {
      if (!released.getAndSet(true)) {
        semaphore.release();
      }
    };
    try {
      delegate.execute(() -> {
        try {
          command.run();
        } finally {
          release.run();
        }
      });
    } catch (Exception e) {
      release.run();
      throw e;
    }
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }
}
