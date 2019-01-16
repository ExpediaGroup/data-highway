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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import com.google.common.collect.ImmutableMap;

import com.hotels.road.offramp.model.Message;

@RunWith(MockitoJUnitRunner.class)
public class CommitterTest {

  private @Mock OfframpClient<?> client;

  private final Duration interval = Duration.ofMillis(500);

  @Test
  public void successfulCommit() throws Exception {
    doReturn(Mono.just(true)).when(client).commit(ImmutableMap.of(0, 3L));

    Committer underTest = Committer.create(client, interval);
    Disposable disposable = underTest.start().subscribe();

    underTest.commit(message(0, 1L));
    underTest.commit(message(0, 2L));

    await().pollInterval(100, MILLISECONDS).atMost(2, SECONDS).until(() -> {
      verify(client).commit(ImmutableMap.of(0, 3L));
    });

    disposable.dispose();
  }

  @Test
  public void failedCommit() throws Exception {
    doReturn(Mono.just(false)).when(client).commit(ImmutableMap.of(0, 3L));

    Committer underTest = Committer.create(client, interval);
    AtomicBoolean failed = new AtomicBoolean(false);
    Disposable disposable = underTest.start().doOnError(t -> failed.compareAndSet(false, true)).subscribe();

    underTest.commit(message(0, 1L));
    underTest.commit(message(0, 2L));

    await().pollInterval(100, MILLISECONDS).atMost(2, SECONDS).untilTrue(failed);

    disposable.dispose();
  }

  private Message<String> message(int partition, long offset) {
    return new Message<>(partition, offset, 2, 1L, "foo");
  }
}
