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
package com.hotels.road.offramp.client;

import static java.util.Collections.singletonMap;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import com.hotels.road.offramp.model.Commit;
import com.hotels.road.offramp.model.CommitResponse;

@RunWith(MockitoJUnitRunner.class)
public class CommitHandlerTest {
  private @Mock EventSender eventSender;

  private final String correlationId = "correlationId";
  private final Map<Integer, Long> offsets = singletonMap(0, 1L);
  private final Commit commit = new Commit(correlationId, offsets);

  private CommitHandler underTest;

  @Before
  public void before() {
    underTest = spy(new CommitHandler(eventSender));
  }

  @Test
  public void success() throws Exception {
    doReturn(commit).when(underTest).createCommit(offsets);

    Mono<Boolean> result = underTest.commit(offsets);
    underTest.complete(new CommitResponse(correlationId, true));

    StepVerifier.create(result).expectNext(true).verifyComplete();
    verify(eventSender).send(commit);
  }

  @Test
  public void failure() throws Exception {
    doReturn(commit).when(underTest).createCommit(offsets);

    Mono<Boolean> result = underTest.commit(offsets);
    underTest.complete(new CommitResponse(correlationId, false));

    StepVerifier.create(result).expectNext(false).verifyComplete();
    verify(eventSender).send(commit);
  }

  @Test(expected = IllegalStateException.class)
  public void unknownCommit() throws Exception {
    underTest.complete(new CommitResponse(correlationId, true));
  }
}
