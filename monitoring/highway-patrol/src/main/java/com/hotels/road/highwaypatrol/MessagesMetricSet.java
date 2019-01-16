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
package com.hotels.road.highwaypatrol;

import static java.util.Collections.singleton;

import java.time.Duration;
import java.util.EnumMap;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import lombok.NonNull;

@Component
public class MessagesMetricSet {
  private final @NonNull Counter receiverErrors;
  private final @NonNull Timer transitTime;
  private final @NonNull Timer onrampTime;
  private final @NonNull Counter messagesCounted;

  private final EnumMap<MessageState, Counter> messageEndStateReservoirs;

  public MessagesMetricSet(MeterRegistry registry) {
    receiverErrors = Counter.builder("highwaypatrol-receiverErrors").register(registry);
    transitTime = Timer.builder("highwaypatrol-transitTime").publishPercentileHistogram().register(registry);
    onrampTime = Timer.builder("highwaypatrol-onrampTime").publishPercentileHistogram().register(registry);
    messagesCounted = Counter.builder("highwaypatrol-messagesCounted").register(registry);

    messageEndStateReservoirs = new EnumMap<>(MessageState.class);
    for (MessageState state : MessageState.values()) {
      messageEndStateReservoirs
          .put(state, registry.counter("highwaypatrol-endState", singleton(Tag.of("state", state.getMetricName()))));
    }
  }

  public void markReceiverError() {
    receiverErrors.increment();
  }

  public void updateTransitTime(long transitTimeMs) {
    transitTime.record(Duration.ofMillis(transitTimeMs));
  }

  public void updateOnrampTime(long elaspedMs) {
    onrampTime.record(Duration.ofMillis(elaspedMs));
  }

  public void markMessageEndState(MessageState state) {
    messagesCounted.increment();
    messageEndStateReservoirs.get(state).increment();
  }
}
