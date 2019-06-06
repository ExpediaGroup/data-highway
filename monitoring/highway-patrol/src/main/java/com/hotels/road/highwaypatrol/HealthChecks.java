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

import static com.google.common.base.Preconditions.checkState;

import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class HealthChecks {
  @Bean
  public AbstractHealthIndicator dummyHealthCheck(Receiver receiver) {
    return new AbstractHealthIndicator() {
      @Override
      protected void doHealthCheck(Health.Builder builder) throws Exception {
        checkState(true, "True is no longer true");
        builder.up().withDetail("truth", "True is true");
      }
    };
  }
}
