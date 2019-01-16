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
package com.hotels.road.onramp.app;

import static java.util.Collections.singleton;

import javax.servlet.Filter;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import io.micrometer.core.instrument.MeterRegistry;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import com.hotels.road.agents.trafficcop.TrafficCopConfiguration;
import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.model.kafka.RoadModelConfiguration;
import com.hotels.road.onramp.OnrampControllerConfiguration;
import com.hotels.road.onramp.kafka.OnrampServiceConfiguration;
import com.hotels.road.security.LdapSecurityConfiguration;
import com.hotels.road.swagger.SwaggerConfiguration;
import com.hotels.road.user.agent.UserAgentMetricFilter;

@EnableSwagger2
@SpringBootApplication
@Import({
    TrafficCopConfiguration.class,
    RoadModelConfiguration.class,
    OnrampControllerConfiguration.class,
    LdapSecurityConfiguration.class,
    OnrampServiceConfiguration.class,
    SwaggerConfiguration.class })
public class OnrampApp {
  @Bean
  public Filter userAgentMetricFilter(MeterRegistry registry) {
    return new UserAgentMetricFilter(registry, singleton("road-onramp-client"));
  }

  public static void main(String[] args) throws Exception {
    DataHighwayApplication.run(OnrampApp.class, args);
  }
}
