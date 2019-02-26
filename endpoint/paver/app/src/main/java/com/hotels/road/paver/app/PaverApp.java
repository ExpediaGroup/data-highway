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
package com.hotels.road.paver.app;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;

import com.hotels.road.agents.trafficcop.TrafficCopConfiguration;
import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.model.kafka.RoadModelConfiguration;
import com.hotels.road.notification.sns.config.SnsConfiguration;
import com.hotels.road.paver.PaverControllerConfiguration;
import com.hotels.road.paver.tollbooth.PaverTollboothServiceConfiguration;
import com.hotels.road.rest.controller.common.CommonClockConfiguration;
import com.hotels.road.security.LdapSecurityConfiguration;
import com.hotels.road.swagger.SwaggerConfiguration;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@EnableSwagger2
@SpringBootApplication
@Import({
    TrafficCopConfiguration.class,
    RoadModelConfiguration.class,
    PaverControllerConfiguration.class,
    LdapSecurityConfiguration.class,
    PaverTollboothServiceConfiguration.class,
    SnsConfiguration.class,
    SwaggerConfiguration.class,
    CommonClockConfiguration.class })
public class PaverApp {
  public static void main(String[] args) throws Exception {
    DataHighwayApplication.run(PaverApp.class, args);
  }
}
