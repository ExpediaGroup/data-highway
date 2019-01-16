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
package com.hotels.road.testdrive;

import static springfox.documentation.spi.DocumentationType.SWAGGER_2;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.Tag;
import springfox.documentation.spring.web.plugins.Docket;

@Configuration
public class TestDriveSwaggerConfiguration {
  @Bean
  public Docket testdriveSwagger() {
    return new Docket(SWAGGER_2)
        .groupName("testdrive")
        .apiInfo(new ApiInfoBuilder().title("Test Drive").build())
        .useDefaultResponseMessages(false)
        .tags(new Tag("testdrive", "Test Drive"))
        .select()
        .paths(PathSelectors.regex("/testdrive.*"))
        .build();
  }
}
