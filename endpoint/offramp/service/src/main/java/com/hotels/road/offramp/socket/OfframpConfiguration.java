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
package com.hotels.road.offramp.socket;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.SchemaProvider;
import com.hotels.road.offramp.model.Event;
import com.hotels.road.pii.PiiReplacerConfiguration;
import com.hotels.road.rest.controller.common.CommonClockConfiguration;
import com.hotels.road.schema.serde.SchemaSerializationModule;

@Configuration
@Import({
    PiiReplacerConfiguration.class,
    CommonClockConfiguration.class })
public class OfframpConfiguration {
  @Bean
  public ObjectMapper jsonMapper() {
    return new ObjectMapper()
        .registerModule(new SchemaSerializationModule())
        .registerModule(Event.module())
        .registerModule(new JavaTimeModule())
        .disable(MapperFeature.DEFAULT_VIEW_INCLUSION)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .disable(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS);
  }

  @Bean
  public SchemaProvider schemaProvider(@Value("#{store}") Map<String, Road> store) {
    return new SchemaProvider(store);
  }
}
