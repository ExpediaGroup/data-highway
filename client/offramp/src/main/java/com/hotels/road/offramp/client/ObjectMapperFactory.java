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

import static lombok.AccessLevel.PRIVATE;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import com.hotels.road.offramp.model.Event;

@AllArgsConstructor(access = PRIVATE)
public final class ObjectMapperFactory {
  public static <T> ObjectMapper create(
      @NonNull Class<T> payloadClass,
      @NonNull PayloadTypeFactory payloadTypeFactory,
      JsonDeserializer<T> payloadDeserialiser) {
    ObjectMapper mapper = new ObjectMapper().disable(FAIL_ON_UNKNOWN_PROPERTIES);
    TypeFactory typeFactory = mapper.getTypeFactory();
    JavaType payloadType = payloadTypeFactory.construct(typeFactory);
    return mapper.registerModule(Event.module(typeFactory, payloadType, payloadClass, payloadDeserialiser));
  }
}
