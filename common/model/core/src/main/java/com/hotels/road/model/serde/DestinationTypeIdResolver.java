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
package com.hotels.road.model.serde;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.DatabindContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.model.core.Destination;
import com.hotels.road.model.core.HiveDestination;

public class DestinationTypeIdResolver implements TypeIdResolver {

  static final Map<String, Class<? extends Destination>> TYPES = ImmutableMap
      .<String, Class<? extends Destination>> of("hive", HiveDestination.class);

  private JavaType baseType;

  @Override
  public void init(JavaType baseType) {
    this.baseType = baseType;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) throws IOException {
    return TypeFactory.defaultInstance().constructSpecializedType(baseType, TYPES.get(id));
  }

  @Override
  public String idFromValue(Object value) {
    return null;
  }

  @Override
  public String idFromBaseType() {
    return null;
  }

  @Override
  public String idFromValueAndType(Object value, Class<?> suggestedType) {
    return null;
  }

  @Override
  public Id getMechanism() {
    return Id.CUSTOM;
  }

  @Override
  public String getDescForKnownTypeIds() {
    return null;
  }

}
