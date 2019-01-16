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
package com.hotels.road.kafkastore.serialization;

import static com.google.common.base.Charsets.UTF_8;

/**
 * A {@link Serializer} for keys and values of type String.
 */
public class StringSerializer extends AbstractNullPrefixingSerializer<String, String> {
  @Override
  protected byte[] serializeNonNullKey(String key) {
    return key.getBytes(UTF_8);
  }

  @Override
  protected byte[] serializeNonNullValue(String value) {
    return value.getBytes(UTF_8);
  }

  @Override
  protected String deserializeNonNullKey(byte[] key) {
    return new String(key, UTF_8);
  }

  @Override
  protected String deserializeNonNullValue(byte[] value) {
    return new String(value, UTF_8);
  }
}
