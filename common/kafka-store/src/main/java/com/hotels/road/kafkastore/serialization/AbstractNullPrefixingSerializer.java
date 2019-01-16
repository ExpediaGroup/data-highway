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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.Function;

import com.hotels.road.kafkastore.exceptions.IgnoreableNoopKeyException;
import com.hotels.road.kafkastore.exceptions.SerializationException;

/**
 * An abstract {@link Serializer} that will prefix all keys and values so that either can be null. The serialized key or
 * value will be prefixed with a single byte to specify if the original key or value is null (0x02) or the following
 * bytes can be deserialized to a non null value (0x01). In order to be compatible with streams serialized with previous
 * versions of the Serializer values prefixed with 0x00 will cause a {@link IgnoreableNoopKeyException} to indicate that
 * they should be ignored.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public abstract class AbstractNullPrefixingSerializer<K, V> implements Serializer<K, V> {
  private static final byte NOOP_PREFIX = (byte) 0x00;
  private static final byte DATA_PREFIX = (byte) 0x01;
  private static final byte NULL_PREFIX = (byte) 0x02;

  private static final byte[] NULL_BYTES = new byte[] { NULL_PREFIX };

  @Override
  public final byte[] serializeKey(K key) throws SerializationException {
    return wrapNullableBytes(key == null ? null : serializeNonNullKey(key));
  }

  @Override
  public final byte[] serializeValue(V value) throws SerializationException {
    return wrapNullableBytes(value == null ? null : serializeNonNullValue(value));
  }

  @Override
  public final K deserializeKey(byte[] key) throws SerializationException {
    return deserialize(key, this::deserializeNonNullKey);
  }

  @Override
  public final V deserializeValue(byte[] value) throws SerializationException {
    return deserialize(value, this::deserializeNonNullValue);
  }

  protected abstract byte[] serializeNonNullKey(K key);

  protected abstract byte[] serializeNonNullValue(V value);

  protected abstract K deserializeNonNullKey(byte[] key);

  protected abstract V deserializeNonNullValue(byte[] value);

  private byte[] wrapNullableBytes(byte[] bytes) {
    if (bytes == null) {
      return NULL_BYTES;
    } else {
      return ByteBuffer.allocate(bytes.length + 1).put(DATA_PREFIX).put(bytes).array();
    }
  }

  private <T> T deserialize(byte[] bytes, Function<byte[], T> func) throws SerializationException {
    if (bytes.length < 1) {
      throw new SerializationException("Serialized array too short. Needs to be at least 1 byte long");
    }
    switch (bytes[0]) {
    case NOOP_PREFIX:
      throw new IgnoreableNoopKeyException();
    case NULL_PREFIX:
      return null;
    case DATA_PREFIX:
      bytes = Arrays.copyOfRange(bytes, 1, bytes.length);
      return func.apply(bytes);
    default:
      throw new SerializationException(String.format("Unexpected serialized format prefix 0x%x", bytes[0]));
    }
  }
}
