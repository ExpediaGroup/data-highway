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
package com.hotels.road.kafkastore.serialization;

import com.hotels.road.kafkastore.exceptions.SerializationException;

/**
 * @param <K> Key type to be serialized from and to.
 * @param <V> Value type to be serialized from and to.
 */
public interface Serializer<K, V> {

  /**
   * Serialize a Map's key to a byte array. The passed value can be null, if your implementation supports null keys, but
   * the returned serialized value cannot be null.
   *
   * @param key The key to serialize
   * @return bytes of the serialized key
   */
  public byte[] serializeKey(K key) throws SerializationException;

  /**
   * Serialize a Map's value to a byte array. The passed value can be null, if your implementation supports null values,
   * but the returned serialized value cannot be null.
   *
   * @param value The value to serialize
   * @return bytes of the serialized value
   */
  public byte[] serializeValue(V value) throws SerializationException;

  /**
   * Deserialize a Map's key. The value passed will never be null but, if your implementation supports null keys, the
   * implementation of this function may return null.
   *
   * @param key Serialized version of a key as returned by serializeKey
   * @return The deserialized key
   */
  public K deserializeKey(byte[] key) throws SerializationException;

  /**
   * Deserialize a Map's value. The value passed will never be null but, if your implementation supports null values,
   * then this function may return null.
   *
   * @param value Serialized version of a value as returned by serializeValue
   * @return The deserialized value
   */
  public V deserializeValue(byte[] value) throws SerializationException;
}
