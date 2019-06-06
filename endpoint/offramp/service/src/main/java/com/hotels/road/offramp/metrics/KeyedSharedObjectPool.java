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
package com.hotels.road.offramp.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A pool such that concurrent requests for a key will get the same instance of the value returned. As soon as there are
 * no remaining leases to a key the value is destroyed.
 *
 * @param <K> The key
 * @param <V> The value
 */
@Slf4j
@RequiredArgsConstructor
public abstract class KeyedSharedObjectPool<K, V> {
  private final ConcurrentMap<K, ReferenceCount<V>> objects = new ConcurrentHashMap<>();

  public V take(K key) {
    return objects.compute(key, (k, v) -> {
      if (v == null) {
        v = new ReferenceCount<>(constructValue(k));
      }
      v.count++;
      return v;
    }).ref;
  }

  public void release(K key) {
    objects.compute(key, (k, v) -> {
      if (v == null) {
        log.warn("Trying to release unused key: {}", k);
        return null;
      }
      v.count--;
      if (v.count <= 0) {
        if (v.count < 0) {
          log.error("Reference count was below zero: {}", k);
        }
        destroyValue(k, v.ref);
        return null;
      } else {
        return v;
      }
    });
  }

  protected abstract V constructValue(K key);

  protected abstract void destroyValue(K key, V value);

  @Data
  private static final class ReferenceCount<V> {
    int count = 0;
    final V ref;
  }
}
