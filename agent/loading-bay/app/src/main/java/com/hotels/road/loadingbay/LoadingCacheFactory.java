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
package com.hotels.road.loadingbay;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@Component
public class LoadingCacheFactory {

  private final long expiryDuration;
  private final TimeUnit expiryUnit;

  @Autowired
  public LoadingCacheFactory(
      @Value("${configuration.cache.expiry.duration:30}") long expiryDuration,
      @Value("${configuration.cache.expiry.time.unit:SECONDS}") TimeUnit expiryUnit) {
    this.expiryDuration = expiryDuration;
    this.expiryUnit = expiryUnit;
  }

  public <K, V> LoadingCache<K, V> newInstance(Function<K, V> function) {
    return CacheBuilder.newBuilder().concurrencyLevel(1).expireAfterWrite(expiryDuration, expiryUnit).build(
        CacheLoader.from(function));
  }

}
