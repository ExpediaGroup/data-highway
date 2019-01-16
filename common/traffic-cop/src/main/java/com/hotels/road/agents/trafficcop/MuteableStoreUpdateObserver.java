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
package com.hotels.road.agents.trafficcop;

import com.hotels.road.kafkastore.StoreUpdateObserver;

public class MuteableStoreUpdateObserver<K, V> implements StoreUpdateObserver<K, V> {
  private volatile boolean muted;
  private final StoreUpdateObserver<K, V> delegate;

  public MuteableStoreUpdateObserver(StoreUpdateObserver<K, V> delegate) {
    this(delegate, true);
  }

  public MuteableStoreUpdateObserver(StoreUpdateObserver<K, V> delegate, boolean muted) {
    this.delegate = delegate;
    this.muted = muted;
  }

  public void mute() {
    muted = true;
  }

  public void unmute() {
    muted = false;
  }

  public boolean isMuted() {
    return muted;
  };

  @Override
  public void handleUpdate(K key, V oldValue, V newValue) {
    if (!muted) {
      delegate.handleUpdate(key, oldValue, newValue);
    }
  }

  @Override
  public void handleRemove(K key, V oldValue) {
    if (!muted) {
      delegate.handleRemove(key, oldValue);
    }
  }

  @Override
  public void handleNew(K key, V value) {
    if (!muted) {
      delegate.handleNew(key, value);
    }
  }
}
