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
package com.hotels.road.offramp.metrics;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import lombok.Value;

@Component
public class MeterPool {
  private final KeyedSharedObjectPool<NameAndTags, Counter> counterPool;
  private final KeyedSharedObjectPool<NameAndTags, SettableTimeGauge> timeGaugePool;
  private final KeyedSharedObjectPool<NameAndTags, Timer> timerPool;
  private final KeyedSharedObjectPool<NameAndTags, SettableGauge> gaugePool;

  public MeterPool(MeterRegistry registry) {
    counterPool = new KeyedSharedObjectPool<NameAndTags, Counter>() {
      @Override
      protected Counter constructValue(NameAndTags key) {
        return registry.counter(key.getName(), key.getTags());
      }

      @Override
      protected void destroyValue(NameAndTags key, Counter value) {
        registry.remove(value);
      }
    };

    timeGaugePool = new KeyedSharedObjectPool<NameAndTags, SettableTimeGauge>() {
      @Override
      protected SettableTimeGauge constructValue(NameAndTags key) {
        AtomicLong value = new AtomicLong();
        TimeGauge timeGauge = registry
            .more()
            .timeGauge(key.getName(), key.getTags(), value, MILLISECONDS, AtomicLong::doubleValue);
        return new SettableTimeGauge(timeGauge, value);
      }

      @Override
      protected void destroyValue(NameAndTags key, SettableTimeGauge value) {
        registry.remove(value);
      }
    };

    timerPool = new KeyedSharedObjectPool<NameAndTags, Timer>() {
      @Override
      protected Timer constructValue(NameAndTags key) {
        return registry.timer(key.getName(), key.getTags());
      }

      @Override
      protected void destroyValue(NameAndTags key, Timer value) {
        registry.remove(value);
      }
    };

    gaugePool = new KeyedSharedObjectPool<MeterPool.NameAndTags, SettableGauge>() {
      @Override
      protected SettableGauge constructValue(NameAndTags key) {
        AtomicLong value = new AtomicLong();
        Gauge gauge = Gauge.builder(key.getName(), value, AtomicLong::doubleValue).tags(key.getTags()).register(registry);
        return new SettableGauge(gauge, value);
      }

      @Override
      protected void destroyValue(NameAndTags key, SettableGauge value) {
        registry.remove(value);
      }
    };
  }

  public Counter takeCounter(String name, Tags tags) {
    return takeMeter(counterPool, name, tags);
  }

  public void returnCounter(Counter counter) {
    returnMeter(counterPool, counter);
  }

  public SettableTimeGauge takeTimeGauge(String name, Tags tags) {
    return takeMeter(timeGaugePool, name, tags);
  }

  public void returnTimeGauge(SettableTimeGauge gauge) {
    returnMeter(timeGaugePool, gauge);
  }

  public Timer takeTimer(String name, Tags tags) {
    return takeMeter(timerPool, name, tags);
  }

  public void returnTimer(Timer timer) {
    returnMeter(timerPool, timer);
  }

  public SettableGauge takeGauge(String name, Tags tags) {
    return takeMeter(gaugePool, name, tags);
  }

  public void returnGauge(SettableGauge gauge) {
    returnMeter(gaugePool, gauge);
  }

  private <M extends Meter> M takeMeter(KeyedSharedObjectPool<NameAndTags, M> pool, String name, Tags tags) {
    return pool.take(new NameAndTags(name, tags));
  }

  private <M extends Meter> void returnMeter(KeyedSharedObjectPool<NameAndTags, M> pool, M meter) {
    pool.release(new NameAndTags(meter.getId().getName(), meter.getId().getTagsAsIterable()));
  }

  @Value
  private static final class NameAndTags {
    public NameAndTags(String name, Iterable<Tag> tags) {
      this.name = name;
      this.tags = Tags.of(tags);
    }

    String name;
    Tags tags;
  }
}
