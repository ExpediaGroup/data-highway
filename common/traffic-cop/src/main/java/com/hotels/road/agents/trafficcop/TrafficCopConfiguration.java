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

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableMap;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.StoreUpdateObserver;
import com.hotels.road.tollbooth.client.kafka.KafkaPatchSetEmitter;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

@Configuration
@ComponentScan
public class TrafficCopConfiguration {
  @Bean
  public <M> Map<String, M> store(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.road.topic}") String topic,
      ModelSerializer<M> serializer,
      StoreUpdateObserver<String, M> observer) {
    MuteableStoreUpdateObserver<String, M> observerProxy = new MuteableStoreUpdateObserver<>(observer);
    List<StoreUpdateObserver<String, M>> observers = singletonList(observerProxy);
    Map<String, M> store = new KafkaStore<>(bootstrapServers, serializer, topic, observers);
    observerProxy.unmute();
    return unmodifiableMap(store);
  }

  @Bean
  public PatchSetEmitter modificationEmitter(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.road.modification.topic}") String topic,
      ObjectMapper mapper) {

    Map<String, Object> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.RETRIES_CONFIG, 1);

    Producer<String, String> kafkaProducer = new KafkaProducer<>(producerProps);

    return new KafkaPatchSetEmitter(topic, kafkaProducer, mapper);
  }

}
