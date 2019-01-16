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
package com.hotels.road.onramp.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.MeterRegistry;

@Configuration
@ComponentScan
public class OnrampServiceConfiguration {
  @SuppressWarnings("deprecation")
  @Bean
  public Producer<byte[], byte[]> kafkaProducer(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.road.batch.size:16384}") int batchSize,
      @Value("${kafka.road.linger.ms:0}") int lingerMs,
      @Value("${kafka.road.buffer.memory:33554432}") long bufferMemory,
      @Value("${kafka.road.acks:1}") String acks,
      @Value("${kafka.road.compression:none}") String compressionType,
      MeterRegistry registry) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.ACKS_CONFIG, acks);
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

    Producer<byte[], byte[]> producer = new KafkaProducer<>(props);

    producer.metrics().forEach((metricName, metric) -> {
      String name = "onramp_kafka_producer_" + metricName.group() + "_" + metricName.name();
      registry.gauge(name, metric, m -> m.value());
    });

    return producer;
  }
}
