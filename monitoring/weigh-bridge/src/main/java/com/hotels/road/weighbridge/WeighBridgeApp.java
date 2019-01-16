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
package com.hotels.road.weighbridge;

import static java.util.Collections.singletonMap;

import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

import com.hotels.road.boot.DataHighwayApplication;

//TODO split model out to a separate module so it can be used when building smart rebalancer
@SpringBootApplication
public class WeighBridgeApp {
  @Bean
  public Map<String, Object> kafkaConfig(@Value("${kafka.bootstrapServers}") String bootstrapServers) {
    return singletonMap("bootstrap.servers", bootstrapServers);
  }

  @Bean
  public AdminClient adminClient(@Value("#{kafkaConfig}") Map<String, Object> kafkaConfig) {
    return AdminClient.create(kafkaConfig);
  }

  @Bean
  public KafkaConsumer<?, ?> kafkaConsumer(@Value("#{kafkaConfig}") Map<String, Object> kafkaConfig) {
    Deserializer<byte[]> deserializer = new ByteArrayDeserializer();
    return new KafkaConsumer<>(kafkaConfig, deserializer, deserializer);
  }

  @Bean
  public ServletRegistrationBean<?> prometheusServletRegistration(WeighBridgeMetrics metrics) {
    CollectorRegistry.defaultRegistry.register(metrics);
    DefaultExports.initialize();
    return new ServletRegistrationBean<>(new MetricsServlet(), "/metrics");
  }

  public static void main(String[] args) throws Exception {
    DataHighwayApplication.run(WeighBridgeApp.class, args);
  }
}
