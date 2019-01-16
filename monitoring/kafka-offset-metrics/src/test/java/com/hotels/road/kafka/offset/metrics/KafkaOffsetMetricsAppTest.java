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
package com.hotels.road.kafka.offset.metrics;

import static java.util.Collections.singletonMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.Banner.Mode.OFF;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.codahale.metrics.Clock;
import com.google.common.collect.ImmutableMap;

public class KafkaOffsetMetricsAppTest {

  private static final String TOPIC = "test.topic";

  @Rule
  public EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(1);

  @Configuration
  static class TestConfig {
    @Primary
    @Bean
    Clock clock() {
      return new Clock() {
        @Override
        public long getTick() {
          return 0L;
        }

        @Override
        public long getTime() {
          return 123000L;
        }
      };
    }

    @Primary
    @Bean
    Supplier<String> hostnameSupplier() {
      return () -> "hostname";
    }
  }

  @Test
  public void test() throws Exception {
    kafka.createTopic(TOPIC);

    try (KafkaConsumer<String, String> consumer = consumer()) {
      consumer.commitSync(singletonMap(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1L)));
    }

    try (ServerSocket serverSocket = new ServerSocket(0)) {
      CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
        try (Socket socket = serverSocket.accept();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
          return reader.readLine();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      try (ConfigurableApplicationContext context = runApp(serverSocket.getLocalPort())) {
        Awaitility.await().atMost(5, SECONDS).pollInterval(100, MILLISECONDS).until(() -> {
          assertThat(future.isDone(), is(true));
          assertThat(future.join(),
              is("road.kafka-offset.host.hostname.group.group_id.topic.test_topic.partition.0.offset 1 123"));
        });
      }
    }
  }

  private ConfigurableApplicationContext runApp(int port) {
    String[] args = ImmutableMap
        .<String, String> builder()
        .put("kafka.bootstrapServers", kafka.bootstrapServers())
        .put("graphite.endpoint", "localhost:" + port)
        .put("metricRate", "1000")
        .build()
        .entrySet()
        .stream()
        .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
        .toArray(i -> new String[i]);
    return new SpringApplicationBuilder(KafkaOffsetMetricsApp.class, TestConfig.class).bannerMode(OFF).run(args);
  }

  private KafkaConsumer<String, String> consumer() {
    Map<String, Object> config = ImmutableMap
        .<String, Object> builder()
        .put("bootstrap.servers", kafka.bootstrapServers())
        .put("group.id", "group.id")
        .put("enable.auto.commit", "false")
        .put("auto.offset.reset", "earliest")
        .build();
    Deserializer<String> deserializer = new StringDeserializer();
    return new KafkaConsumer<>(config, deserializer, deserializer);
  }
}
