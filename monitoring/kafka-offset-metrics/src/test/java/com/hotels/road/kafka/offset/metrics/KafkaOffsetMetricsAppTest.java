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
import static java.util.stream.Collectors.toList;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.Banner.Mode.OFF;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.ImmutableMap;

public class KafkaOffsetMetricsAppTest {

  private static final String TOPIC = "test.topic";

  @Rule
  public EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(1);

  @Test
  public void test() throws Exception {
    kafka.createTopic(TOPIC);

    try (KafkaConsumer<String, String> consumer = consumer()) {
      consumer.commitSync(singletonMap(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(1L)));
    }

    RestTemplate restTemplate = new RestTemplate();
    String fooResourceUrl = "http://localhost:8080/actuator/prometheus";

    try (ConfigurableApplicationContext context = runApp()) {
      Awaitility.await().atMost(5, SECONDS).pollInterval(100, MILLISECONDS).until(() -> {
        ResponseEntity<String> response = restTemplate.getForEntity(fooResourceUrl, String.class);
        assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
        List<String> lines = Arrays.asList(response.getBody().split("\n")).stream().filter(l -> l.startsWith("kafka")).collect(toList());

        //Should contain one metric in the following format
        // kafka-offset{host="<hostname>",group="group_id",topic="test_topic",partition="0",} 1.0
        assertThat(lines, hasSize(1));
        assertThat(lines.get(0), startsWith("kafka-offset"));
        assertThat(lines.get(0), containsString("group=\"group_id\""));
        assertThat(lines.get(0), containsString("topic=\"test_topic\""));
        assertThat(lines.get(0), containsString("partition=\"0\""));
        assertThat(lines.get(0), endsWith(" 1.0"));
      });
    }
  }

  private ConfigurableApplicationContext runApp() {
    String[] args = ImmutableMap
        .<String, String> builder()
        .put("kafka.bootstrapServers", kafka.bootstrapServers())
        .build()
        .entrySet()
        .stream()
        .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
        .toArray(i -> new String[i]);
    return new SpringApplicationBuilder(KafkaOffsetMetricsApp.class).bannerMode(OFF).run(args);
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
