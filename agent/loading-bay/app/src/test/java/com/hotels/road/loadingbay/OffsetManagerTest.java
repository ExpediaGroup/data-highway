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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableMap;

@Slf4j
public class OffsetManagerTest {

  private static final String TOPIC = "topic1";
  @Rule
  public EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(1);

  private Producer<byte[], byte[]> kafkaProducer;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;

  @Before
  public void setUp() throws IOException, InterruptedException {
    kafka.start();
    kafka.createTopic(TOPIC);

    kafkaProducer = new KafkaProducer<>(producerConfig());

    kafkaConsumer = new KafkaConsumer<>(consumerConfig(), new ByteArrayDeserializer(), new ByteArrayDeserializer());

    kafkaConsumer.subscribe(Collections.singleton(TOPIC));
    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(TOPIC, new byte[] {}));
    kafkaConsumer.poll(TimeUnit.SECONDS.toMillis(1));
    kafkaConsumer.commitSync();
    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(TOPIC, new byte[] {}));
  }

  private Map<String, Object> consumerConfig() {
    Map<String, Object> config = ImmutableMap
        .<String, Object> builder()
        .put("bootstrap.servers", kafka.bootstrapServers())
        .put("group.id", TOPIC)
        .put("enable.auto.commit", "false")
        .put("auto.offset.reset", "earliest")
        .build();
    return config;
  }

  private Properties producerConfig() {
    Properties props = new Properties();
    props.put("bootstrap.servers", kafka.bootstrapServers());
    props.put("acks", "1");
    props.put("retries", 0);
    props.put("key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getCanonicalName());
    props.put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getCanonicalName());
    return props;
  }

  @Test
  public void typical() throws Exception {
    try (OffsetManager underTest = new OffsetManager(kafka.bootstrapServers())) {
      Map<Integer, Long> latestOffsets = underTest.getLatestOffsets(TOPIC);
      assertThat(latestOffsets, is(ImmutableMap.of(0, 2L)));
      log.info("latestOffsets : {}", latestOffsets);

      Map<Integer, Long> committedOffsets = underTest.getCommittedOffsets(TOPIC);
      assertThat(committedOffsets, is(ImmutableMap.of(0, 0L)));
      log.info("committedOffsets : {}", committedOffsets);

      log.info("committing offsets");
      underTest.commitOffsets(TOPIC, latestOffsets);

      committedOffsets = underTest.getCommittedOffsets(TOPIC);
      log.info("committedOffsets : {}", committedOffsets);
      assertThat(committedOffsets, is(ImmutableMap.of(0, 2L)));
    }
  }

  @After
  public void tearDown() {
    kafkaConsumer.close();
  }
}
