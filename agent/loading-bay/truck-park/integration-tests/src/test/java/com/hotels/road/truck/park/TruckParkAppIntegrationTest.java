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
package com.hotels.road.truck.park;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.Banner.Mode.OFF;

import static com.hotels.road.schema.gdpr.PiiVisitor.PII;
import static com.hotels.road.schema.gdpr.PiiVisitor.SENSITIVITY;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import io.findify.s3mock.S3Mock;
import io.findify.s3mock.request.CreateBucketConfiguration;
import io.findify.s3mock.response.Content;
import scala.Option;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;

import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.KafkaStoreUtils;
import com.hotels.road.truck.park.decoder.DataDeserializer;
import com.hotels.road.truck.park.schema.KafkaStoreSchemaLookup.Road;
import com.hotels.road.truck.park.schema.KafkaStoreSchemaLookup.SchemaVersion;
import com.hotels.road.truck.park.schema.RoadSchemaConfiguration;

public class TruckParkAppIntegrationTest {
  private static final String BUCKET = "bucket";
  private static final String STORE_TOPIC = "_roads";
  private static final String ROAD_NAME = "road1";
  private static final String TOPIC = "_roads.road1";
  private static final int VERSION = 1;
  private static final Schema SCHEMA = SchemaBuilder
      .record("r")
      .fields()
      .name("piiStringField")
      .prop(SENSITIVITY, PII)
      .type(SchemaBuilder.builder().stringType())
      .noDefault()
      .name("nonPiiStringField")
      .type(SchemaBuilder.builder().stringType())
      .noDefault()
      .name("piiBytesField")
      .prop(SENSITIVITY, PII)
      .type(SchemaBuilder.builder().bytesType())
      .noDefault()
      .name("nonPiiBytesField")
      .type(SchemaBuilder.builder().bytesType())
      .noDefault()
      .endRecord();

  @Rule
  public EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(1);

  private KafkaStore<String, Road> store;
  private int port;
  private S3Mock s3;

  @Configuration
  static class TestConfig {
    @Primary
    @Bean
    Clock clock() {
      return Clock.fixed(LocalDate.of(2017, 4, 24).atStartOfDay().toInstant(ZoneOffset.UTC), ZoneOffset.UTC);
    }

    @Primary
    @Bean
    AmazonS3 s3(@Value("${s3.port}") int port) {
      return AmazonS3Client
          .builder()
          .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
          .withEndpointConfiguration(new EndpointConfiguration("http://127.0.0.1:" + port, "us-west-2"))
          .build();
    }
  }

  @Before
  public void before() throws Exception {
    kafka.createTopic(TOPIC);
    KafkaStoreUtils.checkAndCreateTopic(kafka.zKConnectString(), STORE_TOPIC, 1);

    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    s3 = S3Mock.create(port);
    s3.start();
    s3.p().createBucket(BUCKET, new CreateBucketConfiguration(Option.empty()));
  }

  @After
  public void after() {
    s3.stop();
  }

  @Test
  public void simpleEndToEnd() throws Throwable {
    Map<Integer, SchemaVersion> schemas = Collections.singletonMap(1, new SchemaVersion(SCHEMA, VERSION, false));
    Road road = Road.builder().schemas(schemas).build();

    createRoad(road);

    Map<String, Object> configs = Collections.singletonMap("bootstrap.servers", kafka.bootstrapServers());
    try (KafkaProducer<Void, Record> producer = new KafkaProducer<>(configs, new NullSerializer(),
        new DataSerializer())) {
      Record record = new Record(SCHEMA);
      record.put("piiStringField", "a");
      record.put("nonPiiStringField", "b");
      record.put("piiBytesField", ByteBuffer.wrap(new byte[] { 0 }));
      record.put("nonPiiBytesField", ByteBuffer.wrap(new byte[] { 1 }));
      producer.send(new ProducerRecord<>(TOPIC, record)).get();
    }

    runTest();

    Iterator<Content> iterator = scala.collection.JavaConversions
        .asJavaIterable(s3.p().listBucket(BUCKET, Option.empty(), Option.empty()).contents())
        .iterator();
    String key = iterator.next().key();
    assertThat(key, startsWith("prefix/1492992000000_"));
    GenericDatumReader<Record> datumReader = new GenericDatumReader<>(SCHEMA);
    try (SeekableInput input = new SeekableByteArrayInput(s3.p().getObject(BUCKET, key).bytes());
        DataFileReader<Record> reader = new DataFileReader<>(input, datumReader)) {
      Record next = reader.next();
      assertThat(next.get("piiStringField").toString(), is(""));
      assertThat(next.get("nonPiiStringField").toString(), is("b"));
      assertThat(((ByteBuffer) next.get("piiBytesField")).array(), is(new byte[0]));
      assertThat(((ByteBuffer) next.get("nonPiiBytesField")).array(), is(new byte[] { 1 }));
      assertThat(reader.hasNext(), is(false));
    }
    assertThat(iterator.hasNext(), is(false));

  }

  private void runTest() throws Exception {
    String[] args = ImmutableMap
        .<String, String> builder()
        .put("kafka.bootstrapServers", kafka.bootstrapServers())
        .put("kafka.road.topic", STORE_TOPIC)
        .put("road.name", ROAD_NAME)
        .put("road.topic", TOPIC)
        .put("road.offsets", "0:0,1")
        .put("s3.bucket", BUCKET)
        .put("s3.keyPrefix", "prefix")
        .put("s3.port", Integer.toString(port))
        .build()
        .entrySet()
        .stream()
        .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
        .toArray(i -> new String[i]);

    new SpringApplicationBuilder(TruckParkApp.class, TestConfig.class).bannerMode(OFF).run(args);
  }

  @SuppressWarnings("unchecked")
  private void createRoad(Road road) {
    try (ConfigurableApplicationContext context = new SpringApplicationBuilder(RoadSchemaConfiguration.class)
        .bannerMode(OFF)
        .properties(ImmutableMap
            .<String, Object> builder()
            .put("kafka.bootstrapServers", kafka.bootstrapServers())
            .put("kafka.road.topic", STORE_TOPIC)
            .build())
        .build()
        .run()) {
      store = context.getBean(KafkaStore.class);
      store.put(ROAD_NAME, road);
    }
  }

  static class NullSerializer implements Serializer<Void> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Void data) {
      return null;
    }

    @Override
    public void close() {}

  }

  static class DataSerializer implements Serializer<Record> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Record record) {
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        baos.write(DataDeserializer.MAGIC_BYTE);
        baos.write(Ints.toByteArray(VERSION));
        DatumWriter<Object> writer = new GenericDatumWriter<>(SCHEMA);
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(baos, null);
        writer.write(record, encoder);
        encoder.flush();
        return baos.toByteArray();
      } catch (IOException unreachable) {
        throw new RuntimeException(unreachable);
      }
    }

    @Override
    public void close() {}
  }
}
