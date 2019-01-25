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
package com.hotels.road.tool.cli.integration;

import static com.hotels.road.rest.model.Sensitivity.PII;
import static com.hotels.road.rest.model.Sensitivity.PUBLIC;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.springframework.boot.Banner.Mode.OFF;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.session.SessionAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.userdetails.User;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.offramp.app.OfframpApp;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Sensitivity;
import com.hotels.road.schema.serde.SchemaSerializationModule;
import com.hotels.road.security.RoadWebSecurityConfigurerAdapter;
import com.hotels.road.tool.cli.OfframpConsole;

import avro.shaded.com.google.common.primitives.Ints;
import lombok.Value;
import picocli.CommandLine;

public class OfframpConsoleAppIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static final String ROADS_TOPIC = "_roads";
    private static final String PATCH_TOPIC = "_roads_patch";
    private static final String ROAD1 = "road1";
    private static final String TOPIC1 = "roads." + ROAD1;

    private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
    private final PrintStream stdout = System.out;
    private final PrintStream stderr = System.err;

    static final ObjectMapper mapper = new ObjectMapper();

    public @Rule
    TemporaryFolder temp = new TemporaryFolder();

    @ClassRule
    public static final EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static ConfigurableApplicationContext context;
    private static String host;
    private static int port;

    private static final Schema schema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type()
        .stringType()
        .noDefault()
        .endRecord();

    @BeforeClass
    public static void beforeClass() throws Exception {
        Properties topicConfig = new Properties();
        topicConfig.setProperty("cleanup.policy", "compact");
        kafkaCluster.createTopic(ROADS_TOPIC, 1, 1, topicConfig);
        kafkaCluster.createTopic(TOPIC1, NUM_BROKERS, 1);

        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }

        try (KafkaProducer<String, String> p = kafkaProducer(new StringSerializer())) {
            Road road = new Road();
            road.setTopicName(TOPIC1);
            road.setSchemas(singletonMap(1, new SchemaVersion(schema, 1, false)));
            Authorisation authorisation = new Authorisation();
            road.setAuthorisation(authorisation);
            Authorisation.Offramp offramp = new Authorisation.Offramp();
            authorisation.setOfframp(offramp);
            Map<String, Set<Sensitivity>> authorities = singletonMap("ROLE_USER", ImmutableSet.of(PUBLIC, PII));
            offramp.setAuthorities(authorities);

            String model = new ObjectMapper().registerModule(new SchemaSerializationModule()).writeValueAsString(road);
            p.send(new ProducerRecord<>(ROADS_TOPIC, ROAD1, model)).get();
        }

        String[] args = ImmutableMap
            .builder()
            .put("server.port", port)
            .put("server.ssl.key-store", "classpath:road.jks")
            .put("server.ssl.key-store-password", "data-highway")
            .put("server.ssl.keyStoreType", "PKCS12")
            .put("server.ssl.keyAlias", "data-highway")
            .put("kafka.bootstrapServers", kafkaCluster.bootstrapServers())
            .put("kafka.road.topic", ROADS_TOPIC)
            .put("kafka.road.modification.topic", PATCH_TOPIC)
            .put("spring.data.redis.repositories.enabled", "false")
            .build()
            .entrySet()
            .stream()
            .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
            .toArray(String[]::new);

        context = new SpringApplicationBuilder(OfframpApp.class, TestSecurityConf.class).bannerMode(OFF).run(args);

        GenericData.Record record = new GenericData.Record(schema);
        record.put("f", "x");
        byte[] value = encode(record);
        try (KafkaProducer<String, byte[]> p = kafkaProducer(new ByteArraySerializer())) {
            p.send(new ProducerRecord<>(TOPIC1, value)).get();
        }

        host = "localhost:" + port;
    }

    @Configuration
    @EnableGlobalMethodSecurity(prePostEnabled = true)
    @SpringBootApplication(exclude = {
        SessionAutoConfiguration.class,
        RedisAutoConfiguration.class,
        RedisRepositoriesAutoConfiguration.class })
    public static class TestSecurityConf {
        @Bean
        public WebSecurityConfigurerAdapter webSecurityConfigurerAdapter() {
            return new RoadWebSecurityConfigurerAdapter() {
                @SuppressWarnings("deprecation")
                @Override
                protected void configure(AuthenticationManagerBuilder auth) throws Exception {
                    auth.inMemoryAuthentication().withUser(
                        User.withDefaultPasswordEncoder().username("user").password("pass").authorities("ROLE_USER"));
                }
            };
        }
    }

    @AfterClass
    public static void afterClass() {
        if (context != null) {
            context.close();
            context = null;
        }
    }

    @Value
    static class WhatIsF {
        Integer g;
    }

    @Before
    public void setStreams() {
        System.setOut(new PrintStream(outBuffer));
        System.setErr(new PrintStream(errBuffer));
    }

    @After
    public void restoreStreams() {
        System.setOut(stdout);
        System.setErr(stderr);
    }

    private static <T> KafkaProducer<String, T> kafkaProducer(Serializer<T> serializer) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaCluster.bootstrapServers());
        return new KafkaProducer<>(properties, new StringSerializer(), serializer);
    }

    private static byte[] encode(GenericData.Record record) throws IOException {
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(buffer, null);
            DatumWriter<Object> writer = new GenericDatumWriter<>(record.getSchema());
            buffer.write(0x00);
            buffer.write(Ints.toByteArray(1));
            writer.write(record, encoder);
            encoder.flush();
            return buffer.toByteArray();
        }
    }

    @Test(timeout = 30_000L)
    public void testMessageOutput() throws Exception {
        String[] args = {
            "--host=" + host,
            "--roadName=dummy", "--streamName=client", "--numToConsume=1",
            "--username=user", "--password=pass", "--tlsTrustAll"
        };

        OfframpConsole offrampConsole = new OfframpConsole();
        CommandLine.call(offrampConsole, args);

        String ref = "{"
            + "\"type\":\"MESSAGE\","
            + "\"partition\":0,"
            + "\"offset\":1,"
            + "\"schema\":2,"
            + "\"timestampMs\":3,"
            + "\"payload\":\"xxxxxxxxxx\""
            + "}"
            + "\n";
        assertEquals(ref, outBuffer.toString());
    }

    @Test(timeout = 30_000L)
    public void testMessageOutputFlipped() throws Exception {
        String[] args = {
            "--host=" + host,
            "--roadName=dummy", "--streamName=client", "--numToConsume=1",
            "--username=user", "--password=pass", "--tlsTrustAll",
            "--flipOutput"
        };

        OfframpConsole offrampConsole = new OfframpConsole();
        CommandLine.call(offrampConsole, args);

        String ref = "{"
            + "\"type\":\"MESSAGE\","
            + "\"partition\":0,"
            + "\"offset\":1,"
            + "\"schema\":2,"
            + "\"timestampMs\":3,"
            + "\"payload\":\"xxxxxxxxxx\""
            + "}"
            + "\n";
        assertEquals(ref, errBuffer.toString());
    }

    @Test(timeout = 30_000L)
    public void testMessageOutputYaml() throws Exception {
        String[] args = {
            "--host=" + host,
            "--roadName=dummy", "--streamName=client", "--numToConsume=1",
            "--username=user", "--password=pass", "--tlsTrustAll",
            "--format=YAML"
        };

        OfframpConsole offrampConsole = new OfframpConsole();
        CommandLine.call(offrampConsole, args);

        String ref = "---\n"
            + "type: \"MESSAGE\"\n"
            + "partition: 0\n"
            + "offset: 1\n"
            + "schema: 2\n"
            + "timestampMs: 3\n"
            + "payload: \"xxxxxxxxxx\"\n"
            + "\n";
        assertEquals(ref, outBuffer.toString());
    }
}
