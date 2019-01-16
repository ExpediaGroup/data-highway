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
package com.hotels.road.testdrive;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.springframework.boot.Banner.Mode.OFF;

import static com.hotels.road.offramp.model.DefaultOffset.EARLIEST;
import static com.hotels.road.rest.model.Sensitivity.PUBLIC;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import reactor.core.publisher.Flux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import com.hotels.road.client.RoadClient;
import com.hotels.road.client.simple.SimpleRoadClient;
import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.client.OfframpOptions;
import com.hotels.road.offramp.model.Message;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Offramp;
import com.hotels.road.rest.model.Authorisation.Onramp;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tls.TLSConfig;

public class TestDriveAppTest {
  private static final ObjectMapper mapper = new ObjectMapper();
  private static int port;
  private static String host;
  private static ConfigurableApplicationContext context;
  private static final String ROAD_NAME = "road1";

  @BeforeClass
  public static void beforeClass() throws Exception {
    HttpsURLConnectionTrust.trustAll();

    port = randomPort();
    host = "localhost:" + port;
    context = new SpringApplicationBuilder(TestDriveApp.class)
        .bannerMode(OFF)
        .properties("server.port:" + port, "paver.authorisation.authorities:ROLE_USER")
        .run();
  }

  @AfterClass
  public static void afterClass() {
    if (context != null) {
      context.close();
    }
    HttpsURLConnectionTrust.reset();
  }

  @Before
  public void before() throws Exception {
    TestDriveClient testDriveClient = new TestDriveClient(host, "user", "pass");
    testDriveClient.deleteAll();
  }

  @Test(timeout = 30_000L)
  public void endToEndTest() throws Exception {
    PaverClient paver = new PaverClient(host, "user", "pass");

    assertThat(paver.getRoads().size(), is(0));

    paver.createRoad(basicRoadModel());
    assertThat(paver.getRoad(ROAD_NAME).getName(), is(ROAD_NAME));

    Schema schema = schema();
    paver.addSchema(ROAD_NAME, 1, schema);
    assertThat(paver.getLatestSchema(ROAD_NAME), is(schema));

    ObjectNode payload = mapper.createObjectNode().put("id", 0);
    try (RoadClient<JsonNode> onramp = new SimpleRoadClient<>(host, "user", "pass", ROAD_NAME, 2,
        TLSConfig.trustAll())) {
      StandardResponse response = onramp.sendMessage(payload);
      assertThat(response.isSuccess(), is(true));
    }

    OfframpOptions<JsonNode> options = offrampOptions();
    try (OfframpClient<JsonNode> offramp = OfframpClient.create(options)) {
      Message<JsonNode> message = Flux.from(offramp.messages()).blockFirst();
      assertThat(message.getPartition(), is(0));
      assertThat(message.getOffset(), is(0L));
      assertThat(message.getSchema(), is(1));
      assertThat(message.getPayload(), is(payload));
    }
  }

  @Test(timeout = 30_000L)
  public void nullValueForRequiredField() throws Exception {
    PaverClient paver = new PaverClient(host, "user", "pass");

    assertThat(paver.getRoads().size(), is(0));

    paver.createRoad(basicRoadModel());
    assertThat(paver.getRoad(ROAD_NAME).getName(), is(ROAD_NAME));

    Schema schema = schema();
    paver.addSchema(ROAD_NAME, 1, schema);
    assertThat(paver.getLatestSchema(ROAD_NAME), is(schema));

    JsonNode payload = mapper.createObjectNode().set("id", NullNode.getInstance());
    try (RoadClient<JsonNode> onramp = new SimpleRoadClient<>(host, "user", "pass", ROAD_NAME, 2,
        TLSConfig.trustAll())) {
      StandardResponse response = onramp.sendMessage(payload);
      assertThat(response.isSuccess(), is(false));
    }
  }

  private static int randomPort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private BasicRoadModel basicRoadModel() {
    String description = "Test Road";
    String teamName = "ROAD";
    String contactEmail = "road@hotels.com";
    boolean enabled = true;
    String partitionPath = "";
    Authorisation authorisation = new Authorisation();
    Onramp onrampModel = new Onramp();
    onrampModel.setCidrBlocks(singletonList("0.0.0.0/0"));
    onrampModel.setAuthorities(singletonList("*"));
    authorisation.setOnramp(onrampModel);
    Offramp offrampModel = new Offramp();
    offrampModel.setAuthorities(singletonMap("*", singleton(PUBLIC)));
    authorisation.setOfframp(offrampModel);
    Map<String, String> metadata = emptyMap();
    return new BasicRoadModel(ROAD_NAME, description, teamName, contactEmail, enabled, partitionPath, authorisation,
        metadata);
  }

  private Schema schema() {
    return SchemaBuilder.record("test").fields().requiredInt("id").endRecord();
  }

  private OfframpOptions<JsonNode> offrampOptions() {
    return OfframpOptions
        .builder(JsonNode.class)
        .username("user")
        .password("pass")
        .host(host)
        .roadName(ROAD_NAME)
        .streamName("stream1")
        .defaultOffset(EARLIEST)
        .tlsConfigFactory(TLSConfig.trustAllFactory())
        .build();
  }
}
