/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.road.testtyrus;

import static org.junit.Assert.assertNotNull;
import static org.springframework.boot.Banner.Mode.OFF;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.ServerSocket;
import reactor.core.publisher.Flux;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.client.OfframpOptions;
import com.hotels.road.offramp.model.Message;

import com.hotels.road.security.RoadWebSecurityConfigurerAdapter;
import com.hotels.road.tls.TLSConfig;

import static com.hotels.road.offramp.model.DefaultOffset.EARLIEST;

public class TyrusIntegrationTest {

  private static ConfigurableApplicationContext context;
  private static String host;

  @BeforeClass
  public static void beforeClass() throws java.io.IOException {

    int port;
    try (ServerSocket socket = new ServerSocket(0)) {
      port = socket.getLocalPort();
    }

    host = "localhost:" + port;
    String[] args = ImmutableMap
        .builder()
        .put("server.port", port)
        .put("server.ssl.key-store", "classpath:road.jks")
        .put("server.ssl.key-store-password", "data-highway")
        .put("server.ssl.keyStoreType", "PKCS12")
        .put("server.ssl.keyAlias", "data-highway")
        .build()
        .entrySet()
        .stream()
        .map(e -> String.format("--%s=%s", e.getKey(), e.getValue()))
        .toArray(String[]::new);

    context = new SpringApplicationBuilder(WebSocketHandlerTest.class, TestSecurityConf.class).bannerMode(OFF).run(args);
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

  @Test
  public void testClient() throws Exception {
    OfframpOptions<JsonNode> options = OfframpOptions
        .builder(JsonNode.class)
        .host(host)
        .roadName("dummy")
        .username("user")
        .password("pass")
        .streamName("client")
        .defaultOffset(EARLIEST)
        .tlsConfigFactory(TLSConfig.trustAllFactory())
        .build();

    try (OfframpClient<JsonNode> client = OfframpClient.create(options)) {
      Message<JsonNode> message = Flux.from(client.messages()).limitRequest(1).blockFirst();
      assertNotNull(message);
      assertThat(message.getPayload().asText(), is(TestMessage.getPayload()));
    }
  }

  @AfterClass
  public static void afterClass() {
    if (context != null) {
      context.close();
      context = null;
    }
  }
}
