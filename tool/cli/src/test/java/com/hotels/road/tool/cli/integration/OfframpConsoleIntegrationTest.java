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

import static org.junit.Assert.assertEquals;
import static org.springframework.boot.Banner.Mode.OFF;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
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

import com.google.common.collect.ImmutableMap;
import com.hotels.road.security.RoadWebSecurityConfigurerAdapter;
import com.hotels.road.tool.cli.OfframpConsole;

import picocli.CommandLine;

public class OfframpConsoleIntegrationTest {
  private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
  private final PrintStream stdout = System.out;
  private final PrintStream stderr = System.err;

  private static ConfigurableApplicationContext context;
  private static String host;

  @BeforeClass
  public static void beforeClass() throws Exception {
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

  @AfterClass
  public static void afterClass() {
    if (context != null) {
        context.close();
        context = null;
    }
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
