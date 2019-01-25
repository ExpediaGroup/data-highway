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
package com.hotels.road.tool.cli;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.offramp.client.OfframpOptions;
import com.hotels.road.offramp.model.DefaultOffset;

import picocli.CommandLine;

public class OfframpConsoleTest {

  private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
  private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
  private final PrintStream stdout = System.out;
  private final PrintStream stderr = System.err;

  private OfframpConsole offrampConsoleSpied;
  private @Captor ArgumentCaptor<OfframpOptions<JsonNode>> offrampOptionsCaptor;

  @Before
  public void before() throws Exception {
    MockitoAnnotations.initMocks(this);
    offrampConsoleSpied = spy(new OfframpConsole());
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

  @Test
  public void testClassDefaults() throws Exception {
    assertThat(offrampConsoleSpied.host, nullValue());
    assertThat(offrampConsoleSpied.username, nullValue());
    assertThat(offrampConsoleSpied.password, nullValue());
    assertThat(offrampConsoleSpied.roadName, nullValue());
    assertThat(offrampConsoleSpied.streamName, nullValue());
    assertThat(offrampConsoleSpied.defaultOffset, is(DefaultOffset.LATEST));
    assertThat(offrampConsoleSpied.initialRequestAmount, is(200));
    assertThat(offrampConsoleSpied.replenishingRequestAmount, is(120));
    assertThat(offrampConsoleSpied.commitIntervalMs, is(500L));
    assertThat(offrampConsoleSpied.numToConsume, is(Long.MAX_VALUE));
    assertThat(offrampConsoleSpied.flipOutput, is(false));
    assertThat(offrampConsoleSpied.tlsTrustAll, is(false));
    assertThat(offrampConsoleSpied.debug, is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetOptions() throws Exception {
    String[] args = { "--host=localhost", "--roadName=route66", "--streamName=left" };

    final OfframpOptions<JsonNode> ref = OfframpOptions
        .builder(JsonNode.class)
        .host("localhost")
        .roadName("route66")
        .streamName("left")
        .defaultOffset(DefaultOffset.LATEST)
        .requestBuffer(200, 120)
        .tlsConfigFactory(null)
        .build();

    doNothing().when(offrampConsoleSpied).runClient(offrampOptionsCaptor.capture());
    CommandLine.call(offrampConsoleSpied, args);

    final OfframpOptions<JsonNode> out = offrampOptionsCaptor.getValue();

    assertThat(out, is(notNullValue()));
    assertEquals(out, ref);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testHelp() throws Exception {
    String[] args = { "--help" };

    CommandLine.call(offrampConsoleSpied, args);

    String[] lines = outBuffer.toString().split("\n");

    Stream<String> stream = Stream.of(lines);
    Stream<Integer> indexes = Stream.iterate(1, i -> i + 1);

    String defaultOffsetHelp = "";
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].contains("--defaultOffset")) {
        defaultOffsetHelp = String.join("\n", Arrays.copyOfRange(lines, i, i + 4));
      }
    }
    assertThat(defaultOffsetHelp, containsString(DefaultOffset.LATEST.name()));
    assertThat(defaultOffsetHelp, containsString(DefaultOffset.EARLIEST.name()));
    assertThat(defaultOffsetHelp, containsString(String.format("(default: %s)", DefaultOffset.LATEST.name())));
  }
}
