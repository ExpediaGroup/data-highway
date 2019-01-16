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
package com.hotels.road.client.partitioning;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import lombok.Data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import com.hotels.road.client.OnrampOptions;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;
import com.hotels.road.partition.MessageHashCodeFunctionSupplier;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tls.TLSConfig;

@RunWith(MockitoJUnitRunner.class)
public class PartitionerFactoryTest {

  private static final int RANDOM_VALUE = 42;
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final JsonNode MESSAGE;

  private @Mock
  Random random;
  PartitionerFactory<TestMessage> underTest = new PartitionerFactory<>();

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicHttpsPort());

  static {
    try {
      MESSAGE = mapper.readTree("{\"a\":\"b\"}");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void initialiseMocks() {
    when(random.nextInt()).thenReturn(RANDOM_VALUE);
  }

  @Test
  public void hashFunction_partitionPathValue() throws Exception {
    Supplier<Path> path = () -> KeyPathParser.parse("$.a");
    @SuppressWarnings("resource")
    Function<JsonNode, Integer> hashFunction = new MessageHashCodeFunctionSupplier(path, random).get();

    assertThat(hashFunction.apply(MESSAGE), is("b".hashCode()));
  }

  @Test
  public void hashFunction_nullPartitionPath() throws Exception {
    Supplier<Path> path = () -> null;
    @SuppressWarnings("resource")
    Function<JsonNode, Integer> hashFunction = new MessageHashCodeFunctionSupplier(path, random).get();

    assertThat(hashFunction.apply(MESSAGE), is(RANDOM_VALUE));
  }

  @Test
  public void simpleWireMockTest() throws Exception {
    stubFor(get(urlEqualTo("/paver/v1/roads/test_road"))
        .willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("{}")));
    stubFor(post(urlEqualTo("/onramp/v1/roads/test_road/messages"))
        .willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody(createStubResponse(3, true, "accepted: "))));

    String host = "localhost:" + wireMockRule.httpsPort();
    OnrampOptions options = OnrampOptions.builder()
        .host(host)
        .username("user")
        .password("pass")
        .roadName("test_road")
        .objectMapper(mapper)
        .tlsConfigFactory(TLSConfig.trustAllFactory())
        .build();

    try (CloseableFunction<TestMessage, CompletableFuture<StandardResponse>> function = underTest.newInstance(options, 1, 1, EnqueueBehaviour.COMPLETE_EXCEPTIONALLY)) {
      CompletableFuture<StandardResponse> result = function.apply(new TestMessage("foo"));
      StandardResponse response = result.get();
      assertThat(response.getTimestamp(), is(100L));
      assertThat(response.isSuccess(), is(true));
      assertThat(response.getMessage(), is("accepted: 0"));
    }

    verify(postRequestedFor(urlEqualTo("/onramp/v1/roads/test_road/messages"))
        .withRequestBody(equalTo("[{\"data\":\"foo\"}]")));
  }

  @Data
  static class TestMessage {
    private final String data;
  }

  private String createStubResponse(int n, boolean success, String message) {
    String singleResponseTemplate = "{\"timestamp\": %s, \"success\": %s, \"message\": \"%s\"}";
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[");
    for (int i = 0; i < n; i++) {
      stringBuilder.append(String.format(singleResponseTemplate, 100L + i, success, message + i));
      if (i < n - 1) {
        stringBuilder.append(",");
      }
    }
    stringBuilder.append("]");
    return stringBuilder.toString();
  }

}
