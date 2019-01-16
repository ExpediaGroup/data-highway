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
package com.hotels.road.client.simple;

import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpStatus;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import com.hotels.road.client.SimpleModel;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tls.TLSConfig;

public class SimpleRoadClientTest {
  private static final String INVALID_JSON = "not_a_json_message";
  private static final String INTERNAL_SERVER_ERROR_MESSAGE = "internal server error";
  private static final int THREADS = 4;
  private static final String TEST_ROAD = "test_road";
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicHttpsPort());
  private String wireMockHostPort;
  private SimpleRoadClient<SimpleModel> underTest;

  @Before
  public void setUp() throws URISyntaxException {
    wireMockHostPort = "localhost:" + wireMockRule.httpsPort();
    underTest = new SimpleRoadClient<>(wireMockHostPort, "user", "pass", TEST_ROAD, THREADS, TLSConfig.trustAll());
  }

  @Test
  public void singleMessage() throws Exception {
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages")).withBasicAuth("user", "pass").willReturn(
        aResponse().withStatus(HttpStatus.SC_OK).withBody(createStubResponse(1, true, "accepted: "))));
    SimpleModel messageModel = new SimpleModel();
    messageModel.setId(1L);
    messageModel.setStr("test message");

    StandardResponse response = underTest.sendMessage(messageModel);

    assertThat(response.isSuccess(), is(true));
    assertThat(response.getTimestamp(), is(100L));
    assertThat(response.getMessage(), is("accepted: 0"));
  }

  @Test
  public void multipleMessages() throws Exception {
    List<SimpleModel> messages = createMessages(3);
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages")).withBasicAuth("user", "pass").willReturn(
        aResponse().withStatus(HttpStatus.SC_OK).withBody(createStubResponse(3, true, "accepted: "))));

    List<StandardResponse> responses = underTest.sendMessages(messages);

    assertThat(responses.get(0).isSuccess(), is(true));
    assertThat(responses.get(0).getTimestamp(), is(100L));
    assertThat(responses.get(0).getMessage(), is("accepted: 0"));

    assertThat(responses.get(1).isSuccess(), is(true));
    assertThat(responses.get(1).getTimestamp(), is(101L));
    assertThat(responses.get(1).getMessage(), is("accepted: 1"));

    assertThat(responses.get(2).isSuccess(), is(true));
    assertThat(responses.get(2).getTimestamp(), is(102L));
    assertThat(responses.get(2).getMessage(), is("accepted: 2"));
  }

  @Test(expected = OnrampException.class)
  public void internalServerError() throws Exception {
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages")).willReturn(
        aResponse().withStatus(HttpStatus.SC_INTERNAL_SERVER_ERROR).withBody(INTERNAL_SERVER_ERROR_MESSAGE)));
    SimpleModel messageModel = new SimpleModel();
    messageModel.setId(1L);
    messageModel.setStr("test message");
    underTest.sendMessage(messageModel);
  }

  @Test(expected = OnrampEncodingException.class)
  public void responseParseError() throws Exception {
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages"))
        .willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody(INVALID_JSON)));
    SimpleModel messageModel = new SimpleModel();
    messageModel.setId(1L);
    messageModel.setStr("test message");
    underTest.sendMessage(messageModel);
  }

  @Test(expected = OnrampException.class)
  public void ioError() throws Exception {
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages"))
        .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE)));
    SimpleModel messageModel = new SimpleModel();
    messageModel.setId(1L);
    messageModel.setStr("test message");
    underTest.sendMessage(messageModel);
  }

  @Test
  public void badRequest() throws Exception {
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages"))
        .willReturn(aResponse().withStatus(SC_BAD_REQUEST).withBody(createSingleResponse(0, false, "error"))));
    SimpleModel messageModel = new SimpleModel();
    messageModel.setId(1L);
    messageModel.setStr("test message");
    StandardResponse response = underTest.sendMessage(messageModel);
    assertThat(response.isSuccess(), is(false));
    assertThat(response.getMessage(), is("error"));
  }

  @Test
  public void badRequestMultiple() throws Exception {
    List<SimpleModel> messages = createMessages(3);
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages"))
        .willReturn(aResponse().withStatus(SC_BAD_REQUEST).withBody(createSingleResponse(0, false, "error"))));

    List<StandardResponse> responses = underTest.sendMessages(messages);

    assertThat(responses.size(), is(3));

    assertThat(responses.get(0).isSuccess(), is(false));
    assertThat(responses.get(0).getMessage(), is("error"));

    assertThat(responses.get(1).isSuccess(), is(false));
    assertThat(responses.get(1).getMessage(), is("error"));

    assertThat(responses.get(2).isSuccess(), is(false));
    assertThat(responses.get(2).getMessage(), is("error"));
  }

  @Test(expected = OnrampException.class)
  public void anotherIOError() throws Exception {
    stubFor(post(urlEqualTo("/onramp/v1/roads/" + TEST_ROAD + "/messages"))
        .willReturn(aResponse().withFault(Fault.MALFORMED_RESPONSE_CHUNK)));
    SimpleModel messageModel = new SimpleModel();
    messageModel.setId(1L);
    messageModel.setStr("test message");
    underTest.sendMessage(messageModel);
  }

  @After
  public void tearDown() throws Exception {
    underTest.close();
  }

  private String createStubResponse(int n, boolean success, String message) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[");
    for (int i = 0; i < n; i++) {
      stringBuilder.append(createSingleResponse(100L + i, success, message + i));
      if (i < n - 1) {
        stringBuilder.append(",");
      }
    }
    stringBuilder.append("]");
    return stringBuilder.toString();
  }

  private String createSingleResponse(long timestamp, boolean success, String message) {
    String singleResponseTemplate = "{\"timestamp\": %d, \"success\": %s, \"message\": \"%s\"}";
    return String.format(singleResponseTemplate, timestamp, success, message);
  }

  private List<SimpleModel> createMessages(int n) {
    List<SimpleModel> messages = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      messages.add(new SimpleModel(i, "test message: " + i));
    }
    return messages;
  }
}
