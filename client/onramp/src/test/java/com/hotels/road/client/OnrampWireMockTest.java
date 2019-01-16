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
package com.hotels.road.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import com.hotels.road.client.simple.SimpleRoadClient;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tls.TLSConfig;

public class OnrampWireMockTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicHttpsPort());
  private String wireMockHostPort;

  @Before
  public void setUp() {
    wireMockHostPort = "localhost:" + wireMockRule.httpsPort();
  }

  @Test
  public void postMessage() throws Exception {
    String stubResponse = "[{\"timestamp\": 123, \"success\": true, \"message\": \"accepted\"}]";
    stubFor(post(urlEqualTo("/onramp/v1/roads/a5/messages")).withBasicAuth("user", "pass").willReturn(
        aResponse().withStatus(200).withBody(stubResponse)));
    try (RoadClient<SimpleModel> client = new SimpleRoadClient<>(wireMockHostPort, "user", "pass", "a5", 1,
        TLSConfig.trustAll())) {
      SimpleModel model = new SimpleModel();
      model.setId(1L);
      model.setStr("test message");

      StandardResponse response = client.sendMessage(model);
      assertThat(response.getTimestamp(), is(123L));
      assertThat(response.isSuccess(), is(true));
      assertThat(response.getMessage(), is("accepted"));
    }
  }
}
