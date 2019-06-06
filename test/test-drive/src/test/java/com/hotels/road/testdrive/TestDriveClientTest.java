/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.BasicCredentials;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

public class TestDriveClientTest {
  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicHttpsPort());
  private String wireMockHostPort;
  private TestDriveClient underTest;

  @BeforeClass
  public static void beforeClass() {
    HttpsURLConnectionTrust.trustAll();
  }

  @AfterClass
  public static void afterClass() {
    HttpsURLConnectionTrust.reset();
  }

  @Before
  public void before() {
    wireMockHostPort = "localhost:" + wireMockRule.httpsPort();
    underTest = new TestDriveClient(wireMockHostPort, "user", "pass");
  }

  @Test
  public void getMessages() throws Exception {
    stubFor(get(urlEqualTo("/testdrive/v1/roads/road/messages")).withBasicAuth("user", "pass").willReturn(
        ok("[{\"foo\":\"bar\"}]").withHeader("Content-Type", "application/json")));
    List<JsonNode> messages = underTest.getMessages("road");
    assertThat(messages, is(singletonList(new ObjectMapper().createObjectNode().put("foo", "bar"))));
    verify(getRequestedFor(urlEqualTo("/testdrive/v1/roads/road/messages"))
        .withBasicAuth(new BasicCredentials("user", "pass")));
  }

  @Test
  public void deleteAll() throws Exception {
    stubFor(delete(urlEqualTo("/testdrive/v1/roads")).withBasicAuth("user", "pass").willReturn(ok()));
    underTest.deleteAll();
    verify(deleteRequestedFor(urlEqualTo("/testdrive/v1/roads")).withBasicAuth(new BasicCredentials("user", "pass")));
  }

  @Test
  public void deleteRoad() throws Exception {
    stubFor(delete(urlEqualTo("/testdrive/v1/roads/road")).withBasicAuth("user", "pass").willReturn(ok()));
    underTest.deleteRoad("road");
    verify(
        deleteRequestedFor(urlEqualTo("/testdrive/v1/roads/road")).withBasicAuth(new BasicCredentials("user", "pass")));
  }

  @Test
  public void deleteMessages() throws Exception {
    stubFor(delete(urlEqualTo("/testdrive/v1/roads/road/messages")).withBasicAuth("user", "pass").willReturn(ok()));
    underTest.deleteMessages("road");
    verify(deleteRequestedFor(urlEqualTo("/testdrive/v1/roads/road/messages"))
        .withBasicAuth(new BasicCredentials("user", "pass")));
  }

  @Test
  public void deleteCommits() throws Exception {
    stubFor(
        delete(urlEqualTo("/testdrive/v1/roads/road/streams/stream/messages")).withBasicAuth("user", "pass").willReturn(
            ok()));
    underTest.deleteCommits("road", "stream");
    verify(deleteRequestedFor(urlEqualTo("/testdrive/v1/roads/road/streams/stream/messages"))
        .withBasicAuth(new BasicCredentials("user", "pass")));
  }
}
