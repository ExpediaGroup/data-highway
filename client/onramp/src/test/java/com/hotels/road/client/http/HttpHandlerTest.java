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
package com.hotels.road.client.http;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.client.BasicCredentials;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.io.CharStreams;

import com.hotels.road.tls.TLSConfig;
import com.hotels.road.client.OnrampOptions;

public class HttpHandlerTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicHttpsPort());

  private URI url;
  private OnrampOptions options;

  @Before
  public void before() {
    url = URI.create("https://localhost:" + wireMockRule.httpsPort() + "/");
    options = OnrampOptions.builder()
        .host("host")
        .threads(1)
        .roadName("TEST_ROAD")
        .tlsConfigFactory(TLSConfig.trustAllFactory())
        .build();
  }

  @Test
  public void simpleGet() throws Exception {
    stubFor(get(urlEqualTo("/foo")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("bar")));

    try (HttpHandler handler = new HttpHandler(url, options)) {
      HttpResponse response = handler.get("foo");
      try (Reader reader = new InputStreamReader(response.getEntity().getContent())) {
        assertThat(CharStreams.toString(reader), is("bar"));
      }
    }
  }

  @Test
  public void simplePost() throws Exception {
    stubFor(post(urlEqualTo("/foo")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("bar")));

    try (HttpHandler handler = new HttpHandler(url, options)) {
      HttpEntity entity = EntityBuilder.create().setText("baz").build();
      HttpResponse response = handler.post("foo", entity);
      try (Reader reader = new InputStreamReader(response.getEntity().getContent())) {
        assertThat(CharStreams.toString(reader), is("bar"));
      }
    }

    verify(postRequestedFor(urlEqualTo("/foo")).withRequestBody(equalTo("baz")));
  }

  @Test
  public void paver() throws Exception  {
    HttpHandler handler = HttpHandler.paver(options.withHost("host"));

    assertThat(handler.getUrl(), is(URI.create("https://host/paver/v1/")));
  }

  @Test
  public void onramp() throws Exception  {
    HttpHandler handler = HttpHandler.onramp(options);

    assertThat(handler.getUrl(), is(URI.create("https://host/onramp/v1/")));
  }

  @Test
  public void authenticated() throws Exception {
    stubFor(get(urlEqualTo("/foo")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("bar")));

    try (HttpHandler handler = new HttpHandler(url, options.withUsername("user").withPassword("pass"))) {
      handler.get("foo");
    }

    verify(getRequestedFor(urlEqualTo("/foo")).withBasicAuth(new BasicCredentials("user", "pass")));
  }

  @Test
  public void unauthenticated() throws Exception {
    stubFor(get(urlEqualTo("/foo")).willReturn(aResponse().withStatus(HttpStatus.SC_OK).withBody("bar")));

    try (HttpHandler handler = new HttpHandler(url, options)) {
      handler.get("foo");
    }

    verify(getRequestedFor(urlEqualTo("/foo")).withoutHeader("Authorization"));
  }
}
