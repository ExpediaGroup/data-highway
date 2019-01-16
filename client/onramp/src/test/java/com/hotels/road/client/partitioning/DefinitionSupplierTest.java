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

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ProtocolVersion;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.client.http.HttpHandler;

@RunWith(MockitoJUnitRunner.class)
public class DefinitionSupplierTest {

  private static final String TEST_ROAD = "test_road";

  @Mock
  private HttpHandler handler;

  private DefinitionSupplier underTest;

  @Before
  public void before() throws URISyntaxException {
    underTest = new DefinitionSupplier(handler, TEST_ROAD);
  }

  @Test
  public void getDefinition() throws IOException {
    HttpResponse response = response(HttpStatus.SC_OK, "{\"name\":\"test_road\"}");
    when(handler.get(anyString())).thenReturn(response);

    JsonNode result = underTest.get();

    assertThat(result.get("name").asText(), is(TEST_ROAD));
  }

  @Test(expected = RuntimeException.class)
  public void getDefinitionStatus500() throws IOException {
    HttpResponse response = response(HttpStatus.SC_INTERNAL_SERVER_ERROR, "");
    when(handler.get(anyString())).thenReturn(response);

    underTest.get();
  }

  private HttpResponse response(int statusCode, String content) {
    BasicStatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), statusCode, "Not Important");
    HttpResponse response = new BasicHttpResponse(statusLine);
    BasicHttpEntity entity = new BasicHttpEntity();
    entity.setContent(new ByteArrayInputStream(content.getBytes(UTF_8)));
    response.setEntity(entity);
    return response;
  }
}
