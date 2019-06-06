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

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.springframework.http.RequestEntity.delete;
import static org.springframework.http.RequestEntity.get;

import java.net.URI;
import java.util.Base64;
import java.util.List;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriTemplateHandler;

import com.fasterxml.jackson.databind.JsonNode;

public class TestDriveClient {
  private final RestTemplate rest = new RestTemplate();
  private final UriTemplateHandler handler;
  private final String creds;

  public TestDriveClient(String host, String user, String pass) {
    handler = new DefaultUriBuilderFactory("https://" + host + "/testdrive/v1");
    creds = "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(UTF_8));
  }

  public List<JsonNode> getMessages(String roadName) {
    URI uri = handler.expand("/roads/{roadName}/messages", roadName);
    RequestEntity<Void> request = get(uri).header("Authorization", creds).build();
    return rest.exchange(request, new ParameterizedTypeReference<List<JsonNode>>() {}).getBody();
  }

  public void deleteAll() {
    URI uri = handler.expand("/roads");
    RequestEntity<Void> request = delete(uri).header("Authorization", creds).build();
    rest.exchange(request, String.class);
  }

  public void deleteRoad(@PathVariable String roadName) {
    URI uri = handler.expand("/roads/{roadName}", roadName);
    RequestEntity<Void> request = delete(uri).header("Authorization", creds).build();
    rest.exchange(request, String.class);
  }

  public void deleteMessages(String roadName) {
    URI uri = handler.expand("/roads/{roadName}/messages", roadName);
    RequestEntity<Void> request = delete(uri).header("Authorization", creds).build();
    rest.exchange(request, String.class);
  }

  public void deleteCommits(String roadName, String streamName) {
    URI uri = handler.expand("/roads/{roadName}/streams/{streamName}/messages", roadName, streamName);
    RequestEntity<Void> request = delete(uri).header("Authorization", creds).build();
    rest.exchange(request, String.class);
  }
}
