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
package com.hotels.road.testdrive;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.springframework.http.MediaType.APPLICATION_JSON_UTF8;
import static org.springframework.http.RequestEntity.get;
import static org.springframework.http.RequestEntity.post;

import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.List;

import org.apache.avro.Schema;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.RequestEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriTemplateHandler;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.schema.serde.SchemaSerializationModule;

public class PaverClient {
  private final RestTemplate rest = restTemplate();
  private final UriTemplateHandler handler;
  private final String creds;

  public PaverClient(String host, String user, String pass) {
    handler = new DefaultUriBuilderFactory("https://" + host + "/paver/v1");
    creds = "Basic " + Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(UTF_8));
  }

  public List<String> getRoads() {
    URI uri = handler.expand("/roads");
    RequestEntity<Void> request = get(uri).header("Authorization", creds).build();
    return rest.exchange(request, new ParameterizedTypeReference<List<String>>() {}).getBody();
  }

  public StandardResponse createRoad(BasicRoadModel road) {
    URI uri = handler.expand("/roads");
    RequestEntity<BasicRoadModel> request = post(uri).header("Authorization", creds).body(road);
    return rest.exchange(request, StandardResponse.class).getBody();
  }

  public RoadModel getRoad(String roadName) {
    URI uri = handler.expand("/roads/{roadName}", roadName);
    RequestEntity<Void> request = get(uri).build();
    return rest.exchange(request, RoadModel.class).getBody();
  }

  public StandardResponse addSchema(String roadName, int version, Schema schema) {
    URI uri = handler.expand("/roads/{roadName}/schemas/{version}", roadName, version);
    RequestEntity<Schema> request = post(uri).header("Authorization", creds).contentType(APPLICATION_JSON_UTF8).body(
        schema);
    return rest.exchange(request, StandardResponse.class).getBody();
  }

  public Schema getLatestSchema(String roadName) {
    URI uri = handler.expand("/roads/{roadName}/schemas/latest", roadName);
    RequestEntity<Void> request = get(uri).build();
    return rest.exchange(request, Schema.class).getBody();
  }

  private static RestTemplate restTemplate() {
    RestTemplate restTemplate = new RestTemplate();
    MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
    ObjectMapper mapper = new ObjectMapper().registerModule(new SchemaSerializationModule());
    converter.setObjectMapper(mapper);
    restTemplate.getMessageConverters().add(0, converter);
    restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
      @Override
      public void handleError(ClientHttpResponse response) throws IOException {}
    });
    return restTemplate;
  }
}
