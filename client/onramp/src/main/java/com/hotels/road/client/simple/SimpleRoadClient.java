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

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import static lombok.AccessLevel.PACKAGE;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Objects;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.entity.EntityBuilder;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CharStreams;

import com.hotels.road.client.RoadClient;
import com.hotels.road.client.http.HttpHandler;
import com.hotels.road.client.OnrampOptions;
import com.hotels.road.rest.model.StandardResponse;
import com.hotels.road.tls.TLSConfig;

@RequiredArgsConstructor(access = PACKAGE)
public class SimpleRoadClient<T> implements RoadClient<T> {
  private static final int DEFAULT_THREADS = 1;
  private static final TypeReference<List<StandardResponse>> RESPONSE_REF = new TypeReference<List<StandardResponse>>() {};
  private final @NonNull HttpHandler handler;
  private final @NonNull String roadName;
  private final @NonNull ObjectMapper objectMapper;

  @Deprecated
  /** @deprecated Use {@link #SimpleRoadClient(OnrampOptions)} */
  public SimpleRoadClient(String host, String username, String password, String roadName) {
    this(host, username, password, roadName, DEFAULT_THREADS);
  }

  @Deprecated
  /** @deprecated Use {@link #SimpleRoadClient(OnrampOptions)} */
  public SimpleRoadClient(String host, String username, String password, String roadName, int threads) {
    this(host, username, password, roadName, threads, new ObjectMapper());
  }

  @Deprecated
  /** @deprecated Use {@link #SimpleRoadClient(OnrampOptions)} */
  public SimpleRoadClient(String host, String username, String password, String roadName, ObjectMapper objectMapper) {
    this(host, username, password, roadName, DEFAULT_THREADS, objectMapper);
  }

  @Deprecated
  /** @deprecated Use {@link #SimpleRoadClient(OnrampOptions)} */
  public SimpleRoadClient(
      String host,
      String username,
      String password,
      String roadName,
      int threads,
      ObjectMapper objectMapper) {
    this(host, username, password, roadName, threads, null, objectMapper);
  }

  @Deprecated
  /** @deprecated Use {@link #SimpleRoadClient(OnrampOptions)} */
  public SimpleRoadClient(
      String host,
      String username,
      String password,
      String roadName,
      int threads,
      TLSConfig tlsConfig) {
    this(host, username, password, roadName, threads, tlsConfig, new ObjectMapper());
  }

  @Deprecated
  /** @deprecated Use {@link #SimpleRoadClient(OnrampOptions)} */
  public SimpleRoadClient(
      String host,
      String username,
      String password,
      String roadName,
      int threads,
      TLSConfig tlsConfig,
      ObjectMapper objectMapper) {
    this(toOptions(host, username, password, roadName, threads, tlsConfig, objectMapper));
  }

  public SimpleRoadClient(OnrampOptions options) {
    this(HttpHandler.onramp(options), options.getRoadName(), options.getObjectMapper());
  }

  @Override
  public StandardResponse sendMessage(T message) {
    return sendMessages(singletonList(message)).get(0);
  }

  @Override
  public List<StandardResponse> sendMessages(List<T> messages) {
    try {
      String json = objectMapper.writeValueAsString(messages);
      HttpEntity entity = EntityBuilder.create().setText(json).setContentType(APPLICATION_JSON).gzipCompress().build();
      HttpResponse response = handler.post("roads/" + roadName + "/messages", entity);

      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode >= HttpStatus.SC_INTERNAL_SERVER_ERROR) { // 500
        throw new OnrampServerException(errorResponseAsString(response));
      } else if (statusCode >= HttpStatus.SC_BAD_REQUEST) { // 400
        StandardResponse standardResponse = objectMapper.readValue(response.getEntity().getContent(),
            StandardResponse.class);
        return failedBatchResponse(messages, standardResponse);
      } else if (statusCode >= HttpStatus.SC_MULTIPLE_CHOICES) { // 300
        throw new OnrampServerException(errorResponseAsString(response));
      }

      return objectMapper.readValue(response.getEntity().getContent(), RESPONSE_REF);
    } catch (JsonProcessingException e) {
      throw new OnrampEncodingException(e);
    } catch (Exception e) {
      throw new OnrampException(e);
    }
  }

  private List<StandardResponse> failedBatchResponse(List<T> batch, StandardResponse response) {
    return batch.stream().map(m -> response).collect(toList());
  }

  public String errorResponseAsString(HttpResponse response) throws IOException {
    try (Reader reader = new InputStreamReader(response.getEntity().getContent())) {
      StatusLine statusLine = response.getStatusLine();
      return String.format("%s : %s", Objects.toString(statusLine), CharStreams.toString(reader));
    }
  }

  @Override
  public void close() throws Exception {
    handler.close();
  }

  private static OnrampOptions toOptions(String host, String username, String password, String roadName,
                                         int threads, TLSConfig tlsConfig, ObjectMapper objectMapper) {
    return OnrampOptions.builder()
        .host(host)
        .username(username)
        .password(password)
        .roadName(roadName)
        .threads(threads)
        .tlsConfigFactory(() -> tlsConfig)
        .objectMapper(objectMapper)
        .build();
  }
}
