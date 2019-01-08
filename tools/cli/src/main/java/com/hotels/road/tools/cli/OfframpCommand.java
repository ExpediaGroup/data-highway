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
package com.hotels.road.tools.cli;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.offramp.client.OfframpClient;
import com.hotels.road.offramp.client.OfframpOptions;
import com.hotels.road.offramp.model.DefaultOffset;
import com.hotels.road.tls.TLSConfig;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;


@Slf4j
@SpringBootApplication
public class OfframpCommand implements CommandLineRunner {

  private final String        dataHighwayHost;
  private final String        username;
  private final String        password;
  private final String        roadName;
  private final String        streamName;
  private final DefaultOffset defaultOffset;
  private final TlsTrust      tlsTrust;


  @Autowired
  public OfframpCommand(
      @Value("${dataHighwayHost:none}")     String dataHighwayHost,
      @Value("${username:none}")            String username,
      @Value("${password:none}")            String password,
      @Value("${roadName:none}")            String roadName,
      @Value("${streamName:none}")          String streamName,
      @Value("${defaultOffset:LATEST}")     String defaultOffsetArg,
      @Value("${tlsTrust:NONE}")            String tlsTrustArg
  ){

    this.dataHighwayHost = dataHighwayHost;
    this.username        = username;
    this.password        = password;
    this.roadName        = roadName;
    this.streamName      = streamName;
    this.defaultOffset   = DefaultOffset.valueOf(defaultOffsetArg);
    this.tlsTrust        = TlsTrust.valueOf(tlsTrustArg);
  }


  public static void main(String[] args) {
    log.info("Starting " + OfframpCommand.class.getCanonicalName());

    SpringApplication.run(OfframpCommand.class, args);

    log.info("Finishing " + OfframpCommand.class.getCanonicalName());
  }

  @Override
  public void run(String... args) {

    try {
      OfframpClient<JsonNode> client = getClient();
      Flux.from(client.messages())
          .doOnNext((x) ->
              log.info("Message with payload of {} chars at {} ({})",
                  x.getPayload().get("payload").asText().length(),
                  (new java.util.Date(x.getTimestampMs())).toString(),
                  x.getTimestampMs() ))
//                .doOnError((e) -> log.error(e.getMessage()))
          .then()
          .block();
    } catch (Exception e) {
      log.error("Unknown error: ", e);
      throw e;
    }

    for (int i = 0; i < args.length; ++i) {
      log.debug("args[{}]: {}", i, args[i]);
    }
  }

  private OfframpOptions<JsonNode> getOptions() {

    final TLSConfig.Factory tlsFactory = this.tlsTrust.name().equals("ALL") ? TLSConfig.trustAllFactory() : null;

    return OfframpOptions
        .builder(JsonNode.class)
        .host(this.dataHighwayHost)
        .roadName(this.roadName)
        .streamName(this.streamName)
        .username(this.username)
        .password(this.password)
        .defaultOffset(this.defaultOffset)
        .tlsConfigFactory(tlsFactory)
        .build();
  }

  private OfframpClient<JsonNode> getClient() {
    return OfframpClient.create(getOptions());
  }

  //  private void logArguments(){
  //    log.debug( "\n" +
  //        "dataHighwayHost : " + this.dataHighwayHost + "\n" +
  //        "username        : " + this.username        + "\n" +
  //        "password        : " + this.password        + "\n" +
  //        "roadName        : " + this.roadName        + "\n" +
  //        "streamName      : " + this.streamName      + "\n" +
  //        "defaultOffset   : " + this.defaultOffset   + "\n"
  //    );
  //  }
  //
  //  private void logOptions(OfframpOptions<JsonNode> options){
  //    log.debug( "\n" +
  //        "username        : " + options.getUsername() + "\n" +
  //        "password        : " + options.getPassword() + "\n"
  //    );
  //  }
}
