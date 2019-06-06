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
package com.hotels.road.towtruck;

import static fm.last.commons.lang.units.IecByteUnit.MEBIBYTES;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.boot.DataHighwayApplication;
import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.s3.io.S3MultipartOutputStream;

@SpringBootApplication
@EnableScheduling
public class TowtruckApp {
  @Bean
  public ObjectMapper mapper() {
    return new ObjectMapper();
  }

  @Bean
  public Map<String, JsonNode> store(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.road.topic}") String topic,
      Serializer<String, JsonNode> serializer) {
    return new KafkaStore<>(bootstrapServers, serializer, topic);
  }

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  Supplier<String> keySupplier(Clock clock, @Value("${s3.keyPrefix}") String keyPrefix) {
    return () -> {
      long millis = clock.millis();
      String date = ISODateTimeFormat.date().withZoneUTC().print(millis);
      String time = ISODateTimeFormat.basicDateTimeNoMillis().withZoneUTC().print(millis);
      return String.format("%s/%s/%s.json.gz", keyPrefix, date, time);
    };
  }

  @Bean
  AmazonS3 s3(
      @Value("${s3.endpoint.url}") String s3EndpointUrl,
      @Value("${s3.endpoint.signingRegion}") String signingRegion) {
    return AmazonS3Client
        .builder()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .withEndpointConfiguration(new EndpointConfiguration(s3EndpointUrl, signingRegion))
        .build();
  }

  @Bean
  Function<String, OutputStream> outputStreamFactory(AmazonS3 s3, @Value("${s3.bucket}") String bucket) {
    return key -> {
      try {
        return new GZIPOutputStream(S3MultipartOutputStream
            .builder()
            .s3(s3, bucket, key)
            .partSize((int) MEBIBYTES.toBytes(5))
            .retries(3, 10)
            .async(1, 1)
            .build());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };
  }

  public static void main(String[] args) {
    DataHighwayApplication.run(TowtruckApp.class, args);
  }
}
