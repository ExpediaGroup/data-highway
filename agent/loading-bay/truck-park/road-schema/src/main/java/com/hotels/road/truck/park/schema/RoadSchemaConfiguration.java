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
package com.hotels.road.truck.park.schema;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.kafkastore.KafkaStore;
import com.hotels.road.kafkastore.exceptions.SerializationException;
import com.hotels.road.kafkastore.serialization.Serializer;
import com.hotels.road.schema.serde.SchemaSerializationModule;
import com.hotels.road.truck.park.schema.KafkaStoreSchemaLookup.Road;

@Configuration
public class RoadSchemaConfiguration {

  @Bean
  KafkaStore<String, Road> roadStore(
      @Value("${kafka.bootstrapServers}") String bootstrapServers,
      @Value("${kafka.road.topic}") String topic,
      Serializer<String, Road> serializer) {
    return new KafkaStore<>(bootstrapServers, serializer, topic);
  }

  @Bean
  Serializer<String, Road> serializer(ObjectMapper mapper) {
    mapper.registerModule(new SchemaSerializationModule());
    return new Serializer<String, Road>() {
      @Override
      public byte[] serializeKey(String key) throws SerializationException {
        return key.getBytes(UTF_8);
      }

      @Override
      public byte[] serializeValue(Road value) throws SerializationException {
        try {
          return mapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
          throw new SerializationException(e);
        }
      }

      @Override
      public String deserializeKey(byte[] key) throws SerializationException {
        return new String(key, UTF_8);
      }

      @Override
      public Road deserializeValue(byte[] value) throws SerializationException {
        try {
          return mapper.readValue(value, Road.class);
        } catch (IOException e) {
          throw new SerializationException(e);
        }
      }
    };
  }

  @Bean
  ObjectMapper mapper() {
    return new ObjectMapper();
  }

}
