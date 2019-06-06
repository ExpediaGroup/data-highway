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
package com.hotels.road.agents.trafficcop;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.agents.trafficcop.spi.Agent;
import com.hotels.road.agents.trafficcop.spi.ModelReader;
import com.hotels.road.kafkastore.StoreUpdateObserver;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

public class TrafficCopConfigurationTest {
  @Test
  public void conditionalOnAgent() {
    try (AnnotationConfigApplicationContext context = trafficCopContext()) {
      context.register(TestAgent.class);
      context.refresh();
      assertThat(context.getBean(StoreUpdateObserver.class), is(instanceOf(AgentStoreObserver.class)));
      assertThat(context.getBean(ModelInspector.class), is(notNullValue()));
    }
  }

  @Test
  public void conditionalOnAgent_missing() {
    try (AnnotationConfigApplicationContext context = trafficCopContext()) {
      context.refresh();
      assertThat(context.getBean(StoreUpdateObserver.class), is(instanceOf(NullStoreUpdateObserver.class)));
      try {
        context.getBean(ModelInspector.class);
        fail();
      } catch (NoSuchBeanDefinitionException ignore) {}
    }
  }

  private AnnotationConfigApplicationContext trafficCopContext() {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.register(TrafficCopConfiguration.class, ObjectMapper.class);
    context.register(TestConfiguration.class); // Fake KafkaStore, PatchSetEmitter & ModelReader
    return context;
  }

  static class TestAgent implements Agent<Void> {
    @Override
    public List<PatchOperation> newModel(String key, Void newModel) {
      return null;
    }

    @Override
    public List<PatchOperation> updatedModel(String key, Void oldModel, Void newModel) {
      return null;
    }

    @Override
    public void deletedModel(String key, Void oldModel) {}

    @Override
    public List<PatchOperation> inspectModel(String key, Void model) {
      return null;
    }
  }

  static class TestConfiguration {
    @Primary
    @Bean
    Map<String, Void> store() {
      return new HashMap<>();
    }

    @Primary
    @Bean
    PatchSetEmitter modificationEmitter() {
      return patchSet -> {};
    }

    @Bean
    ModelReader<Void> modelReader() {
      return json -> null;
    }
  }
}
