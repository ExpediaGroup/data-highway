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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.util.function.Supplier;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathParser.Path;

public class PartitionPathSupplierTest {

  @Test
  public void nullOnMissingPath() throws Exception {
    JsonNode model = new ObjectMapper().readTree("{}");
    @SuppressWarnings("resource")
    Supplier<Path> pathSupplier = new PartitionPathSupplier(() -> model);
    assertThat(pathSupplier.get(), is(nullValue()));
  }

  @Test
  public void nullOnNullPathValue() throws Exception {
    JsonNode model = new ObjectMapper().readTree("{\"partitionPath\":null}");
    @SuppressWarnings("resource")
    Supplier<Path> pathSupplier = new PartitionPathSupplier(() -> model);
    assertThat(pathSupplier.get(), is(nullValue()));
  }

  @Test
  public void nullOnEmptyPath() throws Exception {
    JsonNode model = new ObjectMapper().readTree("{\"partitionPath\":\"\"}");
    @SuppressWarnings("resource")
    Supplier<Path> pathSupplier = new PartitionPathSupplier(() -> model);
    assertThat(pathSupplier.get(), is(nullValue()));
  }

  @Test
  public void path() throws Exception {
    JsonNode model = new ObjectMapper().readTree("{\"partitionPath\":\"$.a\"}");
    @SuppressWarnings("resource")
    Supplier<Path> pathSupplier = new PartitionPathSupplier(() -> model);
    assertThat(pathSupplier.get(), is(KeyPathParser.parse("$.a")));
  }

}
