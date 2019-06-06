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
package com.hotels.road.tollbooth.app;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

import com.hotels.road.tollbooth.client.api.PatchOperation;

public class JsonPatchApplierTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private JsonPatchApplier underTest = new JsonPatchApplier(mapper);

  @Test
  public void createRoad() throws Exception {
    JsonNode road = mapper.readTree("{\"name\":\"road1\"}");

    List<PatchOperation> operations = singletonList(PatchOperation.add("", road));

    JsonNode result = underTest.apply(NullNode.getInstance(), operations);

    assertThat(result.isMissingNode(), is(false));
    assertThat(result.path("name").textValue(), is("road1"));
  }

  @Test
  public void removeRoad() throws Exception {
    JsonNode road = mapper.readTree("{\"name\":\"road1\"}");

    List<PatchOperation> operations = singletonList(PatchOperation.remove(""));

    JsonNode result = underTest.apply(road, operations);

    assertThat(result.isMissingNode(), is(true));
  }

  @Test
  public void addDescription() throws Exception {
    JsonNode road = mapper.readTree("{}");

    List<PatchOperation> operations = singletonList(PatchOperation.add("/description", "description1"));

    JsonNode result = underTest.apply(road, operations);

    assertThat(result.isMissingNode(), is(false));
    assertThat(result.get("description").textValue(), is("description1"));
  }

  @Test
  public void updateDescription() throws Exception {
    JsonNode road = mapper.readTree("{\"description\":\"description1\"}");

    List<PatchOperation> operations = singletonList(PatchOperation.replace("/description", "description2"));

    JsonNode result = underTest.apply(road, operations);

    assertThat(result.isMissingNode(), is(false));
    assertThat(result.get("description").textValue(), is("description2"));
  }

  @Test
  public void removeDescription() throws Exception {
    JsonNode road = mapper.readTree("{\"description\":\"description1\"}");

    List<PatchOperation> operations = singletonList(PatchOperation.remove("/description"));

    JsonNode result = underTest.apply(road, operations);

    assertThat(result.isMissingNode(), is(false));
    assertThat(result.get("description"), is(nullValue()));
  }

  @Test(expected = PatchApplicationException.class)
  public void throwsPatchApplicationExceptionWhenNotAValidPatch() throws Exception {
    JsonNode road = mapper.readTree("{\"description\":\"description1\"}");

    List<PatchOperation> operations = singletonList(PatchOperation.replace("/description", "description2"));

    JsonNode jsonNodePatch = mapper.readTree("{}");

    ObjectMapper mockObjectMapper = mock(ObjectMapper.class);
    when(mockObjectMapper.convertValue(any(), eq(JsonNode.class))).thenReturn(jsonNodePatch);

    underTest = new JsonPatchApplier(mockObjectMapper);
    underTest.apply(road, operations);
  }
}
