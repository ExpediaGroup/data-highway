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
package com.hotels.road.paver.service.patchmapping;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.road.paver.service.patchmapping.OfframpAuthPatchMapping.PATH;
import static com.hotels.road.tollbooth.client.api.Operation.REPLACE;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.hotels.road.tollbooth.client.api.PatchOperation;

public class OfframpAuthPatchMappingTest {

  private final OfframpAuthPatchMapping underTest = new OfframpAuthPatchMapping();

  @Test
  public void path() throws Exception {
    assertThat(underTest.getPath(), is(PATH));
  }

  @Test(expected = IllegalArgumentException.class)
  public void remove() throws Exception {
    PatchOperation op = PatchOperation.remove(PATH);
    underTest.convertOperation(null, op);
  }

  @Test(expected = IllegalArgumentException.class)
  public void valueNotMap() throws Exception {
    PatchOperation op = PatchOperation.add(PATH, "notMap");
    underTest.convertOperation(null, op);
  }

  @Test(expected = IllegalArgumentException.class)
  public void mapValueNotList() throws Exception {
    Map<String, String> value = singletonMap("AUTHORITY", "notList");
    PatchOperation op = PatchOperation.add(PATH, value);
    underTest.convertOperation(null, op);
  }

  @Test(expected = IllegalArgumentException.class)
  public void mapValueTypeNotString() throws Exception {
    List<Integer> grants = singletonList(0);
    Map<String, List<Integer>> value = singletonMap("AUTHORITY", grants);
    PatchOperation op = PatchOperation.add(PATH, value);
    underTest.convertOperation(null, op);
  }

  @Test
  public void valid() throws Exception {
    List<String> grants = singletonList("PII");
    Map<String, List<String>> value = singletonMap("AUTHORITY", grants);
    PatchOperation op = PatchOperation.add(PATH, value);
    PatchOperation result = underTest.convertOperation(null, op);

    assertThat(result.getOperation(), is(REPLACE));
    assertThat(result.getPath(), is(PATH));
    assertThat(result.getValue(), is(value));
  }
}
