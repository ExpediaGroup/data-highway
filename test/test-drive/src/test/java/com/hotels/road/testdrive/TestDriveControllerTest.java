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

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.testdrive.MemoryRoadConsumer.StreamKey;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = TestDriveController.class)
public class TestDriveControllerTest {
  private final ObjectMapper mapper = new ObjectMapper();
  private @Autowired TestDriveController underTest;
  private @Autowired ApplicationContext context;

  private @MockBean(name = "store") Map<String, Road> store;
  private @MockBean(name = "messages") Map<String, List<Payload<JsonNode>>> messages;
  private @MockBean(name = "commits") Map<StreamKey, AtomicInteger> commits;

  private MockMvc mockMvc;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    mockMvc = standaloneSetup(underTest).build();
    store = (Map<String, Road>) context.getBean("store");
    messages = (Map<String, List<Payload<JsonNode>>>) context.getBean("messages");
    commits = (Map<StreamKey, AtomicInteger>) context.getBean("commits");
  }

  @Test
  public void getMessages_roadDoesNotExist() throws Exception {
    doReturn(emptyList()).when(messages).getOrDefault("road1", emptyList());
    mockMvc.perform(get("/testdrive/v1/roads/road1/messages")).andExpect(status().isOk()).andExpect(
        content().json("[]"));
  }

  @Test
  public void getMessages() throws Exception {
    JsonNode node = mapper.createObjectNode().put("foo", "bar");
    doReturn(singletonList(new Record(0, 1, 2L, new Payload<JsonNode>((byte) 0, 1, node)))).when(messages).getOrDefault(
        "road1", emptyList());
    mockMvc.perform(get("/testdrive/v1/roads/road1/messages")).andExpect(status().isOk()).andExpect(
        content().json("[{\"foo\":\"bar\"}]"));
  }

  @Test
  public void deleteAll() throws Exception {
    mockMvc.perform(delete("/testdrive/v1/roads")).andExpect(status().isOk());
    verify(store).clear();
    verify(messages).clear();
    verify(commits).clear();
  }

  @Test
  public void deleteRoad() throws Exception {
    StreamKey key = new StreamKey("road1", "stream1");
    doReturn(singleton(key)).when(commits).keySet();
    mockMvc.perform(delete("/testdrive/v1/roads/road1")).andExpect(status().isOk());
    verify(store).remove("road1");
    verify(messages).remove("road1");
    verify(commits).remove(key);
  }

  @Test
  public void deleteMessages() throws Exception {
    doReturn(singleton(new StreamKey("road1", "stream1"))).when(commits).keySet();
    mockMvc.perform(delete("/testdrive/v1/roads/road1/messages")).andExpect(status().isOk());
    verify(messages).remove("road1");
    verify(commits).remove(new StreamKey("road1", "stream1"));
  }

  @Test
  public void deleteCommits() throws Exception {
    doReturn(singleton(new StreamKey("road1", "stream1"))).when(commits).keySet();
    mockMvc.perform(delete("/testdrive/v1/roads/road1/streams/stream1/messages")).andExpect(status().isOk());
    verify(commits).remove(new StreamKey("road1", "stream1"));
  }
}
