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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import com.fasterxml.jackson.databind.JsonNode;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.Payload;
import com.hotels.road.offramp.api.Record;
import com.hotels.road.testdrive.MemoryRoadConsumer.StreamKey;

@Api(tags = "testdrive")
@RestController
@RequestMapping("/testdrive/v1")
class TestDriveController {
  private final Map<String, Road> store;
  private final @Value("#{messages}") Map<String, List<Record>> messages;
  private final Map<StreamKey, AtomicInteger> commits;

  public TestDriveController(
      @Value("#{store}") Map<String, Road> store,
      @Value("#{messages}") Map<String, List<Record>> messages,
      @Value("#{commits}") Map<StreamKey, AtomicInteger> commits) {
    this.store = store;
    this.messages = messages;
    this.commits = commits;
  }

  @ApiOperation("Gets all messages for the given road")
  @GetMapping("/roads/{roadName}/messages")
  List<JsonNode> getMessages(@PathVariable String roadName) {
    return messages
        .getOrDefault(roadName, emptyList())
        .stream()
        .map(Record::getPayload)
        .map(Payload::getMessage)
        .collect(toList());
  }

  @ApiOperation("Deletes all data")
  @DeleteMapping("/roads")
  void deleteAll() {
    store.clear();
    messages.clear();
    commits.clear();
  }

  @ApiOperation("Deletes all data for the given road")
  @DeleteMapping("/roads/{roadName}")
  void deleteRoad(@PathVariable String roadName) {
    store.remove(roadName);
    deleteMessages(roadName);
  }

  @ApiOperation("Deletes all messages for the given road")
  @DeleteMapping("/roads/{roadName}/messages")
  void deleteMessages(@PathVariable String roadName) {
    messages.remove(roadName);
    commits.keySet().stream().filter(k -> k.getRoadName().equals(roadName)).forEach(commits::remove);
  }

  @ApiOperation("Deletes all commits for the given stream")
  @DeleteMapping("/roads/{roadName}/streams/{streamName}/messages")
  void deleteCommits(@PathVariable String roadName, @PathVariable String streamName) {
    commits.remove(new StreamKey(roadName, streamName));
  }
}
