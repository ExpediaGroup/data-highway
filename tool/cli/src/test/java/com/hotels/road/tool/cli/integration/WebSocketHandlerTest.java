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

package com.hotels.road.tool.cli.integration;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;

import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.hotels.road.tool.cli")
class WebSocketHandlerTest extends AbstractWebSocketHandler {

  @Override
  public void afterConnectionEstablished(WebSocketSession session) throws java.io.IOException {
    ObjectMapper mapper = new ObjectMapper();
    String event = mapper.writeValueAsString(TestMessage.getTestMessage());
    int half = event.length() / 2;
    String part1 = event.substring(0, half);
    String part2 = event.substring(half);
    session.sendMessage(new BinaryMessage(part1.getBytes(UTF_8), false));
    session.sendMessage(new BinaryMessage(part2.getBytes(UTF_8), true));
  }

}
