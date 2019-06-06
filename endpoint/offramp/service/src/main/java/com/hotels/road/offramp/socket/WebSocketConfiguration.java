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
package com.hotels.road.offramp.socket;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.handler.PerConnectionWebSocketHandler;

@Configuration
@EnableWebSocket
public class WebSocketConfiguration implements WebSocketConfigurer {
  @Override
  public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
    registry
        .addHandler(webSocketHandler(), "/offramp/**")
        .addInterceptors(new OfframpHandshakeInterceptor())
        .setAllowedOrigins("*");
  }

  @Bean
  public TaskScheduler taskScheduler() {
      return new ConcurrentTaskScheduler(Executors.newSingleThreadScheduledExecutor());
  }

  @Bean
  public WebSocketHandler webSocketHandler() {
    return new PerConnectionWebSocketHandler(OfframpWebSocketHandler.class);
  }

  @Bean
  public ScheduledExecutorService executor() {
    return Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());
  }
}
