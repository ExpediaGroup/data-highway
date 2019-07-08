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
package com.hotels.road.weighbridge.function;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Predicate;

import org.springframework.stereotype.Component;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class HostPredicate implements Predicate<String> {
  static final String HOST = host();

  @Override
  public boolean test(String host) {
    return HOST.equals(host);
  }

  @SneakyThrows(UnknownHostException.class)
  private static String host() {
    String host = InetAddress.getLocalHost().getHostAddress();
    log.info("Running on host '{}'.", host);
    return host;
  }
}
