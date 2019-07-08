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

import static java.util.Optional.ofNullable;

import static com.google.common.collect.Iterables.find;

import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Component;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@RequiredArgsConstructor
public class BrokerNodeFunction {
  private final AdminClient client;

  public BrokerNode apply(Predicate<String> hostNamePredicate) {
    Collection<Node> nodes = KafkaFutures.join(client.describeCluster().nodes());

    try {
      Node node = find(nodes, n -> hostNamePredicate.test(n.host()));
      log.debug("Using broker {}", node);
      return new BrokerNode(node.id(), ofNullable(node.rack()).orElse("none"), node.host());
    } catch (NoSuchElementException e) {
      throw new RuntimeException("No broker found on localhost!");
    }
  }

  @Value
  public static class BrokerNode {
    int id;
    @NonNull String rack;
    @NonNull String host;
  }
}
