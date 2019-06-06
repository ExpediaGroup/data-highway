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
package com.hotels.road.security;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;

import static lombok.AccessLevel.PRIVATE;

import static com.google.common.collect.Iterables.any;

import java.util.List;

import org.apache.commons.net.util.SubnetUtils;
import org.apache.commons.net.util.SubnetUtils.SubnetInfo;

import lombok.RequiredArgsConstructor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

@RequiredArgsConstructor(access = PRIVATE)
public class CidrBlockAuthorisation {
  private final LoadingCache<List<String>, List<SubnetInfo>> cache;

  public CidrBlockAuthorisation() {
    this(cache());
  }

  private static LoadingCache<List<String>, List<SubnetInfo>> cache() {
    return CacheBuilder.newBuilder().expireAfterAccess(10, MINUTES).build(
        CacheLoader.from(x -> convertToSubnetInfos(x)));
  }

  private static List<SubnetInfo> convertToSubnetInfos(List<String> cidrBlocks) {
    return cidrBlocks.stream().map(x -> {
      SubnetUtils subnetUtils = new SubnetUtils(x);
      subnetUtils.setInclusiveHostCount(true);
      return subnetUtils.getInfo();
    }).collect(toList());
  }

  public boolean isAuthorised(List<String> cidrBlocks, String address) {
    return any(cache.getUnchecked(cidrBlocks), cidrBlock -> cidrBlock.isInRange(address));
  }
}
