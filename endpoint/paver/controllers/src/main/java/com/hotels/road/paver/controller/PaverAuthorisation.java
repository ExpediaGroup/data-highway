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
package com.hotels.road.paver.controller;

import java.util.List;

import org.apache.commons.net.util.SubnetUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

import com.google.common.base.Splitter;

import com.hotels.road.security.CidrBlockAuthorisation;

@Component
@Slf4j
public class PaverAuthorisation {
  private final List<String> paverAuthorities;
  private final List<String> cidrBlocks;
  private final CidrBlockAuthorisation cidrBlockAuthorisation;

  public PaverAuthorisation(
      @Value("${paver.authorisation.authorities}") List<String> paverAuthorities,
      @Value("${paver.authorisation.cidr-blocks:192.168.0.0/16}") String cidrBlocks,
      CidrBlockAuthorisation cidrBlockAuthorisation) {
    this.paverAuthorities = paverAuthorities;
    this.cidrBlocks = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(cidrBlocks);
    this.cidrBlockAuthorisation = cidrBlockAuthorisation;

    validateCidrBlocks(this.cidrBlocks);
  }

  private void validateCidrBlocks(List<String> cidrBlocks) {
    for (String cidrBlock : cidrBlocks) {
      try {
        new SubnetUtils(cidrBlock).getInfo();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid IP CIDR range \"" + cidrBlock + "\": " + e.getMessage());
      }
    }
  }

  public boolean isAuthorised(Authentication authentication) {
    if (paverAuthorities.contains("*")) {
      log.debug("Paver allows all");
      return true;
    }
    List<String> userAuthorities = Mono
        .justOrEmpty(authentication)
        .flatMapIterable(Authentication::getAuthorities)
        .map(GrantedAuthority::getAuthority)
        .collectList()
        .block();
    log.debug("Checking user authorities: {}", userAuthorities);
    for (String userAuthority : userAuthorities) {
      if (paverAuthorities.contains(userAuthority)) {
        log.debug("User authority {} is allowed by paver", userAuthority);
        return true;
      }
    }
    log.debug("User has no allowable authorities. Paver allows: {}", paverAuthorities);

    log.debug("Falling back to CIDR authorisation");
    WebAuthenticationDetails details = (WebAuthenticationDetails) authentication.getDetails();
    boolean cidrAuthorised = cidrBlockAuthorisation.isAuthorised(cidrBlocks, details.getRemoteAddress());
    if (!cidrAuthorised) {
      log.debug("IP {} is not in the allowable range: {}", details.getRemoteAddress(), cidrBlocks);
    }
    return cidrAuthorised;
  }
}
