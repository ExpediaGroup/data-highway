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
package com.hotels.road.onramp.controller;

import static com.hotels.road.security.AuthenticationType.AUTHENTICATED;
import static com.hotels.road.security.AuthorisationOutcome.AUTHORISED;
import static com.hotels.road.security.AuthorisationOutcome.ERROR;
import static com.hotels.road.security.AuthorisationOutcome.UNAUTHORISED;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static lombok.AccessLevel.PACKAGE;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.hotels.road.model.core.Road;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Onramp;
import com.hotels.road.security.AuthenticationType;
import com.hotels.road.security.AuthorisationOutcome;
import com.hotels.road.security.CidrBlockAuthorisation;
import com.hotels.road.security.SecurityMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.web.authentication.WebAuthenticationDetails;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor(access = PACKAGE)
public class OnrampAuthorisation {
  private final Map<String, Road> store;
  private final CidrBlockAuthorisation cidrBlockAuthorisation;
  private final @Getter(PACKAGE) SecurityMetrics metrics;
  private final Clock clock = Clock.systemUTC();
  private Instant lastWarn = Instant.EPOCH;

  @Autowired
  public OnrampAuthorisation(
      @Value("#{store}") Map<String, Road> store,
      CidrBlockAuthorisation cidrBlockAuthorisation,
      MeterRegistry registry) {
    this(store, cidrBlockAuthorisation, new SecurityMetrics(registry, "onramp_auth"));
  }

  public boolean isAuthorised(Authentication authentication, String roadName) {
    AuthenticationType type = AuthenticationType.of(authentication);
    AuthorisationOutcome outcome = ERROR;
    try {
      Optional<Road> road = Optional.ofNullable(store.get(roadName));
      if (road.isPresent()) {
        Optional<Onramp> onramp = road.map(Road::getAuthorisation).map(Authorisation::getOnramp);
        if (type == AUTHENTICATED) {
          outcome = isUserAuthorised(authentication, roadName, onramp);
        } else {
          outcome = isCidrAuthorised(authentication, roadName, onramp);
        }
      }
    } catch (Exception e) {
      log.error("Error authorising for road {}: {}", roadName, authentication, e);
      throw e;
    } finally {
      metrics.increment(roadName, type, outcome);
    }
    return outcome == AUTHORISED;
  }

  private AuthorisationOutcome isUserAuthorised(
      Authentication authentication,
      String roadName,
      Optional<Onramp> onramp) {
    List<String> roadAuthorities = onramp.map(Onramp::getAuthorities).orElse(emptyList());
    if (roadAuthorities.contains("*")) {
      log.debug("Onramp allows all");
      return AUTHORISED;
    }
    List<String> userAuthorities = authentication.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(
        toList());
    log.debug("Checking user authorities: {}", userAuthorities);
    for (String userAuthority : userAuthorities) {
      if (roadAuthorities.contains(userAuthority)) {
        log.debug("User authority {} is allowed by onramp", userAuthority);
        return AUTHORISED;
      }
    }
    log.debug("User has no allowable authorities. Onramp allows: {}", roadAuthorities);
    return UNAUTHORISED;
  }

  private AuthorisationOutcome isCidrAuthorised(
      Authentication authentication,
      String roadName,
      Optional<Onramp> onramp) {
    List<String> cidrBlocks = onramp.map(Onramp::getCidrBlocks).orElse(emptyList());
    WebAuthenticationDetails details = (WebAuthenticationDetails) authentication.getDetails();
    boolean authorised = cidrBlockAuthorisation.isAuthorised(cidrBlocks, details.getRemoteAddress());

    log.debug("CIDR Authorisation for road: {}, authorised: {}, remoteAddress: {}", roadName, authorised, details.getRemoteAddress());
    if (!authorised && log.isWarnEnabled()) {
      Instant now = clock.instant();
      if (now.isAfter(lastWarn.plusSeconds(1))) {
        lastWarn = now;
        log.warn("CIDR Authorisation failed for road: {}, remoteAddress: {}", roadName, details.getRemoteAddress());
      }
    }

    return authorised ? AUTHORISED : UNAUTHORISED;
  }
}
