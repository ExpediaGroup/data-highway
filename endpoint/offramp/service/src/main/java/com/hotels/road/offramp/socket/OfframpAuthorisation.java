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

import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toSet;

import static lombok.AccessLevel.PACKAGE;

import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.or;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Sets.union;

import static com.hotels.road.rest.model.Sensitivity.PUBLIC;
import static com.hotels.road.security.AuthenticationType.ANONYMOUS;
import static com.hotels.road.security.AuthorisationOutcome.AUTHORISED;
import static com.hotels.road.security.AuthorisationOutcome.ERROR;
import static com.hotels.road.security.AuthorisationOutcome.UNAUTHORISED;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Offramp;
import com.hotels.road.rest.model.Sensitivity;
import com.hotels.road.security.AuthenticationType;
import com.hotels.road.security.AuthorisationOutcome;
import com.hotels.road.security.SecurityMetrics;

@Component
@RequiredArgsConstructor(access = PACKAGE)
public class OfframpAuthorisation {
  private final Map<String, Road> store;
  private final @Getter(PACKAGE) SecurityMetrics metrics;

  @Autowired
  public OfframpAuthorisation(@Value("#{store}") Map<String, Road> store, MeterRegistry registry) {
    this(store, new SecurityMetrics(registry, "offramp_auth"));
  }

  public void checkAuthorisation(Authentication authentication, String roadName, Set<Sensitivity> grants)
    throws UnknownRoadException {
    AuthenticationType type = AuthenticationType.of(authentication);
    AuthorisationOutcome outcome = ERROR;
    try {
      checkAuthorisation(authentication, roadName, grants, type);
      outcome = AUTHORISED;
    } catch (AccessDeniedException e) {
      outcome = UNAUTHORISED;
      throw e;
    } finally {
      metrics.increment(roadName, type, outcome);
    }
  }

  private void checkAuthorisation(
      Authentication authentication,
      String roadName,
      Set<Sensitivity> grants,
      AuthenticationType type)
    throws UnknownRoadException {
    Road road = ofNullable(store.get(roadName)).orElseThrow(() -> new UnknownRoadException("Unknown road " + roadName));

    if (type == ANONYMOUS) {
      if (grants.isEmpty()) {
        return;
      }
      throw new AccessDeniedException("Access Denied. Anonymous users cannot read sensitive data.");
    }

    Set<Sensitivity> requestedGrants = union(singleton(PUBLIC), grants);

    Map<String, Set<Sensitivity>> authorities = Optional
        .of(road)
        .map(Road::getAuthorisation)
        .map(Authorisation::getOfframp)
        .map(Offramp::getAuthorities)
        .orElseThrow(
            () -> new AccessDeniedException(format("Access Denied. Missing requested grants: %s.", requestedGrants)));

    Set<String> userAuthorities = authentication.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(
        toSet());

    Set<Sensitivity> userGrants = authorities
        .entrySet()
        .stream()
        .filter(compose(or("*"::equals, userAuthorities::contains), Entry::getKey))
        .map(Entry::getValue)
        .flatMap(Collection::stream)
        .collect(toSet());

    Set<Sensitivity> missingGrants = difference(requestedGrants, userGrants);
    if (!missingGrants.isEmpty()) {
      throw new AccessDeniedException(String.format("Access Denied. Missing requested grants: %s.", missingGrants));
    }
  }
}
