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
package com.hotels.road.offramp.socket;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static com.hotels.road.rest.model.Sensitivity.PII;
import static com.hotels.road.rest.model.Sensitivity.PUBLIC;
import static com.hotels.road.security.AuthenticationType.ANONYMOUS;
import static com.hotels.road.security.AuthenticationType.AUTHENTICATED;
import static com.hotels.road.security.AuthorisationOutcome.AUTHORISED;
import static com.hotels.road.security.AuthorisationOutcome.ERROR;
import static com.hotels.road.security.AuthorisationOutcome.UNAUTHORISED;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AnonymousAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import io.micrometer.core.instrument.MeterRegistry;

import com.google.common.collect.ImmutableSet;

import com.hotels.road.model.core.Road;
import com.hotels.road.offramp.api.UnknownRoadException;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Offramp;
import com.hotels.road.security.SecurityMetrics;

@RunWith(MockitoJUnitRunner.class)
public class OfframpAuthorisationTest {
  private static final String ROAD_NAME = "road1";

  private @Mock Authentication authentication;
  private @Mock SecurityMetrics metrics;

  private final Map<String, Road> store = new HashMap<>();
  private final Road road = new Road();
  private final Authorisation authorisation = new Authorisation();
  private final Offramp offramp = new Offramp();

  private OfframpAuthorisation underTest;

  @Before
  public void before() throws Exception {
    road.setAuthorisation(authorisation);
    authorisation.setOfframp(offramp);
    store.put(ROAD_NAME, road);
    underTest = new OfframpAuthorisation(store, metrics);
  }

  @Test(expected = UnknownRoadException.class)
  public void roadDoesNotExist() throws Exception {
    store.clear();
    try {
      underTest.checkAuthorisation(null, ROAD_NAME, emptySet());
    } finally {
      verify(metrics).increment("road1", ANONYMOUS, ERROR);
    }
  }

  @Test
  public void anonymousRequestNoGrants() throws Exception {
    Authentication authentication = mock(AnonymousAuthenticationToken.class);
    underTest.checkAuthorisation(authentication, ROAD_NAME, emptySet());
    verify(metrics).increment("road1", ANONYMOUS, AUTHORISED);
  }

  @Test(expected = AccessDeniedException.class)
  public void anonymousRequestGrants() throws Exception {
    Authentication authentication = mock(AnonymousAuthenticationToken.class);
    try {
      underTest.checkAuthorisation(authentication, ROAD_NAME, singleton(PII));
    } finally {
      verify(metrics).increment("road1", ANONYMOUS, UNAUTHORISED);
    }
  }

  @Test
  public void authenticatedRequestNoGrants() throws Exception {
    offramp.setAuthorities(singletonMap("AUTHORITY", singleton(PUBLIC)));

    doReturn(true).when(authentication).isAuthenticated();
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORITY"))).when(authentication).getAuthorities();

    underTest.checkAuthorisation(authentication, ROAD_NAME, emptySet());
    verify(metrics).increment("road1", AUTHENTICATED, AUTHORISED);
  }

  @Test(expected = AccessDeniedException.class)
  public void authenticatedRequestNoGrantsNoAuthority() throws Exception {
    offramp.setAuthorities(emptyMap());

    doReturn(true).when(authentication).isAuthenticated();
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORITY"))).when(authentication).getAuthorities();

    try {
      underTest.checkAuthorisation(authentication, ROAD_NAME, emptySet());
    } finally {
      verify(metrics).increment("road1", AUTHENTICATED, UNAUTHORISED);
    }
  }

  @Test
  public void authenticatedRequestGrants() throws Exception {
    offramp.setAuthorities(singletonMap("AUTHORITY", ImmutableSet.of(PUBLIC, PII)));

    doReturn(true).when(authentication).isAuthenticated();
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORITY"))).when(authentication).getAuthorities();

    underTest.checkAuthorisation(authentication, ROAD_NAME, singleton(PII));
    verify(metrics).increment("road1", AUTHENTICATED, AUTHORISED);
  }

  @Test
  public void authenticatedRequestGrantsWithWildcard() throws Exception {
    offramp.setAuthorities(singletonMap("*", ImmutableSet.of(PUBLIC, PII)));

    doReturn(true).when(authentication).isAuthenticated();
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORITY"))).when(authentication).getAuthorities();

    underTest.checkAuthorisation(authentication, ROAD_NAME, singleton(PII));
    verify(metrics).increment("road1", AUTHENTICATED, AUTHORISED);
  }

  @Test(expected = AccessDeniedException.class)
  public void authenticatedRequestGrantsDenied() throws Exception {
    offramp.setAuthorities(singletonMap("AUTHORITY", emptySet()));

    doReturn(true).when(authentication).isAuthenticated();
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORITY"))).when(authentication).getAuthorities();

    try {
      underTest.checkAuthorisation(authentication, ROAD_NAME, singleton(PII));
    } finally {
      verify(metrics).increment("road1", AUTHENTICATED, UNAUTHORISED);
    }
  }

  @Test
  public void counterName() throws Exception {
    MeterRegistry registry = mock(MeterRegistry.class);
    OfframpAuthorisation underTest = new OfframpAuthorisation(store, registry);
    assertThat(underTest.getMetrics().getCounterName(), is("offramp_auth"));
  }
}
