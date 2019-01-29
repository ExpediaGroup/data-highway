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
package com.hotels.road.onramp.controller;

import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import static com.hotels.road.security.AuthenticationType.ANONYMOUS;
import static com.hotels.road.security.AuthenticationType.AUTHENTICATED;
import static com.hotels.road.security.AuthorisationOutcome.AUTHORISED;
import static com.hotels.road.security.AuthorisationOutcome.ERROR;
import static com.hotels.road.security.AuthorisationOutcome.UNAUTHORISED;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import com.hotels.road.model.core.Road;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Onramp;
import com.hotels.road.security.CidrBlockAuthorisation;
import com.hotels.road.security.SecurityMetrics;

@RunWith(MockitoJUnitRunner.class)
public class OnrampAuthorisationTest {
  private static final String ROAD_NAME = "road1";

  private @Mock CidrBlockAuthorisation cidrBlockAuthorisation;
  private @Mock Authentication authentication;
  private @Mock WebAuthenticationDetails details;
  private @Mock SecurityMetrics metrics;

  private final Map<String, Road> store = new HashMap<>();
  private final Road road = new Road();
  private final Authorisation authorisation = new Authorisation();
  private final Onramp onramp = new Onramp();

  private OnrampAuthorisation underTest;

  private @Mock Counter counter;

  @Before
  public void before() throws Exception {
    road.setAuthorisation(authorisation);
    authorisation.setOnramp(onramp);
    store.put(ROAD_NAME, road);
    underTest = new OnrampAuthorisation(store, cidrBlockAuthorisation, metrics);
  }

  @Test
  public void authorised() throws Exception {
    onramp.setAuthorities(singletonList("AUTHORIZED"));
    doReturn(true).when(authentication).isAuthenticated();
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORIZED"))).when(authentication).getAuthorities();

    boolean result = underTest.isAuthorised(authentication, ROAD_NAME);
    assertThat(result, is(true));
    verify(metrics).increment("road1", AUTHENTICATED, AUTHORISED);
  }

  @Test
  public void authorisedWildcard() throws Exception {
    doReturn(true).when(authentication).isAuthenticated();
    onramp.setAuthorities(singletonList("*"));
    boolean result = underTest.isAuthorised(authentication, ROAD_NAME);
    assertThat(result, is(true));
    verify(metrics).increment("road1", AUTHENTICATED, AUTHORISED);
  }

  @Test
  public void authorisedCidr() throws Exception {
    onramp.setAuthorities(singletonList("AUTHORIZED"));
    List<String> cidrBlocks = singletonList("0.0.0.0/0");
    onramp.setCidrBlocks(cidrBlocks);
    doReturn(false).when(authentication).isAuthenticated();
    doReturn(details).when(authentication).getDetails();
    String address = "0.0.0.0";
    doReturn(address).when(details).getRemoteAddress();
    doReturn(true).when(cidrBlockAuthorisation).isAuthorised(cidrBlocks, address);

    boolean result = underTest.isAuthorised(authentication, ROAD_NAME);
    assertThat(result, is(true));
    verify(metrics).increment("road1", ANONYMOUS, AUTHORISED);
  }

  @Test
  public void unauthorisedCidr() throws Exception {
    onramp.setAuthorities(singletonList("AUTHORIZED"));
    List<String> cidrBlocks = singletonList("0.0.0.0/0");
    onramp.setCidrBlocks(cidrBlocks);

    doReturn(false).when(authentication).isAuthenticated();
    doReturn(details).when(authentication).getDetails();
    String address = "0.0.0.0";
    doReturn(address).when(details).getRemoteAddress();
    doReturn(false).when(cidrBlockAuthorisation).isAuthorised(cidrBlocks, address);

    boolean result = underTest.isAuthorised(authentication, ROAD_NAME);
    assertThat(result, is(false));
    verify(metrics).increment("road1", ANONYMOUS, UNAUTHORISED);
  }

  @Test
  public void roadDoesNotExist() throws Exception {
    store.clear();

    boolean result = underTest.isAuthorised(authentication, ROAD_NAME);
    assertThat(result, is(false));
    verify(metrics).increment("road1", ANONYMOUS, ERROR);
  }

  @Test
  public void counterName() throws Exception {
    MeterRegistry registry = mock(MeterRegistry.class);
    OnrampAuthorisation underTest = new OnrampAuthorisation(store, null, registry);
    assertThat(underTest.getMetrics().getCounterName(), is("onramp_auth"));
  }
}
