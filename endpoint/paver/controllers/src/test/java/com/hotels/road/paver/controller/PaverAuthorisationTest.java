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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.web.authentication.WebAuthenticationDetails;

import com.hotels.road.security.CidrBlockAuthorisation;

@RunWith(MockitoJUnitRunner.class)
public class PaverAuthorisationTest {

  private @Mock CidrBlockAuthorisation cidrBlockAuthorisation;
  private @Mock Authentication authentication;
  private @Mock WebAuthenticationDetails details;

  private final List<String> paverAuthorities = new ArrayList<>();
  private String cidrBlocks = "";

  private PaverAuthorisation underTest;

  @Before
  public void before() {
    paverAuthorities.clear();
    underTest = new PaverAuthorisation(paverAuthorities, cidrBlocks, cidrBlockAuthorisation);
  }

  @Test
  public void authorised() throws Exception {
    paverAuthorities.add("AUTHORIZED");
    doReturn(singletonList(new SimpleGrantedAuthority("AUTHORIZED"))).when(authentication).getAuthorities();

    boolean result = underTest.isAuthorised(authentication);
    assertThat(result, is(true));
  }

  @Test
  public void authorisedWildcard() throws Exception {
    paverAuthorities.add("*");

    boolean result = underTest.isAuthorised(authentication);
    assertThat(result, is(true));
  }

  @Test
  public void authorisedCidr() throws Exception {
    paverAuthorities.add("AUTHORIZED");
    cidrBlocks = "0.0.0.0/0";
    PaverAuthorisation underTest = new PaverAuthorisation(paverAuthorities, cidrBlocks, cidrBlockAuthorisation);

    doReturn(singletonList(new SimpleGrantedAuthority("FOO"))).when(authentication).getAuthorities();
    doReturn(details).when(authentication).getDetails();
    String address = "0.0.0.0";
    doReturn(address).when(details).getRemoteAddress();
    doReturn(true).when(cidrBlockAuthorisation).isAuthorised(singletonList(cidrBlocks), address);

    boolean result = underTest.isAuthorised(authentication);
    assertThat(result, is(true));
  }

  @Test
  public void unauthorisedCidr() throws Exception {
    paverAuthorities.add("AUTHORIZED");
    cidrBlocks = "0.0.0.0/0";
    PaverAuthorisation underTest = new PaverAuthorisation(paverAuthorities, cidrBlocks, cidrBlockAuthorisation);

    doReturn(singletonList(new SimpleGrantedAuthority("FOO"))).when(authentication).getAuthorities();
    doReturn(details).when(authentication).getDetails();
    String address = "0.0.0.0";
    doReturn(address).when(details).getRemoteAddress();
    doReturn(false).when(cidrBlockAuthorisation).isAuthorised(singletonList(cidrBlocks), address);

    boolean result = underTest.isAuthorised(authentication);
    assertThat(result, is(false));
  }

  @Test
  public void cidr_list_is_parsed_correctly() throws Exception {
    paverAuthorities.add("AUTHORIZED");
    cidrBlocks = "1.0.0.0/8,2.0.0.0/8";
    PaverAuthorisation underTest = new PaverAuthorisation(paverAuthorities, cidrBlocks, cidrBlockAuthorisation);

    doReturn(singletonList(new SimpleGrantedAuthority("FOO"))).when(authentication).getAuthorities();
    doReturn(details).when(authentication).getDetails();
    String address = "1.0.0.10";
    doReturn(address).when(details).getRemoteAddress();
    doReturn(true).when(cidrBlockAuthorisation).isAuthorised(asList("1.0.0.0/8", "2.0.0.0/8"), address);

    boolean result = underTest.isAuthorised(authentication);
    assertThat(result, is(true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void check_cidr_ranges_are_validated() throws Exception {
    String cidrRanges = "327.0.0.1/32";
    new PaverAuthorisation(paverAuthorities, cidrRanges, cidrBlockAuthorisation);
  }
}
