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
package com.hotels.road.offramp.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OfframpServiceFactoryTest {

  private @Mock OfframpServiceV2.Factory serviceV2Factory;
  private @Mock OfframpServiceV2 serviceV2;

  private OfframpServiceFactory serviceFactory;

  @Before
  public void before() throws Exception {
    serviceFactory = new OfframpServiceFactory(serviceV2Factory);
    doReturn(serviceV2).when(serviceV2Factory).create(any(), any(), any(), any());
  }

  @Test
  public void create() throws Exception {
    String offrampVersion = "2";
    OfframpService serviceOut = serviceFactory.create(offrampVersion, any(), any(), any(), any());
    assertThat(serviceOut, is(serviceV2));
  }
}
