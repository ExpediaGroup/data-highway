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
package com.hotels.road.paver.service;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.exception.AlreadyExistsException;
import com.hotels.road.exception.UnknownDestinationException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.HiveDestination;
import com.hotels.road.paver.api.HiveDestinationAdminClient;
import com.hotels.road.paver.service.exception.InvalidLandingIntervalException;
import com.hotels.road.rest.model.HiveDestinationModel;

@RunWith(MockitoJUnitRunner.class)
public class HiveDestinationServiceImplTest {

  private static final String NAME = "road1";

  @Mock
  private HiveDestinationAdminClient hiveDestinationAdminClient;

  private HiveDestinationServiceImpl underTest;

  private HiveDestination hiveDestination;
  private HiveDestinationModel hiveDestinationModel;

  @Before
  public void before() {
    underTest = new HiveDestinationServiceImpl(hiveDestinationAdminClient);

    hiveDestination = new HiveDestination();

    hiveDestinationModel = new HiveDestinationModel();
  }

  @Test
  public void getHiveDestination() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.of(hiveDestination));

    HiveDestinationModel result = underTest.getHiveDestination(NAME);

    hiveDestinationModel.setLandingInterval("PT1H");
    assertThat(result, is(hiveDestinationModel));
  }

  @Test
  public void getHiveDestination_CustomInterval() throws Exception {
    hiveDestination.setLandingInterval("PT24H");
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.of(hiveDestination));

    HiveDestinationModel result = underTest.getHiveDestination(NAME);

    hiveDestinationModel.setLandingInterval("PT24H");
    assertThat(result, is(hiveDestinationModel));
  }

  @Test(expected = UnknownRoadException.class)
  public void getHiveDestination_UnknownRoad() throws Exception {
    doThrow(UnknownRoadException.class).when(hiveDestinationAdminClient).getHiveDestination(NAME);

    underTest.getHiveDestination(NAME);
  }

  @Test(expected = UnknownDestinationException.class)
  public void getHiveDestination_UnknownDestination() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.empty());

    underTest.getHiveDestination(NAME);
  }

  @Test
  public void createHiveDestination() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.empty());

    underTest.createHiveDestination(NAME, hiveDestinationModel);

    verify(hiveDestinationAdminClient).createHiveDestination(NAME, hiveDestination);
  }

  @Test
  public void createHiveDestination_CustomInterval() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.empty());

    hiveDestinationModel.setLandingInterval("P1D");
    underTest.createHiveDestination(NAME, hiveDestinationModel);

    hiveDestination.setLandingInterval("PT24H");
    verify(hiveDestinationAdminClient).createHiveDestination(NAME, hiveDestination);
  }

  @Test(expected = UnknownRoadException.class)
  public void createHiveDestination_UnknownRoad() throws Exception {
    doThrow(UnknownRoadException.class).when(hiveDestinationAdminClient).getHiveDestination(NAME);

    underTest.createHiveDestination(NAME, hiveDestinationModel);
  }

  @Test(expected = AlreadyExistsException.class)
  public void createHiveDestination_AlreadyExists() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.of(hiveDestination));

    underTest.createHiveDestination(NAME, hiveDestinationModel);
  }

  @Test
  public void updateHiveDestination() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.of(hiveDestination));

    underTest.updateHiveDestination(NAME, hiveDestinationModel);

    verify(hiveDestinationAdminClient).updateHiveDestination(NAME, hiveDestination);
  }

  @Test
  public void updateHiveDestination_CustomInterval() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.of(hiveDestination));

    hiveDestinationModel.setLandingInterval("P1D");
    underTest.updateHiveDestination(NAME, hiveDestinationModel);

    HiveDestination expectedHiveDestination = new HiveDestination();
    expectedHiveDestination.setLandingInterval("PT24H");
    verify(hiveDestinationAdminClient).updateHiveDestination(NAME, expectedHiveDestination);
  }

  @Test(expected = UnknownRoadException.class)
  public void updateHiveDestination_UnknownRoad() throws Exception {
    doThrow(UnknownRoadException.class).when(hiveDestinationAdminClient).getHiveDestination(NAME);

    underTest.updateHiveDestination(NAME, hiveDestinationModel);
  }

  @Test(expected = UnknownDestinationException.class)
  public void updateHiveDestination_UnknownDestination() throws Exception {
    when(hiveDestinationAdminClient.getHiveDestination(NAME)).thenReturn(Optional.empty());

    underTest.updateHiveDestination(NAME, hiveDestinationModel);
  }

  @Test
  public void validLandingInterval() throws Exception {
    assertThat(underTest.validateAndNormaliseLandingInterval("PT30M"), is("PT30M"));
  }

  @Test
  public void notSpecifiedInterval() throws Exception {
    assertThat(underTest.validateAndNormaliseLandingInterval(null), is(HiveDestination.DEFAULT_LANDING_INTERVAL));
  }

  @Test(expected = InvalidLandingIntervalException.class)
  public void invalidLandingInterval() throws Exception {
    underTest.validateAndNormaliseLandingInterval("abc");
  }

  @Test(expected = InvalidLandingIntervalException.class)
  public void intervalTooShort() throws Exception {
    underTest.validateAndNormaliseLandingInterval("PT1M");
  }

  @Test(expected = InvalidLandingIntervalException.class)
  public void intervalTooLong() throws Exception {
    underTest.validateAndNormaliseLandingInterval("P7D");
  }

  @Test
  public void deleteHiveDestination() throws Exception {
    underTest.deleteHiveDestination(NAME);
    verify(hiveDestinationAdminClient).deleteHiveDestination(NAME);
  }

  @Test(expected = UnknownRoadException.class)
  public void deleteHiveDestination_UnknownRoad() throws Exception {
    doThrow(UnknownRoadException.class).when(hiveDestinationAdminClient).deleteHiveDestination(NAME);
    underTest.deleteHiveDestination(NAME);
  }

  @Test(expected = UnknownDestinationException.class)
  public void deleteHiveDestination_UnknownDestination() throws Exception {
    doThrow(UnknownDestinationException.class).when(hiveDestinationAdminClient).deleteHiveDestination(NAME);
    underTest.deleteHiveDestination(NAME);
  }
}
