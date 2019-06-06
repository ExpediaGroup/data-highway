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
package com.hotels.road.loadingbay;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.road.loadingbay.model.Destinations;
import com.hotels.road.loadingbay.model.Hive;
import com.hotels.road.loadingbay.model.HiveRoad;
import com.hotels.road.loadingbay.model.HiveStatus;
import com.hotels.road.tollbooth.client.api.PatchOperation;

@RunWith(MockitoJUnitRunner.Silent.class)
public class LoadingBayTest {

  private static final String ROAD_NAME = "road1";

  @Mock
  private HiveTableAction hiveTableAction;

  private LoadingBay underTest;

  @Mock
  private Function<HiveRoad, LanderMonitor> monitorFactory;

  @Mock
  private LanderMonitor landerMonitor;

  private static final String CUSTOM_LANDING_INTERVAL = "PT15M";

  @Before
  public void before() {
    underTest = new LoadingBay(hiveTableAction, monitorFactory);
  }

  @Test
  public void noHiveDestination() {

    Destinations destinations = Destinations.builder().build();
    HiveRoad road = HiveRoad.builder().name(ROAD_NAME).destinations(destinations).build();

    List<PatchOperation> operations = underTest.inspectModel(ROAD_NAME, road);

    assertThat(operations.size(), is(0));
  }

  @Test
  public void actionsCreated() {
    Hive hive = Hive.builder().status(HiveStatus.builder().build()).build();
    Destinations destinations = Destinations.builder().hive(hive).build();
    HiveRoad road = HiveRoad.builder().name(ROAD_NAME).destinations(destinations).build();
    when(monitorFactory.apply(road)).thenReturn(landerMonitor);

    List<PatchOperation> operations = underTest.inspectModel(ROAD_NAME, road);

    assertThat(operations.size(), is(0));
    verify(landerMonitor).setEnabled(false);
    verify(landerMonitor).establishLandingFrequency("PT1H");
  }

  @Test
  public void enableHiveDestination() {
    Hive hive = Hive.builder().enabled(true).status(HiveStatus.builder().build()).build();
    Destinations destinations = Destinations.builder().hive(hive).build();
    HiveRoad road = HiveRoad.builder().name(ROAD_NAME).destinations(destinations).build();
    when(monitorFactory.apply(road)).thenReturn(landerMonitor);

    List<PatchOperation> operations = underTest.inspectModel(ROAD_NAME, road);

    assertThat(operations.size(), is(0));
    verify(landerMonitor).setEnabled(true);
  }

  @Test
  public void customLandingInterval() {
    Hive hive = Hive.builder().landingInterval(CUSTOM_LANDING_INTERVAL).status(HiveStatus.builder().build()).build();
    Destinations destinations = Destinations.builder().hive(hive).build();
    HiveRoad road = HiveRoad.builder().name(ROAD_NAME).destinations(destinations).build();
    when(monitorFactory.apply(road)).thenReturn(landerMonitor);

    List<PatchOperation> operations = underTest.inspectModel(ROAD_NAME, road);

    assertThat(operations.size(), is(0));
    verify(landerMonitor).establishLandingFrequency(CUSTOM_LANDING_INTERVAL);
  }

  @Test
  public void landerLastRunPresent() {
    Hive hive = Hive
        .builder()
        .landingInterval(CUSTOM_LANDING_INTERVAL)
        .status(
            HiveStatus.builder().lastRun(OffsetDateTime.ofInstant(Instant.ofEpochMilli(123L), ZoneOffset.UTC)).build())
        .build();
    Destinations destinations = Destinations.builder().hive(hive).build();
    HiveRoad road = HiveRoad.builder().name(ROAD_NAME).destinations(destinations).build();
    when(monitorFactory.apply(road)).thenReturn(landerMonitor);

    List<PatchOperation> operations = underTest.inspectModel(ROAD_NAME, road);

    assertThat(operations.size(), is(0));
    verify(landerMonitor).establishLandingFrequency(CUSTOM_LANDING_INTERVAL);
  }

  @Test
  public void exceptionDuringAction() {
    RuntimeException e = new RuntimeException("foo");
    doThrow(e).when(hiveTableAction).checkAndApply(any(HiveRoad.class));

    Hive hive = Hive.builder().status(HiveStatus.builder().build()).build();
    Destinations destinations = Destinations.builder().hive(hive).build();
    HiveRoad road = HiveRoad.builder().name(ROAD_NAME).destinations(destinations).build();

    List<PatchOperation> operations = underTest.inspectModel(ROAD_NAME, road);

    assertThat(operations.size(), is(0));
  }
}
