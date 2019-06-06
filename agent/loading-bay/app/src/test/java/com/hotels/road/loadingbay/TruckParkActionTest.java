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

// package com.hotels.road.agent.hive;
//
// import static org.hamcrest.CoreMatchers.is;
// import static org.junit.Assert.assertThat;
// import static org.mockito.ArgumentMatchers.any;
// import static org.mockito.ArgumentMatchers.anyString;
// import static org.mockito.Mockito.never;
// import static org.mockito.Mockito.verify;
// import static org.mockito.Mockito.when;
//
// import static com.hotels.road.agent.hive.AgentProperty.TRUCK_PARK_STATE;
//
// import java.util.List;
// import java.util.Optional;
//
// import org.junit.Before;
// import org.junit.Test;
// import org.junit.runner.RunWith;
// import org.mockito.Mock;
// import org.mockito.junit.MockitoJUnitRunner;
//
// import com.google.common.base.Equivalence;
//
// import com.hotels.road.agent.hive.model.Destinations;
// import com.hotels.road.agent.hive.model.Hive;
// import com.hotels.road.agent.hive.model.HiveRoad;
// import com.hotels.road.agent.hive.model.KafkaStatus;
// import com.hotels.road.agent.hive.truck.park.TruckPark;
// import com.hotels.road.agent.hive.truck.park.TruckParkAdmin;
// import com.hotels.road.tollbooth.client.api.PatchOperation;
//
// @RunWith(MockitoJUnitRunner.Silent.class)
// public class TruckParkActionTest {
//
// private static final String ROAD_NAME = "road1";
//
// static class TestTruckPark implements TruckPark {}
//
// private final TestTruckPark existingConfig = new TestTruckPark();
// private final TestTruckPark newConfig = new TestTruckPark();
//
// @Mock
// private TruckParkAdmin<TestTruckPark> admin;
// @Mock
// private TruckPark.Factory<TestTruckPark> factory;
// @Mock
// private Equivalence<TestTruckPark> equivalence;
//
// private TruckParkAction<TestTruckPark> underTest;
//
// @Before
// public void before() {
// underTest = new TruckParkAction<>(admin, factory, equivalence);
// }
//
// @Test
// public void doNothinWhenHiveDestinationIsDisabledAndNoConfigExists() {
// HiveRoad road = HiveRoad
// .builder()
// .name(ROAD_NAME)
// .destinations(Destinations.builder().hive(Hive.builder().enabled(false).build()).build())
// .build();
//
// when(admin.get(ROAD_NAME)).thenReturn(Optional.empty());
//
// List<PatchOperation> operations = underTest.checkAndApply(road);
//
// verify(admin, never()).create(anyString(), any(TestTruckPark.class));
// verify(admin, never()).update(anyString(), any(TestTruckPark.class));
// verify(admin, never()).delete(anyString());
// assertThat(operations.size(), is(0));
// }
//
// @Test
// public void deleteWhenNoHiveDestinationIsDisabledAndConfigExists() {
// HiveRoad road = HiveRoad
// .builder()
// .name(ROAD_NAME)
// .destinations(Destinations.builder().hive(Hive.builder().enabled(false).build()).build())
// .build();
//
// when(admin.get(ROAD_NAME)).thenReturn(Optional.of(existingConfig));
//
// List<PatchOperation> operations = underTest.checkAndApply(road);
//
// verify(admin, never()).create(anyString(), any(TestTruckPark.class));
// verify(admin, never()).update(anyString(), any(TestTruckPark.class));
// verify(admin).delete(ROAD_NAME);
// assertThat(operations.size(), is(1));
// assertThat(operations.get(0), is(PatchOperation.replace(TRUCK_PARK_STATE.path(), "stopped")));
// }
//
// @Test
// public void createdWhenNotExists() {
// HiveRoad road = HiveRoad
// .builder()
// .name(ROAD_NAME)
// .destinations(Destinations.builder().hive(Hive.builder().enabled(true).build()).build())
// .status(KafkaStatus.builder().partitions(1).build())
// .build();
//
// when(admin.get(ROAD_NAME)).thenReturn(Optional.empty());
// when(factory.newInstance(road)).thenReturn(newConfig);
//
// List<PatchOperation> operations = underTest.checkAndApply(road);
//
// verify(admin).create(ROAD_NAME, newConfig);
// verify(admin, never()).update(anyString(), any(TestTruckPark.class));
// verify(admin, never()).delete(anyString());
//
// assertThat(operations.size(), is(1));
// assertThat(operations.get(0), is(PatchOperation.replace(TRUCK_PARK_STATE.path(), "running")));
// }
//
// @Test
// public void updatedWhenNotEquivalent() {
// HiveRoad road = HiveRoad
// .builder()
// .name(ROAD_NAME)
// .destinations(Destinations.builder().hive(Hive.builder().enabled(true).build()).build())
// .status(KafkaStatus.builder().partitions(1).build())
// .build();
//
// when(admin.get(ROAD_NAME)).thenReturn(Optional.of(existingConfig));
// when(factory.newInstance(road)).thenReturn(newConfig);
// when(equivalence.equivalent(newConfig, existingConfig)).thenReturn(false);
//
// List<PatchOperation> operations = underTest.checkAndApply(road);
//
// verify(admin, never()).create(anyString(), any(TestTruckPark.class));
// verify(admin).update(ROAD_NAME, newConfig);
// verify(admin, never()).delete(anyString());
//
// assertThat(operations.size(), is(1));
// assertThat(operations.get(0), is(PatchOperation.replace(TRUCK_PARK_STATE.path(), "running")));
// }
//
// @Test
// public void doNothingWhenEquivalent() {
// HiveRoad road = HiveRoad
// .builder()
// .name(ROAD_NAME)
// .destinations(Destinations.builder().hive(Hive.builder().enabled(true).build()).build())
// .status(KafkaStatus.builder().partitions(1).build())
// .build();
//
// when(admin.get(ROAD_NAME)).thenReturn(Optional.of(existingConfig));
// when(factory.newInstance(road)).thenReturn(newConfig);
// when(equivalence.equivalent(newConfig, existingConfig)).thenReturn(true);
//
// List<PatchOperation> operations = underTest.checkAndApply(road);
//
// verify(admin, never()).create(anyString(), any(TestTruckPark.class));
// verify(admin, never()).update(anyString(), any(TestTruckPark.class));
// verify(admin, never()).delete(anyString());
//
// assertThat(operations.size(), is(0));
// }
//
// }
