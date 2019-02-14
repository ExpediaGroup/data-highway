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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSortedSet;

import com.hotels.road.model.core.HiveDestination;
import com.hotels.road.model.core.KafkaStatus;
import com.hotels.road.model.core.MessageStatus;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.notification.model.RoadCreatedNotification;
import com.hotels.road.paver.api.RoadAdminClient;
import com.hotels.road.paver.api.SchemaStoreClient;
import com.hotels.road.paver.service.patchmapping.EnabledPatchMapping;
import com.hotels.road.paver.service.patchmapping.PartitionPathPatchMapping;
import com.hotels.road.paver.service.patchmapping.PatchMapping;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Offramp;
import com.hotels.road.rest.model.Authorisation.Onramp;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.security.CidrBlockValidator;
import com.hotels.road.tollbooth.client.api.Operation;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;

@RunWith(MockitoJUnitRunner.class)
public class PaverServiceImplTest {

  private static final Schema MESSAGE_SCHEMA = SchemaBuilder
      .record("r")
      .fields()
      .name("f")
      .type()
      .booleanType()
      .noDefault()
      .endRecord();
  private static final String ROAD_NAME = "wide_road";

  private final List<PatchMapping> mappings = Stream.of(new EnabledPatchMapping(), new PartitionPathPatchMapping())
      .collect(toList());

  private final Road road = new Road();

  @Mock
  private RoadAdminClient roadAdminClient;
  @Mock
  private SchemaStoreClient schemaStoreClient;

  private final CidrBlockValidator cidrBlockValidator = new CidrBlockValidator();

  private PaverService underTest;

  @Mock
  private RoadSchemaNotificationHandler notificationHandler;

  @Before
  public void before() {
    road.setName(ROAD_NAME);
    road.setContactEmail("contect@example.org");
    road.setDescription("road description");
    road.setTeamName("awesome team");
    road.setAuthorisation(new Authorisation());
    road.getAuthorisation().setOnramp(new Onramp());
    road.getAuthorisation().getOnramp().setCidrBlocks(emptyList());
    road.getAuthorisation().getOnramp().setAuthorities(emptyList());
    road.getAuthorisation().setOfframp(new Offramp());
    road.getAuthorisation().getOfframp().setAuthorities(emptyMap());
    underTest = new PaverServiceImpl(roadAdminClient, schemaStoreClient, cidrBlockValidator, mappings,
        notificationHandler, true, () -> 0);
  }

  @Test
  public void addSchemaWithoutVersion() throws Exception {
    when(roadAdminClient.getRoad(ROAD_NAME)).thenReturn(Optional.of(road));
    when(schemaStoreClient.registerSchema(ROAD_NAME, MESSAGE_SCHEMA))
        .thenReturn(new SchemaVersion(MESSAGE_SCHEMA, 1, false));
    SchemaVersion schemaVersion = underTest.addSchema(ROAD_NAME, MESSAGE_SCHEMA);
    assertThat(schemaVersion.getSchema(), is(MESSAGE_SCHEMA));
    verify(notificationHandler).handleSchemaCreated(ROAD_NAME, schemaVersion.getVersion());
  }

  @Test
  public void addSchemaWithVersion() throws Exception {
    when(roadAdminClient.getRoad(ROAD_NAME)).thenReturn(Optional.of(road));
    when(schemaStoreClient.registerSchema(ROAD_NAME, MESSAGE_SCHEMA, 1))
        .thenReturn(new SchemaVersion(MESSAGE_SCHEMA, 1, false));
    SchemaVersion schemaVersion = underTest.addSchema(ROAD_NAME, MESSAGE_SCHEMA, 1);

    assertThat(schemaVersion.getSchema(), is(MESSAGE_SCHEMA));
    verify(schemaStoreClient).registerSchema(ROAD_NAME, MESSAGE_SCHEMA, 1);
    verify(notificationHandler).handleSchemaCreated(ROAD_NAME, schemaVersion.getVersion());
  }

  @Test
  public void listRoads() throws Exception {
    when(roadAdminClient.listRoads()).thenReturn(ImmutableSortedSet.of("road1", "road2"));
    Set<String> roadNames = underTest.getRoadNames();

    assertThat(roadNames.size(), is(2));
    assertTrue(roadNames.contains("road1"));
    assertTrue(roadNames.contains("road2"));
  }

  @Test
  public void createRoad() throws Exception {
    HiveDestination hive = new HiveDestination();
    hive.setEnabled(true);
    hive.setLandingInterval(HiveDestination.DEFAULT_LANDING_INTERVAL);
    road.setDestinations(singletonMap("hive", hive));

    road.getAuthorisation().getOnramp().setCidrBlocks(singletonList("0.0.0.0/0"));

    BasicRoadModel model = new BasicRoadModel(road.getName(), road.getDescription(), road.getTeamName(),
        road.getContactEmail(), road.isEnabled(), road.getPartitionPath(), road.getAuthorisation(), road.getMetadata());
    underTest.createRoad(model);
    ArgumentCaptor<Road> captor = ArgumentCaptor.forClass(Road.class);
    verify(roadAdminClient).createRoad(captor.capture());
    Road createdRoad = captor.getValue();
    assertThat(createdRoad.getCompatibilityMode(), is(Road.DEFAULT_COMPATIBILITY_MODE));
    assertThat(createdRoad, is(road));
    verify(notificationHandler).handleRoadCreated(road);
  }

  @Test
  public void createRoadNullAuth() throws Exception {
    HiveDestination hive = new HiveDestination();
    hive.setEnabled(true);
    hive.setLandingInterval(HiveDestination.DEFAULT_LANDING_INTERVAL);
    road.setDestinations(singletonMap("hive", hive));

    road.getAuthorisation().getOnramp().setCidrBlocks(emptyList());
    road.getAuthorisation().getOnramp().setAuthorities(emptyList());
    road.getAuthorisation().getOfframp().setAuthorities(emptyMap());

    BasicRoadModel model = new BasicRoadModel(road.getName(), road.getDescription(), road.getTeamName(),
        road.getContactEmail(), road.isEnabled(), road.getPartitionPath(), null, road.getMetadata());
    underTest.createRoad(model);
    ArgumentCaptor<Road> captor = ArgumentCaptor.forClass(Road.class);
    verify(roadAdminClient).createRoad(captor.capture());
    Road createdRoad = captor.getValue();
    assertThat(createdRoad.getCompatibilityMode(), is(Road.DEFAULT_COMPATIBILITY_MODE));
    assertThat(createdRoad, is(road));
  }

  @Test
  public void createRoadHiveDestinationDisabled() throws Exception {
    BasicRoadModel model = new BasicRoadModel(road.getName(), road.getDescription(), road.getTeamName(),
        road.getContactEmail(), road.isEnabled(), road.getPartitionPath(), null, road.getMetadata());
    underTest = new PaverServiceImpl(roadAdminClient, schemaStoreClient, cidrBlockValidator, mappings,
        notificationHandler, false, () -> 0);
    underTest.createRoad(model);
    ArgumentCaptor<Road> captor = ArgumentCaptor.forClass(Road.class);
    verify(roadAdminClient).createRoad(captor.capture());
    Road createdRoad = captor.getValue();
    assertThat(createdRoad.getCompatibilityMode(), is(Road.DEFAULT_COMPATIBILITY_MODE));
    assertThat(createdRoad, is(road));
    RoadCreatedNotification
        .builder()
        .roadName(road.getName())
        .contactEmail(road.getContactEmail())
        .description(road.getDescription())
        .teamName(road.getTeamName())
        .build();
  }

  @Test
  public void applyPatch_enableRoad() throws Exception {
    when(roadAdminClient.getRoad(road.getName())).thenReturn(Optional.of(road));
    KafkaStatus status = new KafkaStatus();
    status.setTopicCreated(true);
    road.setStatus(status);
    List<PatchOperation> patchSet = Arrays.asList(PatchOperation.add("/enabled", Boolean.TRUE));
    underTest.applyPatch(road.getName(), patchSet);

    ArgumentCaptor<PatchSet> patchCaptor = ArgumentCaptor.forClass(PatchSet.class);
    verify(roadAdminClient).updateRoad(patchCaptor.capture());
    PatchSet patch = patchCaptor.getValue();

    assertThat(patch.getDocumentId(), is(road.getName()));
    assertThat(patch.getOperations().size(), is(2));
    assertThat(patch.getOperations().get(0).getOperation(), is(Operation.REPLACE));
    assertThat(patch.getOperations().get(0).getPath(), is("/enabled"));
    assertThat(patch.getOperations().get(0).getValue(), is(Boolean.TRUE));
    assertThat(patch.getOperations().get(1).getOperation(), is(Operation.ADD));
    assertThat(patch.getOperations().get(1).getPath(), is("/enabledTimeStamp"));
    assertThat(patch.getOperations().get(1).getValue(), is(0L));
  }

  @Test
  public void applyPatch() throws Exception {
    final String partitionPath = "$.group";
    when(roadAdminClient.getRoad(road.getName())).thenReturn(Optional.of(road));
    List<PatchOperation> patchSet = Arrays.asList(PatchOperation.add("/partitionPath", partitionPath));
    underTest.applyPatch(road.getName(), patchSet);

    ArgumentCaptor<PatchSet> patchCaptor = ArgumentCaptor.forClass(PatchSet.class);
    verify(roadAdminClient).updateRoad(patchCaptor.capture());
    PatchSet patch = patchCaptor.getValue();

    assertThat(patch.getDocumentId(), is(road.getName()));
    assertThat(patch.getOperations().size(), is(1));
    assertThat(patch.getOperations().get(0).getOperation(), is(Operation.REPLACE));
    assertThat(patch.getOperations().get(0).getPath(), is("/partitionPath"));
    assertThat(patch.getOperations().get(0).getValue(), is(partitionPath));
  }

  @Test(expected = IllegalArgumentException.class)
  public void applyingPatchFailsWhenMappingFails() throws Exception {
    when(roadAdminClient.getRoad(road.getName())).thenReturn(Optional.of(road));
    KafkaStatus status = new KafkaStatus();
    status.setTopicCreated(false);
    road.setStatus(status);
    List<PatchOperation> patchSet = Arrays.asList(PatchOperation.add("/enabled", Boolean.TRUE));
    underTest.applyPatch(road.getName(), patchSet);
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteRoad_failsWithActiveHiveDestination() throws Exception {
    when(roadAdminClient.getRoad(road.getName())).thenReturn(Optional.of(road));
    HiveDestination hive = new HiveDestination();
    hive.setEnabled(true);
    hive.setLandingInterval(HiveDestination.DEFAULT_LANDING_INTERVAL);
    road.setDestinations(singletonMap("hive", hive));
    underTest.deleteRoad(road.getName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void deleteRoad_failsWithEnabledRoad() throws Exception {
    when(roadAdminClient.getRoad(road.getName())).thenReturn(Optional.of(road));
    road.setEnabled(true);
    underTest.deleteRoad(road.getName());
  }

  @Test
  public void deleteRoad() throws Exception {
    when(roadAdminClient.getRoad(road.getName())).thenReturn(Optional.of(road));
    MessageStatus m = new MessageStatus();
    m.setLastUpdated(100);
    m.setNumberOfMessages(0);
    road.setMessagestatus(m);
    road.setEnabledTimeStamp(99);
    underTest.deleteRoad(road.getName());
    ArgumentCaptor<PatchSet> patchCaptor = ArgumentCaptor.forClass(PatchSet.class);
    verify(roadAdminClient).updateRoad(patchCaptor.capture());
    PatchSet patch = patchCaptor.getValue();
    assertThat(patch.getDocumentId(), is(road.getName()));
    assertThat(patch.getOperations().size(), is(1));
    assertThat(patch.getOperations().get(0).getOperation(), is(Operation.REMOVE));
    assertThat(patch.getOperations().get(0).getPath(), is(""));
  }

}
