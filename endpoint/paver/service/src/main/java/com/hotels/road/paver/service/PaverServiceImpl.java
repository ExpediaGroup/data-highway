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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

import static com.google.common.base.Preconditions.checkArgument;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hotels.road.exception.InvalidKeyPathException;
import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.exception.ServiceException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.Destination;
import com.hotels.road.model.core.HiveDestination;
import com.hotels.road.model.core.KafkaStatus;
import com.hotels.road.model.core.MessageStatus;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.partition.KeyPathParser;
import com.hotels.road.partition.KeyPathValidator;
import com.hotels.road.paver.api.RoadAdminClient;
import com.hotels.road.paver.api.SchemaStoreClient;
import com.hotels.road.paver.service.exception.NoSuchSchemaException;
import com.hotels.road.paver.service.patchmapping.EnabledPatchMapping;
import com.hotels.road.paver.service.patchmapping.PatchMapping;
import com.hotels.road.rest.model.Authorisation;
import com.hotels.road.rest.model.Authorisation.Offramp;
import com.hotels.road.rest.model.Authorisation.Onramp;
import com.hotels.road.rest.model.BasicRoadModel;
import com.hotels.road.rest.model.RoadModel;
import com.hotels.road.rest.model.RoadType;
import com.hotels.road.rest.model.Sensitivity;
import com.hotels.road.rest.model.validator.RoadNameValidator;
import com.hotels.road.security.CidrBlockValidator;
import com.hotels.road.tollbooth.client.api.PatchOperation;
import com.hotels.road.tollbooth.client.api.PatchSet;

@Component
public class PaverServiceImpl implements PaverService {
  private final RoadAdminClient roadAdminClient;
  private final SchemaStoreClient schemaStoreClient;
  private final CidrBlockValidator cidrBlockBValidator;
  private final Map<String, PatchMapping> patchMappings;
  private final RoadSchemaNotificationHandler roadSchemaNotificationHandler;
  private final boolean autoCreateHiveDestination;
  private final Clock clock;

  @Autowired
  public PaverServiceImpl(
      RoadAdminClient roadAdminClient,
      SchemaStoreClient schemaStoreClient,
      CidrBlockValidator cidrBlockBValidator,
      List<PatchMapping> mappings,
      RoadSchemaNotificationHandler roadSchemaNotificationHandler,
      @Value("${destinations.hive.autoCreate:true}") boolean autoCreateHiveDestination,
      @Value("#{clock}") Clock clock) {
    this.roadAdminClient = roadAdminClient;
    this.schemaStoreClient = schemaStoreClient;
    this.cidrBlockBValidator = cidrBlockBValidator;
    this.roadSchemaNotificationHandler = roadSchemaNotificationHandler;
    this.autoCreateHiveDestination = autoCreateHiveDestination;
    this.clock = clock;
    patchMappings = mappings.stream().collect(Collectors.toMap(PatchMapping::getPath, m -> m));
  }

  @Override
  public SortedSet<String> getRoadNames() {
    return roadAdminClient.listRoads();
  }

  @Override
  public void createRoad(BasicRoadModel basicModel) {
    Road road = new Road();
    road.setName(basicModel.getName());
    road.setType(RoadType.NORMAL);
    road.setDescription(basicModel.getDescription());
    road.setTeamName(basicModel.getTeamName());
    road.setContactEmail(basicModel.getContactEmail());
    road.setEnabled(basicModel.isEnabled());
    road.setEnabledTimeStamp(clock.instant().toEpochMilli());
    road.setPartitionPath(basicModel.getPartitionPath());
    road.setAuthorisation(getAuthorisation(basicModel));
    road.setDeleted(false);
    road.setMetadata(basicModel.getMetadata());
    road.setCompatibilityMode(Road.DEFAULT_COMPATIBILITY_MODE);

    if (autoCreateHiveDestination) {
      HiveDestination hive = new HiveDestination();
      hive.setEnabled(true);
      hive.setLandingInterval(HiveDestination.DEFAULT_LANDING_INTERVAL);
      road.setDestinations(singletonMap("hive", hive));
    }

    roadAdminClient.createRoad(road);
    roadSchemaNotificationHandler.handleRoadCreated(road);
  }

  private Authorisation getAuthorisation(BasicRoadModel basicModel) {
    Authorisation authorisation = Optional.ofNullable(basicModel.getAuthorisation()).orElseGet(Authorisation::new);

    Onramp onramp = Optional.ofNullable(authorisation.getOnramp()).orElseGet(Onramp::new);
    List<String> cidrBlocks = Optional.ofNullable(onramp.getCidrBlocks()).orElseGet(Collections::emptyList);
    cidrBlockBValidator.validate(cidrBlocks);
    authorisation.setOnramp(onramp);
    onramp.setCidrBlocks(cidrBlocks);
    List<String> onrampAuthorities = Optional.ofNullable(onramp.getAuthorities()).orElseGet(Collections::emptyList);
    onramp.setAuthorities(onrampAuthorities);

    Offramp offramp = Optional.ofNullable(authorisation.getOfframp()).orElseGet(Offramp::new);
    Map<String, Set<Sensitivity>> offrampAuthorities = Optional.ofNullable(offramp.getAuthorities()).orElseGet(
        Collections::emptyMap);
    offrampAuthorities.forEach((name, grants) -> {
      if (grants == null || grants.isEmpty()) {
        throw new IllegalArgumentException("Grants may not be null or empty");
      }
    });
    offramp.setAuthorities(offrampAuthorities);
    authorisation.setOfframp(offramp);

    return authorisation;
  }

  @Override
  public RoadModel getRoad(String name) throws UnknownRoadException {
    return convertRoad(getRoadOrThrow(name));
  }

  @Override
  public void applyPatch(String name, List<PatchOperation> modelOperations) throws UnknownRoadException {
    Road road = getRoadOrThrow(name);
    List<PatchOperation> operations = new ArrayList<>();
    for (PatchOperation modelOperation : modelOperations) {
      PatchMapping mapping = patchMappings.get(modelOperation.getPath());
      checkArgument(mapping != null, "\"%s\" is not a valid path to patch", modelOperation.getPath());
      operations.add(mapping.convertOperation(road, modelOperation));
      if(mapping instanceof EnabledPatchMapping) {
        operations.add(PatchOperation.add("/enabledTimeStamp", clock.instant().toEpochMilli()));
      }
    }
    roadAdminClient.updateRoad(new PatchSet(road.getName(), operations));
  }

  @Override
  public Map<Integer, Schema> getActiveSchemas(String name) throws UnknownRoadException {
    getRoadOrThrow(name);
    return schemaStoreClient.getActiveSchemaVersions(name).entrySet().stream().map(Entry::getValue).collect(
        Collectors.toMap(SchemaVersion::getVersion, SchemaVersion::getSchema));
  }

  @Override
  public SchemaVersion getActiveSchema(String name, int version) throws NoSuchSchemaException, UnknownRoadException {
    getRoadOrThrow(name);
    return schemaStoreClient.getActiveSchema(name, version).orElseThrow(() -> new NoSuchSchemaException(name, version));
  }

  @Override
  public SchemaVersion getLatestActiveSchema(String name) throws NoSuchSchemaException, UnknownRoadException {
    getRoadOrThrow(name);
    return schemaStoreClient.getLatestActiveSchema(name).orElseThrow(() -> new NoSuchSchemaException(name, 1));
  }

  @Override
  public SchemaVersion addSchema(String name, Schema schema) throws UnknownRoadException, InvalidKeyPathException {
    return addSchema(name, schema, finalSchema -> {
      SchemaVersion schemaVersion = schemaStoreClient.registerSchema(name, finalSchema);
      return new SchemaVersion(schema, schemaVersion.getVersion(), false);
    });
  }

  @Override
  public SchemaVersion addSchema(String name, Schema schema, int version)
    throws UnknownRoadException, InvalidKeyPathException, InvalidSchemaVersionException {
    return addSchema(name, schema, finalSchema -> {
      schemaStoreClient.registerSchema(name, finalSchema, version);
      return new SchemaVersion(schema, version, false);
    });
  }

  private SchemaVersion addSchema(String name, Schema schema, CheckedFunction<Schema, SchemaVersion> function)
    throws UnknownRoadException, InvalidKeyPathException, InvalidSchemaVersionException {
    Road road = getRoadOrThrow(name);
    if (StringUtils.isNotBlank(road.getPartitionPath())) {
      new KeyPathValidator(KeyPathParser.parse(road.getPartitionPath()), schema).validate();
    }

    SchemaVersion schemaVersion = function.apply(schema);
    roadSchemaNotificationHandler.handleSchemaCreated(name, schemaVersion.getVersion());
    return schemaVersion;
  }

  @FunctionalInterface
  private static interface CheckedFunction<F, T> {
    T apply(F f) throws UnknownRoadException, ServiceException;
  }

  private Road getRoadOrThrow(String name) throws UnknownRoadException {
    return roadAdminClient.getRoad(RoadNameValidator.validateRoadName(name)).orElseThrow(
        () -> new UnknownRoadException(name));
  }

  private RoadModel convertRoad(Road road) {
    Map<String, String> agentMessages = new HashMap<>();
    Optional.ofNullable(road.getStatus()).map(KafkaStatus::getMessage).ifPresent(
        msg -> agentMessages.put("kafka", msg));
    return new RoadModel(
        road.getName(),
        road.getType(),
        road.getDescription(),
        road.getTeamName(),
        road.getContactEmail(),
        road.isEnabled(),
        road.getPartitionPath(),
        road.getAuthorisation(),
        road.getMetadata(),
        road.getStatus() != null && road.getStatus().isTopicCreated(),
        road.getCompatibilityMode(),
        agentMessages);
  }

  @Override
  public void deleteSchemaVersion(String name, int version) throws UnknownRoadException {
    schemaStoreClient.deleteSchemaVersion(name, version);
    roadSchemaNotificationHandler.handleSchemaDeleted(name, version);
  }

  private boolean zeroActiveDestination(Map<String, Destination> destinations) {
    return destinations == null || destinations.isEmpty();
  }

  private boolean hasNoMessages(Road road) {
    if(!road.isEnabled()) {
      MessageStatus messageStatus = road.getMessageStatus();
      if(messageStatus != null && messageStatus.getNumberOfMessages() == 0
        && road.getEnabledTimeStamp() < messageStatus.getLastUpdated()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void deleteRoad(String name) throws UnknownRoadException {
    Road road = getRoadOrThrow(name);
    if(zeroActiveDestination(road.getDestinations()) && hasNoMessages(road)) {
      roadAdminClient.updateRoad(new PatchSet(road.getName(), singletonList(PatchOperation.add("/deleted",true))));
    } else {
      throw new IllegalArgumentException("Road " + name + " can't be deleted. Make sure there are no active destinations, "
      + "the road is disabled and there are no messages for this road");
    }
  }
}
