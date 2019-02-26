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
package com.hotels.road.paver.tollbooth;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import com.google.common.collect.ImmutableMap;

import com.hotels.road.exception.InvalidSchemaException;
import com.hotels.road.exception.InvalidSchemaVersionException;
import com.hotels.road.exception.UnknownRoadException;
import com.hotels.road.model.core.KafkaStatus;
import com.hotels.road.model.core.Road;
import com.hotels.road.model.core.SchemaVersion;
import com.hotels.road.schema.chronology.SchemaCompatibility;
import com.hotels.road.schema.gdpr.InvalidPiiAnnotationException;
import com.hotels.road.schema.serde.SchemaSerializationModule;
import com.hotels.road.tollbooth.client.api.PatchSet;
import com.hotels.road.tollbooth.client.spi.PatchSetEmitter;

public class TollboothSchemaStoreClientTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private Map<Integer, SchemaVersion> schemaVersionsMap;

  private TollboothSchemaStoreClient client;
  private Map<String, Road> store;

  private Road road1;
  private KafkaStatus status;
  private Schema schema1;
  private Schema schema2;
  private Schema schema3;
  private Schema schema4;
  private SchemaVersion schemaVersion1;
  private SchemaVersion schemaVersion2;
  private SchemaVersion schemaVersion3;

  @Before
  public void before() {
    mapper.registerModule(new SchemaSerializationModule());

    road1 = new Road();
    road1.setName("road1");
    road1.setTopicName("road1");
    road1.setDescription("description");
    road1.setContactEmail("contactEmail");
    road1.setEnabled(false);
    status = new KafkaStatus();
    status.setTopicCreated(false);
    road1.setStatus(status);
    road1.setSchemas(Collections.emptyMap());
    road1.setDeleted(false);

    schema1 = SchemaBuilder.builder().record("a").fields().name("v").type().booleanType().noDefault().endRecord();
    schema2 = SchemaBuilder
        .builder()
        .record("a")
        .fields()
        .name("v")
        .type()
        .booleanType()
        .booleanDefault(false)
        .endRecord();
    schema3 = SchemaBuilder
        .builder()
        .record("a")
        .fields()
        .name("v")
        .type()
        .booleanType()
        .booleanDefault(false)
        .optionalString("u")
        .endRecord();
    schema4 = SchemaBuilder
        .builder()
        .record("a")
        .fields()
        .name("v")
        .type()
        .booleanType()
        .booleanDefault(false)
        .requiredString("u")
        .endRecord();

    schemaVersion1 = new SchemaVersion(schema1, 1, false);
    schemaVersion2 = new SchemaVersion(schema2, 2, false);
    schemaVersion3 = new SchemaVersion(schema3, 3, false);

    schemaVersionsMap = ImmutableMap.of(schemaVersion1.getVersion(), schemaVersion1, schemaVersion2.getVersion(),
        schemaVersion2, schemaVersion3.getVersion(), schemaVersion3);

    PatchSetEmitter patchSetEmitter = new PatchSetEmitter() {

      @Override
      public void emit(PatchSet patchSet) {
        try {
          JsonNode roadJson = Optional
              .ofNullable(store.get(patchSet.getDocumentId()))
              .map(r -> mapper.convertValue(r, JsonNode.class))
              .orElse(NullNode.instance);
          JsonNode patchJson = mapper.convertValue(patchSet.getOperations(), JsonNode.class);
          JsonPatch jsonPatch = JsonPatch.fromJson(patchJson);
          JsonNode newRoadJson = jsonPatch.apply(roadJson);
          Road nnewRoad = mapper.convertValue(newRoadJson, Road.class);
          store.put(patchSet.getDocumentId(), nnewRoad);
        } catch (IOException | JsonPatchException e) {
          throw new RuntimeException(e);
        }
      }
    };

    store = new HashMap<>();
    store.put("road1", road1);

    client = new TollboothSchemaStoreClient(Collections.unmodifiableMap(store), patchSetEmitter);
  }

  @Test
  public void getAllSchemaVersions() throws Exception {
    Map<Integer, SchemaVersion> schemas;
    schemas = client.getAllSchemaVersions("road1");
    assertTrue(schemas.isEmpty());

    road1.setSchemas(ImmutableMap.of(schemaVersion1.getVersion(), schemaVersion1));

    schemas = client.getAllSchemaVersions("road1");
    assertThat(schemas.size(), is(1));
    assertTrue(schemas.values().contains(schemaVersion1));
  }

  @Test(expected = UnknownRoadException.class)
  public void getAllSchemaVersions_fails_when_road_isDeleted() throws Exception {
    road1.setDeleted(true);
    client.getAllSchemaVersions("road1");
  }

  @Test
  public void getLatestActiveSchema() throws Exception {
    road1.setSchemas(schemaVersionsMap);

    Optional<SchemaVersion> schema = client.getLatestActiveSchema("road1");
    assertThat(schema.get().getSchema(), is(schema3));
  }

  @Test(expected = UnknownRoadException.class)
  public void getLatestActiveSchema_fails_when_road_isDeleted() throws Exception {
    road1.setDeleted(true);
    client.getLatestActiveSchema("road1");
  }

  @Test
  public void getLatestSchema_none() throws Exception {
    road1.setSchemas(Collections.emptyMap());

    Optional<SchemaVersion> schema = client.getLatestActiveSchema("road1");
    assertThat(schema.isPresent(), is(false));
  }

  @Test
  public void getSchema() throws Exception {
    road1.setSchemas(schemaVersionsMap);

    Optional<SchemaVersion> schemaV1 = client.getSchema("road1", 1);
    Optional<SchemaVersion> schemaV2 = client.getSchema("road1", 2);
    Optional<SchemaVersion> schemaV3 = client.getSchema("road1", 3);
    Optional<SchemaVersion> schemaV4 = client.getSchema("road1", 4);
    assertThat(schemaV1.get().getSchema(), is(schema1));
    assertThat(schemaV2.get().getSchema(), is(schema2));
    assertThat(schemaV3.get().getSchema(), is(schema3));
    assertThat(schemaV4.isPresent(), is(false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void getSchema_invalidVersion() throws Exception {
    client.getSchema("road1", 0);
  }

  @Test
  public void registerSchema_one() throws Exception {
    SchemaVersion registerSchema = client.registerSchema("road1", schema1);

    assertThat(registerSchema, is(schemaVersion1));

    Map<Integer, SchemaVersion> schemas = store.get("road1").getSchemas();
    assertThat(schemas.size(), is(1));
    assertThat(schemas.values().contains(schemaVersion1), is(true));
  }

  @Test
  public void registerSchema_many() throws Exception {
    client.registerSchema("road1", schema1);
    client.registerSchema("road1", schema2);
    client.registerSchema("road1", schema3);

    Map<Integer, SchemaVersion> schemas = store.get("road1").getSchemas();
    assertThat(schemas.size(), is(3));
    assertThat(schemas.get(1), is(schemaVersion1));
    assertThat(schemas.get(2), is(schemaVersion2));
    assertThat(schemas.get(3), is(schemaVersion3));
  }

  @Test(expected = IllegalArgumentException.class)
  public void registerSchema_invalidCompatibilityMode() throws Exception {
    road1.setCompatibilityMode("ThisIsNotValid");
    client.registerSchema("road1", schema1);
  }

  @Test
  public void registerSchema_defaultCompatibilityModeWouldFail() throws Exception {
    road1.setCompatibilityMode(SchemaCompatibility.CAN_BE_READ_BY_LATEST.name());
    client.registerSchema("road1", schema3);
    client.registerSchema("road1", schema4);

    Map<Integer, SchemaVersion> schemas = store.get("road1").getSchemas();
    assertThat(schemas.size(), is(2));
  }

  @Test
  public void registerSchema_specifyVersion() throws Exception {
    SchemaVersion registerSchema = client.registerSchema("road1", schema2, 2);

    assertThat(registerSchema, is(schemaVersion2));

    Map<Integer, SchemaVersion> schemas = store.get("road1").getSchemas();
    assertThat(schemas.size(), is(1));
    assertThat(schemas.get(2), is(schemaVersion2));
  }

  @Test(expected = InvalidSchemaVersionException.class)
  public void registerSchema_specifyVersion_InvalidSchemaVersionException() throws Exception {
    road1.setSchemas(Collections.singletonMap(1, schemaVersion1));
    client.registerSchema("road1", schema1, 1);
  }

  @Test
  public void registerDeleteRegisterSchema() throws Exception {
    client.registerSchema("road1", schema1);
    client.registerSchema("road1", schema2);
    client.deleteSchemaVersion("road1", 2);
    client.registerSchema("road1", schema3);
    // New Schema version = latest schema version (marked as deleted or not) + 1.
    assertThat(client.getLatestActiveSchema("road1").get().getVersion(), is(3));
  }

  @Test(expected = InvalidSchemaException.class)
  public void registerSchema_incompatible() throws Exception {
    client.registerSchema("road1", schema1);
    client.registerSchema("road1", schema2);
    client.registerSchema("road1", schema3);
    client.registerSchema("road1", schema4);
  }

  @Test(expected = InvalidSchemaException.class)
  public void registerSchema_notARecord() throws Exception {
    client.registerSchema("road1", SchemaBuilder.builder().booleanType());
  }

  @Test(expected = UnknownRoadException.class)
  public void registerSchema_fails_when_road_isDeleted() throws Exception {
    road1.setDeleted(true);
    client.registerSchema("road1", schema1);
  }

  @Test(expected = UnknownRoadException.class)
  public void deleteSchemaForRoadThatDoesNotExist() throws Exception {
    client.deleteSchemaVersion("not-exist", 1);
  }

  @Test
  public void deleteSchema() throws Exception {
    client.registerSchema("road1", schema1);
    client.deleteSchemaVersion("road1", 1);
    assertTrue(client.getAllSchemaVersions("road1").get(1).isDeleted());
  }

  @Test(expected = UnknownRoadException.class)
  public void deleteSchema_fails_when_road_isDeleted() throws Exception {
    road1.setDeleted(true);
    client.deleteSchemaVersion("road1", 1);
  }

  @Test
  public void registerAndDeleteMultipleSchemas() throws Exception {
    client.registerSchema("road1", schema1);
    assertThat(client.getLatestActiveSchema("road1").get().getVersion(), is(1));
    client.registerSchema("road1", schema2);
    assertThat(client.getLatestActiveSchema("road1").get().getVersion(), is(2));
    client.registerSchema("road1", schema3);
    assertThat(client.getLatestActiveSchema("road1").get().getVersion(), is(3));
    client.deleteSchemaVersion("road1", 3);
    assertThat(client.getLatestActiveSchema("road1").get().getVersion(), is(2));
    client.deleteSchemaVersion("road1", 2);
    assertThat(client.getLatestActiveSchema("road1").get().getVersion(), is(1));
    client.deleteSchemaVersion("road1", 1);
    assertThat(client.getLatestActiveSchema("road1"), is(Optional.empty()));
  }

  @Test(expected = InvalidPiiAnnotationException.class)
  public void addPiiToExistingFieldFails() throws Exception {
    Schema newSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .prop("sensitivity", "PII")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();

    Schema currentSchema = SchemaBuilder
        .record("r")
        .fields()
        .name("f")
        .type(SchemaBuilder.builder().stringType())
        .noDefault()
        .endRecord();
    client.registerSchema("road1", currentSchema);
    client.registerSchema("road1", newSchema);
  }
}
