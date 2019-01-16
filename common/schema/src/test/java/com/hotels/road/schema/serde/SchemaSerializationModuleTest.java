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
package com.hotels.road.schema.serde;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.avro.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.Module.SetupContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleSerializers;

@RunWith(MockitoJUnitRunner.class)
public class SchemaSerializationModuleTest {

  private final Module underTest = new SchemaSerializationModule();

  @Test
  public void moduleName() {
    String result = underTest.getModuleName();

    assertThat(result, is(SchemaSerializationModule.class.getSimpleName()));
  }

  @Test
  public void version() {
    Version result = underTest.version();

    assertThat(result, is(Version.unknownVersion()));
  }

  @Test
  public void setupModule() throws JsonMappingException {
    SetupContext context = mock(SetupContext.class);

    underTest.setupModule(context);

    ArgumentCaptor<SimpleSerializers> serializersCaptor = ArgumentCaptor.forClass(SimpleSerializers.class);
    ArgumentCaptor<SimpleDeserializers> deserializersCaptor = ArgumentCaptor.forClass(SimpleDeserializers.class);

    verify(context).addSerializers(serializersCaptor.capture());
    verify(context).addDeserializers(deserializersCaptor.capture());

    JavaType javaType = new ObjectMapper().constructType(Schema.class);

    JsonSerializer<?> serializer = serializersCaptor.getValue().findSerializer(null, javaType, null);
    assertThat(serializer, is(instanceOf(SchemaSerializer.class)));

    JsonDeserializer<?> deserializer = deserializersCaptor.getValue().findBeanDeserializer(javaType, null, null);
    assertThat(deserializer, is(instanceOf(SchemaDeserializer.class)));
  }

}
