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
package com.hotels.road.client;

import static com.hotels.road.client.RetryHandler.retryNTimesBackingOffExponentially;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hotels.road.tls.TLSConfig;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;

public class OnrampOptionsTest {
  @Test
  public void requiredAndDefaultValues() throws Exception {
    OnrampOptions options = aWorkingOption().build();

    //Check required values
    assertThat(options.getHost(), is("host"));
    assertThat(options.getRoadName(), is("roadName"));

    //Check default values
    assertThat(options.getThreads(), is(Runtime.getRuntime().availableProcessors()));
    assertThat(options.getObjectMapper(), is(notNullValue()));
    assertThat(options.getTlsConfigFactory(), is(nullValue()));

    assertThat(options.getRetryBehaviour(), is(instanceOf(ExponentialBackoffRetryHandler.class)));
    ExponentialBackoffRetryHandler retryBehaviour = (ExponentialBackoffRetryHandler) options.getRetryBehaviour();
    assertThat(retryBehaviour.getMaxRetries(), is(1));
    assertThat(retryBehaviour.getBaseBackOffDuration(), is(ZERO));
    assertThat(retryBehaviour.getMaxBackOffDuration(), is(ZERO));
  }

  @Test
  public void optional() throws Exception {
    ObjectMapper objectMapper = mock(ObjectMapper.class);
    TLSConfig.Factory tlsConfigFactory = TLSConfig.trustAllFactory();
    OnrampOptions options = aWorkingOption()
        .username("user")
        .password("pass")
        .threads(7)
        .retryBehaviour(retryNTimesBackingOffExponentially(5, ofMillis(19), ofMillis(2018)))
        .objectMapper(objectMapper)
        .tlsConfigFactory(tlsConfigFactory)
        .build();

    assertThat(options.getUsername(), is("user"));
    assertThat(options.getPassword(), is("pass"));
    assertThat(options.getThreads(), is(7));
    assertThat(options.getObjectMapper(), is(objectMapper));
    assertThat(options.getTlsConfigFactory(), is(tlsConfigFactory));

    assertThat(options.getRetryBehaviour(), is(instanceOf(ExponentialBackoffRetryHandler.class)));
    ExponentialBackoffRetryHandler retryBehaviour = (ExponentialBackoffRetryHandler) options.getRetryBehaviour();
    assertThat(retryBehaviour.getMaxRetries(), is(5));
    assertThat(retryBehaviour.getBaseBackOffDuration(), is(ofMillis(19)));
    assertThat(retryBehaviour.getMaxBackOffDuration(), is(ofMillis(2018)));
  }


  @Test(expected = NullPointerException.class)
  public void nullHost() throws Exception  {
    aWorkingOption().host(null).build();
  }

  @Test(expected = NullPointerException.class)
  public void nullRoadName() throws Exception  {
    aWorkingOption().roadName(null).build();
  }

  @Test(expected = NullPointerException.class)
  public void nullRetryBehaviour() throws Exception  {
    aWorkingOption().retryBehaviour(null).build();
  }

  @Test
  public void testSerializability() throws Exception  {
    ObjectMapper objectMapper = new ObjectMapper();
    TLSConfig.Factory tlsConfigFactory = TLSConfig.trustAllFactory();

    OnrampOptions fullyLoaded = aWorkingOption()
        .threads(17)
        .retryBehaviour(retryNTimesBackingOffExponentially(37, ofMillis(18), ofMillis(2020)))
        .objectMapper(objectMapper)
        .tlsConfigFactory(tlsConfigFactory)
        .build();

    OnrampOptions deserialized = SerializationUtils.roundtrip(fullyLoaded);

    assertThat(deserialized.getThreads(), is(17));
    assertThat(deserialized.getObjectMapper(), isA(ObjectMapper.class));
    assertThat(deserialized.getTlsConfigFactory(), is(instanceOf(TLSConfig.TrustAllFactory.class)));

    assertThat(deserialized.getRetryBehaviour(), is(instanceOf(ExponentialBackoffRetryHandler.class)));
    ExponentialBackoffRetryHandler retryBehaviour = (ExponentialBackoffRetryHandler) deserialized.getRetryBehaviour();
    assertThat(retryBehaviour.getMaxRetries(), is(37));
    assertThat(retryBehaviour.getBaseBackOffDuration(), is(ofMillis(18)));
    assertThat(retryBehaviour.getMaxBackOffDuration(), is(ofMillis(2020)));
  }

  private OnrampOptions.Builder aWorkingOption() {
    return OnrampOptions.builder()
        .host("host")
        .roadName("roadName");
  }
}
