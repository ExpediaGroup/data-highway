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
package com.hotels.road.offramp.client;

import static java.util.Collections.singleton;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import static com.hotels.road.offramp.model.DefaultOffset.EARLIEST;
import static com.hotels.road.rest.model.Sensitivity.PII;

import java.net.URI;

import org.junit.Test;

public class OfframpOptionsTest {
  @Test
  public void required() throws Exception {
    OfframpOptions<String> options = OfframpOptions
        .builder(String.class)
        .username("user")
        .password("pass")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    assertThat(options.uri(), is(
        URI.create("wss://host/offramp/v2/roads/roadName/streams/streamName/messages?defaultOffset=LATEST&grants=")));
    assertThat(options.isRetry(), is(true));
  }

  @Test
  public void optional() throws Exception {
    OfframpOptions<String> options = OfframpOptions
        .builder(String.class)
        .username("user")
        .password("pass")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .defaultOffset(EARLIEST)
        .grants(singleton(PII))
        .retry(false)
        .build();

    assertThat(options.uri(), is(URI
        .create("wss://host/offramp/v2/roads/roadName/streams/streamName/messages?defaultOffset=EARLIEST&grants=PII")));
    assertThat(options.isRetry(), is(false));
  }

  @Test(expected = NullPointerException.class)
  public void nullHost() throws Exception {
    OfframpOptions.builder(String.class).host(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullRoadName() throws Exception {
    OfframpOptions.builder(String.class).roadName(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullStreamName() throws Exception {
    OfframpOptions.builder(String.class).streamName(null);
  }

  @Test(expected = NullPointerException.class)
  public void nullDefaultOffset() throws Exception {
    OfframpOptions.builder(String.class).defaultOffset(null);
  }

  @Test
  public void equality() throws Exception {
    OfframpOptions<String> left = OfframpOptions
        .builder(String.class)
        .username("user")
        .password("pass")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    OfframpOptions<String> right = OfframpOptions
        .builder(String.class)
        .username("user")
        .password("pass")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    assertEquals(left, right);
  }

  @Test
  public void inequality() throws Exception {
    OfframpOptions<String> o1 = OfframpOptions
        .builder(String.class)
        .username("user")
        .password("pass")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    OfframpOptions<Byte> o2 = OfframpOptions
        .builder(Byte.class)
        .username("user")
        .password("pass")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    assertNotEquals(o1, o2);

    OfframpOptions<String> o3 = OfframpOptions.builder(String.class)
        .username("user1")
        .password("password1")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    OfframpOptions<String> o4 = OfframpOptions.builder(String.class)
        .username("user2")
        .password("password1")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    assertNotEquals(o3, o4);

    OfframpOptions<String> o5 = OfframpOptions.builder(String.class)
        .username("user1")
        .password("password1")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    OfframpOptions<String> o6 = OfframpOptions.builder(String.class)
        .username("user1")
        .password("password2")
        .host("host")
        .roadName("roadName")
        .streamName("streamName")
        .build();

    assertNotEquals(o5, o6);
  }
}
