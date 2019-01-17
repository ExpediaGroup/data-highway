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
package com.hotels.road.tool.cli;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;


import com.hotels.road.offramp.model.DefaultOffset;


public class OfframpConsoleTest {
    private OfframpConsole oc;

    @Before
    public void before() throws Exception {
        oc = new OfframpConsole();
    }

    @Test
    public void test_classDefaults() throws Exception {
        assertThat(oc.host, nullValue());
        assertThat(oc.username, nullValue());
        assertThat(oc.password, nullValue());
        assertThat(oc.roadName, nullValue());
        assertThat(oc.streamName, nullValue());
        assertThat(oc.defaultOffset, is(DefaultOffset.LATEST));
        assertThat(oc.initialRequestAmount, is(200));
        assertThat(oc.replenishingRequestAmount, is(120));
        assertThat(oc.commitIntervalMs, is(500L));
        assertThat(oc.numToConsume, is(Long.MAX_VALUE));
        assertThat(oc.flipOutput, is(false));
        assertThat(oc.tlsTrustAll, is(false));
        assertThat(oc.debug, is(false));
    }
}
