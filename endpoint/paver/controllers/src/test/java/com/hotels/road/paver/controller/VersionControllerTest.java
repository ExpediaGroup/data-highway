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
package com.hotels.road.paver.controller;

import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;
import static com.hotels.road.paver.controller.VersionController.PNG_URL_FORMAT;
import static com.hotels.road.paver.controller.VersionController.SVG_URL_FORMAT;
import static com.hotels.road.paver.controller.VersionController.VERSION;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = { VersionController.class })
public class VersionControllerTest {
  @Autowired
  private VersionController versionController;

  private MockMvc mockMvc;

  @Before
  public void before() throws Exception {
    mockMvc = standaloneSetup(versionController).build();
  }

  @Test
  public void versionTxt() throws Exception {
    mockMvc
        .perform(get(CONTEXT_PATH + "/version.txt"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(TEXT_PLAIN))
        .andExpect(content().string(VERSION));
  }

  @Test
  public void versionSvg() throws Exception {
    mockMvc.perform(get(CONTEXT_PATH + "/version.svg")).andExpect(status().is3xxRedirection()).andExpect(
        redirectedUrl(String.format(SVG_URL_FORMAT, VERSION)));
  }

  @Test
  public void versionPng() throws Exception {
    mockMvc.perform(get(CONTEXT_PATH + "/version.png")).andExpect(status().is3xxRedirection()).andExpect(
        redirectedUrl(String.format(PNG_URL_FORMAT, VERSION)));
  }
}
