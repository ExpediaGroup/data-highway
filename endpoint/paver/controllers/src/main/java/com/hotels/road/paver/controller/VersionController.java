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
package com.hotels.road.paver.controller;

import static com.hotels.road.maven.version.MavenVersion.version;
import static com.hotels.road.paver.controller.PaverConstants.CONTEXT_PATH;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

@RestController
@RequestMapping(CONTEXT_PATH)
public class VersionController {
  static final String VERSION = version(VersionController.class, "com.hotels.road", "road-paver-controllers");
  static final String SVG_URL_FORMAT = "https://img.shields.io/badge/Version-%s-blue.svg";
  static final String PNG_URL_FORMAT = "https://img.shields.io/badge/Version-%s-blue.png";

  @GetMapping(path = "version.txt")
  public String versionTxt() {
    return VERSION;
  }

  @GetMapping(path = "version.svg")
  public RedirectView versionSvg() {
    return new RedirectView(String.format(SVG_URL_FORMAT, VERSION));
  }

  @GetMapping(path = "version.png")
  public RedirectView versionPng() {
    return new RedirectView(String.format(PNG_URL_FORMAT, VERSION));
  }
}
