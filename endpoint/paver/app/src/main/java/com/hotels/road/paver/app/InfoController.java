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
package com.hotels.road.paver.app;

import static java.util.stream.Collectors.toList;

import java.security.Principal;
import java.util.Map;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import avro.shaded.com.google.common.collect.ImmutableMap;
import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class InfoController {
  @GetMapping("/paver/v1/userinfo")
  public Map<String, Object> home(Principal user) {
    UsernamePasswordAuthenticationToken token = (UsernamePasswordAuthenticationToken) user;
    return ImmutableMap
        .<String, Object> builder()
        .put("username", token.getName())
        .put("authorities", token.getAuthorities().stream().map(GrantedAuthority::getAuthority).collect(toList()))
        .build();
  }
}
