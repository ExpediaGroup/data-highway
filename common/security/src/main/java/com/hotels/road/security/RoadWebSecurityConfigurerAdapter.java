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
package com.hotels.road.security;

import static org.springframework.http.HttpMethod.GET;

import static com.hotels.road.rest.model.StandardResponse.failureResponse;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;

import com.fasterxml.jackson.databind.ObjectMapper;

public class RoadWebSecurityConfigurerAdapter extends WebSecurityConfigurerAdapter {
  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http
        .authorizeRequests()
          .antMatchers(GET, "/actuator/**").permitAll()
          .antMatchers("/paver/v1/swagger").permitAll()
          .antMatchers("/onramp/v1/swagger").permitAll()
          .mvcMatchers("/paver/v1/userinfo").authenticated()
          .antMatchers(GET, "/paver/v1/**").permitAll()
          .antMatchers("/paver/v1/**").permitAll()
          .antMatchers("/onramp/v1/**").permitAll()
          .antMatchers("/offramp/v2/**").permitAll()
          // .antMatchers("/paver/v1/**").authenticated()
          // .antMatchers("/onramp/v1/**").authenticated()
          // .antMatchers("/offramp/v2/**").authenticated()
          .antMatchers("/testdrive/v1/**").permitAll()
          .anyRequest().denyAll()
        .and()
          .httpBasic().realmName("Data Highway")
        .and()
          .exceptionHandling()
            .accessDeniedHandler(this::forbidden)
            .authenticationEntryPoint(this::unauthorised)
        .and()
          .csrf().disable()
          .cors();
  }

  private static final ObjectMapper mapper = new ObjectMapper();

  private void unauthorised(HttpServletRequest request, HttpServletResponse response, Exception exception)
    throws IOException {
    handleError(HttpStatus.UNAUTHORIZED, request, response, exception);
  }

  private void forbidden(HttpServletRequest request, HttpServletResponse response, Exception exception)
    throws IOException {
    handleError(HttpStatus.FORBIDDEN, request, response, exception);
  }

  private void handleError(
      HttpStatus status,
      HttpServletRequest request,
      HttpServletResponse response,
      Exception exception)
    throws IOException {
    response.setStatus(status.value());
    mapper.writeValue(response.getOutputStream(), failureResponse(exception.getMessage()));
  }
}
