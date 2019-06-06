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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.ldap.core.support.BaseLdapPathContextSource;
import org.springframework.ldap.core.support.LdapContextSource;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.ldap.authentication.BindAuthenticator;
import org.springframework.security.ldap.authentication.LdapAuthenticationProvider;
import org.springframework.security.ldap.search.FilterBasedLdapUserSearch;
import org.springframework.security.ldap.search.LdapUserSearch;
import org.springframework.security.ldap.userdetails.DefaultLdapAuthoritiesPopulator;
import org.springframework.security.ldap.userdetails.LdapAuthoritiesPopulator;
import org.springframework.session.hazelcast.config.annotation.web.http.EnableHazelcastHttpSession;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import avro.shaded.com.google.common.collect.ImmutableList;

@Profile("ldap")
@Configuration
@EnableGlobalMethodSecurity(prePostEnabled = true)
@EnableHazelcastHttpSession
public class LdapSecurityConfiguration {
  @Bean
  public BaseLdapPathContextSource contextSource(
      @Value("${ldap.url}") String ldapUrl,
      @Value("${ldap.bindDn}") String bindDn,
      @Value("${ldap.bindPassword}") String bindPassword) {
    LdapContextSource contextSource = new LdapContextSource();
    contextSource.setUrl(ldapUrl);
    contextSource.setUserDn(bindDn);
    contextSource.setPassword(bindPassword);
    contextSource.afterPropertiesSet();
    return contextSource;
  }

  @Bean
  public LdapUserSearch userSearch(
      @Value("${ldap.searchBase}") String searchBase,
      @Value("${ldap.searchFilter}") String searchFilter,
      BaseLdapPathContextSource contextSource) {
    return new FilterBasedLdapUserSearch(searchBase, searchFilter, contextSource);
  }

  @Bean
  public BindAuthenticator bindAuthenticator(BaseLdapPathContextSource contextSource, LdapUserSearch userSearch) {
    BindAuthenticator authenticator = new BindAuthenticator(contextSource);
    authenticator.setUserSearch(userSearch);
    return authenticator;
  }

  @Bean
  public LdapAuthoritiesPopulator authoritiesPopulator(
      BaseLdapPathContextSource contextSource,
      @Value("${ldap.group.searchBase}") String groupSearchBase,
      @Value("${ldap.group.searchSubtree}") boolean searchSubtree,
      @Value("${ldap.group.ignorePartialResultException}") boolean ignorePartialResultException,
      @Value("${ldap.group.searchFilter}") String groupSearchFilter) {
    DefaultLdapAuthoritiesPopulator authoritiesPopulator = new DefaultLdapAuthoritiesPopulator(
        contextSource,
        groupSearchBase);
    authoritiesPopulator.setSearchSubtree(searchSubtree);
    authoritiesPopulator.setIgnorePartialResultException(ignorePartialResultException);
    authoritiesPopulator.setGroupSearchFilter(groupSearchFilter);
    authoritiesPopulator.setRolePrefix("");
    authoritiesPopulator.setConvertToUpperCase(false);
    return authoritiesPopulator;
  }

  @Bean
  public LdapAuthenticationProvider authenticationProvider(
      BindAuthenticator bindAuthenticator,
      LdapAuthoritiesPopulator authoritiesPopulator) {
    return new LdapAuthenticationProvider(bindAuthenticator, authoritiesPopulator);
  }

  @Bean
  public WebMvcConfigurer webConfig() {
    return new WebMvcConfigurer() {
      @Override
      public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**").allowedMethods("HEAD", "GET", "PUT", "POST", "DELETE", "PATCH");
      }
    };
  }

  @Bean
  public CorsConfigurationSource corsConfigurationSource() {
    CorsConfiguration configuration = new CorsConfiguration().applyPermitDefaultValues();
    configuration.setAllowedOrigins(ImmutableList.of("*"));
    configuration.setAllowedMethods(ImmutableList.of("HEAD", "GET", "POST", "PUT", "DELETE", "PATCH"));
    // setAllowCredentials(true) is important, otherwise:
    // The value of the 'Access-Control-Allow-Origin' header in the response must not be the wildcard '*' when the
    // request's credentials mode is 'include'.
    configuration.setAllowCredentials(true);
    // setAllowedHeaders is important! Without it, OPTIONS preflight request
    // will fail with 403 Invalid CORS request
    configuration.setAllowedHeaders(ImmutableList.of("Authorization", "Cache-Control", "Content-Type"));

    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", configuration);
    return source;
  }

  @Bean
  public WebSecurityConfigurerAdapter webSecurityConfigurerAdapter(LdapAuthenticationProvider authenticationProvider) {
    return new RoadWebSecurityConfigurerAdapter() {
      @Override
      public void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider(authenticationProvider);
      }

    };
  }
}
