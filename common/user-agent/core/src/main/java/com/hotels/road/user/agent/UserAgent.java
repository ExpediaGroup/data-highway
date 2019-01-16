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
package com.hotels.road.user.agent;

import static java.util.stream.Collectors.joining;

import static lombok.AccessLevel.PRIVATE;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents a UserAgent.
 *
 * <pre>
 * User-Agent      = "User-Agent" ":" 1*( product | comment )
 * </pre>
 *
 * Where {@code product} is defined as:
 *
 * <pre>
 * product         = token ["/" product-version]
 * product-version = token
 * token           = 1*&lt;any CHAR except CTLs or separators&gt;
 * </pre>
 *
 * And {@code comment} as:
 *
 * <pre>
 * comment         = "(" *( ctext | quoted-pair | comment ) ")"
 * ctext           = &lt;any TEXT excluding "(" and ")"&gt;
 * </pre>
 *
 * @see <a href="https://www.ietf.org/rfc/rfc2616.txt">https://www.ietf.org/rfc/rfc2616.txt</a>
 */
@RequiredArgsConstructor(access = PRIVATE)
public class UserAgent {
  private static final String PRODUCT_NAME = "product";
  private static final String VERSION_NAME = "version";
  private static final String COMMENT_NAME = "comment";
  private static final String PRODUCT = "(?<" + PRODUCT_NAME + ">[A-Za-z0-9\\-]+)";
  private static final String VERSION = "(?<" + VERSION_NAME + ">[0-9\\.]+)";
  private static final String COMMENT = "(?<" + COMMENT_NAME + ">[^\\)]+)";
  private static final String USERAGENT = PRODUCT + "/" + VERSION + "(?: \\(" + COMMENT + "\\))? ?";
  private static final Pattern userAgentPattern = Pattern.compile(USERAGENT);

  private final @NonNull List<Token> tokens;

  public UserAgent add(@NonNull String product, @NonNull String version) {
    return add(product, version, null);
  }

  public UserAgent add(@NonNull String product, @NonNull String version, String comment) {
    return add(new Token(product, version, Optional.ofNullable(comment)));
  }

  public UserAgent add(@NonNull Token token) {
    tokens.add(token);
    return this;
  }

  public Optional<Token> token(@NonNull String product) {
    for (Token token : tokens) {
      if (token.getProduct().equals(product)) {
        return Optional.of(token);
      }
    }
    return Optional.empty();
  }

  @Override
  public String toString() {
    return tokens.stream().map(Token::toString).collect(joining(" "));
  }

  public static UserAgent create() {
    return new UserAgent(new ArrayList<>());
  }

  public static UserAgent parse(String rawUserAgent) {
    UserAgent userAgent = UserAgent.create();
    if (rawUserAgent != null) {
      Matcher matcher = userAgentPattern.matcher(rawUserAgent);
      while (matcher.find()) {
        String product = matcher.group(PRODUCT_NAME);
        String version = matcher.group(VERSION_NAME);
        String comment = matcher.group(COMMENT_NAME);
        userAgent.add(product, version, comment);
      }
    }
    return userAgent;
  }

  @Data
  public static class Token {
    private final @NonNull String product;
    private final @NonNull String version;
    private final @NonNull Optional<String> comment;

    @Override
    public String toString() {
      return product + "/" + version + comment.map(comment -> " (" + comment + ")").orElse("");
    }
  }
}
