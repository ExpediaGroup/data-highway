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
package com.hotels.road.partition;

import static com.google.common.base.CharMatcher.inRange;
import static com.google.common.base.CharMatcher.is;
import static com.google.common.base.CharMatcher.none;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import com.google.common.base.CharMatcher;

public class KeyPathParser {

  public static Path parse(String path) {
    path = path == null ? "" : path.trim();
    if (path.length() < 1) {
      throw new IllegalArgumentException("Empty or null input");
    }
    List<Element> elements = new ArrayList<>();
    StringBuilder value = new StringBuilder();
    State state = State.START;
    char[] chars = path.toCharArray();
    int i;
    for (i = 0; i < chars.length; i++) {
      state = state.next(chars[i], i, elements, value);
    }
    state = state.next((char) 0, i, elements, value);
    if (state != State.EOF) {
      throw new ParsingException("Unexpected end of file");
    }
    return new Path(Collections.unmodifiableList(elements));
  }

  private static final CharMatcher FIRST_VALUE_CHAR_MATCHER = inRange('a', 'z')
      .or(inRange('A', 'Z'))
      .or(inRange('0', '9'))
      .or(is('_'));
  private static final CharMatcher VALUE_CHAR_MATCHER = FIRST_VALUE_CHAR_MATCHER.or(is('-'));

  enum State {
    START(none()),
    ROOT(is('$')),
    OPEN_PAREN(is('[')),
    CLOSE_PAREN(is(']')),
    OPEN_QUOTE(is('"')),
    CLOSE_QUOTE(is('"')),
    DOT(is('.')),
    FIRST_UNQUOTED_VALUE(FIRST_VALUE_CHAR_MATCHER),
    UNQUOTED_VALUE(VALUE_CHAR_MATCHER),
    FIRST_QUOTED_VALUE(VALUE_CHAR_MATCHER.or(is('.'))),
    QUOTED_VALUE(VALUE_CHAR_MATCHER.or(is('.'))),
    END_VALUE(none()),
    EOF(is((char) 0));

    private final CharMatcher charMatcher;
    private NextState nextState;

    private State(CharMatcher charMatcher) {
      this.charMatcher = charMatcher;
    }

    private boolean matches(char c) {
      return charMatcher.matches(c);
    }

    private State next(char c, int i, List<Element> e, StringBuilder v) {
      return nextState.next(c, i, e, v);
    }

    static {
      START.nextState = (c, i, e, v) -> {
        if (ROOT.matches(c)) {
          flushRoot(e, v);
          return END_VALUE;
        }
        throw new ParsingException("'$'", i, c);
      };
      END_VALUE.nextState = (c, i, e, v) -> {
        if (DOT.matches(c)) {
          return DOT;
        } else if (OPEN_PAREN.matches(c)) {
          return OPEN_PAREN;
        } else if (EOF.matches(c)) {
          return EOF;
        }
        throw new ParsingException("'.' or '['", i, c);
      };
      OPEN_PAREN.nextState = (c, i, e, v) -> {
        if (OPEN_QUOTE.matches(c)) {
          return OPEN_QUOTE;
        }
        throw new ParsingException("'\"'", i, c);
      };
      OPEN_QUOTE.nextState = (c, i, e, v) -> {
        if (FIRST_QUOTED_VALUE.matches(c)) {
          v.append(c);
          return FIRST_QUOTED_VALUE;
        }
        throw new ParsingException("[a-zA-Z0-9_-] or '.'", i, c);
      };
      FIRST_QUOTED_VALUE.nextState = (c, i, e, v) -> {
        if (QUOTED_VALUE.matches(c)) {
          v.append(c);
          return QUOTED_VALUE;
        } else if (CLOSE_QUOTE.matches(c)) {
          flushQuoted(e, v);
          return CLOSE_QUOTE;
        }
        throw new ParsingException("[a-zA-Z0-9_-], '.' or '\"'", i, c);
      };
      QUOTED_VALUE.nextState = FIRST_QUOTED_VALUE.nextState;
      CLOSE_QUOTE.nextState = (c, i, e, v) -> {
        if (CLOSE_PAREN.matches(c)) {
          return END_VALUE;
        }
        throw new ParsingException("[a-zA-Z0-9_-], '.' or '\"'", i, c);
      };
      DOT.nextState = (c, i, e, v) -> {
        if (FIRST_UNQUOTED_VALUE.matches(c)) {
          v.append(c);
          return UNQUOTED_VALUE;
        }
        throw new ParsingException("[a-zA-Z0-9_]", i, c);
      };
      UNQUOTED_VALUE.nextState = (c, i, e, v) -> {
        if (UNQUOTED_VALUE.matches(c)) {
          v.append(c);
          return UNQUOTED_VALUE;
        } else if (DOT.matches(c)) {
          flushUnquoted(e, v);
          return DOT;
        } else if (OPEN_PAREN.matches(c)) {
          flushUnquoted(e, v);
          return OPEN_PAREN;
        } else if (EOF.matches(c)) {
          flushUnquoted(e, v);
          return EOF;
        }
        throw new ParsingException("[a-zA-Z0-9_-] or '.'", i, c);
      };
    }
  }

  private static void flushRoot(List<Element> e, StringBuilder v) {
    e.add(RootElement.INSTANCE);
    v.setLength(0);
  }

  private static void flushUnquoted(List<Element> e, StringBuilder v) {
    e.add(new IdElement(v.toString(), "." + v.toString()));
    v.setLength(0);
  }

  private static void flushQuoted(List<Element> e, StringBuilder v) {
    e.add(new IdElement(v.toString(), "[\"" + v.toString() + "\"]"));
    v.setLength(0);
  }

  @FunctionalInterface
  interface NextState {
    State next(char c, int i, List<Element> e, StringBuilder v);
  }

  public static class ParsingException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    ParsingException(String expected, int i, char c) {
      this("Expected " + expected + " at position " + i + " but got '" + c + "'");
    }

    ParsingException(String message) {
      super(message);
    }

  }

  public interface Element {
    boolean isRoot();

    String id();
  }

  @EqualsAndHashCode
  public static class Path {
    private final List<Element> elements;

    private Path(List<Element> elements) {
      this.elements = elements;
    }

    public List<Element> elements() {
      return elements;
    }

    @Override
    public String toString() {
      return elements.stream().map(e -> e.toString()).collect(Collectors.joining(""));
    }
  }

  @EqualsAndHashCode
  @RequiredArgsConstructor
  static class IdElement implements Element {

    private final String id;
    private final String captured;

    @Override
    public boolean isRoot() {
      return false;
    }

    @Override
    public String id() {
      return id;
    }

    @Override
    public String toString() {
      return captured;
    }
  }

  enum RootElement implements Element {
    INSTANCE;

    @Override
    public boolean isRoot() {
      return true;
    }

    @Override
    public String id() {
      return "$";
    }

    @Override
    public String toString() {
      return id();
    }
  }

}
