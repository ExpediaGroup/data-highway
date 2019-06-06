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
package com.hotels.road.partition;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.hotels.road.partition.KeyPathParser.ParsingException;
import com.hotels.road.partition.KeyPathParser.Path;

public class KeyPathParserTest {

  @Test
  public void rootOnly() {
    Path path = KeyPathParser.parse("$");
    assertThat(path.elements().size(), is(1));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
  }

  @Test
  public void oneSingleCharElement() {
    Path path = KeyPathParser.parse("$[\"a\"]");
    assertThat(path.elements().size(), is(2));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("a"));
  }

  @Test(expected = ParsingException.class)
  public void oneSingleCharsElement() {
    Path path = KeyPathParser.parse("$[\"\"]");
    assertThat(path.elements().size(), is(2));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is(""));
  }

  @Test
  public void oneSingleMultiCharElement() {
    Path path = KeyPathParser.parse("$[\"abc\"]");
    assertThat(path.elements().size(), is(2));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("abc"));
  }

  @Test
  public void multipleElements() {
    Path path = KeyPathParser.parse("$[\"a\"][\"bc\"][\"d\"]");
    assertThat(path.elements().size(), is(4));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("a"));
    assertThat(path.elements().get(2).isRoot(), is(false));
    assertThat(path.elements().get(2).id(), is("bc"));
    assertThat(path.elements().get(3).isRoot(), is(false));
    assertThat(path.elements().get(3).id(), is("d"));
  }

  @Test
  public void specialCharsInElement() {
    Path path = KeyPathParser.parse("$[\"a.b\"][\"cd\"]");
    assertThat(path.elements().size(), is(3));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("a.b"));
    assertThat(path.elements().get(2).isRoot(), is(false));
    assertThat(path.elements().get(2).id(), is("cd"));
  }

  @Test
  public void oneSingleCharElementDots() {
    Path path = KeyPathParser.parse("$.a");
    assertThat(path.elements().size(), is(2));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("a"));
  }

  @Test
  public void oneSingleMultiCharElementDots() {
    Path path = KeyPathParser.parse("$.abc");
    assertThat(path.elements().size(), is(2));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("abc"));
  }

  @Test
  public void multipleElementsDots() {
    Path path = KeyPathParser.parse("$.a.bc.d");
    assertThat(path.elements().size(), is(4));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("a"));
    assertThat(path.elements().get(2).isRoot(), is(false));
    assertThat(path.elements().get(2).id(), is("bc"));
    assertThat(path.elements().get(3).isRoot(), is(false));
    assertThat(path.elements().get(3).id(), is("d"));
  }

  @Test
  public void unquotedFollowedByQuoted() {
    Path path = KeyPathParser.parse("$.a[\"b\"]");
    assertThat(path.elements().size(), is(3));
    assertThat(path.elements().get(0).isRoot(), is(true));
    assertThat(path.elements().get(0).id(), is("$"));
    assertThat(path.elements().get(1).isRoot(), is(false));
    assertThat(path.elements().get(1).id(), is("a"));
    assertThat(path.elements().get(2).isRoot(), is(false));
    assertThat(path.elements().get(2).id(), is("b"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullPath() {
    KeyPathParser.parse(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void empty() {
    KeyPathParser.parse("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void blank() {
    KeyPathParser.parse("   ");
  }

  @Test(expected = ParsingException.class)
  public void exepectedFirstElement() {
    KeyPathParser.parse("$.");
  }

  @Test(expected = ParsingException.class)
  public void exepectedFirstElementQuote() {
    KeyPathParser.parse("$[\"");
  }

  @Test(expected = ParsingException.class)
  public void exepectedFirstElementBrace() {
    KeyPathParser.parse("$[");
  }

  @Test(expected = ParsingException.class)
  public void expectedSubsequentElement() {
    KeyPathParser.parse("$[\"a\"].");
  }

  @Test(expected = ParsingException.class)
  public void expectedSubsequentElementDots() {
    KeyPathParser.parse("$.a.");
  }

  @Test(expected = ParsingException.class)
  public void expectedSubsequentElementDots2() {
    KeyPathParser.parse("$[\"a\"].");
  }

  @Test(expected = ParsingException.class)
  public void unfinishedQuote() {
    KeyPathParser.parse("$[\"a]");
  }

  @Test(expected = ParsingException.class)
  public void unfinishedParen() {
    KeyPathParser.parse("$[\"a\"");
  }

  @Test(expected = ParsingException.class)
  public void missingRoot() {
    KeyPathParser.parse("[\"a\"][\"b\"]");
  }

  @Test(expected = ParsingException.class)
  public void missingRootDots() {
    KeyPathParser.parse("a.b");
  }

  @Test(expected = ParsingException.class)
  public void unexpectedCharacterAfterEndValueValue() {
    KeyPathParser.parse("$/");
  }

  @Test(expected = ParsingException.class)
  public void unexpectedCharacterAfterUnquotedValue() {
    KeyPathParser.parse("$.a/");
  }

}
