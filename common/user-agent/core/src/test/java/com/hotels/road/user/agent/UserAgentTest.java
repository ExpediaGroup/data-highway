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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Optional;

import org.junit.Test;

import com.hotels.road.user.agent.UserAgent.Token;

public class UserAgentTest {

  @Test
  public void toString_empty() {
    assertThat(UserAgent.create().toString(), is(""));
  }

  @Test
  public void toString_single() {
    assertThat(UserAgent.create().add("a", "1.0").toString(), is("a/1.0"));
  }

  @Test
  public void toString_single_comment() {
    assertThat(UserAgent.create().add("a", "1.0", "b").toString(), is("a/1.0 (b)"));
  }

  @Test
  public void toString_multiple() {
    assertThat(UserAgent.create().add("a", "1.0").add("b", "2.0").toString(), is("a/1.0 b/2.0"));
  }

  @Test
  public void toString_multiple_comment() {
    assertThat(UserAgent.create().add("a", "1.0", "b").add("c", "2.0", "d").toString(), is("a/1.0 (b) c/2.0 (d)"));
  }

  @Test
  public void token_empty() {
    assertThat(UserAgent.create().token("a"), is(Optional.empty()));
  }

  @Test
  public void token_single() {
    assertThat(UserAgent.create().add("a", "1.0").token("a").get(), is(new Token("a", "1.0", Optional.empty())));
  }

  @Test
  public void parse_empty() {
    assertThat(UserAgent.parse("").toString(), is(""));
  }

  @Test
  public void null_agent_string() {
    assertThat(UserAgent.parse(null).toString(), is(""));
  }

  @Test
  public void parse_single() {
    assertThat(UserAgent.parse("a/1.0").token("a").get(), is(new Token("a", "1.0", Optional.empty())));
  }

  @Test
  public void parse_multiple() {
    UserAgent underTest = UserAgent.parse("a/1.0 (b) c/2.0");
    assertThat(underTest.token("a").get(), is(new Token("a", "1.0", Optional.of("b"))));
    assertThat(underTest.token("c").get(), is(new Token("c", "2.0", Optional.empty())));
  }

  @Test
  public void realBrowserUserAgent() throws Exception {
    String example = "Mozilla/5.0 (Linux; U; Android 4.1.1; en-gb; Build/KLP) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Safari/534.30";

    UserAgent underTest = UserAgent.parse(example);

    assertThat(underTest.token("Mozilla").get(),
        is(new Token("Mozilla", "5.0", Optional.of("Linux; U; Android 4.1.1; en-gb; Build/KLP"))));
    assertThat(underTest.token("AppleWebKit").get(),
        is(new Token("AppleWebKit", "534.30", Optional.of("KHTML, like Gecko"))));
    assertThat(underTest.token("Version").get(), is(new Token("Version", "4.0", Optional.empty())));
    assertThat(underTest.token("Safari").get(), is(new Token("Safari", "534.30", Optional.empty())));

    assertThat(underTest.toString(), is(example));
  }

}
