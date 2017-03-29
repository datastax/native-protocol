/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.protocol.internal.util;

import org.testng.annotations.Test;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

public class IntMapTest {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_not_allow_negative_keys() {
    IntMap.builder().put(-1, "a");
  }

  @Test
  public void should_store_and_retrieve_entries() {
    IntMap<String> map = IntMap.<String>builder().put(1, "foo").put(3, "bar").build();
    assertThat(map.get(0)).isNull();
    assertThat(map.get(1)).isEqualTo("foo");
    assertThat(map.get(2)).isNull();
    assertThat(map.get(3)).isEqualTo("bar");
    assertThat(map.get(4)).isNull();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void should_fail_if_key_already_exists() {
    IntMap.builder().put(1, "foo").put(1, "bar");
  }
}
