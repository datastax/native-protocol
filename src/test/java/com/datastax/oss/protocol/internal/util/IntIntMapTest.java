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

public class IntIntMapTest {

  @Test
  public void should_store_and_retrieve_entries() {
    IntIntMap<String> map =
        IntIntMap.<String>builder()
            .put(1, 1, "foo11")
            .put(1, 2, "foo12")
            .put(1, 3, "foo13")
            .put(2, 1, "foo21")
            .put(3, 1, "foo31")
            .build();

    assertThat(map.get(1, 1)).isEqualTo("foo11");
    assertThat(map.get(1, 2)).isEqualTo("foo12");
    assertThat(map.get(1, 3)).isEqualTo("foo13");
    assertThat(map.get(2, 1)).isEqualTo("foo21");
    assertThat(map.get(3, 1)).isEqualTo("foo31");
  }
}
