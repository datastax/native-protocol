/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.protocol.internal.util.collection;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.util.SerializationHelper;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class NullAllowingImmutableMapTest {

  @Test
  public void should_create_from_elements() {
    assertThat(NullAllowingImmutableMap.of("foo", 1)).hasSize(1).containsEntry("foo", 1);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "bar", 2))
        .hasSize(2)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "foo", 2)).hasSize(1).containsEntry("foo", 1);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "bar", 2, "baz", 3))
        .hasSize(3)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2)
        .containsEntry("baz", 3);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "foo", 2, "baz", 3))
        .hasSize(2)
        .containsEntry("foo", 1)
        .containsEntry("baz", 3);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "bar", 2, "foo", 3))
        .hasSize(2)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "bar", 2, "bar", 3))
        .hasSize(2)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2);
    assertThat(NullAllowingImmutableMap.of("foo", 1, "foo", 2, "foo", 3))
        .hasSize(1)
        .containsEntry("foo", 1);
  }

  @Test
  public void should_create_from_other_map() {
    Map<String, Integer> origin = new HashMap<>();
    origin.put("foo", 1);
    origin.put("bar", 2);
    origin.put("baz", 3);

    assertThat(NullAllowingImmutableMap.copyOf(origin))
        .hasSize(3)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2)
        .containsEntry("baz", 3);
  }

  @Test
  public void should_create_with_builder() {
    assertThat(NullAllowingImmutableMap.builder().put("foo", 1).put("bar", 2).put("baz", 3).build())
        .hasSize(3)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2)
        .containsEntry("baz", 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_duplicate_keys_in_builder() {
    NullAllowingImmutableMap.builder().put("foo", 1).put("bar", 2).put("foo", 3).build();
  }

  @Test
  public void should_serialize_and_deserialize() {
    NullAllowingImmutableMap<String, Integer> in =
        NullAllowingImmutableMap.of("foo", 1, "bar", 2, "baz", 3);
    NullAllowingImmutableMap<String, Integer> out = SerializationHelper.serializeAndDeserialize(in);
    assertThat(out)
        .hasSize(3)
        .containsEntry("foo", 1)
        .containsEntry("bar", 2)
        .containsEntry("baz", 3);
  }

  @Test
  public void should_return_singleton_empty_instance() {
    NullAllowingImmutableMap<String, Integer> m1 = NullAllowingImmutableMap.of();
    NullAllowingImmutableMap<String, Integer> m2 = NullAllowingImmutableMap.of();
    assertThat(m1).isSameAs(m2);
  }

  @Test
  public void should_resize_builder_internal_capacity() {
    // will resize once
    assertThat(
            NullAllowingImmutableMap.builder(10)
                .putAll(IntStream.range(0, 20).boxed().collect(Collectors.toMap(i -> i, i -> i)))
                .build())
        .hasSize(20);
    // will resize twice
    assertThat(
            NullAllowingImmutableMap.builder(10)
                .putAll(IntStream.range(0, 30).boxed().collect(Collectors.toMap(i -> i, i -> i)))
                .build())
        .hasSize(30);
  }
}
