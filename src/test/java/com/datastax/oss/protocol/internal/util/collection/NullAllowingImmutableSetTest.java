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
package com.datastax.oss.protocol.internal.util.collection;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.util.SerializationHelper;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class NullAllowingImmutableSetTest {

  @Test
  public void should_deduplicate() {
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[0])).isEmpty();
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[] {1})).containsExactly(1);
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[] {1, 1})).containsExactly(1);
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[] {1, 2, 3}))
        .containsExactly(1, 2, 3);
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[] {1, 2, 2, 3}))
        .containsExactly(1, 2, 3);
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[] {1, 2, 2, 3, 3, 3}))
        .containsExactly(1, 2, 3);
  }

  @Test
  public void should_keep_first_occurrence_when_deduplicating() {
    String foo = "foo";
    String bar = "bar";
    @SuppressWarnings("RedundantStringConstructorCall")
    String foo2 = new String("foo");
    assertThat(NullAllowingImmutableSet.deduplicate(new Object[] {foo, bar, foo2}))
        .containsExactly(foo, bar);
  }

  @Test
  public void should_create_from_elements() {
    assertThat(NullAllowingImmutableSet.of(1)).hasSize(1).containsExactly(1);
    assertThat(NullAllowingImmutableSet.of((Integer) null))
        .hasSize(1)
        .containsExactly((Integer) null);
    assertThat(NullAllowingImmutableSet.of(1, 2)).hasSize(2).containsExactly(1, 2);
    assertThat(NullAllowingImmutableSet.of(1, 2, 3)).hasSize(3).containsExactly(1, 2, 3);
    assertThat(NullAllowingImmutableSet.of(1, 2, 3, 4, 5, 1))
        .hasSize(5)
        .containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void should_create_from_collection() {
    List<Integer> collection = new ArrayList<>();
    collection.add(1);
    collection.add(null);
    collection.add(3);
    collection.add(1);
    assertThat(NullAllowingImmutableSet.copyOf(collection)).hasSize(3).containsExactly(1, null, 3);
  }

  @Test
  public void should_create_with_builder() {
    assertThat(NullAllowingImmutableSet.builder().add(1).add(null).add(3).build())
        .hasSize(3)
        .containsExactly(1, null, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_if_duplicates_in_builder() {
    NullAllowingImmutableSet.builder().add(1).add(null).add(1).build();
  }

  @Test
  public void should_serialize_and_deserialize() {
    NullAllowingImmutableSet<Integer> in = NullAllowingImmutableSet.of(1, null, 3);
    NullAllowingImmutableSet<Integer> out = SerializationHelper.serializeAndDeserialize(in);
    assertThat(out).hasSize(3).containsExactly(1, null, 3);
  }
}
