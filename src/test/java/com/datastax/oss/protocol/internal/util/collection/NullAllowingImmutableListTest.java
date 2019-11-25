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
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class NullAllowingImmutableListTest {

  @Test
  public void should_create_from_elements() {
    assertThat(NullAllowingImmutableList.of(1)).hasSize(1).containsExactly(1);
    assertThat(NullAllowingImmutableList.of((Integer) null))
        .hasSize(1)
        .containsExactly((Integer) null);
    assertThat(NullAllowingImmutableList.of(1, 2)).hasSize(2).containsExactly(1, 2);
    assertThat(NullAllowingImmutableList.of(1, 2, 3)).hasSize(3).containsExactly(1, 2, 3);
    assertThat(NullAllowingImmutableList.of(1, 2, 3, 4, 5))
        .hasSize(5)
        .containsExactly(1, 2, 3, 4, 5);
  }

  @Test
  public void should_create_from_collection() {
    List<Integer> collection = new ArrayList<>();
    collection.add(1);
    collection.add(null);
    collection.add(3);
    assertThat(NullAllowingImmutableList.copyOf(collection)).hasSize(3).containsExactly(1, null, 3);
  }

  @Test
  public void should_create_with_builder() {
    assertThat(NullAllowingImmutableList.builder().add(1).add(null).add(3).build())
        .hasSize(3)
        .containsExactly(1, null, 3);
  }

  @Test
  public void should_serialize_and_deserialize() {
    NullAllowingImmutableList<Integer> in = NullAllowingImmutableList.of(1, null, 3);
    NullAllowingImmutableList<Integer> out = SerializationHelper.serializeAndDeserialize(in);
    assertThat(out).hasSize(3).containsExactly(1, null, 3);
  }

  @Test
  public void should_return_singleton_empty_instance() {
    NullAllowingImmutableList<Integer> l1 = NullAllowingImmutableList.of();
    NullAllowingImmutableList<Integer> l2 = NullAllowingImmutableList.of();
    assertThat(l1).isSameAs(l2);
  }
}
