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

import java.util.HashMap;
import java.util.Map;

/** An {@link IntMap} containing other {@code IntMap}s. */
public class IntIntMap<V> {

  public static <V> Builder<V> builder() {
    return new Builder<>();
  }

  private final IntMap<IntMap<V>> outer;

  private IntIntMap(IntMap<IntMap<V>> outer) {
    this.outer = outer;
  }

  public V get(int key1, int key2) {
    IntMap<V> inner = outer.get(key1);
    return (inner == null) ? null : inner.get(key2);
  }

  public static class Builder<V> {
    private Map<Integer, IntMap.Builder<V>> innerBuilders = new HashMap<>();

    public Builder<V> put(int key1, int key2, V value) {
      innerBuilders.computeIfAbsent(key1, k -> IntMap.builder()).put(key2, value);
      return this;
    }

    public IntIntMap<V> build() {
      IntMap.Builder<IntMap<V>> outerBuilder = IntMap.builder();
      for (Map.Entry<Integer, IntMap.Builder<V>> entry : innerBuilders.entrySet()) {
        outerBuilder.put(entry.getKey(), entry.getValue().build());
      }
      return new IntIntMap<>(outerBuilder.build());
    }
  }
}
