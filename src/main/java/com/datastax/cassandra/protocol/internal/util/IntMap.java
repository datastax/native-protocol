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
package com.datastax.cassandra.protocol.internal.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Barebones map-like structure with positive integer keys. It is immutable and optimized for low
 * cardinalities (entries are stored in a sparse array).
 */
public class IntMap<V> {

  public static <V> Builder<V> builder() {
    return new Builder<>();
  }

  private final Object[] values;

  private IntMap(Set<Map.Entry<Integer, V>> entries) {
    int maxKey = -1;
    for (Map.Entry<Integer, V> entry : entries) {
      maxKey = Math.max(maxKey, entry.getKey());
    }
    this.values = new Object[maxKey + 1];
    for (Map.Entry<Integer, V> entry : entries) {
      this.values[entry.getKey()] = entry.getValue();
    }
  }

  public V get(int key) {
    if (key < 0) {
      throw new IllegalArgumentException("key must be positive");
    }
    if (key >= values.length) {
      return null;
    } else {
      @SuppressWarnings("unchecked")
      V value = (V) values[key];
      return value;
    }
  }

  public static class Builder<V> {
    private Map<Integer, V> map = new HashMap<>();

    public Builder<V> put(int key, V value) {
      if (key < 0) {
        throw new IllegalArgumentException("key must be positive: " + key);
      }
      if (map.containsKey(key)) {
        throw new IllegalArgumentException("key already exists: " + key);
      }
      map.put(key, value);
      return this;
    }

    public IntMap<V> build() {
      return new IntMap<>(map.entrySet());
    }
  }
}
