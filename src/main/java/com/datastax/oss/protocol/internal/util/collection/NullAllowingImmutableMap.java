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

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable map that allows null keys and values.
 *
 * <p>This implementation is intended for low cardinalities, query operations have linear
 * performance. Insertion order is preserved.
 */
public class NullAllowingImmutableMap<K, V> extends AbstractMap<K, V> implements Serializable {

  @SuppressWarnings("rawtypes")
  private static final NullAllowingImmutableMap EMPTY =
      new NullAllowingImmutableMap<>(NullAllowingImmutableSet.of());

  @SuppressWarnings("unchecked")
  public static <K, V> NullAllowingImmutableMap<K, V> of() {
    return ((NullAllowingImmutableMap<K, V>) EMPTY);
  }

  public static <K, V> NullAllowingImmutableMap<K, V> of(K key1, V value1) {
    return new NullAllowingImmutableMap<>(
        NullAllowingImmutableSet.of(new SimpleImmutableEntry<>(key1, value1)));
  }

  public static <K, V> NullAllowingImmutableMap<K, V> of(K key1, V value1, K key2, V value2) {
    return (Objects.equals(key1, key2))
        ? of(key1, value1)
        : new NullAllowingImmutableMap<>(
            NullAllowingImmutableSet.of(
                new SimpleImmutableEntry<>(key1, value1),
                new SimpleImmutableEntry<>(key2, value2)));
  }

  public static <K, V> NullAllowingImmutableMap<K, V> of(
      K key1, V value1, K key2, V value2, K key3, V value3) {

    boolean equal12 = Objects.equals(key1, key2);
    boolean equal13 = Objects.equals(key1, key3);
    boolean equal23 = Objects.equals(key2, key3);

    if (equal12) {
      return equal13 ? of(key1, value1) : of(key1, value1, key3, value3);
    } else if (equal13) {
      return of(key1, value1, key2, value2);
    } else if (equal23) {
      return of(key1, value1, key2, value2);
    } else {
      return new NullAllowingImmutableMap<>(
          NullAllowingImmutableSet.of(
              new SimpleImmutableEntry<>(key1, value1),
              new SimpleImmutableEntry<>(key2, value2),
              new SimpleImmutableEntry<>(key3, value3)));
    }
  }

  public static <K, V> NullAllowingImmutableMap<K, V> copyOf(Map<K, V> map) {
    if (map instanceof NullAllowingImmutableMap) {
      return (NullAllowingImmutableMap<K, V>) map;
    } else {
      NullAllowingImmutableSet.Builder<Entry<K, V>> entriesBuilder =
          NullAllowingImmutableSet.builder();
      for (Entry<K, V> entry : map.entrySet()) {
        entriesBuilder.add(
            (entry instanceof AbstractMap.SimpleImmutableEntry)
                ? ((SimpleImmutableEntry<K, V>) entry)
                : new SimpleImmutableEntry<>(entry.getKey(), entry.getValue()));
      }
      return new NullAllowingImmutableMap<>(entriesBuilder.build());
    }
  }

  /**
   * Returns a builder to create a new instance, with the default expected size (16). The returned
   * builder is not thread-safe.
   *
   * @see #builder(int)
   */
  public static <K, V> Builder<K, V> builder() {
    return builder(16);
  }

  /**
   * Returns a builder to create a new instance. The returned builder is not thread-safe.
   *
   * @param expectedSize the number of expected entries in the map. This is used to pre-size
   *     internal data structures (if the builder ends up having more entries, it resizes
   *     automatically).
   */
  public static <K, V> Builder<K, V> builder(int expectedSize) {
    return new Builder<>(expectedSize);
  }

  /** @serial the map's entries as an immutable set (which serializes as an array) */
  private final NullAllowingImmutableSet<Entry<K, V>> entries;

  // This does not deduplicate keys, the caller MUST ensure that no duplicates are present.
  private NullAllowingImmutableMap(NullAllowingImmutableSet<Entry<K, V>> entries) {
    this.entries = entries;
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return entries;
  }

  // Override query operations, they're still linear but traverse the set's array directly to avoid
  // creating an iterator:

  @Override
  public boolean containsValue(Object value) {
    for (Object element : entries.elements) {
      @SuppressWarnings("unchecked")
      Entry<K, V> entry = (Entry<K, V>) element;
      if (Objects.equals(entry.getValue(), value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    for (Object element : entries.elements) {
      @SuppressWarnings("unchecked")
      Entry<K, V> entry = (Entry<K, V>) element;
      if (Objects.equals(entry.getKey(), key)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public V get(Object key) {
    for (Object element : entries.elements) {
      @SuppressWarnings("unchecked")
      Entry<K, V> entry = (Entry<K, V>) element;
      if (Objects.equals(entry.getKey(), key)) {
        return entry.getValue();
      }
    }
    return null;
  }

  /** A builder to create an immutable map; this class is not thread-safe. */
  public static class Builder<K, V> {

    private Object[] entries;
    private int size;

    public Builder(int expectedSize) {
      this.entries = new Object[expectedSize];
    }

    /** Duplicate keys are not allowed, and will cause {@link #build()} to fail. */
    public Builder<K, V> put(K key, V value) {
      maybeResize(1);
      entries[size++] = new SimpleImmutableEntry<>(key, value);
      return this;
    }

    public Builder<K, V> putAll(Map<? extends K, ? extends V> newEntries) {
      maybeResize(newEntries.size());
      for (Entry<? extends K, ? extends V> entry : newEntries.entrySet()) {
        @SuppressWarnings("unchecked")
        Object immutableEntry =
            (entry instanceof AbstractMap.SimpleImmutableEntry)
                ? ((SimpleImmutableEntry<K, V>) entry)
                : new SimpleImmutableEntry<>(entry.getKey(), entry.getValue());
        entries[size++] = immutableEntry;
      }
      return this;
    }

    public Map<K, V> build() {
      failIfDuplicateKeys();
      return new NullAllowingImmutableMap<>(
          new NullAllowingImmutableSet<Entry<K, V>>(
              (size == entries.length) ? entries : Arrays.copyOfRange(entries, 0, size)));
    }

    private void maybeResize(int toAdd) {
      int neededSize = size + toAdd;
      if (neededSize < 0) { // overflow
        throw new OutOfMemoryError();
      }
      if (neededSize > entries.length) {
        int newLength = entries.length * 2;
        if (newLength < 0) { // overflow
          newLength = neededSize;
        }
        entries = Arrays.copyOf(entries, newLength);
      }
    }

    private void failIfDuplicateKeys() {
      for (int i = 0; i < size; i++) {
        @SuppressWarnings("unchecked")
        Entry<K, V> entry1 = (Entry<K, V>) entries[i];
        for (int j = i + 1; j < size; j++) {
          @SuppressWarnings("unchecked")
          Entry<K, V> entry2 = (Entry<K, V>) entries[j];
          if (Objects.equals(entry1.getKey(), entry2.getKey())) {
            throw new IllegalArgumentException("Duplicate key " + entry1.getKey());
          }
        }
      }
    }
  }
}
