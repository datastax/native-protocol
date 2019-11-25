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

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable set that allows null elements.
 *
 * <p>This implementation is intended for low cardinalities, query operations have linear
 * performance. Insertion order is preserved.
 */
public class NullAllowingImmutableSet<E> extends AbstractSet<E> implements Serializable {

  @SuppressWarnings("rawtypes")
  private static final NullAllowingImmutableSet EMPTY =
      new NullAllowingImmutableSet<>(new Object[] {});

  @SafeVarargs
  public static <E> NullAllowingImmutableSet<E> of(E... elements) {
    return new NullAllowingImmutableSet<>(deduplicate(elements));
  }

  @SuppressWarnings("unchecked")
  public static <E> NullAllowingImmutableSet<E> of() {
    return ((NullAllowingImmutableSet<E>) EMPTY);
  }

  public static <E> NullAllowingImmutableSet<E> of(E element) {
    return new NullAllowingImmutableSet<>(new Object[] {element});
  }

  public static <E> NullAllowingImmutableSet<E> of(E element1, E element2) {
    return (Objects.equals(element1, element2))
        ? of(element1)
        : new NullAllowingImmutableSet<>(new Object[] {element1, element2});
  }

  public static <E> NullAllowingImmutableSet<E> of(E element1, E element2, E element3) {

    boolean equal12 = Objects.equals(element1, element2);
    boolean equal13 = Objects.equals(element1, element3);
    boolean equal23 = Objects.equals(element2, element3);

    if (equal12) {
      return equal13 ? of(element1) : of(element1, element3);
    } else if (equal13) {
      return of(element1, element2);
    } else if (equal23) {
      return of(element1, element2);
    } else {
      return new NullAllowingImmutableSet<>(new Object[] {element1, element2, element3});
    }
  }

  public static <E> NullAllowingImmutableSet<E> copyOf(Iterable<E> iterable) {
    if (iterable instanceof NullAllowingImmutableSet) {
      return ((NullAllowingImmutableSet<E>) iterable);
    } else if (iterable instanceof Set) {
      // skip deduplication
      Set<?> set = (Set<?>) iterable;
      return new NullAllowingImmutableSet<>(set.toArray());
    } else if (iterable instanceof Collection) {
      Collection<?> collection = (Collection<?>) iterable;
      return new NullAllowingImmutableSet<>(deduplicate(collection.toArray()));
    } else {
      Set<E> set = new LinkedHashSet<>();
      for (E element : iterable) {
        set.add(element);
      }
      return copyOf(set);
    }
  }

  /**
   * Returns a builder to create a new instance, with the default expected size (16). The returned
   * builder is not thread-safe.
   *
   * @see #builder(int)
   */
  public static <E> Builder<E> builder() {
    return builder(16);
  }

  /**
   * Returns a builder to create a new instance. The returned builder is not thread-safe.
   *
   * @param expectedSize the number of expected elements in the set. This is used to pre-size
   *     internal data structures (if the builder ends up having more elements, it resizes
   *     automatically).
   */
  public static <E> Builder<E> builder(int expectedSize) {
    return new Builder<>(expectedSize);
  }

  /** @serial an array containing the set's elements */
  final Object[] elements;

  // This does not deduplicate elements, the caller MUST ensure that no duplicates are present.
  NullAllowingImmutableSet(Object[] elements) {
    this.elements = elements;
  }

  // visible for testing
  static Object[] deduplicate(Object[] elements) {
    Object[] result = new Object[elements.length];
    int copied = 0;
    for (Object element : elements) {
      boolean isDuplicate = false;
      for (int j = 0; j < copied; j++) {
        if (Objects.equals(result[j], element)) {
          isDuplicate = true;
          break;
        }
      }
      if (!isDuplicate) {
        result[copied++] = element;
      }
    }
    return (copied == elements.length) ? result : Arrays.copyOfRange(result, 0, copied);
  }

  private NullAllowingImmutableSet(E element) {
    this(new Object[] {element});
  }

  private NullAllowingImmutableSet(E element1, E element2) {
    this(new Object[] {element1, element2});
  }

  private NullAllowingImmutableSet(E element1, E element2, E element3) {
    this(new Object[] {element1, element2, element3});
  }

  @Override
  public Iterator<E> iterator() {
    return new ArrayIterator<>(elements);
  }

  @Override
  public int size() {
    return elements.length;
  }

  private static class ArrayIterator<E> implements Iterator<E> {

    private final Object[] elements;
    private int nextIndex;

    private ArrayIterator(Object[] elements) {
      this.elements = elements;
      this.nextIndex = 0;
    }

    @Override
    public boolean hasNext() {
      return nextIndex < elements.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public E next() {
      if (!hasNext()) {
        throw new NoSuchElementException("Iterator is exhausted");
      }
      return (E) elements[nextIndex++];
    }
  }

  /** A builder to create an immutable list; this class is not thread-safe. */
  public static class Builder<E> {

    private Object[] elements;
    private int size;

    public Builder(int expectedSize) {
      this.elements = new Object[expectedSize];
    }

    /** Duplicate elements are not allowed, and will cause {@link #build()} to fail. */
    public Builder<E> add(E newElement) {
      maybeResize(1);
      elements[size++] = newElement;
      return this;
    }

    @SuppressWarnings("unchecked")
    public Builder<E> addAll(Iterable<? extends E> newElements) {
      Collection<E> collection;
      if (newElements instanceof Collection) {
        collection = (Collection<E>) newElements;
      } else {
        collection = new LinkedHashSet<>();
        for (E newElement : newElements) {
          collection.add(newElement);
        }
      }
      maybeResize(collection.size());
      for (Object newElement : collection) {
        elements[size++] = newElement;
      }
      return this;
    }

    public NullAllowingImmutableSet<E> build() {
      failIfDuplicates();
      return new NullAllowingImmutableSet<>(
          (size == elements.length) ? elements : Arrays.copyOfRange(elements, 0, size));
    }

    private void maybeResize(int toAdd) {
      int neededSize = size + toAdd;
      if (neededSize < 0) { // overflow
        throw new OutOfMemoryError();
      }
      if (neededSize > elements.length) {
        int newLength = elements.length * 2;
        if (newLength < 0) { // overflow
          newLength = neededSize;
        }
        elements = Arrays.copyOf(elements, newLength);
      }
    }

    private void failIfDuplicates() {
      for (int i = 0; i < size; i++) {
        Object element = elements[i];
        for (int j = i + 1; j < size; j++) {
          if (Objects.equals(element, elements[j])) {
            throw new IllegalArgumentException("Duplicate element " + element);
          }
        }
      }
    }
  }
}
