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
package com.datastax.cassandra.protocol.internal.binary;

import java.net.InetSocketAddress;
import java.util.LinkedList;

/** A DSL that simulates a mock binary string for reading and writing, to use in our unit tests. */
public class MockBinaryString {
  private final LinkedList<Element> elements = new LinkedList<>();

  public MockBinaryString byte_(int value) {
    append(Element.Type.BYTE, (byte) value);
    return this;
  }

  public MockBinaryString int_(int value) {
    append(Element.Type.INT, value);
    return this;
  }

  public MockBinaryString inet(String host, int port) {
    append(Element.Type.INET, InetSocketAddress.createUnresolved(host, port));
    return this;
  }

  public MockBinaryString long_(long value) {
    append(Element.Type.LONG, value);
    return this;
  }

  public MockBinaryString unsignedShort(int value) {
    append(Element.Type.UNSIGNED_SHORT, value);
    return this;
  }

  public MockBinaryString string(String value) {
    append(Element.Type.STRING, value);
    return this;
  }

  public MockBinaryString longString(String value) {
    append(Element.Type.STRING, value);
    return this;
  }

  public MockBinaryString bytes(String value) {
    append(Element.Type.BYTES, value);
    return this;
  }

  public MockBinaryString shortBytes(String value) {
    append(Element.Type.SHORT_BYTES, value);
    return this;
  }

  public MockBinaryString append(MockBinaryString other) {
    for (Element element : other.elements) {
      this.elements.add(element);
    }
    return this;
  }

  public MockBinaryString copy() {
    return new MockBinaryString().append(this);
  }

  Element pop() {
    return elements.pop();
  }

  Element pollLast() {
    return elements.pollLast();
  }

  private void append(Element.Type type, Object value) {
    this.elements.add(new Element(type, value));
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MockBinaryString) {
      MockBinaryString that = (MockBinaryString) other;
      return this.elements.equals(that.elements);
    }
    return false;
  }

  @Override
  public String toString() {
    return elements.toString();
  }

  static class Element {
    enum Type {
      BYTE,
      INT,
      INET,
      LONG,
      UNSIGNED_SHORT,
      STRING,
      LONG_STRING,
      BYTES,
      SHORT_BYTES
    }

    final Type type;
    final Object value;

    private Element(Type type, Object value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof Element) {
        Element that = (Element) other;
        return this.type == that.type
            && (this.value == null ? that.value == null : this.value.equals(that.value));
      }
      return false;
    }

    @Override
    public String toString() {
      return String.format("[%s:%s]", type, value);
    }
  }
}
