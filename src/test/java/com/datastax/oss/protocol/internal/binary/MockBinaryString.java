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
package com.datastax.oss.protocol.internal.binary;

import com.datastax.oss.protocol.internal.PrimitiveSizes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Objects;

/** A DSL that simulates a mock binary string for reading and writing, to use in our unit tests. */
public class MockBinaryString {

  // The suggested alternative is ArrayDeque, but it does not override equals. Keeping LinkedList
  // here since performance is not crucial in tests.
  @SuppressWarnings("JdkObsolete")
  private final LinkedList<Element> elements = new LinkedList<>();

  public MockBinaryString byte_(int value) {
    append(Element.Type.BYTE, (byte) value);
    return this;
  }

  public MockBinaryString int_(int value) {
    append(Element.Type.INT, value);
    return this;
  }

  public MockBinaryString inetAddr(InetAddress host) {
    append(Element.Type.INETADDR, host);
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
    append(Element.Type.LONG_STRING, value);
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

  public int size() {
    int size = 0;
    for (Element element : elements) {
      String hexString;
      switch (element.type) {
        case BYTE:
          size += 1;
          break;
        case INT:
          size += PrimitiveSizes.INT;
          break;
        case INET:
          size += PrimitiveSizes.sizeOfInet(((InetSocketAddress) element.value));
          break;
        case INETADDR:
          size += PrimitiveSizes.sizeOfInetAddr(((InetAddress) element.value));
          break;
        case LONG:
          size += PrimitiveSizes.LONG;
          break;
        case UNSIGNED_SHORT:
          size += PrimitiveSizes.SHORT;
          break;
        case STRING:
          size += PrimitiveSizes.sizeOfString((String) element.value);
          break;
        case LONG_STRING:
          size += PrimitiveSizes.sizeOfLongString((String) element.value);
          break;
        case BYTES:
          hexString = (String) element.value; // 0xabcdef
          size += PrimitiveSizes.INT + (hexString.length() - 2) / 2;
          break;
        case SHORT_BYTES:
          hexString = (String) element.value;
          size += PrimitiveSizes.SHORT + (hexString.length() - 2) / 2;
          break;
      }
    }
    return size;
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
  public int hashCode() {
    return this.elements.hashCode();
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
      INETADDR,
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
    public int hashCode() {
      return Objects.hash(type, value);
    }

    @Override
    public String toString() {
      return String.format("[%s:%s]", type, value);
    }
  }
}
