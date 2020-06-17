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
import com.datastax.oss.protocol.internal.util.Bytes;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Objects;

/** A DSL that simulates a mock binary string for reading and writing, to use in our unit tests. */
// ErrorProne issues a warning about LinkedList. The suggested alternative is ArrayDeque, but it
// does not override equals. Keeping LinkedList here since performance is not crucial in tests.
@SuppressWarnings("JdkObsolete")
public class MockBinaryString {

  private LinkedList<Element> elements = new LinkedList<>();
  private LinkedList<Element> mark;

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

  public void markReaderIndex() {
    mark = new LinkedList<>();
    mark.addAll(elements);
  }

  public void resetReaderIndex() {
    if (mark == null) {
      throw new IllegalStateException("No mark, call markReaderIndex() first");
    }
    elements = mark;
    mark = null;
  }

  public MockBinaryString copy() {
    return new MockBinaryString().append(this);
  }

  public int size() {
    int size = 0;
    for (Element element : elements) {
      size += element.size();
    }
    return size;
  }

  MockBinaryString slice(int targetSize) {
    // We can't write a perfect implementation for this, since our internal representation is based
    // on primitive types (INT, LONG_STRING...), but not individual bytes.

    // The code below works as long as the split happens on the boundary between two elements. We
    // also support splitting a BYTES element, but that operation alters the contents
    // (re-concatenating the two strings is not strictly equal to the original).
    // This works for the tests that exercise this method so far, because they only check the
    // length, not the actual contents.

    MockBinaryString slice = new MockBinaryString();
    while (slice.size() < targetSize) {
      Element element = pop();
      if (slice.size() + element.size() <= targetSize) {
        slice.append(element.type, element.value);
      } else if (element.type == Element.Type.BYTES) {
        String hexString = (String) element.value;
        // BYTES starts with an int length
        int bytesToCopy = targetSize - slice.size() - 4;
        // Hex strings starts with '0x' and each byte is two characters
        int split = 2 + bytesToCopy * 2;
        slice.append(Element.Type.BYTES, hexString.substring(0, split));

        // Put the rest back in the source string. But we can't put it as a BYTES because size()
        // would over-estimate it by 4 (for the INT size). So write byte-by-byte instead.
        byte[] remainingBytes =
            Bytes.getArray(Bytes.fromHexString("0x" + hexString.substring(split)));
        for (byte b : remainingBytes) {
          this.prepend(Element.Type.BYTE, b);
        }
      } else {
        throw new UnsupportedOperationException("Can't split element other than BYTES");
      }
    }
    return slice;
  }

  Element pop() {
    return elements.pop();
  }

  Element pollFirst() {
    return elements.pollFirst();
  }

  Element pollLast() {
    return elements.pollLast();
  }

  private void append(Element.Type type, Object value) {
    this.elements.add(new Element(type, value));
  }

  private void prepend(Element.Type type, Object value) {
    this.elements.addFirst(new Element(type, value));
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

    int size() {
      String hexString;
      switch (type) {
        case BYTE:
          return 1;
        case INT:
          return PrimitiveSizes.INT;
        case INET:
          return PrimitiveSizes.sizeOfInet(((InetSocketAddress) value));
        case INETADDR:
          return PrimitiveSizes.sizeOfInetAddr(((InetAddress) value));
        case LONG:
          return PrimitiveSizes.LONG;
        case UNSIGNED_SHORT:
          return PrimitiveSizes.SHORT;
        case STRING:
          return PrimitiveSizes.sizeOfString((String) value);
        case LONG_STRING:
          return PrimitiveSizes.sizeOfLongString((String) value);
        case BYTES:
          hexString = (String) value; // 0xabcdef
          return PrimitiveSizes.INT + (hexString.length() - 2) / 2;
        case SHORT_BYTES:
          hexString = (String) value;
          return PrimitiveSizes.SHORT + (hexString.length() - 2) / 2;
        default:
          throw new IllegalStateException("Unsupported element type " + type);
      }
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
