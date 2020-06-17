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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class MockPrimitiveCodec implements PrimitiveCodec<MockBinaryString> {
  public static final MockPrimitiveCodec INSTANCE = new MockPrimitiveCodec();

  @Override
  public MockBinaryString allocate(int size) {
    return new MockBinaryString();
  }

  @Override
  public void release(MockBinaryString toRelease) {
    // do nothing
  }

  @Override
  public int sizeOf(MockBinaryString toMeasure) {
    return toMeasure.size();
  }

  @Override
  public void markReaderIndex(MockBinaryString source) {
    source.markReaderIndex();
  }

  @Override
  public void resetReaderIndex(MockBinaryString source) {
    source.resetReaderIndex();
  }

  @Override
  public MockBinaryString concat(MockBinaryString left, MockBinaryString right) {
    return left.append(right);
  }

  @Override
  public byte readByte(MockBinaryString source) {
    return (byte) pop(source, MockBinaryString.Element.Type.BYTE);
  }

  @Override
  public int readInt(MockBinaryString source) {
    return (Integer) pop(source, MockBinaryString.Element.Type.INT);
  }

  @Override
  public int readInt(MockBinaryString source, int offset) {
    MockBinaryString copy = source.copy();
    int skipped = 0;
    while (skipped < offset) {
      MockBinaryString.Element element = copy.pop();
      skipped += element.size();
    }
    if (skipped != offset) {
      throw new IllegalArgumentException("Offset must match an exact number of elements");
    }
    return readInt(copy);
  }

  @Override
  public InetAddress readInetAddr(MockBinaryString source) {
    return (InetAddress) pop(source, MockBinaryString.Element.Type.INETADDR);
  }

  @Override
  public long readLong(MockBinaryString source) {
    return (Long) pop(source, MockBinaryString.Element.Type.LONG);
  }

  @Override
  public int readUnsignedShort(MockBinaryString source) {
    return (Integer) pop(source, MockBinaryString.Element.Type.UNSIGNED_SHORT);
  }

  @Override
  public ByteBuffer readBytes(MockBinaryString source) {
    String hexString = (String) pop(source, MockBinaryString.Element.Type.BYTES);
    return Bytes.fromHexString(hexString);
  }

  @Override
  public byte[] readShortBytes(MockBinaryString source) {
    String hexString = (String) pop(source, MockBinaryString.Element.Type.SHORT_BYTES);
    return Bytes.fromHexString(hexString).array();
  }

  @Override
  public String readString(MockBinaryString source) {
    return (String) pop(source, MockBinaryString.Element.Type.STRING);
  }

  @Override
  public String readLongString(MockBinaryString source) {
    return (String) pop(source, MockBinaryString.Element.Type.LONG_STRING);
  }

  @Override
  public MockBinaryString readRetainedSlice(MockBinaryString source, int sliceLength) {
    return source.slice(sliceLength);
  }

  @Override
  public void updateCrc(MockBinaryString source, CRC32 crc) {
    source = source.copy(); // don't consume the input
    MockBinaryString.Element element;
    while ((element = source.pollFirst()) != null) {
      if (element.type == MockBinaryString.Element.Type.BYTE) {
        crc.update(((byte) element.value) & 0xFF);
      } else {
        throw new IllegalArgumentException(
            "PrimitiveCodec.updateCrc() is only supported on MockBinaryStrings that were assembled byte-by-byte. "
                + "Unexpected type "
                + element.type);
      }
    }
  }

  @Override
  public void writeByte(byte b, MockBinaryString dest) {
    dest.byte_(b);
  }

  @Override
  public void writeInt(int i, MockBinaryString dest) {
    dest.int_(i);
  }

  @Override
  public void writeInetAddr(InetAddress address, MockBinaryString dest) {
    dest.inetAddr(address);
  }

  @Override
  public void writeLong(long l, MockBinaryString dest) {
    dest.long_(l);
  }

  @Override
  public void writeUnsignedShort(int i, MockBinaryString dest) {
    dest.unsignedShort(i);
  }

  @Override
  public void writeString(String s, MockBinaryString dest) {
    dest.string(s);
  }

  @Override
  public void writeLongString(String s, MockBinaryString dest) {
    dest.longString(s);
  }

  @Override
  public void writeBytes(ByteBuffer bytes, MockBinaryString dest) {
    dest.bytes(Bytes.toHexString(bytes));
  }

  @Override
  public void writeBytes(byte[] bytes, MockBinaryString dest) {
    dest.bytes(Bytes.toHexString(bytes));
  }

  @Override
  public void writeShortBytes(byte[] bytes, MockBinaryString dest) {
    dest.shortBytes(Bytes.toHexString(bytes));
  }

  private Object pop(MockBinaryString source, MockBinaryString.Element.Type expectedType) {
    MockBinaryString.Element element = source.pop();
    assertThat(element.type).isEqualTo(expectedType);
    return element.value;
  }
}
