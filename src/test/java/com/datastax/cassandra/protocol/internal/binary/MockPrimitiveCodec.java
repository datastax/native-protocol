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

import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString.Element;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class MockPrimitiveCodec implements PrimitiveCodec<MockBinaryString> {
  public static final MockPrimitiveCodec INSTANCE = new MockPrimitiveCodec();

  /** In this implementation, {@link #sizeOf(MockBinaryString)} always returns this value. */
  public static final int MOCK_SIZE = 42;

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
    // return a mocked value, that should be enough for our tests
    return MOCK_SIZE;
  }

  @Override
  public MockBinaryString concat(MockBinaryString left, MockBinaryString right) {
    return left.append(right);
  }

  @Override
  public byte readByte(MockBinaryString source) {
    return (byte) pop(source, Element.Type.BYTE);
  }

  @Override
  public int readInt(MockBinaryString source) {
    return (Integer) pop(source, Element.Type.INT);
  }

  @Override
  public InetSocketAddress readInet(MockBinaryString source) {
    return (InetSocketAddress) pop(source, Element.Type.INET);
  }

  @Override
  public long readLong(MockBinaryString source) {
    return (Long) pop(source, Element.Type.LONG);
  }

  @Override
  public int readUnsignedShort(MockBinaryString source) {
    return (Integer) pop(source, Element.Type.UNSIGNED_SHORT);
  }

  @Override
  public ByteBuffer readBytes(MockBinaryString source) {
    String hexString = (String) pop(source, Element.Type.BYTES);
    return Bytes.fromHexString(hexString);
  }

  @Override
  public byte[] readShortBytes(MockBinaryString source) {
    String hexString = (String) pop(source, Element.Type.SHORT_BYTES);
    return Bytes.fromHexString(hexString).array();
  }

  @Override
  public String readString(MockBinaryString source) {
    return (String) pop(source, Element.Type.STRING);
  }

  @Override
  public String readLongString(MockBinaryString source) {
    return (String) pop(source, Element.Type.LONG_STRING);
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
  public void writeInet(InetSocketAddress address, MockBinaryString dest) {
    dest.inet(address.getHostName(), address.getPort());
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
  public void writeShortBytes(byte[] bytes, MockBinaryString dest) {
    dest.shortBytes(Bytes.toHexString(bytes));
  }

  private Object pop(MockBinaryString source, Element.Type expectedType) {
    Element element = source.pop();
    assertThat(element.type).isEqualTo(expectedType);
    return element.value;
  }
}
