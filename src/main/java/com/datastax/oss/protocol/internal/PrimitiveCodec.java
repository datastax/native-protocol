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
package com.datastax.oss.protocol.internal;

import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;

/**
 * Reads and writes the protocol's primitive types (as defined in section 3 of the protocol
 * specification) to a binary representation {@code B}.
 *
 * @param <B> the binary representation we're manipulating. This API is designed with a mutable
 *     implementation in mind (because that is what the core driver is using). If you want to use an
 *     immutable type, write a mutable wrapper for it.
 */
public interface PrimitiveCodec<B> {
  B allocate(int size);

  /**
   * "Releases" an element if the underlying implementation uses reference counting to manage memory
   * allocation. Otherwise, this can simply be a no-op.
   */
  void release(B toRelease);

  /** The number of <b>readable</b> bytes in the given element. */
  int sizeOf(B toMeasure);

  B concat(B left, B right);

  void markReaderIndex(B source);

  void resetReaderIndex(B source);

  byte readByte(B source);

  int readInt(B source);

  /**
   * Reads an int at the given offset (relative to the current read index). This method does not
   * consume any data.
   */
  int readInt(B source, int offset);

  InetAddress readInetAddr(B source);

  long readLong(B source);

  int readUnsignedShort(B source);

  ByteBuffer readBytes(B source);

  byte[] readShortBytes(B source);

  String readString(B source);

  String readLongString(B source);

  /**
   * Returns a view of the next {@code sliceLength} bytes. This consumes the bytes (the next reads
   * must return the data after the slice). If the underlying implementation uses reference counting
   * to manage memory allocation, this must also "retain" the slice, so that it doesn't get recycled
   * if {@code source} is released.
   */
  B readRetainedSlice(B source, int sliceLength);

  /** Feeds all available bytes into the given CRC. This should not "consume" the bytes. */
  void updateCrc(B source, CRC32 crc);

  default UUID readUuid(B source) {
    long msb = readLong(source);
    long lsb = readLong(source);
    return new UUID(msb, lsb);
  }

  default List<String> readStringList(B source) {
    int size = readUnsignedShort(source);
    if (size == 0) {
      return Collections.emptyList();
    } else {
      NullAllowingImmutableList.Builder<String> builder = NullAllowingImmutableList.builder(size);
      for (int i = 0; i < size; i++) {
        builder.add(readString(source));
      }
      return builder.build();
    }
  }

  default Map<String, String> readStringMap(B source) {
    int size = readUnsignedShort(source);
    if (size == 0) {
      return Collections.emptyMap();
    } else {
      NullAllowingImmutableMap.Builder<String, String> builder =
          NullAllowingImmutableMap.builder(size);
      for (int i = 0; i < size; i++) {
        String k = readString(source);
        String v = readString(source);
        builder.put(k, v);
      }
      return builder.build();
    }
  }

  default Map<String, List<String>> readStringMultimap(B source) {
    int size = readUnsignedShort(source);
    if (size == 0) {
      return Collections.emptyMap();
    } else {
      NullAllowingImmutableMap.Builder<String, List<String>> builder =
          NullAllowingImmutableMap.builder(size);
      for (int i = 0; i < size; i++) {
        String key = readString(source);
        List<String> value = readStringList(source);
        builder.put(key, value);
      }
      return builder.build();
    }
  }

  default Map<String, ByteBuffer> readBytesMap(B source) {
    int size = readUnsignedShort(source);
    if (size == 0) {
      return Collections.emptyMap();
    } else {
      NullAllowingImmutableMap.Builder<String, ByteBuffer> builder =
          NullAllowingImmutableMap.builder(size);
      for (int i = 0; i < size; i++) {
        String key = readString(source);
        ByteBuffer value = readBytes(source);
        builder.put(key, value);
      }
      return builder.build();
    }
  }

  default InetSocketAddress readInet(B source) {
    InetAddress addr = readInetAddr(source);
    int port = readInt(source);
    return new InetSocketAddress(addr, port);
  }

  void writeByte(byte b, B dest);

  void writeInt(int i, B dest);

  void writeInetAddr(InetAddress address, B dest);

  void writeLong(long l, B dest);

  void writeUnsignedShort(int i, B dest);

  void writeString(String s, B dest);

  void writeLongString(String s, B dest);

  default void writeUuid(UUID uuid, B dest) {
    writeLong(uuid.getMostSignificantBits(), dest);
    writeLong(uuid.getLeastSignificantBits(), dest);
  }

  void writeBytes(ByteBuffer bytes, B dest);

  void writeBytes(byte[] bytes, B dest);

  void writeShortBytes(byte[] bytes, B dest);

  default void writeStringList(List<String> l, B dest) {
    writeUnsignedShort(l.size(), dest);
    for (String s : l) {
      writeString(s, dest);
    }
  }

  default void writeStringMap(Map<String, String> m, B dest) {
    writeUnsignedShort(m.size(), dest);
    for (Map.Entry<String, String> entry : m.entrySet()) {
      writeString(entry.getKey(), dest);
      writeString(entry.getValue(), dest);
    }
  }

  default void writeStringMultimap(Map<String, List<String>> m, B dest) {
    writeUnsignedShort(m.size(), dest);
    for (Map.Entry<String, List<String>> entry : m.entrySet()) {
      writeString(entry.getKey(), dest);
      writeStringList(entry.getValue(), dest);
    }
  }

  default void writeBytesMap(Map<String, ByteBuffer> m, B dest) {
    writeUnsignedShort(m.size(), dest);
    for (Map.Entry<String, ByteBuffer> entry : m.entrySet()) {
      writeString(entry.getKey(), dest);
      writeBytes(entry.getValue(), dest);
    }
  }

  default void writeInet(InetSocketAddress address, B dest) {
    writeInetAddr(address.getAddress(), dest);
    writeInt(address.getPort(), dest);
  }
}
