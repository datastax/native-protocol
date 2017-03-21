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
package com.datastax.cassandra.protocol.internal;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Reads and writes the protocol's primitive types (as defined in section 3 of the protocol
 * specification) to a binary representation {@code B}.
 *
 * @param <B> the binary representation we're manipulating. This API is designed with a mutable
 *     implementation in mind (because that is what the core driver is using). If you want to use an
 *     immutable type, write a mutable wrapper for it.
 */
public abstract class PrimitiveCodec<B> {
  public abstract B allocate(int size);

  /**
   * "Releases" an element if the underlying implementation uses memory allocation. Otherwise, this
   * can simply be a no-op.
   */
  public abstract void release(B toRelease);

  public abstract int sizeOf(B toMeasure);

  public abstract B concat(B left, B right);

  public abstract byte readByte(B source);

  public abstract int readInt(B source);

  public abstract InetSocketAddress readInet(B source);

  public abstract long readLong(B source);

  public abstract int readUnsignedShort(B source);

  public abstract ByteBuffer readBytes(B source);

  public abstract byte[] readShortBytes(B source);

  public abstract String readString(B source);

  public abstract String readLongString(B source);

  public UUID readUuid(B source) {
    long msb = readLong(source);
    long lsb = readLong(source);
    return new UUID(msb, lsb);
  }

  public List<String> readStringList(B source) {
    int length = readUnsignedShort(source);
    List<String> l = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      l.add(readString(source));
    }
    return Collections.unmodifiableList(l);
  }

  public Map<String, String> readStringMap(B source) {
    int length = readUnsignedShort(source);
    Map<String, String> m = new HashMap<>(length);
    for (int i = 0; i < length; i++) {
      String k = readString(source);
      String v = readString(source);
      m.put(k, v);
    }
    return Collections.unmodifiableMap(m);
  }

  public Map<String, List<String>> readStringMultimap(B source) {
    Map<String, List<String>> m = new HashMap<>();
    int length = readUnsignedShort(source);
    for (int i = 0; i < length; i++) {
      String key = readString(source);
      List<String> value = readStringList(source);
      m.put(key, value);
    }
    return Collections.unmodifiableMap(m);
  }

  public Map<String, ByteBuffer> readBytesMap(B source) {
    int length = readUnsignedShort(source);
    Map<String, ByteBuffer> m = new HashMap<>(length * 2);
    for (int i = 0; i < length; i++) {
      String key = readString(source);
      ByteBuffer value = readBytes(source);
      m.put(key, value);
    }
    return Collections.unmodifiableMap(m);
  }

  public abstract void writeByte(byte b, B dest);

  public abstract void writeInt(int i, B dest);

  public abstract void writeInet(InetSocketAddress address, B dest);

  public abstract void writeLong(long l, B dest);

  public abstract void writeUnsignedShort(int i, B dest);

  public abstract void writeString(String s, B dest);

  public abstract void writeLongString(String s, B dest);

  public void writeUuid(UUID uuid, B dest) {
    writeLong(uuid.getMostSignificantBits(), dest);
    writeLong(uuid.getLeastSignificantBits(), dest);
  }

  public abstract void writeBytes(ByteBuffer bytes, B dest);

  public abstract void writeShortBytes(byte[] bytes, B dest);

  public void writeStringList(List<String> l, B dest) {
    writeUnsignedShort(l.size(), dest);
    for (String s : l) {
      writeString(s, dest);
    }
  }

  public void writeStringMap(Map<String, String> m, B dest) {
    writeUnsignedShort(m.size(), dest);
    for (Map.Entry<String, String> entry : m.entrySet()) {
      writeString(entry.getKey(), dest);
      writeString(entry.getValue(), dest);
    }
  }

  public void writeStringMultimap(Map<String, List<String>> m, B dest) {
    writeUnsignedShort(m.size(), dest);
    for (Map.Entry<String, List<String>> entry : m.entrySet()) {
      writeString(entry.getKey(), dest);
      writeStringList(entry.getValue(), dest);
    }
  }

  public void writeBytesMap(Map<String, ByteBuffer> m, B dest) {
    writeUnsignedShort(m.size(), dest);
    for (Map.Entry<String, ByteBuffer> entry : m.entrySet()) {
      writeString(entry.getKey(), dest);
      writeBytes(entry.getValue(), dest);
    }
  }
}
