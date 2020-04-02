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
package com.datastax.oss.protocol.internal.request;

import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V5;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.request.query.Values;
import com.datastax.oss.protocol.internal.util.Flags;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import java.nio.ByteBuffer;
import java.util.List;

public class Batch extends Message {
  /** @see ProtocolConstants.BatchType */
  public final byte type;

  /** String or byte[] */
  public final List<Object> queriesOrIds;

  public final List<List<ByteBuffer>> values;

  public final int consistency;
  public final int serialConsistency;
  public final long defaultTimestamp;
  public final String keyspace;
  public final int nowInSeconds;

  public final int flags;

  /**
   * This constructor should only be used in message codecs. To build an outgoing message from
   * client code, use {@link #Batch(byte, List, List, int, int, long, String, int)} so that the
   * flags are computed automatically.
   */
  public Batch(
      int flags,
      byte type,
      List<Object> queriesOrIds,
      List<List<ByteBuffer>> values,
      int consistency,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace,
      int nowInSeconds) {
    super(false, ProtocolConstants.Opcode.BATCH);
    this.type = type;
    this.queriesOrIds = queriesOrIds;
    this.values = values;
    this.consistency = consistency;
    this.serialConsistency = serialConsistency;
    this.defaultTimestamp = defaultTimestamp;
    this.keyspace = keyspace;
    this.nowInSeconds = nowInSeconds;
    this.flags = flags;
  }

  public Batch(
      byte type,
      List<Object> queriesOrIds,
      List<List<ByteBuffer>> values,
      int consistency,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace,
      int nowInSeconds) {
    this(
        computeFlags(serialConsistency, defaultTimestamp, keyspace, nowInSeconds),
        type,
        queriesOrIds,
        values,
        consistency,
        serialConsistency,
        defaultTimestamp,
        keyspace,
        nowInSeconds);
  }

  @Override
  public String toString() {
    return "BATCH(" + queriesOrIds.size() + " statements)";
  }

  protected static int computeFlags(
      int serialConsistency, long defaultTimestamp, String keyspace, int nowInSeconds) {
    int flags = 0;
    if (serialConsistency != ProtocolConstants.ConsistencyLevel.SERIAL) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY);
    }
    if (defaultTimestamp != QueryOptions.NO_DEFAULT_TIMESTAMP) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP);
    }
    if (keyspace != null) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE);
    }
    if (nowInSeconds != QueryOptions.NO_NOW_IN_SECONDS) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.NOW_IN_SECONDS);
    }
    return flags;
  }

  public static class Codec extends Message.Codec {
    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.BATCH, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Batch batch = (Batch) message;
      encoder.writeByte(batch.type, dest);
      int queryCount = batch.queriesOrIds.size();
      encoder.writeUnsignedShort(queryCount, dest);
      for (int i = 0; i < queryCount; i++) {
        Object q = batch.queriesOrIds.get(i);
        if (q instanceof String) {
          encoder.writeByte((byte) 0, dest);
          encoder.writeLongString((String) q, dest);
        } else {
          encoder.writeByte((byte) 1, dest);
          encoder.writeShortBytes((byte[]) q, dest);
        }
        Values.writePositionalValues(batch.values.get(i), dest, encoder);
      }

      encoder.writeUnsignedShort(batch.consistency, dest);

      if (protocolVersion >= V5) {
        encoder.writeInt(batch.flags, dest);
      } else {
        encoder.writeByte((byte) batch.flags, dest);
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)) {
        encoder.writeUnsignedShort(batch.serialConsistency, dest);
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)) {
        encoder.writeLong(batch.defaultTimestamp, dest);
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)) {
        encoder.writeString(batch.keyspace, dest);
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.NOW_IN_SECONDS)) {
        encoder.writeInt(batch.nowInSeconds, dest);
      }
    }

    @Override
    public int encodedSize(Message message) {
      Batch batch = (Batch) message;
      int size = PrimitiveSizes.BYTE; // type
      size += PrimitiveSizes.SHORT; // number of queries

      int queryCount = batch.queriesOrIds.size();
      ProtocolErrors.check(
          queryCount <= 0xFFFF, "Batch messages can contain at most %d queries", 0xFFFF);
      ProtocolErrors.check(
          batch.values.size() == queryCount,
          "Batch contains %d queries but %d value lists",
          queryCount,
          batch.values.size());

      for (int i = 0; i < queryCount; i++) {
        Object q = batch.queriesOrIds.get(i);
        size +=
            PrimitiveSizes.BYTE
                + (q instanceof String
                    ? PrimitiveSizes.sizeOfLongString((String) q)
                    : PrimitiveSizes.sizeOfShortBytes((byte[]) q));

        size += Values.sizeOfPositionalValues(batch.values.get(i));
      }
      size += PrimitiveSizes.SHORT; // consistency level
      size += (protocolVersion >= V5) ? PrimitiveSizes.INT : PrimitiveSizes.BYTE; // flags
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)) {
        size += PrimitiveSizes.SHORT;
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)) {
        size += PrimitiveSizes.LONG;
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)) {
        size += PrimitiveSizes.sizeOfString(batch.keyspace);
      }
      if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.NOW_IN_SECONDS)) {
        size += PrimitiveSizes.INT;
      }
      return size;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      byte type = decoder.readByte(source);
      int queryCount = decoder.readUnsignedShort(source);
      NullAllowingImmutableList.Builder<Object> queriesOrIds =
          NullAllowingImmutableList.builder(queryCount);
      NullAllowingImmutableList.Builder<List<ByteBuffer>> values =
          NullAllowingImmutableList.builder(queryCount);
      for (int i = 0; i < queryCount; i++) {
        boolean isQueryString = (decoder.readByte(source) == 0);
        queriesOrIds.add(
            isQueryString ? decoder.readLongString(source) : decoder.readShortBytes(source));
        values.add(Values.readPositionalValues(source, decoder));
      }
      int consistency = decoder.readUnsignedShort(source);
      int flags =
          (protocolVersion >= ProtocolConstants.Version.V5)
              ? decoder.readInt(source)
              : decoder.readByte(source);
      int serialConsistency =
          (Flags.contains(flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY))
              ? decoder.readUnsignedShort(source)
              : ProtocolConstants.ConsistencyLevel.SERIAL;
      long defaultTimestamp =
          (Flags.contains(flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP))
              ? decoder.readLong(source)
              : QueryOptions.NO_DEFAULT_TIMESTAMP;
      String keyspace =
          (Flags.contains(flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE))
              ? decoder.readString(source)
              : null;
      int nowInSeconds =
          Flags.contains(flags, ProtocolConstants.QueryFlag.NOW_IN_SECONDS)
              ? decoder.readInt(source)
              : QueryOptions.NO_NOW_IN_SECONDS;

      return new Batch(
          flags,
          type,
          queriesOrIds.build(),
          values.build(),
          consistency,
          serialConsistency,
          defaultTimestamp,
          keyspace,
          nowInSeconds);
    }
  }
}
