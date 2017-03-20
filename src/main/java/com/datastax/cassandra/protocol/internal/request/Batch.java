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
package com.datastax.cassandra.protocol.internal.request;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.ProtocolErrors;
import com.datastax.cassandra.protocol.internal.request.query.QueryFlag;
import com.datastax.cassandra.protocol.internal.request.query.Values;
import java.nio.ByteBuffer;
import java.util.EnumSet;
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

  private final EnumSet<QueryFlag> flags = EnumSet.noneOf(QueryFlag.class);

  public Batch(
      byte type,
      List<Object> queriesOrIds,
      List<List<ByteBuffer>> values,
      int consistency,
      int serialConsistency,
      long defaultTimestamp) {
    super(false, ProtocolConstants.Opcode.BATCH);
    this.type = type;
    this.queriesOrIds = queriesOrIds;
    this.values = values;
    this.consistency = consistency;
    this.serialConsistency = serialConsistency;
    this.defaultTimestamp = defaultTimestamp;

    if (serialConsistency != ProtocolConstants.ConsistencyLevel.SERIAL) {
      flags.add(QueryFlag.SERIAL_CONSISTENCY);
    }
    if (defaultTimestamp != Long.MIN_VALUE) {
      flags.add(QueryFlag.DEFAULT_TIMESTAMP);
    }
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

      encoder.writeByte((byte) QueryFlag.serialize(batch.flags, protocolVersion), dest);
      if (batch.flags.contains(QueryFlag.SERIAL_CONSISTENCY)) {
        encoder.writeUnsignedShort(batch.serialConsistency, dest);
      }
      if (batch.flags.contains(QueryFlag.DEFAULT_TIMESTAMP)) {
        encoder.writeLong(batch.defaultTimestamp, dest);
      }
    }

    @Override
    public int encodedSize(Message message) {
      Batch batch = (Batch) message;
      int size = 1 + 2; // type + number of queries

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
            1
                + (q instanceof String
                    ? PrimitiveSizes.sizeOfLongString((String) q)
                    : PrimitiveSizes.sizeOfShortBytes((byte[]) q));

        size += Values.sizeOfPositionalValues(batch.values.get(i));
      }
      size += 2; // consistency level
      size += QueryFlag.serializedSize(protocolVersion);
      if (batch.flags.contains(QueryFlag.SERIAL_CONSISTENCY)) {
        size += 2;
      }
      if (batch.flags.contains(QueryFlag.DEFAULT_TIMESTAMP)) {
        size += 8;
      }
      return size;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      throw new UnsupportedOperationException("TODO");
    }
  }
}
