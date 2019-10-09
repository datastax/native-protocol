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
package com.datastax.dse.protocol.internal.request;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolErrors;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.query.Values;
import com.datastax.oss.protocol.internal.util.Flags;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DseBatchCodec extends Message.Codec {

  public DseBatchCodec(int protocolVersion) {
    super(ProtocolConstants.Opcode.BATCH, protocolVersion);
    assert protocolVersion >= DSE_V1;
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
    encoder.writeInt(batch.flags, dest); // Flags are always ints in DSE versions
    if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)) {
      encoder.writeUnsignedShort(batch.serialConsistency, dest);
    }
    if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)) {
      encoder.writeLong(batch.defaultTimestamp, dest);
    }
    if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)) {
      encoder.writeString(batch.keyspace, dest);
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
    size +=
        PrimitiveSizes.SHORT // consistency level
            + PrimitiveSizes.INT; // flags
    if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)) {
      size += PrimitiveSizes.SHORT;
    }
    if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)) {
      size += PrimitiveSizes.LONG;
    }
    if (Flags.contains(batch.flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)) {
      size += PrimitiveSizes.sizeOfString(batch.keyspace);
    }
    return size;
  }

  @Override
  public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
    byte type = decoder.readByte(source);
    int queryCount = decoder.readUnsignedShort(source);
    List<Object> queriesOrIds = new ArrayList<>();
    List<List<ByteBuffer>> values = new ArrayList<>();
    for (int i = 0; i < queryCount; i++) {
      boolean isQueryString = (decoder.readByte(source) == 0);
      queriesOrIds.add(
          isQueryString ? decoder.readLongString(source) : decoder.readShortBytes(source));
      values.add(Values.readPositionalValues(source, decoder));
    }
    int consistency = decoder.readUnsignedShort(source);
    int flags = decoder.readInt(source);
    int serialConsistency =
        (Flags.contains(flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY))
            ? decoder.readUnsignedShort(source)
            : ProtocolConstants.ConsistencyLevel.SERIAL;
    long defaultTimestamp =
        (Flags.contains(flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP))
            ? decoder.readLong(source)
            : Long.MIN_VALUE;
    String keyspace =
        (Flags.contains(flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE))
            ? decoder.readString(source)
            : null;

    return new Batch(
        flags,
        type,
        queriesOrIds,
        values,
        consistency,
        serialConsistency,
        defaultTimestamp,
        keyspace);
  }
}
