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
package com.datastax.cassandra.protocol.internal.request.query;

import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.ProtocolErrors;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class QueryOptions {

  public static final QueryOptions DEFAULT =
      new QueryOptions(
          ProtocolConstants.Opcode.QUERY,
          ProtocolConstants.ConsistencyLevel.ONE,
          Collections.emptyList(),
          Collections.emptyMap(),
          false,
          -1,
          null,
          ProtocolConstants.ConsistencyLevel.SERIAL,
          Long.MIN_VALUE);

  private final EnumSet<QueryFlag> flags = EnumSet.noneOf(QueryFlag.class);
  public final int requestType;
  /** @see ProtocolConstants.ConsistencyLevel */
  public final int consistency;

  public final List<ByteBuffer> positionalValues;
  public final Map<String, ByteBuffer> namedValues;
  public final boolean skipMetadata;
  public final int pageSize;
  public final ByteBuffer pagingState;
  /** @see ProtocolConstants.ConsistencyLevel */
  public final int serialConsistency;

  public final long defaultTimestamp;

  public QueryOptions(
      int requestType,
      int consistency,
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp) {

    ProtocolErrors.check(
        positionalValues.isEmpty() || namedValues.isEmpty(),
        "Can't have both positional and named values");

    this.requestType = requestType;
    this.consistency = consistency;
    this.positionalValues = positionalValues;
    this.namedValues = namedValues;
    this.skipMetadata = skipMetadata;
    this.pageSize = pageSize;
    this.pagingState = pagingState;
    this.serialConsistency = serialConsistency;
    this.defaultTimestamp = defaultTimestamp;

    // Populate flags
    if (!positionalValues.isEmpty()) {
      flags.add(QueryFlag.VALUES);
    }
    if (!namedValues.isEmpty()) {
      flags.add(QueryFlag.VALUES);
      flags.add(QueryFlag.VALUE_NAMES);
    }
    if (skipMetadata) {
      flags.add(QueryFlag.SKIP_METADATA);
    }
    if (pageSize >= 0) {
      flags.add(QueryFlag.PAGE_SIZE);
    }
    if (pagingState != null) {
      flags.add(QueryFlag.PAGING_STATE);
    }
    if (serialConsistency != ProtocolConstants.ConsistencyLevel.SERIAL) {
      flags.add(QueryFlag.SERIAL_CONSISTENCY);
    }
    if (defaultTimestamp != Long.MIN_VALUE) {
      flags.add(QueryFlag.DEFAULT_TIMESTAMP);
    }
  }

  public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
    encoder.writeUnsignedShort(consistency, dest);
    encoder.writeByte((byte) QueryFlag.serialize(flags, protocolVersion), dest);
    if (flags.contains(QueryFlag.VALUES)) {
      if (flags.contains(QueryFlag.VALUE_NAMES)) {
        Values.writeNamedValues(namedValues, dest, encoder);
      } else {
        Values.writePositionalValues(positionalValues, dest, encoder);
      }
    }
    if (flags.contains(QueryFlag.PAGE_SIZE)) {
      encoder.writeInt(pageSize, dest);
    }
    if (flags.contains(QueryFlag.PAGING_STATE)) {
      encoder.writeBytes(pagingState, dest);
    }
    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY)) {
      encoder.writeUnsignedShort(serialConsistency, dest);
    }
    if (flags.contains(QueryFlag.DEFAULT_TIMESTAMP)) {
      encoder.writeLong(defaultTimestamp, dest);
    }
  }

  public int encodedSize(int protocolVersion) {
    int size = 0;
    size += 2; // consistency level
    size += QueryFlag.serializedSize(protocolVersion); // flags
    if (flags.contains(QueryFlag.VALUES)) {
      if (flags.contains(QueryFlag.VALUE_NAMES)) {
        size += Values.sizeOfNamedValues(namedValues);
      } else {
        size += Values.sizeOfPositionalValues(positionalValues);
      }
    }
    if (flags.contains(QueryFlag.PAGE_SIZE)) {
      size += 4;
    }
    if (flags.contains(QueryFlag.PAGING_STATE)) {
      size += PrimitiveSizes.sizeOfBytes(pagingState);
    }
    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY)) {
      size += 2;
    }
    if (flags.contains(QueryFlag.DEFAULT_TIMESTAMP)) {
      size += 8;
    }
    return size;
  }

  @Override
  public String toString() {
    return String.format(
        "[cl=%s, positionalVals=%s, namedVals=%s, skip=%b, psize=%d, state=%s, serialCl=%s]",
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency);
  }
}
