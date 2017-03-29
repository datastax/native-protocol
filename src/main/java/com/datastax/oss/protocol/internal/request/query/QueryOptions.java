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
package com.datastax.oss.protocol.internal.request.query;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolErrors;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class QueryOptions {

  public static final QueryOptions DEFAULT =
      new QueryOptions(
          ProtocolConstants.ConsistencyLevel.ONE,
          Collections.emptyList(),
          Collections.emptyMap(),
          false,
          -1,
          null,
          ProtocolConstants.ConsistencyLevel.SERIAL,
          Long.MIN_VALUE);

  private final EnumSet<QueryFlag> flags;
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

  private QueryOptions(
      EnumSet<QueryFlag> flags,
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

    this.flags = flags;
    this.consistency = consistency;
    this.positionalValues = positionalValues;
    this.namedValues = namedValues;
    this.skipMetadata = skipMetadata;
    this.pageSize = pageSize;
    this.pagingState = pagingState;
    this.serialConsistency = serialConsistency;
    this.defaultTimestamp = defaultTimestamp;
  }

  public QueryOptions(
      int consistency,
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp) {
    this(
        computeFlags(
            positionalValues,
            namedValues,
            skipMetadata,
            pageSize,
            pagingState,
            serialConsistency,
            defaultTimestamp),
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp);
  }

  private static EnumSet<QueryFlag> computeFlags(
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp) {
    EnumSet<QueryFlag> flags = EnumSet.noneOf(QueryFlag.class);
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
    return flags;
  }

  public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
    encoder.writeUnsignedShort(consistency, dest);
    QueryFlag.encode(this.flags, dest, encoder, protocolVersion);
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
    size += PrimitiveSizes.SHORT; // consistency level
    size += QueryFlag.encodedSize(protocolVersion); // flags
    if (flags.contains(QueryFlag.VALUES)) {
      if (flags.contains(QueryFlag.VALUE_NAMES)) {
        size += Values.sizeOfNamedValues(namedValues);
      } else {
        size += Values.sizeOfPositionalValues(positionalValues);
      }
    }
    if (flags.contains(QueryFlag.PAGE_SIZE)) {
      size += PrimitiveSizes.INT;
    }
    if (flags.contains(QueryFlag.PAGING_STATE)) {
      size += PrimitiveSizes.sizeOfBytes(pagingState);
    }
    if (flags.contains(QueryFlag.SERIAL_CONSISTENCY)) {
      size += PrimitiveSizes.SHORT;
    }
    if (flags.contains(QueryFlag.DEFAULT_TIMESTAMP)) {
      size += PrimitiveSizes.LONG;
    }
    return size;
  }

  public static <B> QueryOptions decode(B source, PrimitiveCodec<B> decoder, int protocolVersion) {
    int consistency = decoder.readUnsignedShort(source);
    EnumSet<QueryFlag> flags = QueryFlag.decode(source, decoder, protocolVersion);
    List<ByteBuffer> positionalValues = Collections.emptyList();
    Map<String, ByteBuffer> namedValues = Collections.emptyMap();
    if (flags.contains(QueryFlag.VALUES)) {
      if (flags.contains(QueryFlag.VALUE_NAMES)) {
        namedValues = Values.readNamedValues(source, decoder);
      } else {
        positionalValues = Values.readPositionalValues(source, decoder);
      }
    }

    boolean skipMetadata = flags.contains(QueryFlag.SKIP_METADATA);
    int pageSize = flags.contains(QueryFlag.PAGE_SIZE) ? decoder.readInt(source) : -1;
    ByteBuffer pagingState =
        flags.contains(QueryFlag.PAGING_STATE) ? decoder.readBytes(source) : null;
    int serialConsistency =
        flags.contains(QueryFlag.SERIAL_CONSISTENCY)
            ? decoder.readUnsignedShort(source)
            : ProtocolConstants.ConsistencyLevel.SERIAL;
    long defaultTimestamp =
        flags.contains(QueryFlag.DEFAULT_TIMESTAMP) ? decoder.readLong(source) : Long.MIN_VALUE;

    return new QueryOptions(
        flags,
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp);
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
