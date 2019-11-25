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
package com.datastax.oss.protocol.internal.request.query;

import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V5;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolErrors;
import com.datastax.oss.protocol.internal.util.Flags;
import java.nio.ByteBuffer;
import java.util.Collections;
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
          Long.MIN_VALUE,
          null);

  public final int flags;
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
  public final String keyspace;

  /**
   * This constructor should only be used in message codecs. To build an outgoing message from
   * client code, use {@link #QueryOptions(int, List, Map, boolean, int, ByteBuffer, int, long,
   * String)} so that the flags are computed automatically.
   */
  public QueryOptions(
      int flags,
      int consistency,
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace) {

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
    this.keyspace = keyspace;
  }

  public QueryOptions(
      int consistency,
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace) {
    this(
        computeFlags(
            positionalValues,
            namedValues,
            skipMetadata,
            pageSize,
            pagingState,
            serialConsistency,
            defaultTimestamp,
            keyspace),
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp,
        keyspace);
  }

  protected static int computeFlags(
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace) {
    int flags = 0;
    if (!positionalValues.isEmpty()) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.VALUES);
    }
    if (!namedValues.isEmpty()) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.VALUES);
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.VALUE_NAMES);
    }
    if (skipMetadata) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.SKIP_METADATA);
    }
    if (pageSize > 0) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.PAGE_SIZE);
    }
    if (pagingState != null) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.PAGING_STATE);
    }
    if (serialConsistency != ProtocolConstants.ConsistencyLevel.SERIAL) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY);
    }
    if (defaultTimestamp != Long.MIN_VALUE) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP);
    }
    if (keyspace != null) {
      flags = Flags.add(flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE);
    }
    return flags;
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

  public static class Codec {
    /** @see ProtocolConstants.Version */
    public final int protocolVersion;

    public Codec(int protocolVersion) {
      this.protocolVersion = protocolVersion;
    }

    public <B> void encode(B dest, QueryOptions options, PrimitiveCodec<B> encoder) {
      encoder.writeUnsignedShort(options.consistency, dest);
      if (protocolVersion >= V5) {
        encoder.writeInt(options.flags, dest);
      } else {
        encoder.writeByte((byte) options.flags, dest);
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.VALUES)) {
        if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.VALUE_NAMES)) {
          Values.writeNamedValues(options.namedValues, dest, encoder);
        } else {
          Values.writePositionalValues(options.positionalValues, dest, encoder);
        }
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.PAGE_SIZE)) {
        encoder.writeInt(options.pageSize, dest);
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.PAGING_STATE)) {
        encoder.writeBytes(options.pagingState, dest);
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)) {
        encoder.writeUnsignedShort(options.serialConsistency, dest);
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)) {
        encoder.writeLong(options.defaultTimestamp, dest);
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)) {
        encoder.writeString(options.keyspace, dest);
      }
    }

    public int encodedSize(QueryOptions options) {
      int size = 0;
      size += PrimitiveSizes.SHORT; // consistency level
      size += queryFlagsSize(protocolVersion);
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.VALUES)) {
        if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.VALUE_NAMES)) {
          size += Values.sizeOfNamedValues(options.namedValues);
        } else {
          size += Values.sizeOfPositionalValues(options.positionalValues);
        }
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.PAGE_SIZE)) {
        size += PrimitiveSizes.INT;
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.PAGING_STATE)) {
        size += PrimitiveSizes.sizeOfBytes(options.pagingState);
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)) {
        size += PrimitiveSizes.SHORT;
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)) {
        size += PrimitiveSizes.LONG;
      }
      if (Flags.contains(options.flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)) {
        size += PrimitiveSizes.sizeOfString(options.keyspace);
      }
      return size;
    }

    public <B> QueryOptions decode(B source, PrimitiveCodec<B> decoder) {
      int consistency = decoder.readUnsignedShort(source);
      int flags =
          (protocolVersion >= ProtocolConstants.Version.V5)
              ? decoder.readInt(source)
              : decoder.readByte(source);
      List<ByteBuffer> positionalValues = Collections.emptyList();
      Map<String, ByteBuffer> namedValues = Collections.emptyMap();
      if (Flags.contains(flags, ProtocolConstants.QueryFlag.VALUES)) {
        if (Flags.contains(flags, ProtocolConstants.QueryFlag.VALUE_NAMES)) {
          namedValues = Values.readNamedValues(source, decoder);
        } else {
          positionalValues = Values.readPositionalValues(source, decoder);
        }
      }

      boolean skipMetadata = Flags.contains(flags, ProtocolConstants.QueryFlag.SKIP_METADATA);
      int pageSize =
          Flags.contains(flags, ProtocolConstants.QueryFlag.PAGE_SIZE)
              ? decoder.readInt(source)
              : -1;
      ByteBuffer pagingState =
          Flags.contains(flags, ProtocolConstants.QueryFlag.PAGING_STATE)
              ? decoder.readBytes(source)
              : null;
      int serialConsistency =
          Flags.contains(flags, ProtocolConstants.QueryFlag.SERIAL_CONSISTENCY)
              ? decoder.readUnsignedShort(source)
              : ProtocolConstants.ConsistencyLevel.SERIAL;
      long defaultTimestamp =
          Flags.contains(flags, ProtocolConstants.QueryFlag.DEFAULT_TIMESTAMP)
              ? decoder.readLong(source)
              : Long.MIN_VALUE;
      String keyspace =
          Flags.contains(flags, ProtocolConstants.QueryFlag.WITH_KEYSPACE)
              ? decoder.readString(source)
              : null;

      return new QueryOptions(
          flags,
          consistency,
          positionalValues,
          namedValues,
          skipMetadata,
          pageSize,
          pagingState,
          serialConsistency,
          defaultTimestamp,
          keyspace);
    }
  }

  public static int queryFlagsSize(int protocolVersion) {
    return protocolVersion >= ProtocolConstants.Version.V5
        ? PrimitiveSizes.INT
        : PrimitiveSizes.BYTE;
  }
}
