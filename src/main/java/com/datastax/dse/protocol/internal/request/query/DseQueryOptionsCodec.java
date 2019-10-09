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
package com.datastax.dse.protocol.internal.request.query;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;
import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V2;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.request.query.Values;
import com.datastax.oss.protocol.internal.util.Flags;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DseQueryOptionsCodec extends QueryOptions.Codec {

  public DseQueryOptionsCodec(int protocolVersion) {
    super(protocolVersion);
    assert protocolVersion >= DSE_V1;
  }

  @Override
  public <B> void encode(B dest, QueryOptions options, PrimitiveCodec<B> encoder) {
    encoder.writeUnsignedShort(options.consistency, dest);
    encoder.writeInt(options.flags, dest); // Flags are always ints in DSE versions

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

    if (options instanceof DseQueryOptions) {
      ContinuousPagingOptions continuousPagingOptions =
          ((DseQueryOptions) options).continuousPagingOptions;
      if (continuousPagingOptions != null) {
        encoder.writeInt(continuousPagingOptions.maxPages, dest);
        encoder.writeInt(continuousPagingOptions.pagesPerSecond, dest);
        if (protocolVersion >= DSE_V2) {
          encoder.writeInt(continuousPagingOptions.nextPages, dest);
        }
      }
    }
  }

  @Override
  public int encodedSize(QueryOptions options) {
    int size =
        PrimitiveSizes.SHORT // consistency level
            + PrimitiveSizes.INT; // flags
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

    if (options instanceof DseQueryOptions) {
      ContinuousPagingOptions continuousPagingOptions =
          ((DseQueryOptions) options).continuousPagingOptions;
      if (continuousPagingOptions != null) {
        size += PrimitiveSizes.INT * (protocolVersion >= DSE_V2 ? 3 : 2);
      }
    }
    return size;
  }

  @Override
  public <B> QueryOptions decode(B source, PrimitiveCodec<B> decoder) {
    // Common content between OSS and DSE (copy-pasted from QueryOptions):
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
        Flags.contains(flags, ProtocolConstants.QueryFlag.PAGE_SIZE) ? decoder.readInt(source) : -1;
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

    // DSE-specific content:
    boolean isPageSizeInBytes =
        Flags.contains(flags, DseProtocolConstants.QueryFlag.PAGE_SIZE_BYTES);
    ContinuousPagingOptions continuousPagingOptions = null;
    if (Flags.contains(flags, DseProtocolConstants.QueryFlag.CONTINUOUS_PAGING)) {
      int maxPages = decoder.readInt(source);
      int pagesPerSecond = decoder.readInt(source);
      int nextPages = (protocolVersion >= DSE_V2) ? decoder.readInt(source) : -1;
      continuousPagingOptions = new ContinuousPagingOptions(maxPages, pagesPerSecond, nextPages);
    }

    return new DseQueryOptions(
        flags,
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp,
        keyspace,
        isPageSizeInBytes,
        continuousPagingOptions);
  }
}
