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
package com.datastax.oss.protocol.internal.response.result;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Flags;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class RowsMetadata {
  /**
   * empty if and only if the SKIP_METADATA flag is present (there is no difference between skipping
   * the column specs, and having the specs but no columns in them)
   */
  public final List<ColumnSpec> columnSpecs;

  public final int columnCount;
  public final ByteBuffer pagingState;
  public final int[] pkIndices;
  public final byte[] newResultMetadataId;

  public final int flags;

  /**
   * Builds a new instance with {@code NO_METADATA == false}; the column count is set to the number
   * of column specifications in the provided list.
   */
  public RowsMetadata(
      List<ColumnSpec> columnSpecs,
      ByteBuffer pagingState,
      int[] pkIndices,
      byte[] newResultMetadataId) {
    this(
        computeFlags(false, columnSpecs, pagingState, newResultMetadataId),
        columnSpecs,
        columnSpecs.size(),
        pagingState,
        pkIndices,
        newResultMetadataId);
  }

  /** Builds a new instance with {@code NO_METADATA == true}. */
  public RowsMetadata(
      int columnCount, ByteBuffer pagingState, int[] pkIndices, byte[] newResultMetadataId) {
    this(
        computeFlags(true, Collections.emptyList(), pagingState, newResultMetadataId),
        Collections.emptyList(),
        columnCount,
        pagingState,
        pkIndices,
        newResultMetadataId);
  }

  /**
   * This constructor should only be used in message codecs. To build an outgoing message from
   * client code, use {@link #RowsMetadata(int, ByteBuffer, int[], byte[])} or {@link
   * #RowsMetadata(List, ByteBuffer, int[], byte[])} so that the flags are computed automatically.
   */
  public RowsMetadata(
      int flags,
      List<ColumnSpec> columnSpecs,
      int columnCount,
      ByteBuffer pagingState,
      int[] pkIndices,
      byte[] newResultMetadataId) {
    this.columnSpecs = columnSpecs;
    this.columnCount = columnCount;
    this.pagingState = pagingState;
    this.pkIndices = pkIndices;
    this.newResultMetadataId = newResultMetadataId;
    this.flags = flags;
  }

  protected static int computeFlags(
      boolean noMetadata,
      List<ColumnSpec> columnSpecs,
      ByteBuffer pagingState,
      byte[] newResultMetadataId) {
    int flags = 0;
    if (noMetadata) {
      flags = Flags.add(flags, ProtocolConstants.RowsFlag.NO_METADATA);
    } else if (haveSameTable(columnSpecs)) {
      flags = Flags.add(flags, ProtocolConstants.RowsFlag.GLOBAL_TABLES_SPEC);
    }
    if (pagingState != null) {
      flags = Flags.add(flags, ProtocolConstants.RowsFlag.HAS_MORE_PAGES);
    }
    if (newResultMetadataId != null) {
      flags = Flags.add(flags, ProtocolConstants.RowsFlag.METADATA_CHANGED);
    }
    return flags;
  }

  public <B> void encode(
      B dest, PrimitiveCodec<B> encoder, boolean withPkIndices, int protocolVersion) {
    encoder.writeInt(flags, dest);
    encoder.writeInt(columnCount, dest);
    if (withPkIndices) {
      if (pkIndices == null) {
        encoder.writeInt(0, dest);
      } else {
        encoder.writeInt(pkIndices.length, dest);
        for (int pkIndex : pkIndices) {
          encoder.writeUnsignedShort(pkIndex, dest);
        }
      }
    }
    if (Flags.contains(flags, ProtocolConstants.RowsFlag.HAS_MORE_PAGES)) {
      encoder.writeBytes(pagingState, dest);
    }
    if (Flags.contains(flags, ProtocolConstants.RowsFlag.METADATA_CHANGED)) {
      encoder.writeShortBytes(newResultMetadataId, dest);
    }
    if (!Flags.contains(flags, ProtocolConstants.RowsFlag.NO_METADATA) && !columnSpecs.isEmpty()) {
      boolean globalTable = Flags.contains(flags, ProtocolConstants.RowsFlag.GLOBAL_TABLES_SPEC);
      if (globalTable) {
        ColumnSpec firstSpec = columnSpecs.get(0);
        encoder.writeString(firstSpec.ksName, dest);
        encoder.writeString(firstSpec.tableName, dest);
      }
      for (ColumnSpec spec : columnSpecs) {
        if (!globalTable) {
          encoder.writeString(spec.ksName, dest);
          encoder.writeString(spec.tableName, dest);
        }
        encoder.writeString(spec.name, dest);
        spec.type.encode(dest, encoder, protocolVersion);
      }
    }
  }

  public int encodedSize(boolean withPkIndices, int protocolVersion) {
    int size = PrimitiveSizes.INT; // flags
    size += PrimitiveSizes.INT; // column count
    if (Flags.contains(flags, ProtocolConstants.RowsFlag.METADATA_CHANGED)) {
      size += PrimitiveSizes.sizeOfShortBytes(newResultMetadataId);
    }
    if (withPkIndices) {
      size += PrimitiveSizes.INT;
      if (pkIndices != null) {
        size += pkIndices.length * PrimitiveSizes.SHORT;
      }
    }
    if (Flags.contains(flags, ProtocolConstants.RowsFlag.HAS_MORE_PAGES)) {
      size += PrimitiveSizes.sizeOfBytes(pagingState);
    }
    if (!Flags.contains(flags, ProtocolConstants.RowsFlag.NO_METADATA) && !columnSpecs.isEmpty()) {
      boolean globalTable = Flags.contains(flags, ProtocolConstants.RowsFlag.GLOBAL_TABLES_SPEC);
      if (globalTable) {
        ColumnSpec firstSpec = columnSpecs.get(0);
        size += PrimitiveSizes.sizeOfString(firstSpec.ksName);
        size += PrimitiveSizes.sizeOfString(firstSpec.tableName);
      }
      for (ColumnSpec spec : columnSpecs) {
        if (!globalTable) {
          size += PrimitiveSizes.sizeOfString(spec.ksName);
          size += PrimitiveSizes.sizeOfString(spec.tableName);
        }
        size += PrimitiveSizes.sizeOfString(spec.name);
        size += spec.type.encodedSize(protocolVersion);
      }
    }
    return size;
  }

  public static <B> RowsMetadata decode(
      B source, PrimitiveCodec<B> decoder, boolean withPkIndices, int protocolVersion) {
    int flags = decoder.readInt(source);
    int columnCount = decoder.readInt(source);

    int[] pkIndices = null;
    int pkCount;
    if (withPkIndices && (pkCount = decoder.readInt(source)) > 0) {
      pkIndices = new int[pkCount];
      for (int i = 0; i < pkCount; i++) {
        pkIndices[i] = decoder.readUnsignedShort(source);
      }
    }

    ByteBuffer state =
        (Flags.contains(flags, ProtocolConstants.RowsFlag.HAS_MORE_PAGES))
            ? decoder.readBytes(source)
            : null;

    byte[] newResultMetadataId =
        (Flags.contains(flags, ProtocolConstants.RowsFlag.METADATA_CHANGED))
            ? decoder.readShortBytes(source)
            : null;

    List<ColumnSpec> columnSpecs;
    if (Flags.contains(flags, ProtocolConstants.RowsFlag.NO_METADATA)) {
      columnSpecs = Collections.emptyList();
    } else {
      boolean globalTablesSpec =
          Flags.contains(flags, ProtocolConstants.RowsFlag.GLOBAL_TABLES_SPEC);

      String globalKsName = null;
      String globalCfName = null;
      if (globalTablesSpec) {
        globalKsName = decoder.readString(source);
        globalCfName = decoder.readString(source);
      }
      List<ColumnSpec> tmpSpecs = new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount; i++) {
        String ksName = globalTablesSpec ? globalKsName : decoder.readString(source);
        String cfName = globalTablesSpec ? globalCfName : decoder.readString(source);
        String name = decoder.readString(source);
        RawType type = RawType.decode(source, decoder, protocolVersion);
        tmpSpecs.add(new ColumnSpec(ksName, cfName, name, i, type));
      }
      columnSpecs = Collections.unmodifiableList(tmpSpecs);
    }
    return new RowsMetadata(flags, columnSpecs, columnCount, state, pkIndices, newResultMetadataId);
  }

  private static boolean haveSameTable(List<ColumnSpec> specs) {
    if (specs.isEmpty()) {
      return false;
    }
    boolean first = true;
    String ksName = null;
    String tableName = null;
    for (ColumnSpec spec : specs) {
      if (first) {
        first = false;
        ksName = spec.ksName;
        tableName = spec.tableName;
      } else if (!Objects.equals(spec.ksName, ksName)
          || !Objects.equals(spec.tableName, tableName)) {
        return false;
      }
    }
    return true;
  }
}
