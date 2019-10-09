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
package com.datastax.dse.protocol.internal.response.result;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Flags;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DseRowsMetadata extends RowsMetadata {

  public final int continuousPageNumber;
  public final boolean isLastContinuousPage;

  /**
   * Builds a new instance with {@code NO_METADATA == false}; the column count is set to the number
   * of column specifications in the provided list.
   *
   * @param continuousPageNumber must be negative if this is not a continuous paging response.
   */
  public DseRowsMetadata(
      List<ColumnSpec> columnSpecs,
      ByteBuffer pagingState,
      int[] pkIndices,
      byte[] newResultMetadataId,
      int continuousPageNumber,
      boolean isLastContinuousPage) {
    this(
        computeFlags(
            false,
            columnSpecs,
            pagingState,
            newResultMetadataId,
            continuousPageNumber,
            isLastContinuousPage),
        columnSpecs,
        columnSpecs.size(),
        pagingState,
        pkIndices,
        newResultMetadataId,
        continuousPageNumber,
        isLastContinuousPage);
  }

  /**
   * Builds a new instance with {@code NO_METADATA == true}.
   *
   * @param continuousPageNumber must be negative if this is not a continuous paging response.
   */
  public DseRowsMetadata(
      int columnCount,
      ByteBuffer pagingState,
      int[] pkIndices,
      byte[] newResultMetadataId,
      int continuousPageNumber,
      boolean isLastContinuousPage) {
    this(
        computeFlags(
            true,
            Collections.emptyList(),
            pagingState,
            newResultMetadataId,
            continuousPageNumber,
            isLastContinuousPage),
        Collections.emptyList(),
        columnCount,
        pagingState,
        pkIndices,
        newResultMetadataId,
        continuousPageNumber,
        isLastContinuousPage);
  }

  protected DseRowsMetadata(
      int flags,
      List<ColumnSpec> columnSpecs,
      int columnCount,
      ByteBuffer pagingState,
      int[] pkIndices,
      byte[] newResultMetadataId,
      int continuousPageNumber,
      boolean isLastContinuousPage) {
    super(flags, columnSpecs, columnCount, pagingState, pkIndices, newResultMetadataId);
    this.continuousPageNumber = continuousPageNumber;
    this.isLastContinuousPage = isLastContinuousPage;
  }

  protected static int computeFlags(
      boolean noMetadata,
      List<ColumnSpec> columnSpecs,
      ByteBuffer pagingState,
      byte[] newResultMetadataId,
      int continuousPageNumber,
      boolean isLastContinuousPage) {
    int flags =
        RowsMetadata.computeFlags(noMetadata, columnSpecs, pagingState, newResultMetadataId);
    if (continuousPageNumber >= 0) {
      flags = Flags.add(flags, DseProtocolConstants.RowsFlag.CONTINUOUS_PAGING);
    }
    if (isLastContinuousPage) {
      flags = Flags.add(flags, DseProtocolConstants.RowsFlag.LAST_CONTINUOUS_PAGE);
    }
    return flags;
  }

  @Override
  public <B> void encode(
      B dest, PrimitiveCodec<B> encoder, boolean withPkIndices, int protocolVersion) {
    // The DSE-specific field `continuousPageNumber` is right in the middle of the payload, so we
    // unfortunately have to reimplement the whole method:
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
    if (Flags.contains(flags, DseProtocolConstants.RowsFlag.CONTINUOUS_PAGING)) {
      encoder.writeInt(continuousPageNumber, dest);
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

  @Override
  public int encodedSize(boolean withPkIndices, int protocolVersion) {
    return super.encodedSize(withPkIndices, protocolVersion)
        + (Flags.contains(flags, DseProtocolConstants.RowsFlag.CONTINUOUS_PAGING)
            ? PrimitiveSizes.INT
            : 0);
  }

  public static <B> DseRowsMetadata decode(
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

    int continuousPageNumber =
        (Flags.contains(flags, DseProtocolConstants.RowsFlag.CONTINUOUS_PAGING))
            ? decoder.readInt(source)
            : -1;

    boolean isLastContinuousPage =
        Flags.contains(flags, DseProtocolConstants.RowsFlag.LAST_CONTINUOUS_PAGE);

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
    return new DseRowsMetadata(
        flags,
        columnSpecs,
        columnCount,
        state,
        pkIndices,
        newResultMetadataId,
        continuousPageNumber,
        isLastContinuousPage);
  }
}
