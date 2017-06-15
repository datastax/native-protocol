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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

public class RowsMetadata {
  private enum Flag {
    GLOBAL_TABLES_SPEC(0x0001),
    HAS_MORE_PAGES(0x0002),
    NO_METADATA(0x0004);

    private final int mask;

    Flag(int mask) {
      this.mask = mask;
    }

    public static <B> EnumSet<Flag> decode(
        B source, PrimitiveCodec<B> decoder, int protocolVersion) {
      int bits = decoder.readInt(source);
      EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
      for (Flag flag : Flag.values()) {
        if ((bits & flag.mask) != 0) {
          set.add(flag);
        }
      }
      return set;
    }

    public static <B> void encode(
        EnumSet<Flag> flags, B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      int bits = 0;
      for (Flag flag : flags) {
        bits |= flag.mask;
      }
      encoder.writeInt(bits, dest);
    }

    public static int encodedSize(int protocolVersion) {
      return PrimitiveSizes.INT;
    }
  }

  /**
   * empty if and only if the SKIP_METADATA flag is present (there is no difference between skipping
   * the column specs, and having the specs but no columns in them)
   */
  public final List<ColumnSpec> columnSpecs;

  public final int columnCount;
  public final ByteBuffer pagingState;
  public final int[] pkIndices;

  private final EnumSet<Flag> flags;

  private RowsMetadata(
      EnumSet<Flag> flags,
      List<ColumnSpec> columnSpecs,
      int columnCount,
      ByteBuffer pagingState,
      int[] pkIndices) {
    this.columnSpecs = columnSpecs;
    this.columnCount = columnCount;
    this.pagingState = pagingState;
    this.pkIndices = pkIndices;
    this.flags = flags;
  }

  public RowsMetadata(
      List<ColumnSpec> columnSpecs, int columnCount, ByteBuffer pagingState, int[] pkIndices) {
    this(computeFlags(columnSpecs, pagingState), columnSpecs, columnCount, pagingState, pkIndices);
  }

  private static EnumSet<Flag> computeFlags(List<ColumnSpec> columnSpecs, ByteBuffer pagingState) {
    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    if (pagingState != null) {
      flags.add(Flag.HAS_MORE_PAGES);
    }
    if (columnSpecs.isEmpty()) {
      flags.add(Flag.NO_METADATA);
    } else if (haveSameTable(columnSpecs)) {
      flags.add(Flag.GLOBAL_TABLES_SPEC);
    }
    return flags;
  }

  public <B> void encode(
      B dest, PrimitiveCodec<B> encoder, boolean withPkIndices, int protocolVersion) {
    Flag.encode(flags, dest, encoder, protocolVersion);
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
    if (flags.contains(Flag.HAS_MORE_PAGES)) {
      encoder.writeBytes(pagingState, dest);
    }
    if (!flags.contains(Flag.NO_METADATA)) {
      assert !columnSpecs.isEmpty();
      boolean globalTable = flags.contains(Flag.GLOBAL_TABLES_SPEC);
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
    int size = Flag.encodedSize(protocolVersion);
    size += PrimitiveSizes.INT; // column count
    if (withPkIndices) {
      size += PrimitiveSizes.INT;
      if (pkIndices != null) {
        size += pkIndices.length * PrimitiveSizes.SHORT;
      }
    }
    if (flags.contains(Flag.HAS_MORE_PAGES)) {
      size += PrimitiveSizes.sizeOfBytes(pagingState);
    }
    if (!flags.contains(Flag.NO_METADATA)) {
      assert !columnSpecs.isEmpty();
      boolean globalTable = flags.contains(Flag.GLOBAL_TABLES_SPEC);
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
    EnumSet<Flag> flags = Flag.decode(source, decoder, protocolVersion);
    int columnCount = decoder.readInt(source);

    int[] pkIndices = null;
    int pkCount;
    if (withPkIndices && (pkCount = decoder.readInt(source)) > 0) {
      pkIndices = new int[pkCount];
      for (int i = 0; i < pkCount; i++) {
        pkIndices[i] = decoder.readUnsignedShort(source);
      }
    }

    ByteBuffer state = (flags.contains(Flag.HAS_MORE_PAGES)) ? decoder.readBytes(source) : null;

    List<ColumnSpec> columnSpecs;
    if (flags.contains(Flag.NO_METADATA)) {
      columnSpecs = Collections.emptyList();
    } else {
      boolean globalTablesSpec = flags.contains(Flag.GLOBAL_TABLES_SPEC);

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
    return new RowsMetadata(flags, columnSpecs, columnCount, state, pkIndices);
  }

  private static boolean haveSameTable(List<ColumnSpec> specs) {
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
