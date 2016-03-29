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
package com.datastax.cassandra.protocol.internal.response.result;

import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

public class RowsMetadata {
  private enum Flag {
    // The order of that enum matters!!
    GLOBAL_TABLES_SPEC,
    HAS_MORE_PAGES,
    NO_METADATA;

    static EnumSet<Flag> deserialize(int flags) {
      EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
      Flag[] values = Flag.values();
      for (int n = 0; n < values.length; n++) {
        if ((flags & (1 << n)) != 0) set.add(values[n]);
      }
      return set;
    }

    static int serialize(EnumSet<Flag> flags) {
      int i = 0;
      for (Flag flag : flags) i |= 1 << flag.ordinal();
      return i;
    }
  }

  public final int columnCount;
  public final List<ColumnSpec> columnSpecs;
  public final ByteBuffer pagingState;
  public final int[] pkIndices;

  public RowsMetadata(
      int columnCount, List<ColumnSpec> columnSpecs, ByteBuffer pagingState, int[] pkIndices) {
    this.columnCount = columnCount;
    this.columnSpecs = columnSpecs;
    this.pagingState = pagingState;
    this.pkIndices = pkIndices;
  }

  private static <B> RowsMetadata decode(
      B source, PrimitiveCodec<B> decoder, boolean withPkIndices) {
    EnumSet<Flag> flags = Flag.deserialize(decoder.readInt(source));
    int columnCount = decoder.readInt(source);

    int[] pkIndices = null;
    int pkCount;
    if (withPkIndices && (pkCount = decoder.readInt(source)) > 0) {
      pkIndices = new int[pkCount];
      for (int i = 0; i < pkCount; i++) pkIndices[i] = decoder.readUnsignedShort(source);
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
        RawType type = RawType.decode(source, decoder);
        tmpSpecs.add(new ColumnSpec(ksName, cfName, name, type));
      }
      columnSpecs = Collections.unmodifiableList(tmpSpecs);
    }
    return new RowsMetadata(columnCount, columnSpecs, state, pkIndices);
  }

  public static <B> RowsMetadata decodeWithoutPkIndices(
      B source, PrimitiveCodec<B> decoder, int protocolVersion) {
    return decode(source, decoder, false);
  }

  public static <B> RowsMetadata decodeWithPkIndices(
      B source, PrimitiveCodec<B> decoder, int protocolVersion) {
    return decode(source, decoder, true);
  }
}
