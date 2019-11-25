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
package com.datastax.oss.protocol.internal.response.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;

public class DefaultRows extends Rows {
  private final RowsMetadata metadata;
  private final Queue<List<ByteBuffer>> data;

  public DefaultRows(RowsMetadata metadata, Queue<List<ByteBuffer>> data) {
    this.metadata = metadata;
    this.data = data;
  }

  @Override
  public RowsMetadata getMetadata() {
    return metadata;
  }

  @Override
  public Queue<List<ByteBuffer>> getData() {
    return data;
  }

  @Override
  public String toString() {
    return "ROWS(" + data.size() + " x " + metadata.columnCount + " columns)";
  }

  public static class SubCodec extends Result.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ResultKind.ROWS, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      DefaultRows rows = (DefaultRows) message;
      rows.metadata.encode(dest, encoder, false, protocolVersion);
      encoder.writeInt(rows.data.size(), dest);
      for (List<ByteBuffer> row : rows.data) {
        for (ByteBuffer column : row) {
          encoder.writeBytes(column, dest);
        }
      }
    }

    @Override
    public int encodedSize(Message message) {
      DefaultRows rows = (DefaultRows) message;
      int size = rows.metadata.encodedSize(false, protocolVersion) + PrimitiveSizes.INT;
      for (List<ByteBuffer> row : rows.data) {
        for (ByteBuffer column : row) {
          size += PrimitiveSizes.sizeOfBytes(column);
        }
      }
      return size;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      RowsMetadata metadata = RowsMetadata.decode(source, decoder, false, protocolVersion);
      int rowCount = decoder.readInt(source);

      Queue<List<ByteBuffer>> data = new ArrayDeque<>(rowCount);
      for (int i = 0; i < rowCount; i++) {
        NullAllowingImmutableList.Builder<ByteBuffer> row =
            NullAllowingImmutableList.builder(metadata.columnCount);
        for (int j = 0; j < metadata.columnCount; j++) {
          row.add(decoder.readBytes(source));
        }
        data.add(row.build());
      }

      return new DefaultRows(metadata, data);
    }
  }
}
