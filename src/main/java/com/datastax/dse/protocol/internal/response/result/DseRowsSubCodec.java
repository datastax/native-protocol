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

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class DseRowsSubCodec extends DefaultRows.SubCodec {

  public DseRowsSubCodec(int protocolVersion) {
    super(protocolVersion);
  }

  // No need to override `encode` and `encodedSize`, if the metadata is a DseRowsMetadata it knows
  // how to encode itself.

  @Override
  public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
    DseRowsMetadata metadata = DseRowsMetadata.decode(source, decoder, false, protocolVersion);
    int rowCount = decoder.readInt(source);

    Queue<List<ByteBuffer>> data = new ArrayDeque<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      List<ByteBuffer> row = new ArrayList<>(metadata.columnCount);
      for (int j = 0; j < metadata.columnCount; j++) {
        row.add(decoder.readBytes(source));
      }
      data.add(row);
    }

    return new DefaultRows(metadata, data);
  }
}
