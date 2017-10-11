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

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.util.Bytes;

import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V4;
import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V5;

public class Prepared extends Result {
  public final byte[] preparedQueryId;
  public final byte[] resultMetadataId;
  public final RowsMetadata variablesMetadata;
  public final RowsMetadata resultMetadata;

  public Prepared(
      byte[] preparedQueryId,
      byte[] resultMetadataId,
      RowsMetadata variablesMetadata,
      RowsMetadata resultMetadata) {
    super(ProtocolConstants.ResultKind.PREPARED);
    this.preparedQueryId = preparedQueryId;
    this.resultMetadataId = resultMetadataId;
    this.variablesMetadata = variablesMetadata;
    this.resultMetadata = resultMetadata;
  }

  @Override
  public String toString() {
    return "PREPARED(" + Bytes.toHexString(preparedQueryId) + ')';
  }

  public static class SubCodec extends Result.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ResultKind.PREPARED, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Prepared prepared = (Prepared) message;
      encoder.writeShortBytes(prepared.preparedQueryId, dest);
      if (protocolVersion >= V5) {
        encoder.writeShortBytes(prepared.resultMetadataId, dest);
      }
      boolean hasPkIndices = (protocolVersion >= V4);
      prepared.variablesMetadata.encode(dest, encoder, hasPkIndices, protocolVersion);
      prepared.resultMetadata.encode(dest, encoder, false, protocolVersion);
    }

    @Override
    public int encodedSize(Message message) {
      Prepared prepared = (Prepared) message;
      int size = PrimitiveSizes.sizeOfShortBytes(prepared.preparedQueryId);
      if (protocolVersion >= V5) {
        assert prepared.resultMetadataId != null;
        size += PrimitiveSizes.sizeOfShortBytes(prepared.resultMetadataId);
      }
      boolean hasPkIndices = (protocolVersion >= V4);
      size += prepared.variablesMetadata.encodedSize(hasPkIndices, protocolVersion);
      size += prepared.resultMetadata.encodedSize(false, protocolVersion);
      return size;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      byte[] preparedQueryId = decoder.readShortBytes(source);
      byte[] resultMetadataId = (protocolVersion >= V5) ? decoder.readShortBytes(source) : null;
      boolean hasPkIndices = (protocolVersion >= V4);
      RowsMetadata variablesMetadata =
          RowsMetadata.decode(source, decoder, hasPkIndices, protocolVersion);
      RowsMetadata resultMetadata = RowsMetadata.decode(source, decoder, false, protocolVersion);
      return new Prepared(preparedQueryId, resultMetadataId, variablesMetadata, resultMetadata);
    }
  }
}
