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
package com.datastax.oss.protocol.internal.request;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.util.Bytes;

public class Execute extends Message {

  public final byte[] queryId;
  public final QueryOptions options;

  public Execute(byte[] queryId, QueryOptions options) {
    super(false, ProtocolConstants.Opcode.EXECUTE);
    this.queryId = queryId;
    this.options = options;
  }

  @Override
  public String toString() {
    return "EXECUTE(" + Bytes.toHexString(queryId) + ')';
  }

  public static class Codec extends Message.Codec {

    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.EXECUTE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Execute execute = (Execute) message;
      encoder.writeShortBytes(execute.queryId, dest);
      execute.options.encode(dest, encoder, protocolVersion);
    }

    @Override
    public int encodedSize(Message message) {
      Execute execute = (Execute) message;
      return PrimitiveSizes.sizeOfShortBytes(execute.queryId)
          + execute.options.encodedSize(protocolVersion);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      byte[] queryId = decoder.readShortBytes(source);
      QueryOptions options = QueryOptions.decode(source, decoder, protocolVersion);
      return new Execute(queryId, options);
    }
  }
}
