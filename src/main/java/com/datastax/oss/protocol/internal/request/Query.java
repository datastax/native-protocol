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

public class Query extends Message {
  public final String query;
  public final QueryOptions options;

  public Query(String query, QueryOptions options) {
    super(false, ProtocolConstants.Opcode.QUERY);
    this.query = query;
    this.options = options;
  }

  public Query(String query) {
    this(query, QueryOptions.DEFAULT);
  }

  @Override
  public String toString() {
    return "QUERY (" + query + ')';
  }

  public static class Codec extends Message.Codec {

    private final QueryOptions.Codec optionsCodec;

    public Codec(int protocolVersion, QueryOptions.Codec optionsCodec) {
      super(ProtocolConstants.Opcode.QUERY, protocolVersion);
      this.optionsCodec = optionsCodec;
    }

    public Codec(int protocolVersion) {
      this(protocolVersion, new QueryOptions.Codec(protocolVersion));
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Query query = (Query) message;
      encoder.writeLongString(query.query, dest);
      optionsCodec.encode(dest, query.options, encoder);
    }

    @Override
    public int encodedSize(Message message) {
      Query query = (Query) message;
      return PrimitiveSizes.sizeOfLongString(query.query) + optionsCodec.encodedSize(query.options);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String query = decoder.readLongString(source);
      QueryOptions options = optionsCodec.decode(source, decoder);
      return new Query(query, options);
    }
  }
}
