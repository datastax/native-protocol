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

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.response.Result;

public class SetKeyspace extends Result {
  public final String keyspace;

  public SetKeyspace(String keyspace) {
    super(ProtocolConstants.ResultKind.SET_KEYSPACE);
    this.keyspace = keyspace;
  }

  public static class SubCodec extends Result.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ResultKind.SET_KEYSPACE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      SetKeyspace setKeyspace = (SetKeyspace) message;
      encoder.writeString(setKeyspace.keyspace, dest);
    }

    @Override
    public int encodedSize(Message message) {
      SetKeyspace setKeyspace = (SetKeyspace) message;
      return PrimitiveSizes.sizeOfString(setKeyspace.keyspace);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String keyspace = decoder.readString(source);
      return new SetKeyspace(keyspace);
    }
  }
}
