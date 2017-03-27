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
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.response.Result;

public class Void extends Result {
  public static final Void INSTANCE = new Void();

  private Void() {
    super(ProtocolConstants.ResultKind.VOID);
  }

  public static class SubCodec extends Result.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ResultKind.VOID, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {}

    @Override
    public int encodedSize(Message message) {
      return 0;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      return INSTANCE;
    }
  }
}
