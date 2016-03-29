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
package com.datastax.cassandra.protocol.internal.response.error;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.response.Error;

public class FunctionFailure extends Error {
  public final String keyspace;
  public final String function;
  public final String argTypes;

  public FunctionFailure(String message, String keyspace, String function, String argTypes) {
    super(ProtocolConstants.ErrorCode.FUNCTION_FAILURE, message);
    this.keyspace = keyspace;
    this.function = function;
    this.argTypes = argTypes;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.FUNCTION_FAILURE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int encodedSize(Message message) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      String keyspace = decoder.readString(source);
      String function = decoder.readString(source);
      String argTypes = decoder.readString(source);
      return new FunctionFailure(message, keyspace, function, argTypes);
    }
  }
}
