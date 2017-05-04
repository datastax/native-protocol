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
package com.datastax.oss.protocol.internal.response.error;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import java.util.List;

public class FunctionFailure extends Error {
  public final String keyspace;
  public final String function;
  public final List<String> argTypes;

  public FunctionFailure(String message, String keyspace, String function, List<String> argTypes) {
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
      FunctionFailure functionFailure = (FunctionFailure) message;
      encoder.writeString(functionFailure.message, dest);
      encoder.writeString(functionFailure.keyspace, dest);
      encoder.writeString(functionFailure.function, dest);
      encoder.writeStringList(functionFailure.argTypes, dest);
    }

    @Override
    public int encodedSize(Message message) {
      FunctionFailure functionFailure = (FunctionFailure) message;
      return PrimitiveSizes.sizeOfString(functionFailure.message)
          + PrimitiveSizes.sizeOfString(functionFailure.keyspace)
          + PrimitiveSizes.sizeOfString(functionFailure.function)
          + PrimitiveSizes.sizeOfStringList(functionFailure.argTypes);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      String keyspace = decoder.readString(source);
      String function = decoder.readString(source);
      List<String> argTypes = decoder.readStringList(source);
      return new FunctionFailure(message, keyspace, function, argTypes);
    }
  }
}
