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
package com.datastax.oss.protocol.internal.response;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.nio.ByteBuffer;

public class AuthSuccess extends Message {
  public final ByteBuffer token;

  public AuthSuccess(ByteBuffer token) {
    super(true, ProtocolConstants.Opcode.AUTH_SUCCESS);
    this.token = token;
  }

  @Override
  public String toString() {
    return "AUTH_SUCCESS";
  }

  public static class Codec extends Message.Codec {
    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.AUTH_SUCCESS, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      AuthSuccess authSuccess = (AuthSuccess) message;
      encoder.writeBytes(authSuccess.token, dest);
    }

    @Override
    public int encodedSize(Message message) {
      AuthSuccess authSuccess = (AuthSuccess) message;
      return PrimitiveSizes.sizeOfBytes(authSuccess.token);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      return new AuthSuccess(decoder.readBytes(source));
    }
  }
}
