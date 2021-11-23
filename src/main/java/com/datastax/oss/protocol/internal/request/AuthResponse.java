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
package com.datastax.oss.protocol.internal.request;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;

/**
 * Note that, if the token is writable, the built-in codec will <b>clear its contents</b>
 * immediately after writing it (to avoid keeping sensitive information in memory). If you want to
 * reuse the same buffer across multiple message instances, make it {@linkplain
 * ByteBuffer#asReadOnlyBuffer() read-only}.
 */
public class AuthResponse extends Message {
  public final ByteBuffer token;

  public AuthResponse(ByteBuffer token) {
    super(false, ProtocolConstants.Opcode.AUTH_RESPONSE);
    this.token = token;
  }

  @Override
  public String toString() {
    return "AUTH_RESPONSE";
  }

  public static class Codec extends Message.Codec {

    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.AUTH_RESPONSE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      AuthResponse authResponse = (AuthResponse) message;
      ByteBuffer token = authResponse.token;
      if (token == null) {
        encoder.writeBytes(token, dest);
      } else {
        token.mark();
        encoder.writeBytes(token, dest);
        token.reset();
        Bytes.erase(token);
      }
    }

    @Override
    public int encodedSize(Message message) {
      AuthResponse authResponse = (AuthResponse) message;
      return PrimitiveSizes.sizeOfBytes(authResponse.token);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      ByteBuffer token = decoder.readBytes(source);
      return new AuthResponse(token);
    }
  }
}
