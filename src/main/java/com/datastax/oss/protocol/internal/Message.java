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
package com.datastax.oss.protocol.internal;

public abstract class Message {
  public final boolean isResponse;

  /** @see ProtocolConstants.Opcode */
  public final int opcode;

  protected Message(boolean isResponse, int opcode) {
    this.isResponse = isResponse;
    this.opcode = opcode;
  }

  public abstract static class Codec {
    /** @see ProtocolConstants.Opcode */
    public final int opcode;
    /** @see ProtocolConstants.Version */
    public final int protocolVersion;

    protected Codec(int opcode, int protocolVersion) {
      this.opcode = opcode;
      this.protocolVersion = protocolVersion;
    }

    public abstract <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder);

    public abstract int encodedSize(Message message);

    public abstract <B> Message decode(B source, PrimitiveCodec<B> decoder);
  }
}
