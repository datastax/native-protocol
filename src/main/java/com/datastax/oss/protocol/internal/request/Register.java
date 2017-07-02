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
import java.util.List;

public class Register extends Message {

  /** @see ProtocolConstants.EventType */
  public final List<String> eventTypes;

  public Register(List<String> eventTypes) {
    super(false, ProtocolConstants.Opcode.REGISTER);
    this.eventTypes = eventTypes;
  }

  @Override
  public String toString() {
    return "REGISTER " + eventTypes;
  }

  public static class Codec extends Message.Codec {

    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.REGISTER, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Register register = (Register) message;
      encoder.writeStringList(register.eventTypes, dest);
    }

    @Override
    public int encodedSize(Message message) {
      Register register = (Register) message;
      return PrimitiveSizes.sizeOfStringList(register.eventTypes);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      return new Register(decoder.readStringList(source));
    }
  }
}
