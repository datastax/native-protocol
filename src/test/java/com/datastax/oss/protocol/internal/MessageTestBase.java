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
package com.datastax.oss.protocol.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;

public abstract class MessageTestBase<M extends Message> {

  private final Class<M> messageClass;

  protected MessageTestBase(Class<M> messageClass) {
    this.messageClass = messageClass;
  }

  protected abstract Message.Codec newCodec(int protocolVersion);

  protected MockBinaryString encode(M message, int protocolVersion) {
    MockBinaryString dest = new MockBinaryString();
    Message.Codec codec = newCodec(protocolVersion);
    assertThat(codec.opcode).isEqualTo(message.opcode);
    codec.encode(dest, message, MockPrimitiveCodec.INSTANCE);
    return dest;
  }

  protected int encodedSize(M message, int protocolVersion) {
    Message.Codec codec = newCodec(protocolVersion);
    return codec.encodedSize(message);
  }

  protected M decode(MockBinaryString source, int protocolVersion) {
    Message.Codec codec = newCodec(protocolVersion);
    Message message = codec.decode(source, MockPrimitiveCodec.INSTANCE);
    assertThat(message).isInstanceOf(messageClass);
    assertThat(message.opcode).isEqualTo(codec.opcode);
    return messageClass.cast(message);
  }
}
