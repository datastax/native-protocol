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

public class Prepare extends Message {
  public final String cqlQuery;

  public Prepare(String cqlQuery) {
    super(false, ProtocolConstants.Opcode.PREPARE);
    this.cqlQuery = cqlQuery;
  }

  @Override
  public String toString() {
    return "PREPARE(" + cqlQuery + ')';
  }

  public static class Codec extends Message.Codec {
    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.PREPARE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Prepare prepare = (Prepare) message;
      encoder.writeLongString(prepare.cqlQuery, dest);
    }

    @Override
    public int encodedSize(Message message) {
      Prepare prepare = (Prepare) message;
      return PrimitiveSizes.sizeOfLongString(prepare.cqlQuery);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String cqlQuery = decoder.readLongString(source);
      return new Prepare(cqlQuery);
    }
  }
}
