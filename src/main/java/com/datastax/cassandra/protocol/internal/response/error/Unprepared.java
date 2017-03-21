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
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.response.Error;

public class Unprepared extends Error {
  public final byte[] id;

  public Unprepared(String message, byte[] id) {
    super(ProtocolConstants.ErrorCode.UNPREPARED, message);
    this.id = id;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.UNPREPARED, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Unprepared unprepared = (Unprepared) message;
      encoder.writeString(unprepared.message, dest);
      encoder.writeShortBytes(unprepared.id, dest);
    }

    @Override
    public int encodedSize(Message message) {
      Unprepared unprepared = (Unprepared) message;
      return PrimitiveSizes.sizeOfString(unprepared.message)
          + PrimitiveSizes.sizeOfShortBytes(unprepared.id);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      byte[] id = decoder.readShortBytes(source);
      return new Unprepared(message, id);
    }
  }
}
