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

public class AlreadyExists extends Error {
  public final String keyspace;
  public final String table;

  public AlreadyExists(String message, String keyspace, String table) {
    super(ProtocolConstants.ErrorCode.ALREADY_EXISTS, message);
    this.keyspace = keyspace;
    this.table = table;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.ALREADY_EXISTS, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      AlreadyExists alreadyExists = (AlreadyExists) message;
      encoder.writeString(alreadyExists.message, dest);
      encoder.writeString(alreadyExists.keyspace, dest);
      encoder.writeString(alreadyExists.table, dest);
    }

    @Override
    public int encodedSize(Message message) {
      AlreadyExists alreadyExists = (AlreadyExists) message;
      return PrimitiveSizes.sizeOfString(alreadyExists.message)
          + PrimitiveSizes.sizeOfString(alreadyExists.keyspace)
          + PrimitiveSizes.sizeOfString(alreadyExists.table);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      String keyspace = decoder.readString(source);
      String table = decoder.readString(source);
      return new AlreadyExists(message, keyspace, table);
    }
  }
}
