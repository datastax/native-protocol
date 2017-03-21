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

public class WriteTimeout extends Error {
  /** The consistency level of the query that triggered the exception. */
  public final int consistencyLevel;
  /** The number of nodes having acknowledged the request. */
  public final int received;
  /**
   * The number of replicas whose acknowledgment is required to achieve {@code consistencyLevel}.
   */
  public final int blockFor;
  /** The type of the write that timed out. */
  public final String writeType;

  public WriteTimeout(
      String message, int consistencyLevel, int received, int blockFor, String writeType) {
    super(ProtocolConstants.ErrorCode.WRITE_TIMEOUT, message);
    this.consistencyLevel = consistencyLevel;
    this.received = received;
    this.blockFor = blockFor;
    this.writeType = writeType;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.WRITE_TIMEOUT, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      WriteTimeout writeTimeout = (WriteTimeout) message;
      encoder.writeString(writeTimeout.message, dest);
      encoder.writeUnsignedShort(writeTimeout.consistencyLevel, dest);
      encoder.writeInt(writeTimeout.received, dest);
      encoder.writeInt(writeTimeout.blockFor, dest);
      encoder.writeString(writeTimeout.writeType, dest);
    }

    @Override
    public int encodedSize(Message message) {
      WriteTimeout writeTimeout = (WriteTimeout) message;
      return PrimitiveSizes.sizeOfString(writeTimeout.message)
          + 2
          + 4
          + 4
          + PrimitiveSizes.sizeOfString(writeTimeout.writeType);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      int consistencyLevel = decoder.readUnsignedShort(source);
      int received = decoder.readInt(source);
      int blockFor = decoder.readInt(source);
      String writeType = decoder.readString(source);
      return new WriteTimeout(message, consistencyLevel, received, blockFor, writeType);
    }
  }
}
