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

public class ReadTimeout extends Error {
  /** The consistency level of the query that triggered the exception. */
  public final int consistencyLevel;
  /** The number of nodes having answered the request. */
  public final int received;
  /**
   * The number of replicas whose response is required to achieve {@code consistencyLevel}.
   *
   * <p>It is possible to have {@code received >= blockFor} if {@code data_present} is false. Also
   * in the (unlikely) case where {@code consistencyLevel} is achieved but the coordinator node
   * times out while waiting for read-repair acknowledgement.
   */
  public final int blockFor;
  /** Whether the replica that was asked for data responded. */
  public final boolean dataPresent;

  public ReadTimeout(
      String message, int consistencyLevel, int received, int blockFor, boolean dataPresent) {
    super(ProtocolConstants.ErrorCode.READ_TIMEOUT, message);
    this.consistencyLevel = consistencyLevel;
    this.received = received;
    this.blockFor = blockFor;
    this.dataPresent = dataPresent;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.READ_TIMEOUT, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      ReadTimeout readTimeout = (ReadTimeout) message;
      encoder.writeString(readTimeout.message, dest);
      encoder.writeUnsignedShort(readTimeout.consistencyLevel, dest);
      encoder.writeInt(readTimeout.received, dest);
      encoder.writeInt(readTimeout.blockFor, dest);
      encoder.writeByte((byte) (readTimeout.dataPresent ? 1 : 0), dest);
    }

    @Override
    public int encodedSize(Message message) {
      ReadTimeout readTimeout = (ReadTimeout) message;
      return PrimitiveSizes.sizeOfString(readTimeout.message) + 2 + 4 + 4 + 1;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      int consistencyLevel = decoder.readUnsignedShort(source);
      int received = decoder.readInt(source);
      int blockFor = decoder.readInt(source);
      boolean dataPresent = decoder.readByte(source) != 0;
      return new ReadTimeout(message, consistencyLevel, received, blockFor, dataPresent);
    }
  }
}
