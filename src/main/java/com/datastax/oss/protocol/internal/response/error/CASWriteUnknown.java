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
package com.datastax.oss.protocol.internal.response.error;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;

public class CASWriteUnknown extends Error {

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

  public CASWriteUnknown(String message, int consistencyLevel, int received, int blockFor) {
    super(ProtocolConstants.ErrorCode.CAS_WRITE_UNKNOWN, message);
    this.consistencyLevel = consistencyLevel;
    this.received = received;
    this.blockFor = blockFor;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.CAS_WRITE_UNKNOWN, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      CASWriteUnknown readTimeout = (CASWriteUnknown) message;
      encoder.writeString(readTimeout.message, dest);
      encoder.writeUnsignedShort(readTimeout.consistencyLevel, dest);
      encoder.writeInt(readTimeout.received, dest);
      encoder.writeInt(readTimeout.blockFor, dest);
    }

    @Override
    public int encodedSize(Message message) {
      CASWriteUnknown readTimeout = (CASWriteUnknown) message;
      return PrimitiveSizes.sizeOfString(readTimeout.message)
          + PrimitiveSizes.SHORT
          + PrimitiveSizes.INT
          + PrimitiveSizes.INT;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      int consistencyLevel = decoder.readUnsignedShort(source);
      int received = decoder.readInt(source);
      int blockFor = decoder.readInt(source);
      return new CASWriteUnknown(message, consistencyLevel, received, blockFor);
    }
  }
}
