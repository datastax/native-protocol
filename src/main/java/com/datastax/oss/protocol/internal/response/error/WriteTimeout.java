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

import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V5;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;

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
  /**
   * The number of contentions that occurred during the CAS operation, or {@link #UNKNOWN_CONTENTIONS} if
   * the server did not send it (protocol v4 or below, or a {@code writeType} other than {@code
   * CAS}).
   */
  public final int contentions;

  /** The value of {@link #contentions} when the server did not send it. */
  public static final int UNKNOWN_CONTENTIONS = -1;

  public WriteTimeout(
      String message,
      int consistencyLevel,
      int received,
      int blockFor,
      String writeType,
      int contentions) {
    super(ProtocolConstants.ErrorCode.WRITE_TIMEOUT, message);
    this.consistencyLevel = consistencyLevel;
    this.received = received;
    this.blockFor = blockFor;
    this.writeType = writeType;
    this.contentions = contentions;
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
      writeContentions(dest, encoder, writeTimeout);
    }

    @Override
    public int encodedSize(Message message) {
      WriteTimeout writeTimeout = (WriteTimeout) message;
      return PrimitiveSizes.sizeOfString(writeTimeout.message)
          + PrimitiveSizes.SHORT
          + PrimitiveSizes.INT
          + PrimitiveSizes.INT
          + PrimitiveSizes.sizeOfString(writeTimeout.writeType)
          + contentionsSize(writeTimeout);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      int consistencyLevel = decoder.readUnsignedShort(source);
      int received = decoder.readInt(source);
      int blockFor = decoder.readInt(source);
      String writeType = decoder.readString(source);
      int contentions = readContentions(source, decoder, writeType);
      return new WriteTimeout(message, consistencyLevel, received, blockFor, writeType, contentions);
    }

    private boolean hasContentions(String writeType) {
      return protocolVersion >= V5 && ProtocolConstants.WriteType.CAS.equals(writeType);
    }

    private <B> int readContentions(B source, PrimitiveCodec<B> decoder, String writeType) {
      return hasContentions(writeType)
          ? decoder.readUnsignedShort(source)
          : UNKNOWN_CONTENTIONS;
    }

    private <B> void writeContentions(B dest, PrimitiveCodec<B> encoder, WriteTimeout writeTimeout) {
      if (hasContentions(writeTimeout.writeType)) {
        int s = writeTimeout.contentions == UNKNOWN_CONTENTIONS ? 0 : writeTimeout.contentions;
        encoder.writeUnsignedShort(s, dest);
      }
    }

    private int contentionsSize(WriteTimeout writeTimeout) {
      return hasContentions(writeTimeout.writeType) ? PrimitiveSizes.SHORT : 0;
    }
  }
}
