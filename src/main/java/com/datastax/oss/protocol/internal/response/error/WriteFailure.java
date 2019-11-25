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
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

public class WriteFailure extends Error {
  /** The consistency level of the query that triggered the exception. */
  public final int consistencyLevel;
  /** The number of nodes having acknowledged the request. */
  public final int received;
  /**
   * The number of replicas whose acknowledgment is required to achieve {@code consistencyLevel}.
   */
  public final int blockFor;
  /** The number of nodes that experienced a failure while executing the request. */
  public final int numFailures;
  /**
   * A map of endpoint to failure reason codes. This maps the endpoints of the replica nodes that
   * failed when executing the request to a code representing the reason for the failure.
   */
  public final Map<InetAddress, Integer> reasonMap;
  /** The type of the write that timed out. */
  public final String writeType;

  public WriteFailure(
      String message,
      int consistencyLevel,
      int received,
      int blockFor,
      int numFailures,
      Map<InetAddress, Integer> reasonMap,
      String writeType) {
    super(ProtocolConstants.ErrorCode.WRITE_FAILURE, message);
    this.consistencyLevel = consistencyLevel;
    this.received = received;
    this.blockFor = blockFor;
    this.numFailures = numFailures;
    this.reasonMap = reasonMap;
    this.writeType = writeType;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.WRITE_FAILURE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      WriteFailure writeFailure = (WriteFailure) message;
      encoder.writeString(writeFailure.message, dest);
      encoder.writeUnsignedShort(writeFailure.consistencyLevel, dest);
      encoder.writeInt(writeFailure.received, dest);
      encoder.writeInt(writeFailure.blockFor, dest);
      if (protocolVersion >= V5) {
        ReadFailure.SubCodec.writeReasonMap(writeFailure.reasonMap, dest, encoder);
      } else {
        encoder.writeInt(writeFailure.numFailures, dest);
      }
      encoder.writeString(writeFailure.writeType, dest);
    }

    @Override
    public int encodedSize(Message message) {
      WriteFailure writeFailure = (WriteFailure) message;
      int size =
          PrimitiveSizes.sizeOfString(writeFailure.message)
              + PrimitiveSizes.SHORT // consistencyLevel
              + PrimitiveSizes.INT // received
              + PrimitiveSizes.INT // blockFor
              + PrimitiveSizes.sizeOfString(writeFailure.writeType);
      if (protocolVersion >= V5) {
        size += ReadFailure.SubCodec.sizeOfReasonMap(writeFailure.reasonMap);
      } else {
        size += PrimitiveSizes.INT;
      }
      return size;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      int consistencyLevel = decoder.readUnsignedShort(source);
      int received = decoder.readInt(source);
      int blockFor = decoder.readInt(source);
      int numFailures;
      Map<InetAddress, Integer> reasonMap;
      if (protocolVersion >= V5) {
        reasonMap = ReadFailure.SubCodec.readReasonMap(source, decoder);
        numFailures = reasonMap.size();
      } else {
        reasonMap = Collections.emptyMap();
        numFailures = decoder.readInt(source);
      }
      String writeType = decoder.readString(source);
      return new WriteFailure(
          message, consistencyLevel, received, blockFor, numFailures, reasonMap, writeType);
    }
  }
}
