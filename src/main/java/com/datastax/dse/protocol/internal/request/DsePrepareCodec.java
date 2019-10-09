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
package com.datastax.dse.protocol.internal.request;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;
import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V2;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;

public class DsePrepareCodec extends Message.Codec {

  public DsePrepareCodec(int protocolVersion) {
    super(ProtocolConstants.Opcode.PREPARE, protocolVersion);
    assert protocolVersion >= DSE_V1;
  }

  @Override
  public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
    Prepare prepare = (Prepare) message;
    encoder.writeLongString(prepare.cqlQuery, dest);
    if (protocolVersion >= DSE_V2) {
      // There is only one PREPARE flag for now, so hard-code for simplicity:
      encoder.writeInt((prepare.keyspace == null) ? 0x00 : 0x01, dest);
      if (prepare.keyspace != null) {
        encoder.writeString(prepare.keyspace, dest);
      }
    }
  }

  @Override
  public int encodedSize(Message message) {
    Prepare prepare = (Prepare) message;
    int size = PrimitiveSizes.sizeOfLongString(prepare.cqlQuery);
    if (protocolVersion >= DSE_V2) {
      size += PrimitiveSizes.INT; // flags
      if (prepare.keyspace != null) {
        size += PrimitiveSizes.sizeOfString(prepare.keyspace);
      }
    }
    return size;
  }

  @Override
  public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
    String cqlQuery = decoder.readLongString(source);
    String keyspace = null;
    if (protocolVersion >= DSE_V2) {
      int flags = decoder.readInt(source);
      if ((flags & 0x01) == 0x01) {
        keyspace = decoder.readString(source);
      }
    }
    return new Prepare(cqlQuery, keyspace);
  }
}
