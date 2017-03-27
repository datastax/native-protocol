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
package com.datastax.cassandra.protocol.internal.response;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.ProtocolErrors;
import com.datastax.cassandra.protocol.internal.response.result.Prepared;
import com.datastax.cassandra.protocol.internal.response.result.Rows;
import com.datastax.cassandra.protocol.internal.response.result.SchemaChange;
import com.datastax.cassandra.protocol.internal.response.result.SetKeyspace;
import com.datastax.cassandra.protocol.internal.response.result.Void;
import com.datastax.cassandra.protocol.internal.util.IntMap;

public abstract class Result extends Message {

  /** @see ProtocolConstants.ResultKind */
  public final int kind;

  protected Result(int kind) {
    super(true, ProtocolConstants.Opcode.RESULT);
    this.kind = kind;
  }

  public static class Codec extends Message.Codec {
    private final IntMap<SubCodec> subDecoders;

    public Codec(int protocolVersion, SubCodec... subCodecs) {
      super(ProtocolConstants.Opcode.RESULT, protocolVersion);
      IntMap.Builder<SubCodec> builder = IntMap.builder();
      for (SubCodec subCodec : subCodecs) {
        builder.put(subCodec.kind, subCodec);
      }
      this.subDecoders = builder.build();
    }

    /** Creates an instance with subdecoders for the default kinds. */
    public Codec(int protocolVersion) {
      this(
          protocolVersion,
          new Void.SubCodec(protocolVersion),
          new Rows.SubCodec(protocolVersion),
          new SetKeyspace.SubCodec(protocolVersion),
          new Prepared.SubCodec(protocolVersion),
          new SchemaChange.SubCodec(protocolVersion));
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Result result = (Result) message;
      encoder.writeInt(result.kind, dest);
      getSubCodec(result.kind).encode(dest, result, encoder);
    }

    @Override
    public int encodedSize(Message message) {
      Result result = (Result) message;
      return PrimitiveSizes.INT + getSubCodec(result.kind).encodedSize(result);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      int kind = decoder.readInt(source);
      return getSubCodec(kind).decode(source, decoder);
    }

    private SubCodec getSubCodec(int kind) {
      SubCodec subCodec = subDecoders.get(kind);
      ProtocolErrors.check(subCodec != null, "Unsupported result kind: %d", kind);
      return subCodec;
    }
  }

  public abstract static class SubCodec {
    public final int kind;
    public final int protocolVersion;

    protected SubCodec(int kind, int protocolVersion) {
      this.kind = kind;
      this.protocolVersion = protocolVersion;
    }

    public abstract <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder);

    public abstract int encodedSize(Message message);

    public abstract <B> Message decode(B source, PrimitiveCodec<B> decoder);
  }
}
