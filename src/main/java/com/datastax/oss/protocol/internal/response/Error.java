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
package com.datastax.oss.protocol.internal.response;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode;
import com.datastax.oss.protocol.internal.ProtocolErrors;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.datastax.oss.protocol.internal.response.error.FunctionFailure;
import com.datastax.oss.protocol.internal.response.error.ReadFailure;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.error.WriteFailure;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.datastax.oss.protocol.internal.util.IntMap;

/**
 * Error responses that only contain a code and a single message will be represented as direct
 * instances of this class; those that have more information will be subclasses.
 */
public class Error extends Message {

  /** @see ErrorCode */
  public final int code;

  public final String message;

  public Error(int code, String message) {
    super(true, ProtocolConstants.Opcode.ERROR);
    this.code = code;
    this.message = message;
  }

  @Override
  public String toString() {
    return "ERROR(" + message + ")";
  }

  public static class Codec extends Message.Codec {
    private final IntMap<SubCodec> subCodecs;

    public Codec(int protocolVersion, SubCodec... SubCodecs) {
      super(ProtocolConstants.Opcode.ERROR, protocolVersion);
      IntMap.Builder<SubCodec> builder = IntMap.builder();
      for (SubCodec SubCodec : SubCodecs) {
        builder.put(SubCodec.errorCode, SubCodec);
      }
      this.subCodecs = builder.build();
    }

    /** Creates an instance with SubCodecs for the default error codes. */
    public Codec(int protocolVersion) {
      this(
          protocolVersion,
          new SingleMessageSubCodec(ErrorCode.SERVER_ERROR, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.PROTOCOL_ERROR, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.AUTH_ERROR, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.OVERLOADED, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.IS_BOOTSTRAPPING, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.TRUNCATE_ERROR, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.SYNTAX_ERROR, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.UNAUTHORIZED, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.INVALID, protocolVersion),
          new SingleMessageSubCodec(ErrorCode.CONFIG_ERROR, protocolVersion),
          new Unavailable.SubCodec(protocolVersion),
          new WriteTimeout.SubCodec(protocolVersion),
          new ReadTimeout.SubCodec(protocolVersion),
          new ReadFailure.SubCodec(protocolVersion),
          new FunctionFailure.SubCodec(protocolVersion),
          new WriteFailure.SubCodec(protocolVersion),
          new AlreadyExists.SubCodec(protocolVersion),
          new Unprepared.SubCodec(protocolVersion));
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Error error = (Error) message;
      encoder.writeInt(error.code, dest);
      getSubCodec(error.code).encode(dest, message, encoder);
    }

    @Override
    public int encodedSize(Message message) {
      Error error = (Error) message;
      return PrimitiveSizes.INT + getSubCodec(error.code).encodedSize(message);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      int errorCode = decoder.readInt(source);
      return getSubCodec(errorCode).decode(source, decoder);
    }

    private SubCodec getSubCodec(int errorCode) {
      SubCodec subCodec = subCodecs.get(errorCode);
      ProtocolErrors.check(subCodec != null, "Unsupported error code: %d", errorCode);
      return subCodec;
    }
  }

  public abstract static class SubCodec {
    protected final int errorCode;
    protected final int protocolVersion;

    protected SubCodec(int errorCode, int protocolVersion) {
      this.errorCode = errorCode;
      this.protocolVersion = protocolVersion;
    }

    public abstract <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder);

    public abstract int encodedSize(Message message);

    public abstract <B> Message decode(B source, PrimitiveCodec<B> decoder);
  }

  public static class SingleMessageSubCodec extends SubCodec {

    public SingleMessageSubCodec(int errorCode, int protocolVersion) {
      super(errorCode, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Error error = (Error) message;
      encoder.writeString(error.message, dest);
    }

    @Override
    public int encodedSize(Message message) {
      Error error = (Error) message;
      return PrimitiveSizes.sizeOfString(error.message);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      return new Error(errorCode, message);
    }
  }
}
