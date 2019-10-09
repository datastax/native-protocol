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
package com.datastax.dse.protocol.internal;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.ErrorCode.CLIENT_WRITE_FAILURE;
import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V2;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.AUTH_ERROR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.CONFIG_ERROR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.INVALID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.OVERLOADED;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.PROTOCOL_ERROR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.SERVER_ERROR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.SYNTAX_ERROR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.TRUNCATE_ERROR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.UNAUTHORIZED;

import com.datastax.dse.protocol.internal.request.DseBatchCodec;
import com.datastax.dse.protocol.internal.request.DseExecuteCodec;
import com.datastax.dse.protocol.internal.request.DsePrepareCodec;
import com.datastax.dse.protocol.internal.request.DseQueryCodec;
import com.datastax.dse.protocol.internal.request.Revise;
import com.datastax.dse.protocol.internal.response.result.DsePreparedSubCodec;
import com.datastax.dse.protocol.internal.response.result.DseRowsSubCodec;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Error.SingleMessageSubCodec;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.datastax.oss.protocol.internal.response.error.FunctionFailure;
import com.datastax.oss.protocol.internal.response.error.ReadFailure;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.error.WriteFailure;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;

public class DseProtocolV2ServerCodecs implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addDecoder(new AuthResponse.Codec(DSE_V2))
        .addDecoder(new DseBatchCodec(DSE_V2))
        .addDecoder(new DseExecuteCodec(DSE_V2))
        .addDecoder(new Options.Codec(DSE_V2))
        .addDecoder(new DsePrepareCodec(DSE_V2))
        .addDecoder(new DseQueryCodec(DSE_V2))
        .addDecoder(new Register.Codec(DSE_V2))
        .addDecoder(new Startup.Codec(DSE_V2))
        .addDecoder(new Revise.Codec(DSE_V2));

    registry
        .addEncoder(new AuthChallenge.Codec(DSE_V2))
        .addEncoder(new Authenticate.Codec(DSE_V2))
        .addEncoder(new AuthSuccess.Codec(DSE_V2))
        .addEncoder(
            new Error.Codec(
                DSE_V2,
                // OSS C* errors
                new SingleMessageSubCodec(SERVER_ERROR, DSE_V2),
                new SingleMessageSubCodec(PROTOCOL_ERROR, DSE_V2),
                new SingleMessageSubCodec(AUTH_ERROR, DSE_V2),
                new SingleMessageSubCodec(OVERLOADED, DSE_V2),
                new SingleMessageSubCodec(IS_BOOTSTRAPPING, DSE_V2),
                new SingleMessageSubCodec(TRUNCATE_ERROR, DSE_V2),
                new SingleMessageSubCodec(SYNTAX_ERROR, DSE_V2),
                new SingleMessageSubCodec(UNAUTHORIZED, DSE_V2),
                new SingleMessageSubCodec(INVALID, DSE_V2),
                new SingleMessageSubCodec(CONFIG_ERROR, DSE_V2),
                new Unavailable.SubCodec(DSE_V2),
                new WriteTimeout.SubCodec(DSE_V2),
                new ReadTimeout.SubCodec(DSE_V2),
                new ReadFailure.SubCodec(DSE_V2),
                new FunctionFailure.SubCodec(DSE_V2),
                new WriteFailure.SubCodec(DSE_V2),
                new AlreadyExists.SubCodec(DSE_V2),
                new Unprepared.SubCodec(DSE_V2),
                // DSE-specific errors
                new SingleMessageSubCodec(CLIENT_WRITE_FAILURE, DSE_V2)))
        .addEncoder(new Event.Codec(DSE_V2))
        .addEncoder(new Ready.Codec(DSE_V2))
        .addEncoder(
            new Result.Codec(
                DSE_V2,
                new Void.SubCodec(DSE_V2),
                new SetKeyspace.SubCodec(DSE_V2),
                new SchemaChange.SubCodec(DSE_V2),
                new DsePreparedSubCodec(DSE_V2),
                new DseRowsSubCodec(DSE_V2)))
        .addEncoder(new Supported.Codec(DSE_V2));
  }
}
