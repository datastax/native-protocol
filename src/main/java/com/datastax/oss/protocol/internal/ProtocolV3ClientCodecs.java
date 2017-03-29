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
package com.datastax.oss.protocol.internal;

import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.Supported;

public class ProtocolV3ClientCodecs implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addEncoder(new AuthResponse.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Batch.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Execute.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Options.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Prepare.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Query.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Register.Codec(ProtocolConstants.Version.V3))
        .addEncoder(new Startup.Codec(ProtocolConstants.Version.V3));

    registry
        .addDecoder(new AuthChallenge.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new Authenticate.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new AuthSuccess.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new Error.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new Event.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new Ready.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new Result.Codec(ProtocolConstants.Version.V3))
        .addDecoder(new Supported.Codec(ProtocolConstants.Version.V3));
  }
}