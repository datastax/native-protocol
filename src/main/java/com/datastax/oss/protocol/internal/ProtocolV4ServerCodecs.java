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

import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V4;

public class ProtocolV4ServerCodecs implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addDecoder(new AuthResponse.Codec(V4))
        .addDecoder(new Batch.Codec(V4))
        .addDecoder(new Execute.Codec(V4))
        .addDecoder(new Options.Codec(V4))
        .addDecoder(new Prepare.Codec(V4))
        .addDecoder(new Query.Codec(V4))
        .addDecoder(new Register.Codec(V4))
        .addDecoder(new Startup.Codec(V4));

    registry
        .addEncoder(new AuthChallenge.Codec(V4))
        .addEncoder(new Authenticate.Codec(V4))
        .addEncoder(new AuthSuccess.Codec(V4))
        .addEncoder(new Error.Codec(V4))
        .addEncoder(new Event.Codec(V4))
        .addEncoder(new Ready.Codec(V4))
        .addEncoder(new Result.Codec(V4))
        .addEncoder(new Supported.Codec(V4));
  }
}
