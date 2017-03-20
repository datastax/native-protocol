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
package com.datastax.cassandra.protocol.internal;

import com.datastax.cassandra.protocol.internal.request.AuthResponse;
import com.datastax.cassandra.protocol.internal.request.Batch;
import com.datastax.cassandra.protocol.internal.request.Execute;
import com.datastax.cassandra.protocol.internal.request.Options;
import com.datastax.cassandra.protocol.internal.request.Prepare;
import com.datastax.cassandra.protocol.internal.request.Query;
import com.datastax.cassandra.protocol.internal.request.Register;
import com.datastax.cassandra.protocol.internal.request.Startup;
import com.datastax.cassandra.protocol.internal.response.AuthChallenge;
import com.datastax.cassandra.protocol.internal.response.AuthSuccess;
import com.datastax.cassandra.protocol.internal.response.Authenticate;
import com.datastax.cassandra.protocol.internal.response.Error;
import com.datastax.cassandra.protocol.internal.response.Event;
import com.datastax.cassandra.protocol.internal.response.Ready;
import com.datastax.cassandra.protocol.internal.response.Result;
import com.datastax.cassandra.protocol.internal.response.Supported;

import static com.datastax.cassandra.protocol.internal.ProtocolConstants.Version.V4;

public class ProtocolV4ClientCodecs implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addEncoder(new AuthResponse.Codec(V4))
        .addEncoder(new Batch.Codec(V4))
        .addEncoder(new Execute.Codec(V4))
        .addEncoder(new Options.Codec(V4))
        .addEncoder(new Prepare.Codec(V4))
        .addEncoder(new Query.Codec(V4))
        .addEncoder(new Register.Codec(V4))
        .addEncoder(new Startup.Codec(V4));

    registry
        .addDecoder(new AuthChallenge.Codec(V4))
        .addDecoder(new Authenticate.Codec(V4))
        .addDecoder(new AuthSuccess.Codec(V4))
        .addDecoder(new Error.Codec(V4))
        .addDecoder(new Event.Codec(V4))
        .addDecoder(new Ready.Codec(V4))
        .addDecoder(new Result.Codec(V4))
        .addDecoder(new Supported.Codec(V4));
  }
}
