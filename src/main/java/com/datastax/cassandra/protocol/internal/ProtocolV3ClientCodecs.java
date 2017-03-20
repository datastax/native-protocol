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

import static com.datastax.cassandra.protocol.internal.ProtocolConstants.Version.V3;

public class ProtocolV3ClientCodecs implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addEncoder(new AuthResponse.Codec(V3))
        .addEncoder(new Batch.Codec(V3))
        .addEncoder(new Execute.Codec(V3))
        .addEncoder(new Options.Codec(V3))
        .addEncoder(new Prepare.Codec(V3))
        .addEncoder(new Query.Codec(V3))
        .addEncoder(new Register.Codec(V3))
        .addEncoder(new Startup.Codec(V3));

    registry
        .addDecoder(new AuthChallenge.Codec(V3))
        .addDecoder(new Authenticate.Codec(V3))
        .addDecoder(new AuthSuccess.Codec(V3))
        .addDecoder(new Error.Codec(V3))
        .addDecoder(new Event.Codec(V3))
        .addDecoder(new Ready.Codec(V3))
        .addDecoder(new Result.Codec(V3))
        .addDecoder(new Supported.Codec(V3));
  }
}
