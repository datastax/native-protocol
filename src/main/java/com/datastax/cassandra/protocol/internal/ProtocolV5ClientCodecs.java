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

import com.datastax.cassandra.protocol.internal.request.*;
import com.datastax.cassandra.protocol.internal.response.*;
import com.datastax.cassandra.protocol.internal.response.Error;

import static com.datastax.cassandra.protocol.internal.ProtocolConstants.Version.V5;

public class ProtocolV5ClientCodecs implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addEncoder(new AuthResponse.Codec(V5))
        .addEncoder(new Batch.Codec(V5))
        .addEncoder(new Execute.Codec(V5))
        .addEncoder(new Options.Codec(V5))
        .addEncoder(new Prepare.Codec(V5))
        .addEncoder(new Query.Codec(V5))
        .addEncoder(new Register.Codec(V5))
        .addEncoder(new Startup.Codec(V5));

    registry
        .addDecoder(new AuthChallenge.Codec(V5))
        .addDecoder(new Authenticate.Codec(V5))
        .addDecoder(new AuthSuccess.Codec(V5))
        .addDecoder(new Error.Codec(V5))
        .addDecoder(new Event.Codec(V5))
        .addDecoder(new Ready.Codec(V5))
        .addDecoder(new Result.Codec(V5))
        .addDecoder(new Supported.Codec(V5));
  }
}
