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
package com.datastax.cassandra.protocol.internal.request;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import java.util.HashMap;
import java.util.Map;

public class Startup extends Message {
  private static final String CQL_VERSION_KEY = "CQL_VERSION";
  private static final String COMPRESSION_KEY = "COMPRESSION";

  private static final String CQL_VERSION = "3.0.0";

  public final Map<String, String> options;

  public Startup(String compressionAlgorithm) {
    super(false, ProtocolConstants.Opcode.STARTUP);
    this.options = new HashMap<>();
    this.options.put(CQL_VERSION_KEY, CQL_VERSION);
    if (compressionAlgorithm != null) {
      this.options.put(COMPRESSION_KEY, compressionAlgorithm);
    }
  }

  public Startup() {
    this(null);
  }

  @Override
  public String toString() {
    return "STARTUP " + options;
  }

  public static class Codec extends Message.Codec {

    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.STARTUP, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Startup startup = (Startup) message;
      encoder.writeStringMap(startup.options, dest);
    }

    @Override
    public int encodedSize(Message message) {
      Startup startup = (Startup) message;
      return PrimitiveSizes.sizeOfStringMap(startup.options);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      throw new UnsupportedOperationException("TODO");
    }
  }
}
