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
package com.datastax.oss.protocol.internal.request;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Map;

public class Startup extends Message {
  public static final String CQL_VERSION_KEY = "CQL_VERSION";
  public static final String COMPRESSION_KEY = "COMPRESSION";

  private static final String CQL_VERSION = "3.0.0";

  public final Map<String, String> options;

  public Startup(String compressionAlgorithm) {
    this(
        (compressionAlgorithm == null || compressionAlgorithm.isEmpty())
            ? NullAllowingImmutableMap.of(CQL_VERSION_KEY, CQL_VERSION)
            : NullAllowingImmutableMap.of(
                CQL_VERSION_KEY, CQL_VERSION, COMPRESSION_KEY, compressionAlgorithm));
  }

  public Startup() {
    this((Map<String, String>) null);
  }

  public Startup(Map<String, String> options) {
    super(false, ProtocolConstants.Opcode.STARTUP);
    if (options != null) {
      if (options.containsKey(CQL_VERSION_KEY)) {
        this.options = NullAllowingImmutableMap.copyOf(options);
      } else {
        NullAllowingImmutableMap.Builder<String, String> builder =
            NullAllowingImmutableMap.builder(options.size() + 1);
        this.options = builder.put(CQL_VERSION_KEY, CQL_VERSION).putAll(options).build();
      }
    } else {
      this.options = NullAllowingImmutableMap.of(CQL_VERSION_KEY, CQL_VERSION);
    }
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
      Map<String, String> map = decoder.readStringMap(source);
      return new Startup(map);
    }
  }
}
