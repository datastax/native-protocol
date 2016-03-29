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
package com.datastax.cassandra.protocol.internal.response.error;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.response.Error;

public class Unavailable extends Error {
  /** The consistency level of the query that triggered the exception. */
  public final int consistencyLevel;
  /** The number of nodes that should be alive to respect {@code consistencyLevel}. */
  public final int required;
  /**
   * The number of replicas that were known to be alive when the request was processed (since an
   * unavailable exception has been triggered, {@code alive < required}.
   */
  public final int alive;

  public Unavailable(String message, int consistencyLevel, int required, int alive) {
    super(ProtocolConstants.ErrorCode.UNAVAILABLE, message);
    this.consistencyLevel = consistencyLevel;
    this.required = required;
    this.alive = alive;
  }

  public static class SubCodec extends Error.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ErrorCode.UNAVAILABLE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int encodedSize(Message message) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String message = decoder.readString(source);
      int consistencyLevel = decoder.readUnsignedShort(source);
      int required = decoder.readInt(source);
      int alive = decoder.readInt(source);
      return new Unavailable(message, consistencyLevel, required, alive);
    }
  }
}
