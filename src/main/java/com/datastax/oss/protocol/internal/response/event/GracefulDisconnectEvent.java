/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.protocol.internal.response.event;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Event;

public class GracefulDisconnectEvent extends Event {
  public GracefulDisconnectEvent() {
    super(ProtocolConstants.EventType.GRACEFUL_DISCONNECT);
  }

  @Override
  public String toString() {
    return "EVENT GRACEFUL_DISCONNECT";
  }

  public static class SubCodec extends Event.SubCodec {

    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.EventType.GRACEFUL_DISCONNECT, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      // no-op: the event has no body, the type string is enough
    }

    @Override
    public int encodedSize(Message message) {
      return 0;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      return new GracefulDisconnectEvent();
    }
  }
}
