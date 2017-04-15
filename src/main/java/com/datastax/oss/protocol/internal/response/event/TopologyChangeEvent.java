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
package com.datastax.oss.protocol.internal.response.event;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Event;
import java.net.InetSocketAddress;

public class TopologyChangeEvent extends Event {

  /** @see ProtocolConstants.TopologyChangeType */
  public final String changeType;

  public final InetSocketAddress address;

  public TopologyChangeEvent(String changeType, InetSocketAddress address) {
    super(ProtocolConstants.EventType.TOPOLOGY_CHANGE);
    this.changeType = changeType;
    this.address = address;
  }

  @Override
  public String toString() {
    return String.format("TopologyChangeEvent(%s, %s)", changeType, address);
  }

  public static class SubCodec extends Event.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.EventType.TOPOLOGY_CHANGE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      TopologyChangeEvent event = (TopologyChangeEvent) message;
      encoder.writeString(event.changeType, dest);
      encoder.writeInet(event.address, dest);
    }

    @Override
    public int encodedSize(Message message) {
      TopologyChangeEvent event = (TopologyChangeEvent) message;
      return PrimitiveSizes.sizeOfString(event.changeType)
          + PrimitiveSizes.sizeOfInet(event.address);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String changeType = decoder.readString(source);
      InetSocketAddress address = decoder.readInet(source);
      return new TopologyChangeEvent(changeType, address);
    }
  }
}
