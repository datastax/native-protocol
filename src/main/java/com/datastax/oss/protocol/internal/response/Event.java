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
package com.datastax.oss.protocol.internal.response;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolErrors;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.event.TopologyChangeEvent;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class Event extends Message {
  /** @see ProtocolConstants.EventType */
  public final String type;

  protected Event(String type) {
    super(true, ProtocolConstants.Opcode.EVENT);
    this.type = type;
  }

  public static class Codec extends Message.Codec {
    private final Map<String, SubCodec> subDecoders;

    public Codec(int protocolVersion, SubCodec... subCodecs) {
      super(ProtocolConstants.Opcode.EVENT, protocolVersion);
      Map<String, SubCodec> tmp = new HashMap<>();
      for (SubCodec subCodec : subCodecs) {
        tmp.put(subCodec.type, subCodec);
      }
      this.subDecoders = Collections.unmodifiableMap(tmp);
    }

    /** Creates an instance with subdecoders for the default types. */
    public Codec(int protocolVersion) {
      this(
          protocolVersion,
          new TopologyChangeEvent.SubCodec(protocolVersion),
          new StatusChangeEvent.SubCodec(protocolVersion),
          new SchemaChangeEvent.SubCodec(protocolVersion));
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Event event = (Event) message;
      encoder.writeString(event.type, dest);
      getSubCodec(event.type).encode(dest, message, encoder);
    }

    @Override
    public int encodedSize(Message message) {
      Event event = (Event) message;
      return PrimitiveSizes.sizeOfString(event.type) + getSubCodec(event.type).encodedSize(message);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String type = decoder.readString(source);
      return getSubCodec(type).decode(source, decoder);
    }

    private SubCodec getSubCodec(String type) {
      SubCodec subCodec = subDecoders.get(type);
      ProtocolErrors.check(subCodec != null, "Unsupported event type: %s", type);
      return subCodec;
    }
  }

  public abstract static class SubCodec {
    public final String type;
    public final int protocolVersion;

    protected SubCodec(String type, int protocolVersion) {
      this.type = type;
      this.protocolVersion = protocolVersion;
    }

    public abstract <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder);

    public abstract int encodedSize(Message message);

    public abstract <B> Message decode(B source, PrimitiveCodec<B> decoder);
  }
}
