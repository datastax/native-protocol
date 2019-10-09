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
package com.datastax.dse.protocol.internal.request;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;

public class Revise extends Message {

  public static Revise cancelContinuousPaging(int streamId) {
    return new Revise(DseProtocolConstants.RevisionType.CANCEL_CONTINUOUS_PAGING, streamId, -1);
  }

  public static Revise requestMoreContinuousPages(int streamId, int amount) {
    return new Revise(DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES, streamId, amount);
  }

  /** @see DseProtocolConstants.RevisionType */
  public final int revisionType;

  public final int streamId;
  public final int nextPages;

  public Revise(int revisionType, int streamId, int nextPages) {
    super(false, DseProtocolConstants.Opcode.REVISE_REQUEST);
    this.revisionType = revisionType;
    this.streamId = streamId;
    this.nextPages = nextPages;
  }

  public static class Codec extends Message.Codec {

    public Codec(int protocolVersion) {
      super(DseProtocolConstants.Opcode.REVISE_REQUEST, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Revise revise = (Revise) message;
      encoder.writeInt(revise.revisionType, dest);
      encoder.writeInt(revise.streamId, dest);
      if (revise.revisionType == DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES) {
        encoder.writeInt(revise.nextPages, dest);
      }
    }

    @Override
    public int encodedSize(Message message) {
      Revise revise = (Revise) message;
      return PrimitiveSizes.INT
          * (revise.revisionType == DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES
              ? 3
              : 2);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      int revisionType = decoder.readInt(source);
      int streamId = decoder.readInt(source);
      int nextPages =
          (revisionType == DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES)
              ? decoder.readInt(source)
              : -1;
      return new Revise(revisionType, streamId, nextPages);
    }
  }
}
