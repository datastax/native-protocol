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

import com.datastax.cassandra.protocol.internal.util.IntIntMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FrameCodec<B> {

  /**
   * Builds a new instance with the default codecs for a client (encoding requests, decoding
   * responses).
   */
  public static <B> FrameCodec<B> defaultClient(
      PrimitiveCodec<B> primitiveCodec, Compressor<B> compressor) {
    return new FrameCodec<>(
        primitiveCodec, compressor, new ProtocolV3ClientCodecs(), new ProtocolV4ClientCodecs());
  }

  /**
   * Builds a new instance with the default codecs for a server (decoding requests, encoding
   * responses).
   */
  public static <B> FrameCodec<B> defaultServer(
      PrimitiveCodec<B> primitiveCodec, Compressor<B> compressor) {
    return new FrameCodec<>(
        primitiveCodec, compressor, new ProtocolV3ServerCodecs(), new ProtocolV4ServerCodecs());
  }

  private final PrimitiveCodec<B> primitiveCodec;
  private final Compressor<B> compressor;
  private final IntIntMap<Message.Codec> encoders;
  private final IntIntMap<Message.Codec> decoders;

  public FrameCodec(
      PrimitiveCodec<B> primitiveCodec, Compressor<B> compressor, CodecGroup... codecGroups) {
    this.primitiveCodec = primitiveCodec;
    this.compressor = compressor;
    IntIntMap.Builder<Message.Codec> encodersBuilder = IntIntMap.builder();
    IntIntMap.Builder<Message.Codec> decodersBuilder = IntIntMap.builder();
    CodecGroup.Registry registry =
        new CodecGroup.Registry() {
          @Override
          public CodecGroup.Registry addCodec(Message.Codec codec) {
            addEncoder(codec);
            addDecoder(codec);
            return this;
          }

          @Override
          public CodecGroup.Registry addEncoder(Message.Codec codec) {
            encodersBuilder.put(codec.protocolVersion, codec.opcode, codec);
            return this;
          }

          @Override
          public CodecGroup.Registry addDecoder(Message.Codec codec) {
            decodersBuilder.put(codec.protocolVersion, codec.opcode, codec);
            return this;
          }
        };
    for (CodecGroup codecGroup : codecGroups) {
      codecGroup.registerCodecs(registry);
    }
    this.encoders = encodersBuilder.build();
    this.decoders = decodersBuilder.build();
  }

  public B encode(Frame frame) {
    int protocolVersion = frame.protocolVersion;
    Message request = frame.message;
    int opcode = request.opcode;
    Message.Codec encoder = encoders.get(protocolVersion, opcode);
    ProtocolErrors.check(
        encoder != null, "Unsupported request opcode: %s in protocol %d", opcode, protocolVersion);

    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    if (compressor != null && opcode != ProtocolConstants.Opcode.STARTUP) {
      flags.add(Flag.COMPRESSED);
    }
    if (frame.tracing) {
      flags.add(Flag.TRACING);
    }
    if (!frame.customPayload.isEmpty()) {
      flags.add(Flag.CUSTOM_PAYLOAD);
    }

    int headerSize = headerEncodedSize();
    if (!flags.contains(Flag.COMPRESSED)) {
      // No compression: we can optimize and do everything with a single allocation
      int messageSize = encoder.encodedSize(request);
      if (!frame.customPayload.isEmpty()) {
        messageSize += PrimitiveSizes.sizeOfBytesMap(frame.customPayload);
      }
      B dest = primitiveCodec.allocate(headerSize + messageSize);
      encodeHeader(dest, frame, flags, messageSize);
      encodeCustomPayload(dest, frame.customPayload);
      encoder.encode(dest, request, primitiveCodec);
      return dest;
    } else {
      // We need to compress first in order to know the body size
      // 1) Encode uncompressed message
      int uncompressedMessageSize = encoder.encodedSize(request);
      if (!frame.customPayload.isEmpty()) {
        uncompressedMessageSize += PrimitiveSizes.sizeOfBytesMap(frame.customPayload);
      }
      B uncompressedMessage = primitiveCodec.allocate(uncompressedMessageSize);
      encodeCustomPayload(uncompressedMessage, frame.customPayload);
      encoder.encode(uncompressedMessage, request, primitiveCodec);

      // 2) Compress and measure size, discard uncompressed buffer
      assert compressor != null; // avoid compile warning
      B compressedMessage = compressor.compress(uncompressedMessage);
      primitiveCodec.release(uncompressedMessage);
      int messageSize = primitiveCodec.sizeOf(compressedMessage);

      // 3) Encode final frame
      B header = primitiveCodec.allocate(headerSize);
      encodeHeader(header, frame, flags, messageSize);
      return primitiveCodec.concat(header, compressedMessage);
    }
  }

  private static int headerEncodedSize() {
    return 9;
  }

  private void encodeHeader(B dest, Frame frame, EnumSet<Flag> flags, int messageSize) {
    primitiveCodec.writeByte((byte) frame.protocolVersion, dest);
    primitiveCodec.writeByte((byte) Flag.encode(flags), dest);
    primitiveCodec.writeUnsignedShort(frame.streamId, dest);
    primitiveCodec.writeByte((byte) frame.message.opcode, dest);
    primitiveCodec.writeInt(messageSize, dest);
  }

  private void encodeCustomPayload(B dest, Map<String, ByteBuffer> customPayload) {
    if (!customPayload.isEmpty()) {
      primitiveCodec.writeBytesMap(customPayload, dest);
    }
  }

  public Frame decode(B source) {
    int directionAndVersion = primitiveCodec.readByte(source);
    // first bit = direction, should be 1 for "incoming"
    ProtocolErrors.check((directionAndVersion & 0x80) == 0x80, "Can only decode response frames");

    int protocolVersion = directionAndVersion & 0x7F;
    EnumSet<Flag> flags = Flag.decode(primitiveCodec.readByte(source));
    int streamId = readStreamId(source);
    int opcode = primitiveCodec.readByte(source);
    int length = primitiveCodec.readInt(source);

    int actualLength = primitiveCodec.sizeOf(source);
    ProtocolErrors.check(
        length == actualLength,
        "Declared length in header (%d) does not match actual length (%d)",
        length,
        actualLength);

    if (flags.contains(Flag.COMPRESSED)) {
      source = compressor.decompress(source);
    }

    UUID tracingId = (flags.contains(Flag.TRACING)) ? primitiveCodec.readUuid(source) : null;

    Map<String, ByteBuffer> customPayload =
        (flags.contains(Flag.CUSTOM_PAYLOAD))
            ? primitiveCodec.readBytesMap(source)
            : Collections.emptyMap();

    List<String> warnings =
        (flags.contains(Flag.WARNING))
            ? primitiveCodec.readStringList(source)
            : Collections.emptyList();

    Message.Codec decoder = decoders.get(protocolVersion, opcode);
    Message response = decoder.decode(source, primitiveCodec);

    return new Frame(protocolVersion, streamId, tracingId, customPayload, warnings, response);
  }

  private int readStreamId(B source) {
    return primitiveCodec.readUnsignedShort(source);
  }

  enum Flag {
    COMPRESSED(0x01),
    TRACING(0x02),
    CUSTOM_PAYLOAD(0x04),
    WARNING(0x08);

    private int mask;

    Flag(int mask) {
      this.mask = mask;
    }

    static EnumSet<Flag> decode(int flags) {
      EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
      for (Flag flag : Flag.values()) {
        if ((flags & flag.mask) != 0) {
          set.add(flag);
        }
      }
      return set;
    }

    static int encode(EnumSet<Flag> flags) {
      int i = 0;
      for (Flag flag : flags) {
        i |= flag.mask;
      }
      return i;
    }
  }

  /**
   * Intermediary class to pass request/response codecs to the frame codec.
   *
   * <p>This is just so that we can have the codecs nicely grouped by protocol version.
   */
  public interface CodecGroup {
    interface Registry {
      Registry addCodec(Message.Codec codec);

      /**
       * Add a codec for encoding only; this helps catch programming errors if the client is only
       * supposed to send a subset of the existing messages.
       */
      Registry addEncoder(Message.Codec codec);

      /**
       * Add a codec for decoding only; this helps catch programming errors if the client is only
       * supposed to receive a subset of the existing messages.
       */
      Registry addDecoder(Message.Codec codec);
    }

    void registerCodecs(Registry registry);
  }
}
