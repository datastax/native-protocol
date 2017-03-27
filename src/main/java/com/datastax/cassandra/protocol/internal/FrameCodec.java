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
        primitiveCodec,
        compressor,
        new ProtocolV3ClientCodecs(),
        new ProtocolV4ClientCodecs(),
        new ProtocolV5ClientCodecs());
  }

  /**
   * Builds a new instance with the default codecs for a server (decoding requests, encoding
   * responses).
   */
  public static <B> FrameCodec<B> defaultServer(
      PrimitiveCodec<B> primitiveCodec, Compressor<B> compressor) {
    return new FrameCodec<>(
        primitiveCodec,
        compressor,
        new ProtocolV3ServerCodecs(),
        new ProtocolV4ServerCodecs(),
        new ProtocolV5ServerCodecs());
  }

  private final PrimitiveCodec<B> primitiveCodec;
  private final Compressor<B> compressor;
  private final IntIntMap<Message.Codec> encoders;
  private final IntIntMap<Message.Codec> decoders;

  public FrameCodec(
      PrimitiveCodec<B> primitiveCodec, Compressor<B> compressor, CodecGroup... codecGroups) {
    ProtocolErrors.check(primitiveCodec != null, "primitiveCodec can't be null");
    ProtocolErrors.check(compressor != null, "compressor can't be null, use Compressor.none()");
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

    ProtocolErrors.check(
        protocolVersion >= ProtocolConstants.Version.V4 || frame.customPayload.isEmpty(),
        "Custom payload is not supported in protocol v%d",
        protocolVersion);
    ProtocolErrors.check(
        protocolVersion >= ProtocolConstants.Version.V4 || frame.warnings.isEmpty(),
        "Warnings are not supported in protocol v%d",
        protocolVersion);

    int opcode = request.opcode;
    Message.Codec encoder = encoders.get(protocolVersion, opcode);
    ProtocolErrors.check(
        encoder != null, "Unsupported opcode %s in protocol v%d", opcode, protocolVersion);

    EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
    if (!(compressor instanceof NoopCompressor) && opcode != ProtocolConstants.Opcode.STARTUP) {
      flags.add(Flag.COMPRESSED);
    }
    if (frame.tracing || frame.tracingId != null) {
      flags.add(Flag.TRACING);
    }
    if (!frame.customPayload.isEmpty()) {
      flags.add(Flag.CUSTOM_PAYLOAD);
    }
    if (!frame.warnings.isEmpty()) {
      flags.add(Flag.WARNING);
    }
    if (protocolVersion == ProtocolConstants.Version.BETA) {
      flags.add(Flag.USE_BETA);
    }

    int headerSize = headerEncodedSize();
    if (!flags.contains(Flag.COMPRESSED)) {
      // No compression: we can optimize and do everything with a single allocation
      int messageSize = encoder.encodedSize(request);
      if (frame.tracingId != null) {
        messageSize += PrimitiveSizes.UUID;
      }
      if (!frame.customPayload.isEmpty()) {
        messageSize += PrimitiveSizes.sizeOfBytesMap(frame.customPayload);
      }
      if (!frame.warnings.isEmpty()) {
        messageSize += PrimitiveSizes.sizeOfStringList(frame.warnings);
      }
      B dest = primitiveCodec.allocate(headerSize + messageSize);
      encodeHeader(frame, flags, messageSize, dest);
      encodeTracingId(frame.tracingId, dest);
      encodeCustomPayload(frame.customPayload, dest);
      encodeWarnings(frame.warnings, dest);
      encoder.encode(dest, request, primitiveCodec);
      return dest;
    } else {
      // We need to compress first in order to know the body size
      // 1) Encode uncompressed message
      int uncompressedMessageSize = encoder.encodedSize(request);
      if (frame.tracingId != null) {
        uncompressedMessageSize += PrimitiveSizes.UUID;
      }
      if (!frame.customPayload.isEmpty()) {
        uncompressedMessageSize += PrimitiveSizes.sizeOfBytesMap(frame.customPayload);
      }
      if (!frame.warnings.isEmpty()) {
        uncompressedMessageSize += PrimitiveSizes.sizeOfStringList(frame.warnings);
      }
      B uncompressedMessage = primitiveCodec.allocate(uncompressedMessageSize);
      encodeTracingId(frame.tracingId, uncompressedMessage);
      encodeCustomPayload(frame.customPayload, uncompressedMessage);
      encodeWarnings(frame.warnings, uncompressedMessage);
      encoder.encode(uncompressedMessage, request, primitiveCodec);

      // 2) Compress and measure size, discard uncompressed buffer
      B compressedMessage = compressor.compress(uncompressedMessage);
      primitiveCodec.release(uncompressedMessage);
      int messageSize = primitiveCodec.sizeOf(compressedMessage);

      // 3) Encode final frame
      B header = primitiveCodec.allocate(headerSize);
      encodeHeader(frame, flags, messageSize, header);
      return primitiveCodec.concat(header, compressedMessage);
    }
  }

  private static int headerEncodedSize() {
    return 9;
  }

  private void encodeHeader(Frame frame, EnumSet<Flag> flags, int messageSize, B dest) {
    int versionAndDirection = frame.protocolVersion;
    if (frame.message.isResponse) {
      versionAndDirection |= 0b1000_0000;
    }
    primitiveCodec.writeByte((byte) versionAndDirection, dest);
    Flag.encode(flags, dest, primitiveCodec, frame.protocolVersion);
    primitiveCodec.writeUnsignedShort(frame.streamId, dest);
    primitiveCodec.writeByte((byte) frame.message.opcode, dest);
    primitiveCodec.writeInt(messageSize, dest);
  }

  private void encodeTracingId(UUID tracingId, B dest) {
    if (tracingId != null) {
      primitiveCodec.writeUuid(tracingId, dest);
    }
  }

  private void encodeCustomPayload(Map<String, ByteBuffer> customPayload, B dest) {
    if (!customPayload.isEmpty()) {
      primitiveCodec.writeBytesMap(customPayload, dest);
    }
  }

  private void encodeWarnings(List<String> warnings, B dest) {
    if (!warnings.isEmpty()) {
      primitiveCodec.writeStringList(warnings, dest);
    }
  }

  public Frame decode(B source) {
    int directionAndVersion = primitiveCodec.readByte(source);
    boolean isResponse = (directionAndVersion & 0b1000_0000) == 0b1000_0000;
    int protocolVersion = directionAndVersion & 0b0111_1111;
    EnumSet<Flag> flags = Flag.decode(source, primitiveCodec, protocolVersion);
    boolean beta = flags.contains(Flag.USE_BETA);
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

    boolean isTracing = flags.contains(Flag.TRACING);
    UUID tracingId = (isResponse && isTracing) ? primitiveCodec.readUuid(source) : null;

    Map<String, ByteBuffer> customPayload =
        (flags.contains(Flag.CUSTOM_PAYLOAD))
            ? primitiveCodec.readBytesMap(source)
            : Collections.emptyMap();

    List<String> warnings =
        (isResponse && flags.contains(Flag.WARNING))
            ? primitiveCodec.readStringList(source)
            : Collections.emptyList();

    Message.Codec decoder = decoders.get(protocolVersion, opcode);
    ProtocolErrors.check(
        decoder != null, "Unsupported request opcode: %s in protocol %d", opcode, protocolVersion);
    Message response = decoder.decode(source, primitiveCodec);

    return new Frame(
        protocolVersion, beta, streamId, isTracing, tracingId, customPayload, warnings, response);
  }

  private int readStreamId(B source) {
    return primitiveCodec.readUnsignedShort(source);
  }

  enum Flag {
    COMPRESSED(0x01),
    TRACING(0x02),
    CUSTOM_PAYLOAD(0x04),
    WARNING(0x08),
    USE_BETA(0x10);

    private int mask;

    Flag(int mask) {
      this.mask = mask;
    }

    static <B> EnumSet<Flag> decode(B source, PrimitiveCodec<B> decoder, int protocolVersion) {
      int bits = decoder.readByte(source);
      EnumSet<Flag> set = EnumSet.noneOf(Flag.class);
      for (Flag flag : Flag.values()) {
        if ((bits & flag.mask) != 0) {
          set.add(flag);
        }
      }
      return set;
    }

    static <B> void encode(
        EnumSet<Flag> flags, B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      int bits = 0;
      for (Flag flag : flags) {
        bits |= flag.mask;
      }
      encoder.writeByte((byte) bits, dest);
    }
  }

  /**
   * Intermediary class to pass request/response codecs to the frame codec.
   *
   * <p>
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
