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
package com.datastax.oss.protocol.internal;

import com.datastax.oss.protocol.internal.util.Flags;
import com.datastax.oss.protocol.internal.util.IntIntMap;
import java.nio.ByteBuffer;
import java.util.Collections;
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

    int flags = 0;
    if (!(compressor instanceof NoopCompressor) && opcode != ProtocolConstants.Opcode.STARTUP) {
      flags = Flags.add(flags, ProtocolConstants.FrameFlag.COMPRESSED);
    }
    if (frame.tracing || frame.tracingId != null) {
      flags = Flags.add(flags, ProtocolConstants.FrameFlag.TRACING);
    }
    if (!frame.customPayload.isEmpty()) {
      flags = Flags.add(flags, ProtocolConstants.FrameFlag.CUSTOM_PAYLOAD);
    }
    if (!frame.warnings.isEmpty()) {
      flags = Flags.add(flags, ProtocolConstants.FrameFlag.WARNING);
    }
    if (protocolVersion == ProtocolConstants.Version.BETA) {
      flags = Flags.add(flags, ProtocolConstants.FrameFlag.USE_BETA);
    }

    int headerSize = headerEncodedSize();
    if (!Flags.contains(flags, ProtocolConstants.FrameFlag.COMPRESSED)) {
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

  public static int headerEncodedSize() {
    return 9;
  }

  private void encodeHeader(Frame frame, int flags, int messageSize, B dest) {
    int versionAndDirection = frame.protocolVersion;
    if (frame.message.isResponse) {
      versionAndDirection |= 0b1000_0000;
    }
    primitiveCodec.writeByte((byte) versionAndDirection, dest);
    primitiveCodec.writeByte((byte) flags, dest);
    primitiveCodec.writeUnsignedShort(
        frame.streamId & 0xFFFF, // see readStreamId()
        dest);
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
    int flags = primitiveCodec.readByte(source);
    boolean beta = Flags.contains(flags, ProtocolConstants.FrameFlag.USE_BETA);
    int streamId = readStreamId(source);
    int opcode = primitiveCodec.readByte(source);
    int length = primitiveCodec.readInt(source);

    int actualLength = primitiveCodec.sizeOf(source);
    ProtocolErrors.check(
        length == actualLength,
        "Declared length in header (%d) does not match actual length (%d)",
        length,
        actualLength);

    boolean decompressed = false;
    if (Flags.contains(flags, ProtocolConstants.FrameFlag.COMPRESSED)) {
      B newSource = compressor.decompress(source);
      // if decompress returns a different object, track this so we know to release it when done.
      if (newSource != source) {
        decompressed = true;
        source = newSource;
      }
    }

    int frameSize;
    int compressedFrameSize;
    if (decompressed) {
      frameSize = headerEncodedSize() + primitiveCodec.sizeOf(source);
      compressedFrameSize = headerEncodedSize() + length; // what we measured before decompressing
    } else {
      frameSize = headerEncodedSize() + length;
      compressedFrameSize = -1;
    }

    boolean isTracing = Flags.contains(flags, ProtocolConstants.FrameFlag.TRACING);
    UUID tracingId = (isResponse && isTracing) ? primitiveCodec.readUuid(source) : null;

    Map<String, ByteBuffer> customPayload =
        (Flags.contains(flags, ProtocolConstants.FrameFlag.CUSTOM_PAYLOAD))
            ? primitiveCodec.readBytesMap(source)
            : Collections.emptyMap();

    List<String> warnings =
        (isResponse && Flags.contains(flags, ProtocolConstants.FrameFlag.WARNING))
            ? primitiveCodec.readStringList(source)
            : Collections.emptyList();

    Message.Codec decoder = decoders.get(protocolVersion, opcode);
    ProtocolErrors.check(
        decoder != null, "Unsupported request opcode: %s in protocol %d", opcode, protocolVersion);
    Message response = decoder.decode(source, primitiveCodec);

    if (decompressed) {
      primitiveCodec.release(source);
    }

    return new Frame(
        protocolVersion,
        beta,
        streamId,
        isTracing,
        tracingId,
        frameSize,
        compressedFrameSize,
        customPayload,
        warnings,
        response);
  }

  private int readStreamId(B source) {
    int id = primitiveCodec.readUnsignedShort(source);
    // The protocol spec states that the stream id is a [short], but this is wrong: the stream id
    // is signed. Rather than adding a `readSignedShort` to PrimitiveCodec for this edge case,
    // handle the conversion here.
    return (short) id;
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
