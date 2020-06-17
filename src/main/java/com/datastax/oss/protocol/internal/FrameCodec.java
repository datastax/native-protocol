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
   * The header size for protocol v3 and above. Currently, it is the same for all supported protocol
   * versions.
   *
   * <p>If you have a reference to an instance of this class, {@link #encodedHeaderSize(Frame)} is a
   * more future-proof way to get this information.
   */
  public static final int V3_ENCODED_HEADER_SIZE = 9;

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

  /** Allocates a new buffer and encodes the given frame into it. */
  public B encode(Frame frame) {
    int protocolVersion = frame.protocolVersion;

    ProtocolErrors.check(
        protocolVersion >= ProtocolConstants.Version.V4 || frame.customPayload.isEmpty(),
        "Custom payload is not supported in protocol v%d",
        protocolVersion);
    ProtocolErrors.check(
        protocolVersion >= ProtocolConstants.Version.V4 || frame.warnings.isEmpty(),
        "Warnings are not supported in protocol v%d",
        protocolVersion);

    Message.Codec messageEncoder = getMessageEncoder(frame);

    int headerSize = encodedHeaderSize(frame);
    int bodySize = encodedBodySize(frame);
    int flags = computeFlags(frame);
    if (!Flags.contains(flags, ProtocolConstants.FrameFlag.COMPRESSED)) {
      // No compression: we can optimize and do everything with a single allocation
      B dest = primitiveCodec.allocate(headerSize + bodySize);
      encodeInto(frame, bodySize, flags, messageEncoder, dest);
      return dest;
    } else {
      // We need to compress first in order to know the body size
      // 1) Encode uncompressed body
      B uncompressedBody = primitiveCodec.allocate(bodySize);
      encodeBodyInto(frame, messageEncoder, uncompressedBody);

      // 2) Compress and measure size, discard uncompressed buffer
      B compressedBody = compressor.compress(uncompressedBody);
      primitiveCodec.release(uncompressedBody);
      int compressedBodySize = primitiveCodec.sizeOf(compressedBody);

      // 3) Encode final frame
      B header = primitiveCodec.allocate(headerSize);
      encodeHeaderInto(frame, flags, compressedBodySize, header);
      return primitiveCodec.concat(header, compressedBody);
    }
  }

  /**
   * Encodes the given frame into an existing buffer.
   *
   * <p>Note that this method never compresses the frame body; it is intended for protocol v5+,
   * where multiple frames are concatenated into a single buffer and compressed together, instead of
   * individually.
   *
   * <p>The caller is responsible for ensuring that the buffer has enough space remaining, that is:
   * {@link #encodedHeaderSize(Frame)} + {@link #encodedBodySize(Frame)} bytes.
   *
   * @param bodySize the body size to use in the header, if available. This is just an optimization
   *     because the caller may already know it if it has performed the size check above. If not,
   *     pass a negative value, and it will be recomputed.
   */
  public void encodeInto(Frame frame, int bodySize, B dest) {
    int flags = computeFlags(frame);
    Message.Codec encoder = getMessageEncoder(frame);
    encodeInto(frame, bodySize, flags, encoder, dest);
  }

  private Message.Codec getMessageEncoder(Frame frame) {
    Message.Codec encoder = encoders.get(frame.protocolVersion, frame.message.opcode);
    ProtocolErrors.check(
        encoder != null,
        "Unsupported opcode %s in protocol v%d",
        frame.message.opcode,
        frame.protocolVersion);
    return encoder;
  }

  private int computeFlags(Frame frame) {
    int flags = 0;
    if (!(compressor instanceof NoopCompressor)
        && frame.message.opcode != ProtocolConstants.Opcode.STARTUP
        && frame.message.opcode != ProtocolConstants.Opcode.OPTIONS) {
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
    if (frame.protocolVersion == ProtocolConstants.Version.BETA) {
      flags = Flags.add(flags, ProtocolConstants.FrameFlag.USE_BETA);
    }
    return flags;
  }

  private void encodeInto(
      Frame frame, int bodySize, int flags, Message.Codec messageEncoder, B dest) {
    encodeHeaderInto(frame, flags, bodySize, dest);
    encodeBodyInto(frame, messageEncoder, dest);
  }

  private void encodeHeaderInto(Frame frame, int flags, int bodySize, B dest) {
    if (bodySize < 0) {
      bodySize = encodedBodySize(frame);
    }

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
    primitiveCodec.writeInt(bodySize, dest);
  }

  private void encodeBodyInto(Frame frame, Message.Codec messageEncoder, B dest) {
    encodeTracingId(frame.tracingId, dest);
    encodeCustomPayload(frame.customPayload, dest);
    encodeWarnings(frame.warnings, dest);
    messageEncoder.encode(dest, frame.message, primitiveCodec);
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

  /** How many bytes are needed to encode the given frame's header. */
  public int encodedHeaderSize(@SuppressWarnings("unused") Frame frame) {
    return V3_ENCODED_HEADER_SIZE;
  }

  /**
   * How many bytes are needed to encode the given frame's body (message + tracing id, custom
   * payload and/or warnings if relevant).
   */
  public int encodedBodySize(Frame frame) {
    int size = 0;
    if (frame.tracingId != null) {
      size += PrimitiveSizes.UUID;
    }
    if (!frame.customPayload.isEmpty()) {
      size += PrimitiveSizes.sizeOfBytesMap(frame.customPayload);
    }
    if (!frame.warnings.isEmpty()) {
      size += PrimitiveSizes.sizeOfStringList(frame.warnings);
    }

    Message.Codec encoder = getMessageEncoder(frame);
    return size + encoder.encodedSize(frame.message);
  }

  /**
   * Decodes the size of the body of the next frame contained in the given buffer.
   *
   * <p>The buffer must contain at least the frame's header. This method performs a relative read,
   * it will not consume any data from the buffer.
   */
  public int decodeBodySize(B source) {
    return primitiveCodec.readInt(source, V3_ENCODED_HEADER_SIZE - 4);
  }

  /**
   * Decodes the next frame from the given buffer.
   *
   * <p>The buffer must contain at least one complete frame. It may be followed by additional data
   * (which will not be consumed).
   */
  public Frame decode(B source) {
    int directionAndVersion = primitiveCodec.readByte(source);
    boolean isResponse = (directionAndVersion & 0b1000_0000) == 0b1000_0000;
    int protocolVersion = directionAndVersion & 0b0111_1111;
    int flags = primitiveCodec.readByte(source);
    boolean beta = Flags.contains(flags, ProtocolConstants.FrameFlag.USE_BETA);
    int streamId = readStreamId(source);
    int opcode = primitiveCodec.readByte(source);
    int length = primitiveCodec.readInt(source);

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
      frameSize = V3_ENCODED_HEADER_SIZE + primitiveCodec.sizeOf(source);
      compressedFrameSize =
          V3_ENCODED_HEADER_SIZE + length; // what we measured before decompressing
    } else {
      frameSize = V3_ENCODED_HEADER_SIZE + length;
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
