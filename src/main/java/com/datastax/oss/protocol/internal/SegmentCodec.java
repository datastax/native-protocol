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

import com.datastax.oss.protocol.internal.util.Crc;
import java.util.List;

public class SegmentCodec<B> {

  private static final int COMPRESSED_HEADER_LENGTH = 5;
  private static final int UNCOMPRESSED_HEADER_LENGTH = 3;

  public static final int CRC24_LENGTH = 3;
  public static final int CRC32_LENGTH = 4;

  private final PrimitiveCodec<B> primitiveCodec;
  private final Compressor<B> compressor;
  private final boolean compress;

  public SegmentCodec(PrimitiveCodec<B> primitiveCodec, Compressor<B> compressor) {
    this.primitiveCodec = primitiveCodec;
    this.compressor = compressor;
    this.compress = !(compressor instanceof NoopCompressor);
  }

  /** The length of the segment header, excluding the 3-byte trailing CRC. */
  public int headerLength() {
    return compress ? COMPRESSED_HEADER_LENGTH : UNCOMPRESSED_HEADER_LENGTH;
  }

  public void encode(Segment<B> segment, List<Object> out) {
    B uncompressedPayload = segment.payload;
    int uncompressedPayloadLength = primitiveCodec.sizeOf(uncompressedPayload);
    assert uncompressedPayloadLength <= Segment.MAX_PAYLOAD_LENGTH;
    B encodedPayload;
    if (compress) {
      primitiveCodec.markReaderIndex(uncompressedPayload);
      B compressedPayload = compressor.compressWithoutLength(uncompressedPayload);
      if (primitiveCodec.sizeOf(compressedPayload) >= uncompressedPayloadLength) {
        // Skip compression if it's not worth it
        primitiveCodec.resetReaderIndex(uncompressedPayload);
        encodedPayload = uncompressedPayload;
        primitiveCodec.release(compressedPayload);
        // By convention, this is how we signal this:
        uncompressedPayloadLength = 0;
      } else {
        encodedPayload = compressedPayload;
        primitiveCodec.release(uncompressedPayload);
      }
    } else {
      encodedPayload = uncompressedPayload;
    }
    int payloadLength = primitiveCodec.sizeOf(encodedPayload);

    B header = encodeHeader(payloadLength, uncompressedPayloadLength, segment.isSelfContained);

    int payloadCrc = Crc.computeCrc32(encodedPayload, primitiveCodec);
    B trailer = primitiveCodec.allocate(CRC32_LENGTH);
    for (int i = 0; i < CRC32_LENGTH; i++) {
      primitiveCodec.writeByte((byte) (payloadCrc & 0xFF), trailer);
      payloadCrc >>= 8;
    }

    out.add(header);
    out.add(encodedPayload);
    out.add(trailer);
  }

  // Visible for testing
  B encodeHeader(int payloadLength, int uncompressedLength, boolean isSelfContained) {
    assert payloadLength <= Segment.MAX_PAYLOAD_LENGTH;

    int headerLength = headerLength();

    long headerData = payloadLength;
    int flagOffset = 17;
    if (compress) {
      headerData |= (long) uncompressedLength << 17;
      flagOffset += 17;
    }
    if (isSelfContained) {
      headerData |= 1L << flagOffset;
    }

    int headerCrc = Crc.computeCrc24(headerData, headerLength);

    B header = primitiveCodec.allocate(headerLength + CRC24_LENGTH);
    // Write both data and CRC in little-endian order
    for (int i = 0; i < headerLength; i++) {
      int shift = i * 8;
      primitiveCodec.writeByte((byte) (headerData >> shift & 0xFF), header);
    }
    for (int i = 0; i < CRC24_LENGTH; i++) {
      int shift = i * 8;
      primitiveCodec.writeByte((byte) (headerCrc >> shift & 0xFF), header);
    }
    return header;
  }
  /**
   * Decodes a segment header and checks its CRC. It is assumed that the caller has already checked
   * that there are enough bytes.
   */
  public Header decodeHeader(B source) throws CrcMismatchException {
    int headerLength = headerLength();
    assert primitiveCodec.sizeOf(source) >= headerLength + CRC24_LENGTH;

    // Read header data (little endian):
    long headerData = 0;
    for (int i = 0; i < headerLength; i++) {
      headerData |= (primitiveCodec.readByte(source) & 0xFFL) << (8 * i);
    }

    // Read CRC (little endian) and check it:
    int expectedHeaderCrc = 0;
    for (int i = 0; i < CRC24_LENGTH; i++) {
      expectedHeaderCrc |= (primitiveCodec.readByte(source) & 0xFF) << (8 * i);
    }
    int actualHeaderCrc = Crc.computeCrc24(headerData, headerLength);
    if (actualHeaderCrc != expectedHeaderCrc) {
      throw new CrcMismatchException(
          String.format(
              "CRC mismatch on header %s. Received %s, computed %s.",
              Long.toHexString(headerData),
              Integer.toHexString(expectedHeaderCrc),
              Integer.toHexString(actualHeaderCrc)));
    }

    int payloadLength = (int) headerData & Segment.MAX_PAYLOAD_LENGTH;
    headerData >>= 17;
    int uncompressedPayloadLength;
    if (compress) {
      uncompressedPayloadLength = (int) headerData & Segment.MAX_PAYLOAD_LENGTH;
      headerData >>= 17;
    } else {
      uncompressedPayloadLength = -1;
    }
    boolean isSelfContained = (headerData & 1) == 1;
    return new Header(payloadLength, uncompressedPayloadLength, isSelfContained);
  }

  /**
   * Decodes the rest of a segment from a previously decoded header, and checks the payload's CRC.
   * It is assumed that the caller has already checked that there are enough bytes.
   */
  public Segment<B> decode(Header header, B source) throws CrcMismatchException {
    assert primitiveCodec.sizeOf(source) == header.payloadLength + CRC32_LENGTH;

    // Extract payload:
    B encodedPayload = primitiveCodec.readRetainedSlice(source, header.payloadLength);

    // Read and check CRC:
    int expectedPayloadCrc = 0;
    for (int i = 0; i < CRC32_LENGTH; i++) {
      expectedPayloadCrc |= (primitiveCodec.readByte(source) & 0xFF) << (8 * i);
    }
    primitiveCodec.release(source); // done with this (we retained the payload independently)
    int actualPayloadCrc = Crc.computeCrc32(encodedPayload, primitiveCodec);
    if (actualPayloadCrc != expectedPayloadCrc) {
      primitiveCodec.release(encodedPayload);
      throw new CrcMismatchException(
          String.format(
              "CRC mismatch on payload. Received %s, computed %s.",
              Integer.toHexString(expectedPayloadCrc), Integer.toHexString(actualPayloadCrc)));
    }

    // Decompress payload if needed:
    B payload;
    if (compress && header.uncompressedPayloadLength > 0) {
      payload =
          compressor.decompressWithoutLength(encodedPayload, header.uncompressedPayloadLength);
      primitiveCodec.release(encodedPayload);
    } else {
      payload = encodedPayload;
    }

    return new Segment<>(payload, header.isSelfContained);
  }

  /** Temporary holder for header data during decoding. */
  public static class Header {
    public final int payloadLength;
    public final int uncompressedPayloadLength;
    public final boolean isSelfContained;

    public Header(int payloadLength, int uncompressedPayloadLength, boolean isSelfContained) {
      this.payloadLength = payloadLength;
      this.uncompressedPayloadLength = uncompressedPayloadLength;
      this.isSelfContained = isSelfContained;
    }
  }
}
