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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockCompressor;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import org.junit.Test;

public class SegmentCodecHeaderTest {

  private static final SegmentCodec<MockBinaryString> CODEC_WITHOUT_COMPRESSION =
      new SegmentCodec<>(MockPrimitiveCodec.INSTANCE, Compressor.none());
  private static final SegmentCodec<MockBinaryString> CODEC_WITH_COMPRESSION =
      new SegmentCodec<>(MockPrimitiveCodec.INSTANCE, new MockCompressor());

  @Test
  public void should_encode_uncompressed_header() {
    MockBinaryString encoded = CODEC_WITHOUT_COMPRESSION.encodeHeader(5, -1, true);

    // Header bytes in the order that allows us to visualize the fields:
    // 6-bit padding, selfContained flag, 17-bit length
    int expectedByte2 = 0b000000_1_0;
    int expectedByte1 = 0b00000000;
    int expectedByte0 = 0b00000101;

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                // Note that the actual order is reversed
                .byte_(expectedByte0)
                .byte_(expectedByte1)
                .byte_(expectedByte2)
                // CRC (hard-coded, no point in recomputing it, it would be the same as the
                // production code)
                .byte_(25)
                .byte_(-103)
                .byte_(-102));
  }

  @Test
  public void should_encode_compressed_header() {
    MockBinaryString encoded = CODEC_WITH_COMPRESSION.encodeHeader(5, 12, true);

    // 5-bit padding, selfContained flag, 17-bit uncompressed length, 17-bit length
    int expectedByte4 = 0b00000_1_00;
    int expectedByte3 = 0b00000000;
    int expectedByte2 = 0b0001100_0;
    int expectedByte1 = 0b00000000;
    int expectedByte0 = 0b00000101;

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .byte_(expectedByte0)
                .byte_(expectedByte1)
                .byte_(expectedByte2)
                .byte_(expectedByte3)
                .byte_(expectedByte4)
                .byte_(-98)
                .byte_(8)
                .byte_(56));
  }

  /**
   * Checks that we correctly use 8 bytes when we left-shift the uncompressed length, to avoid
   * overflows.
   */
  @Test
  public void should_encode_compressed_header_when_aligned_uncompressed_length_overflows() {
    MockBinaryString encoded =
        CODEC_WITH_COMPRESSION.encodeHeader(5, Segment.MAX_PAYLOAD_LENGTH, true);

    // 5-bit padding, selfContained flag, 17-bit uncompressed length, 17-bit length
    int expectedByte4 = 0b00000_1_11;
    int expectedByte3 = 0b11111111;
    int expectedByte2 = 0b1111111_0;
    int expectedByte1 = 0b00000000;
    int expectedByte0 = 0b00000101;

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .byte_(expectedByte0)
                .byte_(expectedByte1)
                .byte_(expectedByte2)
                .byte_(expectedByte3)
                .byte_(expectedByte4)
                .byte_(-82)
                .byte_(108)
                .byte_(-36));
  }

  @Test
  public void should_decode_uncompressed_payload() throws CrcMismatchException {
    SegmentCodec.Header header =
        CODEC_WITHOUT_COMPRESSION.decodeHeader(
            new MockBinaryString()
                .byte_(0b00000101)
                .byte_(0b00000000)
                .byte_(0b000000_1_0)
                .byte_(25)
                .byte_(-103)
                .byte_(-102));

    assertThat(header.payloadLength).isEqualTo(5);
    assertThat(header.uncompressedPayloadLength).isEqualTo(-1);
    assertThat(header.isSelfContained).isTrue();
  }

  @Test
  public void should_decode_compressed_payload() throws CrcMismatchException {
    SegmentCodec.Header header =
        CODEC_WITH_COMPRESSION.decodeHeader(
            new MockBinaryString()
                .byte_(0b00000101)
                .byte_(0b00000000)
                .byte_(0b0001100_0)
                .byte_(0b00000000)
                .byte_(0b00000_1_00)
                .byte_(-98)
                .byte_(8)
                .byte_(56));

    assertThat(header.payloadLength).isEqualTo(5);
    assertThat(header.uncompressedPayloadLength).isEqualTo(12);
    assertThat(header.isSelfContained).isTrue();
  }

  @Test(expected = CrcMismatchException.class)
  public void should_fail_to_decode_if_corrupted() throws CrcMismatchException {
    CODEC_WITHOUT_COMPRESSION.decodeHeader(
        new MockBinaryString()
            .byte_(0b00000101)
            .byte_(0b00000000)
            .byte_(0b000000_1_0)
            // Not the right CRC:
            .byte_(0)
            .byte_(0)
            .byte_(0));
  }
}
