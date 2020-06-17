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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockCompressor;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SegmentCodecTest {

  private SegmentCodec<MockBinaryString> codecWithoutCompression;
  private MockCompressor mockCompressor;
  private SegmentCodec<MockBinaryString> codecWithCompression;

  @Before
  public void setup() {
    codecWithoutCompression = new SegmentCodec<>(MockPrimitiveCodec.INSTANCE, Compressor.none());
    mockCompressor = spy(new MockCompressor());
    codecWithCompression = new SegmentCodec<>(MockPrimitiveCodec.INSTANCE, mockCompressor);
  }

  @Test
  public void should_encode_without_compression() throws CrcMismatchException {
    MockBinaryString originalPayload = new MockBinaryString().byte_(0x01);
    Segment<MockBinaryString> segment = new Segment<>(originalPayload.copy(), true);

    List<Object> out = new ArrayList<>();
    codecWithoutCompression.encode(segment, out);

    assertThat(out).hasSize(3).allMatch(o -> o instanceof MockBinaryString);
    MockBinaryString headerBytes = (MockBinaryString) out.get(0);
    MockBinaryString payloadBytes = (MockBinaryString) out.get(1);
    MockBinaryString trailerBytes = (MockBinaryString) out.get(2);

    assertThat(headerBytes.size()).isEqualTo(6);
    SegmentCodec.Header header = codecWithoutCompression.decodeHeader(headerBytes);
    assertThat(header.payloadLength).isEqualTo(originalPayload.size());

    assertThat(payloadBytes).isEqualTo(originalPayload);

    assertThat(trailerBytes.size()).isEqualTo(4);
  }

  @Test
  public void should_encode_with_compression() throws CrcMismatchException {
    MockBinaryString originalPayload =
        new MockBinaryString().byte_(0x01).byte_(0x02).byte_(0x03).byte_(0x04);
    MockBinaryString compressedPayload = new MockBinaryString().byte_(0x01);
    assertThat(compressedPayload.size()).isLessThan(originalPayload.size());
    mockCompressor.prime(originalPayload, compressedPayload);

    List<Object> out = new ArrayList<>();
    Segment<MockBinaryString> segment = new Segment<>(originalPayload.copy(), true);
    codecWithCompression.encode(segment, out);

    assertThat(out).hasSize(3).allMatch(o -> o instanceof MockBinaryString);
    MockBinaryString headerBytes = (MockBinaryString) out.get(0);
    MockBinaryString payloadBytes = (MockBinaryString) out.get(1);
    MockBinaryString trailerBytes = (MockBinaryString) out.get(2);

    verify(mockCompressor).compressWithoutLength(originalPayload);

    assertThat(headerBytes.size()).isEqualTo(8);
    SegmentCodec.Header header = codecWithCompression.decodeHeader(headerBytes);
    assertThat(header.payloadLength).isEqualTo(compressedPayload.size());
    assertThat(header.uncompressedPayloadLength).isEqualTo(originalPayload.size());

    assertThat(payloadBytes).isEqualTo(compressedPayload);

    assertThat(trailerBytes.size()).isEqualTo(4);
  }

  @Test
  public void should_skip_compression_when_compressed_payload_bigger() throws CrcMismatchException {
    MockBinaryString uncompressedPayload = new MockBinaryString().byte_(0x01);
    MockBinaryString compressedPayload =
        new MockBinaryString().byte_(0x01).byte_(0x02).byte_(0x03).byte_(0x04);
    assertThat(compressedPayload.size()).isGreaterThan(uncompressedPayload.size());
    mockCompressor.prime(uncompressedPayload, compressedPayload);

    List<Object> out = new ArrayList<>();
    Segment<MockBinaryString> segment = new Segment<>(uncompressedPayload.copy(), true);
    codecWithCompression.encode(segment, out);

    assertThat(out).hasSize(3).allMatch(o -> o instanceof MockBinaryString);
    MockBinaryString headerBytes = (MockBinaryString) out.get(0);
    MockBinaryString payloadBytes = (MockBinaryString) out.get(1);
    MockBinaryString trailerBytes = (MockBinaryString) out.get(2);

    verify(mockCompressor).compressWithoutLength(uncompressedPayload);

    assertThat(headerBytes.size()).isEqualTo(8);
    SegmentCodec.Header header = codecWithCompression.decodeHeader(headerBytes);
    assertThat(header.payloadLength).isEqualTo(uncompressedPayload.size());
    assertThat(header.uncompressedPayloadLength).isEqualTo(0);

    assertThat(payloadBytes).isEqualTo(uncompressedPayload);

    assertThat(trailerBytes.size()).isEqualTo(4);
  }

  @Test
  public void should_decode_without_compression() throws CrcMismatchException {
    MockBinaryString payloadBytes = new MockBinaryString().byte_(0x01);

    // Assume the header is already decoded (this is covered in detail in SegmentCodecHeaderTest)
    SegmentCodec.Header header = new SegmentCodec.Header(payloadBytes.size(), 0, true);
    // Hardcode the CRC bytes (not a lot of value in recomputing it here, it would be the same code)
    MockBinaryString bytes = payloadBytes.copy().byte_(11).byte_(43).byte_(-101).byte_(-70);

    Segment<MockBinaryString> segment = codecWithoutCompression.decode(header, bytes);

    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(payloadBytes);
  }

  @Test
  public void should_decode_with_compression() throws CrcMismatchException {
    MockBinaryString uncompressedPayload =
        new MockBinaryString().byte_(0x01).byte_(0x02).byte_(0x03).byte_(0x04);
    MockBinaryString compressedPayload = new MockBinaryString().byte_(0x01);
    assertThat(compressedPayload.size()).isLessThan(uncompressedPayload.size());
    mockCompressor.prime(uncompressedPayload, compressedPayload);

    SegmentCodec.Header header =
        new SegmentCodec.Header(compressedPayload.size(), uncompressedPayload.size(), true);

    MockBinaryString bytes = compressedPayload.copy().byte_(11).byte_(43).byte_(-101).byte_(-70);
    Segment<MockBinaryString> segment = codecWithCompression.decode(header, bytes);

    verify(mockCompressor).decompressWithoutLength(compressedPayload, uncompressedPayload.size());

    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(uncompressedPayload);
  }

  @Test
  public void should_decode_when_compression_was_skipped() throws CrcMismatchException {
    MockBinaryString payloadBytes = new MockBinaryString().byte_(0x01);

    SegmentCodec.Header header = new SegmentCodec.Header(payloadBytes.size(), 0, true);

    MockBinaryString bytes = payloadBytes.copy().byte_(11).byte_(43).byte_(-101).byte_(-70);

    Segment<MockBinaryString> segment = codecWithCompression.decode(header, bytes);

    verify(mockCompressor, never()).decompressWithoutLength(any(), anyInt());

    assertThat(segment.isSelfContained).isTrue();
    assertThat(segment.payload).isEqualTo(payloadBytes);
  }

  @Test(expected = CrcMismatchException.class)
  public void should_throw_if_crc_does_not_match() throws CrcMismatchException {
    MockBinaryString payloadBytes = new MockBinaryString().byte_(0x01);

    SegmentCodec.Header header = new SegmentCodec.Header(payloadBytes.size(), 0, true);

    MockBinaryString bytes = payloadBytes.copy().byte_(0).byte_(0).byte_(0).byte_(0);

    codecWithoutCompression.decode(header, bytes);
  }
}
