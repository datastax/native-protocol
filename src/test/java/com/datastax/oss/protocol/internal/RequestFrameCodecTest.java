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

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockCompressor;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.oss.protocol.internal.request.Options;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(DataProviderRunner.class)
public class RequestFrameCodecTest extends FrameCodecTestBase {

  private MockPrimitiveCodec primitiveCodec;
  private List<Integer> expectedAllocations;

  @Before
  public void setup() {
    primitiveCodec = Mockito.spy(new MockPrimitiveCodec());
    expectedAllocations = new ArrayList<>();
  }

  @Test
  @UseDataProvider("requestParameters")
  public void should_encode_request_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addEncoder(new MockOptionsCodec(protocolVersion)));

    Frame frame =
        Frame.forRequest(protocolVersion, STREAM_ID, tracing, customPayload, Options.INSTANCE);
    MockBinaryString actual = frameCodec.encode(frame);
    MockBinaryString expected =
        mockRequestPayload(protocolVersion, compressor, tracing, customPayload, false);

    assertThat(actual).isEqualTo(expected);
    for (Integer size : expectedAllocations) {
      Mockito.verify(primitiveCodec).allocate(size);
    }
  }

  @Test
  @UseDataProvider("requestParameters")
  public void should_decode_request_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addDecoder(new MockOptionsCodec(protocolVersion)));

    MockBinaryString encoded =
        mockRequestPayload(protocolVersion, compressor, tracing, customPayload, true);
    Frame frame = frameCodec.decode(encoded);

    assertThat(frame.protocolVersion).isEqualTo(protocolVersion);
    assertThat(frame.beta).isEqualTo(frame.protocolVersion == ProtocolConstants.Version.BETA);
    assertThat(frame.streamId).isEqualTo(STREAM_ID);
    assertThat(frame.tracing).isEqualTo(tracing);
    assertThat(frame.tracingId).isNull(); // always for requests
    // Always the same because our mock primitive codec always measures the same size, but in
    // reality it would change when compressed:
    assertThat(frame.size).isEqualTo(9 + MockPrimitiveCodec.MOCK_SIZE);
    assertThat(frame.compressedSize)
        .isEqualTo((compressor.algorithm() == null) ? -1 : 9 + MockPrimitiveCodec.MOCK_SIZE);
    assertThat(frame.customPayload).isEqualTo(customPayload);
    assertThat(frame.message).isEqualTo(Options.INSTANCE);
  }

  // assembles the binary string corresponding to an OPTIONS request
  private MockBinaryString mockRequestPayload(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      boolean forDecoding) {
    // Header
    MockBinaryString binary = new MockBinaryString().byte_(protocolVersion);
    int flags = 0;
    if (!(compressor instanceof NoopCompressor)) {
      flags |= 0x01;
    }
    if (tracing) {
      flags |= 0x02;
    }
    if (!customPayload.isEmpty()) {
      flags |= 0x04;
    }
    if (protocolVersion == ProtocolConstants.Version.BETA) {
      flags |= 0x10;
    }
    binary.byte_(flags);
    binary.unsignedShort(STREAM_ID).byte_(ProtocolConstants.Opcode.OPTIONS);

    int uncompressedSize = MockOptionsCodec.MOCK_ENCODED_SIZE;
    if (!customPayload.isEmpty()) {
      assertThat(customPayload).isEqualTo(SOME_PAYLOAD);
      uncompressedSize += PrimitiveSizes.sizeOfBytesMap(SOME_PAYLOAD);
    }

    int headerSize = 9;

    int bodySize;
    if (forDecoding) {
      // If we're decoding, decode() will call MockPrimitiveCodec.sizeOf on the rest of the
      // frame, and check that it matches the size in the header.
      bodySize = MockPrimitiveCodec.MOCK_SIZE;
    } else if (!(compressor instanceof NoopCompressor)) {
      // If we're encoding with compression, encode() will call MockPrimitiveCodec.sizeOf to
      // measure the size of the compressed message, and use that in the header.
      bodySize = MockPrimitiveCodec.MOCK_SIZE;
      // It will allocate one buffer for the header, and one for the uncompressed body
      expectedAllocations.add(headerSize);
      expectedAllocations.add(uncompressedSize);
    } else {
      // If we're encoding without compression, encode() will write the uncompressed size in the
      // header
      bodySize = uncompressedSize;
      // It will allocate a single buffer
      expectedAllocations.add(headerSize + bodySize);
    }
    binary.int_(bodySize);

    // Message body
    if (!(compressor instanceof NoopCompressor)) {
      binary.string(MockCompressor.START);
    }
    if (customPayload.size() > 0) {
      binary.unsignedShort(2).string("foo").bytes("0x0a").string("bar").bytes("0x0b");
    }
    binary.string(MockOptionsCodec.MOCK_ENCODED);
    if (!(compressor instanceof NoopCompressor)) {
      binary.string(MockCompressor.END);
    }
    return binary;
  }

  public static class MockOptionsCodec extends Message.Codec {

    public static final String MOCK_ENCODED = "mock encoded OPTIONS";
    public static final int MOCK_ENCODED_SIZE = 12;

    MockOptionsCodec(int protocolVersion) {
      super(ProtocolConstants.Opcode.OPTIONS, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      encoder.writeString(MOCK_ENCODED, dest);
    }

    @Override
    public int encodedSize(Message message) {
      return MOCK_ENCODED_SIZE;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      assertThat(decoder.readString(source)).isEqualTo(MOCK_ENCODED);
      return Options.INSTANCE;
    }
  }

  @DataProvider
  public static Object[][] requestParameters() {
    // before v4: no payload
    Object[][] v3Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV3OrBelow(),
            TestDataProviders.fromList(Compressor.none(), new MockCompressor()),
            TestDataProviders.fromList(false, true), // tracing
            TestDataProviders.fromList(Frame.NO_PAYLOAD));

    // v4+
    Object[][] v4Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV4OrAbove(),
            TestDataProviders.fromList(Compressor.none(), new MockCompressor()),
            TestDataProviders.fromList(false, true),
            TestDataProviders.fromList(Frame.NO_PAYLOAD, SOME_PAYLOAD));

    Object[][] all = new Object[v3Parameters.length + v4Parameters.length][];
    System.arraycopy(v3Parameters, 0, all, 0, v3Parameters.length);
    System.arraycopy(v4Parameters, 0, all, v3Parameters.length, v4Parameters.length);
    return all;
  }
}
