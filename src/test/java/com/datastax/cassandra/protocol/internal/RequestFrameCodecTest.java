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

import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.binary.MockCompressor;
import com.datastax.cassandra.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.cassandra.protocol.internal.request.Options;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class RequestFrameCodecTest extends FrameCodecTest {

  @Test(dataProvider = "requestParameters")
  public void should_encode_request_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            new MockPrimitiveCodec(),
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addEncoder(new MockOptionsCodec(protocolVersion)));

    MockBinaryString actual =
        frameCodec.encode(
            new Frame(
                protocolVersion,
                STREAM_ID,
                tracing,
                null,
                customPayload,
                Collections.emptyList(),
                Options.INSTANCE));
    MockBinaryString expected =
        mockRequestPayload(protocolVersion, compressor, tracing, customPayload, false);

    assertThat(actual).isEqualTo(expected);
  }

  @Test(dataProvider = "requestParameters")
  public void should_decode_request_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            new MockPrimitiveCodec(),
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addDecoder(new MockOptionsCodec(protocolVersion)));

    MockBinaryString encoded =
        mockRequestPayload(protocolVersion, compressor, tracing, customPayload, true);
    Frame frame = frameCodec.decode(encoded);

    assertThat(frame.protocolVersion).isEqualTo(protocolVersion);
    assertThat(frame.streamId).isEqualTo(STREAM_ID);
    assertThat(frame.tracing).isEqualTo(tracing);
    assertThat(frame.tracingId).isNull(); // always for requests
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
    if (compressor != null) {
      flags |= 0x01;
    }
    if (tracing) {
      flags |= 0x02;
    }
    if (!customPayload.isEmpty()) {
      flags |= 0x04;
    }
    binary.byte_(flags);
    binary.unsignedShort(STREAM_ID).byte_(ProtocolConstants.Opcode.OPTIONS);

    int uncompressedSize = MockOptionsCodec.MOCK_ENCODED_SIZE;
    if (!customPayload.isEmpty()) {
      assertThat(customPayload).isEqualTo(SOME_PAYLOAD);
      uncompressedSize += PrimitiveSizes.sizeOfBytesMap(SOME_PAYLOAD);
    }

    int sizeInHeader;
    if (forDecoding || compressor != null) {
      // If we're decoding, decode() will call MockPrimitiveCodec.sizeOf on the rest of the
      // frame, and check that it matches the size in the header.
      // If we're encoding with compression, encode() will call MockPrimitiveCodec.sizeOf to
      // measure the size of the compressed message, and use that in the header.
      sizeInHeader = MockPrimitiveCodec.MOCK_SIZE;
    } else {
      // If we're encoding without compression, encode() will write the uncompressed size in the
      // header
      sizeInHeader = uncompressedSize;
    }
    binary.int_(sizeInHeader);

    // Message body
    if (compressor != null) {
      binary.string(MockCompressor.START);
    }
    if (customPayload.size() > 0) {
      binary.unsignedShort(2).string("foo").bytes("0x0a").string("bar").bytes("0x0b");
    }
    binary.string(MockOptionsCodec.MOCK_ENCODED);
    if (compressor != null) {
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

  @DataProvider(name = "requestParameters")
  public static Object[][] getRequestParameters() {
    // before v4: no payload
    Object[][] v3Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV3OrBelow(),
            TestDataProviders.fromList(null, new MockCompressor()),
            TestDataProviders.fromList(false, true), // tracing
            TestDataProviders.fromList(Frame.NO_PAYLOAD));

    // v4+
    Object[][] v4Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV4OrAbove(),
            TestDataProviders.fromList(null, new MockCompressor()),
            TestDataProviders.fromList(false, true),
            TestDataProviders.fromList(Frame.NO_PAYLOAD, SOME_PAYLOAD));

    Object[][] all = new Object[v3Parameters.length + v4Parameters.length][];
    System.arraycopy(v3Parameters, 0, all, 0, v3Parameters.length);
    System.arraycopy(v4Parameters, 0, all, v3Parameters.length, v4Parameters.length);
    return all;
  }
}
