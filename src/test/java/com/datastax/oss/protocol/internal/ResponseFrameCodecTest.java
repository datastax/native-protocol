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

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockCompressor;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.oss.protocol.internal.response.Ready;
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

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

@RunWith(DataProviderRunner.class)
public class ResponseFrameCodecTest extends FrameCodecTestBase {

  private MockPrimitiveCodec primitiveCodec;
  private List<Integer> expectedAllocations;

  @Before
  public void setup() {
    primitiveCodec = Mockito.spy(new MockPrimitiveCodec());
    expectedAllocations = new ArrayList<>();
  }

  @Test
  @UseDataProvider("responseParameters")
  public void should_encode_response_frame(
      int protocolVersion,
      int streamId,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addEncoder(new MockReadyCodec(protocolVersion)));

    Frame frame =
        Frame.forResponse(
            protocolVersion,
            streamId,
            tracing ? TRACING_ID : null,
            customPayload,
            warnings,
            new Ready());
    MockBinaryString actual = frameCodec.encode(frame);
    MockBinaryString expected =
        mockResponsePayload(
            protocolVersion, streamId, compressor, tracing, customPayload, warnings, false);

    assertThat(actual).isEqualTo(expected);
    for (Integer size : expectedAllocations) {
      Mockito.verify(primitiveCodec).allocate(size);
    }
  }

  @Test
  @UseDataProvider("responseParameters")
  public void should_decode_response_frame(
      int protocolVersion,
      int streamId,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addDecoder(new MockReadyCodec(protocolVersion)));

    MockBinaryString encoded =
        mockResponsePayload(
            protocolVersion, streamId, compressor, tracing, customPayload, warnings, true);
    Frame frame = frameCodec.decode(encoded);

    assertThat(frame.protocolVersion).isEqualTo(protocolVersion);
    assertThat(frame.streamId).isEqualTo(streamId);
    assertThat(frame.tracing).isEqualTo(tracing);
    assertThat(frame.tracingId).isEqualTo(tracing ? TRACING_ID : null);
    assertThat(frame.customPayload).isEqualTo(customPayload);
    assertThat(frame.warnings).isEqualTo(warnings);
    assertThat(frame.message).isInstanceOf(Ready.class);
  }

  // assembles the binary string corresponding to a READY response
  private MockBinaryString mockResponsePayload(
      int protocolVersion,
      int streamId,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      boolean forDecoding) {
    // Header
    MockBinaryString binary = new MockBinaryString().byte_(protocolVersion | 0b1000_0000);
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
    if (!warnings.isEmpty()) {
      flags |= 0x08;
    }
    if (protocolVersion == ProtocolConstants.Version.BETA) {
      flags |= 0x10;
    }
    binary.byte_(flags);
    binary.unsignedShort(streamId & 0xFFFF);
    binary.byte_(ProtocolConstants.Opcode.READY);

    int uncompressedSize = MockReadyCodec.MOCK_ENCODED_SIZE;
    if (tracing) {
      uncompressedSize += 16; // size of tracing id
    }
    if (!customPayload.isEmpty()) {
      assertThat(customPayload).isEqualTo(SOME_PAYLOAD);
      uncompressedSize += PrimitiveSizes.sizeOfBytesMap(SOME_PAYLOAD);
    }
    if (!warnings.isEmpty()) {
      assertThat(warnings).isEqualTo(SOME_WARNINGS);
      uncompressedSize += PrimitiveSizes.sizeOfStringList(SOME_WARNINGS);
    }

    int headerSize = 9;

    int bodySize;
    if (forDecoding) {
      // If we're decoding, decode() will call MockPrimitiveCodec.sizeOf on the rest of the
      // frame, and checks that it matches the size in the header.
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
    if (tracing) {
      binary.long_(TRACING_ID.getMostSignificantBits()).long_(TRACING_ID.getLeastSignificantBits());
    }
    if (customPayload.size() > 0) {
      binary.unsignedShort(2).string("foo").bytes("0x0a").string("bar").bytes("0x0b");
    }
    if (warnings.size() > 0) {
      binary.unsignedShort(2).string("warning 1").string("warning 2");
    }
    binary.string(MockReadyCodec.MOCK_ENCODED);
    if (!(compressor instanceof NoopCompressor)) {
      binary.string(MockCompressor.END);
    }
    return binary;
  }

  public static class MockReadyCodec extends Message.Codec {

    public static final String MOCK_ENCODED = "mock encoded READY";
    public static final int MOCK_ENCODED_SIZE = 12;

    MockReadyCodec(int protocolVersion) {
      super(ProtocolConstants.Opcode.READY, protocolVersion);
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
      return new Ready();
    }
  }

  @DataProvider
  public static Object[][] responseParameters() {
    // before v4: no payload, no warnings
    Object[][] v3Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV3OrBelow(),
            TestDataProviders.fromList(2, -1),
            TestDataProviders.fromList(Compressor.none(), new MockCompressor()),
            TestDataProviders.fromList(false, true), // tracing
            TestDataProviders.fromList(Frame.NO_PAYLOAD),
            TestDataProviders.fromList(NO_WARNINGS));

    // v4+
    Object[][] v4Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV4OrAbove(),
            TestDataProviders.fromList(2, -1),
            TestDataProviders.fromList(Compressor.none(), new MockCompressor()),
            TestDataProviders.fromList(false, true),
            TestDataProviders.fromList(Frame.NO_PAYLOAD, SOME_PAYLOAD),
            TestDataProviders.fromList(NO_WARNINGS, SOME_WARNINGS));

    Object[][] all = new Object[v3Parameters.length + v4Parameters.length][];
    System.arraycopy(v3Parameters, 0, all, 0, v3Parameters.length);
    System.arraycopy(v4Parameters, 0, all, v3Parameters.length, v4Parameters.length);
    return all;
  }
}
