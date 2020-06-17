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
import com.datastax.oss.protocol.internal.response.Ready;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

@RunWith(DataProviderRunner.class)
public class ResponseFrameCodecTest extends FrameCodecTestBase {

  private MockPrimitiveCodec primitiveCodec;
  private List<Integer> expectedAllocations;

  @Before
  public void setup() {
    primitiveCodec = Mockito.spy(MockPrimitiveCodec.INSTANCE);
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
            registry -> registry.addEncoder(new MockReadyCodec(protocolVersion)));

    Frame frame =
        Frame.forResponse(
            protocolVersion,
            streamId,
            tracing ? TRACING_ID : null,
            customPayload,
            warnings,
            new Ready());
    MockBinaryString expected =
        mockResponsePayload(
            protocolVersion, streamId, compressor, tracing, customPayload, warnings, true);
    MockBinaryString actual = frameCodec.encode(frame);

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
            registry -> registry.addDecoder(new MockReadyCodec(protocolVersion)));

    MockBinaryString encoded =
        mockResponsePayload(
            protocolVersion, streamId, compressor, tracing, customPayload, warnings, false);
    Frame frame = frameCodec.decode(encoded);

    assertThat(frame.protocolVersion).isEqualTo(protocolVersion);
    assertThat(frame.streamId).isEqualTo(streamId);
    assertThat(frame.tracing).isEqualTo(tracing);
    assertThat(frame.tracingId).isEqualTo(tracing ? TRACING_ID : null);
    assertThat(frame.customPayload).isEqualTo(customPayload);
    assertThat(frame.warnings).isEqualTo(warnings);
    assertThat(frame.message).isInstanceOf(Ready.class);
  }

  @Test
  public void should_decode_single_frame_from_larger_buffer() {
    int protocolVersion = ProtocolConstants.Version.V5;

    // Assemble a buffer containing two frames
    MockBinaryString encodedFrame1 =
        mockResponsePayload(
            protocolVersion,
            1,
            Compressor.none(),
            false,
            Collections.emptyMap(),
            Collections.emptyList(),
            false);
    MockBinaryString encodedFrame2 =
        mockResponsePayload(
            protocolVersion,
            2,
            Compressor.none(),
            false,
            Collections.emptyMap(),
            Collections.emptyList(),
            false);
    MockBinaryString encoded =
        MockPrimitiveCodec.INSTANCE.concat(encodedFrame1.copy(), encodedFrame2);

    // Decode the first frame
    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            Compressor.none(),
            registry -> registry.addDecoder(new MockReadyCodec(protocolVersion)));
    Frame frame = frameCodec.decode(encoded);
    assertThat(frame.streamId).isEqualTo(1);

    // The buffer should still contain the data of the second frame
    assertThat(encoded).isEqualTo(encodedFrame2);
  }

  @Test
  public void should_decode_body_size_from_partial_input() {
    int protocolVersion = ProtocolConstants.Version.V5;

    // Assemble just a header
    MockBinaryString initial =
        new MockBinaryString()
            .byte_(protocolVersion | 0b1000_0000)
            .byte_(0) // flags
            .unsignedShort(0) // stream id
            .byte_(ProtocolConstants.Opcode.READY) // opcode
            .int_(1234) // body size
        ;
    MockBinaryString encoded = initial.copy();

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            Compressor.none(),
            registry -> registry.addDecoder(new MockReadyCodec(protocolVersion)));
    int bodySize = frameCodec.decodeBodySize(encoded);

    assertThat(bodySize).isEqualTo(1234);
    // This should not have changed the input
    assertThat(encoded).isEqualTo(initial);
  }

  // assembles the binary string corresponding to a READY response
  private MockBinaryString mockResponsePayload(
      int protocolVersion,
      int streamId,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      boolean forEncoding) {

    // Only two compressors are supported in this test
    assertThat(compressor).isInstanceOfAny(MockCompressor.class, NoopCompressor.class);
    boolean compress = (compressor instanceof MockCompressor);

    MockBinaryString header = new MockBinaryString().byte_(protocolVersion | 0b1000_0000);
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
    header.byte_(flags);
    header.unsignedShort(streamId & 0xFFFF);
    header.byte_(ProtocolConstants.Opcode.READY);
    // the header still needs the body size, but assemble the body first

    MockBinaryString uncompressedBody = new MockBinaryString();
    if (tracing) {
      uncompressedBody
          .long_(TRACING_ID.getMostSignificantBits())
          .long_(TRACING_ID.getLeastSignificantBits());
    }
    if (customPayload.size() > 0) {
      uncompressedBody.unsignedShort(2).string("foo").bytes("0x0a").string("bar").bytes("0x0b");
    }
    if (warnings.size() > 0) {
      uncompressedBody.unsignedShort(2).string("warning 1").string("warning 2");
    }
    uncompressedBody.string(MockReadyCodec.MOCK_ENCODED);

    MockBinaryString actualBody;
    if (compress) {
      MockCompressor mockCompressor = (MockCompressor) compressor;
      actualBody =
          mockCompressor.prime(
              uncompressedBody,
              // Generate a random compressed payload, we just need to ensure uniqueness because the
              // compressor is reused across tests
              new MockBinaryString().int_(COMPRESSED_COUNT.getAndIncrement()));
    } else {
      actualBody = uncompressedBody;
    }
    header.int_(actualBody.size());

    // Set expectations for the allocations performed during encoding:
    if (forEncoding) {
      if (compress) {
        // The uncompressed body is encoded and then compressed. The header is encoded separately.
        expectedAllocations.add(uncompressedBody.size());
        expectedAllocations.add(header.size());
      } else {
        // Everything is encoded into a single buffer
        expectedAllocations.add(header.size() + actualBody.size());
      }
    }

    return header.append(actualBody);
  }

  private static final AtomicInteger COMPRESSED_COUNT = new AtomicInteger();

  public static class MockReadyCodec extends Message.Codec {

    public static final String MOCK_ENCODED = "mock encoded READY";

    MockReadyCodec(int protocolVersion) {
      super(ProtocolConstants.Opcode.READY, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      encoder.writeString(MOCK_ENCODED, dest);
    }

    @Override
    public int encodedSize(Message message) {
      return PrimitiveSizes.sizeOfString(MOCK_ENCODED);
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
