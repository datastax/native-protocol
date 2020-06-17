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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockCompressor;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
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

@RunWith(DataProviderRunner.class)
public class RequestFrameCodecTest extends FrameCodecTestBase {

  private static final List<String> MOCK_EVENT_TYPES =
      NullAllowingImmutableList.of(
          ProtocolConstants.EventType.SCHEMA_CHANGE, ProtocolConstants.EventType.STATUS_CHANGE);

  private MockPrimitiveCodec primitiveCodec;
  private List<Integer> expectedAllocations;

  @Before
  public void setup() {
    primitiveCodec = spy(new MockPrimitiveCodec());
    expectedAllocations = new ArrayList<>();
  }

  @Test
  @UseDataProvider("requestParameters")
  public void should_encode_request_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload) {

    Register registerRequest = new Register(MOCK_EVENT_TYPES);

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            registry -> registry.addEncoder(new MockRegisterCodec(protocolVersion)));

    Frame frame =
        Frame.forRequest(protocolVersion, STREAM_ID, tracing, customPayload, registerRequest);
    MockBinaryString expected =
        mockRequestPayload(protocolVersion, compressor, tracing, customPayload, true);
    MockBinaryString actual = frameCodec.encode(frame);

    assertThat(actual).isEqualTo(expected);
    for (Integer size : expectedAllocations) {
      verify(primitiveCodec).allocate(size);
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
            registry -> registry.addDecoder(new MockRegisterCodec(protocolVersion)));

    MockBinaryString encoded =
        mockRequestPayload(protocolVersion, compressor, tracing, customPayload, false);
    Frame frame = frameCodec.decode(encoded);

    assertThat(frame.protocolVersion).isEqualTo(protocolVersion);
    assertThat(frame.beta).isEqualTo(frame.protocolVersion == ProtocolConstants.Version.BETA);
    assertThat(frame.streamId).isEqualTo(STREAM_ID);
    assertThat(frame.tracing).isEqualTo(tracing);
    assertThat(frame.tracingId).isNull(); // always for requests
    assertThat(frame.customPayload).isEqualTo(customPayload);
  }

  @Test
  public void should_not_compress_startup_message() {
    should_not_compress_message(new Startup());
  }

  @Test
  public void should_not_compress_options_message() {
    should_not_compress_message(Options.INSTANCE);
  }

  private void should_not_compress_message(Message message) {
    Compressor<MockBinaryString> compressor = spy(new MockCompressor());
    int protocolVersion = 5;
    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            primitiveCodec,
            compressor,
            registry -> {
              registry.addEncoder(new Startup.Codec(protocolVersion));
              registry.addEncoder(new Options.Codec(protocolVersion));
            });
    Frame frame =
        Frame.forRequest(protocolVersion, STREAM_ID, false, Collections.emptyMap(), message);
    frameCodec.encode(frame);
    verify(compressor, never()).compress(any(MockBinaryString.class));
  }

  // assembles the binary string corresponding to a REGISTER request
  private MockBinaryString mockRequestPayload(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      boolean forEncoding) {

    // Only two compressors are supported in this test
    assertThat(compressor).isInstanceOfAny(MockCompressor.class, NoopCompressor.class);
    boolean compress = (compressor instanceof MockCompressor);

    MockBinaryString header = new MockBinaryString().byte_(protocolVersion);
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
    header.byte_(flags);
    header.unsignedShort(STREAM_ID).byte_(ProtocolConstants.Opcode.REGISTER);
    // the header still needs the body size, but assemble the body first

    MockBinaryString uncompressedBody = new MockBinaryString();
    if (customPayload.size() > 0) {
      uncompressedBody.unsignedShort(2).string("foo").bytes("0x0a").string("bar").bytes("0x0b");
    }
    uncompressedBody.string(MockRegisterCodec.MOCK_ENCODED);

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

  public static class MockRegisterCodec extends Message.Codec {

    public static final String MOCK_ENCODED = "mock encoded REGISTER";

    MockRegisterCodec(int protocolVersion) {
      super(ProtocolConstants.Opcode.REGISTER, protocolVersion);
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
      return new Register(MOCK_EVENT_TYPES);
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
