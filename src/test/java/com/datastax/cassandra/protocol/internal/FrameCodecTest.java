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
import com.datastax.cassandra.protocol.internal.response.Ready;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class FrameCodecTest {
  private static final int STREAM_ID = 2;
  private static final UUID TRACING_ID = UUID.randomUUID();
  private static final Map<String, ByteBuffer> SOME_PAYLOAD;
  private static final List<String> NO_WARNINGS = Collections.emptyList();
  private static final List<String> SOME_WARNINGS = Arrays.asList("warning 1", "warning 2");

  static {
    Map<String, ByteBuffer> tmp = new LinkedHashMap<>();
    tmp.put("foo", Bytes.fromHexString("0x0a"));
    tmp.put("bar", Bytes.fromHexString("0x0b"));
    SOME_PAYLOAD = Collections.unmodifiableMap(tmp);
  }

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
    int size;
    // If we're testing decoding, MockPrimitiveCodec.sizeOf will be used to check that the rest of
    // the frame matches the size.
    // If we're testing encoding with compression, MockPrimitiveCodec.sizeOf will be used to
    // measure the size of the compressed payload.
    if (forDecoding || compressor != null) {
      size = MockPrimitiveCodec.MOCK_SIZE;
    } else {
      // size will be what is returned by the mock message codec
      size = MockOptionsCodec.MOCK_ENCODED_SIZE;
      if (customPayload.size() > 0) {
        assertThat(customPayload).isEqualTo(SOME_PAYLOAD);
        size += PrimitiveSizes.sizeOfBytesMap(SOME_PAYLOAD);
      }
    }
    binary.int_(size);

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

  @Test(dataProvider = "responseParameters")
  public void should_encode_response_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            new MockPrimitiveCodec(),
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addEncoder(new MockReadyCodec(protocolVersion)));

    MockBinaryString actual =
        frameCodec.encode(
            new Frame(
                protocolVersion,
                STREAM_ID,
                tracing,
                tracing ? TRACING_ID : null,
                customPayload,
                warnings,
                new Ready()));
    MockBinaryString expected =
        mockResponsePayload(protocolVersion, compressor, tracing, customPayload, warnings, false);

    assertThat(actual).isEqualTo(expected);
  }

  @Test(dataProvider = "responseParameters")
  public void should_decode_response_frame(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings) {

    FrameCodec<MockBinaryString> frameCodec =
        new FrameCodec<>(
            new MockPrimitiveCodec(),
            compressor,
            (FrameCodec.CodecGroup)
                registry -> registry.addDecoder(new MockReadyCodec(protocolVersion)));

    MockBinaryString encoded =
        mockResponsePayload(protocolVersion, compressor, tracing, customPayload, warnings, true);
    Frame frame = frameCodec.decode(encoded);

    assertThat(frame.protocolVersion).isEqualTo(protocolVersion);
    assertThat(frame.streamId).isEqualTo(STREAM_ID);
    assertThat(frame.tracing).isEqualTo(tracing);
    assertThat(frame.tracingId).isEqualTo(tracing ? TRACING_ID : null);
    assertThat(frame.customPayload).isEqualTo(customPayload);
    assertThat(frame.warnings).isEqualTo(warnings);
    assertThat(frame.message).isInstanceOf(Ready.class);
  }

  // assembles the binary string corresponding to a READY response
  private MockBinaryString mockResponsePayload(
      int protocolVersion,
      Compressor<MockBinaryString> compressor,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      boolean forDecoding) {
    // Header
    MockBinaryString binary = new MockBinaryString().byte_(protocolVersion | 0b1000_0000);
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
    if (!warnings.isEmpty()) {
      flags |= 0x08;
    }
    binary.byte_(flags);
    binary.unsignedShort(STREAM_ID).byte_(ProtocolConstants.Opcode.READY);
    int size;
    // If we're testing decoding, MockPrimitiveCodec.sizeOf will be used to check that the rest of
    // the frame matches the size.
    // If we're testing encoding with compression, MockPrimitiveCodec.sizeOf will be used to
    // measure the size of the compressed payload.
    if (forDecoding || compressor != null) {
      size = MockPrimitiveCodec.MOCK_SIZE;
    } else {
      // size will be what is returned by the mock message codec
      size = MockReadyCodec.MOCK_ENCODED_SIZE;
      if (tracing) {
        size += 16; // size of tracing id
      }
      if (!customPayload.isEmpty()) {
        assertThat(customPayload).isEqualTo(SOME_PAYLOAD);
        size += PrimitiveSizes.sizeOfBytesMap(SOME_PAYLOAD);
      }
      if (!warnings.isEmpty()) {
        assertThat(warnings).isEqualTo(SOME_WARNINGS);
        size += PrimitiveSizes.sizeOfStringList(SOME_WARNINGS);
      }
    }
    binary.int_(size);

    // Message body
    if (compressor != null) {
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
    if (compressor != null) {
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

  @DataProvider(name = "responseParameters")
  public static Object[][] getResponseParameters() {
    // before v4: no payload, no warnings
    Object[][] v3Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV3OrBelow(),
            TestDataProviders.fromList(null, new MockCompressor()),
            TestDataProviders.fromList(false, true), // tracing
            TestDataProviders.fromList(Frame.NO_PAYLOAD),
            TestDataProviders.fromList(NO_WARNINGS));

    // v4+
    Object[][] v4Parameters =
        TestDataProviders.combine(
            TestDataProviders.protocolV4OrAbove(),
            TestDataProviders.fromList(null, new MockCompressor()),
            TestDataProviders.fromList(false, true),
            TestDataProviders.fromList(Frame.NO_PAYLOAD, SOME_PAYLOAD),
            TestDataProviders.fromList(NO_WARNINGS, SOME_WARNINGS));

    Object[][] all = new Object[v3Parameters.length + v4Parameters.length][];
    System.arraycopy(v3Parameters, 0, all, 0, v3Parameters.length);
    System.arraycopy(v4Parameters, 0, all, v3Parameters.length, v4Parameters.length);
    return all;
  }
}
