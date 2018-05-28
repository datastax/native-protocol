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
package com.datastax.oss.protocol.internal.request;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class StartupTest extends MessageTestBase<Startup> {

  public StartupTest() {
    super(Startup.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Startup.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_with_compression(int protocolVersion) {
    Startup initial = new Startup("LZ4");

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .unsignedShort(2) // size of string map
                // string map entries
                .string("CQL_VERSION")
                .string("3.0.0")
                .string("COMPRESSION")
                .string("LZ4"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "CQL_VERSION".length())
                + (PrimitiveSizes.SHORT + "3.0.0".length())
                + (PrimitiveSizes.SHORT + "COMPRESSION".length())
                + (PrimitiveSizes.SHORT + "LZ4".length()));

    Startup decoded = decode(encoded, protocolVersion);

    assertThat(decoded.options)
        .hasSize(2)
        .containsEntry("CQL_VERSION", "3.0.0")
        .containsEntry("COMPRESSION", "LZ4");
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_without_compression(int protocolVersion) {
    Startup initial = new Startup();

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(new MockBinaryString().unsignedShort(1).string("CQL_VERSION").string("3.0.0"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "CQL_VERSION".length())
                + (PrimitiveSizes.SHORT + "3.0.0".length()));

    Startup decoded = decode(encoded, protocolVersion);

    assertThat(decoded.options).hasSize(1).containsEntry("CQL_VERSION", "3.0.0");
  }
}
