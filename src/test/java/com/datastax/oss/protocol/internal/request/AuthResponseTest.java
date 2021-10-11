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
package com.datastax.oss.protocol.internal.request;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class AuthResponseTest extends MessageTestBase<AuthResponse> {

  public AuthResponseTest() {
    super(AuthResponse.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new AuthResponse.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    ByteBuffer token = Bytes.fromHexString("0xcafebabe");
    byte[] tokenBytes = token.array();
    AuthResponse initial = new AuthResponse(token);

    int encodedSize = encodedSize(initial, protocolVersion);
    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded).isEqualTo(new MockBinaryString().bytes("0xcafebabe"));
    assertThat(encodedSize).isEqualTo(PrimitiveSizes.INT + "cafebabe".length() / 2);

    // Check that the token was consumed, and the contents were cleared
    assertThat(token.hasRemaining()).isFalse();
    assertThat(tokenBytes).containsOnly(0);

    AuthResponse decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.token)).isEqualTo("0xcafebabe");
  }

  @Test
  public void should_not_attempt_to_clear_token_if_read_only() {
    ByteBuffer token = Bytes.fromHexString("0xcafebabe");
    byte[] tokenBytes = token.array();

    encode(new AuthResponse(token.asReadOnlyBuffer()), ProtocolConstants.Version.V4);

    // Check that the contents are still intact
    assertThat(tokenBytes).containsExactly(0xca, 0xfe, 0xba, 0xbe);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_null_token(int protocolVersion) {
    AuthResponse initial = new AuthResponse(null);
    int encodedSize = encodedSize(initial, protocolVersion);
    MockBinaryString encoded = encode(initial, protocolVersion);
    assertThat(encoded).isEqualTo(new MockBinaryString().bytes("0x"));
    assertThat(encodedSize).isEqualTo(PrimitiveSizes.INT);
    AuthResponse decoded = decode(encoded, protocolVersion);
    assertThat(Bytes.toHexString(decoded.token)).isEqualTo("0x");
  }
}
