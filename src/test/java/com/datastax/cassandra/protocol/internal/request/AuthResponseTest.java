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
package com.datastax.cassandra.protocol.internal.request;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.request.AuthResponse;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class AuthResponseTest extends MessageTest<AuthResponse> {

  public AuthResponseTest() {
    super(AuthResponse.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new AuthResponse.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode(int protocolVersion) {
    AuthResponse authResponse = new AuthResponse(Bytes.fromHexString("0xcafebabe"));

    assertThat(encode(authResponse, protocolVersion))
        .isEqualTo(new MockBinaryString().bytes("0xcafebabe"));

    assertThat(encodedSize(authResponse, protocolVersion)).isEqualTo(8);
  }
}
