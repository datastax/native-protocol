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
package com.datastax.cassandra.protocol.internal.response;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AuthenticateTest extends MessageTest<Authenticate> {

  protected AuthenticateTest() {
    super(Authenticate.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Authenticate.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    String authenticator = "MockAuthenticator";
    Authenticate initial = new Authenticate(authenticator);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded).isEqualTo(new MockBinaryString().string(authenticator));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(PrimitiveSizes.SHORT + authenticator.length());

    Authenticate decoded = decode(encoded, protocolVersion);
    assertThat(decoded.authenticator).isEqualTo(authenticator);
  }
}
