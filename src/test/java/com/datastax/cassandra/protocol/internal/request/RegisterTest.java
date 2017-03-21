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
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class RegisterTest extends MessageTest<Register> {

  public RegisterTest() {
    super(Register.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Register.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    List<String> eventTypes =
        Arrays.asList(
            ProtocolConstants.EventType.SCHEMA_CHANGE, ProtocolConstants.EventType.STATUS_CHANGE);
    Register initial = new Register(eventTypes);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .unsignedShort(2)
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.EventType.STATUS_CHANGE));

    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            2
                + (2 + ProtocolConstants.EventType.SCHEMA_CHANGE.length())
                + (2 + ProtocolConstants.EventType.STATUS_CHANGE.length()));

    Register decoded = decode(encoded, protocolVersion);

    assertThat(decoded.eventTypes)
        .containsExactly(
            ProtocolConstants.EventType.SCHEMA_CHANGE, ProtocolConstants.EventType.STATUS_CHANGE);
  }
}
