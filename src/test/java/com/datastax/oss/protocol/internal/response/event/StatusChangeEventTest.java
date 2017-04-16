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
package com.datastax.oss.protocol.internal.response.event;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Event;
import java.net.InetSocketAddress;
import org.testng.annotations.Test;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

public class StatusChangeEventTest extends MessageTestBase<StatusChangeEvent> {

  protected StatusChangeEventTest() {
    super(StatusChangeEvent.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Event.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 9042);
    StatusChangeEvent initial = new StatusChangeEvent(ProtocolConstants.StatusChangeType.UP, addr);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.STATUS_CHANGE)
                .string(ProtocolConstants.StatusChangeType.UP)
                .inetAddr(addr.getAddress())
                .int_(addr.getPort()));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.STATUS_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.StatusChangeType.UP.length())
                + (PrimitiveSizes.BYTE + 4 + PrimitiveSizes.INT));

    StatusChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.STATUS_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.StatusChangeType.UP);
    assertThat(decoded.address.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
    assertThat(decoded.address.getPort()).isEqualTo(9042);
  }
}
