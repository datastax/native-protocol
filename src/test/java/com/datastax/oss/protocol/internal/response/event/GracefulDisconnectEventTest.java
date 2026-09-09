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
package com.datastax.oss.protocol.internal.response.event;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Event;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class GracefulDisconnectEventTest extends MessageTestBase<GracefulDisconnectEvent> {

  public GracefulDisconnectEventTest() {
    super(GracefulDisconnectEvent.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Event.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    GracefulDisconnectEvent initial = new GracefulDisconnectEvent();

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(new MockBinaryString().string(ProtocolConstants.EventType.GRACEFUL_DISCONNECT));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(PrimitiveSizes.SHORT + ProtocolConstants.EventType.GRACEFUL_DISCONNECT.length());

    GracefulDisconnectEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.GRACEFUL_DISCONNECT);
  }
}
