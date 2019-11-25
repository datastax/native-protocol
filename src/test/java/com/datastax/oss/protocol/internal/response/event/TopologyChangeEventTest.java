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
import java.net.InetSocketAddress;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class TopologyChangeEventTest extends MessageTestBase<TopologyChangeEvent> {

  public TopologyChangeEventTest() {
    super(TopologyChangeEvent.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Event.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    InetSocketAddress addr = new InetSocketAddress("127.0.0.1", 9042);
    TopologyChangeEvent initial =
        new TopologyChangeEvent(ProtocolConstants.TopologyChangeType.NEW_NODE, addr);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.TOPOLOGY_CHANGE)
                .string(ProtocolConstants.TopologyChangeType.NEW_NODE)
                .inetAddr(addr.getAddress())
                .int_(addr.getPort()));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.TOPOLOGY_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.TopologyChangeType.NEW_NODE.length())
                + (PrimitiveSizes.BYTE + 4 + PrimitiveSizes.INT));

    TopologyChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.TOPOLOGY_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.TopologyChangeType.NEW_NODE);
    assertThat(decoded.address.getAddress().getHostAddress()).isEqualTo("127.0.0.1");
    assertThat(decoded.address.getPort()).isEqualTo(9042);
  }
}
