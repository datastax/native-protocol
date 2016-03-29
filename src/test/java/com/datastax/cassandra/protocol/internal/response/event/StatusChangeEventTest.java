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
package com.datastax.cassandra.protocol.internal.response.event;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.response.Event;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class StatusChangeEventTest extends MessageTest<StatusChangeEvent> {

  protected StatusChangeEventTest() {
    super(StatusChangeEvent.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Event.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode(int protocolVersion) {
    StatusChangeEvent event =
        decode(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.STATUS_CHANGE)
                .string(ProtocolConstants.StatusChangeType.UP)
                .inet("1.2.3.4", 9042),
            protocolVersion);

    assertThat(event.type).isEqualTo(ProtocolConstants.EventType.STATUS_CHANGE);
    assertThat(event.changeType).isEqualTo(ProtocolConstants.StatusChangeType.UP);
    assertThat(event.address.getHostName()).isEqualTo("1.2.3.4");
    assertThat(event.address.getPort()).isEqualTo(9042);
  }
}
