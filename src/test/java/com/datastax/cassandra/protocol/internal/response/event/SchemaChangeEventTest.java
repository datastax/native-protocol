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
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class SchemaChangeEventTest extends MessageTest<SchemaChangeEvent> {

  protected SchemaChangeEventTest() {
    super(SchemaChangeEvent.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Event.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_keyspace_change(int protocolVersion) {
    SchemaChangeEvent event =
        decode(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.KEYSPACE)
                .string("test"),
            protocolVersion);

    assertThat(event.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    Assertions.assertThat(event.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    Assertions.assertThat(event.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.KEYSPACE);
    Assertions.assertThat(event.keyspace).isEqualTo("test");
    Assertions.assertThat(event.object).isNull();
    Assertions.assertThat(event.arguments).isNull();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_table_change(int protocolVersion) {
    SchemaChangeEvent event =
        decode(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.TABLE)
                .string("test")
                .string("mytable"),
            protocolVersion);

    assertThat(event.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    Assertions.assertThat(event.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    Assertions.assertThat(event.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.TABLE);
    Assertions.assertThat(event.keyspace).isEqualTo("test");
    Assertions.assertThat(event.object).isEqualTo("mytable");
    Assertions.assertThat(event.arguments).isNull();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_type_change(int protocolVersion) {
    SchemaChangeEvent event =
        decode(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.TYPE)
                .string("test")
                .string("mytype"),
            protocolVersion);

    assertThat(event.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    Assertions.assertThat(event.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    Assertions.assertThat(event.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.TYPE);
    Assertions.assertThat(event.keyspace).isEqualTo("test");
    Assertions.assertThat(event.object).isEqualTo("mytype");
    Assertions.assertThat(event.arguments).isNull();
  }

  @Test(
    dataProviderClass = TestDataProviders.class,
    dataProvider = "protocolV3OrBelow",
    expectedExceptions = IllegalArgumentException.class
  )
  public void should_fail_to_decode_function_change_in_v3_or_below(int protocolVersion) {
    decode(
        new MockBinaryString()
            .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
            .string(ProtocolConstants.SchemaChangeType.CREATED)
            .string(ProtocolConstants.SchemaChangeTarget.FUNCTION)
            .string("test")
            .string("myfunction")
            .unsignedShort(2)
            .string("int")
            .string("int"),
        protocolVersion);
  }

  @Test(
    dataProviderClass = TestDataProviders.class,
    dataProvider = "protocolV3OrBelow",
    expectedExceptions = IllegalArgumentException.class
  )
  public void should_fail_to_decode_aggregate_change_in_v3_or_below(int protocolVersion) {
    decode(
        new MockBinaryString()
            .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
            .string(ProtocolConstants.SchemaChangeType.CREATED)
            .string(ProtocolConstants.SchemaChangeTarget.AGGREGATE)
            .string("test")
            .string("myaggregate")
            .unsignedShort(2)
            .string("int")
            .string("int"),
        protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_function_change_in_v4_or_above(int protocolVersion) {
    SchemaChangeEvent event =
        decode(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.FUNCTION)
                .string("test")
                .string("myfunction")
                .unsignedShort(2)
                .string("int")
                .string("int"),
            protocolVersion);

    assertThat(event.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    Assertions.assertThat(event.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    Assertions.assertThat(event.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.FUNCTION);
    Assertions.assertThat(event.keyspace).isEqualTo("test");
    Assertions.assertThat(event.object).isEqualTo("myfunction");
    Assertions.assertThat(event.arguments).containsExactly("int", "int");
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_aggregate_change_in_v4_or_above(int protocolVersion) {
    SchemaChangeEvent event =
        decode(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.AGGREGATE)
                .string("test")
                .string("myaggregate")
                .unsignedShort(2)
                .string("int")
                .string("int"),
            protocolVersion);

    assertThat(event.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    Assertions.assertThat(event.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    Assertions.assertThat(event.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.AGGREGATE);
    Assertions.assertThat(event.keyspace).isEqualTo("test");
    Assertions.assertThat(event.object).isEqualTo("myaggregate");
    Assertions.assertThat(event.arguments).containsExactly("int", "int");
  }
}
