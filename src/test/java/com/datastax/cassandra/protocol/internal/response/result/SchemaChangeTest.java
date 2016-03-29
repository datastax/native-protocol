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
package com.datastax.cassandra.protocol.internal.response.result;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.response.Result;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class SchemaChangeTest extends MessageTest<SchemaChange> {
  public SchemaChangeTest() {
    super(SchemaChange.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_keyspace_change(int protocolVersion) {
    SchemaChange schemaChange =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.KEYSPACE)
                .string("test"),
            protocolVersion);

    assertThat(schemaChange.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(schemaChange.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.KEYSPACE);
    assertThat(schemaChange.keyspace).isEqualTo("test");
    assertThat(schemaChange.object).isNull();
    assertThat(schemaChange.arguments).isNull();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_table_change(int protocolVersion) {
    SchemaChange schemaChange =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.TABLE)
                .string("test")
                .string("mytable"),
            protocolVersion);

    assertThat(schemaChange.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(schemaChange.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.TABLE);
    assertThat(schemaChange.keyspace).isEqualTo("test");
    assertThat(schemaChange.object).isEqualTo("mytable");
    assertThat(schemaChange.arguments).isNull();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_type_change(int protocolVersion) {
    SchemaChange schemaChange =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.TYPE)
                .string("test")
                .string("mytype"),
            protocolVersion);

    assertThat(schemaChange.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(schemaChange.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.TYPE);
    assertThat(schemaChange.keyspace).isEqualTo("test");
    assertThat(schemaChange.object).isEqualTo("mytype");
    assertThat(schemaChange.arguments).isNull();
  }

  @Test(
    dataProviderClass = TestDataProviders.class,
    dataProvider = "protocolV3OrBelow",
    expectedExceptions = IllegalArgumentException.class
  )
  public void should_fail_to_decode_function_change_in_v3_or_below(int protocolVersion) {
    decode(
        new MockBinaryString()
            .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
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
            .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
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
    SchemaChange schemaChange =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.FUNCTION)
                .string("test")
                .string("myfunction")
                .unsignedShort(2)
                .string("int")
                .string("int"),
            protocolVersion);

    assertThat(schemaChange.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(schemaChange.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.FUNCTION);
    assertThat(schemaChange.keyspace).isEqualTo("test");
    assertThat(schemaChange.object).isEqualTo("myfunction");
    assertThat(schemaChange.arguments).containsExactly("int", "int");
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_aggregate_change_in_v4_or_above(int protocolVersion) {
    SchemaChange schemaChange =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.AGGREGATE)
                .string("test")
                .string("myaggregate")
                .unsignedShort(2)
                .string("int")
                .string("int"),
            protocolVersion);

    assertThat(schemaChange.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(schemaChange.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.AGGREGATE);
    assertThat(schemaChange.keyspace).isEqualTo("test");
    assertThat(schemaChange.object).isEqualTo("myaggregate");
    assertThat(schemaChange.arguments).containsExactly("int", "int");
  }
}
