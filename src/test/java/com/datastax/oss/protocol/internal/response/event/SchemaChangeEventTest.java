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

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class SchemaChangeEventTest extends MessageTestBase<SchemaChangeEvent> {

  public SchemaChangeEventTest() {
    super(SchemaChangeEvent.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Event.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_keyspace_change(int protocolVersion) {
    SchemaChangeEvent initial =
        new SchemaChangeEvent(
            ProtocolConstants.SchemaChangeType.CREATED,
            ProtocolConstants.SchemaChangeTarget.KEYSPACE,
            "test",
            null,
            null);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.KEYSPACE)
                .string("test"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.SCHEMA_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeType.CREATED.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeTarget.KEYSPACE.length())
                + (PrimitiveSizes.SHORT + "test".length()));

    SchemaChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(decoded.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.KEYSPACE);
    assertThat(decoded.keyspace).isEqualTo("test");
    assertThat(decoded.object).isNull();
    assertThat(decoded.arguments).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_table_change(int protocolVersion) {
    SchemaChangeEvent initial =
        new SchemaChangeEvent(
            ProtocolConstants.SchemaChangeType.CREATED,
            ProtocolConstants.SchemaChangeTarget.TABLE,
            "test",
            "mytable",
            null);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.TABLE)
                .string("test")
                .string("mytable"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.SCHEMA_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeType.CREATED.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeTarget.TABLE.length())
                + (PrimitiveSizes.SHORT + "test".length())
                + (PrimitiveSizes.SHORT + "mytable".length()));

    SchemaChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(decoded.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.TABLE);
    assertThat(decoded.keyspace).isEqualTo("test");
    assertThat(decoded.object).isEqualTo("mytable");
    assertThat(decoded.arguments).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_type_change(int protocolVersion) {
    SchemaChangeEvent initial =
        new SchemaChangeEvent(
            ProtocolConstants.SchemaChangeType.CREATED,
            ProtocolConstants.SchemaChangeTarget.TYPE,
            "test",
            "mytype",
            null);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.TYPE)
                .string("test")
                .string("mytype"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.SCHEMA_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeType.CREATED.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeTarget.TYPE.length())
                + (PrimitiveSizes.SHORT + "test".length())
                + (PrimitiveSizes.SHORT + "mytype".length()));

    SchemaChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(decoded.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.TYPE);
    assertThat(decoded.keyspace).isEqualTo("test");
    assertThat(decoded.object).isEqualTo("mytype");
    assertThat(decoded.arguments).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrBelow")
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

  @Test(expected = IllegalArgumentException.class)
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrBelow")
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

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV4OrAbove")
  public void should_encode_and_decode_function_change_in_v4_or_above(int protocolVersion) {
    SchemaChangeEvent initial =
        new SchemaChangeEvent(
            ProtocolConstants.SchemaChangeType.CREATED,
            ProtocolConstants.SchemaChangeTarget.FUNCTION,
            "test",
            "myfunction",
            NullAllowingImmutableList.of("int", "int"));

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.FUNCTION)
                .string("test")
                .string("myfunction")
                .unsignedShort(2)
                .string("int")
                .string("int"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.SCHEMA_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeType.CREATED.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeTarget.FUNCTION.length())
                + (PrimitiveSizes.SHORT + "test".length())
                + (PrimitiveSizes.SHORT + "myfunction".length())
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "int".length()) * 2);

    SchemaChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(decoded.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.FUNCTION);
    assertThat(decoded.keyspace).isEqualTo("test");
    assertThat(decoded.object).isEqualTo("myfunction");
    assertThat(decoded.arguments).containsExactly("int", "int");
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV4OrAbove")
  public void should_encode_and_decode_aggregate_change_in_v4_or_above(int protocolVersion) {
    SchemaChangeEvent initial =
        new SchemaChangeEvent(
            ProtocolConstants.SchemaChangeType.CREATED,
            ProtocolConstants.SchemaChangeTarget.AGGREGATE,
            "test",
            "myaggregate",
            NullAllowingImmutableList.of("int", "int"));

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .string(ProtocolConstants.EventType.SCHEMA_CHANGE)
                .string(ProtocolConstants.SchemaChangeType.CREATED)
                .string(ProtocolConstants.SchemaChangeTarget.AGGREGATE)
                .string("test")
                .string("myaggregate")
                .unsignedShort(2)
                .string("int")
                .string("int"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + ProtocolConstants.EventType.SCHEMA_CHANGE.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeType.CREATED.length())
                + (PrimitiveSizes.SHORT + ProtocolConstants.SchemaChangeTarget.AGGREGATE.length())
                + (PrimitiveSizes.SHORT + "test".length())
                + (PrimitiveSizes.SHORT + "myaggregate".length())
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "int".length()) * 2);

    SchemaChangeEvent decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.EventType.SCHEMA_CHANGE);
    assertThat(decoded.changeType).isEqualTo(ProtocolConstants.SchemaChangeType.CREATED);
    assertThat(decoded.target).isEqualTo(ProtocolConstants.SchemaChangeTarget.AGGREGATE);
    assertThat(decoded.keyspace).isEqualTo("test");
    assertThat(decoded.object).isEqualTo("myaggregate");
    assertThat(decoded.arguments).containsExactly("int", "int");
  }

  @Test
  public void should_convert_to_string() {
    assertThat(
            new SchemaChangeEvent(
                    ProtocolConstants.SchemaChangeType.CREATED,
                    ProtocolConstants.SchemaChangeTarget.KEYSPACE,
                    "ks",
                    null,
                    null)
                .toString())
        .isEqualTo("EVENT SCHEMA_CHANGE(CREATED KEYSPACE ks)");
    assertThat(
            new SchemaChangeEvent(
                    ProtocolConstants.SchemaChangeType.CREATED,
                    ProtocolConstants.SchemaChangeTarget.TABLE,
                    "ks",
                    "table",
                    null)
                .toString())
        .isEqualTo("EVENT SCHEMA_CHANGE(CREATED TABLE ks.table)");
    assertThat(
            new SchemaChangeEvent(
                    ProtocolConstants.SchemaChangeType.CREATED,
                    ProtocolConstants.SchemaChangeTarget.FUNCTION,
                    "ks",
                    "fn",
                    NullAllowingImmutableList.of("int", "int"))
                .toString())
        .isEqualTo("EVENT SCHEMA_CHANGE(CREATED FUNCTION ks.fn[int, int])");

    // Null argument list should never happen, but make sure it's handled gracefully:
    assertThat(
            new SchemaChangeEvent(
                    ProtocolConstants.SchemaChangeType.CREATED,
                    ProtocolConstants.SchemaChangeTarget.FUNCTION,
                    "ks",
                    "fn",
                    null)
                .toString())
        .isEqualTo("EVENT SCHEMA_CHANGE(CREATED FUNCTION ks.fn)");
  }
}
