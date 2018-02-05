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
package com.datastax.oss.protocol.internal.request;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.request.query.QueryOptionsBuilder;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class QueryTest extends MessageTestBase<Query> {
  private String queryString = "select * from system.local";

  public QueryTest() {
    super(Query.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Query.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_query_with_default_options_v3_v4(int protocolVersion) {
    Query initial = new Query(queryString);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0) // no flags
            );

    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_query_with_default_options(int protocolVersion) {
    Query initial = new Query(queryString);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0) // no flags
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_query_with_different_CL_v3_v4(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withConsistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .byte_(0));

    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_query_with_different_CL(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withConsistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_positional_values_v3_v4(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().withPositionalValue("0xcafebabe").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x01)
                .unsignedShort(1)
                .bytes("0xcafebabe") // count + list of values
            );

    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2));

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).containsExactly(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_positional_values(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().withPositionalValue("0xcafebabe").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x01)
                .unsignedShort(1)
                .bytes("0xcafebabe") // count + list of values
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2));

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).containsExactly(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_non_default_options_v3_v4(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withSkipMetadata()
            .withPageSize(10)
            .withPagingState("0xcafebabe")
            .withSerialConsistency(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
            .withDefaultTimestamp(42)
            .build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x02 | 0x04 | 0x08 | 0x10 | 0x20)
                .int_(10)
                .bytes("0xcafebabe")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
                .long_(42));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE
                + PrimitiveSizes.INT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.LONG);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isTrue();
    assertThat(decoded.options.pageSize).isEqualTo(10);
    assertThat(decoded.options.pagingState).isEqualTo(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(42);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_non_default_options(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withSkipMetadata()
            .withPageSize(10)
            .withPagingState("0xcafebabe")
            .withSerialConsistency(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
            .withDefaultTimestamp(42)
            .build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x02 | 0x04 | 0x08 | 0x10 | 0x20)
                .int_(10)
                .bytes("0xcafebabe")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
                .long_(42));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.LONG);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isTrue();
    assertThat(decoded.options.pageSize).isEqualTo(10);
    assertThat(decoded.options.pagingState).isEqualTo(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(42);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_named_values_v3_v4(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().withNamedValue("foo", "0xcafebabe").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x01 | 0x40)
                .unsignedShort(1)
                .string("foo")
                .bytes("0xcafebabe") // count + list of values
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "foo".length())
                + (PrimitiveSizes.INT + "cafebabe".length() / 2));

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues)
        .hasSize(1)
        .containsEntry("foo", Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_named_values(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().withNamedValue("foo", "0xcafebabe").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x01 | 0x40)
                .unsignedShort(1)
                .string("foo")
                .bytes("0xcafebabe") // count + list of values
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "foo".length())
                + (PrimitiveSizes.INT + "cafebabe".length() / 2));

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues)
        .hasSize(1)
        .containsEntry("foo", Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();
  }

  @Test(expected = IllegalArgumentException.class)
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_not_allow_both_named_and_positional_values(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withPositionalValue("0xcafebabe")
            .withNamedValue("foo", "0xcafebabe")
            .build();
    Query query = new Query(queryString, options);

    encode(query, protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_keyspace(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().withKeyspace("ks").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x80) // WITH_KEYSPACE
                .string("ks"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "ks".length()));

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isEqualTo(initial.options.keyspace);
  }
}
