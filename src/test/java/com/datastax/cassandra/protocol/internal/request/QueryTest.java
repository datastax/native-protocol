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
import com.datastax.cassandra.protocol.internal.request.Query;
import com.datastax.cassandra.protocol.internal.request.query.QueryOptions;
import com.datastax.cassandra.protocol.internal.response.QueryOptionsBuilder;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class QueryTest extends MessageTest<Query> {
  private String queryString = "select * from system.local";

  public QueryTest() {
    super(Query.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Query.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_query_with_default_options(int protocolVersion) {
    Query query = new Query(queryString);

    assertThat(encode(query, protocolVersion))
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0) // no flags
            );

    assertThat(encodedSize(query, protocolVersion)).isEqualTo(4 + queryString.length() + 2 + 1);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_query_with_different_CL(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .consistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .build();
    Query query = new Query(queryString, options);

    assertThat(encode(query, protocolVersion))
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .byte_(0));

    assertThat(encodedSize(query, protocolVersion)).isEqualTo(4 + queryString.length() + 2 + 1);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_positional_values(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().positionalValue("0xcafebabe").build();
    Query query = new Query(queryString, options);

    assertThat(encode(query, protocolVersion))
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x01)
                .unsignedShort(1)
                .bytes("0xcafebabe") // count + list of values
            );

    assertThat(encodedSize(query, protocolVersion))
        .isEqualTo(4 + queryString.length() + 2 + 1 + 2 + 8);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_non_default_options(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withSkipMetadata()
            .withPageSize(10)
            .withPagingState("0xcafebabe")
            .withSerialConsistency(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
            .build();
    Query query = new Query(queryString, options);

    assertThat(encode(query, protocolVersion))
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x02 | 0x04 | 0x08 | 0x10)
                .int_(10)
                .bytes("0xcafebabe")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL));
    assertThat(encodedSize(query, protocolVersion))
        .isEqualTo(4 + queryString.length() + 2 + 1 + 4 + 8 + 2);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_named_values(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().namedValue("foo", "0xcafebabe").build();
    Query query = new Query(queryString, options);

    assertThat(encode(query, protocolVersion))
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x01 | 0x40)
                .unsignedShort(1)
                .string("foo")
                .bytes("0xcafebabe") // count + list of values
            );
    assertThat(encodedSize(query, protocolVersion))
        .isEqualTo(4 + queryString.length() + 2 + 1 + 2 + (2 + "foo".length()) + 8);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_set_default_timestamp_in_protocol_v3(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().withDefaultTimestamp(10).build();
    Query query = new Query(queryString, options);

    assertThat(encode(query, protocolVersion))
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x20)
                .long_(10));
    assertThat(encodedSize(query, protocolVersion)).isEqualTo(4 + queryString.length() + 2 + 1 + 8);
  }

  @Test(
    dataProviderClass = TestDataProviders.class,
    dataProvider = "protocolV3OrAbove",
    expectedExceptions = IllegalArgumentException.class
  )
  public void should_not_allow_both_named_and_positional_values_in_protocol_v3_or_above(
      int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .positionalValue("0xcafebabe")
            .namedValue("foo", "0xcafebabe")
            .build();
    Query query = new Query(queryString, options);

    encode(query, protocolVersion);
  }
}
