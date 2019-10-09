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
package com.datastax.dse.protocol.internal.request;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptionsBuilder;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.request.query.QueryOptionsBuilder;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DseQueryTest extends MessageTestBase<Query> {

  private String queryString = "select * from system.local";

  public DseQueryTest() {
    super(Query.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new DseQueryCodec(protocolVersion);
  }

  @Test
  public void should_encode_and_decode_with_continuous_paging_options_in_dse_v1() {
    int protocolVersion = DseProtocolConstants.Version.DSE_V1;

    DseQueryOptions dseOptions =
        new DseQueryOptionsBuilder()
            .withConsistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .withPositionalValue("0xcafebabe")
            .withPageSize(8192)
            .withPageSizeInBytes()
            .withMaxPages(10)
            .withPagesPerSecond(5)
            .build();

    Query initial = new Query(queryString, dseOptions);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString(queryString)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(
                    ProtocolConstants.QueryFlag.VALUES
                        | ProtocolConstants.QueryFlag.PAGE_SIZE
                        | DseProtocolConstants.QueryFlag.PAGE_SIZE_BYTES
                        | DseProtocolConstants.QueryFlag.CONTINUOUS_PAGING)
                .unsignedShort(1)
                .bytes("0xcafebabe")
                .int_(8192)
                .int_(10)
                .int_(5));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(queryString);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(decoded.options.positionalValues).containsExactly(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(8192);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();

    assertThat(decoded.options).isInstanceOf(DseQueryOptions.class);
    DseQueryOptions decodedDseOptions = (DseQueryOptions) decoded.options;
    assertThat(decodedDseOptions.isPageSizeInBytes).isTrue();
    assertThat(decodedDseOptions.continuousPagingOptions.maxPages).isEqualTo(10);
    assertThat(decodedDseOptions.continuousPagingOptions.pagesPerSecond).isEqualTo(5);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void should_encode_and_decode_with_continuous_paging_options_in_dse_v2_and_above(
      int protocolVersion) {
    DseQueryOptions dseOptions =
        new DseQueryOptionsBuilder()
            .withConsistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .withPositionalValue("0xcafebabe")
            .withPageSize(8192)
            .withPageSizeInBytes()
            .withMaxPages(10)
            .withPagesPerSecond(5)
            .withNextPages(2)
            .build();

    Query initial = new Query(queryString, dseOptions);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString(queryString)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(
                    ProtocolConstants.QueryFlag.VALUES
                        | ProtocolConstants.QueryFlag.PAGE_SIZE
                        | DseProtocolConstants.QueryFlag.PAGE_SIZE_BYTES
                        | DseProtocolConstants.QueryFlag.CONTINUOUS_PAGING)
                .unsignedShort(1)
                .bytes("0xcafebabe")
                .int_(8192)
                .int_(10)
                .int_(5)
                .int_(2));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(queryString);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(decoded.options.positionalValues).containsExactly(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(8192);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();

    assertThat(decoded.options).isInstanceOf(DseQueryOptions.class);
    DseQueryOptions decodedDseOptions = (DseQueryOptions) decoded.options;
    assertThat(decodedDseOptions.isPageSizeInBytes).isTrue();
    assertThat(decodedDseOptions.continuousPagingOptions.maxPages).isEqualTo(10);
    assertThat(decodedDseOptions.continuousPagingOptions.pagesPerSecond).isEqualTo(5);
    assertThat(decodedDseOptions.continuousPagingOptions.nextPages).isEqualTo(2);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV1OrAbove")
  public void should_encode_and_decode_without_continuous_paging_options_in_dse_protocols(
      int protocolVersion) {
    QueryOptions dseOptions =
        new QueryOptionsBuilder()
            .withConsistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .withPositionalValue("0xcafebabe")
            .withPageSize(8192)
            .build();

    Query initial = new Query(queryString, dseOptions);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString(queryString)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(ProtocolConstants.QueryFlag.VALUES | ProtocolConstants.QueryFlag.PAGE_SIZE)
                .unsignedShort(1)
                .bytes("0xcafebabe")
                .int_(8192));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + queryString.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.SHORT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + PrimitiveSizes.INT);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(queryString);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(decoded.options.positionalValues).containsExactly(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(8192);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
    assertThat(decoded.options.keyspace).isNull();

    // This will be ignored by the OSS code, that expects a QueryOption, but just for reference:
    assertThat(decoded.options).isInstanceOf(DseQueryOptions.class);
    DseQueryOptions decodedDseOptions = (DseQueryOptions) decoded.options;
    assertThat(decodedDseOptions.isPageSizeInBytes).isFalse();
    assertThat(decodedDseOptions.continuousPagingOptions).isNull();
  }
}
