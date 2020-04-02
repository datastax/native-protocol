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
package com.datastax.oss.protocol.internal.request;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.request.query.QueryOptionsBuilder;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ExecuteTest extends MessageTestBase<Execute> {

  private byte[] queryId = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));
  private byte[] resultMetadataId = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));

  public ExecuteTest() {
    super(Execute.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Execute.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_with_default_options_v3_v4(int protocolVersion) {
    Execute initial = new Execute(queryId, QueryOptions.DEFAULT);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .shortBytes("0xcafebabe")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0) // no flags
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + queryId.length) + PrimitiveSizes.SHORT + PrimitiveSizes.BYTE);

    Execute decoded = decode(encoded, protocolVersion);

    assertThat(decoded.queryId).isEqualTo(initial.queryId);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_default_options(int protocolVersion) {
    Execute initial = new Execute(queryId, resultMetadataId, QueryOptions.DEFAULT);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .shortBytes("0xcafebabe")
                .shortBytes("0xdeadbeef")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0) // no flags
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + queryId.length)
                + (PrimitiveSizes.SHORT + resultMetadataId.length)
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT);

    Execute decoded = decode(encoded, protocolVersion);

    assertThat(decoded.queryId).isEqualTo(initial.queryId);
    assertThat(decoded.resultMetadataId).isEqualTo(initial.resultMetadataId);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_now_in_seconds(int protocolVersion) {
    int nowInSeconds = 123456789;
    QueryOptions options = new QueryOptionsBuilder().withNowInSeconds(nowInSeconds).build();
    Execute initial = new Execute(queryId, resultMetadataId, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .shortBytes("0xcafebabe")
                .shortBytes("0xdeadbeef")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x100) // NOW_IN_SECONDS
                .int_(nowInSeconds));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.SHORT + queryId.length)
                + (PrimitiveSizes.SHORT + resultMetadataId.length)
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT);

    Execute decoded = decode(encoded, protocolVersion);

    assertThat(decoded.options.nowInSeconds).isEqualTo(nowInSeconds);
  }
}
