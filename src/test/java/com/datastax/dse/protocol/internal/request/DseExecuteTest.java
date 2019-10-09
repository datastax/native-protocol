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

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DseExecuteTest extends MessageTestBase<Execute> {

  private byte[] queryId = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));
  private byte[] resultMetadataId = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));

  public DseExecuteTest() {
    super(Execute.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new DseExecuteCodec(protocolVersion);
  }

  @Test
  public void should_encode_and_decode_without_result_metadata_id_in_protocol_dse_v1() {
    Execute initial = new Execute(queryId, QueryOptions.DEFAULT);

    MockBinaryString encoded = encode(initial, DSE_V1);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .shortBytes("0xcafebabe")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0) // no flags
            );
    assertThat(encodedSize(initial, DSE_V1))
        .isEqualTo(
            (PrimitiveSizes.SHORT + queryId.length) + PrimitiveSizes.SHORT + PrimitiveSizes.INT);

    Execute decoded = decode(encoded, DSE_V1);

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
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void should_encode_and_decode_with_result_metadata_id_in_protocol_dse_v2_or_above(
      int protocolVersion) {
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
}
