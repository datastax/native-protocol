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
package com.datastax.dse.protocol.internal.response.result;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;
import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DsePreparedTest extends MessageTestBase<Prepared> {

  private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);
  private static final byte[] PREPARED_QUERY_ID = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));
  private static final byte[] RESULT_METADATA_ID =
      Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));

  public DsePreparedTest() {
    super(Prepared.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion, new DsePreparedSubCodec(protocolVersion));
  }

  @Test
  public void should_encode_and_decode_without_result_metadata_id_in_protocol_dse_v1() {
    {
      RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
      RowsMetadata resultMetadata =
          new RowsMetadata(
              Arrays.asList(
                  new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                  new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
              null,
              null,
              null);
      Prepared initial = new Prepared(PREPARED_QUERY_ID, null, variablesMetadata, resultMetadata);

      MockBinaryString encoded = encode(initial, DSE_V1);

      assertThat(encoded)
          .isEqualTo(
              new MockBinaryString()
                  .int_(ProtocolConstants.ResultKind.PREPARED)
                  .shortBytes("0xcafebabe") // query id
                  // Variables metadata with no variables:
                  .int_(0x0000)
                  .int_(0)
                  .int_(0)
                  // Result metadata with 2 columns:
                  .int_(0x0001)
                  .int_(2)
                  .string("ks1")
                  .string("table1")
                  .string("column1")
                  .unsignedShort(ProtocolConstants.DataType.BLOB)
                  .string("column2")
                  .unsignedShort(ProtocolConstants.DataType.BLOB));
      assertThat(encodedSize(initial, DSE_V1))
          .isEqualTo(
              PrimitiveSizes.INT
                  + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                  + (PrimitiveSizes.INT + PrimitiveSizes.INT + PrimitiveSizes.INT)
                  + (PrimitiveSizes.INT
                      + PrimitiveSizes.INT
                      + (PrimitiveSizes.SHORT + "ks1".length())
                      + (PrimitiveSizes.SHORT + "table1".length())
                      + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                      + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT)));

      Prepared decoded = decode(encoded, DSE_V1);

      assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
      assertThat(decoded.variablesMetadata)
          .hasNoColumnSpecs()
          .hasColumnCount(0)
          .hasNoPagingState()
          .hasNoPkIndices();
      assertThat(decoded.resultMetadata)
          .hasNoPagingState()
          .hasColumnSpecs(
              new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
              new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE))
          .hasColumnCount(2)
          .hasNoPkIndices();
    }
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void should_encode_and_decode_with_result_metadata_id_in_protocol_dse_v2_and_above(
      int protocolVersion) {
    RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            null,
            null);
    Prepared initial =
        new Prepared(PREPARED_QUERY_ID, RESULT_METADATA_ID, variablesMetadata, resultMetadata);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                .shortBytes("0xdeadbeef") // result metadata id
                // Variables metadata with no variables:
                .int_(0x0000)
                .int_(0)
                .int_(0)
                // Result metadata with 2 columns:
                .int_(0x0001)
                .int_(2)
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.BLOB));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                + (PrimitiveSizes.SHORT + "deadbeef".length() / 2)
                + (PrimitiveSizes.INT + PrimitiveSizes.INT + PrimitiveSizes.INT)
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT)));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(Bytes.toHexString(decoded.resultMetadataId)).isEqualTo("0xdeadbeef");
    assertThat(decoded.variablesMetadata)
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPagingState()
        .hasNoPkIndices();
    assertThat(decoded.resultMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE))
        .hasColumnCount(2)
        .hasNoPkIndices();
  }
}
