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

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.ArrayDeque;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DseRowsTest extends MessageTestBase<Rows> {

  private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);
  private byte[] newResultMetadataId = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));

  public DseRowsTest() {
    super(Rows.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion, new DseRowsSubCodec(protocolVersion));
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV1OrAbove")
  public void should_encode_and_decode_with_continuous_paging_metadata_in_dse_protocols(
      int protocolVersion) {
    DseRowsMetadata metadata =
        new DseRowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            null,
            null,
            10,
            true);
    Rows initial = new DefaultRows(metadata, new ArrayDeque<>());

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.ROWS)
                // Simple metadata with 2 columns:
                .int_(
                    ProtocolConstants.RowsFlag.GLOBAL_TABLES_SPEC
                        | DseProtocolConstants.RowsFlag.CONTINUOUS_PAGING
                        | DseProtocolConstants.RowsFlag.LAST_CONTINUOUS_PAGE)
                .int_(2) // column count
                .int_(10) // continuous paging number
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .int_(0) // row count
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            // kind:
            PrimitiveSizes.INT
                // metadata:
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT))
                // row count:
                + PrimitiveSizes.INT);

    Rows decoded = decode(encoded, protocolVersion);

    RowsMetadata decodedMetadata = decoded.getMetadata();
    assertThat(decodedMetadata).isInstanceOf(DseRowsMetadata.class);
    DseRowsMetadata decodedDseMetadata = (DseRowsMetadata) decodedMetadata;
    assertThat(decodedDseMetadata.continuousPageNumber).isEqualTo(10);
    assertThat(decodedDseMetadata.isLastContinuousPage).isTrue();
  }

  /**
   * Test with continuous paging + new metadata id since both are recent addition to the protocol;
   * also throw in a paging state for good measure, to check that everything is ordered correctly.
   */
  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void
      should_encode_and_decode_with_continuous_paging_metadata_and_new_metadata_id_in_dse_v2_and_above(
          int protocolVersion) {
    DseRowsMetadata metadata =
        new DseRowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            Bytes.fromHexString("0xcafebabe"),
            null,
            newResultMetadataId,
            10,
            true);
    Rows initial = new DefaultRows(metadata, new ArrayDeque<>());

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.ROWS)
                // Simple metadata with 2 columns:
                .int_(
                    ProtocolConstants.RowsFlag.GLOBAL_TABLES_SPEC
                        | ProtocolConstants.RowsFlag.HAS_MORE_PAGES
                        | ProtocolConstants.RowsFlag.METADATA_CHANGED
                        | DseProtocolConstants.RowsFlag.CONTINUOUS_PAGING
                        | DseProtocolConstants.RowsFlag.LAST_CONTINUOUS_PAGE)
                .int_(2) // column count
                .bytes("0xcafebabe") // paging state
                .shortBytes("0xdeadbeef") // new metadata id
                .int_(10) // continuous paging number
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .int_(0) // row count
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            // kind:
            PrimitiveSizes.INT
                // metadata:
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                    + (PrimitiveSizes.SHORT + "deadbeef".length() / 2)
                    + PrimitiveSizes.INT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT))
                // row count:
                + PrimitiveSizes.INT);

    Rows decoded = decode(encoded, protocolVersion);

    RowsMetadata decodedMetadata = decoded.getMetadata();
    assertThat(decodedMetadata).hasPagingState("0xcafebabe").hasNewResultMetadataId("0xdeadbeef");

    assertThat(decodedMetadata).isInstanceOf(DseRowsMetadata.class);
    DseRowsMetadata decodedDseMetadata = (DseRowsMetadata) decodedMetadata;
    assertThat(decodedDseMetadata.continuousPageNumber).isEqualTo(10);
    assertThat(decodedDseMetadata.isLastContinuousPage).isTrue();
  }
}
