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
package com.datastax.oss.protocol.internal.response.result;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class RowsMetadataTest {
  private static final RawType INT_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
  private static final RawType VARCHAR_TYPE =
      RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR);
  private byte[] newResultMetadataId = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_metadata_with_zero_columns(int protocolVersion) {
    RowsMetadata initial = new RowsMetadata(Collections.emptyList(), null, null, null);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0000) // no flags
                .int_(0) // column count
            );
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion)).isEqualTo(4 + 4);

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasNoPagingState()
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPkIndices()
        .hasNoNewResultMetadataId();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_metadata_with_no_metadata_flag(int protocolVersion) {
    RowsMetadata initial = new RowsMetadata(3, null, null, null);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0004) // no metadata
                .int_(3) // column count
            );
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion)).isEqualTo(4 + 4);

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasNoPagingState()
        .hasNoColumnSpecs()
        .hasColumnCount(3)
        .hasNoPkIndices()
        .hasNoNewResultMetadataId();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_column_specs(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, INT_TYPE),
                new ColumnSpec("ks2", "table2", "column2", 1, VARCHAR_TYPE)),
            null,
            null,
            null);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0000) // no flags
                .int_(2) // column count
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.INT)
                .string("ks2")
                .string("table2")
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.VARCHAR));
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + ((PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + (PrimitiveSizes.SHORT + "column1".length())
                    + PrimitiveSizes.SHORT)
                + ((PrimitiveSizes.SHORT + "ks2".length())
                    + (PrimitiveSizes.SHORT + "table2".length())
                    + (PrimitiveSizes.SHORT + "column2".length())
                    + PrimitiveSizes.SHORT));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasNoPagingState()
        .hasColumnSpecs(initial.columnSpecs)
        .hasColumnCount(2)
        .hasNoPkIndices()
        .hasNoNewResultMetadataId();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_column_specs_with_global_table(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, INT_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, VARCHAR_TYPE)),
            null,
            null,
            null);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0001) // global table spec
                .int_(2) // column count
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.INT)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.VARCHAR));
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "ks1".length())
                + (PrimitiveSizes.SHORT + "table1".length())
                + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasNoPagingState()
        .hasColumnSpecs(initial.columnSpecs)
        .hasColumnCount(2)
        .hasNoPkIndices()
        .hasNoNewResultMetadataId();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_paging_state(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, INT_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, VARCHAR_TYPE)),
            Bytes.fromHexString("0xcafebabe"),
            null,
            null);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0001 | 0x0002) // global table spec, has more pages
                .int_(2) // column count
                .bytes("0xcafebabe") // paging state
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.INT)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.VARCHAR));
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + (PrimitiveSizes.SHORT + "ks1".length())
                + (PrimitiveSizes.SHORT + "table1".length())
                + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasPagingState("0xcafebabe")
        .hasColumnSpecs(initial.columnSpecs)
        .hasColumnCount(2)
        .hasNoPkIndices()
        .hasNoNewResultMetadataId();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_metadata_with_new_result_metadata_id(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(Collections.emptyList(), null, null, newResultMetadataId);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0008) // metadata changed
                .int_(0) // column count
                .shortBytes("0xdeadbeef"));
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion))
        .isEqualTo(4 + 4 + (PrimitiveSizes.SHORT + "deadbeef".length() / 2));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasNoPagingState()
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPkIndices()
        .hasNewResultMetadataId("0xdeadbeef");
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_metadata_with_paging_state_and_new_result_metadata_id(
      int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            Collections.emptyList(), Bytes.fromHexString("0xcafebabe"), null, newResultMetadataId);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x000A) // has more pages + metadata changed
                .int_(0) // column count
                .bytes("0xcafebabe")
                .shortBytes("0xdeadbeef"));
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion))
        .isEqualTo(
            4
                + 4
                + (PrimitiveSizes.INT + "cafebabe".length() / 2)
                + (PrimitiveSizes.SHORT + "deadbeef".length() / 2));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasPagingState("0xcafebabe")
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPkIndices()
        .hasNewResultMetadataId("0xdeadbeef");
  }

  private MockBinaryString encodeWithoutPkIndices(RowsMetadata metadata, int protocolVersion) {
    MockBinaryString dest = new MockBinaryString();
    metadata.encode(dest, MockPrimitiveCodec.INSTANCE, false, protocolVersion);
    return dest;
  }

  private int encodedSizeWithoutPkIndices(RowsMetadata metadata, int protocolVersion) {
    return metadata.encodedSize(false, protocolVersion);
  }

  private RowsMetadata decodeWithoutPkIndices(MockBinaryString source, int protocolVersion) {
    return RowsMetadata.decode(source, MockPrimitiveCodec.INSTANCE, false, protocolVersion);
  }
}
