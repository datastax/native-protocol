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
package com.datastax.oss.protocol.internal.response.result;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;
import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V3;
import static com.datastax.oss.protocol.internal.ProtocolConstants.Version.V4;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class PreparedTest extends MessageTestBase<Prepared> {
  private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);
  private static final byte[] PREPARED_QUERY_ID = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));
  private static final byte[] RESULT_METADATA_ID =
      Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));

  public PreparedTest() {
    super(Prepared.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion);
  }

  @Test
  public void should_encode_and_decode_with_empty_result_in_protocol_v3() {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            null,
            null);
    RowsMetadata resultMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, null, variablesMetadata, resultMetadata);
    int protocolVersion = V3;

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                // Variables metadata with 2 variables:
                .int_(0x0001)
                .int_(2)
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                // Empty result metadata:
                .int_(0x0000)
                .int_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT))
                + (PrimitiveSizes.INT + PrimitiveSizes.INT));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(variablesMetadata.columnSpecs)
        .hasColumnCount(2)
        .hasNoPkIndices();
    assertThat(decoded.resultMetadata)
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPagingState()
        .hasNoPkIndices();
  }

  @Test
  public void should_encode_and_decode_with_non_empty_result_in_protocol_v3() {
    RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            null,
            null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, null, variablesMetadata, resultMetadata);
    int protocolVersion = V3;

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                // Variables metadata with no variables:
                .int_(0x0000)
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
                + (PrimitiveSizes.INT + PrimitiveSizes.INT)
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT)));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata)
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPagingState()
        .hasNoPkIndices();
    assertThat(decoded.resultMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(resultMetadata.columnSpecs)
        .hasColumnCount(2)
        .hasNoPkIndices();
  }

  @Test
  public void should_encode_and_decode_with_empty_result_in_protocol_v4() {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            new int[] {0},
            null);
    RowsMetadata resultMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, null, variablesMetadata, resultMetadata);

    MockBinaryString encoded = encode(initial, V4);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                // Variables metadata with 2 variables and 1 pk index:
                .int_(0x0001)
                .int_(2)
                .int_(1)
                .unsignedShort(0)
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                // Empty result metadata:
                .int_(0x0000)
                .int_(0));
    assertThat(encodedSize(initial, V4))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT))
                + (PrimitiveSizes.INT + PrimitiveSizes.INT));

    Prepared decoded = decode(encoded, V4);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE))
        .hasColumnCount(2)
        .hasPkIndices(0);
    assertThat(decoded.resultMetadata)
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPagingState()
        .hasNoPkIndices();
  }

  @Test
  public void should_encode_and_decode_with_non_empty_result_in_protocol_v4() {
    RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            null,
            null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, null, variablesMetadata, resultMetadata);

    MockBinaryString encoded = encode(initial, V4);

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
    assertThat(encodedSize(initial, V4))
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

    Prepared decoded = decode(encoded, V4);

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

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_empty_result_in_protocol_v5_or_above(
      int protocolVersion) {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            NullAllowingImmutableList.of(
                new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE)),
            null,
            new int[] {0},
            null);
    RowsMetadata resultMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    Prepared initial =
        new Prepared(PREPARED_QUERY_ID, RESULT_METADATA_ID, variablesMetadata, resultMetadata);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                .shortBytes("0xdeadbeef") // result metadata id
                // Variables metadata with 2 variables and 1 pk index:
                .int_(0x0001)
                .int_(2)
                .int_(1)
                .unsignedShort(0)
                .string("ks1")
                .string("table1")
                .string("column1")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                .string("column2")
                .unsignedShort(ProtocolConstants.DataType.BLOB)
                // Empty result metadata:
                .int_(0x0000)
                .int_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                + (PrimitiveSizes.SHORT + "deadbeef".length() / 2)
                + (PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + PrimitiveSizes.INT
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.SHORT + "ks1".length())
                    + (PrimitiveSizes.SHORT + "table1".length())
                    + ((PrimitiveSizes.SHORT + "column1".length()) + PrimitiveSizes.SHORT)
                    + ((PrimitiveSizes.SHORT + "column2".length()) + PrimitiveSizes.SHORT))
                + (PrimitiveSizes.INT + PrimitiveSizes.INT));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(Bytes.toHexString(decoded.resultMetadataId)).isEqualTo("0xdeadbeef");
    assertThat(decoded.variablesMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", 0, BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", 1, BLOB_TYPE))
        .hasColumnCount(2)
        .hasPkIndices(0);
    assertThat(decoded.resultMetadata)
        .hasNoColumnSpecs()
        .hasColumnCount(0)
        .hasNoPagingState()
        .hasNoPkIndices();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_non_empty_result_in_protocol_v5_and_above(
      int protocolVersion) {
    RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null, null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            NullAllowingImmutableList.of(
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
