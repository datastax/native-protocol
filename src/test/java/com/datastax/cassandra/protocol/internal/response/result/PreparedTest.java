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
package com.datastax.cassandra.protocol.internal.response.result;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.response.Result;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class PreparedTest extends MessageTest<Prepared> {
  private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);
  private static final byte[] PREPARED_QUERY_ID = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));

  public PreparedTest() {
    super(Prepared.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion);
  }

  @Test
  public void should_encode_and_decode_without_result_metadata_in_protocol_v3() {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE)),
            null,
            null);
    RowsMetadata resultMetadata = new RowsMetadata(Collections.emptyList(), null, null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, variablesMetadata, resultMetadata);
    int protocolVersion = ProtocolConstants.Version.V3;

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
                .int_(0x0004)
                .int_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            4
                + (2 + "cafebabe".length() / 2)
                + (4
                    + 4
                    + (2 + "ks1".length())
                    + (2 + "table1".length())
                    + ((2 + "column1".length()) + 2)
                    + ((2 + "column2".length()) + 2))
                + (4 + 4));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(variablesMetadata.columnSpecs)
        .hasNoPkIndices();
    assertThat(decoded.resultMetadata).hasNoColumnSpecs().hasNoPagingState().hasNoPkIndices();
  }

  @Test
  public void should_encode_and_decode_with_result_metadata_in_protocol_v3() {
    RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE)),
            null,
            null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, variablesMetadata, resultMetadata);
    int protocolVersion = ProtocolConstants.Version.V3;

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                // Variables metadata with no variables:
                .int_(0x0004)
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
            4
                + (2 + "cafebabe".length() / 2)
                + (4 + 4)
                + (4
                    + 4
                    + (2 + "ks1".length())
                    + (2 + "table1".length())
                    + ((2 + "column1".length()) + 2)
                    + ((2 + "column2".length()) + 2)));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata).hasNoColumnSpecs().hasNoPagingState().hasNoPkIndices();
    assertThat(decoded.resultMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(resultMetadata.columnSpecs)
        .hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_without_result_metadata_in_protocol_v4_or_above(int protocolVersion) {
    RowsMetadata variablesMetadata =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE)),
            null,
            new int[] {0});
    RowsMetadata resultMetadata = new RowsMetadata(Collections.emptyList(), null, null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, variablesMetadata, resultMetadata);

    MockBinaryString encoded = encode(initial, protocolVersion);

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
                .int_(0x0004)
                .int_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            4
                + (2 + "cafebabe".length() / 2)
                + (4
                    + 4
                    + 4
                    + 2
                    + (2 + "ks1".length())
                    + (2 + "table1".length())
                    + ((2 + "column1".length()) + 2)
                    + ((2 + "column2".length()) + 2))
                + (4 + 4));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE))
        .hasPkIndices(0);
    assertThat(decoded.resultMetadata).hasNoColumnSpecs().hasNoPagingState().hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_with_result_metadata_in_protocol_v4_or_above(int protocolVersion) {
    RowsMetadata variablesMetadata = new RowsMetadata(Collections.emptyList(), null, null);
    RowsMetadata resultMetadata =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE)),
            null,
            null);
    Prepared initial = new Prepared(PREPARED_QUERY_ID, variablesMetadata, resultMetadata);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ResultKind.PREPARED)
                .shortBytes("0xcafebabe") // query id
                // Variables metadata with no variables:
                .int_(0x0004)
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
            4
                + (2 + "cafebabe".length() / 2)
                + (4 + 4 + 4)
                + (4
                    + 4
                    + (2 + "ks1".length())
                    + (2 + "table1".length())
                    + ((2 + "column1".length()) + 2)
                    + ((2 + "column2".length()) + 2)));

    Prepared decoded = decode(encoded, protocolVersion);

    assertThat(Bytes.toHexString(decoded.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(decoded.variablesMetadata).hasNoColumnSpecs().hasNoPagingState().hasNoPkIndices();
    assertThat(decoded.resultMetadata)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE))
        .hasNoPkIndices();
  }
}
