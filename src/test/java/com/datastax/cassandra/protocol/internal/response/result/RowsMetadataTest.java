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

import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class RowsMetadataTest {
  private static final RawType INT_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
  private static final RawType VARCHAR_TYPE =
      RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR);

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_minimal_metadata(int protocolVersion) {
    RowsMetadata initial = new RowsMetadata(Collections.emptyList(), null, null);

    MockBinaryString encoded = encodeWithoutPkIndices(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(0x0004) // no metadata
                .int_(0) // column count
            );
    assertThat(encodedSizeWithoutPkIndices(initial, protocolVersion)).isEqualTo(4 + 4);

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded).hasNoPagingState().hasNoColumnSpecs().hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_column_specs(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", INT_TYPE),
                new ColumnSpec("ks2", "table2", "column2", VARCHAR_TYPE)),
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
            4
                + 4
                + ((2 + "ks1".length()) + (2 + "table1".length()) + (2 + "column1".length()) + 2)
                + ((2 + "ks2".length()) + (2 + "table2".length()) + (2 + "column2".length()) + 2));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded).hasNoPagingState().hasColumnSpecs(initial.columnSpecs).hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_column_specs_with_global_table(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", INT_TYPE),
                new ColumnSpec("ks1", "table1", "column2", VARCHAR_TYPE)),
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
            4
                + 4
                + (2 + "ks1".length())
                + (2 + "table1".length())
                + ((2 + "column1".length()) + 2)
                + ((2 + "column2".length()) + 2));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded).hasNoPagingState().hasColumnSpecs(initial.columnSpecs).hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_paging_state(int protocolVersion) {
    RowsMetadata initial =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", INT_TYPE),
                new ColumnSpec("ks1", "table1", "column2", VARCHAR_TYPE)),
            Bytes.fromHexString("0xcafebabe"),
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
            4
                + 4
                + (4 + "cafebabe".length() / 2)
                + (2 + "ks1".length())
                + (2 + "table1".length())
                + ((2 + "column1".length()) + 2)
                + ((2 + "column2".length()) + 2));

    RowsMetadata decoded = decodeWithoutPkIndices(encoded, protocolVersion);

    assertThat(decoded)
        .hasPagingState("0xcafebabe")
        .hasColumnSpecs(initial.columnSpecs)
        .hasNoPkIndices();
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
