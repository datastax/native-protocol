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
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class RowsMetadataTest {
  private static final RawType INT_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.INT);
  private static final RawType VARCHAR_TYPE =
      RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR);

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_minimal_metadata(int protocolVersion) {
    assertThat(
            decodeForRows(
                new MockBinaryString()
                    .int_(0x0004) // no metadata (nor global table spec, nor more pages)
                    .int_(0), // column count
                protocolVersion))
        .hasColumnCount(0)
        .hasNoPagingState()
        .hasNoColumnSpecs()
        .hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_column_specs(int protocolVersion) {
    assertThat(
            decodeForRows(
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
                    .unsignedShort(ProtocolConstants.DataType.VARCHAR),
                protocolVersion))
        .hasColumnCount(2)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", INT_TYPE),
            new ColumnSpec("ks2", "table2", "column2", VARCHAR_TYPE))
        .hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_use_global_table_spec(int protocolVersion) {
    assertThat(
            decodeForRows(
                new MockBinaryString()
                    .int_(0x0001) // global table spec
                    .int_(2) // column count
                    .string("ks1")
                    .string("table1")
                    .string("column1")
                    .unsignedShort(ProtocolConstants.DataType.INT)
                    .string("column2")
                    .unsignedShort(ProtocolConstants.DataType.VARCHAR),
                protocolVersion))
        .hasColumnCount(2)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", INT_TYPE),
            new ColumnSpec("ks1", "table1", "column2", VARCHAR_TYPE))
        .hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_paging_state(int protocolVersion) {
    assertThat(
            decodeForRows(
                new MockBinaryString()
                    .int_(0x0001 | 0x0002) // global table spec, has more pages
                    .int_(2) // column count
                    .bytes("0xcafebabe") // paging state
                    .string("ks1")
                    .string("table1")
                    .string("column1")
                    .unsignedShort(ProtocolConstants.DataType.INT)
                    .string("column2")
                    .unsignedShort(ProtocolConstants.DataType.VARCHAR),
                protocolVersion))
        .hasColumnCount(2)
        .hasPagingState("0xcafebabe")
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", INT_TYPE),
            new ColumnSpec("ks1", "table1", "column2", VARCHAR_TYPE))
        .hasNoPkIndices();
  }

  private RowsMetadata decodeForRows(MockBinaryString source, int protocolVersion) {
    return RowsMetadata.decodeWithoutPkIndices(
        source, MockPrimitiveCodec.INSTANCE, protocolVersion);
  }
}
