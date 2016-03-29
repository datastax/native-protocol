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
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class PreparedTest extends MessageTest<Prepared> {

  private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);

  public PreparedTest() {
    super(Prepared.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion);
  }

  @Test
  public void should_decode_without_result_metadata_in_protocol_v3() {
    Prepared prepared =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.PREPARED)
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
                .int_(0),
            ProtocolConstants.Version.V3);

    assertThat(Bytes.toHexString(prepared.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(prepared.variablesMetadata)
        .hasColumnCount(2)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE))
        .hasNoPkIndices();
    assertThat(prepared.resultMetadata)
        .hasColumnCount(0)
        .hasNoColumnSpecs()
        .hasNoPagingState()
        .hasNoPkIndices();
  }

  @Test
  public void should_decode_with_result_metadata_in_protocol_v3() {
    Prepared prepared =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.PREPARED)
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
                .unsignedShort(ProtocolConstants.DataType.BLOB),
            ProtocolConstants.Version.V3);

    assertThat(Bytes.toHexString(prepared.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(prepared.variablesMetadata)
        .hasColumnCount(0)
        .hasNoColumnSpecs()
        .hasNoPagingState()
        .hasNoPkIndices();
    assertThat(prepared.resultMetadata)
        .hasColumnCount(2)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE))
        .hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_without_result_metadata_in_protocol_v4_or_above(int protocolVersion) {
    Prepared prepared =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.PREPARED)
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
                .int_(0),
            protocolVersion);

    assertThat(Bytes.toHexString(prepared.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(prepared.variablesMetadata)
        .hasColumnCount(2)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE))
        .hasPkIndices(0);
    assertThat(prepared.resultMetadata)
        .hasColumnCount(0)
        .hasNoColumnSpecs()
        .hasNoPagingState()
        .hasNoPkIndices();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV4OrAbove")
  public void should_decode_with_result_metadata_in_protocol_v4_or_above(int protocolVersion) {
    Prepared prepared =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ResponseKind.PREPARED)
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
                .unsignedShort(ProtocolConstants.DataType.BLOB),
            protocolVersion);

    assertThat(Bytes.toHexString(prepared.preparedQueryId)).isEqualTo("0xcafebabe");
    assertThat(prepared.variablesMetadata)
        .hasColumnCount(0)
        .hasNoColumnSpecs()
        .hasNoPagingState()
        .hasNoPkIndices();
    assertThat(prepared.resultMetadata)
        .hasColumnCount(2)
        .hasNoPagingState()
        .hasColumnSpecs(
            new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
            new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE))
        .hasNoPkIndices();
  }
}
