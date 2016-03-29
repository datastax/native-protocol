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
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class RowsTest extends MessageTest<Rows> {
  public RowsTest() {
    super(Rows.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode(int protocolVersion) {
    assertThat(
            decode(
                new MockBinaryString()
                    .int_(ProtocolConstants.ResponseKind.ROWS)
                    // Simple metadata with 2 columns:
                    .int_(0x0001)
                    .int_(2)
                    .string("ks1")
                    .string("table1")
                    .string("column1")
                    .unsignedShort(ProtocolConstants.DataType.BLOB)
                    .string("column2")
                    .unsignedShort(ProtocolConstants.DataType.BLOB)
                    // Rows:
                    .int_(3) // count
                    .bytes("0x11")
                    .bytes("0x12")
                    .bytes("0x21")
                    .bytes("0x22")
                    .bytes("0x31")
                    .bytes("0x32"),
                protocolVersion))
        .hasNextRow("0x11", "0x12")
        .hasNextRow("0x21", "0x22")
        .hasNextRow("0x31", "0x32");
  }
}
