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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class RowsTest extends MessageTest<Rows> {
  private static final RawType BLOB_TYPE = RawType.PRIMITIVES.get(ProtocolConstants.DataType.BLOB);

  public RowsTest() {
    super(Rows.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Result.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    RowsMetadata metadata =
        new RowsMetadata(
            Arrays.asList(
                new ColumnSpec("ks1", "table1", "column1", BLOB_TYPE),
                new ColumnSpec("ks1", "table1", "column2", BLOB_TYPE)),
            null,
            null);
    Queue<List<ByteBuffer>> data = new LinkedList<>();
    data.add(Arrays.asList(Bytes.fromHexString("0x11"), Bytes.fromHexString("0x12")));
    data.add(Arrays.asList(Bytes.fromHexString("0x21"), Bytes.fromHexString("0x22")));
    data.add(Arrays.asList(Bytes.fromHexString("0x31"), Bytes.fromHexString("0x32")));
    Rows initial = new Rows(metadata, data);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
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
                .bytes("0x32"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            4
                + +(4
                    + 4
                    + (2 + "ks1".length())
                    + (2 + "table1".length())
                    + ((2 + "column1".length()) + 2)
                    + ((2 + "column2".length()) + 2))
                + (4
                    + (4 + "11".length() / 2)
                    + (4 + "12".length() / 2)
                    + (4 + "21".length() / 2)
                    + (4 + "22".length() / 2)
                    + (4 + "31".length() / 2)
                    + (4 + "32".length() / 2)));

    Rows decoded = decode(encoded, protocolVersion);

    assertThat(decoded)
        .hasNextRow("0x11", "0x12")
        .hasNextRow("0x21", "0x22")
        .hasNextRow("0x31", "0x32");
  }
}
