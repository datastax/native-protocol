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
package com.datastax.cassandra.protocol.internal.request;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class BatchTest extends MessageTest<Batch> {
  private final byte[] queryId = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));

  public BatchTest() {
    super(Batch.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Batch.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrV4")
  public void should_encode_and_decode_with_default_options_v3_v4(int protocolVersion) {
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            Arrays.asList("SELECT * FROM foo", queryId),
            Arrays.asList(
                Collections.emptyList(),
                Arrays.asList(Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.SERIAL,
            Long.MIN_VALUE);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .byte_(ProtocolConstants.BatchType.LOGGED)
                .unsignedShort(2) // number of queries
                // query 1: string, no values
                .byte_(0)
                .longString("SELECT * FROM foo")
                .unsignedShort(0)
                // query 2: id, 2 values
                .byte_(1)
                .shortBytes("0xcafebabe")
                .unsignedShort(2)
                .bytes("0x0a")
                .bytes("0x0b")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0) // flags (empty)
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            1 // batch type
                + 2 // number of queries
                // query 1
                + (1 + (4 + "SELECT * FROM foo".length()) + 2)
                // query 2
                + (1
                    + (2 + "cafebabe".length() / 2)
                    + 2
                    + (4 + "0a".length() / 2)
                    + (4 + "0b".length() / 2))
                + 2
                + 1);

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV5OrAbove")
  public void should_encode_and_decode_with_default_options(int protocolVersion) {
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            Arrays.asList("SELECT * FROM foo", queryId),
            Arrays.asList(
                Collections.emptyList(),
                Arrays.asList(Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.SERIAL,
            Long.MIN_VALUE);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .byte_(ProtocolConstants.BatchType.LOGGED)
                .unsignedShort(2) // number of queries
                // query 1: string, no values
                .byte_(0)
                .longString("SELECT * FROM foo")
                .unsignedShort(0)
                // query 2: id, 2 values
                .byte_(1)
                .shortBytes("0xcafebabe")
                .unsignedShort(2)
                .bytes("0x0a")
                .bytes("0x0b")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0) // flags (empty)
            );

    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            1 // batch type
                + 2 // number of queries
                // query 1
                + (1 + (4 + "SELECT * FROM foo".length()) + 2)
                // query 2
                + (1
                    + (2 + "cafebabe".length() / 2)
                    + 2
                    + (4 + "0a".length() / 2)
                    + (4 + "0b".length() / 2))
                + 2
                + 4); // flags

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrV4")
  public void should_encode_with_custom_options_v3_v4(int protocolVersion) {
    long timestamp = 1234L;
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            Arrays.asList("SELECT * FROM foo", queryId),
            Arrays.asList(
                Collections.emptyList(),
                Arrays.asList(Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL, // non-default serial CL
            timestamp);

    MockBinaryString encoded = encode(initial, protocolVersion);
    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .byte_(ProtocolConstants.BatchType.LOGGED)
                .unsignedShort(2) // number of queries
                // query 1: string, no values
                .byte_(0)
                .longString("SELECT * FROM foo")
                .unsignedShort(0)
                // query 2: id, 2 values
                .byte_(1)
                .shortBytes("0xcafebabe")
                .unsignedShort(2)
                .bytes("0x0a")
                .bytes("0x0b")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x10 | 0x20) // flags (serial CL and timestamp)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
                .long_(timestamp));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            1 // batch type
                + 2 // number of queries
                // query 1
                + (1 + (4 + "SELECT * FROM foo".length()) + 2)
                // query 2
                + (1
                    + (2 + "cafebabe".length() / 2)
                    + 2
                    + (4 + "0a".length() / 2)
                    + (4 + "0b".length() / 2))
                + 2
                + 1 // flags
                + 2
                + 8);

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV5OrAbove")
  public void should_encode_with_custom_options(int protocolVersion) {
    long timestamp = 1234L;
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            Arrays.asList("SELECT * FROM foo", queryId),
            Arrays.asList(
                Collections.emptyList(),
                Arrays.asList(Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL, // non-default serial CL
            timestamp);

    MockBinaryString encoded = encode(initial, protocolVersion);
    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .byte_(ProtocolConstants.BatchType.LOGGED)
                .unsignedShort(2) // number of queries
                // query 1: string, no values
                .byte_(0)
                .longString("SELECT * FROM foo")
                .unsignedShort(0)
                // query 2: id, 2 values
                .byte_(1)
                .shortBytes("0xcafebabe")
                .unsignedShort(2)
                .bytes("0x0a")
                .bytes("0x0b")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x10 | 0x20) // flags (serial CL and timestamp)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
                .long_(timestamp));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            1 // batch type
                + 2 // number of queries
                // query 1
                + (1 + (4 + "SELECT * FROM foo".length()) + 2)
                // query 2
                + (1
                    + (2 + "cafebabe".length() / 2)
                    + 2
                    + (4 + "0a".length() / 2)
                    + (4 + "0b".length() / 2))
                + 2
                + 4 // flags
                + 2
                + 8);

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
  }
}
