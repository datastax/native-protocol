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
package com.datastax.oss.protocol.internal.request;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class BatchTest extends MessageTestBase<Batch> {
  private final byte[] queryId = Bytes.getArray(Bytes.fromHexString("0xcafebabe"));

  public BatchTest() {
    super(Batch.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Batch.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_with_default_options_v3_v4(int protocolVersion) {
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            NullAllowingImmutableList.of("SELECT * FROM foo", queryId),
            NullAllowingImmutableList.of(
                Collections.emptyList(),
                NullAllowingImmutableList.of(
                    Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.SERIAL,
            Long.MIN_VALUE,
            null,
            Integer.MIN_VALUE);

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
            PrimitiveSizes.BYTE // batch type
                + PrimitiveSizes.SHORT // number of queries
                // query 1
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.INT + "SELECT * FROM foo".length())
                    + PrimitiveSizes.SHORT)
                // query 2
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.INT + "0a".length() / 2)
                    + (PrimitiveSizes.INT + "0b".length() / 2))
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE);

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
    assertThat(decoded.keyspace).isEqualTo(initial.keyspace);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_default_options(int protocolVersion) {
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            NullAllowingImmutableList.of("SELECT * FROM foo", queryId),
            NullAllowingImmutableList.of(
                Collections.emptyList(),
                NullAllowingImmutableList.of(
                    Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.SERIAL,
            Long.MIN_VALUE,
            null,
            Integer.MIN_VALUE);

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
            PrimitiveSizes.BYTE // batch type
                + PrimitiveSizes.SHORT // number of queries
                // query 1
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.INT + "SELECT * FROM foo".length())
                    + PrimitiveSizes.SHORT)
                // query 2
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.INT + "0a".length() / 2)
                    + (PrimitiveSizes.INT + "0b".length() / 2))
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT); // flags

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
    assertThat(decoded.keyspace).isEqualTo(initial.keyspace);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_with_custom_options_v3_v4(int protocolVersion) {
    long timestamp = 1234L;
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            NullAllowingImmutableList.of("SELECT * FROM foo", queryId),
            NullAllowingImmutableList.of(
                Collections.emptyList(),
                NullAllowingImmutableList.of(
                    Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL, // non-default serial CL
            timestamp,
            null,
            Integer.MIN_VALUE);

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
            PrimitiveSizes.BYTE // batch type
                + PrimitiveSizes.SHORT // number of queries
                // query 1
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.INT + "SELECT * FROM foo".length())
                    + PrimitiveSizes.SHORT)
                // query 2
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.INT + "0a".length() / 2)
                    + (PrimitiveSizes.INT + "0b".length() / 2))
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.BYTE // flags
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.LONG);

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
    assertThat(decoded.keyspace).isEqualTo(initial.keyspace);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_with_custom_options(int protocolVersion) {
    long timestamp = 1234L;
    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            NullAllowingImmutableList.of("SELECT * FROM foo", queryId),
            NullAllowingImmutableList.of(
                Collections.emptyList(),
                NullAllowingImmutableList.of(
                    Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL, // non-default serial CL
            timestamp,
            "ks",
            Integer.MIN_VALUE);

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
                .int_(0x10 | 0x20 | 0x80) // flags (serial CL, timestamp and keyspace)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
                .long_(timestamp)
                .string("ks"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.BYTE // batch type
                + PrimitiveSizes.SHORT // number of queries
                // query 1
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.INT + "SELECT * FROM foo".length())
                    + PrimitiveSizes.SHORT)
                // query 2
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.INT + "0a".length() / 2)
                    + (PrimitiveSizes.INT + "0b".length() / 2))
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT // flags
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.LONG
                + (PrimitiveSizes.SHORT + "ks".length()));

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.type).isEqualTo(ProtocolConstants.BatchType.LOGGED);
    assertThat(decoded.queriesOrIds).hasSize(2);
    assertThat(decoded.queriesOrIds.get(0)).isEqualTo("SELECT * FROM foo");
    assertThat(Bytes.toHexString((byte[]) decoded.queriesOrIds.get(1))).isEqualTo("0xcafebabe");
    assertThat(decoded.values).isEqualTo(initial.values);
    assertThat(decoded.consistency).isEqualTo(initial.consistency);
    assertThat(decoded.serialConsistency).isEqualTo(initial.serialConsistency);
    assertThat(decoded.defaultTimestamp).isEqualTo(initial.defaultTimestamp);
    assertThat(decoded.keyspace).isEqualTo(initial.keyspace);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_with_now_in_seconds(int protocolVersion) {
    int nowInSeconds = 123456789;

    Batch initial =
        new Batch(
            ProtocolConstants.BatchType.LOGGED,
            NullAllowingImmutableList.of("SELECT * FROM foo", queryId),
            NullAllowingImmutableList.of(
                Collections.emptyList(),
                NullAllowingImmutableList.of(
                    Bytes.fromHexString("0x0a"), Bytes.fromHexString("0x0b"))),
            ProtocolConstants.ConsistencyLevel.ONE,
            ProtocolConstants.ConsistencyLevel.SERIAL,
            Long.MIN_VALUE,
            null,
            nowInSeconds);

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
                .int_(0x100) // flags (now_in_seconds)
                .int_(nowInSeconds));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.BYTE // batch type
                + PrimitiveSizes.SHORT // number of queries
                // query 1
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.INT + "SELECT * FROM foo".length())
                    + PrimitiveSizes.SHORT)
                // query 2
                + (PrimitiveSizes.BYTE
                    + (PrimitiveSizes.SHORT + "cafebabe".length() / 2)
                    + PrimitiveSizes.SHORT
                    + (PrimitiveSizes.INT + "0a".length() / 2)
                    + (PrimitiveSizes.INT + "0b".length() / 2))
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT // flags
                + PrimitiveSizes.INT);

    Batch decoded = decode(encoded, protocolVersion);

    assertThat(decoded.nowInSeconds).isEqualTo(nowInSeconds);
  }
}
