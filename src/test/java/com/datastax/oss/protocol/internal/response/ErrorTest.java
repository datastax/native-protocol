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
package com.datastax.oss.protocol.internal.response;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.datastax.oss.protocol.internal.response.error.CASWriteUnknown;
import com.datastax.oss.protocol.internal.response.error.FunctionFailure;
import com.datastax.oss.protocol.internal.response.error.ReadFailure;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.error.WriteFailure;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ErrorTest extends MessageTestBase<Error> {

  private static final String MOCK_MESSAGE = "mock message";

  public ErrorTest() {
    super(Error.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Error.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_simple_error(int protocolVersion) {
    int[] simpleErrorCodes = {
      ProtocolConstants.ErrorCode.SERVER_ERROR,
      ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
      ProtocolConstants.ErrorCode.AUTH_ERROR,
      ProtocolConstants.ErrorCode.OVERLOADED,
      ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING,
      ProtocolConstants.ErrorCode.TRUNCATE_ERROR,
      ProtocolConstants.ErrorCode.SYNTAX_ERROR,
      ProtocolConstants.ErrorCode.UNAUTHORIZED,
      ProtocolConstants.ErrorCode.INVALID,
      ProtocolConstants.ErrorCode.CONFIG_ERROR,
      ProtocolConstants.ErrorCode.CDC_WRITE_FAILURE
    };
    for (int errorCode : simpleErrorCodes) {
      Error initial = new Error(errorCode, MOCK_MESSAGE);

      MockBinaryString encoded = encode(initial, protocolVersion);

      assertThat(encoded).isEqualTo(new MockBinaryString().int_(errorCode).string(MOCK_MESSAGE));
      assertThat(encodedSize(initial, protocolVersion))
          .isEqualTo(PrimitiveSizes.INT + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length()));

      Error decoded = decode(encoded, protocolVersion);

      assertThat(decoded.code).isEqualTo(errorCode);
      assertThat(decoded.message).isEqualTo(MOCK_MESSAGE);
    }
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_unprepared(int protocolVersion) {
    Unprepared initial =
        new Unprepared(MOCK_MESSAGE, Bytes.getArray(Bytes.fromHexString("0xcafebabe")));

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.UNPREPARED)
                .string(MOCK_MESSAGE)
                .shortBytes("0xcafebabe"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + (PrimitiveSizes.SHORT + "cafebabe".length() / 2));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.UNPREPARED);
    assertThat(decoded).isInstanceOf(Unprepared.class);
    Unprepared unprepared = (Unprepared) decoded;
    assertThat(unprepared.message).isEqualTo(MOCK_MESSAGE);
    assertThat(Bytes.toHexString(unprepared.id)).isEqualTo("0xcafebabe");
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_already_exists(int protocolVersion) {
    AlreadyExists initial = new AlreadyExists(MOCK_MESSAGE, "ks", "table");

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.ALREADY_EXISTS)
                .string(MOCK_MESSAGE)
                .string("ks")
                .string("table"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + (PrimitiveSizes.SHORT + "ks".length())
                + (PrimitiveSizes.SHORT + "table".length()));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.ALREADY_EXISTS);
    assertThat(decoded).isInstanceOf(AlreadyExists.class);
    AlreadyExists alreadyExists = (AlreadyExists) decoded;
    assertThat(alreadyExists.message).isEqualTo(MOCK_MESSAGE);
    assertThat(alreadyExists.keyspace).isEqualTo("ks");
    assertThat(alreadyExists.table).isEqualTo("table");
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_unavailable(int protocolVersion) {
    Unavailable initial =
        new Unavailable(MOCK_MESSAGE, ProtocolConstants.ConsistencyLevel.QUORUM, 2, 1);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.UNAVAILABLE)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(1));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT);

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.UNAVAILABLE);
    assertThat(decoded).isInstanceOf(Unavailable.class);
    Unavailable unavailable = (Unavailable) decoded;
    assertThat(unavailable.message).isEqualTo(MOCK_MESSAGE);
    assertThat(unavailable.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(unavailable.required).isEqualTo(2);
    assertThat(unavailable.alive).isEqualTo(1);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_read_timeout(int protocolVersion) {
    ReadTimeout initial =
        new ReadTimeout(MOCK_MESSAGE, ProtocolConstants.ConsistencyLevel.QUORUM, 2, 3, false);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.READ_TIMEOUT)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .byte_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.BYTE);

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.READ_TIMEOUT);
    assertThat(decoded).isInstanceOf(ReadTimeout.class);
    ReadTimeout readTimeout = (ReadTimeout) decoded;
    assertThat(readTimeout.message).isEqualTo(MOCK_MESSAGE);
    assertThat(readTimeout.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(readTimeout.received).isEqualTo(2);
    assertThat(readTimeout.blockFor).isEqualTo(3);
    assertThat(readTimeout.dataPresent).isFalse();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_read_failure_v3_v4(int protocolVersion) {
    ReadFailure initial =
        new ReadFailure(
            MOCK_MESSAGE, ProtocolConstants.ConsistencyLevel.QUORUM, 2, 3, 1, null, false);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.READ_FAILURE)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .int_(1)
                .byte_(0));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.BYTE);

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.READ_FAILURE);
    assertThat(decoded).isInstanceOf(ReadFailure.class);
    ReadFailure readFailure = (ReadFailure) decoded;
    assertThat(readFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(readFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(readFailure.received).isEqualTo(2);
    assertThat(readFailure.blockFor).isEqualTo(3);
    assertThat(readFailure.numFailures).isEqualTo(1);
    assertThat(readFailure.reasonMap).isEmpty();
    assertThat(readFailure.dataPresent).isFalse();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_read_failure(int protocolVersion)
      throws UnknownHostException {
    InetAddress addr = InetAddress.getLoopbackAddress();
    Map<InetAddress, Integer> reasonMap = NullAllowingImmutableMap.of(addr, 42);
    ReadFailure initial =
        new ReadFailure(
            MOCK_MESSAGE, ProtocolConstants.ConsistencyLevel.QUORUM, 2, 3, 1, reasonMap, false);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.READ_FAILURE)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2) // received
                .int_(3) // blockFor
                .int_(1) // size of reasonmap
                .inetAddr(addr) // addr
                .unsignedShort(42) // error code
                .byte_(0)); // dataPresent
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT // error code
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length()) // message
                + PrimitiveSizes.SHORT // consistencyLevel
                + PrimitiveSizes.INT // received
                + PrimitiveSizes.INT // blockFor
                + PrimitiveSizes.INT // size of reasonmap
                + (PrimitiveSizes.BYTE + addr.getAddress().length) // addr
                + PrimitiveSizes.SHORT // error code
                + PrimitiveSizes.BYTE); // dataPresent

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.READ_FAILURE);
    assertThat(decoded).isInstanceOf(ReadFailure.class);
    ReadFailure readFailure = (ReadFailure) decoded;
    assertThat(readFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(readFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(readFailure.received).isEqualTo(2);
    assertThat(readFailure.blockFor).isEqualTo(3);
    assertThat(readFailure.numFailures).isEqualTo(1);
    assertThat(readFailure.reasonMap).isEqualTo(reasonMap);
    assertThat(readFailure.dataPresent).isFalse();
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_write_timeout(int protocolVersion) {
    WriteTimeout initial =
        new WriteTimeout(
            MOCK_MESSAGE,
            ProtocolConstants.ConsistencyLevel.QUORUM,
            2,
            3,
            ProtocolConstants.WriteType.SIMPLE);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.WRITE_TIMEOUT)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .string(ProtocolConstants.WriteType.SIMPLE));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + ProtocolConstants.WriteType.SIMPLE.length()));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.WRITE_TIMEOUT);
    assertThat(decoded).isInstanceOf(WriteTimeout.class);
    WriteTimeout writeTimeout = (WriteTimeout) decoded;
    assertThat(writeTimeout.message).isEqualTo(MOCK_MESSAGE);
    assertThat(writeTimeout.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(writeTimeout.received).isEqualTo(2);
    assertThat(writeTimeout.blockFor).isEqualTo(3);
    assertThat(writeTimeout.writeType).isEqualTo(ProtocolConstants.WriteType.SIMPLE);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrV4")
  public void should_encode_and_decode_write_failure_v3_v4(int protocolVersion) {
    WriteFailure initial =
        new WriteFailure(
            MOCK_MESSAGE,
            ProtocolConstants.ConsistencyLevel.QUORUM,
            2,
            3,
            1,
            null,
            ProtocolConstants.WriteType.SIMPLE);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.WRITE_FAILURE)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .int_(1)
                .string(ProtocolConstants.WriteType.SIMPLE));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + ProtocolConstants.WriteType.SIMPLE.length()));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.WRITE_FAILURE);
    assertThat(decoded).isInstanceOf(WriteFailure.class);
    WriteFailure writeFailure = (WriteFailure) decoded;
    assertThat(writeFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(writeFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(writeFailure.received).isEqualTo(2);
    assertThat(writeFailure.blockFor).isEqualTo(3);
    assertThat(writeFailure.numFailures).isEqualTo(1);
    assertThat(writeFailure.reasonMap).isEmpty();
    assertThat(writeFailure.writeType).isEqualTo(ProtocolConstants.WriteType.SIMPLE);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_write_failure(int protocolVersion)
      throws UnknownHostException {
    InetAddress addr = InetAddress.getLoopbackAddress();
    Map<InetAddress, Integer> reasonMap = NullAllowingImmutableMap.of(addr, 42);
    WriteFailure initial =
        new WriteFailure(
            MOCK_MESSAGE,
            ProtocolConstants.ConsistencyLevel.QUORUM,
            2,
            3,
            1,
            reasonMap,
            ProtocolConstants.WriteType.SIMPLE);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.WRITE_FAILURE)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2) // received
                .int_(3) // blockFor
                .int_(1) // size of reasonmap
                .inetAddr(addr) // addr
                .unsignedShort(42) // error code
                .string(ProtocolConstants.WriteType.SIMPLE)); // writeType
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT // error code
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length()) // message
                + PrimitiveSizes.SHORT // consistencyLevel
                + PrimitiveSizes.INT // received
                + PrimitiveSizes.INT // blockFor
                + PrimitiveSizes.INT // size of reasonmap
                + (PrimitiveSizes.BYTE + addr.getAddress().length) // addr
                + PrimitiveSizes.SHORT // error code
                + (PrimitiveSizes.SHORT
                    + ProtocolConstants.WriteType.SIMPLE.length())); // writeType

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.WRITE_FAILURE);
    assertThat(decoded).isInstanceOf(WriteFailure.class);
    WriteFailure writeFailure = (WriteFailure) decoded;
    assertThat(writeFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(writeFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(writeFailure.received).isEqualTo(2);
    assertThat(writeFailure.blockFor).isEqualTo(3);
    assertThat(writeFailure.numFailures).isEqualTo(1);
    assertThat(writeFailure.reasonMap).isEqualTo(reasonMap);
    assertThat(writeFailure.writeType).isEqualTo(ProtocolConstants.WriteType.SIMPLE);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode_function_failure(int protocolVersion) {
    FunctionFailure initial =
        new FunctionFailure(
            MOCK_MESSAGE, "keyspace", "function", NullAllowingImmutableList.of("int", "varchar"));

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.FUNCTION_FAILURE)
                .string(MOCK_MESSAGE)
                .string("keyspace")
                .string("function")
                .unsignedShort(2)
                .string("int")
                .string("varchar"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + (PrimitiveSizes.SHORT + "keyspace".length())
                + (PrimitiveSizes.SHORT + "function".length())
                + (PrimitiveSizes.SHORT
                    + PrimitiveSizes.SHORT
                    + "int".length()
                    + PrimitiveSizes.SHORT
                    + "varchar".length()));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.FUNCTION_FAILURE);
    assertThat(decoded).isInstanceOf(FunctionFailure.class);
    FunctionFailure functionFailure = (FunctionFailure) decoded;
    assertThat(functionFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(functionFailure.keyspace).isEqualTo("keyspace");
    assertThat(functionFailure.function).isEqualTo("function");
    assertThat(functionFailure.argTypes).containsExactly("int", "varchar");
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV5OrAbove")
  public void should_encode_and_decode_cas_write_unknown(int protocolVersion) {
    CASWriteUnknown initial =
        new CASWriteUnknown(MOCK_MESSAGE, ProtocolConstants.ConsistencyLevel.QUORUM, 2, 3);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.CAS_WRITE_UNKNOWN)
                .string(MOCK_MESSAGE)
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length())
                + PrimitiveSizes.SHORT
                + PrimitiveSizes.INT
                + PrimitiveSizes.INT);

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.CAS_WRITE_UNKNOWN);
    assertThat(decoded).isInstanceOf(CASWriteUnknown.class);
    CASWriteUnknown casWriteUnknown = (CASWriteUnknown) decoded;
    assertThat(casWriteUnknown.message).isEqualTo(MOCK_MESSAGE);
    assertThat(casWriteUnknown.consistencyLevel)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(casWriteUnknown.received).isEqualTo(2);
    assertThat(casWriteUnknown.blockFor).isEqualTo(3);
  }
}
