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
package com.datastax.cassandra.protocol.internal.response;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.response.error.AlreadyExists;
import com.datastax.cassandra.protocol.internal.response.error.FunctionFailure;
import com.datastax.cassandra.protocol.internal.response.error.ReadFailure;
import com.datastax.cassandra.protocol.internal.response.error.ReadTimeout;
import com.datastax.cassandra.protocol.internal.response.error.Unavailable;
import com.datastax.cassandra.protocol.internal.response.error.Unprepared;
import com.datastax.cassandra.protocol.internal.response.error.WriteFailure;
import com.datastax.cassandra.protocol.internal.response.error.WriteTimeout;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ErrorTest extends MessageTest<Error> {

  private static final String MOCK_MESSAGE = "mock message";

  protected ErrorTest() {
    super(Error.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Error.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
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
      ProtocolConstants.ErrorCode.CONFIG_ERROR
    };
    for (int errorCode : simpleErrorCodes) {
      Error initial = new Error(errorCode, MOCK_MESSAGE);

      MockBinaryString encoded = encode(initial, protocolVersion);

      assertThat(encoded).isEqualTo(new MockBinaryString().int_(errorCode).string(MOCK_MESSAGE));
      assertThat(encodedSize(initial, protocolVersion)).isEqualTo(4 + 2 + MOCK_MESSAGE.length());

      Error decoded = decode(encoded, protocolVersion);

      assertThat(decoded.code).isEqualTo(errorCode);
      assertThat(decoded.message).isEqualTo(MOCK_MESSAGE);
    }
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
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
        .isEqualTo(4 + (2 + MOCK_MESSAGE.length()) + (2 + "cafebabe".length() / 2));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.UNPREPARED);
    assertThat(decoded).isInstanceOf(Unprepared.class);
    Unprepared unprepared = (Unprepared) decoded;
    assertThat(unprepared.message).isEqualTo(MOCK_MESSAGE);
    assertThat(Bytes.toHexString(unprepared.id)).isEqualTo("0xcafebabe");
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
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
        .isEqualTo(4 + (2 + MOCK_MESSAGE.length()) + (2 + "ks".length()) + (2 + "table".length()));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.ALREADY_EXISTS);
    assertThat(decoded).isInstanceOf(AlreadyExists.class);
    AlreadyExists alreadyExists = (AlreadyExists) decoded;
    Assertions.assertThat(alreadyExists.message).isEqualTo(MOCK_MESSAGE);
    assertThat(alreadyExists.keyspace).isEqualTo("ks");
    assertThat(alreadyExists.table).isEqualTo("table");
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
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
        .isEqualTo(4 + (2 + MOCK_MESSAGE.length()) + 2 + 4 + 4);

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.UNAVAILABLE);
    assertThat(decoded).isInstanceOf(Unavailable.class);
    Unavailable unavailable = (Unavailable) decoded;
    assertThat(unavailable.message).isEqualTo(MOCK_MESSAGE);
    assertThat(unavailable.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(unavailable.required).isEqualTo(2);
    assertThat(unavailable.alive).isEqualTo(1);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
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
        .isEqualTo(4 + (2 + MOCK_MESSAGE.length()) + 2 + 4 + 4 + 1);

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

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_read_failure(int protocolVersion) {
    ReadFailure initial =
        new ReadFailure(MOCK_MESSAGE, ProtocolConstants.ConsistencyLevel.QUORUM, 2, 3, 1, false);

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
        .isEqualTo(4 + (2 + MOCK_MESSAGE.length()) + 2 + 4 + 4 + 4 + 1);

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.READ_FAILURE);
    assertThat(decoded).isInstanceOf(ReadFailure.class);
    ReadFailure readFailure = (ReadFailure) decoded;
    assertThat(readFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(readFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(readFailure.received).isEqualTo(2);
    assertThat(readFailure.blockFor).isEqualTo(3);
    assertThat(readFailure.numFailures).isEqualTo(1);
    assertThat(readFailure.dataPresent).isFalse();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
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
            4
                + (2 + MOCK_MESSAGE.length())
                + 2
                + 4
                + 4
                + (2 + ProtocolConstants.WriteType.SIMPLE.length()));

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

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_write_failure(int protocolVersion) {
    WriteFailure initial =
        new WriteFailure(
            MOCK_MESSAGE,
            ProtocolConstants.ConsistencyLevel.QUORUM,
            2,
            3,
            1,
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

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.WRITE_FAILURE);
    assertThat(decoded).isInstanceOf(WriteFailure.class);
    WriteFailure writeFailure = (WriteFailure) decoded;
    assertThat(writeFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(writeFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(writeFailure.received).isEqualTo(2);
    assertThat(writeFailure.blockFor).isEqualTo(3);
    assertThat(writeFailure.numFailures).isEqualTo(1);
    assertThat(writeFailure.writeType).isEqualTo(ProtocolConstants.WriteType.SIMPLE);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_function_failure(int protocolVersion) {
    FunctionFailure initial =
        new FunctionFailure(MOCK_MESSAGE, "keyspace", "function", "arg types");

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.FUNCTION_FAILURE)
                .string(MOCK_MESSAGE)
                .string("keyspace")
                .string("function")
                .string("arg types"));

    Error decoded = decode(encoded, protocolVersion);

    assertThat(decoded.code).isEqualTo(ProtocolConstants.ErrorCode.FUNCTION_FAILURE);
    assertThat(decoded).isInstanceOf(FunctionFailure.class);
    FunctionFailure functionFailure = (FunctionFailure) decoded;
    assertThat(functionFailure.message).isEqualTo(MOCK_MESSAGE);
    assertThat(functionFailure.keyspace).isEqualTo("keyspace");
    assertThat(functionFailure.function).isEqualTo("function");
    assertThat(functionFailure.argTypes).isEqualTo("arg types");
  }
}
