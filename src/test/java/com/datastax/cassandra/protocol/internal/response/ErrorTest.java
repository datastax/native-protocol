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

  protected ErrorTest() {
    super(Error.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Error.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_simple_error(int protocolVersion) {
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
      Error error =
          decode(new MockBinaryString().int_(errorCode).string("mock message"), protocolVersion);
      assertThat(error.code).isEqualTo(errorCode);
      assertThat(error.message).isEqualTo("mock message");
    }
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_unprepared(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.UNPREPARED)
                .string("mock message")
                .shortBytes("0xcafebabe"),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.UNPREPARED);
    assertThat(error).isInstanceOf(Unprepared.class);
    Unprepared unprepared = (Unprepared) error;
    Assertions.assertThat(unprepared.message).isEqualTo("mock message");
    Assertions.assertThat(Bytes.toHexString(unprepared.id)).isEqualTo("0xcafebabe");
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_already_exists(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.ALREADY_EXISTS)
                .string("mock message")
                .string("ks")
                .string("table"),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.ALREADY_EXISTS);
    assertThat(error).isInstanceOf(AlreadyExists.class);
    AlreadyExists alreadyExists = (AlreadyExists) error;
    Assertions.assertThat(alreadyExists.message).isEqualTo("mock message");
    assertThat(alreadyExists.keyspace).isEqualTo("ks");
    assertThat(alreadyExists.table).isEqualTo("table");
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_unavailable(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.UNAVAILABLE)
                .string("mock message")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(1),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.UNAVAILABLE);
    assertThat(error).isInstanceOf(Unavailable.class);
    Unavailable unavailable = (Unavailable) error;
    assertThat(unavailable.message).isEqualTo("mock message");
    assertThat(unavailable.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(unavailable.required).isEqualTo(2);
    assertThat(unavailable.alive).isEqualTo(1);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_read_timeout(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.READ_TIMEOUT)
                .string("mock message")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .byte_(0),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.READ_TIMEOUT);
    assertThat(error).isInstanceOf(ReadTimeout.class);
    ReadTimeout readTimeout = (ReadTimeout) error;
    Assertions.assertThat(readTimeout.message).isEqualTo("mock message");
    assertThat(readTimeout.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(readTimeout.received).isEqualTo(2);
    assertThat(readTimeout.blockFor).isEqualTo(3);
    assertThat(readTimeout.dataPresent).isFalse();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_read_failure(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.READ_FAILURE)
                .string("mock message")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .int_(1)
                .byte_(0),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.READ_FAILURE);
    assertThat(error).isInstanceOf(ReadFailure.class);
    ReadFailure readFailure = (ReadFailure) error;
    assertThat(readFailure.message).isEqualTo("mock message");
    assertThat(readFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(readFailure.received).isEqualTo(2);
    assertThat(readFailure.blockFor).isEqualTo(3);
    assertThat(readFailure.numFailures).isEqualTo(1);
    assertThat(readFailure.dataPresent).isFalse();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_write_timeout(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.WRITE_TIMEOUT)
                .string("mock message")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .string(ProtocolConstants.WriteType.SIMPLE),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.WRITE_TIMEOUT);
    assertThat(error).isInstanceOf(WriteTimeout.class);
    WriteTimeout writeTimeout = (WriteTimeout) error;
    Assertions.assertThat(writeTimeout.message).isEqualTo("mock message");
    assertThat(writeTimeout.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(writeTimeout.received).isEqualTo(2);
    assertThat(writeTimeout.blockFor).isEqualTo(3);
    assertThat(writeTimeout.writeType).isEqualTo(ProtocolConstants.WriteType.SIMPLE);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_write_failure(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.WRITE_FAILURE)
                .string("mock message")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .int_(2)
                .int_(3)
                .int_(1)
                .string(ProtocolConstants.WriteType.SIMPLE),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.WRITE_FAILURE);
    assertThat(error).isInstanceOf(WriteFailure.class);
    WriteFailure writeFailure = (WriteFailure) error;
    Assertions.assertThat(writeFailure.message).isEqualTo("mock message");
    assertThat(writeFailure.consistencyLevel).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(writeFailure.received).isEqualTo(2);
    assertThat(writeFailure.blockFor).isEqualTo(3);
    assertThat(writeFailure.numFailures).isEqualTo(1);
    assertThat(writeFailure.writeType).isEqualTo(ProtocolConstants.WriteType.SIMPLE);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_function_failure(int protocolVersion) {
    Error error =
        decode(
            new MockBinaryString()
                .int_(ProtocolConstants.ErrorCode.FUNCTION_FAILURE)
                .string("mock message")
                .string("keyspace")
                .string("function")
                .string("arg types"),
            protocolVersion);
    assertThat(error.code).isEqualTo(ProtocolConstants.ErrorCode.FUNCTION_FAILURE);
    assertThat(error).isInstanceOf(FunctionFailure.class);
    FunctionFailure functionFailure = (FunctionFailure) error;
    Assertions.assertThat(functionFailure.message).isEqualTo("mock message");
    assertThat(functionFailure.keyspace).isEqualTo("keyspace");
    assertThat(functionFailure.function).isEqualTo("function");
    assertThat(functionFailure.argTypes).isEqualTo("arg types");
  }
}
