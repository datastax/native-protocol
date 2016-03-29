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
package com.datastax.cassandra.protocol.internal;

import java.nio.ByteBuffer;

public class ProtocolConstants {

  public static final ByteBuffer UNSET_VALUE = ByteBuffer.allocate(0);

  public static class Version {
    public static final int V3 = 3;
    public static final int V4 = 4;
    public static final int MIN = V3;
    public static final int MAX = V4;
  }

  public static class Opcode {
    public static final int ERROR = 0x00;
    public static final int STARTUP = 0x01;
    public static final int READY = 0x02;
    public static final int AUTHENTICATE = 0x03;
    public static final int OPTIONS = 0x05;
    public static final int SUPPORTED = 0x06;
    public static final int QUERY = 0x07;
    public static final int RESULT = 0x08;
    public static final int PREPARE = 0x09;
    public static final int EXECUTE = 0x0A;
    public static final int REGISTER = 0x0B;
    public static final int EVENT = 0x0C;
    public static final int BATCH = 0x0D;
    public static final int AUTH_CHALLENGE = 0x0E;
    public static final int AUTH_RESPONSE = 0x0F;
    public static final int AUTH_SUCCESS = 0x10;
  }

  public static class ResponseKind {
    public static final int VOID = 0x0001;
    public static final int ROWS = 0x0002;
    public static final int SET_KEYSPACE = 0x0003;
    public static final int PREPARED = 0x0004;
    public static final int SCHEMA_CHANGE = 0x0005;
  }

  public static class ErrorCode {
    public static final int SERVER_ERROR = 0x0000;
    public static final int PROTOCOL_ERROR = 0x000A;
    public static final int AUTH_ERROR = 0x0100;
    public static final int UNAVAILABLE = 0x1000;
    public static final int OVERLOADED = 0x1001;
    public static final int IS_BOOTSTRAPPING = 0x1002;
    public static final int TRUNCATE_ERROR = 0x1003;
    public static final int WRITE_TIMEOUT = 0x1100;
    public static final int READ_TIMEOUT = 0x1200;
    public static final int READ_FAILURE = 0x1300;
    public static final int FUNCTION_FAILURE = 0x1400;
    public static final int WRITE_FAILURE = 0x1500;
    public static final int SYNTAX_ERROR = 0x2000;
    public static final int UNAUTHORIZED = 0x2100;
    public static final int INVALID = 0x2200;
    public static final int CONFIG_ERROR = 0x2300;
    public static final int ALREADY_EXISTS = 0x2400;
    public static final int UNPREPARED = 0x2500;
  }

  public static class ConsistencyLevel {
    public static final int ANY = 0x0000;
    public static final int ONE = 0x0001;
    public static final int TWO = 0x0002;
    public static final int THREE = 0x0003;
    public static final int QUORUM = 0x0004;
    public static final int ALL = 0x0005;
    public static final int LOCAL_QUORUM = 0x0006;
    public static final int EACH_QUORUM = 0x0007;
    public static final int SERIAL = 0x0008;
    public static final int LOCAL_SERIAL = 0x0009;
    public static final int LOCAL_ONE = 0x000A;
  }

  public static class WriteType {
    public static final String SIMPLE = "SIMPLE";
    public static final String BATCH = "BATCH";
    public static final String UNLOGGED_BATCH = "UNLOGGED_BATCH";
    public static final String COUNTER = "COUNTER";
    public static final String BATCH_LOG = "BATCH_LOG";
  }

  public static class DataType {
    public static final int CUSTOM = 0x0000;
    public static final int ASCII = 0x0001;
    public static final int BIGINT = 0x0002;
    public static final int BLOB = 0x0003;
    public static final int BOOLEAN = 0x0004;
    public static final int COUNTER = 0x0005;
    public static final int DECIMAL = 0x0006;
    public static final int DOUBLE = 0x0007;
    public static final int FLOAT = 0x0008;
    public static final int INT = 0x0009;
    public static final int TIMESTAMP = 0x000B;
    public static final int UUID = 0x000C;
    public static final int VARCHAR = 0x000D;
    public static final int VARINT = 0x000E;
    public static final int TIMEUUID = 0x000F;
    public static final int INET = 0x0010;
    public static final int DATE = 0x0011;
    public static final int TIME = 0x0012;
    public static final int SMALLINT = 0x0013;
    public static final int TINYINT = 0x0014;
    public static final int LIST = 0x0020;
    public static final int MAP = 0x0021;
    public static final int SET = 0x0022;
    public static final int UDT = 0x0030;
    public static final int TUPLE = 0x0031;
  }

  public static class EventType {
    public static final String TOPOLOGY_CHANGE = "TOPOLOGY_CHANGE";
    public static final String STATUS_CHANGE = "STATUS_CHANGE";
    public static final String SCHEMA_CHANGE = "SCHEMA_CHANGE";
  }

  public static class SchemaChangeType {
    public static final String CREATED = "CREATED";
    public static final String UPDATED = "UPDATED";
    public static final String DROPPED = "DROPPED";
  }

  public static class SchemaChangeTarget {
    public static final String KEYSPACE = "KEYSPACE";
    public static final String TABLE = "TABLE";
    public static final String TYPE = "TYPE";
    public static final String FUNCTION = "FUNCTION";
    public static final String AGGREGATE = "AGGREGATE";
  }

  public static class TopologyChangeType {
    public static final String NEW_NODE = "NEW_NODE";
    public static final String REMOVED_NODE = "REMOVED_NODE";
  }

  public static class StatusChangeType {
    public static final String UP = "UP";
    public static final String DOWN = "DOWN";
  }

  public static class BatchType {
    public static final byte LOGGED = 0x00;
    public static final byte UNLOGGED = 0x01;
    public static final byte COUNTER = 0x02;
  }
}
