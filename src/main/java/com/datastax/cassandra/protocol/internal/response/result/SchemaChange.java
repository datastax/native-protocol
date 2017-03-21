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
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.ProtocolErrors;
import com.datastax.cassandra.protocol.internal.response.Result;
import java.util.List;

public class SchemaChange extends Result {
  /** @see ProtocolConstants.SchemaChangeType */
  public final String changeType;
  /** @see ProtocolConstants.SchemaChangeTarget */
  public final String target;

  public final String keyspace;
  public final String object;
  public final List<String> arguments;

  public SchemaChange(
      String changeType, String target, String keyspace, String object, List<String> arguments) {
    super(ProtocolConstants.ResponseKind.SCHEMA_CHANGE);
    this.changeType = changeType;
    this.target = target;
    this.keyspace = keyspace;
    this.object = object;
    this.arguments = arguments;
  }

  public static class SubCodec extends Result.SubCodec {
    public SubCodec(int protocolVersion) {
      super(ProtocolConstants.ResponseKind.SCHEMA_CHANGE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      SchemaChange schemaChange = (SchemaChange) message;

      encoder.writeString(schemaChange.changeType, dest);
      encoder.writeString(schemaChange.target, dest);
      encoder.writeString(schemaChange.keyspace, dest);

      switch (schemaChange.target) {
        case ProtocolConstants.SchemaChangeTarget.KEYSPACE:
          break;
        case ProtocolConstants.SchemaChangeTarget.TABLE:
        case ProtocolConstants.SchemaChangeTarget.TYPE:
          encoder.writeString(schemaChange.object, dest);
          break;
        case ProtocolConstants.SchemaChangeTarget.AGGREGATE:
        case ProtocolConstants.SchemaChangeTarget.FUNCTION:
          encoder.writeString(schemaChange.object, dest);
          encoder.writeStringList(schemaChange.arguments, dest);
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown schema change target: " + schemaChange.target);
      }
    }

    @Override
    public int encodedSize(Message message) {
      SchemaChange schemaChange = (SchemaChange) message;

      ProtocolErrors.check(
          protocolVersion >= ProtocolConstants.Version.V4
              || !ProtocolConstants.SchemaChangeTarget.AGGREGATE.equals(schemaChange.target)
                  && !ProtocolConstants.SchemaChangeTarget.FUNCTION.equals(schemaChange.target),
          "%s schema change events are not supported in protocol version %d",
          schemaChange.target,
          protocolVersion);
      int size = PrimitiveSizes.sizeOfString(schemaChange.changeType);
      size += PrimitiveSizes.sizeOfString(schemaChange.target);
      size += PrimitiveSizes.sizeOfString(schemaChange.keyspace);

      switch (schemaChange.target) {
        case ProtocolConstants.SchemaChangeTarget.KEYSPACE:
          break;
        case ProtocolConstants.SchemaChangeTarget.TABLE:
        case ProtocolConstants.SchemaChangeTarget.TYPE:
          size += PrimitiveSizes.sizeOfString(schemaChange.object);
          break;
        case ProtocolConstants.SchemaChangeTarget.AGGREGATE:
        case ProtocolConstants.SchemaChangeTarget.FUNCTION:
          size += PrimitiveSizes.sizeOfString(schemaChange.object);
          size += PrimitiveSizes.sizeOfStringList(schemaChange.arguments);
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown schema change target: " + schemaChange.target);
      }
      return size;
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      String changeType = decoder.readString(source);
      String target = decoder.readString(source);
      ProtocolErrors.check(
          protocolVersion >= ProtocolConstants.Version.V4
              || !ProtocolConstants.SchemaChangeTarget.AGGREGATE.equals(target)
                  && !ProtocolConstants.SchemaChangeTarget.FUNCTION.equals(target),
          "%s schema change events are not supported in protocol version %d",
          target,
          protocolVersion);

      String keyspace = decoder.readString(source);
      String object;
      List<String> arguments;
      switch (target) {
        case ProtocolConstants.SchemaChangeTarget.KEYSPACE:
          object = null;
          arguments = null;
          break;
        case ProtocolConstants.SchemaChangeTarget.TABLE:
        case ProtocolConstants.SchemaChangeTarget.TYPE:
          object = decoder.readString(source);
          arguments = null;
          break;
        case ProtocolConstants.SchemaChangeTarget.AGGREGATE:
        case ProtocolConstants.SchemaChangeTarget.FUNCTION:
          object = decoder.readString(source);
          arguments = decoder.readStringList(source);
          break;
        default:
          throw new IllegalArgumentException("Unknown schema change target: " + target);
      }
      return new SchemaChange(changeType, target, keyspace, object, arguments);
    }
  }
}
