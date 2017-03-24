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

import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A data type as returned in protocol responses.
 *
 * <p>This class aims to encode protocol-level information in the simplest way possible. Any extra
 * features should be handled in the upper layers of the driver.
 */
public abstract class RawType {

  public static <B> RawType decode(B source, PrimitiveCodec<B> decoder, int protocolVersion) {
    int id = decoder.readUnsignedShort(source);
    switch (id) {
      case ProtocolConstants.DataType.CUSTOM:
        String className = decoder.readString(source);
        return new RawCustom(className);
      case ProtocolConstants.DataType.LIST:
        return new RawList(decode(source, decoder, protocolVersion));
      case ProtocolConstants.DataType.SET:
        return new RawSet(decode(source, decoder, protocolVersion));
      case ProtocolConstants.DataType.MAP:
        RawType key = decode(source, decoder, protocolVersion);
        RawType value = decode(source, decoder, protocolVersion);
        return new RawMap(key, value);
      case ProtocolConstants.DataType.UDT:
        String keyspace = decoder.readString(source);
        String typeName = decoder.readString(source);
        int nFields = decoder.readUnsignedShort(source);
        Map<String, RawType> fields = new HashMap<>(nFields * 2);
        for (int i = 0; i < nFields; i++) {
          String fieldName = decoder.readString(source);
          RawType fieldType = decode(source, decoder, protocolVersion);
          fields.put(fieldName, fieldType);
        }
        return new RawUdt(keyspace, typeName, Collections.unmodifiableMap(fields));
      case ProtocolConstants.DataType.TUPLE:
        nFields = decoder.readUnsignedShort(source);
        List<RawType> fieldTypes = new ArrayList<>(nFields);
        for (int i = 0; i < nFields; i++) {
          fieldTypes.add(decode(source, decoder, protocolVersion));
        }
        return new RawTuple(Collections.unmodifiableList(fieldTypes));
      default:
        RawType type = PRIMITIVES.get(id);
        if (type == null) {
          throw new IllegalArgumentException("Unknown type id: " + id);
        }
        return type;
    }
  }

  /** @see ProtocolConstants.DataType */
  public final int id;

  protected RawType(int id) {
    this.id = id;
  }

  public abstract <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion);

  public abstract int encodedSize(int protocolVersion);

  public static class RawPrimitive extends RawType {
    private RawPrimitive(int id) {
      super(id);
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
    }

    @Override
    public int encodedSize(int protocolVersion) {
      return 2;
    }
  }

  public static class RawCustom extends RawType {
    public final String className;

    private RawCustom(String className) {
      super(ProtocolConstants.DataType.CUSTOM);
      this.className = className;
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
      encoder.writeString(className, dest);
    }

    @Override
    public int encodedSize(int protocolVersion) {
      return 2 + PrimitiveSizes.sizeOfString(className);
    }
  }

  public static class RawList extends RawType {
    public final RawType elementType;

    private RawList(RawType elementType) {
      super(ProtocolConstants.DataType.LIST);
      this.elementType = elementType;
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
      elementType.encode(dest, encoder, protocolVersion);
    }

    @Override
    public int encodedSize(int protocolVersion) {
      return 2 + elementType.encodedSize(protocolVersion);
    }
  }

  public static class RawSet extends RawType {
    public final RawType elementType;

    private RawSet(RawType elementType) {
      super(ProtocolConstants.DataType.SET);
      this.elementType = elementType;
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
      elementType.encode(dest, encoder, protocolVersion);
    }

    @Override
    public int encodedSize(int protocolVersion) {
      return 2 + elementType.encodedSize(protocolVersion);
    }
  }

  public static class RawMap extends RawType {
    public final RawType keyType;
    public final RawType valueType;

    public RawMap(RawType keyType, RawType valueType) {
      super(ProtocolConstants.DataType.MAP);
      this.keyType = keyType;
      this.valueType = valueType;
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
      keyType.encode(dest, encoder, protocolVersion);
      valueType.encode(dest, encoder, protocolVersion);
    }

    @Override
    public int encodedSize(int protocolVersion) {
      return 2 + keyType.encodedSize(protocolVersion) + valueType.encodedSize(protocolVersion);
    }
  }

  public static class RawUdt extends RawType {
    public final String keyspace;
    public final String typeName;
    public final Map<String, RawType> fields;

    public RawUdt(String keyspace, String typeName, Map<String, RawType> fields) {
      super(ProtocolConstants.DataType.UDT);
      this.keyspace = keyspace;
      this.typeName = typeName;
      this.fields = fields;
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
      encoder.writeString(keyspace, dest);
      encoder.writeString(typeName, dest);
      encoder.writeUnsignedShort(fields.size(), dest);
      for (Map.Entry<String, RawType> entry : fields.entrySet()) {
        encoder.writeString(entry.getKey(), dest);
        entry.getValue().encode(dest, encoder, protocolVersion);
      }
    }

    @Override
    public int encodedSize(int protocolVersion) {
      int size =
          2 + PrimitiveSizes.sizeOfString(keyspace) + PrimitiveSizes.sizeOfString(typeName) + 2;
      for (Map.Entry<String, RawType> entry : fields.entrySet()) {
        size += PrimitiveSizes.sizeOfString(entry.getKey());
        size += entry.getValue().encodedSize(protocolVersion);
      }
      return size;
    }
  }

  public static class RawTuple extends RawType {
    public final List<RawType> fieldTypes;

    public RawTuple(List<RawType> fieldTypes) {
      super(ProtocolConstants.DataType.TUPLE);
      this.fieldTypes = fieldTypes;
    }

    @Override
    public <B> void encode(B dest, PrimitiveCodec<B> encoder, int protocolVersion) {
      encoder.writeUnsignedShort(id, dest);
      encoder.writeUnsignedShort(fieldTypes.size(), dest);
      for (RawType fieldType : fieldTypes) {
        fieldType.encode(dest, encoder, protocolVersion);
      }
    }

    @Override
    public int encodedSize(int protocolVersion) {
      int size = 2 + 2;
      for (RawType fieldType : fieldTypes) {
        size += fieldType.encodedSize(protocolVersion);
      }
      return size;
    }
  }

  /** Visible for tests */
  public static final Map<Integer, RawType> PRIMITIVES;

  static {
    Map<Integer, RawType> tmp = new HashMap<>();
    for (int id :
        new int[] {
          ProtocolConstants.DataType.ASCII,
          ProtocolConstants.DataType.BIGINT,
          ProtocolConstants.DataType.BLOB,
          ProtocolConstants.DataType.BOOLEAN,
          ProtocolConstants.DataType.COUNTER,
          ProtocolConstants.DataType.DECIMAL,
          ProtocolConstants.DataType.DOUBLE,
          ProtocolConstants.DataType.FLOAT,
          ProtocolConstants.DataType.INET,
          ProtocolConstants.DataType.INT,
          ProtocolConstants.DataType.TIMESTAMP,
          ProtocolConstants.DataType.UUID,
          ProtocolConstants.DataType.VARCHAR,
          ProtocolConstants.DataType.VARINT,
          ProtocolConstants.DataType.TIMEUUID,
          ProtocolConstants.DataType.SMALLINT,
          ProtocolConstants.DataType.TINYINT,
          ProtocolConstants.DataType.DURATION,
          ProtocolConstants.DataType.DATE,
          ProtocolConstants.DataType.TIME
        }) {
      tmp.put(id, new RawPrimitive(id));
    }
    PRIMITIVES = Collections.unmodifiableMap(tmp);
  }
}
