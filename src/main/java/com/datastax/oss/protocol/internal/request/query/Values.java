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
package com.datastax.oss.protocol.internal.request.query;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Values {

  public static <B> void writePositionalValues(
      List<ByteBuffer> values, B dest, PrimitiveCodec<B> encoder) {
    encoder.writeUnsignedShort(values.size(), dest);
    for (ByteBuffer value : values) {
      writeValue(value, dest, encoder);
    }
  }

  public static int sizeOfPositionalValues(List<ByteBuffer> values) {
    int size = PrimitiveSizes.SHORT;
    for (ByteBuffer value : values) {
      size += sizeOfValue(value);
    }
    return size;
  }

  public static <B> void writeNamedValues(
      Map<String, ByteBuffer> values, B dest, PrimitiveCodec<B> encoder) {
    encoder.writeUnsignedShort(values.size(), dest);
    for (Map.Entry<String, ByteBuffer> entry : values.entrySet()) {
      encoder.writeString(entry.getKey(), dest);
      writeValue(entry.getValue(), dest, encoder);
    }
  }

  public static int sizeOfNamedValues(Map<String, ByteBuffer> values) {
    int size = PrimitiveSizes.SHORT;
    for (Map.Entry<String, ByteBuffer> entry : values.entrySet()) {
      size += PrimitiveSizes.sizeOfString(entry.getKey());
      size += sizeOfValue(entry.getValue());
    }
    return size;
  }

  @SuppressWarnings("ReferenceEquality")
  private static <B> void writeValue(ByteBuffer value, B dest, PrimitiveCodec<B> encoder) {
    if (value == null) {
      encoder.writeInt(-1, dest);
    } else if (value == ProtocolConstants.UNSET_VALUE) {
      encoder.writeInt(-2, dest);
    } else {
      encoder.writeBytes(value, dest);
    }
  }

  @SuppressWarnings("ReferenceEquality")
  private static int sizeOfValue(ByteBuffer value) {
    return (value == null || value == ProtocolConstants.UNSET_VALUE)
        ? PrimitiveSizes.INT
        : PrimitiveSizes.sizeOfBytes(value);
  }

  public static <B> List<ByteBuffer> readPositionalValues(B source, PrimitiveCodec<B> decoder) {
    int size = decoder.readUnsignedShort(source);
    if (size == 0) {
      return Collections.emptyList();
    } else {
      NullAllowingImmutableList.Builder<ByteBuffer> values =
          NullAllowingImmutableList.builder(size);
      for (int i = 0; i < size; i++) {
        values.add(readValue(source, decoder));
      }
      return values.build();
    }
  }

  public static <B> Map<String, ByteBuffer> readNamedValues(B source, PrimitiveCodec<B> decoder) {
    int size = decoder.readUnsignedShort(source);
    if (size == 0) {
      return Collections.emptyMap();
    } else {
      NullAllowingImmutableMap.Builder<String, ByteBuffer> values =
          NullAllowingImmutableMap.builder(size);
      for (int i = 0; i < size; i++) {
        String key = decoder.readString(source);
        ByteBuffer value = readValue(source, decoder);
        values.put(key, value);
      }
      return values.build();
    }
  }

  public static <B> ByteBuffer readValue(B source, PrimitiveCodec<B> decoder) {
    return decoder.readBytes(source);
  }
}
