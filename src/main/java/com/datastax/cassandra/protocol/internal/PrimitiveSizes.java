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

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Computes the sizes of the protocol's primitive types. */
public class PrimitiveSizes {
  private PrimitiveSizes() {}

  public static int sizeOfString(String str) {
    return 2 + encodedUTF8Length(str);
  }

  public static int sizeOfLongString(String s) {
    return 4 + encodedUTF8Length(s);
  }

  public static int sizeOfUuid(UUID uuid) {
    return 16;
  }

  public static int sizeOfStringList(List<String> l) {
    int size = 2;
    for (String str : l) {
      size += sizeOfString(str);
    }
    return size;
  }

  public static int sizeOfBytes(byte[] bytes) {
    return 4 + (bytes == null ? 0 : bytes.length);
  }

  public static int sizeOfBytes(ByteBuffer bytes) {
    return 4 + (bytes == null ? 0 : bytes.remaining());
  }

  public static int sizeOfShortBytes(byte[] bytes) {
    return 2 + bytes.length;
  }

  public static int sizeOfStringMap(Map<String, String> m) {
    int size = 2; // length
    for (Map.Entry<String, String> entry : m.entrySet()) {
      size += sizeOfString(entry.getKey());
      size += sizeOfString(entry.getValue());
    }
    return size;
  }

  public static int sizeOfStringMultimap(Map<String, List<String>> m) {
    int size = 2; // length
    for (Map.Entry<String, List<String>> entry : m.entrySet()) {
      size += sizeOfString(entry.getKey());
      size += sizeOfStringList(entry.getValue());
    }
    return size;
  }

  public static int sizeOfBytesMap(Map<String, ByteBuffer> m) {
    int size = 2;
    for (Map.Entry<String, ByteBuffer> entry : m.entrySet()) {
      size += sizeOfString(entry.getKey());
      size += sizeOfBytes(entry.getValue());
    }
    return size;
  }

  private static int encodedUTF8Length(String st) {
    int strlen = st.length();
    int utflen = 0;
    for (int i = 0; i < strlen; i++) {
      int c = st.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        utflen++;
      } else if (c > 0x07FF) {
        utflen += 3;
      } else {
        utflen += 2;
      }
    }
    return utflen;
  }

  public static int sizeOfInet(InetSocketAddress address) {
    byte[] raw = address.getAddress().getAddress(); // sic
    return 1 // number of bytes in address
        + raw.length // bytes of address
        + 4; // port
  }
}
