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
package com.datastax.oss.protocol.internal;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/** Computes the sizes of the protocol's primitive types. */
public class PrimitiveSizes {
  public static final int BYTE = 1;
  public static final int SHORT = 2;
  public static final int INT = 4;
  public static final int LONG = 8;
  public static final int UUID = 16;

  private PrimitiveSizes() {}

  public static int sizeOfString(String str) {
    return SHORT + encodedUTF8Length(str);
  }

  public static int sizeOfLongString(String s) {
    return INT + encodedUTF8Length(s);
  }

  public static int sizeOfStringList(List<String> l) {
    int size = SHORT;
    for (String str : l) {
      size += sizeOfString(str);
    }
    return size;
  }

  public static int sizeOfBytes(byte[] bytes) {
    return INT + (bytes == null ? 0 : bytes.length);
  }

  public static int sizeOfBytes(ByteBuffer bytes) {
    return INT + (bytes == null ? 0 : bytes.remaining());
  }

  public static int sizeOfShortBytes(byte[] bytes) {
    return SHORT + bytes.length;
  }

  public static int sizeOfShortBytes(ByteBuffer bytes) {
    return SHORT + (bytes == null ? 0 : bytes.remaining());
  }

  public static int sizeOfStringMap(Map<String, String> m) {
    int size = SHORT; // length
    for (Map.Entry<String, String> entry : m.entrySet()) {
      size += sizeOfString(entry.getKey());
      size += sizeOfString(entry.getValue());
    }
    return size;
  }

  public static int sizeOfStringMultimap(Map<String, List<String>> m) {
    int size = SHORT; // length
    for (Map.Entry<String, List<String>> entry : m.entrySet()) {
      size += sizeOfString(entry.getKey());
      size += sizeOfStringList(entry.getValue());
    }
    return size;
  }

  public static int sizeOfBytesMap(Map<String, ByteBuffer> m) {
    int size = SHORT;
    for (Map.Entry<String, ByteBuffer> entry : m.entrySet()) {
      size += sizeOfString(entry.getKey());
      size += sizeOfBytes(entry.getValue());
    }
    return size;
  }

  // Visible for testing
  static int encodedUTF8Length(String st) {
    int length = 0;
    for (int i = 0; i < st.length(); i++) {
      char c = st.charAt(i);
      if (Character.isHighSurrogate(c)) {
        if (i < st.length() - 1) {
          char c1 = st.charAt(i + 1);
          if (Character.isLowSurrogate(c1)) {
            // correct surrogate pair: 4 bytes
            length += 4;
            i++;
            continue;
          }
        }
        // wrong high surrogate, not followed by a low surrogate
        length += 1;
      } else if (Character.isLowSurrogate(c)) {
        // wrong low surrogate, not preceded by a high surrogate
        length += 1;
      } else {
        if (c <= 0x7f) {
          length += 1;
        } else if (c <= 0x7ff) {
          length += 2;
        } else {
          length += 3;
        }
      }
    }
    return length;
  }

  public static int sizeOfInet(InetSocketAddress address) {
    return sizeOfInetAddr(address.getAddress()) + INT; // port
  }

  public static int sizeOfInetAddr(InetAddress address) {
    byte[] raw = address.getAddress();
    return BYTE // number of bytes in address
        + raw.length; // bytes of address
  }
}
