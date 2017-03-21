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
package com.datastax.cassandra.protocol.internal.request.query;

import java.util.EnumSet;

public enum QueryFlag {
  VALUES(0x01),
  SKIP_METADATA(0x02),
  PAGE_SIZE(0x04),
  PAGING_STATE(0x08),
  SERIAL_CONSISTENCY(0x10),
  DEFAULT_TIMESTAMP(0x20),
  VALUE_NAMES(0x40);

  private int mask;

  QueryFlag(int mask) {
    this.mask = mask;
  }

  public static EnumSet<QueryFlag> decode(int flags, int protocolVersion) {
    EnumSet<QueryFlag> set = EnumSet.noneOf(QueryFlag.class);
    for (QueryFlag flag : QueryFlag.values()) {
      if ((flags & flag.mask) != 0) {
        set.add(flag);
      }
    }
    return set;
  }

  public static int encode(EnumSet<QueryFlag> flags, int protocolVersion) {
    int i = 0;
    for (QueryFlag flag : flags) {
      i |= flag.mask;
    }
    return i;
  }

  public static int encodedSize(int protocolVersion) {
    return 1;
  }
}
