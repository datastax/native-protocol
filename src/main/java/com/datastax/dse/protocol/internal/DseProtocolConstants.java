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
package com.datastax.dse.protocol.internal;

import com.datastax.oss.protocol.internal.ProtocolConstants;

/** See {@link ProtocolConstants}, this only lists the additional DSE ones. */
public class DseProtocolConstants {

  public static class Version {
    public static final int DSE_V1 = 65;
    public static final int DSE_V2 = 66;
    public static final int MIN = DSE_V1;
    public static final int MAX = DSE_V2;
    /** If no beta version is currently supported, this will be negative. */
    public static final int BETA = -1;
  }

  public static class Opcode {
    public static final int REVISE_REQUEST = 0xff;
  }

  public static class QueryFlag {
    public static final int PAGE_SIZE_BYTES = 0x40000000;
    public static final int CONTINUOUS_PAGING = 0x80000000;
  }

  public static class RowsFlag {
    public static final int CONTINUOUS_PAGING = 0x40000000;
    public static final int LAST_CONTINUOUS_PAGE = 0x80000000;
  }

  public static class RevisionType {
    public static final int CANCEL_CONTINUOUS_PAGING = 0x00000001;
    public static final int MORE_CONTINUOUS_PAGES = 0x00000002;
  }

  public static class ErrorCode {
    public static final int CLIENT_WRITE_FAILURE = 0x8000;
  }
}
