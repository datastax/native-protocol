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
package com.datastax.dse.protocol.internal.request;

import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;

/**
 * A specialized {@code QUERY} message where the query string is represented directly as a byte
 * array.
 *
 * <p>It is used to avoid materializing a string if the incoming query is already encoded (namely,
 * in DSE graph).
 */
public class RawBytesQuery extends Message {

  public final byte[] query;
  public final DseQueryOptions options;

  public RawBytesQuery(byte[] query, DseQueryOptions options) {
    super(false, ProtocolConstants.Opcode.QUERY);
    this.query = query;
    this.options = options;
  }

  @Override
  public String toString() {
    return "QUERY (" + Bytes.toHexString(query) + ')';
  }
}
