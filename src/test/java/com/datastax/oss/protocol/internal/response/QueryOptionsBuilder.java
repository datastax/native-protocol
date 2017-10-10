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
package com.datastax.oss.protocol.internal.response;

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryOptionsBuilder {

  private int consistency = ProtocolConstants.ConsistencyLevel.ONE;
  private List<ByteBuffer> positionalValues = new ArrayList<>();
  private Map<String, ByteBuffer> namedValues = new HashMap<>();
  private boolean skipMetadata = false;
  private int pageSize = -1;
  private ByteBuffer pagingState = null;
  private int serialConsistency = ProtocolConstants.ConsistencyLevel.SERIAL;
  private long defaultTimestamp = Long.MIN_VALUE;
  private String keyspace = null;

  public QueryOptionsBuilder consistencyLevel(int consistency) {
    this.consistency = consistency;
    return this;
  }

  public QueryOptionsBuilder withSkipMetadata() {
    this.skipMetadata = true;
    return this;
  }

  public QueryOptionsBuilder withPagingState(String hexString) {
    pagingState = Bytes.fromHexString(hexString);
    return this;
  }

  public QueryOptionsBuilder withPageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  public QueryOptionsBuilder withSerialConsistency(int serialConsistency) {
    this.serialConsistency = serialConsistency;
    return this;
  }

  public QueryOptionsBuilder withDefaultTimestamp(long defaultTimestamp) {
    this.defaultTimestamp = defaultTimestamp;
    return this;
  }

  public QueryOptionsBuilder withKeyspace(String keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  public QueryOptionsBuilder positionalValue(String hexString) {
    positionalValues.add(Bytes.fromHexString(hexString));
    return this;
  }

  public QueryOptionsBuilder namedValue(String name, String hexString) {
    namedValues.put(name, Bytes.fromHexString(hexString));
    return this;
  }

  public QueryOptions build() {
    return new QueryOptions(
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp,
        keyspace);
  }
}
