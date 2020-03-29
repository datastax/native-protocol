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

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class QueryOptionsBuilderBase<
    ResultT extends QueryOptions, SelfT extends QueryOptionsBuilderBase> {

  protected int consistency = ProtocolConstants.ConsistencyLevel.ONE;
  protected List<ByteBuffer> positionalValues = new ArrayList<>();
  protected Map<String, ByteBuffer> namedValues = new HashMap<>();
  protected boolean skipMetadata = false;
  protected int pageSize = -1;
  protected ByteBuffer pagingState = null;
  protected int serialConsistency = ProtocolConstants.ConsistencyLevel.SERIAL;
  protected long defaultTimestamp = Long.MIN_VALUE;
  protected String keyspace = null;
  protected int nowInSeconds = Integer.MIN_VALUE;

  @SuppressWarnings("unchecked")
  private SelfT self() {
    return ((SelfT) this);
  }

  public SelfT withConsistencyLevel(int consistency) {
    this.consistency = consistency;
    return self();
  }

  public SelfT withSkipMetadata() {
    this.skipMetadata = true;
    return self();
  }

  public SelfT withPagingState(String hexString) {
    pagingState = Bytes.fromHexString(hexString);
    return self();
  }

  public SelfT withPageSize(int pageSize) {
    this.pageSize = pageSize;
    return self();
  }

  public SelfT withSerialConsistency(int serialConsistency) {
    this.serialConsistency = serialConsistency;
    return self();
  }

  public SelfT withDefaultTimestamp(long defaultTimestamp) {
    this.defaultTimestamp = defaultTimestamp;
    return self();
  }

  public SelfT withKeyspace(String keyspace) {
    this.keyspace = keyspace;
    return self();
  }

  public SelfT withNowInSeconds(int nowInSeconds) {
    this.nowInSeconds = nowInSeconds;
    return self();
  }

  public SelfT withPositionalValue(String hexString) {
    positionalValues.add(Bytes.fromHexString(hexString));
    return self();
  }

  public SelfT withNamedValue(String name, String hexString) {
    namedValues.put(name, Bytes.fromHexString(hexString));
    return self();
  }

  public abstract ResultT build();
}
