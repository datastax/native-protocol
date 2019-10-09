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
package com.datastax.dse.protocol.internal.request.query;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.util.Flags;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Note: this is only used for messages that are built from within the DSE-specific driver code,
 * such as continuous paging queries. Messages that come from the shared OSS core (such as a regular
 * CQL query) still use a normal {@link QueryOptions}.
 */
public class DseQueryOptions extends QueryOptions {

  public final boolean isPageSizeInBytes;
  public final ContinuousPagingOptions continuousPagingOptions;

  public DseQueryOptions(
      int consistency,
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace,
      boolean isPageSizeInBytes,
      ContinuousPagingOptions continuousPagingOptions) {
    this(
        computeFlags(
            positionalValues,
            namedValues,
            skipMetadata,
            pageSize,
            pagingState,
            serialConsistency,
            defaultTimestamp,
            keyspace,
            isPageSizeInBytes,
            continuousPagingOptions),
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp,
        keyspace,
        isPageSizeInBytes,
        continuousPagingOptions);
  }

  protected static int computeFlags(
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace,
      boolean isPageSizeInBytes,
      ContinuousPagingOptions continuousPagingOptions) {
    int flags =
        QueryOptions.computeFlags(
            positionalValues,
            namedValues,
            skipMetadata,
            pageSize,
            pagingState,
            serialConsistency,
            defaultTimestamp,
            keyspace);
    if (isPageSizeInBytes) {
      flags = Flags.add(flags, DseProtocolConstants.QueryFlag.PAGE_SIZE_BYTES);
    }
    if (continuousPagingOptions != null) {
      flags = Flags.add(flags, DseProtocolConstants.QueryFlag.CONTINUOUS_PAGING);
    }
    return flags;
  }

  public DseQueryOptions(
      int flags,
      int consistency,
      List<ByteBuffer> positionalValues,
      Map<String, ByteBuffer> namedValues,
      boolean skipMetadata,
      int pageSize,
      ByteBuffer pagingState,
      int serialConsistency,
      long defaultTimestamp,
      String keyspace,
      boolean isPageSizeInBytes,
      ContinuousPagingOptions continuousPagingOptions) {
    super(
        flags,
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp,
        keyspace);
    this.isPageSizeInBytes = isPageSizeInBytes;
    this.continuousPagingOptions = continuousPagingOptions;
  }
}
