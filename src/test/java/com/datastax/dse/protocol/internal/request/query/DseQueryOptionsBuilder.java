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

import com.datastax.oss.protocol.internal.request.query.QueryOptionsBuilderBase;

public class DseQueryOptionsBuilder
    extends QueryOptionsBuilderBase<DseQueryOptions, DseQueryOptionsBuilder> {

  protected boolean isPageSizeInBytes;
  protected boolean hasContinuousPagingOptions;
  protected int maxPages;
  protected int pagesPerSecond;
  protected int nextPages;

  public DseQueryOptionsBuilder withPageSizeInBytes() {
    this.isPageSizeInBytes = true;
    this.hasContinuousPagingOptions = true;
    return this;
  }

  public DseQueryOptionsBuilder withMaxPages(int maxPages) {
    this.maxPages = maxPages;
    this.hasContinuousPagingOptions = true;
    return this;
  }

  public DseQueryOptionsBuilder withPagesPerSecond(int pagesPerSecond) {
    this.pagesPerSecond = pagesPerSecond;
    this.hasContinuousPagingOptions = true;
    return this;
  }

  public DseQueryOptionsBuilder withNextPages(int nextPages) {
    this.nextPages = nextPages;
    return this;
  }

  @Override
  public DseQueryOptions build() {
    return new DseQueryOptions(
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
        hasContinuousPagingOptions
            ? new ContinuousPagingOptions(maxPages, pagesPerSecond, nextPages)
            : null);
  }
}
