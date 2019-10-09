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

public class ContinuousPagingOptions {

  public final int maxPages;
  public final int pagesPerSecond;
  public final int nextPages;

  public ContinuousPagingOptions(int maxPages, int pagesPerSecond, int nextPages) {
    this.maxPages = maxPages;
    this.pagesPerSecond = pagesPerSecond;
    this.nextPages = nextPages;
  }
}
