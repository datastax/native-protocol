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

import com.datastax.cassandra.protocol.internal.response.result.ColumnSpec;
import com.datastax.cassandra.protocol.internal.response.result.RowsMetadata;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class RowsMetadataAssert extends AbstractAssert<RowsMetadataAssert, RowsMetadata> {
  public RowsMetadataAssert(RowsMetadata actual) {
    super(actual, RowsMetadataAssert.class);
  }

  public RowsMetadataAssert hasColumnCount(int expected) {
    assertThat(actual.columnCount).isEqualTo(expected);
    return this;
  }

  public RowsMetadataAssert hasPagingState(String expected) {
    assertThat(actual.pagingState).isEqualTo(Bytes.fromHexString(expected));
    return this;
  }

  public RowsMetadataAssert hasNoPagingState() {
    assertThat(actual.pagingState).isNull();
    return this;
  }

  public RowsMetadataAssert hasColumnSpecs(ColumnSpec... expected) {
    assertThat(actual.columnSpecs).containsExactly(expected);
    return this;
  }

  public RowsMetadataAssert hasNoColumnSpecs() {
    assertThat(actual.columnSpecs).isEmpty();
    return this;
  }

  public RowsMetadataAssert hasPkIndices(int... pkIndices) {
    assertThat(actual.pkIndices).containsExactly(pkIndices);
    return this;
  }

  public RowsMetadataAssert hasNoPkIndices() {
    assertThat(actual.pkIndices).isNull();
    return this;
  }
}
