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
package com.datastax.oss.protocol.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.List;
import org.assertj.core.api.AbstractAssert;

public class RowsAssert extends AbstractAssert<RowsAssert, Rows> {
  public RowsAssert(Rows actual) {
    super(actual, RowsAssert.class);
  }

  /** Note: this consumes the row */
  public RowsAssert hasNextRow(String... columnHexStrings) {
    ByteBuffer[] buffers = new ByteBuffer[columnHexStrings.length];
    for (int i = 0; i < columnHexStrings.length; i++)
      buffers[i] = Bytes.fromHexString(columnHexStrings[i]);

    List<ByteBuffer> row = actual.getData().poll();
    assertThat(row).containsExactly(buffers);
    return this;
  }
}
