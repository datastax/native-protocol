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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptionsBuilder;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.nio.charset.Charset;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class RawBytesQueryTest extends MessageTestBase<RawBytesQuery> {

  private String queryString = "select * from system.local";

  public RawBytesQueryTest() {
    super(RawBytesQuery.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new DseQueryCodec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV1OrAbove")
  public void should_encode(int protocolVersion) {
    DseQueryOptions queryOptions = new DseQueryOptionsBuilder().build();
    byte[] bytes = queryString.getBytes(Charset.forName("UTF-8"));
    RawBytesQuery initial = new RawBytesQuery(bytes, queryOptions);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .bytes(Bytes.toHexString(bytes))
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .int_(0x00) // no flags
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo((PrimitiveSizes.INT + bytes.length) + PrimitiveSizes.SHORT + PrimitiveSizes.INT);

    // The codec always decodes as a regular Query with DseQueryOptions, so do not cover that again
  }
}
