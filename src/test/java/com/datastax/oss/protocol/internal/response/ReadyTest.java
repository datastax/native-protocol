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

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

@RunWith(DataProviderRunner.class)
public class ReadyTest extends MessageTestBase<Ready> {
  public ReadyTest() {
    super(Ready.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Ready.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = TestDataProviders.class, value = "protocolV3OrAbove")
  public void should_encode_and_decode(int protocolVersion) {
    Ready initial = new Ready();

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded).isEqualTo(new MockBinaryString());
    assertThat(encodedSize(initial, protocolVersion)).isEqualTo(0);

    Ready decoded = decode(encoded, protocolVersion);

    assertThat(decoded).isInstanceOf(Ready.class);
  }
}
