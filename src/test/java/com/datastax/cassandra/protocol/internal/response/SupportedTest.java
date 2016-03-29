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
package com.datastax.cassandra.protocol.internal.response;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SupportedTest extends MessageTest<Supported> {
  protected SupportedTest() {
    super(Supported.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Supported.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_without_options(int protocolVersion) {
    Supported supported = decode(new MockBinaryString().unsignedShort(0), protocolVersion);
    assertThat(supported.options).isEmpty();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_decode_with_options(int protocolVersion) {
    Supported supported =
        decode(
            new MockBinaryString()
                .unsignedShort(2)
                .string("option1")
                .unsignedShort(2)
                .string("value11")
                .string("value12")
                .string("option2")
                .unsignedShort(1)
                .string("value21"),
            protocolVersion);
    assertThat(supported.options).containsOnlyKeys("option1", "option2");
    assertThat(supported.options.get("option1")).containsExactly("value11", "value12");
    assertThat(supported.options.get("option2")).containsExactly("value21");
  }
}
