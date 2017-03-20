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
package com.datastax.cassandra.protocol.internal.binary;

import com.datastax.cassandra.protocol.internal.Compressor;

import static org.assertj.core.api.Assertions.assertThat;

public class MockCompressor implements Compressor<MockBinaryString> {

  public static final String START = "start compression";
  public static final String END = "end compression";

  @Override
  public MockBinaryString compress(MockBinaryString uncompressed) {
    return new MockBinaryString().string(START).append(uncompressed).string(END);
  }

  @Override
  public MockBinaryString decompress(MockBinaryString compressed) {
    MockPrimitiveCodec decoder = MockPrimitiveCodec.INSTANCE;

    assertThat(decoder.readString(compressed)).isEqualTo(START);

    MockBinaryString.Element element = compressed.pollLast();
    assertThat(element.type).isEqualTo(MockBinaryString.Element.Type.STRING);
    assertThat(element.value).isEqualTo(END);

    return compressed;
  }
}
