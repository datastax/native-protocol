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
import com.datastax.oss.protocol.internal.MessageTest;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.TestDataProviders;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  public void should_encode_and_decode_without_options(int protocolVersion) {
    Supported initial = new Supported(Collections.emptyMap());

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded).isEqualTo(new MockBinaryString().unsignedShort(0));
    assertThat(encodedSize(initial, protocolVersion)).isEqualTo(PrimitiveSizes.SHORT);

    Supported decoded = decode(encoded, protocolVersion);

    assertThat(decoded.options).isEmpty();
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_with_options(int protocolVersion) {
    Map<String, List<String>> options = new LinkedHashMap<>();
    options.put("option1", Arrays.asList("value11", "value12"));
    options.put("option2", Collections.singletonList("value21"));
    Supported initial = new Supported(options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .unsignedShort(2)
                .string("option1")
                .unsignedShort(2)
                .string("value11")
                .string("value12")
                .string("option2")
                .unsignedShort(1)
                .string("value21"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            PrimitiveSizes.SHORT
                + (PrimitiveSizes.SHORT + "option1".length())
                + (PrimitiveSizes.SHORT
                    + (PrimitiveSizes.SHORT + "value11".length())
                    + (PrimitiveSizes.SHORT + "value12".length()))
                + (PrimitiveSizes.SHORT + "option2".length())
                + (PrimitiveSizes.SHORT + (PrimitiveSizes.SHORT + "value21".length())));

    Supported decoded = decode(encoded, protocolVersion);

    assertThat(decoded.options).hasSize(2);
    assertThat(decoded.options.get("option1")).containsExactly("value11", "value12");
    assertThat(decoded.options.get("option2")).containsExactly("value21");
  }
}
