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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class PrimitiveSizesTest {

  /**
   * Check that {@link PrimitiveSizes#encodedUTF8Length} returns the correct value for every
   * possible Unicode code point.
   *
   * <p>We compare with {@link String#getBytes(Charset)}, since that's how we encode strings in the
   * driver.
   */
  @Test
  public void should_measure_size_of_encoded_codepoint() {
    // Basic Multilingual Plane
    // Note that this includes all surrogates (0xD800 => 0xDFFF), which are invalid as a single
    // character. String.getBytes encodes them as "?".
    for (char codePoint = 0; codePoint < Character.MAX_VALUE; codePoint++) {
      String s = new String(new char[] {codePoint});
      assertThat(PrimitiveSizes.encodedUTF8Length(s))
          .as(String.format("BMP %d", (int) codePoint))
          .isEqualTo(s.getBytes(StandardCharsets.UTF_8).length);
    }
    // Other planes (require a surrogate pair)
    for (int codePoint = 0x10000; codePoint < 0x10FFFF; codePoint++) {
      char highSurrogate = Character.highSurrogate(codePoint);
      char lowSurrogate = Character.lowSurrogate(codePoint);
      String s = new String(new char[] {highSurrogate, lowSurrogate});
      assertThat(PrimitiveSizes.encodedUTF8Length(s))
          .as(
              String.format(
                  "%d (high %d, low %d)", codePoint, (int) highSurrogate, (int) lowSurrogate))
          .isEqualTo(s.getBytes(StandardCharsets.UTF_8).length);
    }
  }
}
