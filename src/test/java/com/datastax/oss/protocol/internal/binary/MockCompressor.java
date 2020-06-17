/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.protocol.internal.binary;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Compressor;
import java.util.HashMap;
import java.util.Map;

public class MockCompressor implements Compressor<MockBinaryString> {

  private final Map<MockBinaryString, MockBinaryString> decompressedToCompressed = new HashMap<>();
  private final Map<MockBinaryString, MockBinaryString> compressedToDecompressed = new HashMap<>();

  /**
   * "Primes" the given decompressed<->compressed bidirectional mapping. Future attempts to compress
   * or decompress the corresponding value will return the other value.
   *
   * <p>If {@code decompressed} was already primed, this method has no effect, and it returns the
   * previously associated compressed value (that is still in effect). {@code compressed} is
   * ignored.
   *
   * <p>Otherwise, the method returns {@code compressed}.
   *
   * @throws IllegalArgumentException if {@code compressed} was already used for another mapping.
   */
  public MockBinaryString prime(MockBinaryString decompressed, MockBinaryString compressed) {
    if (decompressedToCompressed.containsKey(decompressed)) {
      return decompressedToCompressed.get(decompressed);
    } else if (compressedToDecompressed.containsKey(compressed)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is already used as the compressed form of %s",
              compressed, compressedToDecompressed.get(compressed)));
    } else {
      decompressedToCompressed.put(decompressed, compressed);
      compressedToDecompressed.put(compressed, decompressed);
      return compressed;
    }
  }

  @Override
  public String algorithm() {
    return "MOCK";
  }

  @Override
  public MockBinaryString compress(MockBinaryString uncompressed) {
    MockBinaryString compressed = decompressedToCompressed.get(uncompressed);
    if (compressed == null) {
      throw new IllegalStateException(
          String.format("Unknown uncompressed input %s, must be primed first", uncompressed));
    }
    return compressed.copy();
  }

  @Override
  public MockBinaryString decompress(MockBinaryString compressed) {
    MockBinaryString decompressed = compressedToDecompressed.get(compressed);
    if (decompressed == null) {
      throw new IllegalStateException(
          String.format("Unknown compressed input %s, must be primed first", compressed));
    }
    return decompressed.copy();
  }

  @Override
  public MockBinaryString compressWithoutLength(MockBinaryString uncompressed) {
    // The two sets of methods are used in different contexts, for tests it doesn't matter if they
    // use the same implementation
    return compress(uncompressed);
  }

  @Override
  public MockBinaryString decompressWithoutLength(
      MockBinaryString compressed, int uncompressedLength) {
    MockBinaryString uncompressed = decompress(compressed);
    assertThat(uncompressed.size()).isEqualTo(uncompressedLength);
    return uncompressed;
  }
}
