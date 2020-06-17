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
package com.datastax.oss.protocol.internal;

/** @param <B> the binary representation to compress. */
public interface Compressor<B> {
  static <B> Compressor<B> none() {
    return new NoopCompressor<>();
  }

  /**
   * The name of the algorithm used.
   *
   * <p>It's the string that will be used in the {@code STARTUP} message. Null or empty means no
   * compression.
   */
  String algorithm();

  /**
   * Compresses a payload using the "legacy" format of protocol v4- frame bodies.
   *
   * <p>The resulting payload encodes the uncompressed length, and is therefore self-sufficient for
   * decompression.
   */
  B compress(B uncompressed);

  /** Decompresses a payload that was compressed with {@link #compress(Object)}. */
  B decompress(B compressed);

  /**
   * Compresses a payload using the "modern" format of protocol v5+ segments.
   *
   * <p>The resulting payload does not encode the uncompressed length. It must be stored separately,
   * and provided to the decompression method.
   */
  B compressWithoutLength(B uncompressed);

  /** Decompresses a payload that was compressed with {@link #compressWithoutLength(Object)}. */
  B decompressWithoutLength(B compressed, int uncompressedLength);
}
