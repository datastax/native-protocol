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

public class NoopCompressor<B> implements Compressor<B> {

  @Override
  public String algorithm() {
    return null;
  }

  @Override
  public B compress(B uncompressed) {
    return uncompressed;
  }

  @Override
  public B decompress(B compressed) {
    return compressed;
  }

  @Override
  public B compressWithoutLength(B uncompressed) {
    return uncompressed;
  }

  @Override
  public B decompressWithoutLength(B compressed, int uncompressedLength) {
    return compressed;
  }
}
