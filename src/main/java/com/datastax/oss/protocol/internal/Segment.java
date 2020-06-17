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

/**
 * A container of {@link Frame}s in protocol v5 and above. This is a new protocol construct that
 * allows checksumming and compressing multiple messages together.
 *
 * <p>{@link #payload} contains either:
 *
 * <ul>
 *   <li>a sequence of encoded {@link Frame}s, all concatenated together. In this case, {@link
 *       #isSelfContained} is true.
 *   <li>or a slice of an encoded large {@link Frame} (if that frame is longer than {@link
 *       #MAX_PAYLOAD_LENGTH}). In this case, {@link #isSelfContained} is false.
 * </ul>
 *
 * The payload is not compressed; compression is handled at a lower level when encoding or decoding
 * this object.
 *
 * <p>Naming is provisional: it's possible that this type will be renamed to "frame", and {@link
 * Frame} to something else, at some point in the future (this is an ongoing discussion on the
 * server ticket).
 */
public class Segment<B> {

  public static int MAX_PAYLOAD_LENGTH = 128 * 1024 - 1;

  public final B payload;
  public final boolean isSelfContained;

  public Segment(B payload, boolean isSelfContained) {
    this.payload = payload;
    this.isSelfContained = isSelfContained;
  }
}
