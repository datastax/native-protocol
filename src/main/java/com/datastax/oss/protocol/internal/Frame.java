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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Frame {
  public static final Map<String, ByteBuffer> NO_PAYLOAD = Collections.emptyMap();

  public static Frame forRequest(
      int protocolVersion,
      int streamId,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      Message message) {
    return new Frame(
        protocolVersion,
        false,
        streamId,
        tracing,
        null,
        -1,
        -1,
        customPayload,
        Collections.emptyList(),
        message);
  }

  public static Frame forResponse(
      int protocolVersion,
      int streamId,
      UUID tracingId,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      Message message) {
    return new Frame(
        protocolVersion,
        false,
        streamId,
        false,
        tracingId,
        -1,
        -1,
        customPayload,
        warnings,
        message);
  }

  public final int protocolVersion;

  /**
   * Whether the frame was decoded from a payload that has the {@code USE_BETA flag}. For encoding
   * this is ignored, the codec will set the flag if {@code protocolVersion ==
   * ProtocolConstants.Version.Beta}, regardless of this field.
   */
  public final boolean beta;

  public final int streamId;
  public final UUID tracingId;

  /**
   * Whether the frame was decoded from a payload that has the {@code TRACING flag}. For encoding
   * this is ignored, the codec will set the flag if the {@code tracingId != null}, regardless of
   * this field.
   */
  public final boolean tracing;

  /**
   * The binary size of the frame in bytes (including the header).
   *
   * <p>This is exposed for information purposes, and only for instances returned by the frame
   * decoder. Otherwise, it is set to -1.
   */
  public final int size;

  /**
   * The binary size of the compressed frame (including the header).
   *
   * <p>This is exposed for information purposes, and only for instances returned by the frame
   * decoder, <b>if compression is enabled and the frame was compressed when it was received</b>.
   * Otherwise, it is set to -1.
   */
  public final int compressedSize;

  public final Map<String, ByteBuffer> customPayload;
  public final List<String> warnings;
  public final Message message;

  /**
   * This constructor is mainly intended for internal use by the frame codec. If you want to build
   * frames to pass for encoding, see {@link #forRequest(int, int, boolean, Map, Message)} or {@link
   * #forResponse(int, int, UUID, Map, List, Message)}.
   */
  public Frame(
      int protocolVersion,
      boolean beta,
      int streamId,
      boolean tracing,
      UUID tracingId,
      int size,
      int compressedSize,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      Message message) {
    ProtocolErrors.check(
        customPayload.isEmpty() || protocolVersion >= 4, "Custom payloads require protocol V4");
    this.protocolVersion = protocolVersion;
    this.beta = beta;
    this.streamId = streamId;
    this.tracingId = tracingId;
    this.tracing = tracing;
    this.size = size;
    this.compressedSize = compressedSize;
    this.customPayload = customPayload;
    this.warnings = warnings;
    this.message = message;
  }
}
