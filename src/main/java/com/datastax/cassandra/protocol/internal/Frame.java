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
package com.datastax.cassandra.protocol.internal;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Frame {
  public static final Map<String, ByteBuffer> NO_PAYLOAD = Collections.emptyMap();

  public final int protocolVersion;
  public final int streamId;
  public final UUID tracingId;
  public final boolean tracing;
  public final Map<String, ByteBuffer> customPayload;
  public final List<String> warnings;
  public final Message message;

  public Frame(
      int protocolVersion,
      int streamId,
      UUID tracingId,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      Message message) {
    this(
        protocolVersion,
        streamId,
        tracingId,
        (tracingId != null),
        customPayload,
        warnings,
        message);
  }

  public Frame(
      int protocolVersion,
      int streamId,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      Message message) {
    this(protocolVersion, streamId, null, tracing, customPayload, Collections.emptyList(), message);
  }

  private Frame(
      int protocolVersion,
      int streamId,
      UUID tracingId,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      List<String> warnings,
      Message message) {
    ProtocolErrors.check(
        customPayload.isEmpty() || protocolVersion >= 4, "Custom payloads require protocol V4");
    this.protocolVersion = protocolVersion;
    this.streamId = streamId;
    this.tracingId = tracingId;
    this.tracing = tracing;
    this.customPayload = customPayload;
    this.warnings = warnings;
    this.message = message;
  }
}
