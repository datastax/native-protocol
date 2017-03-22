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

/**
 * Errors thrown when a message to (de)serialize doesn't respect the protocol spec.
 *
 * <p>Clients are supposed to passed well-formed messages, so these are IllegalArgumentExceptions
 * indicating a programming error.
 */
public class ProtocolErrors {
  public static void check(boolean condition, String errorWhenFalse, Object... arguments) {
    if (!condition) {
      throw new IllegalArgumentException(String.format(errorWhenFalse, arguments));
    }
  }
}
