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
package com.datastax.cassandra.protocol.internal.response.result;

import java.util.Objects;

public class ColumnSpec {
  public final String ksName;
  public final String tableName;
  public final String name;
  public final RawType type;

  public ColumnSpec(String ksName, String tableName, String name, RawType type) {
    this.ksName = ksName;
    this.tableName = tableName;
    this.name = name;
    this.type = type;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ColumnSpec) {
      ColumnSpec that = (ColumnSpec) other;
      return Objects.equals(this.ksName, that.ksName)
          && Objects.equals(this.tableName, that.tableName)
          && Objects.equals(this.name, that.name)
          && this.type == that.type;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(ksName, tableName, name, type);
  }
}
