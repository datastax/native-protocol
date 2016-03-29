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

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.DataProvider;

public class TestDataProviders {
  @DataProvider(name = "protocolV3OrAbove")
  public static Object[][] protocolV3OrAbove() {
    return protocolVersions(3, null);
  }

  @DataProvider(name = "protocolV3OrBelow")
  public static Object[][] protocolV3OrBelow() {
    return protocolVersions(null, 3);
  }

  @DataProvider(name = "protocolV4OrAbove")
  public static Object[][] protocolV4OrAbove() {
    return protocolVersions(4, null);
  }

  /**
   * @param min inclusive
   * @param max inclusive
   */
  private static Object[][] protocolVersions(Integer min, Integer max) {
    if (min == null) {
      min = ProtocolConstants.Version.MIN;
    }
    if (max == null) {
      max = ProtocolConstants.Version.MAX;
    }
    List<Object> l = new ArrayList<>();
    for (int i = min; i <= max; i++) {
      l.add(i);
    }
    return fromList(l);
  }

  private static Object[][] fromList(List<Object> l) {
    Object[][] result = new Object[l.size()][];
    for (int i = 0; i < l.size(); i++) {
      result[i] = new Object[1];
      result[i][0] = l.get(i);
    }
    return result;
  }
}
