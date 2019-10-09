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
package com.datastax.dse.protocol.internal;

import com.datastax.oss.protocol.internal.TestDataProviders;
import com.tngtech.java.junit.dataprovider.DataProvider;
import java.util.ArrayList;
import java.util.List;

public class DseTestDataProviders {
  @DataProvider
  public static Object[][] protocolDseV1OrAbove() {
    return protocolVersions(null, null);
  }

  @DataProvider
  public static Object[][] protocolDseV2OrAbove() {
    return protocolVersions(DseProtocolConstants.Version.DSE_V2, null);
  }

  /**
   * @param min inclusive
   * @param max inclusive
   */
  private static Object[][] protocolVersions(Integer min, Integer max) {
    if (min == null) {
      min = DseProtocolConstants.Version.MIN;
    }
    if (max == null) {
      max = Math.max(DseProtocolConstants.Version.MAX, DseProtocolConstants.Version.BETA);
    }
    List<Object> l = new ArrayList<>();
    for (int i = min; i <= max; i++) {
      l.add(i);
    }
    return TestDataProviders.fromList(l);
  }
}
