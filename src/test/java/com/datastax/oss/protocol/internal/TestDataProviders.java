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
package com.datastax.oss.protocol.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.java.junit.dataprovider.DataProvider;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class TestDataProviders {
  @DataProvider
  public static Object[][] protocolV3OrAbove() {
    return protocolVersions(3, null);
  }

  @DataProvider
  public static Object[][] protocolV3OrBelow() {
    return protocolVersions(null, 3);
  }

  @DataProvider
  public static Object[][] protocolV4OrAbove() {
    return protocolVersions(4, null);
  }

  @DataProvider
  public static Object[][] protocolV5OrAbove() {
    return protocolVersions(5, null);
  }

  @DataProvider
  public static Object[][] protocolV3OrV4() {
    return protocolVersions(3, 4);
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
      max = Math.max(ProtocolConstants.Version.MAX, ProtocolConstants.Version.BETA);
    }
    List<Object> l = new ArrayList<>();
    for (int i = min; i <= max; i++) {
      l.add(i);
    }
    return fromList(l);
  }

  public static Object[][] fromList(List<Object> l) {
    Object[][] result = new Object[l.size()][];
    for (int i = 0; i < l.size(); i++) {
      result[i] = new Object[1];
      result[i][0] = l.get(i);
    }
    return result;
  }

  public static Object[][] fromList(Object... l) {
    Object[][] result = new Object[l.length][];
    for (int i = 0; i < l.length; i++) {
      result[i] = new Object[1];
      result[i][0] = l[i];
    }
    return result;
  }

  // example: [ [a,b], [c,d] ], [ [1], [2] ], [ [true], [false] ]
  // => [ [a,b,1,true], [a,b,1,false], [a,b,2,true], [a,b,2,false], ... ]
  public static Object[][] combine(Object[][]... providers) {
    int numberOfProviders = providers.length; // (ex: 3)

    // ex: 2 * 2 * 2 combinations
    int numberOfCombinations = 1;
    for (Object[][] provider : providers) {
      numberOfCombinations *= provider.length;
    }

    Object[][] result = new Object[numberOfCombinations][];
    // The current index in each provider (ex: [1,0,1] => [c,d,1,false])
    int[] indices = new int[numberOfProviders];

    for (int c = 0; c < numberOfCombinations; c++) {
      int combinationLength = 0;
      for (int p = 0; p < numberOfProviders; p++) {
        combinationLength += providers[p][indices[p]].length;
      }
      Object[] combination = new Object[combinationLength];
      int destPos = 0;
      for (int p = 0; p < numberOfProviders; p++) {
        Object[] src = providers[p][indices[p]];
        System.arraycopy(src, 0, combination, destPos, src.length);
        destPos += src.length;
      }
      result[c] = combination;

      // Update indices: try to increment from the right, if it overflows reset and move left
      for (int p = providers.length - 1; p >= 0; p--) {
        if (indices[p] < providers[p].length - 1) {
          // ex: [0,0,0], p = 2 => [0,0,1]
          indices[p] += 1;
          break;
        } else {
          // ex: [0,0,1], p = 2 => [0,0,0], loop to increment to [0,1,0]
          indices[p] = 0;
        }
      }
    }
    return result;
  }

  @Test
  public void should_combine_providers() {
    Object[][] provider1 = new Object[][] {new Object[] {"a", "b"}, new Object[] {"c", "d"}};
    Object[][] provider2 = fromList(1, 2);
    Object[][] provider3 = fromList(true, false);

    Object[][] combined = combine(provider1, provider2, provider3);
    assertThat(combined).hasSize(8);
  }
}
