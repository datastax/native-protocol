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

import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class PrimitiveCodecTest {
  static final Random RANDOM = new Random();

  @Test
  public void should_read_uuid() {
    long msb = RANDOM.nextLong();
    long lsb = RANDOM.nextLong();

    UUID uuid = MockPrimitiveCodec.INSTANCE.readUuid(new MockBinaryString().long_(msb).long_(lsb));

    org.assertj.core.api.Assertions.assertThat(uuid.getMostSignificantBits()).isEqualTo(msb);
    Assertions.assertThat(uuid.getLeastSignificantBits()).isEqualTo(lsb);
  }

  @Test
  public void should_read_string_list() {
    List<String> strings =
        MockPrimitiveCodec.INSTANCE.readStringList(
            new MockBinaryString().unsignedShort(3).string("foo").string("bar").string("baz"));
    Assertions.assertThat(strings).containsExactly("foo", "bar", "baz");
  }

  @Test
  public void should_read_string_multimap() {
    Map<String, List<String>> map =
        MockPrimitiveCodec.INSTANCE.readStringMultimap(
            new MockBinaryString()
                .unsignedShort(3)
                .string("key1")
                .unsignedShort(2)
                .string("value11")
                .string("value12")
                .string("key2")
                .unsignedShort(1)
                .string("value21")
                .string("key3")
                .unsignedShort(2)
                .string("value31")
                .string("value32"));

    Assertions.assertThat(map).containsOnlyKeys("key1", "key2", "key3");
    Assertions.assertThat(map.get("key1")).containsExactly("value11", "value12");
    Assertions.assertThat(map.get("key2")).containsExactly("value21");
    Assertions.assertThat(map.get("key3")).containsExactly("value31", "value32");
  }

  @Test
  public void should_read_bytes_map() {
    Map<String, ByteBuffer> map =
        MockPrimitiveCodec.INSTANCE.readBytesMap(
            new MockBinaryString().unsignedShort(1).string("key").bytes("0xcafebabe"));
    Assertions.assertThat(map)
        .containsOnlyKeys("key")
        .containsEntry("key", Bytes.fromHexString("0xcafebabe"));
  }

  @Test
  public void should_write_uuid() {
    long msb = RANDOM.nextLong();
    long lsb = RANDOM.nextLong();

    MockBinaryString dest = new MockBinaryString();
    MockPrimitiveCodec.INSTANCE.writeUuid(new UUID(msb, lsb), dest);

    assertThat(dest).isEqualTo(new MockBinaryString().long_(msb).long_(lsb));
  }

  @Test
  public void should_write_string_list() {
    List<String> l = new ArrayList<>();
    l.add("foo");
    l.add("bar");

    MockBinaryString dest = new MockBinaryString();
    MockPrimitiveCodec.INSTANCE.writeStringList(l, dest);

    assertThat(dest).isEqualTo(new MockBinaryString().unsignedShort(2).string("foo").string("bar"));
  }

  @Test
  public void should_write_string_map() {
    Map<String, String> m = new HashMap<>();
    m.put("foo", "1");
    m.put("bar", "2");

    MockBinaryString dest = new MockBinaryString();
    MockPrimitiveCodec.INSTANCE.writeStringMap(m, dest);

    assertThat(dest)
        .isEqualTo(
            new MockBinaryString()
                .unsignedShort(2)
                .string("bar")
                .string("2")
                .string("foo")
                .string("1"));
  }

  @Test
  public void should_write_bytes_map() {
    Map<String, ByteBuffer> m = new HashMap<>();
    m.put("key", Bytes.fromHexString("0xcafebabe"));

    MockBinaryString dest = new MockBinaryString();
    MockPrimitiveCodec.INSTANCE.writeBytesMap(m, dest);

    assertThat(dest)
        .isEqualTo(new MockBinaryString().unsignedShort(1).string("key").bytes("0xcafebabe"));
  }
}
