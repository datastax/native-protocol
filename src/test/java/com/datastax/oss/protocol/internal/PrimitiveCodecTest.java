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

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.testng.annotations.Test;

import static com.datastax.oss.protocol.internal.Assertions.assertThat;

public class PrimitiveCodecTest {
  static final Random RANDOM = new Random();

  @Test
  public void should_read_uuid() {
    long msb = RANDOM.nextLong();
    long lsb = RANDOM.nextLong();

    UUID uuid = MockPrimitiveCodec.INSTANCE.readUuid(new MockBinaryString().long_(msb).long_(lsb));

    assertThat(uuid.getMostSignificantBits()).isEqualTo(msb);
    assertThat(uuid.getLeastSignificantBits()).isEqualTo(lsb);
  }

  @Test
  public void should_read_string_list() {
    List<String> strings =
        MockPrimitiveCodec.INSTANCE.readStringList(
            new MockBinaryString().unsignedShort(3).string("foo").string("bar").string("baz"));
    assertThat(strings).containsExactly("foo", "bar", "baz");
  }

  @Test
  public void should_read_string_map() {
    Map<String, String> map =
        MockPrimitiveCodec.INSTANCE.readStringMap(
            new MockBinaryString()
                .unsignedShort(3)
                .string("key1")
                .string("value1")
                .string("key2")
                .string("value2")
                .string("key3")
                .string("value3"));

    assertThat(map)
        .containsOnlyKeys("key1", "key2", "key3")
        .containsEntry("key1", "value1")
        .containsEntry("key2", "value2")
        .containsEntry("key3", "value3");
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

    assertThat(map).containsOnlyKeys("key1", "key2", "key3");
    assertThat(map.get("key1")).containsExactly("value11", "value12");
    assertThat(map.get("key2")).containsExactly("value21");
    assertThat(map.get("key3")).containsExactly("value31", "value32");
  }

  @Test
  public void should_read_bytes_map() {
    Map<String, ByteBuffer> map =
        MockPrimitiveCodec.INSTANCE.readBytesMap(
            new MockBinaryString().unsignedShort(1).string("key").bytes("0xcafebabe"));
    assertThat(map).containsOnlyKeys("key").containsEntry("key", Bytes.fromHexString("0xcafebabe"));
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
  public void should_write_string_multimap() {
    Map<String, List<String>> m = new LinkedHashMap<>();
    m.put("foo", Arrays.asList("1", "2", "3"));
    m.put("bar", Arrays.asList("4", "5", "6"));

    MockBinaryString dest = new MockBinaryString();
    MockPrimitiveCodec.INSTANCE.writeStringMultimap(m, dest);

    assertThat(dest)
        .isEqualTo(
            new MockBinaryString()
                .unsignedShort(2)
                .string("foo")
                .unsignedShort(3)
                .string("1")
                .string("2")
                .string("3")
                .string("bar")
                .unsignedShort(3)
                .string("4")
                .string("5")
                .string("6"));
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
