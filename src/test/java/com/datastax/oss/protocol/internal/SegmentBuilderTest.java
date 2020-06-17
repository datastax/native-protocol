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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.binary.MockPrimitiveCodec;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptionsBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SegmentBuilderTest {

  // The constant names denote the total encoded size (frame + body)
  private static final Frame _38B_FRAME = frame(new Query("SELECT * FROM table"));
  private static final Frame _51B_FRAME = frame(new Query("SELECT * FROM table WHERE id = 1"));
  private static final Frame _1KB_FRAME =
      frame(
          new Query(
              "SELECT * FROM table WHERE id = ?",
              new QueryOptionsBuilder().withPositionalValue(_967Bytes()).build()));

  @Test
  public void should_concatenate_frames_when_under_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);
    builder.addFrame(_38B_FRAME, "a");
    builder.addFrame(_51B_FRAME, "b");
    // Nothing produced yet since we would still have room for more frames
    assertThat(builder.segments).isEmpty();

    builder.flush();
    assertThat(builder.segments).hasSize(1);
    Segment<MockBinaryString> segment = builder.segments.get(0);
    assertThat(segment.payload.size()).isEqualTo(38 + 51);
    assertThat(segment.isSelfContained).isTrue();
    String state = builder.states.get(0);
    assertThat(state).isEqualTo("ab");
  }

  @Test
  public void should_start_new_segment_when_over_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    builder.addFrame(_38B_FRAME, "a");
    builder.addFrame(_51B_FRAME, "b");
    builder.addFrame(_38B_FRAME, "c");
    // Adding the 3rd frame brings the total size over 100, so a first segment should be emitted
    // with the first two messages:
    assertThat(builder.segments).hasSize(1);

    builder.addFrame(_38B_FRAME, "d");
    builder.flush();
    assertThat(builder.segments).hasSize(2);

    Segment<MockBinaryString> segment1 = builder.segments.get(0);
    assertThat(segment1.payload.size()).isEqualTo(38 + 51);
    assertThat(segment1.isSelfContained).isTrue();
    String state1 = builder.states.get(0);
    assertThat(state1).isEqualTo("ab");
    Segment<MockBinaryString> segment2 = builder.segments.get(1);
    assertThat(segment2.payload.size()).isEqualTo(38 + 38);
    assertThat(segment2.isSelfContained).isTrue();
    String state2 = builder.states.get(1);
    assertThat(state2).isEqualTo("cd");
  }

  @Test
  public void should_start_new_segment_when_at_limit() {
    TestSegmentBuilder builder = new TestSegmentBuilder(38 + 51);

    builder.addFrame(_38B_FRAME, "a");
    builder.addFrame(_51B_FRAME, "b");
    builder.addFrame(_38B_FRAME, "c");
    assertThat(builder.segments).hasSize(1);

    builder.addFrame(_51B_FRAME, "d");
    builder.flush();
    assertThat(builder.segments).hasSize(2);

    Segment<MockBinaryString> segment1 = builder.segments.get(0);
    assertThat(segment1.payload.size()).isEqualTo(38 + 51);
    assertThat(segment1.isSelfContained).isTrue();
    String state1 = builder.states.get(0);
    assertThat(state1).isEqualTo("ab");
    Segment<MockBinaryString> segment2 = builder.segments.get(1);
    assertThat(segment2.payload.size()).isEqualTo(38 + 51);
    assertThat(segment2.isSelfContained).isTrue();
    String state2 = builder.states.get(1);
    assertThat(state2).isEqualTo("cd");
  }

  @Test
  public void should_split_large_frame() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    String frameState = "abcdefghijk";
    builder.addFrame(_1KB_FRAME, frameState);

    assertThat(builder.segments).hasSize(11);
    for (int i = 0; i < 11; i++) {
      Segment<MockBinaryString> slice = builder.segments.get(i);
      assertThat(slice.payload.size()).isEqualTo(i == 10 ? 24 : 100);
      assertThat(slice.isSelfContained).isFalse();
      String state = builder.states.get(i);
      assertThat(state).hasSize(1);
      assertThat(state.charAt(0)).isEqualTo(frameState.charAt(i));
    }
  }

  @Test
  public void should_split_large_frame_when_exact_multiple() {
    TestSegmentBuilder builder = new TestSegmentBuilder(256);

    String frameState = "abcd";
    builder.addFrame(_1KB_FRAME, frameState);

    assertThat(builder.segments).hasSize(4);
    for (int i = 0; i < 4; i++) {
      Segment<MockBinaryString> slice = builder.segments.get(i);
      assertThat(slice.payload.size()).isEqualTo(256);
      assertThat(slice.isSelfContained).isFalse();
      String state = builder.states.get(i);
      assertThat(state).hasSize(1);
      assertThat(state.charAt(0)).isEqualTo(frameState.charAt(i));
    }
  }

  @Test
  public void should_mix_small_frames_and_large_frames() {
    TestSegmentBuilder builder = new TestSegmentBuilder(100);

    builder.addFrame(_38B_FRAME, "a");
    builder.addFrame(_51B_FRAME, "b");

    // Large frame: process immediately, does not impact accumulated small frames
    String largeFrameState = "cdefghijklm";
    builder.addFrame(_1KB_FRAME, largeFrameState);
    assertThat(builder.segments).hasSize(11);

    // Another small frames bring us above the limit
    builder.addFrame(_38B_FRAME, "n");
    assertThat(builder.segments).hasSize(12);

    // One last frame and finish
    builder.addFrame(_38B_FRAME, "o");
    builder.flush();
    assertThat(builder.segments).hasSize(13);

    for (int i = 0; i < 11; i++) {
      Segment<MockBinaryString> slice = builder.segments.get(i);
      assertThat(slice.payload.size()).isEqualTo(i == 10 ? 24 : 100);
      assertThat(slice.isSelfContained).isFalse();
      String state = builder.states.get(i);
      assertThat(state).hasSize(1);
      assertThat(state.charAt(0)).isEqualTo(largeFrameState.charAt(i));
    }

    Segment<MockBinaryString> smallMessages1 = builder.segments.get(11);
    assertThat(smallMessages1.payload.size()).isEqualTo(38 + 51);
    assertThat(smallMessages1.isSelfContained).isTrue();
    assertThat(builder.states.get(11)).isEqualTo("ab");
    Segment<MockBinaryString> smallMessages2 = builder.segments.get(12);
    assertThat(smallMessages2.payload.size()).isEqualTo(38 + 38);
    assertThat(smallMessages2.isSelfContained).isTrue();
    assertThat(builder.states.get(12)).isEqualTo("no");
  }

  /**
   * Test implementation that simply stores segment in the order they were produced, and uses
   * strings to simulate the state.
   */
  static class TestSegmentBuilder extends SegmentBuilder<MockBinaryString, String> {

    List<Segment<MockBinaryString>> segments = new ArrayList<>();
    List<String> states = new ArrayList<>();

    TestSegmentBuilder(int maxPayloadLength) {
      super(
          MockPrimitiveCodec.INSTANCE,
          FrameCodec.defaultClient(MockPrimitiveCodec.INSTANCE, Compressor.none()),
          maxPayloadLength);
    }

    @Override
    protected String mergeStates(List<String> frameStates) {
      StringBuilder result = new StringBuilder();
      for (String state : frameStates) {
        result.append(state);
      }
      return result.toString();
    }

    @Override
    protected List<String> splitState(String frameState, int sliceCount) {
      if (frameState.length() != sliceCount) {
        throw new IllegalArgumentException(
            "State to be split should have exactly one character by slice");
      }
      List<String> result = new ArrayList<>();
      for (int i = 0; i < sliceCount; i++) {
        result.add(String.valueOf(frameState.charAt(i)));
      }
      return result;
    }

    @Override
    protected void processSegment(Segment<MockBinaryString> segment, String segmentState) {
      segments.add(segment);
      states.add(segmentState);
    }
  }

  private static Frame frame(Message message) {
    return Frame.forRequest(5, 0, false, Collections.emptyMap(), message);
  }

  private static String _967Bytes() {
    StringBuilder result = new StringBuilder("0x");
    for (int i = 0; i < 967; i++) {
      // The actual contents don't matter, we only care about the size
      result.append("00");
    }
    return result.toString();
  }
}
