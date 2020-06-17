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

import java.util.ArrayList;
import java.util.List;

/**
 * Abstracts the logic of batching a sequence of outgoing {@link Frame frames} into one or more
 * {@link Segment segments} in protocol v5 and above.
 *
 * <p>This class is not thread-safe.
 *
 * @param <B> the binary representation we're manipulating.
 * @param <StateT> additional information that comes with incoming frames, and must be propagated to
 *     outgoing segments (in the Java driver, this is the {@code ChannelPromise} that represents the
 *     outcome of the write).
 */
public abstract class SegmentBuilder<B, StateT> {

  private final PrimitiveCodec<B> primitiveCodec;
  private final FrameCodec<B> frameCodec;
  private final int maxPayloadLength;

  private List<Frame> currentPayloadFrames = new ArrayList<>();
  private List<StateT> currentPayloadStates = new ArrayList<>();
  private int currentPayloadLength;

  protected SegmentBuilder(PrimitiveCodec<B> primitiveCodec, FrameCodec<B> frameCodec) {
    this(primitiveCodec, frameCodec, Segment.MAX_PAYLOAD_LENGTH);
  }

  // Visible for testing. In production, the max length is hard-coded
  SegmentBuilder(PrimitiveCodec<B> primitiveCodec, FrameCodec<B> frameCodec, int maxPayloadLength) {
    this.primitiveCodec = primitiveCodec;
    this.frameCodec = frameCodec;
    this.maxPayloadLength = maxPayloadLength;
  }

  /**
   * When we batch multiple frames into one segment, how frame states are combined to form the
   * segment's state.
   */
  protected abstract StateT mergeStates(List<StateT> frameStates);

  /**
   * When we slice one frame into multiple segments, how the frame's state is split into the slice
   * states.
   */
  protected abstract List<StateT> splitState(StateT frameState, int sliceCount);

  /** What to do whenever a full segment is ready. */
  protected abstract void processSegment(Segment<B> segment, StateT segmentState);

  /**
   * Adds a new frame. It will be encoded into one or more segments, that will be passed to {@link
   * #processSegment(Segment, Object)} at some point in the future.
   *
   * <p>The caller <b>must</b> invoke {@link #flush()} after the last frame.
   */
  public void addFrame(Frame frame, StateT frameState) {
    int frameBodyLength = frameCodec.encodedBodySize(frame);
    int frameLength = frameCodec.encodedHeaderSize(frame) + frameBodyLength;

    if (frameLength > maxPayloadLength) {
      // Large request: split into multiple dedicated segments and process them immediately.
      B frameBuffer = primitiveCodec.allocate(frameLength);
      frameCodec.encodeInto(frame, frameBodyLength, frameBuffer);
      boolean isExactMultiple = frameLength % maxPayloadLength == 0;
      int sliceCount = (frameLength / maxPayloadLength) + (isExactMultiple ? 0 : 1);
      onLargeFrameSplit(frame, frameLength, sliceCount);
      List<StateT> sliceStates = splitState(frameState, sliceCount);
      for (int i = 0; i < sliceCount; i++) {
        int sliceLength =
            i < sliceCount - 1 || isExactMultiple
                ? maxPayloadLength
                : frameLength % maxPayloadLength;
        B slicePayload = primitiveCodec.readRetainedSlice(frameBuffer, sliceLength);
        processSegment(new Segment<>(slicePayload, false), sliceStates.get(i));
      }
      // We've retained each slice, and won't reference this buffer anymore
      primitiveCodec.release(frameBuffer);
    } else {
      // Small request: append to an existing segment, together with other messages.
      if (currentPayloadLength + frameLength > maxPayloadLength) {
        // Current segment is full, process and start a new one:
        onSegmentFull(frame, frameLength, currentPayloadLength, currentPayloadFrames.size());
        processCurrentPayload();
        resetCurrentPayload();
      }
      currentPayloadFrames.add(frame);
      currentPayloadStates.add(frameState);
      currentPayloadLength += frameLength;
      onSmallFrameAdded(frame, frameLength, currentPayloadLength, currentPayloadFrames.size());
    }
  }

  /**
   * Signals that we're done adding frames.
   *
   * <p>This must be called after adding the last frame, it will possibly trigger the generation of
   * one last segment.
   */
  public void flush() {
    if (!currentPayloadFrames.isEmpty()) {
      onLastSegmentFlushed(currentPayloadLength, currentPayloadFrames.size());
      processCurrentPayload();
      resetCurrentPayload();
    }
  }

  /**
   * Invoked whenever a large frame needs to be split into multiple segments. This is intended for
   * logs in subclasses, the default implementation is empty.
   *
   * @param frame the frame that is being split.
   * @param frameLength the length of that frame in bytes.
   * @param sliceCount the number of slices.
   */
  @SuppressWarnings("unused")
  protected void onLargeFrameSplit(Frame frame, int frameLength, int sliceCount) {
    // by default, nothing to do
  }

  /**
   * Invoked whenever the current self-contained segment for small frames is full. It's about to get
   * processed, and a new segment will be started. This is intended for logs in subclasses, the
   * default implementation is empty.
   *
   * @param frame the frame that triggered this action. Note that it will <em>not</em> be included
   *     in the current segment (since adding it would have brought the segment over its maximum
   *     length).
   * @param frameLength the length of that frame in bytes.
   * @param currentPayloadLength the length of the segment's payload in bytes.
   * @param currentFrameCount the number of frames in the segment.
   */
  @SuppressWarnings("unused")
  protected void onSegmentFull(
      Frame frame, int frameLength, int currentPayloadLength, int currentFrameCount) {
    // by default, nothing to do
  }

  /**
   * Invoked whenever a small frame was successfully added to the current self-contained segment,
   * without bringing it over its size limit. This is intended for logs in subclasses, the default
   * implementation is empty.
   *
   * @param frame the frame.
   * @param frameLength the length of that frame in bytes.
   * @param currentPayloadLength the new total length of the segment's payload, after the frame was
   *     added.
   * @param currentFrameCount the total number of frames in the payload, after the frame was added.
   */
  @SuppressWarnings("unused")
  protected void onSmallFrameAdded(
      Frame frame, int frameLength, int currentPayloadLength, int currentFrameCount) {
    // by default, nothing to do
  }

  /**
   * Invoked whenever {@link #flush()} was called and it produces one last self-contained segment.
   * This is intended for logs in subclasses, the default implementation is empty.
   *
   * @param currentPayloadLength the length of the segment's payload in bytes.
   * @param currentFrameCount the number of frames in the segment.
   */
  @SuppressWarnings("unused")
  protected void onLastSegmentFlushed(int currentPayloadLength, int currentFrameCount) {
    // by default, nothing to do
  }

  private void processCurrentPayload() {
    assert currentPayloadLength <= maxPayloadLength;
    B payload = primitiveCodec.allocate(currentPayloadLength);
    for (Frame frame : currentPayloadFrames) {
      // Note that the body size will be computed twice, we already checked it when we added the
      // frame but haven't kept it. This is a quick CPU-bound operation so it shouldn't be a
      // problem.
      frameCodec.encodeInto(frame, -1, payload);
    }
    assert primitiveCodec.sizeOf(payload) == currentPayloadLength;
    StateT state = mergeStates(currentPayloadStates);
    processSegment(new Segment<>(payload, true), state);
  }

  private void resetCurrentPayload() {
    currentPayloadFrames.clear();
    currentPayloadStates.clear();
    currentPayloadLength = 0;
  }
}
