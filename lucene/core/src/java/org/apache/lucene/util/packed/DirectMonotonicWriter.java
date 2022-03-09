/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.util.packed;



import java.io.IOException;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;

/**
 * Write monotonically-increasing sequences of integers. This writer splits
 * data into blocks and then for each block, computes the average slope, the
 * minimum value and only encode the delta from the expected value using a
 * {@link DirectWriter}.
 * 
 * @see DirectMonotonicReader
 * @lucene.internal 
 */ // 可以参考http://www.nosqlnotes.com/technotes/searchengine/lucene-docvalues/
public final class DirectMonotonicWriter {
 // 使用DirectMonotonicWriter压缩的数据必须是单调递增的
  public static final int MIN_BLOCK_SHIFT = 2;
  public static final int MAX_BLOCK_SHIFT = 22;

  final IndexOutput meta; // dvm文件
  final IndexOutput data;   // 仅仅new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
  final long numValues;
  final long baseDataPointer;
  final long[] buffer; // 最大存放1<<10个元素。（默认就是当前刷新时，一个block的最大文档数，最大为2^10(StoredField刷新时)）
  int bufferSize;  // 缓存的数据个数，就是buffer里面的有效个数。通过改动bufferSize可以清空buffer,最大1024个元素，一压酥
  long count;
  boolean finished;

  DirectMonotonicWriter(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    if (blockShift < MIN_BLOCK_SHIFT || blockShift > MAX_BLOCK_SHIFT) {
      throw new IllegalArgumentException("blockShift must be in [" + MIN_BLOCK_SHIFT + "-" + MAX_BLOCK_SHIFT + "], got " + blockShift);
    }
    if (numValues < 0) { // chunk的个数
      throw new IllegalArgumentException("numValues can't be negative, got " + numValues);
    }
    final long numBlocks = numValues == 0 ? 0 : ((numValues - 1) >>> blockShift) + 1; // 多少个block
    if (numBlocks > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("blockShift is too low for the provided number of values: blockShift=" + blockShift +
          ", numValues=" + numValues + ", MAX_ARRAY_LENGTH=" + ArrayUtil.MAX_ARRAY_LENGTH);
    }
    this.meta = metaOut;
    this.data = dataOut;
    this.numValues = numValues;
    final int blockSize = 1 << blockShift; // 一个block最多可以包含的chunk个数
    this.buffer = new long[(int) Math.min(numValues, blockSize)]; // 缓存最大1024个数据
    this.bufferSize = 0;
    this.baseDataPointer = dataOut.getFilePointer();
  }
  // 是对文档Id编号进行了特殊处理，才有最大值，最小值，平均值。没124个chunk，刷新成一个block
  private void flush() throws IOException { // 在storedField写入时，buffer里面每个元素，都是一个chunk
    assert bufferSize != 0;

    final float avgInc = (float) ((double) (buffer[bufferSize-1] - buffer[0]) / Math.max(1, bufferSize - 1));// 数据的平均值
    for (int i = 0; i < bufferSize; ++i) {
      final long expected = (long) (avgInc * (long) i);
      buffer[i] -= expected;//每个值和期望值的差距
    }

    long min = buffer[0];
    for (int i = 1; i < bufferSize; ++i) {
      min = Math.min(buffer[i], min);
    }

    long maxDelta = 0; // 可能的最大值
    for (int i = 0; i < bufferSize; ++i) {
      buffer[i] -= min;//比最小值大多少
      // use | will change nothing when it comes to computing required bits
      // but has the benefit of working fine with negative values too
      // (in case of overflow)
      maxDelta |= buffer[i];
    }

    meta.writeLong(min); //第一步，先存储chunk个数与理论平均值差距的最小值
    meta.writeInt(Float.floatToIntBits(avgInc)); // 第二步，存储理论平均值
    meta.writeLong(data.getFilePointer() - baseDataPointer); // 在fdx中的起始位置
    if (maxDelta == 0) {
      meta.writeByte((byte) 0);
    } else {
      final int bitsRequired = DirectWriter.unsignedBitsRequired(maxDelta); // 需要几位
      DirectWriter writer = DirectWriter.getInstance(data, bufferSize, bitsRequired);// 
      for (int i = 0; i < bufferSize; ++i) {
        writer.add(buffer[i]); // 每个chunk与理论平均值之间的差距
      }
      writer.finish();// 数据写入data中
      meta.writeByte((byte) bitsRequired); // 写入需要的byte个数
    }
    bufferSize = 0;
  }

  long previous = Long.MIN_VALUE;

  /** Write a new value. Note that data might not make it to storage until
   * {@link #finish()} is called.
   *  @throws IllegalArgumentException if values don't come in order */
  public void add(long v) throws IOException {
    if (v < previous) {
      throw new IllegalArgumentException("Values do not come in order: " + previous + ", " + v);
    } //
    if (bufferSize == buffer.length) { // 一个buffer满了,最多一个buffer包含1024个chunk
      flush();
    }
    buffer[bufferSize++] = v;
    previous = v;
    count++;
  }

  /** This must be called exactly once after all values have been {@link #add(long) added}. */
  public void finish() throws IOException {
    if (count != numValues) {
      throw new IllegalStateException("Wrong number of values added, expected: " + numValues + ", got: " + count);
    }
    if (finished) {
      throw new IllegalStateException("#finish has been called already");
    }
    if (bufferSize > 0) { // 只要有缓存数据，就得刷到文件中
      flush();
    }
    finished = true; // 刷新完了
  }

  /** Returns an instance suitable for encoding {@code numValues} into monotonic
   *  blocks of 2<sup>{@code blockShift}</sup> values. Metadata will be written
   *  to {@code metaOut} and actual data to {@code dataOut}. */
  public static DirectMonotonicWriter getInstance(IndexOutput metaOut, IndexOutput dataOut, long numValues, int blockShift) {
    return new DirectMonotonicWriter(metaOut, dataOut, numValues, blockShift);
  }

}
