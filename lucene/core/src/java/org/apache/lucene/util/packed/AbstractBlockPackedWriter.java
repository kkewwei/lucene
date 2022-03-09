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


import static org.apache.lucene.util.packed.PackedInts.checkBlockSize;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataOutput;

abstract class AbstractBlockPackedWriter {

  static final int MIN_BLOCK_SIZE = 64;// 最小的block个数
  static final int MAX_BLOCK_SIZE = 1 << (30 - 3);
  static final int MIN_VALUE_EQUALS_0 = 1 << 0;
  static final int BPV_SHIFT = 1;

  // same as DataOutput.writeVLong but accepts negative values
  static void writeVLong(DataOutput out, long i) throws IOException {
    int k = 0;
    while ((i & ~0x7FL) != 0L && k++ < 8) {
      out.writeByte((byte)((i & 0x7FL) | 0x80L));
      i >>>= 7;
    }
    out.writeByte((byte) i);
  }

  protected DataOutput out; // _0.tvd   FSIndexOutput
  protected final long[] values;// 可能每个文档的域个数，
  protected byte[] blocks; //需要几个byte来存放values里面的所有数据
  protected int off; //  这一批缓存准备向文件中刷入的数据
  protected long ord;
  protected boolean finished;

  /**
   * Sole constructor.
   * @param blockSize the number of values of a single block, must be a multiple of <tt>64</tt>
   */
  public AbstractBlockPackedWriter(DataOutput out, int blockSize) {
    checkBlockSize(blockSize, MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
    reset(out);
    values = new long[blockSize]; // 默认大小64个
  }

  /** Reset this writer to wrap <code>out</code>. The block size remains unchanged. */
  public void reset(DataOutput out) {
    assert out != null;
    this.out = out;
    off = 0;
    ord = 0L;
    finished = false;
  }

  private void checkNotFinished() {
    if (finished) {
      throw new IllegalStateException("Already finished");
    }
  }

  /** Append a new long. */
  public void add(long l) throws IOException { // l可以是文档个数
    checkNotFinished();
    if (off == values.length) {
      flush();  //
    }
    values[off++] = l; // 可能每个文档的域个数， 直接是覆盖型的
    ++ord;
  }

  // For testing only
  void addBlockOfZeros() throws IOException {
    checkNotFinished();
    if (off != 0 && off != values.length) {
      throw new IllegalStateException("" + off);
    }
    if (off == values.length) {
      flush();
    }
    Arrays.fill(values, 0);
    off = values.length;
    ord += values.length;
  }

  /** Flush all buffered data to disk. This instance is not usable anymore
   *  after this method has been called until {@link #reset(DataOutput)} has
   *  been called. */
  public void finish() throws IOException {
    checkNotFinished();
    if (off > 0) {
      flush();
    }
    finished = true;
  }

  /** Return the number of values which have been added. */
  public long ord() {
    return ord;
  }

  protected abstract void flush() throws IOException;

  protected final void writeValues(int bitsRequired) throws IOException {
    final PackedInts.Encoder encoder = PackedInts.getEncoder(PackedInts.Format.PACKED, PackedInts.VERSION_CURRENT, bitsRequired); //BulkOperationPacked2
    final int iterations = values.length / encoder.byteValueCount(); // 需要几个block来存放这些数据
    final int blockSize = encoder.byteBlockCount() * iterations; // 需要几个byte来存放这些这些数据
    if (blocks == null || blocks.length < blockSize) {
      blocks = new byte[blockSize];
    }
    if (off < values.length) {
      Arrays.fill(values, off, values.length, 0L);
    }
    encoder.encode(values, 0, blocks, 0, iterations);
    final int blockCount = (int) PackedInts.Format.PACKED.byteCount(PackedInts.VERSION_CURRENT, off, bitsRequired);
    out.writeBytes(blocks, blockCount); // 放入_0.tvd中
  }

}
