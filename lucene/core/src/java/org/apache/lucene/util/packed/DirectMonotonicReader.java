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
import java.util.Arrays;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Retrieves an instance previously written by {@link DirectMonotonicWriter}.
 * @see DirectMonotonicWriter 
 */
public final class DirectMonotonicReader extends LongValues implements Accountable {

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(DirectMonotonicReader.class);

  /** An instance that always returns {@code 0}. */
  private static final LongValues EMPTY = new LongValues() {

    @Override
    public long get(long index) {
      return 0;
    }

  };

  /** In-memory metadata that needs to be kept around for
   *  {@link DirectMonotonicReader} to read data from disk. */
  public static class Meta implements Accountable {
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Meta.class);

    final int blockShift; // 10
    final int numBlocks;// 一次最对可以存1024个数字，称为一个block，看用了几个block
    final long[] mins;
    final float[] avgs;
    final byte[] bpvs; 
    final long[] offsets;

    Meta(long numValues, int blockShift) {
      this.blockShift = blockShift;
      long numBlocks = numValues >>> blockShift;
      if ((numBlocks << blockShift) < numValues) {
        numBlocks += 1;
      }
      this.numBlocks = (int) numBlocks;
      this.mins = new long[this.numBlocks];
      this.avgs = new float[this.numBlocks];
      this.bpvs = new byte[this.numBlocks];
      this.offsets = new long[this.numBlocks];
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED
          + RamUsageEstimator.sizeOf(mins)
          + RamUsageEstimator.sizeOf(avgs)
          + RamUsageEstimator.sizeOf(bpvs)
          + RamUsageEstimator.sizeOf(offsets);
    }
  }
  // 可以参考DirectMonotonicWriter，是对文档进行处理的
  /** Load metadata from the given {@link IndexInput}.
   *  @see DirectMonotonicReader#getInstance(Meta, RandomAccessInput) */
  public static Meta loadMeta(IndexInput metaIn, long numValues, int blockShift) throws IOException {
    Meta meta = new Meta(numValues, blockShift); // 3个chunk，只有一个,一个block有1024个次
    for (int i = 0; i < meta.numBlocks; ++i) { //循环每个block,该block多少个chunk
      meta.mins[i] = metaIn.readLong(); // 每个chunK的
      meta.avgs[i] = Float.intBitsToFloat(metaIn.readInt());
      meta.offsets[i] = metaIn.readLong(); // 这个block在data中的起始位置
      meta.bpvs[i] = metaIn.readByte(); // byteRequire
    }
    return meta;
  }
  // 还原一级索引16*文件在dvm中存放的起始位置
  /**
   * Retrieves an instance from the specified slice.
   */
  public static DirectMonotonicReader getInstance(Meta meta, RandomAccessInput data) throws IOException {
    final LongValues[] readers = new LongValues[meta.numBlocks]; // 多少个block
    for (int i = 0; i < meta.mins.length; ++i) {
      if (meta.bpvs[i] == 0) {
        readers[i] = EMPTY;
      } else { // 读具体的原始值
        readers[i] = DirectReader.getInstance(data, meta.bpvs[i], meta.offsets[i]);
      }
    }

    return new DirectMonotonicReader(meta.blockShift, readers, meta.mins, meta.avgs, meta.bpvs);
  }

  private final int blockShift; // 一个block最多放多少个chunk,一般都是1024个
  private final LongValues[] readers; // 一个元素就是一个block，每个元素（例如DirectPackedReader4）里面每个元素代表一个chunk的buffer
  private final long[] mins; // ，每个block里面所有元素相对最小值
  private final float[] avgs; // 每个block里面所有元素的相对平均值
  private final byte[] bpvs; // requireBit
  private final int nonZeroBpvs;

  private DirectMonotonicReader(int blockShift, LongValues[] readers, long[] mins, float[] avgs, byte[] bpvs) {
    this.blockShift = blockShift;
    this.readers = readers;
    this.mins = mins;
    this.avgs = avgs;
    this.bpvs = bpvs;
    if (readers.length != mins.length || readers.length != avgs.length || readers.length != bpvs.length) {
      throw new IllegalArgumentException();
    }
    int nonZeroBpvs = 0;
    for (byte b : bpvs) {
      if (b != 0) {
        nonZeroBpvs++;
      }
    }
    this.nonZeroBpvs = nonZeroBpvs;
  }

  @Override
  public long get(long index) { //index是文档id,返回的是这个chunk的文档个数
    final int block = (int) (index >>> blockShift); // 在第几个block上
    final long blockIndex = index & ((1 << blockShift) - 1); // 这个block内第几个chunk
    final long delta = readers[block].get(blockIndex); // 从fdx中读偏移量
    return mins[block] + (long) (avgs[block] * blockIndex) + delta; // 返回这chunk对应的数字
  }

  /** Get lower/upper bounds for the value at a given index without hitting the direct reader. */
  private long[] getBounds(long index) { // index=第i个chunk
    final int block = Math.toIntExact(index >>> blockShift); // 这个chunk在第几个block上上
    final long blockIndex = index & ((1 << blockShift) - 1); // 落到某个block内的chunk数
    final long lowerBound = mins[block] + (long) (avgs[block] * blockIndex); // 这个chunk的下限
    final long upperBound = lowerBound + (1L << bpvs[block]) - 1; // 这个chunk的上限
    if (bpvs[block] == 64 || upperBound < lowerBound) { // overflow
      return new long[] { Long.MIN_VALUE, Long.MAX_VALUE };
    } else {
      return new long[] { lowerBound, upperBound };
    }
  }

  /**
   * Return the index of a key if it exists, or its insertion point otherwise
   * like {@link Arrays#binarySearch(long[], int, int, long)}.
   *
   * @see Arrays#binarySearch(long[], int, int, long) 将dfx和fdm的值都加载到了内存中
   */ // 二分搜索，扎到这个文档所在的chunk。比如32 32 12,那么在存储的时候就是0 32 64 78。当我们搜索的时候，key只会是0 32  64 78.
  public long binarySearch(long fromIndex, long toIndex, long key) { //查找这个key落在了哪个chunk上
    if (fromIndex < 0 || fromIndex > toIndex) {//fromIndex和toIndex都是chunkId
      throw new IllegalArgumentException("fromIndex=" + fromIndex + ",toIndex=" + toIndex);
    }
    long lo = fromIndex;
    long hi = toIndex - 1;

    while (lo <= hi) {
      final long mid = (lo + hi) >>> 1;
      // Try to run as many iterations of the binary search as possible without
      // hitting the direct readers, since they might hit a page fault.
      final long[] bounds = getBounds(mid); // 尝试找到这个chunk(数字)的上下界
      if (bounds[1] < key) { //
        lo = mid + 1;
      } else if (bounds[0] > key) {
        hi = mid - 1;
      } else {// 此大概率是low=high,此时midVal=lowest
        final long midVal = get(mid); // //index是文档id,返回的是这个chunk的文档数
        if (midVal < key) {
          lo = mid + 1;
        } else if (midVal > key) { //只能跑这里
          hi = mid - 1;
        } else { // 直到找到对应的chunk
          return mid;
        }
      }
    }

    return -1 - lo; // 若没找到，此时一般都是log=high+1，已经错过了，那么-2-(-1-lo)=log-1
  }

  @Override
  public long ramBytesUsed() {
    // Don't include meta, which should be accounted separately
    return BASE_RAM_BYTES_USED + RamUsageEstimator.shallowSizeOf(readers) +
        // Assume empty objects for the readers
        nonZeroBpvs * RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER);
  }

}
