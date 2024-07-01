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
import org.apache.lucene.util.LongValues;

/**
 * Retrieves an instance previously written by {@link DirectMonotonicWriter}.
 *
 * @see DirectMonotonicWriter
 */
public final class DirectMonotonicReader extends LongValues {

  /**
   * In-memory metadata that needs to be kept around for {@link DirectMonotonicReader} to read data
   * from disk.
   */
  public static class Meta {

    // Use a shift of 63 so that there would be a single block regardless of the number of values.
    private static final Meta SINGLE_ZERO_BLOCK = new Meta(1L, 63);

    private final int blockShift;// 10
    private final int numBlocks;// 一次最对可以存1024个数字，称为一个block，看用了几个block
    private final long[] mins;
    private final float[] avgs;
    private final byte[] bpvs;
    private final long[] offsets;

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
  }
// 可以参考DirectMonotonicWriter，是对文档进行处理的
  /**
   * Load metadata from the given {@link IndexInput}.
   *
   * @see DirectMonotonicReader#getInstance(Meta, RandomAccessInput)
   */
  public static Meta loadMeta(IndexInput metaIn, long numValues, int blockShift) // 这个是block的meta
      throws IOException {
    boolean allValuesZero = true;
    Meta meta = new Meta(numValues, blockShift);// 3个chunk，只有一个chunk,一个block有1024个次
    for (int i = 0; i < meta.numBlocks; ++i) {//循环每个block
      long min = metaIn.readLong();// 这个block
      meta.mins[i] = min;// 这个block的最小值
      int avgInt = metaIn.readInt();
      meta.avgs[i] = Float.intBitsToFloat(avgInt);
      meta.offsets[i] = metaIn.readLong(); // 这个block在data中的起始位置
      byte bpvs = metaIn.readByte(); // byteRequire
      meta.bpvs[i] = bpvs;
      allValuesZero = allValuesZero && min == 0L && avgInt == 0 && bpvs == 0;
    }
    // save heap in case all values are zero
    return allValuesZero ? Meta.SINGLE_ZERO_BLOCK : meta;
  }
// 还原一级索引16*文件在dvm中存放的起始位置
  /** Retrieves a non-merging instance from the specified slice. */
  public static DirectMonotonicReader getInstance(Meta meta, RandomAccessInput data)
      throws IOException {
    return getInstance(meta, data, false);
  }

  /** Retrieves an instance from the specified slice. */
  public static DirectMonotonicReader getInstance(
      Meta meta, RandomAccessInput data, boolean merging) throws IOException {
    final LongValues[] readers = new LongValues[meta.numBlocks];// 多少个block
    for (int i = 0; i < meta.numBlocks; ++i) {
      if (meta.bpvs[i] == 0) {
        readers[i] = LongValues.ZEROES;
      } else if (merging
          && i < meta.numBlocks - 1 // we only know the number of values for the last block
          && meta.blockShift >= DirectReader.MERGE_BUFFER_SHIFT) {// 读具体的原始值
        readers[i] =
            DirectReader.getMergeInstance(
                data, meta.bpvs[i], meta.offsets[i], 1L << meta.blockShift);
      } else {
        readers[i] = DirectReader.getInstance(data, meta.bpvs[i], meta.offsets[i]);
      }
    }

    return new DirectMonotonicReader(meta.blockShift, readers, meta.mins, meta.avgs, meta.bpvs);
  }

  private final int blockShift;// 一个block最多放多少个chunk,一般都是1024个
  private final long blockMask;
  private final LongValues[] readers;// 一个元素就是一个block，每个元素（例如DirectPackedReader4）里面每个元素代表一个chunk的buffer
  private final long[] mins;// ，每个block里面所有元素相对最小值
  private final float[] avgs; // 每个block里面所有元素的相对平均值
  private final byte[] bpvs;// requireBit

  private DirectMonotonicReader(
      int blockShift, LongValues[] readers, long[] mins, float[] avgs, byte[] bpvs) {
    this.blockShift = blockShift;
    this.blockMask = (1L << blockShift) - 1;
    this.readers = readers;
    this.mins = mins;
    this.avgs = avgs;
    this.bpvs = bpvs;
    if (readers.length != mins.length
        || readers.length != avgs.length
        || readers.length != bpvs.length) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public long get(long index) { //index是chunkId, 返回这chunk对应的数字/或者返回的是这个chunk的在fdt中的存放
    final int block = (int) (index >>> blockShift);// 在第几个block上
    final long blockIndex = index & blockMask; // 这个block内第几个chunk
    final long delta = readers[block].get(blockIndex);// 从fdx中读偏移量：为了计算出这个chunk真正起始docId
    return mins[block] + (long) (avgs[block] * blockIndex) + delta; // 返回这chunk对应的数字
  }

  /** Get lower/upper bounds for the value at a given index without hitting the direct reader. */
  private long[] getBounds(long index) { // index=第i个chunk， 返回的是block的上下限
    final int block = Math.toIntExact(index >>> blockShift);// 这个chunk在第几个block上
    final long blockIndex = index & blockMask;// 落到某个block内的chunk数（1024个value组装成一个block）
    final long lowerBound = mins[block] + (long) (avgs[block] * blockIndex); // 这个block的下限
    final long upperBound = lowerBound + (1L << bpvs[block]) - 1; // 这个block的上限
    if (bpvs[block] == 64 || upperBound < lowerBound) { // overflow
      return new long[] {Long.MIN_VALUE, Long.MAX_VALUE};
    } else {
      return new long[] {lowerBound, upperBound};
    }
  }

  /**
   * Return the index of a key if it exists, or its insertion point otherwise like {@link
   * Arrays#binarySearch(long[], int, int, long)}.
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
      final long[] bounds = getBounds(mid); // 尝试找到这个chunk对应block的上下界
      if (bounds[1] < key) { // 在右边
        lo = mid + 1;
      } else if (bounds[0] > key) {
        hi = mid - 1;
      } else {//
        final long midVal = get(mid); // mid是chunkId，这里返回值是这个chunkId的起始docId
        if (midVal < key) {
          lo = mid + 1;
        } else if (midVal > key) { //只能跑这里
          hi = mid - 1;
        } else { // 直到找到对应的chunk
          return mid; // 返回chunkId
        }
      }
    }

    return -1 - lo; // 若没找到，此时一般都是log=high+1，已经错过了，那么-2-(-1-lo)=log-1
  }
}
