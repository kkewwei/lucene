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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.RamUsageEstimator;
// 压缩原理非常简单，看最大值需要多少位，则选择什么装，比如最大8位，使用byte装，若16位，使用short装，若24位，使用3个byte装，若48，则使用3个short装
/**
 * Utility class to compress integers into a {@link LongValues} instance.
 */
public class PackedLongValues extends LongValues implements Accountable {// 若最大值为负数，退化为64位long装数

  private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(PackedLongValues.class);

  static final int DEFAULT_PAGE_SIZE = 1024;
  static final int MIN_PAGE_SIZE = 64;
  // More than 1M doesn't really makes sense with these appending buffers
  // since their goal is to try to have small numbers of bits per value
  static final int MAX_PAGE_SIZE = 1 << 20;//1m

  /** Return a new {@link Builder} that will compress efficiently positive integers. */
  public static PackedLongValues.Builder packedBuilder(int pageSize, float acceptableOverheadRatio) {
    return new PackedLongValues.Builder(pageSize, acceptableOverheadRatio);
  }

  /** @see #packedBuilder(int, float) */
  public static PackedLongValues.Builder packedBuilder(float acceptableOverheadRatio) {
    return packedBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
  }

  /** Return a new {@link Builder} that will compress efficiently integers that
   *  are close to each other. */
  public static PackedLongValues.Builder deltaPackedBuilder(int pageSize, float acceptableOverheadRatio) {
    return new DeltaPackedLongValues.Builder(pageSize, acceptableOverheadRatio);
  }

  /** @see #deltaPackedBuilder(int, float) */
  public static PackedLongValues.Builder deltaPackedBuilder(float acceptableOverheadRatio) {
    return deltaPackedBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
  }

  /** Return a new {@link Builder} that will compress efficiently integers that
   *  would be a monotonic function of their index. */
  public static PackedLongValues.Builder monotonicBuilder(int pageSize, float acceptableOverheadRatio) {
    return new MonotonicLongValues.Builder(pageSize, acceptableOverheadRatio);
  }

  /** @see #monotonicBuilder(int, float) */
  public static PackedLongValues.Builder monotonicBuilder(float acceptableOverheadRatio) {
    return monotonicBuilder(DEFAULT_PAGE_SIZE, acceptableOverheadRatio);
  }

  final PackedInts.Reader[] values;
  final int pageShift, pageMask; // pageShift=1024
  private final long size;
  private final long ramBytesUsed;

  PackedLongValues(int pageShift, int pageMask, PackedInts.Reader[] values, long size, long ramBytesUsed) {
    this.pageShift = pageShift;
    this.pageMask = pageMask;
    this.values = values;
    this.size = size;
    this.ramBytesUsed = ramBytesUsed;
  }

  /** Get the number of values in this array. */
  public final long size() {
    return size;
  }

  int decodeBlock(int block, long[] dest) {
    final PackedInts.Reader vals = values[block]; // 解压其中一个value
    final int size = vals.size();// 获取压缩的个数
    for (int k = 0; k < size; ) {
      k += vals.get(k, dest, k, size - k);
    }
    return size;
  }

  long get(int block, int element) {
    return values[block].get(element);
  }

  @Override
  public final long get(long index) {
    assert index >= 0 && index < size();
    final int block = (int) (index >> pageShift);
    final int element = (int) (index & pageMask);
    return get(block, element);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  /** Return an iterator over the values of this array. */
  public Iterator iterator() {
    return new Iterator();
  }

  /** An iterator over long values. */
  final public class Iterator {

    final long[] currentValues;//  从values中解压缩出来的一个数据, 放入这里。边读取， 边解压边读取
    int vOff, pOff;
    int currentCount; // number of entries of the current page

    Iterator() {
      currentValues = new long[pageMask + 1];
      vOff = pOff = 0;
      fillBlock();
    }

    private void fillBlock() {
      if (vOff == values.length) {
        currentCount = 0;
      } else {
        currentCount = decodeBlock(vOff, currentValues); // 解压block
        assert currentCount > 0;
      }
    }

    /** Whether or not there are remaining values. */
    public final boolean hasNext() {
      return pOff < currentCount;
    }

    /** Return the next long in the buffer. */
    public final long next() {
      assert hasNext();
      long result = currentValues[pOff++];
      if (pOff == currentCount) {
        vOff += 1;
        pOff = 0;
        fillBlock(); // 若没有了，就会解压下一个value
      }
      return result;
    }

  }
  // 辅助产生每个Builder
  /** A Builder for a {@link PackedLongValues} instance. */
  public static class Builder implements Accountable {

    private static final int INITIAL_PAGE_COUNT = 16;
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(Builder.class);

    final int pageShift, pageMask; // pageShift默认1024
    final float acceptableOverheadRatio;
    long[] pending; // 暂存的是每个termId，相同的算俩 / 文档Id
    long size; // 暂存的文档个数

    PackedInts.Reader[] values; //  存放的是这里[]Direct64, 里面有数数组直接填写。初始长度为16,装满了还能继续装
    long ramBytesUsed;
    int valuesOff;// value里面当前起始位置
    int pendingOff; //目前已经存进来了几个term

    Builder(int pageSize, float acceptableOverheadRatio) {
      pageShift = checkBlockSize(pageSize, MIN_PAGE_SIZE, MAX_PAGE_SIZE); // page长度，多少个二进制
      pageMask = pageSize - 1;
      this.acceptableOverheadRatio = acceptableOverheadRatio;
      values = new PackedInts.Reader[INITIAL_PAGE_COUNT];
      pending = new long[pageSize];
      valuesOff = 0;
      pendingOff = 0;
      size = 0;
      ramBytesUsed = baseRamBytesUsed() + RamUsageEstimator.sizeOf(pending) + RamUsageEstimator.shallowSizeOf(values);
    }

    /** Build a {@link PackedLongValues} instance that contains values that
     *  have been added to this builder. This operation is destructive. */
    public PackedLongValues build() {
      finish(); // 把termId压缩存放
      pending = null;
      final PackedInts.Reader[] values = ArrayUtil.copyOfSubArray(this.values, 0, valuesOff);
      final long ramBytesUsed = PackedLongValues.BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(values);
      return new PackedLongValues(pageShift, pageMask, values, size, ramBytesUsed);
    }

    long baseRamBytesUsed() {
      return BASE_RAM_BYTES_USED;
    }

    @Override
    public final long ramBytesUsed() {
      return ramBytesUsed;
    }

    /** Return the number of elements that have been added to this builder. */
    public final long size() {
      return size;
    }

    /** Add a new element to this builder. */
    public Builder add(long l) {
      if (pending == null) {
        throw new IllegalStateException("Cannot be reused after build()");
      }
      if (pendingOff == pending.length) { // 目前1024个，若存满了则扩容一次
        // check size
        if (values.length == valuesOff) { // 是否需要库容
          final int newLength = ArrayUtil.oversize(valuesOff + 1, 8);
          grow(newLength);
        }
        pack();
      }
      pending[pendingOff++] = l;
      size += 1;
      return this;
    }

    final void finish() {
      if (pendingOff > 0) { // 还有点的话，就再次压缩
        if (values.length == valuesOff) {
          grow(valuesOff + 1);
        }
        pack(); // 给压缩了
      }
    }

    private void pack() {
      pack(pending, pendingOff, valuesOff, acceptableOverheadRatio);
      ramBytesUsed += values[valuesOff].ramBytesUsed();
      valuesOff += 1;
      // reset pending buffer
      pendingOff = 0;
    }
    //第一个参数为准备打包的数组, 第二个是要打包的数字的个数，，第三个是打包后生成的Mutable是第几个（和咱要说的没有任何关系），第四个的是压缩时能够使用多少内存
    void pack(long[] values, int numValues, int block, float acceptableOverheadRatio) {
      assert numValues > 0;
      // compute max delta
      long minValue = values[0];
      long maxValue = values[0];
      for (int i = 1; i < numValues; ++i) { // 找到最大的和最小的那个数
        minValue = Math.min(minValue, values[i]);
        maxValue = Math.max(maxValue, values[i]);
      }

      // build a new packed reader
      if (minValue == 0 && maxValue == 0) {
        this.values[block] = new PackedInts.NullReader(numValues);//如果所有的值都是0，则单独生成一个NullReader，标识所有的值都是0.
      } else {
        final int bitsRequired = minValue < 0 ? 64 : PackedInts.bitsRequired(maxValue); //最大的数字需要多少位，最小数如果是负数，则需要64位，否则为（64-他的开始的0的数量）
        final PackedInts.Mutable mutable = PackedInts.getMutable(numValues, bitsRequired, acceptableOverheadRatio);//这个方法便是生成一个可以装数的包， Packed64
        for (int i = 0; i < numValues; ) { // 装每个数
          i += mutable.set(i, values, i, numValues - i); // 跑到Packed64.set()
        }
        this.values[block] = mutable; // 每次打包都能产生一个value
      }
    }

    void grow(int newBlockCount) {
      ramBytesUsed -= RamUsageEstimator.shallowSizeOf(values);
      values = ArrayUtil.growExact(values, newBlockCount);
      ramBytesUsed += RamUsageEstimator.shallowSizeOf(values);
    }

  }

}
