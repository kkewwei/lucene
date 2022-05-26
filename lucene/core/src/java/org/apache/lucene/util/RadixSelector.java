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
package org.apache.lucene.util;

import java.util.Arrays;

/** Radix selector.
 *  <p>This implementation works similarly to a MSB radix sort except that it
 *  only recurses into the sub partition that contains the desired value.
 *  @lucene.internal */  // 原理类似基数排序，MSB从最左边开始排,从最大的位开始排序
public abstract class RadixSelector extends Selector { // 基数选择器

  // after that many levels of recursion we fall back to introselect anyway
  // this is used as a protection against the fact that radix sort performs
  // worse when there are long common prefixes (probably because of cache
  // locality) // 当多次递归后，需要退化到introselect，因为相同前缀较多时，基数排序性能较差
  private static final int LEVEL_THRESHOLD = 8;
  // size of histograms: 256 + 1 to indicate that the string is finished
  private static final int HISTOGRAM_SIZE = 257; // 字母只有256个
  // buckets below this size will be sorted with introselect
  private static final int LENGTH_THRESHOLD = 100; // 小于100的数据，将使用introselect排序

  // we store one histogram per recursion level
  private final int[] histogram = new int[HISTOGRAM_SIZE]; // 字母只有256个
  private final int[] commonPrefix; // 公用的，以免每次使用都得重新构建, 长度现场构建

  private final int maxLength;

  /**
   * Sole constructor.
   * @param maxLength the maximum length of keys, pass {@link Integer#MAX_VALUE} if unknown.
   */
  protected RadixSelector(int maxLength) {
    this.maxLength = maxLength; //
    this.commonPrefix = new int[Math.min(24, maxLength)];// 单个point内value个数不能超过24
  }

  /** Return the k-th byte of the entry at index {@code i}, or {@code -1} if
   * its length is less than or equal to {@code k}. This may only be called
   * with a value of {@code i} between {@code 0} included and
   * {@code maxLength} excluded. */
  protected abstract int byteAt(int i, int k); // 返回第i个point的第k个byte(此时位数已经不相同了)

  /** Get a fall-back selector which may assume that the first {@code d} bytes
   *  of all compared strings are equal. This fallback selector is used when
   *  the range becomes narrow or when the maximum level of recursion has
   *  been exceeded. */
  protected Selector getFallbackSelector(int d) { // 当范围很小时，或者递归很深时，就进来
    return new IntroSelector() {
      @Override
      protected void swap(int i, int j) {
        RadixSelector.this.swap(i, j);
      }

      @Override
      protected int compare(int i, int j) {
        for (int o = d; o < maxLength; ++o) {
          final int b1 = byteAt(i, o);
          final int b2 = byteAt(j, o);
          if (b1 != b2) {
            return b1 - b2;
          } else if (b1 == -1) {
            break;
          }
        }
        return 0;
      }

      @Override
      protected void setPivot(int i) {
        pivot.setLength(0);
        for (int o = d; o < maxLength; ++o) {
          final int b = byteAt(i, o);
          if (b == -1) {
            break;
          }
          pivot.append((byte) b);
        }
      }

      @Override
      protected int comparePivot(int j) {
        for (int o = 0; o < pivot.length(); ++o) {
          final int b1 = pivot.byteAt(o) & 0xff;
          final int b2 = byteAt(j, d + o);
          if (b1 != b2) {
            return b1 - b2;
          }
        }
        if (d + pivot.length() == maxLength) {
          return 0;
        }
        return -1 - byteAt(j, d + pivot.length());
      }

      private final BytesRefBuilder pivot = new BytesRefBuilder();
    };
  }

  @Override
  public void select(int from, int to, int k) {// from和to、middle都是point下标，不是叶子下标
    checkArgs(from, to, k);
    select(from, to, k, 0, 0);
  }
  // k: 基本是middle, d：不相同的前缀+doc组成的第几个字符。l：递归的次数
  private void select(int from, int to, int k, int d, int l) {// from和to、middle都是point下标，不是叶子下标
    if (to - from <= LENGTH_THRESHOLD || d >= LEVEL_THRESHOLD) { //当数据个数少于 100，或者递归次数超过8次，就开始蜕化为
      getFallbackSelector(d).select(from, to, k); //
    } else {
      radixSelect(from, to, k, d, l); // 基数选择
    }
  }

  /**
   * @param d the character number to compare 要比较的字符编号（不相同的前缀长度+文档Id取值构成）
   * @param l the level of recursion // 递归的次数
   */ // k  右子树第一个叶子节点的第一个point
  private void radixSelect(int from, int to, int k, int d, int l) {// from和to、middle都是point下标，不是叶子下标
    final int[] histogram = this.histogram; //统计的是每一字符多少个数据
    Arrays.fill(histogram, 0); // 每次使用都得清空

    final int commonPrefixLength = computeCommonPrefixLengthAndBuildHistogram(from, to, d, histogram);
    if (commonPrefixLength > 0) { // 所有point中有相同的逻辑位数的前缀
      // if there are no more chars to compare or if all entries fell into the
      // first bucket (which means strings are shorter than d) then we are done
      // otherwise recurse
      if (d + commonPrefixLength < maxLength
          && histogram[0] < to - from) {
        radixSelect(from, to, k, d + commonPrefixLength, l); // 忽略过相同的前缀部分，继续排序
      }
      return;
    }
    assert assertHistogram(commonPrefixLength, histogram);
    // 所有point逻辑位数没有一个相同前缀
    int bucketFrom = from; //
    for (int bucket = 0; bucket < HISTOGRAM_SIZE; ++bucket) { // 遍历每一个桶
      final int bucketTo = bucketFrom + histogram[bucket]; // 获取桶里面数据的范围

      if (bucketTo > k) { // 若读取的数据个数超过中位数（文档下标），
        partition(from, to, bucket, bucketFrom, bucketTo, d); // 通过快排的思想，完成左边小于桶，右边大于桶

        if (bucket != 0 && d + 1 < maxLength) {// 最中间的那个桶继续排序
          // all elements in bucket 0 are equal so we only need to recurse if bucket != 0
          select(bucketFrom, bucketTo, k, d + 1, l + 1);
        }
        return; // 只要拍完序，立马返回
      }
      bucketFrom = bucketTo;
    }
    throw new AssertionError("Unreachable code");
  }

  // only used from assert
  private boolean assertHistogram(int commonPrefixLength, int[] histogram) {
    int numberOfUniqueBytes = 0;
    for (int freq : histogram) {
      if (freq > 0) {
        numberOfUniqueBytes++; // 有多少个unique
      }
    }
    if (numberOfUniqueBytes == 1) {
      assert commonPrefixLength >= 1;
    } else {
      assert commonPrefixLength == 0;
    }
    return true;
  }

  /** Return a number for the k-th character between 0 and {@link #HISTOGRAM_SIZE}. */
  private int getBucket(int i, int k) { // 从第i个point（文档）中的最大偏移级中读取第k个元素（不相同的前缀长度+文档Id取值构成）
    return byteAt(i, k) + 1; // 还+1，得到的应该是第几个bucket
  }

  /** Build a histogram of the number of values per {@link #getBucket(int, int) bucket} // 每个数组统计每个桶元素的个数
   *  and return a common prefix length for all visited values.
   *  @see #buildHistogram */ // k：要比较的字符编号
  private int computeCommonPrefixLengthAndBuildHistogram(int from, int to, int k, int[] histogram) {
    final int[] commonPrefix = this.commonPrefix; //公用的，为了减少产生次数
    int commonPrefixLength = Math.min(commonPrefix.length, maxLength - k); // 逻辑位数相同的前缀
    for (int j = 0; j < commonPrefixLength; ++j) { // 好像是从from中读取值，
      final int b = byteAt(from, k + j); // 从第from的point中读取最大维度差（切分维度）中第k+j字符
      commonPrefix[j] = b;
      if (b == -1) {
        commonPrefixLength = j + 1;
        break;
      }
    }
    // 遍历from+1到to的每个值，和from中得到的值进行比较，找所有数据中相同的前缀
    int i;
    outer: for (i = from + 1; i < to; ++i) {//这些词中，commonPrefixLength个中相同的前缀
      for (int j = 0; j < commonPrefixLength; ++j) {
        final int b = byteAt(i, k + j); // 从第k个字符开始统计
        if (b != commonPrefix[j]) {
          commonPrefixLength = j; // 最多只有前commonPrefixLength的doc的前j位是相同的,下次比较就少比较点
          if (commonPrefixLength == 0) { // we have no common prefix，最起码此前数据，第一位都是相同的（第一位都不相同）
            histogram[commonPrefix[0] + 1] = i - from; // 统计第k个字母出现次数，+1是习惯，每个地方都加1了。
            histogram[b + 1] = 1; // 只有相同前缀为0，第一个字符都不相同
            break outer;
          }
          break;
        }
      }
    }

    if (i < to) { // 没有共同相同的前缀
      // the loop got broken because there is no common prefix
      assert commonPrefixLength == 0; // 这些数据已经没有相同的前缀了
      buildHistogram(i + 1, to, k, histogram); // 统计每个首字母的次数
    } else { // 能比较到最后一位，说明前面最少一位是相同的
      assert commonPrefixLength > 0;
      histogram[commonPrefix[0] + 1] = to - from; // 貌似没统计完呀
    }

    return commonPrefixLength; //同时返回了相同的前缀
  }

  /** Build an histogram of the k-th characters of values occurring between
   *  offsets {@code from} and {@code to}, using {@link #getBucket}. */
  private void buildHistogram(int from, int to, int k, int[] histogram) {
    for (int i = from; i < to; ++i) {
      histogram[getBucket(i, k)]++;// k（不相同的前缀长度+文档Id取值构成）
    }
  }
   // 使用快排的思想，第bucket个桶左边的数据全部小于第bucket个桶，右边的数据，全部大于第bucket个桶
  /** Reorder elements so that all of them that fall into {@code bucket} are
   *  between offsets {@code bucketFrom} and {@code bucketTo}. */// from和to、middle都是point下标，不是叶子下标
  private void partition(int from, int to, int bucket, int bucketFrom, int bucketTo, int d) { // 第bucket个桶中包含中间数据
    int left = from;
    int right = to - 1;//非闭区间

    int slot = bucketFrom;
    // 这里有变成快排序的思想
    for (;;) {
      int leftBucket = getBucket(left, d); // 获取左边桶的下标元素，d:要比较的字符编号（不相同的前缀长度+文档Id取值构成）
      int rightBucket = getBucket(right, d);// 获取最大数是第几个桶

      while (leftBucket <= bucket && left < bucketFrom) { // 按照桶来
        if (leftBucket == bucket) { // 找到了一个这个桶的一个元素，和中间的值一样，那么先给你换到中间
          swap(left, slot++); // 跑到MutablePointsReaderUtils.partition()里面匿名类定义的那里
        } else {
          ++left;
        }
        leftBucket = getBucket(left, d); // 找到当前
      }
      // 再从右边找，
      while (rightBucket >= bucket && right >= bucketTo) {
        if (rightBucket == bucket) {
          swap(right, slot++);
        } else {
          --right;
        }
        rightBucket = getBucket(right, d);
      }
      //
      if (left < bucketFrom && right >= bucketTo) {
        swap(left++, right--);
      } else {
        assert left == bucketFrom;
        assert right == bucketTo - 1;
        break;
      }
    }
  }
}
