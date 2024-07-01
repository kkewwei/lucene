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
package org.apache.lucene.util.bkd;

import java.util.Arrays;
import org.apache.lucene.codecs.MutablePointTree;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.RadixSelector;
import org.apache.lucene.util.Selector;
import org.apache.lucene.util.StableMSBRadixSorter;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Utility APIs for sorting and partitioning buffered points.
 *
 * @lucene.internal
 */
public final class MutablePointTreeReaderUtils {

  MutablePointTreeReaderUtils() {}

  /** Sort the given {@link MutablePointTree} based on its packed value then doc ID. */
  public static void sort(BKDConfig config, int maxDoc, MutablePointTree reader, int from, int to) {

    boolean sortedByDocID = true;
    int prevDoc = 0;
    for (int i = from; i < to; ++i) {
      int doc = reader.getDocID(i);
      if (doc < prevDoc) {
        sortedByDocID = false;
        break;
      }
      prevDoc = doc;
    }

    // No need to tie break on doc IDs if already sorted by doc ID, since we use a stable sort.
    // This should be a common situation as IndexWriter accumulates data in doc ID order when
    // index sorting is not enabled.
    final int bitsPerDocId = sortedByDocID ? 0 : PackedInts.bitsRequired(maxDoc - 1);// 若已经按docId排序，就不用再排序了；否则当成高位比较
    new StableMSBRadixSorter(config.packedBytesLength() + (bitsPerDocId + 7) / 8) {

      @Override
      protected void swap(int i, int j) {
        reader.swap(i, j);
      }

      @Override
      protected void save(int i, int j) {
        reader.save(i, j);
      }

      @Override
      protected void restore(int i, int j) {
        reader.restore(i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        if (k < config.packedBytesLength()) {
          return Byte.toUnsignedInt(reader.getByteAt(i, k));
        } else {// 超过数据部分长度，开始比较docId
          final int shift = bitsPerDocId - ((k - config.packedBytesLength() + 1) << 3);
          return (reader.getDocID(i) >>> Math.max(0, shift)) & 0xff;
        }
      }
    }.sort(from, to);
  }

  /** Sort points on the given dimension. */
  public static void sortByDim(// 基于快排，对某一维度进行排序
      BKDConfig config,
      int sortedDim,
      int[] commonPrefixLengths,
      MutablePointTree reader,
      int from,
      int to,
      BytesRef scratch1,
      BytesRef scratch2) {

    final ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(config.bytesPerDim());
    final int start = sortedDim * config.bytesPerDim();// point内偏移量
    // No need for a fancy radix sort here, this is called on the leaves only so
    // there are not many values to sort
    new IntroSorter() {

      final BytesRef pivot = scratch1;
      int pivotDoc = -1;

      @Override
      protected void swap(int i, int j) {
        reader.swap(i, j);
      }

      @Override
      protected void setPivot(int i) {
        reader.getValue(i, pivot);
        pivotDoc = reader.getDocID(i);
      }

      @Override
      protected int comparePivot(int j) {// 先比较不同的值，在比较docId
        reader.getValue(j, scratch2);
        int cmp =
            comparator.compare(
                pivot.bytes, pivot.offset + start, scratch2.bytes, scratch2.offset + start);
        if (cmp == 0) {
          cmp =
              Arrays.compareUnsigned(
                  pivot.bytes,
                  pivot.offset + config.packedIndexBytesLength(),
                  pivot.offset + config.packedBytesLength(),
                  scratch2.bytes,
                  scratch2.offset + config.packedIndexBytesLength(),
                  scratch2.offset + config.packedBytesLength());
          if (cmp == 0) {
            cmp = pivotDoc - reader.getDocID(j);
          }
        }
        return cmp;
      }
    }.sort(from, to);
  }

  /**
   * Partition points around {@code mid}. All values on the left must be less than or equal to it
   * and all values on the right must be greater than or equal to it.
   */
  public static void partition(
      BKDConfig config,
      int maxDoc,
      int splitDim,
      int commonPrefixLen,
      MutablePointTree reader,
      int from,
      int to,
      int mid, // from和to、middle都是point下标，不是叶子下标
      BytesRef scratch1,
      BytesRef scratch2) {
    final int dimOffset = splitDim * config.bytesPerDim() + commonPrefixLen;// 相同纬度的数据，从这个位开始不一致了（该元素内的偏移量）
    final int dimCmpBytes = config.bytesPerDim() - commonPrefixLen;// 需要比较的位数
    final int dataCmpBytes =
        (config.numDims() - config.numIndexDims()) * config.bytesPerDim() + dimCmpBytes;
    final int bitsPerDocId = PackedInts.bitsRequired(maxDoc - 1);// 最大的那个文档id需要多少位
    new RadixSelector(dataCmpBytes + (bitsPerDocId + 7) / 8) {//  这里位数为两类，可以从byteAt()看出，读取每一类的方式也不一样
      // 第一类就是普通的dimCmpBytes，读取的是不相同的字符；第二类是 (bitsPerDocId + 7) / 8， 把文档ID分成几份，每一份内的元素该位相同
      @Override
      protected Selector getFallbackSelector(int k) {// 使用快排进行排序, k:表示第几个字符
        final int dimStart = splitDim * config.bytesPerDim();
        final int dataStart =
            (k < dimCmpBytes)
                ? config.packedIndexBytesLength()
                : config.packedIndexBytesLength() + k - dimCmpBytes;
        final int dataEnd = config.numDims() * config.bytesPerDim();
        final ByteArrayComparator dimComparator =
            ArrayUtil.getUnsignedComparator(config.bytesPerDim());
        return new IntroSelector() {

          final BytesRef pivot = scratch1;
          int pivotDoc;

          @Override
          protected void swap(int i, int j) {
            reader.swap(i, j);
          }

          @Override
          protected void setPivot(int i) {
            reader.getValue(i, pivot);
            pivotDoc = reader.getDocID(i);
          }

          @Override
          protected int comparePivot(int j) { // 当范围很小时，或者递归很深时，就进来
            if (k < dimCmpBytes) {
              reader.getValue(j, scratch2);// 优先比较
              int cmp =
                  dimComparator.compare(
                      pivot.bytes, pivot.offset + dimStart,
                      scratch2.bytes, scratch2.offset + dimStart);

              if (cmp != 0) {
                return cmp;
              }
            }
            if (k < dataCmpBytes) {
              reader.getValue(j, scratch2);
              int cmp =
                  Arrays.compareUnsigned(
                      pivot.bytes,
                      pivot.offset + dataStart,
                      pivot.offset + dataEnd,
                      scratch2.bytes,
                      scratch2.offset + dataStart,
                      scratch2.offset + dataEnd);
              if (cmp != 0) {
                return cmp;
              }
            }
            return pivotDoc - reader.getDocID(j);// 通过文档大小相比较
          }
        };
      }

      @Override
      protected void swap(int i, int j) {
        reader.swap(i, j);
      }
      // 可以从maxLength=dataCmpBytes + (bitsPerDocId + 7) / 8可以看出，属于不同的读法，
      @Override
      protected int byteAt(int i, int k) {// 第i个w文档，k表示不同前缀相对位置
        if (k < dimCmpBytes) { // 读取的是dataCmpBytes中的数据
          return Byte.toUnsignedInt(reader.getByteAt(i, dimOffset + k));
        } else if (k < dataCmpBytes) {
          return Byte.toUnsignedInt(
              reader.getByteAt(i, config.packedIndexBytesLength() + k - dimCmpBytes));
        } else {// 比如bitsPerDocId=21位，将docId按照8位一份，比如分成了4份：比如读取第一份：那么reader.getDocID(i)>>8，读取高3份的值
          final int shift = bitsPerDocId - ((k - dataCmpBytes + 1) << 3);// 通过(k - dataCmpBytes)去掉原本影响
          return (reader.getDocID(i) >>> Math.max(0, shift)) & 0xff;
        }
      }
    }.select(from, to, mid);
  }
}
