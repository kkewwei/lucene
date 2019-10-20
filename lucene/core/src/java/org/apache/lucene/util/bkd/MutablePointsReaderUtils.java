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

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IntroSelector;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.MSBRadixSorter;
import org.apache.lucene.util.RadixSelector;
import org.apache.lucene.util.Selector;
import org.apache.lucene.util.packed.PackedInts;

/** Utility APIs for sorting and partitioning buffered points.
 *
 * @lucene.internal */
public final class MutablePointsReaderUtils {

  MutablePointsReaderUtils() {}

  /** Sort the given {@link MutablePointValues} based on its packed value then doc ID. */
  public static void sort(int maxDoc, int packedBytesLength,
                          MutablePointValues reader, int from, int to) {
    final int bitsPerDocId = PackedInts.bitsRequired(maxDoc - 1);
    new MSBRadixSorter(packedBytesLength + (bitsPerDocId + 7) / 8) {

      @Override
      protected void swap(int i, int j) {
        reader.swap(i, j);
      }

      @Override
      protected int byteAt(int i, int k) {
        if (k < packedBytesLength) {
          return Byte.toUnsignedInt(reader.getByteAt(i, k));
        } else {
          final int shift = bitsPerDocId - ((k - packedBytesLength + 1) << 3);
          return (reader.getDocID(i) >>> Math.max(0, shift)) & 0xff;
        }
      }

      @Override
      protected org.apache.lucene.util.Sorter getFallbackSorter(int k) {
        return new IntroSorter() {

          final BytesRef pivot = new BytesRef();
          final BytesRef scratch = new BytesRef();
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
          protected int comparePivot(int j) {
            if (k < packedBytesLength) {
              reader.getValue(j, scratch);
              int cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + k, pivot.offset + k + packedBytesLength - k,
                  scratch.bytes, scratch.offset + k, scratch.offset + k + packedBytesLength - k);
              if (cmp != 0) {
                return cmp;
              }
            }
            return pivotDoc - reader.getDocID(j);
          }
        };
      }

    }.sort(from, to);
  }
  // 基于快排，对某一维度进行排序
  /** Sort points on the given dimension. */
  public static void sortByDim(int numDataDim, int numIndexDim, int sortedDim, int bytesPerDim, int[] commonPrefixLengths,
                               MutablePointValues reader, int from, int to,
                               BytesRef scratch1, BytesRef scratch2) {

    final int start = sortedDim * bytesPerDim + commonPrefixLengths[sortedDim];
    final int dimEnd =  sortedDim * bytesPerDim + bytesPerDim;
    final int dataStart = numIndexDim * bytesPerDim;
    final int dataEnd = dataStart + (numDataDim - numIndexDim) * bytesPerDim;
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
      protected int comparePivot(int j) {
        reader.getValue(j, scratch2);
        int cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + start, pivot.offset + dimEnd, scratch2.bytes,
            scratch2.offset + start, scratch2.offset + dimEnd);
        if (cmp == 0) {
          cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + dataStart, pivot.offset + dataEnd,
              scratch2.bytes, scratch2.offset + dataStart, scratch2.offset + dataEnd);
          if (cmp == 0) {
            cmp = pivotDoc - reader.getDocID(j);
          }
        }
        return cmp;
      }
    }.sort(from, to);
  }

  /** Partition points around {@code mid}. All values on the left must be less
   *  than or equal to it and all values on the right must be greater than or
   *  equal to it. */
  public static void partition(int numDataDim, int numIndexDim, int maxDoc, int splitDim, int bytesPerDim, int commonPrefixLen,
                               MutablePointValues reader, int from, int to, int mid,
                               BytesRef scratch1, BytesRef scratch2) {
    final int dimOffset = splitDim * bytesPerDim + commonPrefixLen; // 相同纬度的数据，从这个位开始不一致了（该元素内的偏移量）
    final int dimCmpBytes = bytesPerDim - commonPrefixLen; // 需要比较的位数
    final int dataOffset = numIndexDim * bytesPerDim; // 整个point的数据结尾位置
    final int dataCmpBytes = (numDataDim - numIndexDim) * bytesPerDim + dimCmpBytes; // 该元素不相同的个数
    final int bitsPerDocId = PackedInts.bitsRequired(maxDoc - 1);
    new RadixSelector(dataCmpBytes + (bitsPerDocId + 7) / 8) { //  这里位数为两类，可以从byteAt()看出，读取每一类的方式也不一样
    // 第一类就是普通的dimCmpBytes，读取的是不相同的字符；第二类是 (bitsPerDocId + 7) / 8， 读取的是文档Id，就是说把文档Id作为了桶的一部分
      @Override
      protected Selector getFallbackSelector(int k) { // 使用快排进行排序
        final int dataStart = (k < dimCmpBytes) ? dataOffset : dataOffset + k - dimCmpBytes;
        final int dataEnd = numDataDim * bytesPerDim;
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
          protected int comparePivot(int j) {
            if (k < dimCmpBytes) {
              reader.getValue(j, scratch2);
              int cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + dimOffset + k, pivot.offset + dimOffset + dimCmpBytes,
                  scratch2.bytes, scratch2.offset + dimOffset + k, scratch2.offset + dimOffset + dimCmpBytes);
              if (cmp != 0) {
                return cmp;
              }
            }
            if (k < dataCmpBytes) {
              reader.getValue(j, scratch2);
              int cmp = FutureArrays.compareUnsigned(pivot.bytes, pivot.offset + dataStart, pivot.offset + dataEnd,
                  scratch2.bytes, scratch2.offset + dataStart, scratch2.offset + dataEnd);
              if (cmp != 0) {
                return cmp;
              }
            }
            return pivotDoc - reader.getDocID(j); // 通过文档大小相比较
          }
        };
      }

      @Override
      protected void swap(int i, int j) {
        reader.swap(i, j);
      }
      // 可以从maxLength=dataCmpBytes + (bitsPerDocId + 7) / 8可以看出，属于不同的读法，
      @Override
      protected int byteAt(int i, int k) { // 第i个元素，k表示相对位置
        if (k < dimCmpBytes) { // 读取的是dataCmpBytes中的数据
          return Byte.toUnsignedInt(reader.getByteAt(i, dimOffset + k));
        } else if (k < dataCmpBytes) {
          return Byte.toUnsignedInt(reader.getByteAt(i, dataOffset + k - dimCmpBytes));
        } else { // 读取的是docId的高位
          final int shift = bitsPerDocId - ((k - dataCmpBytes + 1) << 3); // 通过(k - dataCmpBytes)去掉原本影响，，每次向右移8位,比如第一次是22，第二次是14
          return (reader.getDocID(i) >>> Math.max(0, shift)) & 0xff; // 应该仅仅是为了hash，从docId中取值
        }
      }
    }.select(from, to, mid);
  }
}
