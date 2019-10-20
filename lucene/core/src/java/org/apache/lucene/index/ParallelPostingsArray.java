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
package org.apache.lucene.index;

import org.apache.lucene.util.ArrayUtil;
//
class ParallelPostingsArray {
  final static int BYTES_PER_POSTING = 3 * Integer.BYTES;

  final int size;// Array的长度
  final int[] textStarts; // maps term ID to the terms's text start in the bytesHash//是不变的, 记录的是整个termId在ByteBlockPool中的起始位置，等于FreqProxTermsWriterPerField.BytesRefHash.bytesStart。
  final int[] addressOffset; // maps term ID to current stream address// 下标为termId，记录了该term的利用intBoolBuffer中的那两个位置来存放term在byteBuffer中可用位置
  final int[] byteStarts; // maps term ID to stream start offset in the byte pool // 下标为下标为termId， 记录了该term在的两个byteSlice()中第一个slict的起始位置
  // 一个termId在byteBufferPool中主要存储了两大部分：termContent, 两个byteSlice。textStarts是从termContent就开始记录了。而byteStarts是从第一个byteSlice开始记录的。termContent记录的更完整。
  ParallelPostingsArray(final int size) {
    this.size = size;
    textStarts = new int[size]; // 都是会动态扩容的， 被赋值给了BytesRefHash.bytesStart
    addressOffset = new int[size];
    byteStarts = new int[size];
  }

  int bytesPerPosting() {
    return BYTES_PER_POSTING;
  }

  ParallelPostingsArray newInstance(int size) {
    return new ParallelPostingsArray(size);
  }

  final ParallelPostingsArray grow() {
    int newSize = ArrayUtil.oversize(size + 1, bytesPerPosting());
    ParallelPostingsArray newArray = newInstance(newSize);
    copyTo(newArray, size);
    return newArray;
  }

  void copyTo(ParallelPostingsArray toArray, int numToCopy) {
    System.arraycopy(textStarts, 0, toArray.textStarts, 0, numToCopy);
    System.arraycopy(addressOffset, 0, toArray.addressOffset, 0, numToCopy);
    System.arraycopy(byteStarts, 0, toArray.byteStarts, 0, numToCopy);
  }
}
