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

import java.io.IOException;

import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
 // 一个段相同域名只拥有一个
/** Buffers up pending byte[][] value(s) per doc, then flushes when segment flushes. */
class PointValuesWriter {
  private final FieldInfo fieldInfo;
  private final ByteBlockPool bytes; // 每个字段point值长度一样，没统计每个字段的范围
  private final Counter iwBytesUsed;
  private int[] docIDs; // 按序累加，docIDs[i]表示：第i个point个值属于第几个文档。第0位存放20，第1位存放5，value按照大小排序。表示第0小value文档id=20
  private int numPoints;  // 有时一个文档相同域，包含多个Point，这里是汇总Point个数（相同文档相同域算两个）
  private int numDocs; // 该字段已经写入了多少个文档。（point是经过distinct过后的）// 是一个从0开始递增的值，可以理解为是每一个点数据的一个唯一编号，并且通过这个编号能映射出该点数据属于哪一个文档(document)。映射关系则是通过docIDs[ ]数组实现。
  private int lastDocID = -1; // 最大文档编号
  private final int packedBytesLength; // 一个intPoint占用的空间

  public PointValuesWriter(DocumentsWriterPerThread docWriter, FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = docWriter.bytesUsed;
    this.bytes = new ByteBlockPool(docWriter.byteBlockAllocator);
    docIDs = new int[16];// 某个point是属于第几个文档，要和ords结合用，ords记录的是第几个point跑到第几位了
    iwBytesUsed.addAndGet(16 * Integer.BYTES);
    packedBytesLength = fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes();
  }

  // TODO: if exactly the same value is added to exactly the same doc, should we dedup?
  public void addPackedValue(int docID, BytesRef value) { // Point有多个值的话，都会堆砌到一个BytesRef中
    if (value == null) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": point value must not be null");
    }
    if (value.length != packedBytesLength) {
      throw new IllegalArgumentException("field=" + fieldInfo.name + ": this field's value has length=" + value.length + " but should be " + (fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes()));
    }

    if (docIDs.length == numPoints) { // 文档数超了的话，就扩容
      docIDs = ArrayUtil.grow(docIDs, numPoints+1);
      iwBytesUsed.addAndGet((docIDs.length - numPoints) * Integer.BYTES);
    }
    bytes.append(value); // 每个
    docIDs[numPoints] = docID;
    if (docID != lastDocID) {
      numDocs++;
      lastDocID = docID;
    }

    numPoints++;
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, PointsWriter writer) throws IOException {
    PointValues points = new MutablePointValues() {
      final int[] ords = new int[numPoints]; // 给每个包含point的域都赋值一个。若之后文档排序（桶交换快排），仅仅是对这个号排序
      {
        for (int i = 0; i < numPoints; ++i) {
          ords[i] = i; // 每个Point都会排个序
        }
      }

      @Override
      public void intersect(IntersectVisitor visitor) throws IOException {
        final BytesRef scratch = new BytesRef();
        final byte[] packedValue = new byte[packedBytesLength];
        for(int i=0;i<numPoints;i++) { // 遍历每个元素的值
          getValue(i, scratch); // 从bytePool中读取第几个value
          assert scratch.length == packedValue.length;
          System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedBytesLength);
          visitor.visit(getDocID(i), packedValue);
        }
      }

      @Override
      public long estimatePointCount(IntersectVisitor visitor) {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMinPackedValue() {
        throw new UnsupportedOperationException();
      }

      @Override
      public byte[] getMaxPackedValue() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumDimensions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumIndexDimensions() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getBytesPerDimension() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long size() {
        return numPoints;
      }

      @Override
      public int getDocCount() {
        return numDocs;
      }

      @Override
      public void swap(int i, int j) {
        int tmp = ords[i];
        ords[i] = ords[j];
        ords[j] = tmp;
      }
      // 第0位存放20，第1位存放5，value按照大小排序，第0小value文档id=20
      @Override
      public int getDocID(int i) { // 返回文档ID, i是第几个写入的
        return docIDs[ords[i]];//ords记录的是第几个point跑到第几位了，docIDs某个point是属于第几个文档，要和ords结合用，
      }

      @Override // 读取第i个point的值
      public void getValue(int i, BytesRef packedValue) {
        final long offset = (long) packedBytesLength * ords[i]; // 这个文档的偏移量
        packedValue.length = packedBytesLength;
        bytes.setRawBytesRef(packedValue, offset);
      }

      @Override
      public byte getByteAt(int i, int k) { // 第i个元素的第k位
        final long offset = (long) packedBytesLength * ords[i] + k;
        return bytes.readByte(offset); // 从BytePool中读取offset
      }
    };

    final PointValues values;
    if (sortMap == null) { // 跑到这里
      values = points; //
    } else {
      values = new MutableSortingPointValues((MutablePointValues) points, sortMap);
    }
    PointsReader reader = new PointsReader() {
      @Override
      public PointValues getValues(String fieldName) {
        if (fieldName.equals(fieldInfo.name) == false) {
          throw new IllegalArgumentException("fieldName must be the same");
        }
        return values;
      }

      @Override
      public void checkIntegrity() {
        throw new UnsupportedOperationException();
      }

      @Override
      public long ramBytesUsed() {
        return 0L;
      }

      @Override
      public void close() {
      }
    };
    writer.writeField(fieldInfo, reader); // 进来，writer=Lucene86PointsWriter,flush时才初始化
  }

  static final class MutableSortingPointValues extends MutablePointValues {

    private final MutablePointValues in;
    private final Sorter.DocMap docMap;

    public MutableSortingPointValues(final MutablePointValues in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public void intersect(IntersectVisitor visitor) throws IOException {
      in.intersect(new IntersectVisitor() {
        @Override
        public void visit(int docID) throws IOException {
          visitor.visit(docMap.oldToNew(docID));
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
          visitor.visit(docMap.oldToNew(docID), packedValue);
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
          return visitor.compare(minPackedValue, maxPackedValue);
        }
      });
    }

    @Override
    public long estimatePointCount(IntersectVisitor visitor) {
      return in.estimatePointCount(visitor);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDimensions() throws IOException {
      return in.getNumDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      return in.size();
    }

    @Override
    public int getDocCount() {
      return in.getDocCount();
    }

    @Override
    public void getValue(int i, BytesRef packedValue) {
      in.getValue(i, packedValue);
    }

    @Override
    public byte getByteAt(int i, int k) {
      return in.getByteAt(i, k);
    }

    @Override
    public int getDocID(int i) {
      return docMap.oldToNew(in.getDocID(i));
    }

    @Override
    public void swap(int i, int j) {
      in.swap(i, j);
    }
  }
}
