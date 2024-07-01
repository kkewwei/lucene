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
import org.apache.lucene.codecs.MutablePointTree;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.bkd.BKDConfig;
// 一个段相同域名只拥有一个
/** Buffers up pending byte[][] value(s) per doc, then flushes when segment flushes. */
class PointValuesWriter {
  private final FieldInfo fieldInfo;
  private final PagedBytes bytes; // 每个字段point值长度一样，没统计每个字段的范围
  private final DataOutput bytesOut;// 内存结构的
  private final Counter iwBytesUsed;
  private final SharedIndexingScratch sharedScratch;
  private int[] docIDs;// 按序累加，docIDs[i]表示：第i个point个值属于第几个文档。第0位存放20，第1位存放5，value按照大小排序。表示第0小value文档id=20
  private int numPoints; // 这个segment该字段包含的总value个数（相同value算两个）
  private int numDocs; //这个segment该字段已经写入了多少个文档。（point是经过distinct过后的）// 是一个从0开始递增的值，可以理解为是每一个点数据的一个唯一编号，并且通过这个编号能映射出该点数据属于哪一个文档(document)。映射关系则是通过docIDs[ ]数组实现。
  private int lastDocID = -1;// 最大文档编号
  private final int packedBytesLength; // 一个intPoint占用的空间

  private static final int POINTS_BUFFER_INT_VALUES =
      SharedIndexingScratch.BYTES_SCRATCH_SIZE / Integer.BYTES;
  private static final int POINTS_BUFFER_LONG_VALUES =
      SharedIndexingScratch.BYTES_SCRATCH_SIZE / Long.BYTES;

  static {
    // If Lucene ever supports packed points > BYTES_SCRATCH_SIZE either the buffer needs to be
    // increased or a fallback indexing code-path bypassing the buffer needs to be added.
    assert SharedIndexingScratch.BYTES_SCRATCH_SIZE
            >= PointValues.MAX_NUM_BYTES * BKDConfig.MAX_DIMS
        : "BYTES_SCRATCH_SIZE="
            + SharedIndexingScratch.BYTES_SCRATCH_SIZE
            + " must be >= MAX_NUM_BYTES * MAX_DIMS="
            + (PointValues.MAX_NUM_BYTES * BKDConfig.MAX_DIMS);
  }

  PointValuesWriter(Counter bytesUsed, FieldInfo fieldInfo, SharedIndexingScratch sharedScratch) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = bytesUsed;
    this.sharedScratch = sharedScratch;
    this.bytes = new PagedBytes(12);
    bytesOut = bytes.getDataOutput();
    docIDs = new int[16];// 某个point是属于第几个文档，要和ords结合用，ords记录的是第几个point跑到第几位了
    iwBytesUsed.addAndGet(16 * Integer.BYTES);
    packedBytesLength = fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes();
  }

  // TODO: if exactly the same value is added to exactly the same doc, should we dedup?
  public void addPackedValue(int docID, BytesRef value) throws IOException {// Point有多个值的话，都会堆砌到一个BytesRef中
    if (value == null) {
      throw new IllegalArgumentException(
          "field=" + fieldInfo.name + ": point value must not be null");
    }
    if (value.length != packedBytesLength) {
      throw new IllegalArgumentException(
          "field="
              + fieldInfo.name
              + ": this field's value has length="
              + value.length
              + " but should be "
              + (fieldInfo.getPointDimensionCount() * fieldInfo.getPointNumBytes()));
    }

    if (docIDs.length == numPoints) { // 文档数超了的话，就扩容
      docIDs = ArrayUtil.grow(docIDs, numPoints + 1);
      iwBytesUsed.addAndGet((docIDs.length - numPoints) * (long) Integer.BYTES);
    }
    final long bytesRamBytesUsedBefore = bytes.ramBytesUsed();
    bytesOut.writeBytes(value.bytes, value.offset, value.length); // 每个
    iwBytesUsed.addAndGet(bytes.ramBytesUsed() - bytesRamBytesUsedBefore);
    docIDs[numPoints] = docID;// 这个value属于哪个term
    if (docID != lastDocID) {
      numDocs++;
      lastDocID = docID;
    }

    numPoints++;
  }

  void addDense1DIntValues(int firstDocID, LongValuesCursor cursor) throws IOException {
    validate1DPacked(Integer.BYTES);
    final int size = cursor.size();
    if (size == 0) {
      return;
    }
    final long ramBefore = reserveDenseRange(firstDocID, size);
    final byte[] buffer = sharedScratch.bytesScratch();
    int remaining = size;
    while (remaining > 0) {
      int chunk = Math.min(POINTS_BUFFER_INT_VALUES, remaining);
      cursor.fillIntPoints(buffer, 0, chunk);
      bytesOut.writeBytes(buffer, 0, chunk * Integer.BYTES);
      remaining -= chunk;
    }
    commitDenseRange(firstDocID, size, ramBefore);
  }

  void addDense1DLongValues(int firstDocID, LongValuesCursor cursor) throws IOException {
    validate1DPacked(Long.BYTES);
    final int size = cursor.size();
    if (size == 0) {
      return;
    }
    final long ramBefore = reserveDenseRange(firstDocID, size);
    final byte[] buffer = sharedScratch.bytesScratch();
    int remaining = size;
    while (remaining > 0) {
      int chunk = Math.min(POINTS_BUFFER_LONG_VALUES, remaining);
      cursor.fillLongPoints(buffer, 0, chunk);
      bytesOut.writeBytes(buffer, 0, chunk * Long.BYTES);
      remaining -= chunk;
    }
    commitDenseRange(firstDocID, size, ramBefore);
  }

  void addDenseNDValues(int firstDocID, BytesRefValuesCursor cursor) throws IOException {
    final int size = cursor.size();
    if (size == 0) {
      return;
    }
    final long ramBefore = reserveDenseRange(firstDocID, size);
    final int width = packedBytesLength;
    final int perChunk = SharedIndexingScratch.BYTES_SCRATCH_SIZE / width;
    final byte[] buffer = sharedScratch.bytesScratch();
    int remaining = size;
    while (remaining > 0) {
      int chunk = Math.min(perChunk, remaining);
      cursor.fillPackedPoints(buffer, 0, chunk, width);
      bytesOut.writeBytes(buffer, 0, chunk * width);
      remaining -= chunk;
    }
    commitDenseRange(firstDocID, size, ramBefore);
  }

  private void validate1DPacked(int byteWidth) {
    if (fieldInfo.getPointDimensionCount() != 1 || fieldInfo.getPointNumBytes() != byteWidth) {
      throw new IllegalArgumentException(
          "field="
              + fieldInfo.name
              + ": 1D dense path requires pointDimensionCount=1 and pointNumBytes="
              + byteWidth
              + ", got pointDimensionCount="
              + fieldInfo.getPointDimensionCount()
              + " pointNumBytes="
              + fieldInfo.getPointNumBytes());
    }
  }

  private long reserveDenseRange(int firstDocID, int size) {
    assert firstDocID > lastDocID
        : "firstDocID=" + firstDocID + " must be > lastDocID=" + lastDocID;
    final int oldLength = docIDs.length;
    if (oldLength < numPoints + size) {
      docIDs = ArrayUtil.grow(docIDs, numPoints + size);
      iwBytesUsed.addAndGet((docIDs.length - oldLength) * (long) Integer.BYTES);
    }
    for (int i = 0; i < size; i++) {
      docIDs[numPoints + i] = firstDocID + i;
    }
    return bytes.ramBytesUsed();
  }

  private void commitDenseRange(int firstDocID, int size, long ramBefore) {
    iwBytesUsed.addAndGet(bytes.ramBytesUsed() - ramBefore);
    numDocs += size;
    lastDocID = firstDocID + size - 1;
    numPoints += size;
  }

  /**
   * Get number of buffered documents
   *
   * @return number of buffered documents
   */
  public int getNumDocs() {
    return numDocs;
  }

  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, PointsWriter writer)
      throws IOException {
    final PagedBytes.Reader bytesReader = bytes.freeze(false);
    MutablePointTree points =
        new MutablePointTree() {
          final int[] ords = new int[numPoints]; // 给每个包含point的域都赋值一个。若之后文档排序（桶交换快排），仅仅是对这个号排序
          int[] temp;

          {
            for (int i = 0; i < numPoints; ++i) {
              ords[i] = i; // 每个Point都会排个序
            }
          }

          @Override
          public long size() {
            return numPoints;
          }

          @Override
          public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
            final BytesRef scratch = new BytesRef();
            final byte[] packedValue = new byte[packedBytesLength];
            for (int i = 0; i < numPoints; i++) {// 遍历每个元素的值
              getValue(i, scratch);// 从bytePool中读取第几个value
              assert scratch.length == packedValue.length;
              System.arraycopy(scratch.bytes, scratch.offset, packedValue, 0, packedBytesLength);
              visitor.visit(getDocID(i), packedValue);
            }
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

          @Override
          public void getValue(int i, BytesRef packedValue) { // 读取第i个point的值
            final long offset = (long) packedBytesLength * ords[i]; // 这个文档的偏移量
            bytesReader.fillSlice(packedValue, offset, packedBytesLength);
          }

          @Override
          public byte getByteAt(int i, int k) {// 第i个元素的第k位
            final long offset = (long) packedBytesLength * ords[i] + k;
            return bytesReader.getByte(offset);// 从BytePool中读取offset
          }

          @Override
          public void save(int i, int j) {
            if (temp == null) {
              temp = new int[ords.length];
            }
            temp[j] = ords[i];
          }

          @Override
          public void restore(int i, int j) {
            if (temp != null) {
              System.arraycopy(temp, i, ords, i, j - i);
            }
          }
        };

    final PointValues.PointTree values;
    if (sortMap == null) {// 跑到这里
      values = points;
    } else {
      values = new MutableSortingPointValues(points, sortMap);
    }
    PointsReader reader =
        new PointsReader() {
          @Override
          public PointValues getValues(String fieldName) {
            if (fieldName.equals(fieldInfo.name) == false) {
              throw new IllegalArgumentException("fieldName must be the same");
            }
            return new PointValues() {
              @Override
              public PointTree getPointTree() throws IOException {
                return values;
              }

              @Override
              public byte[] getMinPackedValue() throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public byte[] getMaxPackedValue() throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int getNumDimensions() throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int getNumIndexDimensions() throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int getBytesPerDimension() throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long size() {
                throw new UnsupportedOperationException();
              }

              @Override
              public int getDocCount() {
                throw new UnsupportedOperationException();
              }
            };
          }

          @Override
          public void checkIntegrity() {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() {}
        };
    writer.writeField(fieldInfo, reader);// 进来，writer=Lucene86PointsWriter,flush时才初始化
  }

  static final class MutableSortingPointValues extends MutablePointTree {

    private final MutablePointTree in;
    private final Sorter.DocMap docMap;

    public MutableSortingPointValues(final MutablePointTree in, Sorter.DocMap docMap) {
      this.in = in;
      this.docMap = docMap;
    }

    @Override
    public long size() {
      return in.size();
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      in.visitDocValues(
          new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) throws IOException {
              visitor.visit(docMap.oldToNew(docID));
            }

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              visitor.visit(docMap.oldToNew(docID), packedValue);
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return visitor.compare(minPackedValue, maxPackedValue);
            }
          });
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

    @Override
    public void save(int i, int j) {
      in.save(i, j);
    }

    @Override
    public void restore(int i, int j) {
      in.restore(i, j);
    }
  }
}
