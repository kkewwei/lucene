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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.column.OrdinalsCursor;
import org.apache.lucene.document.column.OrdinalsTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
// 每个docId只能拥有一个value
/**
 * Buffers up pending byte[] per doc, deref and sorting via int ord, then flushes when segment
 * flushes.
 */
class SortedDocValuesWriter extends DocValuesWriter<SortedDocValues> {// DocValuesWriter仅能否针对不分词的字段设置
  final BytesRefHash hash;// 存储的是真正每个field的值
  private final PackedLongValues.Builder pending;// 是写入顺序
  private final DocsWithFieldSet docsWithField;
  private final Counter iwBytesUsed;
  private long bytesUsed; // this currently only tracks differences in 'pending'
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;
  private final SharedIndexingScratch scratch;

  private PackedLongValues finalOrds; // // 第7个写入的词的termId=2
  private int[] finalSortedValues;// 对所有的terms内容进行了排序，。 value[1]=10 ,词大小排序order=1的，是term10
  private int[] finalOrdMap;// finalSortedValues和finalOrdMap含义想法。 finalOrdMap[5]=2, termId=5,大小排序是2

  public SortedDocValuesWriter(
      FieldInfo fieldInfo, Counter iwBytesUsed, ByteBlockPool pool, SharedIndexingScratch scratch) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.scratch = scratch;
    hash =
        new BytesRefHash(
            pool,
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID <= lastDocID) {// 每个docId只能拥有一个value
      throw new IllegalArgumentException(
          "DocValuesField \""
              + fieldInfo.name
              + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException(
          "field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException(
          "DocValuesField \""
              + fieldInfo.name
              + "\" is too large, must be <= "
              + (BYTE_BLOCK_SIZE - 2));
    }

    addOneValue(value);
    docsWithField.add(docID);

    lastDocID = docID;
  }

  private void addOneValue(BytesRef value) {
    int termID = hash.add(value);// 写入顺序id, 针对整个value，存起来
    if (termID < 0) { // 该value已经存在
      termID = -termID - 1;
    } else {// 该value不存在
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * Integer.BYTES); // 为每个单独的value保留2个空间
    }

    pending.add(termID);
    updateBytesUsed();
  }

  /**
   * Bulk-adds dictionary-encoded values from a tuple cursor. Each {@code (docID, ordinal)} pair is
   * translated to the writer's internal hash term ID on first sight per distinct ordinal;
   * subsequent docs that use the same ordinal pay only an array lookup. Doc-ids from the cursor are
   * batch-local and are offset by {@code baseDocID} to produce segment-level ids; they must be
   * strictly increasing (at most one value per doc).
   *
   * <p>All ordinals must be in {@code [0, dictionary.length)}.
   */
  void addOrdinalTuples(int baseDocID, List<BytesRef> dictionary, OrdinalsTupleCursor cursor) {
    int dictSize = dictionary.size();
    int[] ordToHash =
        dictSize <= SharedIndexingScratch.INTS_SCRATCH_SIZE
            ? scratch.intsScratch()
            : new int[dictSize];
    Arrays.fill(ordToHash, 0, dictSize, -1);
    int batchDocID;
    while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      int docID = baseDocID + batchDocID;
      if (docID <= lastDocID) {
        throw new IllegalArgumentException(
            "DocValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      int ord = cursor.ordValue();
      int id = lookupOrTranslate(ord, dictionary, ordToHash);
      pending.add(id);
      docsWithField.add(docID);
      lastDocID = docID;
    }
    updateBytesUsed();
  }

  /**
   * Bulk-adds one dictionary-encoded value per consecutive doc-id starting at {@code firstDocID}.
   * The cursor provides exactly one ordinal per doc; all ordinals must be in {@code [0,
   * dictionary.length)}.
   *
   * <p>This path performs one {@code BytesRefHash} lookup per distinct used dictionary entry rather
   * than one per document.
   */
  void addDenseOrdinalValues(int firstDocID, List<BytesRef> dictionary, OrdinalsCursor cursor) {
    int n = cursor.size();
    if (n == 0) {
      return;
    }
    assert firstDocID > lastDocID;
    int dictSize = dictionary.size();
    int[] ordToHash =
        dictSize <= SharedIndexingScratch.INTS_SCRATCH_SIZE
            ? scratch.intsScratch()
            : new int[dictSize];
    Arrays.fill(ordToHash, 0, dictSize, -1);
    int processed = 0;
    try {
      while (processed < n) {
        int ord = cursor.nextOrd();
        int id = lookupOrTranslate(ord, dictionary, ordToHash);
        pending.add(id);
        processed++;
      }
    } finally {
      if (processed > 0) {
        docsWithField.addRange(firstDocID, firstDocID + processed);
        lastDocID = firstDocID + processed - 1;
      }
      updateBytesUsed();
    }
  }

  private int lookupOrTranslate(int ord, List<BytesRef> dictionary, int[] ordToHash) {
    if (ord < 0 || ord >= dictionary.size()) {
      throw new IllegalArgumentException(
          "DocValuesField \""
              + fieldInfo.name
              + "\": ordinal "
              + ord
              + " is out of range [0, "
              + dictionary.size()
              + ")");
    }
    int id = ordToHash[ord];
    if (id < 0) {
      id = hash.add(dictionary.get(ord));
      if (id < 0) {
        id = -id - 1;
      } else {
        iwBytesUsed.addAndGet(2 * Integer.BYTES);
      }
      ordToHash[ord] = id;
    }
    return id;
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  private void finish() {
    if (finalSortedValues == null) {
      int valueCount = hash.size();
      updateBytesUsed();
      assert finalOrdMap == null && finalOrds == null;
      finalSortedValues = hash.sort();// 对所有的terms内容进行了排序，。 value[1]=10 ,词order=1的，是term10
      finalOrds = pending.build();// finalOrds[7]=2, 第7个写入的词的termId=2
      finalOrdMap = new int[valueCount];
      for (int ord = 0; ord < valueCount; ord++) {
        finalOrdMap[finalSortedValues[ord]] = ord;
      }
    }
  }

  @Override
  SortedDocValues getDocValues() {
    finish();
    return new BufferedSortedDocValues(
        hash, finalOrds, finalSortedValues, finalOrdMap, docsWithField.iterator());
  }

  private static int[] sortDocValues(int maxDoc, Sorter.DocMap sortMap, SortedDocValues oldValues)
      throws IOException {
    int[] ords = new int[maxDoc];
    Arrays.fill(ords, -1);
    int docID;
    while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
      int newDocID = sortMap.oldToNew(docID);
      ords[newDocID] = oldValues.ordValue();
    }
    return ords;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer)
      throws IOException {
    finish();

    dvConsumer.addSortedField(
        fieldInfo,
        getDocValuesProducer(
            fieldInfo, hash, finalOrds, finalSortedValues, finalOrdMap, docsWithField, sortMap));
  }

  static DocValuesProducer getDocValuesProducer(
      FieldInfo writerFieldInfo,
      BytesRefHash hash,
      PackedLongValues ords,//
      int[] sortedValues,//sortedValues[3]=2: 大小排第3的termId=2
      int[] ordMap, // // 和sortedValues含义相反。ordMap[2]=3, termId=2的词，大小排第3
      DocsWithFieldSet docsWithField,// docsWithField[2]=5, 第2个写入的是文档id为5。因为写入的docId不一定是连续的
      Sorter.DocMap sortMap)
      throws IOException {
    final int[] sorted;
    if (sortMap != null) {
      sorted =
          sortDocValues(
              sortMap.size(),
              sortMap,
              new BufferedSortedDocValues(
                  hash, ords, sortedValues, ordMap, docsWithField.iterator()));
    } else {
      sorted = null;
    }
    return new EmptyDocValuesProducer() {
      @Override
      public SortedDocValues getSorted(FieldInfo fieldInfoIn) {
        if (fieldInfoIn != writerFieldInfo) {
          throw new IllegalArgumentException("wrong fieldInfo");
        }
        final SortedDocValues buf =
            new BufferedSortedDocValues(hash, ords, sortedValues, ordMap, docsWithField.iterator());
        if (sorted == null) {// 没有对field排排序
          return buf;
        }
        return new SortingSortedDocValues(buf, sorted);
      }
    };
  }

  static class BufferedSortedDocValues extends SortedDocValues {// 说明每个doc只有一个value
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();
    final int[] sortedValues;// 对所有的terms内容进行了排序，记录词的orderId->词的写入termsId
    final int[] ordMap;// // sortedValues和ordMap含义相反 finalOrdMap[5]=2,// ordMap[5]=2, termId=5,，大小排序是2
    private int ord;
    final PackedLongValues.Iterator iter;// 按写入docId循序，给每个写入词一个编码，没遇到的，order编号+1。顺着docId顺序，获取对应的docId的termId
    final DocIdSetIterator docsWithField; // 写入的docId不一定是连续的

    public BufferedSortedDocValues(// 说明每个doc只有一个value
        BytesRefHash hash,
        PackedLongValues docToOrd,
        int[] sortedValues,// sortedValues[1]=10 ,词大小排序order=1的，是term10
        int[] ordMap,//和sortedValues含义相反
        DocIdSetIterator docsWithField) {
      this.hash = hash;
      this.sortedValues = sortedValues;// 对所有的terms内容进行了排序
      this.iter = docToOrd.iterator();
      this.ordMap = ordMap;
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {// 先获取nextDoc，在获取具体的ord
      int docID = docsWithField.nextDoc();
      if (docID != NO_MORE_DOCS) {
        ord = Math.toIntExact(iter.next());//iter.next()返回的是词的termid，
        ord = ordMap[ord];// 然后再映射，获取的大小排序的order
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }

    @Override
    public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
      docsWithField.intoBitSet(upTo, bitSet, offset);
    }

    @Override
    public int docIDRunEnd() throws IOException {
      return docsWithField.docIDRunEnd();
    }

    @Override
    public int ordValue() {
      return ord; // 返回词的排序order
    }

    @Override
    public BytesRef lookupOrd(int ord) { //order获取具体的值
      assert ord >= 0 && ord < sortedValues.length;
      assert sortedValues[ord] >= 0 && sortedValues[ord] < sortedValues.length;
      hash.get(sortedValues[ord], scratch);
      return scratch;
    }

    @Override
    public int getValueCount() {
      return hash.size();
    }
  }

  static class SortingSortedDocValues extends SortedDocValues {

    private final SortedDocValues in;
    private final int[] ords;
    private int docID = -1;

    SortingSortedDocValues(SortedDocValues in, int[] ords) {
      this.in = in;
      this.ords = ords;
      assert ords != null;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      while (true) {
        docID++;
        if (docID == ords.length) {
          docID = NO_MORE_DOCS;
          break;
        }
        if (ords[docID] != -1) {
          break;
        }
        // skip missing docs
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("use nextDoc instead");
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      // needed in IndexSorter#StringSorter
      docID = target;
      return ords[target] != -1;
    }

    @Override
    public int ordValue() {
      return ords[docID];
    }

    @Override
    public long cost() {
      return in.cost();
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return in.getValueCount();
    }
  }
}
