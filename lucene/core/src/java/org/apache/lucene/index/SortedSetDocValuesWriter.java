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
import org.apache.lucene.document.column.OrdinalsTupleCursor;
import org.apache.lucene.index.SortedDocValuesWriter.BufferedSortedDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Buffers up pending byte[]s per doc, deref and sorting via int ord, then flushes when segment
 * flushes.
 */// 一个docId可以拥有这个字段多个value
class SortedSetDocValuesWriter extends DocValuesWriter<SortedSetDocValues> {
  final BytesRefHash hash;// 真正存放value值的地方，每个value都是唯一的
  private PackedLongValues.Builder pending; // stream of all termIDs 存放的事termIds，和pendingCounts配合使用，pendingCounts存放文档id=2的value有4个词，具体的四个value放在currentValues中（重复的termId会过滤掉）
  private PackedLongValues.Builder pendingCounts; //termIDs per doc pendingCounts[2]=4  文档id=2的value有4个
  private DocsWithFieldSet docsWithField; //  （每次flush使用一个，档案文档id）写一个文档的域，存放一个docId。写入的docId不一定是连续的
  private final Counter iwBytesUsed;// 最终使用的都是同一个
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc = -1;//  正在处理的文档编号
  private int[] currentValues = new int[8];//临时变量：和pendingCounts配合使用，pendingCounts存放文档id=2的value有4个词，具体的四个value放在currentValues中
  private int currentUpto; // 当前文档当前域存放的第几个词（重复的词算两个），作为currentValues的下标。每写完一个文档的一个域，就清0，把数据转到pending中了，currentValues数据就全部丢失了
  private int maxCount;
  private final SharedIndexingScratch scratch;
  // finalOrdCounts可以知道每个doc的term个数，具体每个doc的termId存放在finalOrds中。finalSortedValues和finalOrdMap是termId和term大小排序的map
  private PackedLongValues finalOrds;// 和finalOrdCounts配合使用，会存放第0个文档的五个termId
  private PackedLongValues finalOrdCounts;// finalOrdCounts[0]=5, 第0个doc，有5个不同的term。。可以为null，说明每个doc只有一个value
  private int[] finalSortedValues;// finalSortedValues和finalOrdMap是相反的。finalSortedValues[6]=0表示term大小排序第6，是第0个写入的term
  private int[] finalOrdMap;//finalOrdMap[0]=6, 第0个写入的词，大小排序排第6

  SortedSetDocValuesWriter(
      FieldInfo fieldInfo, Counter iwBytesUsed, ByteBlockPool pool, SharedIndexingScratch scratch) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    this.scratch = scratch;
    hash =
        new BytesRefHash(
            pool,
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.packedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed =
        pending.ramBytesUsed()
            + docsWithField.ramBytesUsed()
            + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(bytesUsed);
  }
  // 在拆分文档每个域的时候会直接进来，因为分词和docvalue是两个冲突的事情
  public void addValue(int docID, BytesRef value) {
    assert docID >= currentDoc;
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
    // 一个文档一般只有一个同名的域，下面一定不相等
    if (docID != currentDoc) { // 该doc第一次写入, 将上次的给存储起来
      finishCurrentDoc();
      currentDoc = docID;
    }
    // 在finishCurrentDoc里面已经将
    addOneValue(value); //将value存放到currentValues中
    updateBytesUsed();
  }

  /**
   * Bulk-adds dictionary-encoded values from a tuple cursor. Each {@code (docID, ordinal)} pair is
   * translated to the writer's internal hash term ID on first sight per distinct ordinal;
   * subsequent docs that use the same ordinal pay only an array lookup.
   *
   * <p>All ordinals must be in {@code [0, dictionary.length)}. Doc-ids from the cursor are
   * batch-local and are offset by {@code baseDocID} to produce segment-level ids.
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
      assert docID >= currentDoc;
      if (docID != currentDoc) {
        finishCurrentDoc();
        currentDoc = docID;
      }
      int ord = cursor.ordValue();
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
      int hashID = ordToHash[ord];
      if (hashID < 0) {
        hashID = hash.add(dictionary.get(ord));
        if (hashID < 0) {
          hashID = -hashID - 1;
        } else {
          iwBytesUsed.addAndGet(2 * Integer.BYTES);
        }
        ordToHash[ord] = hashID;
      }
      if (currentUpto == currentValues.length) {
        currentValues = ArrayUtil.grow(currentValues, currentValues.length + 1);
        iwBytesUsed.addAndGet((currentValues.length - currentUpto) * (long) Integer.BYTES);
      }
      currentValues[currentUpto] = hashID;
      currentUpto++;
    }
    updateBytesUsed();
  }
  // 开始在lucene拆分文档和flush时候都会调用，在flush时候用的目的是最终把缓存给刷新pending中。
  // finalize currentDoc: this deduplicates the current term ids
  private void finishCurrentDoc() { // 把上个doc的值给存储起来
    if (currentDoc == -1) { // 目前是提交commit时候内存中的文档数
      return;
    }
    if (currentUpto > 1) {
      Arrays.sort(currentValues, 0, currentUpto);// 按照termId排序,仅仅是为了下面存储时去掉重复的value。针对对value进行排序是在
    }
    int lastValue = -1;
    int count = 0; // 一个文档中，
    for (int i = 0; i < currentUpto; i++) { // 最大只能为0
      int termID = currentValues[i];
      // if it's not a duplicate
      if (termID != lastValue) {// 压缩存储的时候重复存储算一个
        pending.add(termID); // record the term id
        count++;
      }
      lastValue = termID;
    }
    // record the number of unique term ids for this doc 会压缩存储
    if (pendingCounts != null) {
      pendingCounts.add(count);//该文档该域有几个value, 每个value是存在pending中的
    } else if (count != 1) {
      pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
      for (int i = 0; i < docsWithField.cardinality(); ++i) {
        pendingCounts.add(1);
      }
      pendingCounts.add(count);
    }
    maxCount = Math.max(maxCount, count);
    currentUpto = 0; // 每写完一个文档就清0了
    docsWithField.add(currentDoc); // 正在处理的文档编号
  }// 根据docsWithField记录文档id,右pendingCounts记录每个文档有多少个词，然后依次从pending找到对应的termId

  private void addOneValue(BytesRef value) {
    int termID = hash.add(value); // hash本类会专门产生一个, 整个域作为一个词来获取termId
    if (termID < 0) {
      termID = -termID - 1;
    } else {
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * Integer.BYTES);
    }

    if (currentUpto == currentValues.length) {// 满了就扩容
      currentValues = ArrayUtil.grow(currentValues, currentValues.length + 1);
      iwBytesUsed.addAndGet((currentValues.length - currentUpto) * (long) Integer.BYTES);
    }

    currentValues[currentUpto] = termID;// 在finishCurrentDoc中会存储到pending中
    currentUpto++;
  }

  private void updateBytesUsed() {
    final long newBytesUsed =
        pending.ramBytesUsed()
            + (pendingCounts == null ? 0 : pendingCounts.ramBytesUsed())
            + docsWithField.ramBytesUsed()
            + RamUsageEstimator.sizeOf(currentValues);
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  private void finish() {
    if (finalOrds == null) {
      assert finalOrdCounts == null && finalSortedValues == null && finalOrdMap == null;
      finishCurrentDoc();
      int valueCount = hash.size();// 总共多少个value,
      finalOrds = pending.build();
      finalOrdCounts = pendingCounts == null ? null : pendingCounts.build();// 每个文档的term个数
      finalSortedValues = hash.sort();// 对sortValue进行排序，finalSortedValues[0]=5表示大小排最小的词，termId=5
      finalOrdMap = new int[valueCount];
      for (int ord = 0; ord < finalOrdMap.length; ord++) {
        finalOrdMap[finalSortedValues[ord]] = ord;//finalOrdMap[0]=6, 第0个写入的词，大小排序排第6
      }
    }
  }

  @Override
  SortedSetDocValues getDocValues() {
    finish();
    return getValues(
        finalSortedValues, finalOrdMap, hash, finalOrds, finalOrdCounts, maxCount, docsWithField);
  }

  private SortedSetDocValues getValues(
      int[] sortedValues,
      int[] ordMap,
      BytesRefHash hash,
      PackedLongValues ords,
      PackedLongValues ordCounts,
      int maxCount,
      DocsWithFieldSet docsWithField) {
    if (ordCounts == null) {// 说明每个doc只有一个value
      return DocValues.singleton(
          new BufferedSortedDocValues(hash, ords, sortedValues, ordMap, docsWithField.iterator()));
    } else {
      return new BufferedSortedSetDocValues(
          sortedValues, ordMap, hash, ords, ordCounts, maxCount, docsWithField.iterator());
    }
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer)
      throws IOException {
    finish();
    final PackedLongValues ords = finalOrds;// 和pendingCounts配合使用。
    final PackedLongValues ordCounts = finalOrdCounts;// pendingCounts[2]=4  第2个写入文档有4个词。可以为null，说明每个doc只有一个value
    final int[] sortedValues = finalSortedValues;// 按照byte排序。sortedValues[3]=2: 大小排第3的termId=2
    final int[] ordMap = finalOrdMap;//  termsId顺序（与写入顺序还有些区别，比如ordMap[0]=5, termId为0的term, 大小排序第5

    if (ordCounts == null) {// 可以为null，说明每个doc只有一个value
      DocValuesProducer singleValueProducer =
          SortedDocValuesWriter.getDocValuesProducer(
              fieldInfo, hash, ords, sortedValues, ordMap, docsWithField, sortMap);
      dvConsumer.addSortedSetField(
          fieldInfo,
          new EmptyDocValuesProducer() {
            @Override
            public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
              return DocValues.singleton(singleValueProducer.getSorted(fieldInfo));
            }
          });
      return;
    }

    final DocOrds docOrds;//下面说明每个doc只有不止有value
    if (sortMap != null) {// 为null
      docOrds =
          new DocOrds(
              state.segmentInfo.maxDoc(),
              sortMap,
              getValues(sortedValues, ordMap, hash, ords, ordCounts, maxCount, docsWithField),
              PackedInts.FASTEST,
              PackedInts.bitsRequired(maxCount));
    } else {// 进来
      docOrds = null;
    }
    dvConsumer.addSortedSetField(//真正写入docValue相关信息, 跑到PerFieldDocValuesFormat$FieldsWriter
        fieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedSetDocValues getSortedSet(FieldInfo fieldInfoIn) {
            if (fieldInfoIn != fieldInfo) {
              throw new IllegalArgumentException("wrong fieldInfo");
            }
            final SortedSetDocValues buf = // distinct(doc词)的个数,
                getValues(sortedValues, ordMap, hash, ords, ordCounts, maxCount, docsWithField);
            if (docOrds == null) {
              return buf;
            } else {
              return new SortingSortedSetDocValues(buf, docOrds);
            }
          }
        });
  }

  private static class BufferedSortedSetDocValues extends SortedSetDocValues {
    final int[] sortedValues;// 按照byte排序。sortedValues[3]=2:下标是小排到大的序号，sortedValues[3]=2: 大小排第3的termId=2，第2个写入的term
    final int[] ordMap; // // ordMap是写入顺序->元素是排第几的大小。ordMap[3]=2就是写入时第三个的元素，大小排第二。
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();
    final PackedLongValues.Iterator ordsIter;//   ordsIter[8]=5 第8个写入的词的termId=5
    final PackedLongValues.Iterator ordCountsIter; // ordCountsIter[2]=4  第2个写入文档有4个词
    final DocIdSetIterator docsWithField;// docsWithField[2]=5, 第2个写入的是文档id为5
    final int[] currentDoc;// 当前文档每个词的大小排序, 临时使用：currentDoc[3]=4 termId=3的大小排序第4。

    private int ordCount;//当前文档，总共多少词
    private int ordUpto;// 遍历到当前文档的第几个词

    BufferedSortedSetDocValues(// 只要某个文档value个数>1, 就用这个存储
        int[] sortedValues,
        int[] ordMap,
        BytesRefHash hash,
        PackedLongValues ords,
        PackedLongValues ordCounts,// 表示每个doc包含的term个数
        int maxCount,
        DocIdSetIterator docsWithField) {
      this.currentDoc = new int[maxCount];
      this.sortedValues = sortedValues; // sortedValues[3]=2: 排在第3位的次的termId为2（terdId和写入顺序不一致）
      this.ordMap = ordMap; //   termsId顺序（与写入顺序还有些区别，比如ordMap[0]=5, termId为0的term, 大小排序第5
      this.hash = hash;
      this.ordsIter = ords.iterator(); //   ordsIter[5]=8 第5个写入的词的termId=8，和sortedValues含义一样
      this.ordCountsIter = ordCounts.iterator(); //   ordCountsIter[2]=4  第2个写入文档有4个词
      this.docsWithField = docsWithField;//  docsWithField[2]=5, 第2个写入的是文档id为5
    }// // 根据docsWithField记录文档id, pendingCounts记录每个文档有多少个词，然后依次从pending找到对应的termId
    // 1.写入顺序->termId顺序: ordsIter;  2.termID顺序->大小顺序：ordMap；3.大小顺序->termId: sortedValues   4.文档顺序->文档Id:docsWithField;   5. 文档对应顺序->term个数： ordCountsIter
    @Override
    public int docID() {
      return docsWithField.docID();
    }
    // nextDoc和nextOrd是配合使用的，首先获取到docID，然后获取这个doc对应termd，对应大小排序值，放入currentDoc
    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc(); // 下一个文档的id号码
      if (docID != NO_MORE_DOCS) {
        ordCount = (int) ordCountsIter.next(); // 该文档改字段term的个数，
        assert ordCount > 0;
        for (int i = 0; i < ordCount; i++) {//然后遍历该文档所有同名域的termID
          currentDoc[i] = ordMap[Math.toIntExact(ordsIter.next())]; // 首先获得termId, 然后存放词大小排第几
        }//currentDoc[3]=4 这个域的第三个value的term排第4位
        Arrays.sort(currentDoc, 0, ordCount); // 大小排序，第0个词，大小排序第5，
        ordUpto = 0;
      }
      return docID;
    }

    @Override
    public long nextOrd() {//该文档的下一个词
      return currentDoc[ordUpto++];
    }// 排序后的值

    @Override
    public int docValueCount() {
      return ordCount;
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
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getValueCount() {
      return ordMap.length;
    }

    @Override
    public BytesRef lookupOrd(long ord) {// ord：第几小的词
      assert ord >= 0 && ord < ordMap.length
          : "ord=" + ord + " is out of bounds 0 .. " + (ordMap.length - 1);
      hash.get(sortedValues[Math.toIntExact(ord)], scratch); // 按照termid对应的term顺序排序后的
      return scratch;
    }
  }

  static class SortingSortedSetDocValues extends SortedSetDocValues {

    private final SortedSetDocValues in;
    private final DocOrds ords;
    private int docID = -1;
    private long ordUpto;
    private int count;

    SortingSortedSetDocValues(SortedSetDocValues in, DocOrds ords) {
      this.in = in;
      this.ords = ords;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      do {
        docID++;
        if (docID == ords.offsets.length) {
          return docID = NO_MORE_DOCS;
        }
      } while (ords.offsets[docID] <= 0);
      initCount();
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
      initCount();
      return ords.offsets[docID] > 0;
    }

    @Override
    public long nextOrd() {
      return ords.ords.get(ordUpto++);
    }

    @Override
    public int docValueCount() {
      assert docID >= 0;
      return count;
    }

    @Override
    public long cost() {
      return in.cost();
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
      return in.lookupOrd(ord);
    }

    @Override
    public long getValueCount() {
      return in.getValueCount();
    }

    private void initCount() {
      assert docID >= 0;
      ordUpto = ords.offsets[docID] - 1;
      count = (int) ords.docValueCounts.get(docID);
    }
  }

  static final class DocOrds {
    final long[] offsets;
    final PackedLongValues ords;
    final GrowableWriter docValueCounts;

    public static final int START_BITS_PER_VALUE = 2;

    DocOrds(
        int maxDoc,
        Sorter.DocMap sortMap,
        SortedSetDocValues oldValues,
        float acceptableOverheadRatio,
        int bitsPerValue)
        throws IOException {
      offsets = new long[maxDoc];
      PackedLongValues.Builder builder = PackedLongValues.packedBuilder(acceptableOverheadRatio);
      docValueCounts = new GrowableWriter(bitsPerValue, maxDoc, acceptableOverheadRatio);
      long ordOffset = 1;
      int docID;
      while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
        int newDocID = sortMap.oldToNew(docID);
        long startOffset = ordOffset;
        int docValueCount = oldValues.docValueCount();
        ordOffset += docValueCount;
        for (int i = 0; i < docValueCount; i++) {
          builder.add(oldValues.nextOrd());
        }
        docValueCounts.set(newDocID, ordOffset - startOffset);
        if (startOffset != ordOffset) { // do we have any values?
          offsets[newDocID] = startOffset;
        }
      }
      ords = builder.build();
    }
  }
}
