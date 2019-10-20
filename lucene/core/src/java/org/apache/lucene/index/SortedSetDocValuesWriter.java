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
import java.util.Arrays;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;
// 所有文档相同域将都共享这一个该对象
/** Buffers up pending byte[]s per doc, deref and sorting via
 *  int ord, then flushes when segment flushes. */
class SortedSetDocValuesWriter extends DocValuesWriter<SortedSetDocValues> {
  final BytesRefHash hash;// 真正存放value值的地方，每个value都是唯一的
  private PackedLongValues.Builder pending; // stream of all termIDs 存储每个文档每个域termId，相同Id的算两个。在未刷新前，所有文档相同域的值都会放到这里
  private PackedLongValues.Builder pendingCounts; // termIDs per doc 对一个文档所有域弄完docValue后，统计这个文档这个域的个数。pendingCounts统计的是每个文档中同名字段的个数，而pending统计的是每个同名字段的termId
  private DocsWithFieldSet docsWithField; // 写一个文档的域，存放一个docId
  private final Counter iwBytesUsed; // 最终使用的都是同一个
  private long bytesUsed; // this only tracks differences in 'pending' and 'pendingCounts'
  private final FieldInfo fieldInfo;
  private int currentDoc = -1; // 正在处理每个doc的field
  private int currentValues[] = new int[8]; // 下标就是个递增的编号，值是对DocValue里面的value分配的一个termId，根据这个id去hash里面映射value。默认
  private int currentUpto; // 当前文档当前域存放的第几个词（重复的词算两个，一般一个文档一个域只有一个词，排除），作为currentValues的下标。每写完一个文档的一个域，就清0，把数据转到pending中了，currentValues数据就全部丢失了
  private int maxCount;
  //
  private PackedLongValues finalOrds;
  private PackedLongValues finalOrdCounts;
  private int[] finalSortedValues;
  private int[] finalOrdMap;


  public SortedSetDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash = new BytesRefHash(
        new ByteBlockPool(
            new ByteBlockPool.DirectTrackingAllocator(iwBytesUsed)),
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.packedBuilder(PackedInts.COMPACT);
    pendingCounts = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }
  // 在拆分文档每个域的时候会直接进来，因为分词和docvalue是两个冲突的事情
  public void addValue(int docID, BytesRef value) {
    assert docID >= currentDoc;
    if (value == null) {
      throw new IllegalArgumentException("field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException("DocValuesField \"" + fieldInfo.name + "\" is too large, must be <= " + (BYTE_BLOCK_SIZE - 2));
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
  // 开始在lucene拆分文档和flush时候都会调用，在flush时候用的目的是最终把缓存给刷新pending中。
  // finalize currentDoc: this deduplicates the current term ids
  private void finishCurrentDoc() { // 把上个doc的值给存储起来
    if (currentDoc == -1) { // 目前是提交commit时候内存中的文档数
      return;
    }
    Arrays.sort(currentValues, 0, currentUpto); // 按照termId排序
    int lastValue = -1;
    int count = 0; // 一个文档中，同名的域有几个
    for (int i = 0; i < currentUpto; i++) { // 最大只能为0
      int termID = currentValues[i];
      // if it's not a duplicate
      if (termID != lastValue) {
        pending.add(termID); // record the term id
        count++;
      }
      lastValue = termID;
    }
    // record the number of unique term ids for this doc
    pendingCounts.add(count);
    maxCount = Math.max(maxCount, count);
    currentUpto = 0; // 这里清0了
    docsWithField.add(currentDoc); // 存起来
  }

  private void addOneValue(BytesRef value) {
    int termID = hash.add(value); // hash本类会专门产生一个, 整个域作为一个词来获取termId
    if (termID < 0) {
      termID = -termID-1;
    } else {
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * Integer.BYTES);
    }
    
    if (currentUpto == currentValues.length) { // 满了就扩容
      currentValues = ArrayUtil.grow(currentValues, currentValues.length+1);
      iwBytesUsed.addAndGet((currentValues.length - currentUpto) * Integer.BYTES);
    }
    
    currentValues[currentUpto] = termID; // 在finishCurrentDoc中会存储到pending中
    currentUpto++;
  }
  
  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + pendingCounts.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  private long[][] sortDocValues(int maxDoc, Sorter.DocMap sortMap, SortedSetDocValues oldValues) throws IOException {
    long[][] ords = new long[maxDoc][];
    int docID;
    while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
      int newDocID = sortMap.oldToNew(docID);
      long[] docOrds = new long[1];
      int upto = 0;
      while (true) {
        long ord = oldValues.nextOrd();
        if (ord == NO_MORE_ORDS) {
          break;
        }
        if (upto == docOrds.length) {
          docOrds = ArrayUtil.grow(docOrds);
        }
        docOrds[upto++] = ord;
      }
      ords[newDocID] = ArrayUtil.copyOfSubArray(docOrds, 0, upto);
    }
    return ords;
  }

  @Override
  SortedSetDocValues getDocValues() {
    if (finalOrds == null) {
      assert finalOrdCounts == null && finalSortedValues == null && finalOrdMap == null;
      finishCurrentDoc();
      int valueCount = hash.size();
      finalOrds = pending.build();
      finalOrdCounts = pendingCounts.build();
      finalSortedValues = hash.sort();
      finalOrdMap = new int[valueCount];
    }
    for (int ord = 0; ord < finalOrdMap.length; ord++) {
      finalOrdMap[finalSortedValues[ord]] = ord;
    }
    return new BufferedSortedSetDocValues(finalSortedValues, finalOrdMap, hash, finalOrds, finalOrdCounts, maxCount, docsWithField.iterator());
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer) throws IOException {
    final int valueCount = hash.size();
    final PackedLongValues ords;
    final PackedLongValues ordCounts;
    final int[] sortedValues;
    final int[] ordMap;

    if (finalOrds == null) { // 进来
      assert finalOrdCounts == null && finalSortedValues == null && finalOrdMap == null;
      finishCurrentDoc();
      ords = pending.build(); // PackedLongValues，全部压缩到ords中了，  存放的是这个域所有的termId, 依次存放，相同算两个，每个文档每个域只有一个termid, 写入顺序
      ordCounts = pendingCounts.build(); // 全部压缩存储到了ordCounts中    存放的是这个域在这个文档有几个同名的域，都是1
      sortedValues = hash.sort(); // 所有文档这个域的distinct(termId)的排序，按照byte排序
      ordMap = new int[valueCount]; // 每个termId->编号， 与sortedValues映射关系刚好相反.
      for(int ord=0;ord<valueCount;ord++) {
        ordMap[sortedValues[ord]] = ord; //// 比如ordMap[0]的值termid, 该termid对应的value就是该域第一个写入的文档值
      }// 只存储了a,b,c,d按照顺序，想知道第三个写入的数据是哪个？就需要使用[a,b,c,d]中第ordMap[3]就是写入时第三个。
    } else {
      ords = finalOrds;
      ordCounts = finalOrdCounts;
      sortedValues = finalSortedValues;
      ordMap = finalOrdMap;
    }

    final long[][] sorted;
    if (sortMap != null) { // 为null
      sorted = sortDocValues(state.segmentInfo.maxDoc(), sortMap,
          new BufferedSortedSetDocValues(sortedValues, ordMap, hash, ords, ordCounts, maxCount, docsWithField.iterator()));
    } else { // 进来
      sorted = null;
    }
    dvConsumer.addSortedSetField(fieldInfo, //真正写入docValue相关信息, 跑到PerFieldDocValuesFormat$FieldsWriter
                                 new EmptyDocValuesProducer() {
                                   @Override
                                   public SortedSetDocValues getSortedSet(FieldInfo fieldInfoIn) {
                                     if (fieldInfoIn != fieldInfo) {
                                       throw new IllegalArgumentException("wrong fieldInfo");
                                     }
                                     final SortedSetDocValues buf =  // distinct(doc词)的个数,
                                         new BufferedSortedSetDocValues(sortedValues, ordMap, hash, ords, ordCounts, maxCount, docsWithField.iterator());
                                     if (sorted == null) {
                                       return buf;
                                     } else {
                                       return new SortingLeafReader.SortingSortedSetDocValues(buf, sorted);
                                     }
                                   }
                                 });
  }

  private static class BufferedSortedSetDocValues extends SortedSetDocValues {
    final int[] sortedValues;
    final int[] ordMap; // 词（相同词算两个）
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();
    final PackedLongValues.Iterator ordsIter;
    final PackedLongValues.Iterator ordCountsIter;
    final DocIdSetIterator docsWithField;
    final int currentDoc[]; // 存放的是当前文档该域多个value的termId值，默认length=1
    
    private int ordCount;
    private int ordUpto;

    public BufferedSortedSetDocValues(int[] sortedValues, int[] ordMap, BytesRefHash hash, PackedLongValues ords, PackedLongValues ordCounts, int maxCount, DocIdSetIterator docsWithField) {
      this.currentDoc = new int[maxCount];
      this.sortedValues = sortedValues; // 所有文档这个域的distinct(termId)的排序之后的termId
      this.ordMap = ordMap; // 存放的是下标->termIde，比如ordMap[0]的值termid, 该termid对应的value就是该域第一个写入的文档值
      this.hash = hash;
      this.ordsIter = ords.iterator();    //  存放的是这个域所有的termId, 依次存放，相同算两个，每个文档每个域只有一个termid
      this.ordCountsIter = ordCounts.iterator();   //  存放的是这个域在这个文档有几个同名的域，都是1, 这里会进行解压缩
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }
    // nextDoc和nextOrd是配合使用的，先通过nextDoc给currentDoc赋值，然后在nextOrd中才能够遍历取值
    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc(); // DocIdSetIterator
      if (docID != NO_MORE_DOCS) {
        ordCount = (int) ordCountsIter.next(); // 该文档同名域的个数，
        assert ordCount > 0;
        for (int i = 0; i < ordCount; i++) {//然后遍历该文档所有同名域的termID
          currentDoc[i] = ordMap[Math.toIntExact(ordsIter.next())]; // 解码出来，就是域的多个value的写入顺序
        }
        Arrays.sort(currentDoc, 0, ordCount); // 基本没用，为了应对terms那种的数组存放
        ordUpto = 0;
      }
      return docID;
    }

    @Override
    public long nextOrd() {
      if (ordUpto == ordCount) {
        return NO_MORE_ORDS;
      } else {
        return currentDoc[ordUpto++];
      }
    }

    @Override
    public long cost() {
      return docsWithField.cost();
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
    public BytesRef lookupOrd(long ord) {
      assert ord >= 0 && ord < ordMap.length: "ord=" + ord + " is out of bounds 0 .. " + (ordMap.length-1);
      hash.get(sortedValues[Math.toIntExact(ord)], scratch);  // 按照termid对应的term顺序排序后的
      return scratch;
    }
  }

}
