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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.RamUsageEstimator;
// https://blog.csdn.net/RovisuKi/article/details/99943543
/** Default general purpose indexing chain, which handles
 *  indexing all types of fields. */ // 写入链的起始位置，重点类。每个semgent落盘时，DocumentsWriterPerThread.DefaultIndexingChain就置空了，下次用的时候再生成
final class DefaultIndexingChain extends DocConsumer {//被DocumentsWriterPerThread所拥有的，以DocumentsWriterPerThread为单一索引
  final Counter bytesUsed;// 是从DocumentsWriterPerThread.bytesUsed中传递过来的
  final DocumentsWriterPerThread docWriter; // DocumentsWriterPerThread
  final FieldInfos.Builder fieldInfos;

  // Writes postings and term vectors:
  final TermsHash termsHash;  // FreqProxTermsWriter, 词向量信息存储在这里
  // Writes stored fields
  final StoredFieldsConsumer storedFieldsConsumer;  // StoredFieldsConsumer ，每个segment新产生一个， 存储域值的，就是把value给存储起来，所有文档所有域共用着一个变量

  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25: 是个链表结构，，随时可能通过rehash进行扩容
  private PerField[] fieldHash = new PerField[2];  // 哈希表来方便更快查找域(比如如何快速索引到PostingList对象)，segment内唯一，segment生成后就清空
  private int hashMask = 1; // 就是为了hash进范围

  private int totalFieldCount; // 该链域的个数
  private long nextFieldGen; // 整个链共享的字段，每写入一个文档，都加1

  // Holds fields seen in each document 和fieldHash存的很像，只是fieldHash通过hash作了映射，便于快速查找对应的字段
  private PerField[] fields = new PerField[1];// fields仅仅是为了快速遍历当前文档所有的域
  private final InfoStream infoStream;

  public DefaultIndexingChain(DocumentsWriterPerThread docWriter) { // 写入链
    this.docWriter = docWriter;// DocumentsWriterPerThread
    this.fieldInfos = docWriter.getFieldInfosBuilder();
    this.bytesUsed = docWriter.bytesUsed;
    this.infoStream = docWriter.getIndexWriterConfig().getInfoStream();

    final TermsHash termVectorsWriter;
    if (docWriter.getSegmentInfo().getIndexSort() == null) { // 为空
      storedFieldsConsumer = new StoredFieldsConsumer(docWriter);  // 倒排索引的俩结构
      termVectorsWriter = new TermVectorsConsumer(docWriter);
    } else {
      storedFieldsConsumer = new SortingStoredFieldsConsumer(docWriter);
      termVectorsWriter = new SortingTermVectorsConsumer(docWriter);
    }
    termsHash = new FreqProxTermsWriter(docWriter, termVectorsWriter);
  }

  private LeafReader getDocValuesLeafReader() {
    return new DocValuesLeafReader() {
      @Override
      public NumericDocValues getNumericDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
          return (NumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public BinaryDocValues getBinaryDocValues(String field) {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
          return (BinaryDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedDocValues getSortedDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          return (SortedDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
          return (SortedNumericDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        PerField pf = getPerField(field);
        if (pf == null) {
          return null;
        }
        if (pf.fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
          return (SortedSetDocValues) pf.docValuesWriter.getDocValues();
        }
        return null;
      }

      @Override
      public FieldInfos getFieldInfos() {
        return fieldInfos.finish();
      }

    };
  }

  private Sorter.DocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    Sort indexSort = state.segmentInfo.getIndexSort();
    if (indexSort == null) {
      return null;
    }

    LeafReader docValuesReader = getDocValuesLeafReader();

    List<IndexSorter.DocComparator> comparators = new ArrayList<>();
    for (int i = 0; i < indexSort.getSort().length; i++) {
      SortField sortField = indexSort.getSort()[i];
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new UnsupportedOperationException("Cannot sort index using sort field " + sortField);
      }
      comparators.add(sorter.getDocComparator(docValuesReader, state.segmentInfo.maxDoc()));
    }
    Sorter sorter = new Sorter(indexSort);
    // returns null if the documents are already sorted
    return sorter.sort(state.segmentInfo.maxDoc(), comparators.toArray(new IndexSorter.DocComparator[0]));
  }

  @Override // 确认了，这里都会落盘
  public Sorter.DocMap flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.DocMap sortMap = maybeSortSegment(state); // 为null，函数无用
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);// 写入nvm文件
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write norms");
    }
    SegmentReadState readState = new SegmentReadState(state.directory, state.segmentInfo, state.fieldInfos, IOContext.READ, state.segmentSuffix);
    
    t0 = System.nanoTime();
    writeDocValues(state, sortMap);// 写入DocValue
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap); //数字型写入
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write points");
    }
    
    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);// 啥都不做
    storedFieldsConsumer.flush(state, sortMap); // 将storeField刷入fdt文件中
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to finish stored fields");
    }

    t0 = System.nanoTime();
    Map<String,TermsHashPerField> fieldsToFlush = new HashMap<>();
    for (int i=0;i<fieldHash.length;i++) {
      PerField perField = fieldHash[i];
      while (perField != null) {
        if (perField.invertState != null) {
          fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
        }
        perField = perField.next;
      }
    }

    try (NormsProducer norms = readState.fieldInfos.hasNorms() // 为null
        ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
        : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      } // 写tvd、tvm文件，然后在写tip、tim文件，doc.pox,pay（若有删除，里面会进行term的删除操作）
      termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance); // 进入的是FreqProxTermsWriter类,不是TermHash类
    }
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime(); // 写入fnm文件
    docWriter.codec.fieldInfosFormat().write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);//Lucene60FieldInfosFormat.
    if (infoStream.isEnabled("IW")) {
      infoStream.message("IW", ((System.nanoTime()-t0)/1000000) + " msec to write fieldInfos");
    }

    return sortMap;
  }

  /** Writes all buffered points. */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    PointsWriter pointsWriter = null; // 该segment全局共享一个writer
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.pointValuesWriter != null) {
            if (perField.fieldInfo.getPointDimensionCount() == 0) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no points but wrote them");
            }
            if (pointsWriter == null) { //
              // lazy init
              PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat(); // Lucene86PointsFormat
              if (fmt == null) {
                throw new IllegalStateException("field=\"" + perField.fieldInfo.name + "\" was indexed as points but codec does not support points");
              }
              pointsWriter = fmt.fieldsWriter(state); // 每个segment会创建一个新的Lucene60PointsWriter， 该segment所有域都会共享这一个字段.segment完成会关闭这个writer
            }

            perField.pointValuesWriter.flush(state, sortMap, pointsWriter); // 需要进来看下
            perField.pointValuesWriter = null;
          } else if (perField.fieldInfo.getPointDimensionCount() != 0) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has points but did not write them");
          }
          perField = perField.next;
        }
      }
      if (pointsWriter != null) { // dim文件写完了
        pointsWriter.finish();
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(pointsWriter); //关闭dim文件
      } else {
        IOUtils.closeWhileHandlingException(pointsWriter);
      }
    }
  }
 //     * writeDocValues函数遍历得到每个PerField，PerField中的docValuesWriter根据不同的Field值域类型被定义为BinaryDocValuesWriter、NumericDocValuesWriter、SortedDocValuesWriter、SortedNumericDocValuesWriter和SortedSetDocValuesWriter
  /** Writes all buffered doc values (called from {@link #flush}). */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    int maxDoc = state.segmentInfo.maxDoc();
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i=0;i<fieldHash.length;i++) { // 是个hash链表结构, segment内唯一的域
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.docValuesWriter != null) {
            if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
              // BUG
              throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has no docValues but wrote them");
            }
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat(); //
              dvConsumer = fmt.fieldsConsumer(state); // PerFieldDocValuesFormat$FieldsWriter
            }
            perField.docValuesWriter.flush(state, sortMap, dvConsumer);// 要进来看下，docvalue真正向磁盘写入
            perField.docValuesWriter = null;// 置空了
          } else if (perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
            // BUG
            throw new AssertionError("segment=" + state.segmentInfo + ": field=\"" + perField.fieldInfo.name + "\" has docValues but did not write them");
          }
          perField = perField.next;
        }
      }

      // TODO: catch missing DV fields here?  else we have
      // null/"" depending on how docs landed in segments?
      // but we can't detect all cases, and we should leave
      // this behavior undefined. dv is not "schemaless": it's column-stride.
      success = true;
    } finally {
      if (success) {
        IOUtils.close(dvConsumer); // 向dvd/dvm写入footer并关闭文档
      } else {
        IOUtils.closeWhileHandlingException(dvConsumer);
      }
    }

    if (state.fieldInfos.hasDocValues() == false) {
      if (dvConsumer != null) {
        // BUG
        throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
      }
    } else if (dvConsumer == null) {
      // BUG
      throw new AssertionError("segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
    }
  }
  // 写入nvd数据文件以及nvm元数据文件，在调用flush时候才会写入
  private void writeNorms(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    boolean success = false;
    NormsConsumer normsConsumer = null;
    try {
      if (state.fieldInfos.hasNorms()) {
        NormsFormat normsFormat = state.segmentInfo.getCodec().normsFormat();
        assert normsFormat != null;
        normsConsumer = normsFormat.normsConsumer(state);

        for (FieldInfo fi : state.fieldInfos) {
          PerField perField = getPerField(fi.name);
          assert perField != null;

          // we must check the final value of omitNorms for the fieldinfo: it could have 
          // changed for this field since the first time we added it.
          if (fi.omitsNorms() == false && fi.getIndexOptions() != IndexOptions.NONE) {
            assert perField.norms != null: "field=" + fi.name;
            perField.norms.finish(state.segmentInfo.maxDoc());
            perField.norms.flush(state, sortMap, normsConsumer);
          }
        }
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(normsConsumer);
      } else {
        IOUtils.closeWhileHandlingException(normsConsumer);
      }
    }
  }

  @Override
  @SuppressWarnings("try")
  public void abort() throws IOException{
    // finalizer will e.g. close any open files in the term vectors writer:
    try (Closeable finalizer = termsHash::abort){
      storedFieldsConsumer.abort();
    } finally {
      Arrays.fill(fieldHash, null);
    }
  }

  private void rehash() {
    int newHashSize = (fieldHash.length*2); // 扩容一倍
    assert newHashSize > fieldHash.length;

    PerField newHashArray[] = new PerField[newHashSize];

    // Rehash
    int newHashMask = newHashSize-1;
    for(int j=0;j<fieldHash.length;j++) {// 链表
      PerField fp0 = fieldHash[j];
      while(fp0 != null) {
        final int hashPos2 = fp0.fieldInfo.name.hashCode() & newHashMask; // 全部重新hash一次
        PerField nextFP0 = fp0.next; // 头插法
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  /** Calls StoredFieldsWriter.startDocument, aborting the
   *  segment if it hits any exception. */
  private void startStoredFields(int docID) throws IOException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      docWriter.onAbortingException(th);
      throw th;
    }
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the
   *  segment if it hits any exception. */
  private void finishStoredFields() throws IOException {
    try {
      storedFieldsConsumer.finishDocument(); // 将store信息存入CompressingStoredFieldsWriter中
    } catch (Throwable th) {
      docWriter.onAbortingException(th);
      throw th;
    }
  }
  // 一个文档建立好了lucene索引
  @Override
  public void processDocument(int docID, Iterable<? extends IndexableField> document) throws IOException {

    // How many indexed field names we've seen (collapses
    // multiple field instances by the same name):
    int fieldCount = 0;

    long fieldGen = nextFieldGen++; // 多少个文档了

    // NOTE: we need two passes here, in case there are
    // multi-valued fields, because we must process all
    // instances of a given field at once, since the
    // analyzer is free to reuse TokenStream across fields
    // (i.e., we cannot have more than one TokenStream
    // running "at once"):

    termsHash.startDocument();// 每写完一个文档，都会清空一次TermVectorsConsumer里面缓存的上一个文档里面的所有字段信息
    // 也是蛮重要的。写fdt和fdx。若block刷新后，storedFieldWriter=null后，就是这里初始化一个新的文档
    startStoredFields(docID);
    try {
      for (IndexableField field : document) {// 写入每一个字段
        fieldCount = processField(docID, field, fieldGen, fieldCount);// fieldCount主要是是否进行分词
      }
    } finally { //
      if (docWriter.hasHitAbortingException() == false) { // 没有遇到抛出异常
        // Finish each indexed field name seen in the document:
        for (int i=0;i<fieldCount;i++) { // 有多少个域需要分词
          fields[i].finish(docID);// 主要统计该域的词信息，将TermVectorsConsumerPerField放到TermVectorsConsumer里面
        }
        finishStoredFields();  //写完所有域后，再整体将store信息存入CompressingStoredFieldsWriter中。若内存使用或者文档个数超过阈值了，会flush存储到fdt中
      }
    }
   //
    try {
      termsHash.finishDocument(docID);// 主要是是清理nextTermsHash, 内存里超过128个文档会触发一次刷新操作
    } catch (Throwable th) {
      // Must abort, on the possibility that on-disk term
      // vectors are now corrupt:
      docWriter.onAbortingException(th);
      throw th;
    }
  }
  // 写一个Field，  fieldGen：这是该链第几个文档 fieldCount：是这个文档中的第几个域
  private int processField(int docID, IndexableField field, long fieldGen, int fieldCount) throws IOException {
    String fieldName = field.name();
    IndexableFieldType fieldType = field.fieldType();

    PerField fp = null; //

    if (fieldType.indexOptions() == null) {
      throw new NullPointerException("IndexOptions must not be null (field: \"" + field.name() + "\")");
    }

    // Invert indexed fields:
    if (fieldType.indexOptions() != IndexOptions.NONE) { // 只要不为NONE. 就会建倒排索引结构
      fp = getOrAddField(fieldName, fieldType, true);// 每个字段只会保存一个 PerField 对象。要进去看下
      boolean first = fp.fieldGen != fieldGen; // 这个文档中这个域不是重复写入？
      fp.invert(docID, field, first); //创建倒排索引

      if (first) { // 该域是该segment第一次写入，就得放进来
        fields[fieldCount++] = fp;  // 这里才是真正存放，和fieldHash存放的是一个对象，
        fp.fieldGen = fieldGen;
      }
    } else { // 验证这些参数都是没有的
      verifyUnIndexedFieldType(fieldName, fieldType);
    }

    // Add stored fields:
    if (fieldType.stored()) {
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }
      if (fieldType.stored()) {
        String value = field.stringValue(); // 域的值
        if (value != null && value.length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
          throw new IllegalArgumentException("stored field \"" + field.name() + "\" is too large (" + value.length() + " characters) to store");
        }
        try { // //创建storeField, 只是将field值存放在CompressingStoredFieldsWriter的bufferedDocs中
          storedFieldsConsumer.writeField(fp.fieldInfo, field); // 面向行的存储，docvalue是面向列的存储
        } catch (Throwable th) {
          docWriter.onAbortingException(th);
          throw th;
        }
      }
    }

    DocValuesType dvType = fieldType.docValuesType();
    if (dvType == null) {
      throw new NullPointerException("docValuesType must not be null (field: \"" + fieldName + "\")");
    }
    if (dvType != DocValuesType.NONE) { // docValue必须不分词的字段才行
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      }//创建docValue，主要是为了聚合使用
      indexDocValue(docID, fp, dvType, field);// 是面向列的存储，一列的元素放在一个field中。
    }
    if (fieldType.pointDimensionCount() != 0) {// 数值型的，全部进来了
      if (fp == null) {
        fp = getOrAddField(fieldName, fieldType, false);
      } //创建point value
      indexPoint(docID, fp, field);
    }
    
    return fieldCount;
  }

  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    if (ft.storeTermVectors()) {
      throw new IllegalArgumentException("cannot store term vectors "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorPositions()) {
      throw new IllegalArgumentException("cannot store term vector positions "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorOffsets()) {
      throw new IllegalArgumentException("cannot store term vector offsets "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
    if (ft.storeTermVectorPayloads()) {
      throw new IllegalArgumentException("cannot store term vector payloads "
                                         + "for a field that is not indexed (field=\"" + name + "\")");
    }
  }
   // 放入内存阶段
  /** Called from processDocument to index one field's point */
  private void indexPoint(int docID, PerField fp, IndexableField field) {
    int pointDimensionCount = field.fieldType().pointDimensionCount(); //多少个维度
    int pointIndexDimensionCount = field.fieldType().pointIndexDimensionCount();

    int dimensionNumBytes = field.fieldType().pointNumBytes(); // 每个维度的长度

    // Record dimensions for this field; this setter will throw IllegalArgExc if
    // the dimensions were already set to something different:
    if (fp.fieldInfo.getPointDimensionCount() == 0) {
      fieldInfos.globalFieldNumbers.setDimensions(fp.fieldInfo.number, fp.fieldInfo.name, pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);
    }
    // 这里会更新pointDataDimensionCount，pointIndexDimensionCount，是否该判断下，没必要一定要提那些
    fp.fieldInfo.setPointDimensions(pointDimensionCount, pointIndexDimensionCount, dimensionNumBytes);

    if (fp.pointValuesWriter == null) {
      fp.pointValuesWriter = new PointValuesWriter(docWriter, fp.fieldInfo);
    }
    fp.pointValuesWriter.addPackedValue(docID, field.binaryValue());// 这里添加下数据
  }

  private void validateIndexSortDVType(Sort indexSort, String fieldToValidate, DocValuesType dvType) throws IOException {
    for (SortField sortField : indexSort.getSort()) {
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new IllegalStateException("Cannot sort index with sort order " + sortField);
      }
      sorter.getDocComparator(new DocValuesLeafReader() {
        @Override
        public NumericDocValues getNumericDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.NUMERIC) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be NUMERIC but it is [" + dvType + "]");
          }
          return DocValues.emptyNumeric();
        }

        @Override
        public BinaryDocValues getBinaryDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.BINARY) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be BINARY but it is [" + dvType + "]");
          }
          return DocValues.emptyBinary();
        }

        @Override
        public SortedDocValues getSortedDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be SORTED but it is [" + dvType + "]");
          }
          return DocValues.emptySorted();
        }

        @Override
        public SortedNumericDocValues getSortedNumericDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_NUMERIC) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be SORTED_NUMERIC but it is [" + dvType + "]");
          }
          return DocValues.emptySortedNumeric();
        }

        @Override
        public SortedSetDocValues getSortedSetDocValues(String field) {
          if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_SET) {
            throw new IllegalArgumentException("SortField " + sortField + " expected field [" + field + "] to be SORTED_SET but it is [" + dvType + "]");
          }
          return DocValues.emptySortedSet();
        }

        @Override
        public FieldInfos getFieldInfos() {
          throw new UnsupportedOperationException();
        }
      }, 0);
    }
  }

  /** Called from processDocument to index one field's doc value */
  private void indexDocValue(int docID, PerField fp, DocValuesType dvType, IndexableField field) throws IOException {

    if (fp.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
      // This is the first time we are seeing this field indexed with doc values, so we
      // now record the DV type so that any future attempt to (illegally) change
      // the DV type of this field, will throw an IllegalArgExc:
      if (docWriter.getSegmentInfo().getIndexSort() != null) {
        final Sort indexSort = docWriter.getSegmentInfo().getIndexSort();
        validateIndexSortDVType(indexSort, fp.fieldInfo.name, dvType);
      }
      fieldInfos.globalFieldNumbers.setDocValuesType(fp.fieldInfo.number, fp.fieldInfo.name, dvType); // 把字段-docvalu给存储起来
    }

    fp.fieldInfo.setDocValuesType(dvType);

    switch(dvType) {

      case NUMERIC:
        if (fp.docValuesWriter == null) { // 每次刷新到磁盘时会清空该对象
          fp.docValuesWriter = new NumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        if (field.numericValue() == null) {
          throw new IllegalArgumentException("field=\"" + fp.fieldInfo.name + "\": null value not allowed");
        }
        ((NumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
        break;

      case BINARY:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new BinaryDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;
        
      case SORTED_NUMERIC:
        if (fp.docValuesWriter == null) {
          fp.docValuesWriter = new SortedNumericDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedNumericDocValuesWriter) fp.docValuesWriter).addValue(docID, field.numericValue().longValue());
        break;

      case SORTED_SET:
        if (fp.docValuesWriter == null) { // 所有文档所有域全局唯一
          fp.docValuesWriter = new SortedSetDocValuesWriter(fp.fieldInfo, bytesUsed);
        }
        ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
  }

  /** Returns a previously created {@link PerField}, or null
   *  if this field name wasn't seen yet. */
  private PerField getPerField(String name) {
    final int hashPos = name.hashCode() & hashMask;
    PerField fp = fieldHash[hashPos];
    while (fp != null && !fp.fieldInfo.name.equals(name)) {
      fp = fp.next;
    }
    return fp;
  }

  /** Returns a previously created {@link PerField},
   *  absorbing the type information from {@link FieldType},
   *  and creates a new {@link PerField} if this field name
   *  wasn't seen yet. */
  private PerField getOrAddField(String name, IndexableFieldType fieldType, boolean invert) {
    // invert在索引字段时候会自动传递进来，若字段设置了非IndexOptions.NONE， 那么invert一定会传递进来
    // Make sure we have a PerField allocated
    final int hashPos = name.hashCode() & hashMask; // //计算哈希值
    PerField fp = fieldHash[hashPos]; // //找到哈希表中对应的位置
    while (fp != null && !fp.fieldInfo.name.equals(name)) { //链式哈希表(碰撞发）
      fp = fp.next;
    }

    if (fp == null) { // 若该字段不存在
      // First time we are seeing this field in this segment

      FieldInfo fi = fieldInfos.getOrAdd(name);
      initIndexOptions(fi, fieldType.indexOptions()); // 设置是否建立倒排索引结构
      Map<String, String> attributes = fieldType.getAttributes();
      if (attributes != null) {
        attributes.forEach((k, v) -> fi.putAttribute(k, v));
      }

      LiveIndexWriterConfig indexWriterConfig = docWriter.getIndexWriterConfig();
      fp = new PerField(docWriter.getIndexCreatedVersionMajor(), fi, invert,
          indexWriterConfig.getSimilarity(), indexWriterConfig.getInfoStream(), indexWriterConfig.getAnalyzer());
      fp.next = fieldHash[hashPos]; // 头插法插入链中，探针法
      fieldHash[hashPos] = fp;
      totalFieldCount++; // 域的总长度

      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length/2) {
        rehash();
      }
      // 这里只是扩容，
      if (totalFieldCount > fields.length) {
        PerField[] newFields = new PerField[ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }
   //
    } else if (invert && fp.invertState == null) {
      initIndexOptions(fp.fieldInfo, fieldType.indexOptions());
      fp.setInvertState();
    }

    return fp;
  }

  private void initIndexOptions(FieldInfo info, IndexOptions indexOptions) {
    // Messy: must set this here because e.g. FreqProxTermsWriterPerField looks at the initial
    // IndexOptions to decide what arrays it must create).
    assert info.getIndexOptions() == IndexOptions.NONE;
    // This is the first time we are seeing this field indexed, so we now
    // record the index options so that any future attempt to (illegally)
    // change the index options of this field, will throw an IllegalArgExc:  第一次我们会记录这个字段的field 设置，下次若不一样，就会直接抛异常
    fieldInfos.globalFieldNumbers.setIndexOptions(info.number, info.name, indexOptions);
    info.setIndexOptions(indexOptions);
  }
  // segment内共享，segment完成后就清空
  /** NOTE: not static: accesses at least docState, termsHash. */
  private final class PerField implements Comparable<PerField> {

    final int indexCreatedVersionMajor;
    final FieldInfo fieldInfo;  //
    final Similarity similarity;
    // 只有设置了倒排索引，才会给这些变量赋值
    FieldInvertState invertState; // 统计倒排信息，每个field都会独享一个(每个文档统计使用前，都会清空该字段值)，在进入域分词的时候会被清空
    TermsHashPerField termsHashPerField;  // FreqProxTermsWriterPerField, 里面包含了TermVectorsConsumer和TermVectorsConsumerPerField

    // Non-null if this field ever had doc values in this
    // segment:
    DocValuesWriter<?> docValuesWriter;// 一个段该域所有文档共享一个该字段。每次刷新到磁盘时会清空。第一次写入时就会构建该对象SortedSetDocValuesWriter
    // 会在放入内存阶段初始化
    // Non-null if this field ever had points in this segment:
    PointValuesWriter pointValuesWriter; // 一个段该域拥有这一个， flush完后，就会清空

    /** We use this to know when a PerField is seen for the
     *  first time in the current document. */
    long fieldGen = -1; //这是该链第几个文档，作用就是判断在该文档中该域第几次写入

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NormValuesWriter norms;  // NormValuesWriter
    
    // reused  segment级别同一个Field共享的
    TokenStream tokenStream;
    private final InfoStream infoStream;
    private final Analyzer analyzer;

    PerField(int indexCreatedVersionMajor, FieldInfo fieldInfo, boolean invert, Similarity similarity, InfoStream infoStream, Analyzer analyzer) {
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.fieldInfo = fieldInfo;
      this.similarity = similarity;
      this.infoStream = infoStream;
      this.analyzer = analyzer;
      if (invert) {// 若设置了存储倒排索引
        setInvertState();
      }
    }

    void setInvertState() { // 倒排索引参数设置，都在这里给设置了
      invertState = new FieldInvertState(indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
      termsHashPerField = termsHash.addField(invertState, fieldInfo); // 产生FreqProxTermsWriterPerField及TermVectorsConsumerPerField
      if (fieldInfo.omitsNorms() == false) {
        assert norms == null;
        // Even if no documents actually succeed in setting a norm, we still write norms for this segment:
        norms = new NormValuesWriter(fieldInfo, bytesUsed);
      }
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldInfo.name.compareTo(other.fieldInfo.name);
    }
    //每个文档写完之后就会进来
    public void finish(int docID) throws IOException {
      if (fieldInfo.omitsNorms() == false) { // 为啥Norm直接跳多了
        long normValue;
        if (invertState.length == 0) {
          // the field exists in this document, but it did not have
          // any indexed tokens, so we assign a default value of zero
          // to the norm
          normValue = 0;
        } else {
          normValue = similarity.computeNorm(invertState);
          if (normValue == 0) {
            throw new IllegalStateException("Similarity " + similarity + " return 0 for non-empty field");
          }
        }
        norms.addValue(docID, normValue);
      }

      termsHashPerField.finish(); // FreqProxTermsWriterPerField
    }

    /** Inverts one field for one document; first is true
     *  if this is the first time we are seeing this field
     *  name in this document. */
    public void invert(int docID, IndexableField field, boolean first) throws IOException {// PerFieldl里面开始
      if (first) {// 在这个文档中第一次看到这个域
        // First time we're seeing this field (indexed) in
        // this document:
        invertState.reset(); // 每次写入一个新的文档，这里都会被清空
      }

      IndexableFieldType fieldType = field.fieldType();

      IndexOptions indexOptions = fieldType.indexOptions();
      fieldInfo.setIndexOptions(indexOptions);

      if (fieldType.omitNorms()) {
        fieldInfo.setOmitsNorms();
      }

      final boolean analyzed = fieldType.tokenized() && analyzer != null;// 是否分词
        
      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch' clauses,
       * but rather a finally that takes note of the problem.
       */
      boolean succeededInProcessingField = false;
      try (TokenStream stream = tokenStream = field.tokenStream(analyzer, tokenStream)) { // 进行了分词，跑入了Field.tokenStream()
        // reset the TokenStream to the first token
        stream.reset();
        invertState.setAttributeSource(stream); // 设置放到invertState中，可以获取很多分词后的参数信息，是lucene自带特性
        termsHashPerField.start(field, first); // 这里会针对FreqProxTermsWriterPerField

        while (stream.incrementToken()) { // 这样是循环每个词的

          // If we hit an exception in stream.next below
          // (which is fairly common, e.g. if analyzer
          // chokes on a given document), then it's
          // non-aborting and (above) this one document
          // will be marked as deleted, but still
          // consume a docID

          int posIncr = invertState.posIncrAttribute.getPositionIncrement(); // 词的位置增量
          invertState.position += posIncr;  // 这里position已经增加了，和offset还不一致，offset是域全部写完了再更新
          if (invertState.position < invertState.lastPosition) {
            if (posIncr == 0) {
              throw new IllegalArgumentException("first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            } else if (posIncr < 0) {
              throw new IllegalArgumentException("position increment must be >= 0 (got " + posIncr + ") for field '" + field.name() + "'");
            } else {
              throw new IllegalArgumentException("position overflowed Integer.MAX_VALUE (got posIncr=" + posIncr + " lastPosition=" + invertState.lastPosition + " position=" + invertState.position + ") for field '" + field.name() + "'");
            }
          } else if (invertState.position > IndexWriter.MAX_POSITION) {
            throw new IllegalArgumentException("position " + invertState.position + " is too large for field '" + field.name() + "': max allowed position is " + IndexWriter.MAX_POSITION);
          }
          invertState.lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }
              
          int startOffset = invertState.offset + invertState.offsetAttribute.startOffset(); // 词的起始位置
          int endOffset = invertState.offset + invertState.offsetAttribute.endOffset(); // 这个词的末尾
          if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
            throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go backwards "
                                               + "startOffset=" + startOffset + ",endOffset=" + endOffset + ",lastStartOffset=" + invertState.lastStartOffset + " for field '" + field.name() + "'");
          }
          invertState.lastStartOffset = startOffset;

          try {
            invertState.length = Math.addExact(invertState.length, invertState.termFreqAttribute.getTermFrequency());     // 相加
          } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("too many tokens for field \"" + field.name() + "\"");
          }
          
          //System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          try {
            termsHashPerField.add(invertState.termAttribute.getBytesRef(), docID);
          } catch (MaxBytesLengthExceededException e) {
            byte[] prefix = new byte[30];
            BytesRef bigTerm = invertState.termAttribute.getBytesRef();
            System.arraycopy(bigTerm.bytes, bigTerm.offset, prefix, 0, 30);
            String msg = "Document contains at least one immense term in field=\"" + fieldInfo.name + "\" (whose UTF8 encoding is longer than the max length " + DocumentsWriterPerThread.MAX_TERM_LENGTH_UTF8 + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The prefix of the first immense term is: '" + Arrays.toString(prefix) + "...', original message: " + e.getMessage();
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "ERROR: " + msg);
            }
            // Document will be deleted above:
            throw new IllegalArgumentException(msg, e);
          } catch (Throwable th) {
            docWriter.onAbortingException(th);
            throw th;
          }
        }

        // trigger streams to perform end-of-stream operations
        stream.end();

        // TODO: maybe add some safety? then again, it's already checked 
        // when we come back around to the field...
        invertState.position += invertState.posIncrAttribute.getPositionIncrement(); //
        invertState.offset += invertState.offsetAttribute.endOffset(); //

        /* if there is an exception coming through, we won't set this to true here:*/
        succeededInProcessingField = true;
      } finally {
        if (!succeededInProcessingField && infoStream.isEnabled("DW")) {
          infoStream.message("DW", "An exception was thrown while processing field " + fieldInfo.name);
        }
      }

      if (analyzed) { // 若分词的话，
        invertState.position += analyzer.getPositionIncrementGap(fieldInfo.name);
        invertState.offset += analyzer.getOffsetGap(fieldInfo.name);
      }
    }
  }

  @Override
  DocIdSetIterator getHasDocValues(String field) {
    PerField perField = getPerField(field);
    if (perField != null) {
      if (perField.docValuesWriter != null) {
        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
          return null;
        }

        return perField.docValuesWriter.getDocValues();
      }
    }
    return null;
  }
}
