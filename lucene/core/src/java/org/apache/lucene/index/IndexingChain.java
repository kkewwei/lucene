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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.BytesRefValuesCursor;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.ColumnFieldAdapter;
import org.apache.lucene.document.column.ColumnValidation;
import org.apache.lucene.document.column.DictionaryColumn;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.LongValuesCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.OrdinalsCursor;
import org.apache.lucene.document.column.OrdinalsTupleCursor;
import org.apache.lucene.document.column.TokenStreamColumn;
import org.apache.lucene.document.column.VectorColumn;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.Version;

/** Default general purpose indexing chain, which handles indexing all types of fields. */
final class IndexingChain implements Accountable {// 写入链的起始位置，重点类。每个semgent落盘时，DocumentsWriterPerThread.DefaultIndexingChain就置空了，下次用的时候再生成

  final Counter bytesUsed = Counter.newCounter();// 是从DocumentsWriterPerThread.bytesUsed中传递过来的
  final FieldInfos.Builder fieldInfos;

  // Writes postings and term vectors:
  final TermsHash termsHash; // FreqProxTermsWriter, 词向量信息存储在这里
  // Shared pool for doc-value terms
  final ByteBlockPool docValuesBytePool;
  // Shared scratch buffers for dense points encoding
  final SharedIndexingScratch sharedIndexingScratch;
  // Writes stored fields
  final StoredFieldsConsumer storedFieldsConsumer;// StoredFieldsConsumer ，每个segment新产生一个， 存储域值的，就是把value给存储起来，所有文档所有域共用着一个变量
  final VectorValuesConsumer vectorValuesConsumer;
  final TermVectorsConsumer termVectorsWriter;

  // NOTE: I tried using Hash Map<String,PerField>
  // but it was ~2% slower on Wiki and Geonames with Java
  // 1.7.0_25:  是个链表结构，，随时可能通过rehash进行扩容
  private PerField[] fieldHash = new PerField[2];// 哈希表来方便更快查找域(比如如何快速索引到PostingList对象)，segment内唯一，segment生成后就清空
  private int hashMask = 1;// 就是为了hash进范围

  private int totalFieldCount;// 该链域的个数
  private long nextFieldGen;// 整个链共享的字段，每写入一个文档，都加1

  // Holds fields seen in each document 和fieldHash存的很像，只是fieldHash通过hash作了映射，便于快速查找对应的字段
  private PerField[] fields = new PerField[1];// fields仅仅是为了快速遍历当前文档所有的域，只会保持融合后的fields, fieldname不重复
  private PerField[] docFields = new PerField[2];// fieldname可以重复
  private final InfoStream infoStream;
  private final ByteBlockPool.Allocator byteBlockAllocator;
  private final LiveIndexWriterConfig indexWriterConfig;
  private final int indexCreatedVersionMajor;
  private final Consumer<Throwable> abortingExceptionConsumer;
  private final PerField parentPf;
  private final NumericDocValuesField parentField;
  private boolean hasHitAbortingException;

  IndexingChain(
      int indexCreatedVersionMajor,
      SegmentInfo segmentInfo,
      Directory directory,
      FieldInfos.Builder fieldInfos,
      LiveIndexWriterConfig indexWriterConfig,
      Consumer<Throwable> abortingExceptionConsumer) { // 写入链
    this.indexCreatedVersionMajor = indexCreatedVersionMajor;
    byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(bytesUsed);
    IntBlockPool.Allocator intBlockAllocator = new IntBlockAllocator(bytesUsed);
    this.indexWriterConfig = indexWriterConfig;
    assert segmentInfo.getIndexSort() == indexWriterConfig.getIndexSort();
    this.fieldInfos = fieldInfos;
    this.infoStream = indexWriterConfig.getInfoStream();
    this.abortingExceptionConsumer = abortingExceptionConsumer;
    this.vectorValuesConsumer =
        new VectorValuesConsumer(indexWriterConfig.getCodec(), directory, segmentInfo, infoStream);

    if (segmentInfo.getIndexSort() == null) {
      storedFieldsConsumer =
          new StoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new TermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    } else {
      storedFieldsConsumer =
          new SortingStoredFieldsConsumer(indexWriterConfig.getCodec(), directory, segmentInfo);
      termVectorsWriter =
          new SortingTermVectorsConsumer(
              intBlockAllocator,
              byteBlockAllocator,
              directory,
              segmentInfo,
              indexWriterConfig.getCodec());
    }
    termsHash =
        new FreqProxTermsWriter(
            intBlockAllocator, byteBlockAllocator, bytesUsed, termVectorsWriter);
    docValuesBytePool = new ByteBlockPool(byteBlockAllocator);
    sharedIndexingScratch = new SharedIndexingScratch(bytesUsed);
    if (indexWriterConfig.getParentField() != null) {
      this.parentField = new NumericDocValuesField(indexWriterConfig.getParentField(), -1);
      parentPf = getOrAddPerField(this.parentField.name());
      updateDocFieldSchema(this.parentField.name(), parentPf.schema, this.parentField.fieldType());
    } else {
      this.parentField = null;
      this.parentPf = null;
    }
  }

  private void onAbortingException(Throwable th) {
    assert th != null;
    this.hasHitAbortingException = true;
    abortingExceptionConsumer.accept(th);
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

  private Sorter.PackableDocMap maybeSortSegment(SegmentWriteState state) throws IOException {
    Sort indexSort = state.segmentInfo.getIndexSort();
    if (indexSort == null) {
      return null;
    }

    LeafReader docValuesReader = getDocValuesLeafReader();
    Function<IndexSorter.DocComparator, IndexSorter.DocComparator> comparatorWrapper =
        Function.identity();

    if (state.segmentInfo.getHasBlocks() && state.fieldInfos.getParentField() != null) {
      final DocIdSetIterator readerValues =
          docValuesReader.getNumericDocValues(state.fieldInfos.getParentField());
      if (readerValues == null) {
        throw new CorruptIndexException(
            "missing doc values for parent field \"" + state.fieldInfos.getParentField() + "\"",
            "IndexingChain");
      }
      BitSet parents = BitSet.of(readerValues, state.segmentInfo.maxDoc());
      comparatorWrapper =
          in ->
              (docID1, docID2) ->
                  in.compare(parents.nextSetBit(docID1), parents.nextSetBit(docID2));
    }
    if (state.segmentInfo.getHasBlocks()
        && state.fieldInfos.getParentField() == null
        && indexCreatedVersionMajor >= Version.LUCENE_10_0_0.major) {
      throw new CorruptIndexException(
          "parent field is not set but the index has blocks and uses index sorting. indexCreatedVersionMajor: "
              + indexCreatedVersionMajor,
          "IndexingChain");
    }
    List<IndexSorter.DocComparator> comparators = new ArrayList<>();
    for (int i = 0; i < indexSort.getSort().length; i++) {
      SortField sortField = indexSort.getSort()[i];
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new UnsupportedOperationException("Cannot sort index using sort field " + sortField);
      }

      IndexSorter.DocComparator docComparator =
          sorter.getDocComparator(docValuesReader, state.segmentInfo.maxDoc());
      comparators.add(comparatorWrapper.apply(docComparator));
    }
    Sorter sorter = new Sorter(indexSort);
    // returns null if the documents are already sorted
    return sorter.sortAndLeaveUnpacked(
        state.segmentInfo.maxDoc(), comparators.toArray(IndexSorter.DocComparator[]::new));
  }
// 确认了，这里都会落盘,es refresh
  Sorter.PackableDocMap flush(SegmentWriteState state) throws IOException {

    // NOTE: caller (DocumentsWriterPerThread) handles
    // aborting on any exception from this method
    Sorter.PackableDocMap sortMap = maybeSortSegment(state);// 是否基于某个字段排序
    int maxDoc = state.segmentInfo.maxDoc();
    long t0 = System.nanoTime();
    writeNorms(state, sortMap);// 写入nvm文件
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write norms");
    }
    SegmentReadState readState =
        new SegmentReadState(
            state.directory,
            state.segmentInfo,
            state.fieldInfos,
            IOContext.DEFAULT,
            state.segmentSuffix);

    t0 = System.nanoTime();
    writeDocValues(state, sortMap);// 写入DocValue
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write docValues");
    }

    t0 = System.nanoTime();
    writePoints(state, sortMap); //数字型写入
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write points");
    }

    t0 = System.nanoTime();
    vectorValuesConsumer.flush(state, sortMap);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write vectors");
    }

    // it's possible all docs hit non-aborting exceptions...
    t0 = System.nanoTime();
    storedFieldsConsumer.finish(maxDoc);// 啥都不做
    storedFieldsConsumer.flush(state, sortMap);// 将storeField刷入fdt文件中
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW",
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to finish stored fields");
    }

    t0 = System.nanoTime();
    Map<String, TermsHashPerField> fieldsToFlush = new HashMap<>();// 准备刷新的字段
    for (int i = 0; i < fieldHash.length; i++) {
      PerField perField = fieldHash[i];
      while (perField != null) {
        if (perField.invertState != null) {
          fieldsToFlush.put(perField.fieldInfo.name, perField.termsHashPerField);
        }
        perField = perField.next;
      }
    }

    try (NormsProducer norms =
        readState.fieldInfos.hasNorms()
            ? state.segmentInfo.getCodec().normsFormat().normsProducer(readState)
            : null) {
      NormsProducer normsMergeInstance = null;
      if (norms != null) {
        // Use the merge instance in order to reuse the same IndexInput for all terms
        normsMergeInstance = norms.getMergeInstance();
      } // 写tvd、tvm文件，然后在写tip、tim文件，doc.pox,pay（若有term类删除，里面会进行term的删除操作，将存活文档放入state.liveDocs中，硬删除）
      termsHash.flush(fieldsToFlush, state, sortMap, normsMergeInstance);// 进入的是FreqProxTermsWriter类,不是TermHash类
    }
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW",
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0)
              + " ms to write postings and finish vectors");
    }

    // Important to save after asking consumer to flush so
    // consumer can alter the FieldInfo* if necessary.  EG,
    // FreqProxTermsWriter does this with
    // FieldInfo.storePayload.
    t0 = System.nanoTime();// 写入fnm文件
    indexWriterConfig
        .getCodec()
        .fieldInfosFormat()
        .write(state.directory, state.segmentInfo, "", state.fieldInfos, IOContext.DEFAULT);
    if (infoStream.isEnabled("IW")) {
      infoStream.message(
          "IW", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0) + " ms to write fieldInfos");
    }

    return sortMap;
  }

  /** Writes all buffered points. */
  private void writePoints(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    PointsWriter pointsWriter = null; // 该segment全局共享一个writer
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {
        PerField perField = fieldHash[i];
        while (perField != null) {
          if (perField.pointValuesWriter != null) {
            // We could have initialized pointValuesWriter, but failed to write even a single doc
            if (perField.fieldInfo.getPointDimensionCount() > 0) {
              if (pointsWriter == null) {
                // lazy init
                PointsFormat fmt = state.segmentInfo.getCodec().pointsFormat();// Lucene86PointsFormat
                if (fmt == null) {
                  throw new IllegalStateException(
                      "field=\""
                          + perField.fieldInfo.name
                          + "\" was indexed as points but codec does not support points");
                }
                pointsWriter = fmt.fieldsWriter(state); // 每个segment会创建一个新的Lucene60PointsWriter， 该segment所有域都会共享这一个字段.segment完成会关闭这个writer
              }
              perField.pointValuesWriter.flush(state, sortMap, pointsWriter);// 需要进来看下
            }
            perField.pointValuesWriter = null;
          }
          perField = perField.next;
        }
      }
      if (pointsWriter != null) { // dim文件写完了
        pointsWriter.finish();
      }
      success = true;
    } finally {
      if (success) {//关闭dim文件
        IOUtils.close(pointsWriter);
      } else {
        IOUtils.closeWhileHandlingException(pointsWriter);
      }
    }
  }
  //     * writeDocValues函数遍历得到每个PerField，PerField中的docValuesWriter根据不同的Field值域类型被定义为BinaryDocValuesWriter、NumericDocValuesWriter、SortedDocValuesWriter、SortedNumericDocValuesWriter和SortedSetDocValuesWriter
  /** Writes all buffered doc values (called from {@link #flush}). */
  private void writeDocValues(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    DocValuesConsumer dvConsumer = null;
    boolean success = false;
    try {
      for (int i = 0; i < fieldHash.length; i++) {// 是个hash链表结构, segment内唯一的域
        PerField perField = fieldHash[i];
        while (perField != null) { // 轮询每个字段
          if (perField.docValuesWriter != null) {
            if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
              // BUG
              throw new AssertionError(
                  "segment="
                      + state.segmentInfo
                      + ": field=\""
                      + perField.fieldInfo.name
                      + "\" has no docValues but wrote them");
            }
            if (dvConsumer == null) {
              // lazy init
              DocValuesFormat fmt = state.segmentInfo.getCodec().docValuesFormat();
              dvConsumer = fmt.fieldsConsumer(state);// PerFieldDocValuesFormat$FieldsWriter
            }
            perField.docValuesWriter.flush(state, sortMap, dvConsumer);// 要进来看下，docvalue真正向磁盘写入
            perField.docValuesWriter = null;// refresh时置空了
          } else if (perField.fieldInfo != null
              && perField.fieldInfo.getDocValuesType() != DocValuesType.NONE) {
            // BUG
            throw new AssertionError(
                "segment="
                    + state.segmentInfo
                    + ": field=\""
                    + perField.fieldInfo.name
                    + "\" has docValues but did not write them");
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
        IOUtils.close(dvConsumer);
      } else {
        IOUtils.closeWhileHandlingException(dvConsumer);
      }
    }

    if (state.fieldInfos.hasDocValues() == false) {
      if (dvConsumer != null) {
        // BUG
        throw new AssertionError(
            "segment=" + state.segmentInfo + ": fieldInfos has no docValues but wrote them");
      }
    } else if (dvConsumer == null) {
      // BUG
      throw new AssertionError(
          "segment=" + state.segmentInfo + ": fieldInfos has docValues but did not wrote them");
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
            assert perField.norms != null : "field=" + fi.name;
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

  @SuppressWarnings("try")
  void abort() throws IOException {
    // finalizer will e.g. close any open files in the term vectors writer:
    try (Closeable finalizer = termsHash::abort) {
      storedFieldsConsumer.abort();
      vectorValuesConsumer.abort();
    } finally {
      Arrays.fill(fieldHash, null);
    }
  }

  private void rehash() {
    int newHashSize = (fieldHash.length * 2); // 扩容一倍
    assert newHashSize > fieldHash.length;

    PerField[] newHashArray = new PerField[newHashSize];

    // Rehash
    int newHashMask = newHashSize - 1;
    for (int j = 0; j < fieldHash.length; j++) {// 链表
      PerField fp0 = fieldHash[j];
      while (fp0 != null) {
        final int hashPos2 = fp0.fieldName.hashCode() & newHashMask; // 全部重新hash一次
        PerField nextFP0 = fp0.next; // 头插法
        fp0.next = newHashArray[hashPos2];
        newHashArray[hashPos2] = fp0;
        fp0 = nextFP0;
      }
    }

    fieldHash = newHashArray;
    hashMask = newHashMask;
  }

  /** Calls StoredFieldsWriter.startDocument, aborting the segment if it hits any exception. */
  private void startStoredFields(int docID) throws IOException {
    try {
      storedFieldsConsumer.startDocument(docID);
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }

  /** Calls StoredFieldsWriter.finishDocument, aborting the segment if it hits any exception. */
  private void finishStoredFields() throws IOException {
    try {
      storedFieldsConsumer.finishDocument();// 将store信息存入CompressingStoredFieldsWriter中
    } catch (Throwable th) {
      onAbortingException(th);
      throw th;
    }
  }
// 一个文档建立好了lucene索引
  void processDocument(
      int docID, Iterable<? extends IndexableField> document, boolean lastDocInBlock)
      throws IOException {
    // number of unique fields by name which need to be init in segment or full validation
    int fieldsNeedInitOrValidate = 0;// 不同名称的fieldname个数
    int indexedFieldCount = 0; // number of unique fields indexed with postings
    long fieldGen = nextFieldGen++;// 多少个文档了
    int docFieldIdx = 0;

    // NOTE: we need two passes here, in case there are
    // multi-valued fields, because we must process all
    // instances of a given field at once, since the
    // analyzer is free to reuse TokenStream across fields
    // (i.e., we cannot have more than one TokenStream
    // running "at once"):
    termsHash.startDocument(); // 每写完一个文档，都会清空一次TermVectorsConsumer里面缓存的上一个文档里面的所有字段信息
    startStoredFields(docID);// 也是蛮重要的。写fdt和fdx。若block刷新后，storedFieldWriter=null后，就是这里初始化一个新的文档
    try {
      // Handle the parent field first (before document fields). Its schema was already
      // set up in the constructor, so we only need to set the docID and trigger
      // initializeFieldInfo on the first encounter in this segment.
      if (parentPf != null && lastDocInBlock) {
        parentPf.schema.resetJustDocId(docID);
        if (parentPf.fieldInfo == null) {
          fields[fieldsNeedInitOrValidate++] = parentPf;
        }
      }

      // 1st pass over doc fields – verify that doc schema matches the index schema
      // build schema for each unique doc field
      for (IndexableField field : document) {
        final String fieldName = field.name();
        final IndexableFieldType fieldType = field.fieldType();
        PerField pf = getOrAddPerField(fieldName);
        if (pf == parentPf) {
          throw new IllegalArgumentException(
              "\"" + fieldName + "\" is a reserved field and should not be added to any document");
        }
        if (pf.fieldGen != fieldGen) { // first time we see this field in this document
          pf.fieldGen = fieldGen;
          pf.reset(docID, fieldType);
          if (pf.validatedFrozenFieldType == null) {
            fields[fieldsNeedInitOrValidate++] = pf;
          }
        } else if (pf.multiValueForcesDeoptimize(fieldType)) {
          // Multi-valued field with a different field type than the cached frozen type.
          // Drop the validated frozen field type to force the validation path.
          pf.validatedFrozenFieldType = null;
          fields[fieldsNeedInitOrValidate++] = pf;
        }
        if (docFieldIdx >= docFields.length) oversizeDocFields();
        docFields[docFieldIdx++] = pf;
        if (pf.validatedFrozenFieldType == null) {
          updateDocFieldSchema(fieldName, pf.schema, fieldType);
        }
      }

      if (fieldsNeedInitOrValidate > 0) {
        initAndValidateFields(fieldsNeedInitOrValidate);
      }

      // 2nd pass – index parent field first, then document fields
      if (parentPf != null && lastDocInBlock) {
        // parentField is currently a NumericDocValuesField so processField always returns false
        // here, but we check defensively in case the parent field representation changes.
        if (processField(docID, parentField, parentPf)) {
          fields[indexedFieldCount] = parentPf;
          indexedFieldCount++;
        }
      }
      // 2nd pass – document fields
      docFieldIdx = 0;
      for (IndexableField field : document) {// 未每个字段构建索引类型
        if (processField(docID, field, docFields[docFieldIdx])) {// fieldCount主要是是否进行分词
          fields[indexedFieldCount] = docFields[docFieldIdx];
          indexedFieldCount++;
        }
        docFieldIdx++;
      }
    } finally {
      if (hasHitAbortingException == false) { // 没有遇到抛出异常
        // Finish each indexed field name seen in the document:
        for (int i = 0; i < indexedFieldCount; i++) {// 有多少个域需要分词
          fields[i].finish(docID);// 主要统计该域的词信息，将TermVectorsConsumerPerField放到TermVectorsConsumer里面
        }
        finishStoredFields(); //写完所有域后，再整体将store信息存入CompressingStoredFieldsWriter中。若内存使用或者文档个数超过阈值了（产生一个chunk），会flush存储到fdt中
        // TODO: for broken docs, optimize termsHash.finishDocument
        try {
          termsHash.finishDocument(docID);// 主要是是清理nextTermsHash, 内存里超过128个文档会触发一次刷新操作
        } catch (Throwable th) {
          // Must abort, on the possibility that on-disk term
          // vectors are now corrupt:
          abortingExceptionConsumer.accept(th);
          throw th;
        }
      }
    }
  }

  private void initAndValidateFields(int fieldCount) throws IOException {
    // For each field, if it's the first time we see this field in this segment,
    // initialize its FieldInfo.
    // If we have already seen this field, verify that its schema
    // within the current doc matches its schema in the index.
    for (int i = 0; i < fieldCount; i++) {
      PerField pf = fields[i];
      if (pf.fieldInfo == null) {
        initializeFieldInfo(pf);
      } else {
        pf.schema.assertSameSchema(pf.fieldInfo);
      }
      pf.trySetValidatedFrozenFieldType();
    }
  }

  private void oversizeDocFields() {
    PerField[] newDocFields =
        new PerField
            [ArrayUtil.oversize(docFields.length + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
    System.arraycopy(docFields, 0, newDocFields, 0, docFields.length);
    docFields = newDocFields;
  }

  /**
   * Process a column-oriented batch of documents. Iterates the batch's columns, validates each
   * column's field type, and feeds values to the appropriate DocValuesWriter.
   *
   * @param baseDocID the segment-level doc ID for the first document in the batch (batch-local doc
   *     0 maps to this value)
   * @param columnBatch the column-oriented batch
   */
  void processBatch(int baseDocID, ColumnBatch columnBatch) throws IOException {
    final int numDocs = columnBatch.numDocs();
    boolean hasRowColumns = false;
    long batchGen = nextFieldGen++;

    // First pass: validate all columns and accumulate each field's schema. A batch may carry more
    // than one column for a field name to combine distinct features (see featureMask).
    int columnIdx = 0;
    int uniqueFieldCount = 0;
    for (Column column : columnBatch.columns()) {
      final String fieldName = column.name();
      final IndexableFieldType fieldType = column.fieldType();

      ColumnValidation.validateColumnHasIndexingFeature(fieldName, fieldType);

      switch (column) {
        case BinaryColumn bc -> ColumnValidation.validateBinaryColumn(bc, fieldType);
        case LongColumn lc -> ColumnValidation.validateLongColumn(lc, fieldType);
        case DictionaryColumn dc -> ColumnValidation.validateDictionaryColumn(dc, fieldType);
        case VectorColumn<?> vc -> ColumnValidation.validateVectorColumn(vc, fieldType);
        case TokenStreamColumn tsc -> ColumnValidation.validateTokenStreamColumn(tsc, fieldType);
        default ->
            throw new IllegalArgumentException(
                "Unknown column type: " + column.getClass().getName());
      }

      if (fieldType.stored() || fieldType.indexOptions() != IndexOptions.NONE) {
        hasRowColumns = true;
      }

      PerField pf = getOrAddPerField(fieldName);
      if (pf == parentPf) {
        throw new IllegalArgumentException(
            "\"" + fieldName + "\" is a reserved field and should not be added to any document");
      }
      if (columnIdx >= docFields.length) {
        oversizeDocFields();
      }
      docFields[columnIdx++] = pf;

      int columnFeatures = ColumnValidation.featureMask(fieldType);
      if (pf.fieldGen != batchGen) {
        // First column for this field name in this batch: start a fresh schema and feature set, and
        // collect the field once so its FieldInfo is initialized/validated after the loop.
        pf.fieldGen = batchGen;
        pf.columnFeatures = (byte) columnFeatures;
        pf.schema.reset(baseDocID);
        fields[uniqueFieldCount++] = pf;
      } else {
        // Each indexing feature must come from a single column for a given field name.
        int overlap = pf.columnFeatures & columnFeatures;
        if (overlap != 0) {
          throw new IllegalArgumentException(
              "ColumnBatch has multiple columns for field \""
                  + fieldName
                  + "\" claiming the same indexing feature "
                  + ColumnValidation.featureNames(overlap)
                  + "; each feature may appear in at most one column.");
        }
        pf.columnFeatures |= (byte) columnFeatures;
      }

      updateDocFieldSchema(fieldName, pf.schema, fieldType);
    }

    // Initialize field infos / validate schemas once per unique field name in the batch.
    if (uniqueFieldCount > 0) {
      initAndValidateFields(uniqueFieldCount);
    }

    if (parentPf != null) {
      processParentFieldForColumnBatch(baseDocID, numDocs);
    }

    // Row-oriented pass: stored fields and term inversion only. Uses fresh tuple cursors.
    if (hasRowColumns) {
      processRowColumns(baseDocID, numDocs, columnBatch.columns());
    }

    // Column-oriented pass: doc values, points, and vectors. Each column is asked for a fresh
    // cursor.
    int colOrientedIdx = 0;
    for (Column column : columnBatch.columns()) {
      final IndexableFieldType fieldType = column.fieldType();
      if (fieldType.docValuesType() == DocValuesType.NONE
          && fieldType.pointDimensionCount() == 0
          && fieldType.vectorDimension() == 0) {
        colOrientedIdx++;
        continue; // no column-oriented features
      }
      PerField pf = docFields[colOrientedIdx++];

      switch (column) {
        case LongColumn longCol -> processLongColumn(baseDocID, numDocs, longCol, pf, fieldType);
        case BinaryColumn binaryCol ->
            processBinaryColumn(baseDocID, numDocs, binaryCol, pf, fieldType);
        case DictionaryColumn dictCol ->
            processDictionaryColumn(baseDocID, numDocs, dictCol, pf, fieldType);
        case VectorColumn<?> vectorCol ->
            processVectorColumn(baseDocID, numDocs, vectorCol, pf, fieldType);
        default ->
            throw new IllegalArgumentException(
                "Unknown column type: " + column.getClass().getName());
      }
    }
  }

  private void processParentFieldForColumnBatch(int baseDocID, int numDocs) throws IOException {
    if (parentPf.fieldInfo == null) {
      initializeFieldInfo(parentPf);
      parentPf.trySetValidatedFrozenFieldType();
    }
    // Index the parent field for every document (each batch doc is an individual document,
    // not part of a block, so every doc is its own parent).
    final NumericDocValuesWriter parentWriter = (NumericDocValuesWriter) parentPf.docValuesWriter;
    parentWriter.addRepeatValues(baseDocID, parentField.numericValue().longValue(), numDocs);
  }

  /**
   * Processes row-oriented features (stored fields and term inversion) for columns that have stored
   * or indexed fields. The outer loop iterates every batch-local doc-id in {@code [0, numDocs)} for
   * row-eligible columns. Per-doc framing for stored fields and term inversion is gated on whether
   * the batch actually has any stored / indexed columns: {@code hasStored} gates {@code
   * startStoredFields}/{@code finishStoredFields} (parallel to {@code hasInverted} gating {@code
   * termsHash}), so an indexed-only batch never forces the segment's {@code StoredFieldsWriter}
   * into existence. {@link StoredFieldsConsumer#startDocument(int)} retroactively fills empty
   * frames for skipped doc-ids when a later doc actually writes a stored field, preserving doc
   * alignment across the {@code addDocument}/{@code addBatch} boundary. Doc values and points are
   * handled separately in the column-oriented pass.
   */
  private void processRowColumns(int baseDocID, int numDocs, Iterable<Column> columns)
      throws IOException {
    // Collect row-oriented columns. PerFields are sourced from processBatch's validation-pass
    // cache in docFields[] (indexed by original column position); rowPfIndices[] stores the
    // original index for each row-mode column so the inner loop can look up via
    // docFields[rowPfIndices[i]].
    int numRowCols = 0;
    ColumnFieldAdapter[] adapters = new ColumnFieldAdapter[4];
    int[] heads = new int[4];
    int[] rowPfIndices = new int[4];
    boolean hasInverted = false;
    boolean hasStored = false;

    int originalIdx = 0;
    for (Column column : columns) {
      IndexableFieldType fieldType = column.fieldType();
      if (fieldType.stored() == false && fieldType.indexOptions() == IndexOptions.NONE) {
        originalIdx++;
        continue;
      }
      if (numRowCols >= adapters.length) {
        adapters = ArrayUtil.grow(adapters, numRowCols + 1);
        heads = ArrayUtil.grow(heads, numRowCols + 1);
        rowPfIndices = ArrayUtil.grow(rowPfIndices, numRowCols + 1);
      }
      ColumnFieldAdapter adapter = ColumnFieldAdapter.create(column);
      adapters[numRowCols] = adapter;
      rowPfIndices[numRowCols] = originalIdx;
      heads[numRowCols] = adapter.nextDoc();
      if (fieldType.indexOptions() != IndexOptions.NONE) {
        hasInverted = true;
      }
      if (fieldType.stored()) {
        hasStored = true;
      }
      numRowCols++;
      originalIdx++;
    }

    // Row-dense outer loop: frame every doc in [0, numDocs). Column cursors stay sparse, but the
    // per-doc framing is fixed so stored fields and termsHash stay aligned with the reserved doc
    // ids even for docs that have no row-oriented values.
    for (int batchDocID = 0; batchDocID < numDocs; batchDocID++) {
      int segDocID = baseDocID + batchDocID;
      long fieldGen = nextFieldGen++;
      int indexedFieldCount = 0;

      if (hasInverted) {
        termsHash.startDocument();
      }
      if (hasStored) {
        startStoredFields(segDocID);
      }
      try {
        for (int i = 0; i < numRowCols; i++) {
          int head = heads[i];
          if (head < batchDocID) {
            throw new IllegalArgumentException(
                "Row column \""
                    + adapters[i].name()
                    + "\" returned out-of-order batch doc-id "
                    + head);
          }
          while (head == batchDocID) {
            PerField pf = docFields[rowPfIndices[i]];
            if (pf.fieldGen != fieldGen) {
              pf.fieldGen = fieldGen;
              pf.reset(segDocID, adapters[i].fieldType());
            }
            if (invertAndStore(segDocID, adapters[i], pf)) {
              fields[indexedFieldCount] = pf;
              indexedFieldCount++;
            }
            head = adapters[i].nextDoc();
          }
          heads[i] = head;
        }
      } finally {
        if (hasHitAbortingException == false) {
          for (int i = 0; i < indexedFieldCount; i++) {
            fields[i].finish(segDocID);
          }
          if (hasStored) {
            finishStoredFields();
          }
          if (hasInverted) {
            try {
              termsHash.finishDocument(segDocID);
            } catch (Throwable th) {
              abortingExceptionConsumer.accept(th);
              throw th;
            }
          }
        }
      }
    }

    // Any remaining cursor head after the outer loop is a doc-id >= numDocs.
    for (int i = 0; i < numRowCols; i++) {
      if (heads[i] != DocIdSetIterator.NO_MORE_DOCS) {
        throw new IllegalArgumentException(
            "Row column \""
                + adapters[i].name()
                + "\" returned batch doc-id "
                + heads[i]
                + " which is out of range [0, "
                + numDocs
                + ")");
      }
    }
  }

  private static void processLongColumn(
      int baseDocID, int numDocs, LongColumn column, PerField pf, IndexableFieldType fieldType)
      throws IOException {
    final DocValuesType dvType = fieldType.docValuesType();
    final boolean hasPoints = fieldType.pointDimensionCount() != 0;

    // Dense fast path: bulk-feed DV and/or points from a values cursor.
    if (column.density() == Column.Density.DENSE) {
      processDenseLongColumn(baseDocID, numDocs, column, pf, dvType, hasPoints);
      return;
    }

    // Sparse, DV-only: per-doc tuple-cursor path.
    if (hasPoints == false) {
      LongTupleCursor cursor = column.tuples();
      switch (dvType) {
        case NUMERIC -> {
          NumericDocValuesWriter writer = (NumericDocValuesWriter) pf.docValuesWriter;
          int batchDocID;
          while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            ColumnValidation.checkDocID(column, batchDocID, numDocs);
            writer.addValue(baseDocID + batchDocID, cursor.longValue());
          }
        }
        case SORTED_NUMERIC -> {
          SortedNumericDocValuesWriter writer = (SortedNumericDocValuesWriter) pf.docValuesWriter;
          int batchDocID;
          while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            ColumnValidation.checkDocID(column, batchDocID, numDocs);
            writer.addValue(baseDocID + batchDocID, cursor.longValue());
          }
        }
        // $CASES-OMITTED$
        default ->
            throw new IllegalArgumentException(
                "LongColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
      }
      return;
    }

    // Sparse, with points (+ optional numeric DV). Per-doc tuple cursor.
    final LongColumn.NumericKind kind = column.numericKind();
    final int byteWidth =
        (kind == LongColumn.NumericKind.INT || kind == LongColumn.NumericKind.FLOAT)
            ? Integer.BYTES
            : Long.BYTES;
    final byte[] pointScratch = new byte[byteWidth];
    final BytesRef pointBytesRef = new BytesRef(pointScratch);
    final PointValuesWriter pointWriter = pf.pointValuesWriter;
    final LongTupleCursor cursor = column.tuples();

    switch (dvType) {
      case NONE -> {
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          encodeSortablePointBytes(cursor.longValue(), kind, pointScratch);
          pointWriter.addPackedValue(baseDocID + batchDocID, pointBytesRef);
        }
      }
      case NUMERIC -> {
        NumericDocValuesWriter dvWriter = (NumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          long raw = cursor.longValue();
          dvWriter.addValue(segDocID, raw);
          encodeSortablePointBytes(raw, kind, pointScratch);
          pointWriter.addPackedValue(segDocID, pointBytesRef);
        }
      }
      case SORTED_NUMERIC -> {
        SortedNumericDocValuesWriter dvWriter = (SortedNumericDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          long raw = cursor.longValue();
          dvWriter.addValue(segDocID, raw);
          encodeSortablePointBytes(raw, kind, pointScratch);
          pointWriter.addPackedValue(segDocID, pointBytesRef);
        }
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "LongColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
    }
  }

  private static void encodeSortablePointBytes(
      long raw, LongColumn.NumericKind kind, byte[] scratch) {
    switch (kind) {
      case INT, FLOAT -> NumericUtils.intToSortableBytes((int) raw, scratch, 0);
      case LONG, DOUBLE -> NumericUtils.longToSortableBytes(raw, scratch, 0);
    }
  }

  /**
   * Bulk-feeds DV and/or points from a {@link LongValuesCursor} for a DENSE {@link LongColumn}.
   * Handles every {DV, points} combination; each consumer takes its own fresh cursor.
   *
   * <p>Process the DV pass first: it does minimal transformation on the backing values, so the
   * cursor's source array stays warm in cache for the heavier points pass that follows.
   */
  private static void processDenseLongColumn(
      int baseDocID,
      int numDocs,
      LongColumn column,
      PerField pf,
      DocValuesType dvType,
      boolean hasPoints)
      throws IOException {
    if (dvType != DocValuesType.NONE) {
      LongValuesCursor dvCursor = column.values();
      ColumnValidation.checkDenseCount(column, dvCursor.size(), numDocs);
      switch (dvType) {
        case NUMERIC -> {
          NumericDocValuesWriter writer = (NumericDocValuesWriter) pf.docValuesWriter;
          writer.addDenseValues(baseDocID, dvCursor);
        }
        case SORTED_NUMERIC -> {
          SortedNumericDocValuesWriter writer = (SortedNumericDocValuesWriter) pf.docValuesWriter;
          writer.addDenseValues(baseDocID, dvCursor);
        }
        // $CASES-OMITTED$
        default ->
            throw new IllegalArgumentException(
                "LongColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
      }
    }
    if (hasPoints) {
      LongValuesCursor pointsCursor = column.values();
      ColumnValidation.checkDenseCount(column, pointsCursor.size(), numDocs);
      final LongColumn.NumericKind kind = column.numericKind();
      if (kind == LongColumn.NumericKind.INT || kind == LongColumn.NumericKind.FLOAT) {
        pf.pointValuesWriter.addDense1DIntValues(baseDocID, pointsCursor);
      } else {
        pf.pointValuesWriter.addDense1DLongValues(baseDocID, pointsCursor);
      }
    }
  }

  private static void processBinaryColumn(
      int baseDocID, int numDocs, BinaryColumn column, PerField pf, IndexableFieldType fieldType)
      throws IOException {
    final DocValuesType dvType = fieldType.docValuesType();
    final boolean hasPoints = fieldType.pointDimensionCount() != 0;

    if (column.density() == Column.Density.DENSE) {
      processDenseBinaryColumn(baseDocID, numDocs, column, pf, dvType, hasPoints);
      return;
    }

    final PointValuesWriter pointWriter = hasPoints ? pf.pointValuesWriter : null;
    final ObjectTupleCursor<BytesRef> cursor = column.tuples();

    if (dvType == DocValuesType.NONE) {
      // Points only: bytes are passed through unchanged (caller is responsible for producing
      // sort-encoded bytes of the correct total length).
      int batchDocID;
      while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
        ColumnValidation.checkDocID(column, batchDocID, numDocs);
        pointWriter.addPackedValue(baseDocID + batchDocID, cursor.value());
      }
      return;
    }

    switch (dvType) {
      case BINARY -> {
        BinaryDocValuesWriter writer = (BinaryDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          BytesRef value = cursor.value();
          writer.addValue(segDocID, value);
          if (hasPoints) {
            pointWriter.addPackedValue(segDocID, value);
          }
        }
      }
      case SORTED -> {
        SortedDocValuesWriter writer = (SortedDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          BytesRef value = cursor.value();
          writer.addValue(segDocID, value);
          if (hasPoints) {
            pointWriter.addPackedValue(segDocID, value);
          }
        }
      }
      case SORTED_SET -> {
        SortedSetDocValuesWriter writer = (SortedSetDocValuesWriter) pf.docValuesWriter;
        int batchDocID;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          int segDocID = baseDocID + batchDocID;
          BytesRef value = cursor.value();
          writer.addValue(segDocID, value);
          if (hasPoints) {
            pointWriter.addPackedValue(segDocID, value);
          }
        }
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "BinaryColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
    }
  }

  private static void processDenseBinaryColumn(
      int baseDocID,
      int numDocs,
      BinaryColumn column,
      PerField pf,
      DocValuesType dvType,
      boolean hasPoints)
      throws IOException {
    // DV pass first: dense values cursor
    if (dvType != DocValuesType.NONE) {
      BytesRefValuesCursor dvCursor = column.values();
      ColumnValidation.checkDenseCount(column, dvCursor.size(), numDocs);
      switch (dvType) {
        case BINARY -> {
          BinaryDocValuesWriter writer = (BinaryDocValuesWriter) pf.docValuesWriter;
          for (int i = 0; i < dvCursor.size(); i++) {
            writer.addValue(baseDocID + i, dvCursor.nextValue());
          }
        }
        case SORTED -> {
          SortedDocValuesWriter writer = (SortedDocValuesWriter) pf.docValuesWriter;
          for (int i = 0; i < dvCursor.size(); i++) {
            writer.addValue(baseDocID + i, dvCursor.nextValue());
          }
        }
        case SORTED_SET -> {
          SortedSetDocValuesWriter writer = (SortedSetDocValuesWriter) pf.docValuesWriter;
          for (int i = 0; i < dvCursor.size(); i++) {
            writer.addValue(baseDocID + i, dvCursor.nextValue());
          }
        }
        // $CASES-OMITTED$
        default ->
            throw new IllegalArgumentException(
                "BinaryColumn \"" + column.name() + "\" has incompatible docValuesType: " + dvType);
      }
    }
    if (hasPoints) {
      // Points pass: fresh dense values cursor → bulk ND points add.
      BytesRefValuesCursor pc = column.values();
      ColumnValidation.checkDenseCount(column, pc.size(), numDocs);
      pf.pointValuesWriter.addDenseNDValues(baseDocID, pc);
    }
  }

  private static void processDictionaryColumn(
      int baseDocID,
      int numDocs,
      DictionaryColumn column,
      PerField pf,
      IndexableFieldType fieldType)
      throws IOException {
    final DocValuesType dvType = fieldType.docValuesType();
    final List<BytesRef> dict = column.dictionary();

    switch (dvType) {
      case SORTED -> {
        SortedDocValuesWriter writer = (SortedDocValuesWriter) pf.docValuesWriter;
        if (column.density() == Column.Density.DENSE) {
          OrdinalsCursor cursor = column.values();
          ColumnValidation.checkDenseCount(column, cursor.size(), numDocs);
          writer.addDenseOrdinalValues(baseDocID, dict, cursor);
        } else {
          writer.addOrdinalTuples(baseDocID, dict, column.tuples());
        }
      }
      case SORTED_SET -> {
        SortedSetDocValuesWriter writer = (SortedSetDocValuesWriter) pf.docValuesWriter;
        OrdinalsTupleCursor cursor = column.tuples();
        writer.addOrdinalTuples(baseDocID, dict, cursor);
      }
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "DictionaryColumn \""
                  + column.name()
                  + "\" has incompatible docValuesType: "
                  + dvType);
    }
  }

  @SuppressWarnings("unchecked")
  private static void processVectorColumn(
      int baseDocID, int numDocs, VectorColumn<?> column, PerField pf, IndexableFieldType fieldType)
      throws IOException {
    final VectorEncoding encoding = fieldType.vectorEncoding();
    final int dimension = fieldType.vectorDimension();
    final ObjectTupleCursor<?> cursor = column.tuples();
    int prevBatchDocID = -1;
    int consumed = 0;
    int batchDocID;
    switch (encoding) {
      case FLOAT32 -> {
        KnnFieldVectorsWriter<float[]> writer =
            (KnnFieldVectorsWriter<float[]>) pf.knnFieldVectorsWriter;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          ColumnValidation.checkVectorDocIDStrictlyIncreasing(column, batchDocID, prevBatchDocID);
          float[] vec = (float[]) cursor.value();
          ColumnValidation.checkVectorDimension(column, vec.length, dimension, batchDocID);
          writer.addValue(baseDocID + batchDocID, vec);
          prevBatchDocID = batchDocID;
          consumed++;
        }
      }
      case BYTE -> {
        KnnFieldVectorsWriter<byte[]> writer =
            (KnnFieldVectorsWriter<byte[]>) pf.knnFieldVectorsWriter;
        while ((batchDocID = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          ColumnValidation.checkDocID(column, batchDocID, numDocs);
          ColumnValidation.checkVectorDocIDStrictlyIncreasing(column, batchDocID, prevBatchDocID);
          byte[] vec = (byte[]) cursor.value();
          ColumnValidation.checkVectorDimension(column, vec.length, dimension, batchDocID);
          writer.addValue(baseDocID + batchDocID, vec);
          prevBatchDocID = batchDocID;
          consumed++;
        }
      }
    }
    if (column.density() == Column.Density.DENSE) {
      ColumnValidation.checkDenseCount(column, consumed, numDocs);
    }
  }

  private void initializeFieldInfo(PerField pf) throws IOException {
    // Create and add a new fieldInfo to fieldInfos for this segment.
    // During the creation of FieldInfo there is also verification of the correctness of all its
    // parameters.

    // If the fieldInfo doesn't exist in globalFieldNumbers for the whole index,
    // it will be added there.
    // If the field already exists in globalFieldNumbers (i.e. field present in other segments),
    // we check consistency of its schema with schema for the whole index.
    FieldSchema s = pf.schema;// 比如针对age，会来两个同名字段: indexed, doc_value的，这里schema就是合并成了统一额字段
    if (indexWriterConfig.getIndexSort() != null && s.docValuesType != DocValuesType.NONE) {
      final Sort indexSort = indexWriterConfig.getIndexSort();
      validateIndexSortDVType(indexSort, pf.fieldName, s.docValuesType);
    }
    if (s.vectorDimension != 0) {
      validateMaxVectorDimension(
          pf.fieldName,
          s.vectorDimension,
          indexWriterConfig.getCodec().knnVectorsFormat().getMaxDimensions(pf.fieldName));
    }
    FieldInfo fi =
        fieldInfos.add(
            new FieldInfo(
                pf.fieldName,
                -1,
                s.storeTermVector,
                s.omitNorms,
                // storePayloads is set up during indexing, if payloads were seen
                false,
                s.indexOptions,
                s.docValuesType,
                s.docValuesSkipIndex,
                -1,
                s.attributes,
                s.pointDimensionCount,
                s.pointIndexDimensionCount,
                s.pointNumBytes,
                s.vectorDimension,
                s.vectorEncoding,
                s.vectorSimilarityFunction,
                pf.fieldName.equals(fieldInfos.getSoftDeletesFieldName()),
                pf.fieldName.equals(fieldInfos.getParentFieldName())));
    pf.setFieldInfo(fi);
    if (fi.getIndexOptions() != IndexOptions.NONE) {
      pf.setInvertState();
    }
    DocValuesType dvType = fi.getDocValuesType();
    switch (dvType) {
      case NONE:
        break;
      case NUMERIC: // 啥时候是number，啥时候是SORTED_NUMERIC
        pf.docValuesWriter = new NumericDocValuesWriter(fi, bytesUsed);
        break;
      case BINARY:
        pf.docValuesWriter = new BinaryDocValuesWriter(fi, bytesUsed);
        break;
      case SORTED:
        pf.docValuesWriter =
            new SortedDocValuesWriter(fi, bytesUsed, docValuesBytePool, sharedIndexingScratch);
        break;
      case SORTED_NUMERIC:
        pf.docValuesWriter = new SortedNumericDocValuesWriter(fi, bytesUsed);
        break;
      case SORTED_SET:
        pf.docValuesWriter =
            new SortedSetDocValuesWriter(fi, bytesUsed, docValuesBytePool, sharedIndexingScratch);
        break;
      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
    if (fi.getPointDimensionCount() != 0) {
      pf.pointValuesWriter = new PointValuesWriter(bytesUsed, fi, sharedIndexingScratch);
    }
    if (fi.getVectorDimension() != 0) {
      try {
        pf.knnFieldVectorsWriter = vectorValuesConsumer.addField(fi);
      } catch (Throwable th) {
        onAbortingException(th);
        throw th;
      }
    }
  }
// 写一个Field，  fieldGen：这是该链第几个文档 fieldCount：是这个文档中的第几个域
  /** Index each field Returns {@code true}, if we are indexing a unique field with postings */
  private boolean processField(int docID, IndexableField field, PerField pf) throws IOException {
    boolean indexedField = invertAndStore(docID, field, pf);
    IndexableFieldType fieldType = field.fieldType();
    DocValuesType dvType = fieldType.docValuesType();
    if (dvType != DocValuesType.NONE) {
      indexDocValue(docID, pf, dvType, field);
    }
    if (fieldType.pointDimensionCount() != 0) {
      pf.pointValuesWriter.addPackedValue(docID, field.binaryValue());
    }
    if (fieldType.vectorDimension() != 0) {
      indexVectorValue(docID, pf, fieldType.vectorEncoding(), field);
    }
    return indexedField;
  }

  /**
   * Inverts indexed fields and writes stored fields. Shared by the single-doc row path ({@link
   * #processField}) and the column-batch row pass ({@link #processRowColumns}). Returns {@code
   * true} if this is a unique indexed field with postings.
   */
  private boolean invertAndStore(int docID, IndexableField field, PerField pf) throws IOException {
    IndexableFieldType fieldType = field.fieldType();
    boolean indexedField = false;

    if (fieldType.indexOptions() != IndexOptions.NONE) {// 只要不为NONE. 就会建倒排索引结构
      if (pf.first) { // first time we see this field in this doc// 这个文档中这个域不是重复写入？
        pf.invert(docID, field, true);
        pf.first = false;// 该域是该segment第一次写入，就得放进来
        indexedField = true;
      } else {
        pf.invert(docID, field, false);
      }
    }
    // 看es中，只有_source和id字段作为stored存储
    if (fieldType.stored()) {
      StoredValue storedValue = field.storedValue(); // 域的值
      if (storedValue == null) {
        throw new IllegalArgumentException("Cannot store a null value");
      } else if (storedValue.getType() == StoredValue.Type.STRING
          && storedValue.getStringValue().length() > IndexWriter.MAX_STORED_STRING_LENGTH) {
        throw new IllegalArgumentException(
            "stored field \""
                + field.name()
                + "\" is too large ("
                + storedValue.getStringValue().length()
                + " characters) to store");
      }
      try {//创建storeField, 只是将field值存放在CompressingStoredFieldsWriter的bufferedDocs中
        storedFieldsConsumer.writeField(pf.fieldInfo, storedValue); // 面向行的存储，docvalue是面向列的存储
      } catch (Throwable th) {
        onAbortingException(th);
        throw th;
      }
    }

    return indexedField;
  }

  /**
   * Returns a previously created {@link PerField}, absorbing the type information from {@link
   * FieldType}, and creates a new {@link PerField} if this field name wasn't seen yet.
   */
  private PerField getOrAddPerField(String fieldName) {
    final int hashPos = fieldName.hashCode() & hashMask;
    PerField pf = fieldHash[hashPos];
    while (pf != null && pf.fieldName.equals(fieldName) == false) {// 找到一个不为null的
      pf = pf.next;
    }
    if (pf == null) {
      // first time we encounter field with this name in this segment
      FieldSchema schema = new FieldSchema(fieldName);
      pf =
          new PerField(
              fieldName,
              indexCreatedVersionMajor,
              schema,
              indexWriterConfig.getSimilarity(),
              indexWriterConfig.getInfoStream(),
              indexWriterConfig.getAnalyzer());
      pf.next = fieldHash[hashPos];
      fieldHash[hashPos] = pf;
      totalFieldCount++;
      // At most 50% load factor:
      if (totalFieldCount >= fieldHash.length / 2) {
        rehash();
      }
      if (totalFieldCount > fields.length) {
        PerField[] newFields =
            new PerField
                [ArrayUtil.oversize(totalFieldCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(fields, 0, newFields, 0, fields.length);
        fields = newFields;
      }
    }
    return pf;
  }

  // update schema for field as seen in a particular document
  private static void updateDocFieldSchema(
      String fieldName, FieldSchema schema, IndexableFieldType fieldType) {
    if (fieldType.indexOptions() != IndexOptions.NONE) {
      schema.setIndexOptions(
          fieldType.indexOptions(), fieldType.omitNorms(), fieldType.storeTermVectors());
    } else {
      // TODO: should this be checked when a fieldType is created?
      verifyUnIndexedFieldType(fieldName, fieldType);
    }
    if (fieldType.docValuesType() != DocValuesType.NONE) {
      schema.setDocValues(fieldType.docValuesType(), fieldType.docValuesSkipIndexType());
    } else if (fieldType.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
      throw new IllegalArgumentException(
          "field '"
              + schema.name
              + "' cannot have docValuesSkipIndexType="
              + fieldType.docValuesSkipIndexType()
              + " without doc values");
    }
    if (fieldType.pointDimensionCount() != 0) {
      schema.setPoints(
          fieldType.pointDimensionCount(),
          fieldType.pointIndexDimensionCount(),
          fieldType.pointNumBytes());
    }
    if (fieldType.vectorDimension() != 0) {
      schema.setVectors(
          fieldType.vectorEncoding(),
          fieldType.vectorSimilarityFunction(),
          fieldType.vectorDimension());
    }
    if (fieldType.getAttributes() != null && fieldType.getAttributes().isEmpty() == false) {
      schema.updateAttributes(fieldType.getAttributes());
    }
  }

  private static void verifyUnIndexedFieldType(String name, IndexableFieldType ft) {
    if (ft.storeTermVectors()) {
      throw new IllegalArgumentException(
          "cannot store term vectors "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorPositions()) {
      throw new IllegalArgumentException(
          "cannot store term vector positions "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorOffsets()) {
      throw new IllegalArgumentException(
          "cannot store term vector offsets "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
    if (ft.storeTermVectorPayloads()) {
      throw new IllegalArgumentException(
          "cannot store term vector payloads "
              + "for a field that is not indexed (field=\""
              + name
              + "\")");
    }
  }

  private static void validateMaxVectorDimension(
      String fieldName, int vectorDim, int maxVectorDim) {
    if (vectorDim > maxVectorDim) {
      throw new IllegalArgumentException(
          "Field ["
              + fieldName
              + "] vector's dimensions must be <= ["
              + maxVectorDim
              + "]; got "
              + vectorDim);
    }
  }

  private void validateIndexSortDVType(Sort indexSort, String fieldToValidate, DocValuesType dvType)
      throws IOException {
    for (SortField sortField : indexSort.getSort()) {
      IndexSorter sorter = sortField.getIndexSorter();
      if (sorter == null) {
        throw new IllegalStateException("Cannot sort index with sort order " + sortField);
      }
      sorter.getDocComparator(
          new DocValuesLeafReader() {
            @Override
            public NumericDocValues getNumericDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.NUMERIC) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be NUMERIC but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptyNumeric();
            }

            @Override
            public BinaryDocValues getBinaryDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.BINARY) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be BINARY but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptyBinary();
            }

            @Override
            public SortedDocValues getSortedDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySorted();
            }

            @Override
            public SortedNumericDocValues getSortedNumericDocValues(String field) {
              if (Objects.equals(field, fieldToValidate)
                  && dvType != DocValuesType.SORTED_NUMERIC) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED_NUMERIC but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySortedNumeric();
            }

            @Override
            public SortedSetDocValues getSortedSetDocValues(String field) {
              if (Objects.equals(field, fieldToValidate) && dvType != DocValuesType.SORTED_SET) {
                throw new IllegalArgumentException(
                    "SortField "
                        + sortField
                        + " expected field ["
                        + field
                        + "] to be SORTED_SET but it is ["
                        + dvType
                        + "]");
              }
              return DocValues.emptySortedSet();
            }

            @Override
            public FieldInfos getFieldInfos() {
              throw new UnsupportedOperationException();
            }
          },
          0);
    }
  }

  /** Called from processDocument to index one field's doc value */
  private void indexDocValue(int docID, PerField fp, DocValuesType dvType, IndexableField field) {
    switch (dvType) {
      case NUMERIC:
        if (field.numericValue() == null) { // 每次刷新到磁盘时会清空该对象
          throw new IllegalArgumentException(
              "field=\"" + fp.fieldInfo.name + "\": null value not allowed");
        }
        ((NumericDocValuesWriter) fp.docValuesWriter)
            .addValue(docID, field.numericValue().longValue());
        break;

      case BINARY:
        ((BinaryDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;
       // 看起来存储的是二进制
      case SORTED:
        ((SortedDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case SORTED_NUMERIC:
        ((SortedNumericDocValuesWriter) fp.docValuesWriter)
            .addValue(docID, field.numericValue().longValue());
        break;

      case SORTED_SET:// 所有文档所有域全局唯一
        ((SortedSetDocValuesWriter) fp.docValuesWriter).addValue(docID, field.binaryValue());
        break;

      case NONE:
      default:
        throw new AssertionError("unrecognized DocValues.Type: " + dvType);
    }
  }
  // 写入向量字段
  @SuppressWarnings("unchecked")
  private void indexVectorValue(
      int docID, PerField pf, VectorEncoding vectorEncoding, IndexableField field)
      throws IOException {
    switch (vectorEncoding) {
      case BYTE ->
          ((KnnFieldVectorsWriter<byte[]>) pf.knnFieldVectorsWriter)
              .addValue(docID, ((KnnByteVectorField) field).vectorValue());
      case FLOAT32 ->
          ((KnnFieldVectorsWriter<float[]>) pf.knnFieldVectorsWriter)
              .addValue(docID, ((KnnFloatVectorField) field).vectorValue());
    }
  }

  /** Returns a previously created {@link PerField}, or null if this field name wasn't seen yet. */
  private PerField getPerField(String name) {// invert在索引字段时候会自动传递进来，若字段设置了非IndexOptions.NONE， 那么invert一定会传递进来
    final int hashPos = name.hashCode() & hashMask;//计算哈希值
    PerField fp = fieldHash[hashPos]; //找到哈希表中对应的位置
    while (fp != null && !fp.fieldName.equals(name)) { //链式哈希表(碰撞发）
      fp = fp.next;
    }
    return fp;
  }

  @Override
  public long ramBytesUsed() {
    return bytesUsed.get()
        + storedFieldsConsumer.accountable.ramBytesUsed()
        + termVectorsWriter.accountable.ramBytesUsed()
        + vectorValuesConsumer.getAccountable().ramBytesUsed();
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return List.of(
        storedFieldsConsumer.accountable,
        termVectorsWriter.accountable,
        vectorValuesConsumer.getAccountable());
  }
  // segment内共享，segment完成后就清空
  /** NOTE: not static: accesses at least docState, termsHash. */
  private final class PerField implements Comparable<PerField> {
    final String fieldName;
    final int indexCreatedVersionMajor;
    final FieldSchema schema;
    FieldInfo fieldInfo;
    final Similarity similarity;
    // 只有设置了倒排索引，才会给这些变量赋值
    FieldInvertState invertState;// 统计倒排信息，每个field都会独享一个(每个文档统计使用前，都会清空该字段值)，在进入域分词的时候会被清空
    TermsHashPerField termsHashPerField;// FreqProxTermsWriterPerField, 里面包含了TermVectorsConsumer和TermVectorsConsumerPerField

    // Non-null if this field ever had doc values in this
    // segment:
    DocValuesWriter<?> docValuesWriter; // 一个段该域所有文档共享一个该字段。每次es refresh刷新到磁盘时会清空。第一次写入时就会构建该对象SortedSetDocValuesWriter
    // 会在放入内存阶段初始化
    // Non-null if this field ever had points in this segment:
    PointValuesWriter pointValuesWriter; // 一个段该域拥有这一个， flush完后，就会清空

    // Non-null if this field had vectors in this segment
    KnnFieldVectorsWriter<?> knnFieldVectorsWriter;

    /** We use this to know when a PerField is seen for the first time in the current document. */
    long fieldGen = -1; //这是该链第几个文档，作用就是判断在该文档中该域第几次写入

    /**
     * Bit set of indexing features (as returned by {@link ColumnValidation#featureMask}) already
     * claimed for this field name within the current {@code addBatch} call. A column batch may
     * carry several columns for one field name to combine distinct features (e.g. a stored column
     * plus an inverted column), but each feature — inversion, stored, doc values, points, vectors —
     * must come from a single column. Reset to 0 on the first sighting of the name in a batch
     * (keyed off {@code fieldGen}).
     */
    byte columnFeatures;

    // Used by the hash table
    PerField next;

    // Lazy init'd:
    NormValuesWriter norms;

    // reused   segment级别同一个Field共享的
    TokenStream tokenStream;
    private final InfoStream infoStream;
    private final Analyzer analyzer;
    private boolean first; // first in a document

    /**
     * Allows IndexingChain to skip schema validation if fields keep using the same frozen field
     * type
     */
    private FieldType validatedFrozenFieldType;

    private IndexableFieldType candidateFieldType;

    PerField(
        String fieldName,
        int indexCreatedVersionMajor,
        FieldSchema schema,
        Similarity similarity,
        InfoStream infoStream,
        Analyzer analyzer) {
      this.fieldName = fieldName;
      this.indexCreatedVersionMajor = indexCreatedVersionMajor;
      this.schema = schema;
      this.similarity = similarity;
      this.infoStream = infoStream;
      this.analyzer = analyzer;
    }

    void reset(int docId, IndexableFieldType fieldType) {
      first = true;
      candidateFieldType = fieldType;
      if (fieldType == validatedFrozenFieldType) {
        schema.resetJustDocId(docId);
      } else {
        // Encountered new FieldType. Deoptimize the schema validation skip.
        validatedFrozenFieldType = null;
        schema.reset(docId);
      }
    }

    boolean multiValueForcesDeoptimize(IndexableFieldType fieldType) {
      return validatedFrozenFieldType != null && fieldType != validatedFrozenFieldType;
    }

    void trySetValidatedFrozenFieldType() {
      assert fieldInfo != null;
      if (candidateFieldType instanceof FieldType ft && ft.isFrozen()) {
        validatedFrozenFieldType = ft;
      }
      candidateFieldType = null;
    }

    void setFieldInfo(FieldInfo fieldInfo) {
      assert this.fieldInfo == null;
      this.fieldInfo = fieldInfo;
    }

    void setInvertState() {// 倒排索引参数设置，都在这里给设置了
      invertState =
          new FieldInvertState(
              indexCreatedVersionMajor, fieldInfo.name, fieldInfo.getIndexOptions());
      termsHashPerField = termsHash.addField(invertState, fieldInfo); // 产生FreqProxTermsWriterPerField及TermVectorsConsumerPerField
      if (fieldInfo.omitsNorms() == false) {
        assert norms == null;
        // Even if no documents actually succeed in setting a norm, we still write norms for this
        // segment
        norms = new NormValuesWriter(fieldInfo, bytesUsed);
      }
      if (fieldInfo.hasTermVectors()) {
        termVectorsWriter.setHasVectors();
      }
    }

    @Override
    public int compareTo(PerField other) {
      return this.fieldName.compareTo(other.fieldName);
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
            throw new IllegalStateException(
                "Similarity " + similarity + " return 0 for non-empty field");
          }
        }
        norms.addValue(docID, normValue);
      }
      termsHashPerField.finish(); // FreqProxTermsWriterPerField
    }

    /**
     * Inverts one field for one document; first is true if this is the first time we are seeing
     * this field name in this document.
     */
    public void invert(int docID, IndexableField field, boolean first) throws IOException {
      assert field.fieldType().indexOptions().subsumes(IndexOptions.DOCS);

      if (first) {// 在这个文档中第一次看到这个域
        // First time we're seeing this field (indexed) in this document
        invertState.reset(); // 每次写入一个新的文档，这里都会被清空
      }

      switch (field.invertableType()) {
        case BINARY:
          invertTerm(docID, field, first);
          break;
        case TOKEN_STREAM:// 一般都是跑到这里
          invertTokenStream(docID, field, first);
          break;
        default:
          throw new AssertionError();
      }
    }

    private void invertTokenStream(int docID, IndexableField field, boolean first)
        throws IOException {
      final boolean analyzed = field.fieldType().tokenized() && analyzer != null;
      /*
       * To assist people in tracking down problems in analysis components, we wish to write the field name to the
       * infostream
       * when we fail. We expect some caller to eventually deal with the real exception, so we don't want any 'catch'
       *  clauses,
       * but rather a finally that takes note of the problem.
       */
      boolean succeededInProcessingField = false;
      try (TokenStream stream = tokenStream = field.tokenStream(analyzer, tokenStream)) {// 进行了分词，跑入了Field.tokenStream()
        // reset the TokenStream to the first token
        stream.reset();
        invertState.setAttributeSource(stream); // 设置放到invertState中，可以获取很多分词后的参数信息，是lucene自带特性
        termsHashPerField.start(field, first); // 这里会针对FreqProxTermsWriterPerField

        while (stream.incrementToken()) {// 这样是循环每个词的

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
              throw new IllegalArgumentException(
                  "first position increment must be > 0 (got 0) for field '" + field.name() + "'");
            } else if (posIncr < 0) {
              throw new IllegalArgumentException(
                  "position increment must be >= 0 (got "
                      + posIncr
                      + ") for field '"
                      + field.name()
                      + "'");
            } else {
              throw new IllegalArgumentException(
                  "position overflowed Integer.MAX_VALUE (got posIncr="
                      + posIncr
                      + " lastPosition="
                      + invertState.lastPosition
                      + " position="
                      + invertState.position
                      + ") for field '"
                      + field.name()
                      + "'");
            }
          } else if (invertState.position > IndexWriter.MAX_POSITION) {
            throw new IllegalArgumentException(
                "position "
                    + invertState.position
                    + " is too large for field '"
                    + field.name()
                    + "': max allowed position is "
                    + IndexWriter.MAX_POSITION);
          }
          invertState.lastPosition = invertState.position;
          if (posIncr == 0) {
            invertState.numOverlap++;
          }

          int startOffset = invertState.offset + invertState.offsetAttribute.startOffset(); // 词的起始位置
          int endOffset = invertState.offset + invertState.offsetAttribute.endOffset(); // 这个词的末尾
          if (startOffset < invertState.lastStartOffset || endOffset < startOffset) {
            throw new IllegalArgumentException(
                "startOffset must be non-negative, and endOffset must be >= startOffset, and offsets must not go "
                    + "backwards "
                    + "startOffset="
                    + startOffset
                    + ",endOffset="
                    + endOffset
                    + ",lastStartOffset="
                    + invertState.lastStartOffset
                    + " for field '"
                    + field.name()
                    + "'");
          }
          invertState.lastStartOffset = startOffset;

          try {
            if (fieldInfo.isTermDocField()) {
              invertState.length = Math.addExact(invertState.length, 1);
            } else {
              invertState.length =// 相加
                  Math.addExact(
                      invertState.length, invertState.termFreqAttribute.getTermFrequency());
            }
          } catch (ArithmeticException ae) {
            throw new IllegalArgumentException(
                "too many tokens for field \"" + field.name() + "\"", ae);
          }

          // System.out.println("  term=" + invertState.termAttribute);

          // If we hit an exception in here, we abort
          // all buffered documents since the last
          // flush, on the likelihood that the
          // internal state of the terms hash is now
          // corrupt and should not be flushed to a
          // new segment:
          try {
            termsHashPerField.add(invertState.termAttribute.getBytesRef(), docID);
          } catch (MaxBytesLengthExceededException e) {
            BytesRef bigTerm = invertState.termAttribute.getBytesRef();
            byte[] prefix =
                ArrayUtil.copyOfSubArray(bigTerm.bytes, bigTerm.offset, bigTerm.offset + 30);
            String msg =
                "Document contains at least one immense term in field=\""
                    + fieldInfo.name
                    + "\" (whose UTF8 encoding is longer than the max length "
                    + IndexWriter.MAX_TERM_LENGTH
                    + "), all of which were skipped.  Please correct the analyzer to not produce such terms.  The "
                    + "prefix of the first immense term is: '"
                    + Arrays.toString(prefix)
                    + "...', original message: "
                    + e.getMessage();
            if (infoStream.isEnabled("IW")) {
              infoStream.message("IW", "ERROR: " + msg);
            }
            // Document will be deleted above:
            throw new IllegalArgumentException(msg, e);
          } catch (TermsHashPerField.DuplicateTermException e) {
            throw new IllegalArgumentException(
                "Document update skipped due to duplicate termdoc term", e);
          } catch (Throwable th) {
            onAbortingException(th);
            throw th;
          }
        }

        // trigger streams to perform end-of-stream operations
        stream.end();

        // TODO: maybe add some safety? then again, it's already checked
        // when we come back around to the field...
        invertState.position += invertState.posIncrAttribute.getPositionIncrement();
        invertState.offset += invertState.offsetAttribute.endOffset();

        /* if there is an exception coming through, we won't set this to true here:*/
        succeededInProcessingField = true;
      } finally {
        if (!succeededInProcessingField && infoStream.isEnabled("DW")) {
          infoStream.message(
              "DW", "An exception was thrown while processing field " + fieldInfo.name);
        }
      }

      if (analyzed) {// 若分词的话，
        invertState.position += analyzer.getPositionIncrementGap(fieldInfo.name);
        invertState.offset += analyzer.getOffsetGap(fieldInfo.name);
      }
    }

    private void invertTerm(int docID, IndexableField field, boolean first) throws IOException {
      BytesRef binaryValue = field.binaryValue();
      if (binaryValue == null) {
        throw new IllegalArgumentException(
            "Field "
                + field.name()
                + " returns TERM for invertableType() and null for binaryValue(), which is illegal");
      }
      final IndexableFieldType fieldType = field.fieldType();
      if (fieldType.tokenized()
          || fieldType.indexOptions().subsumes(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
          || fieldType.storeTermVectorPositions()
          || fieldType.storeTermVectorOffsets()
          || fieldType.storeTermVectorPayloads()) {
        throw new IllegalArgumentException(
            "Fields that are tokenized or index proximity data must produce a non-null TokenStream, but "
                + field.name()
                + " did not");
      }
      invertState.setAttributeSource(null);
      invertState.position++;
      invertState.length++;
      termsHashPerField.start(field, first);
      invertState.length = Math.addExact(invertState.length, 1);
      try {
        termsHashPerField.add(binaryValue, docID);
      } catch (MaxBytesLengthExceededException e) {
        byte[] prefix =
            ArrayUtil.copyOfSubArray(
                binaryValue.bytes, binaryValue.offset, binaryValue.offset + 30);
        String msg =
            "Document contains at least one immense term in field=\""
                + fieldInfo.name
                + "\" (whose length is longer than the max length "
                + IndexWriter.MAX_TERM_LENGTH
                + "), all of which were skipped. The prefix of the first immense term is: '"
                + Arrays.toString(prefix)
                + "...'";
        if (infoStream.isEnabled("IW")) {
          infoStream.message("IW", "ERROR: " + msg);
        }
        throw new IllegalArgumentException(msg, e);
      }
    }
  }

  DocIdSetIterator getHasDocValues(String field) {
    PerField perField = getPerField(field);
    if (perField != null) {
      if (perField.docValuesWriter != null) {
        if (perField.fieldInfo.getDocValuesType() == DocValuesType.NONE) {
          return null;
        }

        return perField.docValuesWriter.getDocValues();// 将跑到 NumericDocValuesWriter.getDocValues() ， 获取这个还未刷新的segment中包含_soft_delete
      }
    }
    return null;
  }

  private static class IntBlockAllocator extends IntBlockPool.Allocator {
    private final Counter bytesUsed;

    IntBlockAllocator(Counter bytesUsed) {
      super(IntBlockPool.INT_BLOCK_SIZE);
      this.bytesUsed = bytesUsed;
    }

    /* Allocate another int[] from the shared pool */
    @Override
    public int[] getIntBlock() {
      int[] b = new int[IntBlockPool.INT_BLOCK_SIZE];
      bytesUsed.addAndGet(IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES);
      return b;
    }

    @Override
    public void recycleIntBlocks(int[][] blocks, int offset, int length) {
      bytesUsed.addAndGet(-(length * (IntBlockPool.INT_BLOCK_SIZE * Integer.BYTES)));
    }
  }

  /**
   * A schema of the field in the current document. With every new document this schema is reset. As
   * the document fields are processed, we update the schema with options encountered in this
   * document. Once the processing for the document is done, we compare the built schema of the
   * current document with the corresponding FieldInfo (FieldInfo is built on a first document in
   * the segment where we encounter this field). If there is inconsistency, we raise an error. This
   * ensures that a field has the same data structures across all documents.
   */
  private static final class FieldSchema {
    private final String name;
    private int docID = 0;
    private final Map<String, String> attributes = new HashMap<>();
    private boolean omitNorms = false;
    private boolean storeTermVector = false;
    private IndexOptions indexOptions = IndexOptions.NONE;
    private DocValuesType docValuesType = DocValuesType.NONE;
    private DocValuesSkipIndexType docValuesSkipIndex = DocValuesSkipIndexType.NONE;
    private int pointDimensionCount = 0;
    private int pointIndexDimensionCount = 0;
    private int pointNumBytes = 0;
    private int vectorDimension = 0;
    private VectorEncoding vectorEncoding = VectorEncoding.FLOAT32;
    private VectorSimilarityFunction vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;

    private static final String errMsg =
        "Inconsistency of field data structures across documents for field ";

    FieldSchema(String name) {
      this.name = name;
    }

    private void assertSame(String label, boolean expected, boolean given) {
      if (expected != given) {
        raiseNotSame(label, expected, given);
      }
    }

    private void assertSame(String label, int expected, int given) {
      if (expected != given) {
        raiseNotSame(label, expected, given);
      }
    }

    private <T extends Enum<?>> void assertSame(String label, T expected, T given) {
      if (expected != given) {
        raiseNotSame(label, expected, given);
      }
    }

    private void raiseNotSame(String label, Object expected, Object given) {
      throw new IllegalArgumentException(
          errMsg
              + "["
              + name
              + "] of doc ["
              + docID
              + "]. "
              + label
              + ": expected '"
              + expected
              + "', but it has '"
              + given
              + "'.");
    }

    void updateAttributes(Map<String, String> attrs) {
      attrs.forEach((k, v) -> this.attributes.put(k, v));
    }

    void setIndexOptions(
        IndexOptions newIndexOptions, boolean newOmitNorms, boolean newStoreTermVector) {
      if (indexOptions == IndexOptions.NONE) {
        indexOptions = newIndexOptions;
        omitNorms = newOmitNorms;
        storeTermVector = newStoreTermVector;
      } else {
        assertSame("index options", indexOptions, newIndexOptions);
        assertSame("omit norms", omitNorms, newOmitNorms);
        assertSame("store term vector", storeTermVector, newStoreTermVector);
      }
    }

    void setDocValues(
        DocValuesType newDocValuesType, DocValuesSkipIndexType newDocValuesSkipIndex) {
      if (docValuesType == DocValuesType.NONE) {
        this.docValuesType = newDocValuesType;
        this.docValuesSkipIndex = newDocValuesSkipIndex;
      } else {
        assertSame("doc values type", docValuesType, newDocValuesType);
        assertSame("doc values skip index type", docValuesSkipIndex, newDocValuesSkipIndex);
      }
    }

    void setPoints(int dimensionCount, int indexDimensionCount, int numBytes) {
      if (pointIndexDimensionCount == 0) {
        pointDimensionCount = dimensionCount;
        pointIndexDimensionCount = indexDimensionCount;
        pointNumBytes = numBytes;
      } else {
        assertSame("point dimension", pointDimensionCount, dimensionCount);
        assertSame("point index dimension", pointIndexDimensionCount, indexDimensionCount);
        assertSame("point num bytes", pointNumBytes, numBytes);
      }
    }

    void setVectors(
        VectorEncoding encoding, VectorSimilarityFunction similarityFunction, int dimension) {
      if (vectorDimension == 0) {
        this.vectorEncoding = encoding;
        this.vectorSimilarityFunction = similarityFunction;
        this.vectorDimension = dimension;
      } else {
        assertSame("vector encoding", vectorEncoding, encoding);
        assertSame("vector similarity function", vectorSimilarityFunction, similarityFunction);
        assertSame("vector dimension", vectorDimension, dimension);
      }
    }

    void resetJustDocId(int doc) {
      docID = doc;
    }

    void reset(int doc) {
      resetJustDocId(doc);
      omitNorms = false;
      storeTermVector = false;
      indexOptions = IndexOptions.NONE;
      docValuesType = DocValuesType.NONE;
      pointDimensionCount = 0;
      pointIndexDimensionCount = 0;
      pointNumBytes = 0;
      vectorDimension = 0;
      vectorEncoding = VectorEncoding.FLOAT32;
      vectorSimilarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    }

    void assertSameSchema(FieldInfo fi) {
      assertSame("index options", fi.getIndexOptions(), indexOptions);
      assertSame("omit norms", fi.omitsNorms(), omitNorms);
      assertSame("store term vector", fi.hasTermVectors(), storeTermVector);
      assertSame("doc values type", fi.getDocValuesType(), docValuesType);
      assertSame("doc values skip index type", fi.docValuesSkipIndexType(), docValuesSkipIndex);
      assertSame(
          "vector similarity function", fi.getVectorSimilarityFunction(), vectorSimilarityFunction);
      assertSame("vector encoding", fi.getVectorEncoding(), vectorEncoding);
      assertSame("vector dimension", fi.getVectorDimension(), vectorDimension);
      assertSame("point dimension", fi.getPointDimensionCount(), pointDimensionCount);
      assertSame(
          "point index dimension", fi.getPointIndexDimensionCount(), pointIndexDimensionCount);
      assertSame("point num bytes", fi.getPointNumBytes(), pointNumBytes);
    }
  }
}
