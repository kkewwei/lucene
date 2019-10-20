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
import java.util.Map;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
// 整个segment共享一个
class TermVectorsConsumer extends TermsHash {
  TermVectorsWriter writer; // CompressingTermVectorsWriter, tvd文件。写完一个segment 落盘后就置空

  /** Scratch term used by TermVectorsConsumerPerField.finishDocument. */
  final BytesRef flushTerm = new BytesRef();

  final DocumentsWriterPerThread docWriter;

  /** Used by TermVectorsConsumerPerField when serializing
   *  the term vectors. */
  final ByteSliceReader vectorSliceReaderPos = new ByteSliceReader();
  final ByteSliceReader vectorSliceReaderOff = new ByteSliceReader();

  boolean hasVectors;
  private int numVectorFields;// perFields的个数, 该文档总共有几个field，每个文档写入时置位
  int lastDocID;// 存储的上一个docId
  private TermVectorsConsumerPerField[] perFields = new TermVectorsConsumerPerField[1];// 每个域都会有一个。每个文档写完就会清空一次
  // 整个segment共享一个
  TermVectorsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter, false, null);
    this.docWriter = docWriter;
  }
  // 在segment产生时就会调用
  @Override
  void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap, NormsProducer norms) throws IOException {
    if (writer != null) {
      int numDocs = state.segmentInfo.maxDoc();
      assert numDocs > 0;
      // At least one doc in this run had term vectors enabled
      try {
        fill(numDocs); // 没啥用
        assert state.segmentInfo != null;
        writer.finish(state.fieldInfos, numDocs); // 完成tvd文件的flush
      } finally {
        IOUtils.close(writer);
        writer = null;
        lastDocID = 0;
        hasVectors = false;
      }
    }
  }

  /** Fills in no-term-vectors for all docs we haven't seen
   *  since the last doc that had term vectors. */
  void fill(int docID) throws IOException {
    while(lastDocID < docID) { //始终不会进入，官宣确定了1千万条写入都不会跑到这里
      writer.startDocument(0);
      writer.finishDocument();
      lastDocID++;
    }
  }
  // writer：每个segment产生后落盘， 那么该对象就被置空
  void initTermVectorsWriter() throws IOException {
    if (writer == null) { // initTermVectorsWriter
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(docWriter.directory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }
  // 对一个文件的所有字段进行了lenece索引结构建立后，才跑到这里
  @Override
  void finishDocument(int docID) throws IOException {

    if (!hasVectors) {
      return;
    }
    // 实际是TermVectorsConsumerPerField.compare(TermVectorsConsumerPerField)按照fieldName进行比较
    // Fields in term vectors are UTF16 sorted:
    ArrayUtil.introSort(perFields, 0, numVectorFields); // 基于fieldName进行了排序

    initTermVectorsWriter(); //构建//tcx,tvd存储文件

    fill(docID); // 每写完一个文档就会调用一次，没见执行过

    // Append term vectors to the real outputs:
    writer.startDocument(numVectorFields); // 产生一个DocData空壳
    for (int i = 0; i < numVectorFields; i++) {
      perFields[i].finishDocument(); // 会把perFields里面的数据放入CompressingTermVectorsWriter的positionsBuf中
    } // 可能会触发flush
    writer.finishDocument(); // CompressingTermVectorsWriter，会进行chunk检查，是否文档数大于128个而触发刷新

    assert lastDocID == docID: "lastDocID=" + lastDocID + " docID=" + docID;

    lastDocID++;

    super.reset(); // 清空的是TermVectorsConsumer的
    resetFields(); // perFields里面的值已经转移出来了
  }

  @Override
  public void abort() {
    hasVectors = false;
    try {
      super.abort();
    } finally {
      IOUtils.closeWhileHandlingException(writer);
      writer = null;
      lastDocID = 0;
      reset();
    }
  }

  void resetFields() {
    Arrays.fill(perFields, null); // don't hang onto stuff from previous doc
    numVectorFields = 0;
  }

  @Override
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) {
    return new TermVectorsConsumerPerField(invertState, this, fieldInfo);
  }
  // 在单个文档写文的时候，每个域都会调用这里
  void addFieldToFlush(TermVectorsConsumerPerField fieldToFlush) {
    if (numVectorFields == perFields.length) { // 满了，该扩容了。
      int newSize = ArrayUtil.oversize(numVectorFields + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      TermVectorsConsumerPerField[] newArray = new TermVectorsConsumerPerField[newSize];
      System.arraycopy(perFields, 0, newArray, 0, numVectorFields);
      perFields = newArray;
    }

    perFields[numVectorFields++] = fieldToFlush;
  }

  @Override
  void startDocument() {
    resetFields();
    numVectorFields = 0;
  }
}
