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

import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
 // 每个segment新产生一个，随着DefaultIndexingChain新产生而产生一个新的
class StoredFieldsConsumer {
  final DocumentsWriterPerThread docWriter; //   
  StoredFieldsWriter writer; // 写fdx 和fdt文件的地方   CompressingStoredFieldsWriter。每刷新产生segment一次，则该对象就被置空。下次写入就写到另外一个索引文档
  int lastDoc; //

  StoredFieldsConsumer(DocumentsWriterPerThread docWriter) {
    this.docWriter = docWriter;
    this.lastDoc = -1;
  }

  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) { // 若已经初始化了就忽略。每次经过writeBlock()之后就被清空了
      this.writer = // 建立fdx和fdt文件    将跑到Lucene50StoredFieldsFormat.fieldsWriter(es7.9.1时)里面
          docWriter.codec.storedFieldsFormat().fieldsWriter(docWriter.directory, docWriter.getSegmentInfo(),
              IOContext.DEFAULT);
    }
  }

  void startDocument(int docID) throws IOException {
    assert lastDoc < docID;
    initStoredFieldsWriter(); // 初始化了fdt和fdx文件
    while (++lastDoc < docID) {
      writer.startDocument();
      writer.finishDocument();
    }
    writer.startDocument(); // 啥事也不干
  }

  void writeField(FieldInfo info, IndexableField field) throws IOException {
    writer.writeField(info, field); // 比较简单，会存储字段编号，类型，字段value
  }
  // 将整个文档所有域在内存中的结束位置给存储起来。若内存大小16k或者文档数128个超过限制了，会刷到磁盘中
  void finishDocument() throws IOException {
    writer.finishDocument(); // CompressingStoredFieldsWriter，storeField
  }

  void finish(int maxDoc) throws IOException {
    while (lastDoc < maxDoc-1) {
      startDocument(lastDoc);
      finishDocument();
      ++lastDoc;
    }
  }
   // 刷新成segment时会调用
  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    try {
      writer.finish(state.fieldInfos, state.segmentInfo.maxDoc());
    } finally {
      IOUtils.close(writer);
      writer = null;
    }
  }

  void abort() {
    if (writer != null) {
      IOUtils.closeWhileHandlingException(writer);
      writer = null;
    }
  }
}
