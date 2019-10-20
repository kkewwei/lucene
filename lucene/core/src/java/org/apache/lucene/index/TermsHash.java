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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/** This class is passed each token produced by the analyzer
 *  on each field during indexing, and it stores these
 *  tokens in a hash table, and allocates separate byte
 *  streams per token.  Consumers of this class, eg {@link
 *  FreqProxTermsWriter} and {@link TermVectorsConsumer},
 *  write their own byte streams under each term. */
abstract class TermsHash { // FreqProxTermsWriter或者TermVectorsConsumer，TermVectorsConsumer是没有nextTermsHash的
 // 一个线程公用一个
  final TermsHash nextTermsHash;  // TermVectorsConsumer，负责写 tvx, tvd, tvf 信息

  final IntBlockPool intPool;// 存放bytePool中当前可用，一个线程公用一个。两个都是新产生的
  final ByteBlockPool bytePool;//一个线程公用一个, TermVectorsConsumer和FreqProxTermsWriter分别有一个
  ByteBlockPool termBytePool; // 无论是FreqProxTermsWriter或者TermVectorsConsumer，其值都是FreqProxTermsWriter.bytePool，一个线程公用一个
  final Counter bytesUsed;// 这个DocumentsWriterPerThread所使用的内存
  final boolean trackAllocations;
  // 需要注意，这里产生的bytePool、intPool、termBytePool是所有的域共享的存储
  TermsHash(final DocumentsWriterPerThread docWriter, boolean trackAllocations, TermsHash nextTermsHash) {
    this.trackAllocations = trackAllocations;
    this.nextTermsHash = nextTermsHash;
    this.bytesUsed = trackAllocations ? docWriter.bytesUsed : Counter.newCounter(); // 都是新产生的
    intPool = new IntBlockPool(docWriter.intBlockAllocator);// 都是新产生的
    bytePool = new ByteBlockPool(docWriter.byteBlockAllocator);//  // 只有TermVectorsConsumer、TermVectorsConsumerPerField的bytePool是单独相等的。
    // TermVectorsConsumer.termBytePool。FreqProxTermsWriter.bytePool, termBytePool, TermVectorsConsumerPerField.termBytePool，FreqProxTermsWriterPerField.termBytePool, FreqProxTermsWriterPerField.bytePool，两个TermsHashPerField.bytesHash里面的pool全部都是同一个。
    if (nextTermsHash != null) {  // 若不为空，可能使用的是同一个bytePool
      // We are primary
      termBytePool = bytePool;
      nextTermsHash.termBytePool = bytePool;  // 注意这里使用的是同一个byteBlockPool
    }
  }

  public void abort() {
    try {
      reset();
    } finally {
      if (nextTermsHash != null) {
        nextTermsHash.abort();
      }
    }
  }

  // Clear all state
  void reset() {
    // we don't reuse so we drop everything and don't fill with 0
    intPool.reset(false, false); 
    bytePool.reset(false, false);
  }
  // 不是直接跑到这里，第二次才是
  void flush(Map<String,TermsHashPerField> fieldsToFlush, final SegmentWriteState state,
      Sorter.DocMap sortMap, NormsProducer norms) throws IOException { //sortMap为空
    if (nextTermsHash != null) {
      Map<String,TermsHashPerField> nextChildFields = new HashMap<>();
      for (final Map.Entry<String,TermsHashPerField> entry : fieldsToFlush.entrySet()) { // segment范围内就4个域
        nextChildFields.put(entry.getKey(), entry.getValue().getNextPerField());
      }// 写tvd/tvm文件
      nextTermsHash.flush(nextChildFields, state, sortMap, norms);// 跑到TermVectorsConsumer那里，完成termVector相关的刷新工作
    }
  }

  abstract TermsHashPerField addField(FieldInvertState fieldInvertState, FieldInfo fieldInfo);

  void finishDocument(int docID) throws IOException {
    if (nextTermsHash != null) {// 直接跑到next
      nextTermsHash.finishDocument(docID); // 可能会触发128个文档的刷新
    }
  }
  // 还没有对域开始真正处理，每写一个新词，就需要对nextTermsHash开始清理
  void startDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.startDocument();
    }
  }
}
